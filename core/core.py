import threading
from queue import Queue, PriorityQueue, Empty, Full
from time import sleep, time
from typing import Tuple, List

from . import api
from . import cache
from . import library
from . import logger
from . import scripts
from . import storage


# TODO: Success hashes object


config_file = 'core/config.yaml'
config: dict = {'state': 0}
lock: threading.Lock = threading.Lock()
script_manager: scripts.ScriptManager = scripts.ScriptManager()
success_hashes: library.Schedule = library.Schedule()
task_queue: PriorityQueue = None
target_queue: Queue = None


def refresh(**kwargs) -> None:
    lock.acquire()
    for k, v in kwargs.items():
        config[k] = v
    storage.reload_config(config_file)
    config.update(storage.snapshot())
    success_hashes.update(cache.load_success_hashes())
    lock.release()


class MonitorError(Exception):
    pass


class CollectorError(Exception):
    pass


class WorkerError(Exception):
    pass


class Collector(threading.Thread):
    def __init__(self):
        super().__init__(name='Collector', daemon=True)
        self.log: logger.Logger = logger.Logger(self.name)
        self.lock: threading.Lock = threading.Lock()
        self.schedule_indices: library.Schedule = library.Schedule()
        self.schedule_targets: library.UniqueSchedule = library.UniqueSchedule(config['targets_hashes_size'])

    def throw(self, message) -> None:
        if storage.production:
            if self.log.error(message):
                script_manager.event_handler.error(message, self.name)
        else:
            script_manager.event_handler.fatal(CollectorError(message), self.name)
            self.log.fatal(CollectorError(message))

    def reset_index(self):
        self.schedule_indices.clear()
        for i in script_manager.parsers:
            parser = script_manager.parsers[i]()
            index = parser.index()
            if isinstance(index, api.IInterval):
                self.schedule_indices[time()] = index
            else:
                if config['production']:
                    self.log.error(f'Unknown index while resetting index "{i}"')
                else:
                    self.log.fatal(CollectorError(f'Unknown index while resetting index "{i}"'))

    def insert_index(self, index: api.IndexType):
        if isinstance(index, api.IInterval):
            self.schedule_indices[time() + index.interval] = index
        else:
            if config['production']:
                self.log.error(f'Unknown index "{index}"')
            else:
                self.log.fatal(CollectorError(f'Unknown index "{index}"'))

    def reindex(self, start: float):
        to_delete: Tuple[float] = ()
        to_insert: Tuple[api.TargetType] = ()
        for k, v in self.schedule_indices.get_slice_gen(start):  # TODO: Script unload protection
            targets: Tuple[api.TargetType] = script_manager.parsers[v.script]().targets()
            if isinstance(targets, tuple) or isinstance(targets, list):
                self.log.debug(f'{len(targets)} targets received from "{v.script}"')
                self.insert_targets(targets)
            else:
                if config['production']:
                    self.log.error(f'Wrong target list received from "{v.script}"')
                else:
                    self.log.fatal(CollectorError(f'Wrong target list received from "{v.script}"'))
            if isinstance(v, api.IInterval):
                to_insert += (v,)
                to_delete += (k,)
        for i in to_insert:
            self.insert_index(i)
        self.schedule_indices.pop_item(to_delete)

    def insert_targets(self, targets: Tuple[api.TargetType]):
        for i in targets:
            success_hashes.del_slice(time())
            if i.content_hash() not in success_hashes.values():
                if isinstance(i, api.TInterval):
                    try:
                        self.schedule_targets[time() + i.interval] = i
                    except ValueError:
                        pass

    def send_tasks(self, start: float):
        ids: Tuple[float] = ()
        for k, v in self.schedule_targets.get_slice_gen(start):  # TODO: Script unload protection
            try:
                task_queue.put(library.PrioritizedItem(100, v), timeout=config['task_queue_put_wait'])
            except Full as e:
                if config['production']:
                    self.log.error(f'Target lost: {v}')
                else:
                    self.log.fatal(CollectorError(f'Target lost: {v}'), e)
            ids += (k,)
        self.schedule_targets.pop_item(ids)

    def run(self) -> None:
        parsers_hash: str = ''
        while True:
            start: float = time()
            if config['state'] == 1:
                if script_manager.hash() != parsers_hash:
                    self.log.info('Reindexing parsers')
                    self.reset_index()
                    parsers_hash = script_manager.hash()
                    self.log.info('Reindexing parsers complete')
                if any(self.schedule_indices.get_slice_gen(start)):  # TODO: One reindex per tick
                    self.reindex(start)
                if not target_queue.empty():
                    targets: Tuple[api.TargetType] = ()
                    try:
                        while True:
                            targets += (target_queue.get_nowait(),)
                    except Empty:
                        pass
                    self.insert_targets(targets)
                if any(self.schedule_targets.get_slice_gen(start)):
                    self.send_tasks(start)
            elif config['state'] == 3:
                self.log.info('Thread closed')
                break
            delta: float = time() - start
            sleep(config['collector_tick'] - delta if config['collector_tick'] - delta >= 0 else 0)


class Worker(threading.Thread):
    def __init__(self, _id: int):
        super().__init__(name=f'Worker-{_id}', daemon=True)
        self.id = _id
        self.log: logger.Logger = logger.Logger(self.name)
        self.lock: threading.Lock = threading.Lock()

    def throw(self, message) -> None:
        if storage.production:
            if self.log.error(message):
                script_manager.event_handler.error(message, self.name)
        else:
            script_manager.event_handler.fatal(WorkerError(message), self.name)
            self.log.fatal(WorkerError(message))

    def execute(self, target: api.TargetType) -> bool:
        self.log.debug(f'Executing: {target}')
        if target.content_hash() not in success_hashes.values():  # TODO: Optimize here
            status: api.StatusType = script_manager.parsers[target.script]().execute(target)
            if isinstance(status, api.SWaiting):
                self.log.debug(f'Result: {status}')
                try:
                    target_queue.put(status.target, timeout=config['target_queue_put_wait'])
                except Full as e:
                    if config['production']:
                        self.log.error(f'Target lost: {status.target}')
                    else:
                        self.log.fatal(WorkerError(f'Target lost: {status.target}'), e)
            elif isinstance(status, api.SSuccess):
                self.log.info(f'Item now available: {status.result}')
                success_hashes[time() + config['success_hashes_time']] = target.content_hash()
                script_manager.event_handler.success_status(status)
            elif isinstance(status, api.SFail):
                self.log.warn(f'Target lost: {status}')
                script_manager.event_handler.fail_status(status)
                return False
            else:
                self.log.debug(f'Result: {status}')
                if config['production']:
                    self.log.error(f'Unknown status received while executing "{target}"')
                else:
                    self.log.fatal(CollectorError(f'Unknown status received while executing "{target}"'))
                return True
            return True

    def run(self) -> None:
        while True:
            start: float = time()
            if config['state'] == 1:
                target: library.PrioritizedItem = None
                try:
                    target = task_queue.get(timeout=config['task_queue_get_wait'])
                except Empty:
                    pass
                if target:
                    self.execute(target.content)
            elif config['state'] == 3:
                self.log.info('Thread closed')
                break
            delta: float = time() - start
            sleep(config['worker_tick'] - delta if config['worker_tick'] - delta >= 0 else 0)


class Main:
    def __init__(self, _config_file: str = None):
        global config_file
        if _config_file:
            config_file = _config_file
        storage.check_config(config_file)
        refresh()
        self.log: logger.Logger = logger.Logger('Core')
        self.collector: Collector = Collector()
        self.workers_increment: int = 0
        self.workers: List[Worker] = []

    @staticmethod
    def turn_off() -> bool:
        refresh(state=3)
        script_manager.event_handler.monitor_turning_off()
        return True

    @staticmethod
    def turn_on() -> bool:
        script_manager.load_all()
        script_manager.event_handler.monitor_turning_on()
        refresh(state=1)
        return True

    def get_worker(self, _id: int):
        for i in self.workers:
            if i.id == _id:
                return i

    def add_workers(self, count: int):
        for i in range(count):
            self.workers.append(Worker(self.workers_increment))
            self.get_worker(self.workers_increment).start()
            self.workers_increment += 1

    def wait_workers(self):
        for i in self.workers:
            i.join(config['worker_wait'])

    @staticmethod
    def init_globals():
        global task_queue
        global target_queue
        task_queue = PriorityQueue(storage.task_queue_size)
        target_queue = Queue(storage.target_queue_size)

    def start(self):
        self.turn_on()

        self.init_globals()

        self.collector.start()
        self.add_workers(config['workers_count'])

        script_manager.event_handler.monitor_turned_on()

        try:
            while self.collector.is_alive():
                sleep(1)
            self.log.fatal(MonitorError('Collector unexpectedly has turned off'))
        except KeyboardInterrupt:
            self.log.info('Signal Interrupt!')
        finally:
            self.log.info('Turning off...')
            self.turn_off()
            self.wait_workers()
            self.collector.join(config['collector_wait'])
            script_manager.unload_all()
            self.log.info('Saving success hashes')
            cache.dump_success_hashes(success_hashes)
            self.log.info('Saving success hashes complete')
            self.log.info('Done')
