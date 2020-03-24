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
state: dict = {'mode': 0}
lock: threading.Lock = threading.Lock()
script_manager: scripts.ScriptManager = scripts.ScriptManager()
success_hashes: library.Schedule = library.Schedule()
task_queue: PriorityQueue = PriorityQueue(storage.task_queue_size)
target_queue: Queue = Queue(storage.target_queue_size)


def refresh(**kwargs) -> None:
    lock.acquire()
    for k, v in kwargs.items():
        state[k] = v
    storage.reload_config(config_file)
    lock.release()


def refresh_success_hashes():
    success_hashes.update(cache.load_success_hashes())


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
        self.schedule_indices: library.Schedule = library.Schedule()
        self.schedule_targets: library.UniqueSchedule = library.UniqueSchedule(storage.targets_hashes_size)
        self.parsers_hash: str = ''

    def throw(self, message) -> None:
        if storage.production:
            if self.log.error(message):
                script_manager.event_handler.error(message, self.name)
        else:
            script_manager.event_handler.fatal(CollectorError(message), self.name)
            self.log.fatal(CollectorError(message))

    def insert_index(self, index: api.IndexType, now: float, force: bool = False) -> None:
        if isinstance(index, api.IOnce):
            if force:
                self.schedule_indices[now] = index
        elif isinstance(index, api.IInterval):
            self.schedule_indices[now + (0 if force else index.interval)] = index
        else:
            if storage.production:
                self.log.error(f'Unknown index "{index}"')
            else:
                self.log.fatal(CollectorError(f'Unknown index "{index}"'))

    def insert_target(self, target):
        if not isinstance(target, (tuple, list)):
            target = (target,)
        for i in target:
            if i.content_hash() not in success_hashes.values():
                try:
                    if isinstance(i, api.TSmart):
                        time_ = library.smart_extractor(library.smart_gen(i.timestamp, i.length, i.scatter), time())
                        if time_:  # TODO: Fix here (expired must be checked once)
                            self.schedule_targets[time_] = i
                        else:
                            self.log.warn(f'Smart target expired: {i}')
                    elif isinstance(i, api.TScheduled):
                        self.schedule_targets[i.timestamp] = i
                    elif isinstance(i, api.TInterval):
                        self.schedule_targets[time() + i.interval] = i
                except ValueError:
                    if storage.production:
                        self.log.warn(f'Target lost while inserting: {i}')
                    else:
                        self.log.fatal(CollectorError(f'Target lost while inserting: {i}'))
                except IndexError:
                    self.log.warn(f'Inserting non-unique target')

    def step_parsers_check(self) -> None:
        if script_manager.hash() != self.parsers_hash:
            self.log.info('Reindexing parsers')
            for i in script_manager.parsers:
                ok, index = script_manager.execute_parser(i, 'index', ())
                if ok:
                    self.insert_index(index, time(), True)
                else:
                    self.log.error(f'Parser execution failed {i}')
            self.parsers_hash = script_manager.hash()
            self.log.info('Reindexing parsers complete')

    def step_reindex(self) -> bool:
        try:
            id_, index = next(self.schedule_indices.get_slice_gen(time()))
            ok, targets = script_manager.execute_parser(index.script, 'targets', ())
            if ok:
                if isinstance(targets, (tuple, list)):
                    self.log.debug(f'{len(targets)} targets received from "{index.script}"')
                    self.insert_target(targets)
                else:
                    if storage.production:
                        self.log.error(f'Wrong target list received from "{index.script}"')
                    else:
                        self.log.fatal(CollectorError(f'Wrong target list received from "{index.script}"'))
                self.insert_index(index, time())
                self.schedule_indices.pop_item(id_)
                return True
            else:
                self.log.error(f'Parser execution failed {index.script}')
                return False
        except StopIteration:
            return False

    def step_target_queue_check(self) -> None:
        if not target_queue.empty():
            try:
                while True:
                    self.insert_target(target_queue.get_nowait())
            except Empty:
                return

    def step_send_tasks(self) -> None:
        if any(self.schedule_targets.get_slice_gen(time())):
            ids: Tuple[float] = ()
            for k, v in self.schedule_targets.get_slice_gen(time()):
                if v.script in script_manager.scripts and v.script in script_manager.parsers:
                    try:
                        if isinstance(v, api.TSmart):
                            priority = 10
                        elif isinstance(v, api.TScheduled):
                            priority = 50
                        elif isinstance(v, api.TInterval):
                            priority = 100
                        else:
                            priority = 1000
                        task_queue.put(library.PrioritizedItem(priority, v), timeout=storage.task_queue_put_wait)
                    except Full as e:
                        if storage.production:
                            self.log.error(f'Target lost in pipeline: {v}')
                        else:
                            self.log.fatal(CollectorError(f'Target lost in pipeline: {v}'), e)
                    ids += (k,)
                else:
                    self.log.error(f'Target lost in pipeline (script unloaded): {v}')
            self.schedule_targets.pop_item(ids)

    def run(self) -> None:
        while True:
            start: float = time()
            if state['mode'] == 1:
                success_hashes.del_slice(time())
                self.step_parsers_check()
                self.step_reindex()
                self.step_target_queue_check()
                self.step_send_tasks()
            elif state['mode'] == 3:
                self.log.info('Thread closed')
                break
            delta: float = time() - start
            sleep(storage.collector_tick - delta if storage.collector_tick - delta >= 0 else 0)


class Worker(threading.Thread):
    def __init__(self, id_: int):
        super().__init__(name=f'Worker-{id_}', daemon=True)
        self.id = id_
        self.log: logger.Logger = logger.Logger(self.name)

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
            try:
                ok, status = script_manager.execute_parser(target.script, 'execute', (target,))
                if ok:
                    if isinstance(status, api.SWaiting):
                        self.log.debug(f'Result: {status}')
                        try:
                            target_queue.put(status.target, timeout=storage.target_queue_put_wait)
                        except Full as e:
                            if storage.production:
                                self.log.error(f'Target lost in pipeline: {status.target}')
                            else:
                                self.log.fatal(WorkerError(f'Target lost in pipeline: {status.target}'), e)
                    elif isinstance(status, api.SSuccess):
                        self.log.info(f'Item now available: {status.result}')
                        success_hashes[time() + storage.success_hashes_time] = target.content_hash()
                        script_manager.event_handler.success_status(status)
                    elif isinstance(status, api.SFail):
                        self.log.warn(f'Target lost: {status}')
                        script_manager.event_handler.fail_status(status)
                        return False
                    else:
                        self.log.debug(f'Result: {status}')
                        if storage.production:
                            self.log.error(f'Unknown status received while executing: {target}')
                        else:
                            self.log.fatal(CollectorError(f'Unknown status received while executing: {target}'))
                        return True
                    return True
                else:
                    self.log.error(f'Parser execution failed "{target}"')
                    return False
            except scripts.ScriptManagerError:
                self.log.error(f'Target lost in pipeline (script unloaded): {target}')

    def run(self) -> None:
        while True:
            start: float = time()
            if state['mode'] == 1:
                target: library.PrioritizedItem = None
                try:
                    target = task_queue.get_nowait()
                except Empty:
                    pass
                if target:
                    self.execute(target.content)
            elif state['mode'] == 3:
                self.log.info('Thread closed')
                break
            delta: float = time() - start
            sleep(storage.worker_tick - delta if storage.worker_tick - delta >= 0 else 0)


class Main:
    def __init__(self, config_file_: str = None):
        global config_file
        if config_file_:
            config_file = config_file_
        storage.check_config(config_file)
        refresh()
        self.log: logger.Logger = logger.Logger('Core')
        self.collector: Collector = Collector()
        self.workers_increment: int = 0
        self.workers: List[Worker] = []

    def turn_on(self) -> bool:
        if storage.production:
            self.log.info('Production mode enabled!')
        script_manager.index.reindex()
        script_manager.load_all()
        script_manager.event_handler.monitor_turning_on()
        refresh(mode=1)
        refresh_success_hashes()
        return True

    @staticmethod
    def turn_off() -> bool:
        refresh(mode=3)
        script_manager.event_handler.monitor_turning_off()
        return True

    def get_worker(self, id_: int):
        for i in self.workers:
            if i.id == id_:
                return i

    def add_workers(self, count: int):
        for i in range(count):
            self.workers.append(Worker(self.workers_increment))
            self.get_worker(self.workers_increment).start()
            self.workers_increment += 1

    def wait_workers(self):
        for i in self.workers:
            i.join(storage.worker_wait)

    def start(self):
        self.turn_on()

        self.collector.start()
        self.add_workers(storage.workers_count)

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
            self.collector.join(storage.collector_wait)
            self.log.info('Saving success hashes')
            cache.dump_success_hashes(success_hashes)
            self.log.info('Saving success hashes complete')
            script_manager.event_handler.monitor_turned_off()
            script_manager.unload_all()
            self.log.info('Done')
