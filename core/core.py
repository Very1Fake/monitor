import queue
import threading
import time
from typing import Tuple, List, Dict

from . import api
from . import cache
from . import library
from . import logger
from . import scripts
from . import storage

# TODO: Success hashes object


config_file = 'core/config.yaml'
script_manager: scripts.ScriptManager = scripts.ScriptManager()
success_hashes: library.Schedule = library.Schedule()
task_queue: queue.PriorityQueue = queue.PriorityQueue(storage.task_queue_size)
target_queue: queue.Queue = queue.Queue(storage.target_queue_size)


def refresh() -> None:
    storage.reload_config(config_file)


def refresh_success_hashes():
    success_hashes.update(cache.load_success_hashes())


class MonitorError(Exception):
    pass


class ThreadManagerError(Exception):
    pass


class CollectorError(Exception):
    pass


class WorkerError(Exception):
    pass


class Collector(threading.Thread):
    _state: int
    log: logger.Logger
    schedule_indices: library.Schedule
    schedule_targets: library.UniqueSchedule
    parsers_hash: str

    def __init__(self):
        super().__init__(name='Collector', daemon=True)
        self._state = 0
        self.log = logger.Logger(self.name)
        self.schedule_indices = library.Schedule()
        self.schedule_targets = library.UniqueSchedule(storage.targets_hashes_size)
        self.parsers_hash = ''

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, value: int) -> None:
        if isinstance(value, int):
            if self._state == 1 and value not in (2, 5):
                raise CollectorError('In this state, you can change state only to 2 or 5')
            elif self._state in (2, 4, 5):
                raise CollectorError('State locked')
            elif self._state == 3 and value not in (4, 5):
                raise CollectorError('In this state, you can change state only to 4 or 5')
            else:
                self._state = value
        else:
            ValueError('state must be int')

    def throw(self, message, description: str = '') -> None:
        if storage.production:
            if self.log.error(message):
                script_manager.event_handler.error(description + '\n' + message if description else message, self.name)
        else:
            script_manager.event_handler.fatal(CollectorError(
                description + '\n' + message if description else message
            ), self.name)
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
                        time_ = library.smart_extractor(
                            library.smart_gen(i.timestamp, i.length, i.scatter),
                            time.time()
                        )
                        if time_:  # TODO: Fix here (expired must be checked once)
                            self.schedule_targets[time_] = i
                        else:
                            self.log.warn(f'Smart target expired: {i}')
                    elif isinstance(i, api.TScheduled):
                        self.schedule_targets[i.timestamp] = i
                    elif isinstance(i, api.TInterval):
                        self.schedule_targets[time.time() + i.interval] = i
                except ValueError:
                    if storage.production:
                        self.log.warn(f'Target lost while inserting: {i}')
                    else:
                        self.log.fatal(CollectorError(f'Target lost while inserting: {i}'))
                except IndexError:
                    self.log.test(f'Inserting non-unique target')

    def step_parsers_check(self) -> None:
        if script_manager.hash() != self.parsers_hash:
            self.log.info('Reindexing parsers')
            for i in script_manager.parsers:
                ok, index = script_manager.execute_parser(i, 'index', ())
                if ok:
                    self.insert_index(index, time.time(), True)
                else:
                    self.log.error(f'Parser execution failed {i}')
            self.parsers_hash = script_manager.hash()
            self.log.info('Reindexing parsers complete')

    def step_reindex(self) -> bool:
        try:
            id_, index = next(self.schedule_indices.get_slice_gen(time.time()))
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
                self.insert_index(index, time.time())
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
            except queue.Empty:
                return

    def step_send_tasks(self) -> None:
        if any(self.schedule_targets.get_slice_gen(time.time())):
            ids: Tuple[float] = ()
            for k, v in self.schedule_targets.get_slice_gen(time.time()):
                if v.script in script_manager.scripts and v.script in script_manager.parsers:
                    try:
                        if isinstance(v, api.TSmart):
                            priority = storage.priority_TSmart[0] + v.reuse(storage.priority_TSmart[1])
                        elif isinstance(v, api.TScheduled):
                            priority = storage.priority_TScheduled[0] + v.reuse(storage.priority_TScheduled[1])
                        elif isinstance(v, api.TInterval):
                            priority = storage.priority_TInterval[0] + v.reuse(storage.priority_TInterval[1])
                        else:
                            priority = 1001
                        task_queue.put(library.PrioritizedItem(priority, v), timeout=storage.task_queue_put_wait)
                    except queue.Full as e:
                        if storage.production:
                            self.log.error(f'Target lost in pipeline: {v}')
                        else:
                            self.log.fatal(CollectorError(f'Target lost in pipeline: {v}'), e)
                else:
                    self.log.error(f'Target lost in pipeline (script unloaded): {v}')
                ids += (k,)
            self.schedule_targets.pop_item(ids)

    def run(self) -> None:
        self.state = 1
        while True:
            start: float = time.time()
            if self.state == 1:  # Active state
                try:
                    success_hashes.del_slice(time.time())
                    self.step_parsers_check()
                    self.step_reindex()
                    self.step_target_queue_check()
                    self.step_send_tasks()
                except Exception as e:
                    self.throw(
                        f'While working: {e.__class__.__name__}: {e.__str__()}',
                        f'Collector unexpectedly turned off'
                    )
                    break
            elif self.state == 2:  # Pausing state
                self.log.info('Thread paused')
                self.state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self.log.info('Thread resumed')
                self.state = 1
            elif self.state == 5:  # Stopping state
                self.log.info('Thread closed')
                break
            delta: float = time.time() - start
            time.sleep(storage.collector_tick - delta if storage.collector_tick - delta >= 0 else 0)


class Worker(threading.Thread):
    id: int
    additional: bool
    _state: int
    log: logger.Logger

    def __init__(self, id_: int, additional: bool = False, postfix: str = ''):
        super().__init__(name=f'Worker-{id_}{postfix}', daemon=True)
        self.id = id_
        self.additional = additional
        self._state = 0
        self.log = logger.Logger(self.name)

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, value: int) -> None:
        if isinstance(value, int):
            if self._state == 1 and value not in (2, 5):
                raise WorkerError('In this state, you can change state only to 2 or 5')
            elif self._state in (2, 4, 5):
                raise WorkerError('State locked')
            elif self._state == 3 and value not in (4, 5):
                raise WorkerError('In this state, you can change state only to 4 or 5')
            else:
                self._state = value
        else:
            ValueError('state must be int')

    def throw(self, message, description: str = '') -> None:
        if storage.production:
            if self.log.error(message):
                script_manager.event_handler.error(description + '\n' + message if description else message, self.name)
        else:
            script_manager.event_handler.fatal(WorkerError(
                description + '\n' + message if description else message
            ), self.name)
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
                        except queue.Full as e:
                            if storage.production:
                                self.log.error(f'Target lost in pipeline: {status.target}')
                            else:
                                self.log.fatal(WorkerError(f'Target lost in pipeline: {status.target}'), e)
                    elif isinstance(status, api.SSuccess):
                        self.log.info(f'Item now available: {status.result}')
                        success_hashes[time.time() + storage.success_hashes_time] = target.content_hash()
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
        self.state = 1
        while True:
            start: float = time.time()
            if self.state == 1:
                try:
                    target: library.PrioritizedItem = None
                    try:
                        target = task_queue.get_nowait()
                    except queue.Empty:
                        pass
                    if target:
                        self.execute(target.content)
                except Exception as e:
                    self.throw(
                        f'While working: {e.__class__.__name__}: {e.__str__()}',
                        f'{self.name} unexpectedly turned off'
                    )
                    break
            elif self.state == 2:  # Pausing state
                self.log.info('Thread paused')
                self.state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self.log.info('Thread resumed')
                self.state = 1
            elif self.state == 5:  # Stopping state
                self.log.info('Thread closed')
                break
            delta: float = time.time() - start
            time.sleep(storage.worker_tick - delta if storage.worker_tick - delta >= 0 else 0)


class ThreadManager(threading.Thread):
    state: int
    log: logger.Logger
    workers_increment_id: int
    workers: Dict[int, Worker]
    collector: Collector

    def __init__(self) -> None:
        super().__init__(name='ThreadManager', daemon=True)
        self.state = 0
        self.log = logger.Logger('ThreadManager')
        self.workers_increment_id = 0
        self.workers = {}
        self.collector = None

    def throw(self, message, description: str = '') -> None:
        if storage.production:
            if self.log.error(message):
                script_manager.event_handler.error(description + '\n' + message if description else message, self.name)
        else:
            script_manager.event_handler.fatal(ThreadManagerError(
                description + '\n' + message if description else message
            ), self.name)
            self.log.fatal(ThreadManagerError(message))

    def workers_count(self, additional: bool = False) -> int:
        count = 0
        for i in self.workers.values():
            if i.additional == additional:
                count += 1
        return count

    def check_collector(self) -> None:
        if not self.collector:
            self.collector = Collector()
            self.log.info('Collector initialized')
        if not self.collector.is_alive():
            try:
                self.collector.start()
                self.log.info('Collector started')
            except RuntimeError:
                self.log.error('Collector was unexpectedly stopped')
                self.collector = None

    def check_workers(self) -> None:
        if self.workers_count() < storage.workers_count:
            self.workers[self.workers_increment_id] = Worker(self.workers.__len__())
            self.log.info(f'Worker-{self.workers_increment_id} initialized')
            self.workers_increment_id += 1
        for v in self.workers.values():
            if not v.is_alive():
                try:
                    v.start()
                    self.log.info(f'{v.name} started')
                except RuntimeError:
                    self.log.error(f'{v.name} was unexpectedly stopped')
                    del self.workers[v.id]

    def stop_threads(self) -> None:
        for i in tuple(self.workers.values()):
            i.state = 5
            i.join(storage.worker_wait)
        self.collector.state = 5
        self.collector.join(storage.collector_wait)

    def run(self) -> None:
        self.state = 1
        while True:
            try:
                start: float = time.time()
                if self.state == 1:
                    self.check_collector()
                    self.check_workers()
                elif self.state == 2:  # Pausing state
                    self.log.info('Thread paused')
                    self.state = 3
                elif self.state == 3:  # Paused state
                    pass
                elif self.state == 4:  # Resuming state
                    self.log.info('Thread resumed')
                    self.state = 1
                elif self.state == 5:  # Stopping state
                    self.log.info('Thread closing')
                    self.stop_threads()
                    self.log.info('Thread closed')
                    break
                delta: float = time.time() - start
                time.sleep(storage.thread_manager_tick - delta if storage.thread_manager_tick - delta >= 0 else 0)
            except Exception as e:
                self.log.fatal_msg(f'Exception raised, emergency stop initiated. {e.__class__.__name__}: {e.__str__()}')
                self.stop_threads()
                self.throw(
                    f'While working: {e.__class__.__name__}: {e.__str__()}',
                    f'ThreadManager unexpectedly turned off'
                )
                break

    def close(self) -> float:
        self.state = 5
        return storage.collector_wait + self.workers.__len__() * (storage.worker_wait + 1)


class Main:
    log: logger.Logger
    thread_manager: ThreadManager

    def __init__(self, config_file_: str = None):
        global config_file
        if config_file_:
            config_file = config_file_
        storage.check_config(config_file)
        refresh()
        self.log = logger.Logger('Core')
        self.thread_manager = ThreadManager()

    def turn_on(self) -> bool:
        if storage.production:
            self.log.info('Production mode enabled!')
        script_manager.index.reindex()
        script_manager.load_all()
        script_manager.event_handler.monitor_turning_on()
        refresh_success_hashes()
        return True

    @staticmethod
    def turn_off() -> bool:
        refresh()
        script_manager.event_handler.monitor_turning_off()
        return True

    def start(self):
        self.turn_on()

        self.thread_manager.start()

        script_manager.event_handler.monitor_turned_on()

        try:
            while self.thread_manager.is_alive():
                time.sleep(1)
            self.log.fatal(MonitorError('ThreadManager unexpectedly has turned off'))
        except KeyboardInterrupt:
            self.log.info('Signal Interrupt!')
        finally:
            self.log.info('Turning off...')
            self.turn_off()
            self.thread_manager.join(self.thread_manager.close())
            self.log.info('Saving success hashes')
            cache.dump_success_hashes(success_hashes)
            self.log.info('Saving success hashes complete')
            script_manager.event_handler.monitor_turned_off()
            script_manager.unload_all()
            script_manager.del_()
            self.log.info('Done')
