import queue
import threading
import time
import traceback
from typing import Tuple, Dict, Type

import uctp
import yaml
from Crypto.PublicKey import RSA

from . import analytics
from . import api
from . import cache
from . import codes
from . import commands
from . import library
from . import logger
from . import scripts
from . import storage

# TODO: throw() for state setters


config_file = 'core/config.yaml'
script_manager: scripts.ScriptManager = scripts.ScriptManager()
success_hashes: library.Schedule = library.Schedule()


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


class StateError(Exception):
    pass


class ThreadClass(threading.Thread):
    _exception: Type[Exception]
    _state: int

    log: logger.Logger

    def __init__(self, name: str, exception: Type[Exception]):
        if not isinstance(name, str):
            raise TypeError('name must be str')

        super().__init__(name=name, daemon=True)

        self._state = 0
        self._exception = exception

        self.log = logger.Logger(name)

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, value: int) -> None:
        if isinstance(value, int):
            if self._state == 1 and value not in (2, 5):
                raise StateError('In this state, you can change state only to 2 or 5')
            elif self._state in (2, 4, 5):
                raise StateError('State locked')
            elif self._state == 3 and value not in (4, 5):
                raise StateError('In this state, you can change state only to 4 or 5')
            else:
                self._state = value
        else:
            ValueError('state must be int')

    def throw(self, code: codes.Code) -> None:
        script_manager.event_handler.alert(code, self.name)
        if not storage.main.production:
            self.log.fatal(self._exception(code))


task_queue: queue.PriorityQueue = queue.PriorityQueue(storage.queues.task_queue_size)
target_queue: queue.Queue = queue.Queue(storage.queues.target_queue_size)
server = uctp.peer.Peer(
    'monitor',
    RSA.import_key(open('./monitor.pem').read()),
    '0.0.0.0',
    trusted=uctp.peer.Trusted(*yaml.safe_load(open('authority.yaml'))['trusted']),
    aliases=uctp.peer.Aliases(yaml.safe_load(open('authority.yaml'))['aliases']),
    auth_timeout=4,
    buffer=8192
)


class Collector(ThreadClass):
    log: logger.Logger
    schedule_indices: library.Schedule
    schedule_targets: library.UniqueSchedule
    parsers_hash: str

    def __init__(self):
        super().__init__('Collector', CollectorError)
        self.schedule_indices = library.Schedule()
        self.schedule_targets = library.UniqueSchedule(storage.collector.targets_hashes_size)
        self.parsers_hash = ''

    def insert_index(self, index: api.IndexType, now: float, force: bool = False) -> None:
        if isinstance(index, api.IOnce):
            if force:
                self.schedule_indices[now] = index
        elif isinstance(index, api.IInterval):
            self.schedule_indices[now + (0 if force else index.interval)] = index
        else:
            if storage.main.production:
                self.log.error(codes.Code(40301, index))
            else:
                self.log.fatal(CollectorError(codes.Code(40301, index)))

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
                    self.log.warn(codes.Code(30301, i))
                except IndexError:
                    self.log.test(f'Inserting non-unique target')

    def step_parsers_check(self) -> None:
        if script_manager.hash() != self.parsers_hash:
            self.log.info(codes.Code(20301))
            for i in script_manager.parsers:
                ok, index = script_manager.execute_parser(i, 'index', ())
                if ok:
                    self.insert_index(index, time.time(), True)
                else:
                    self.log.error(codes.Code(40302, i))
            self.parsers_hash = script_manager.hash()
            self.log.info(codes.Code(20302))

    def step_reindex(self) -> bool:
        try:
            id_, index = next(self.schedule_indices.get_slice_gen(time.time()))
            ok, targets = script_manager.execute_parser(index.script, 'targets', ())
            if ok:
                if isinstance(targets, (tuple, list)):
                    self.log.debug(codes.Code(10301, f'{len(targets)}, {index.script}'))
                    self.insert_target(targets)
                else:
                    if storage.main.production:
                        self.log.error(codes.Code(40302, index.script))
                    else:
                        self.log.fatal(CollectorError((codes.Code(40302, index.script))))
                self.insert_index(index, time.time())
                self.schedule_indices.pop_item(id_)
                return True
            else:
                self.log.error(codes.Code(40303, index.script))
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
                            priority = storage.api.priority_TSmart[0] + v.reuse(storage.api.priority_TSmart[1])
                        elif isinstance(v, api.TScheduled):
                            priority = storage.api.priority_TScheduled[0] + v.reuse(storage.api.priority_TScheduled[1])
                        elif isinstance(v, api.TInterval):
                            priority = storage.api.priority_TInterval[0] + v.reuse(storage.api.priority_TInterval[1])
                        else:
                            priority = 1001
                        task_queue.put(library.PrioritizedItem(priority, v), timeout=storage.queues.task_queue_put_wait)
                    except queue.Full:
                        self.log.warn(codes.Code(30302, v))
                else:
                    self.log.error(codes.Code(40304, v))
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
                    self.throw(codes.Code(50301, f'While working: {e.__class__.__name__}: {e.__str__()}'))
                    break
            elif self.state == 2:  # Pausing state
                self.log.info(codes.Code(20002))
                self.state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self.log.info(codes.Code(20003))
                self.state = 1
            elif self.state == 5:  # Stopping state
                self.log.info(codes.Code(20005))
                break
            delta: float = time.time() - start
            time.sleep(storage.collector.collector_tick - delta if storage.collector.collector_tick - delta >= 0 else 0)


class Worker(ThreadClass):
    id: int
    additional: bool
    speed: float
    idle: int
    start_time: float

    def __init__(self, id_: int, additional: bool = False, postfix: str = ''):
        super().__init__(f'Worker-{id_}{postfix}', WorkerError)
        self.id = id_
        self.additional = additional
        self.speed = .0
        self.idle = 0
        self.start_time = time.time()

    def execute(self, target: api.TargetType) -> bool:
        self.log.debug(codes.Code(10401, target))
        if target.content_hash() not in success_hashes.values():  # TODO: Optimize here
            try:
                ok, status = script_manager.execute_parser(target.script, 'execute', (target,))
                if ok:
                    if isinstance(status, api.SWaiting):
                        self.log.debug(codes.Code(10402, status))
                        try:
                            target_queue.put(status.target, timeout=storage.queues.target_queue_put_wait)
                        except queue.Full:
                            self.log.warn(codes.Code(30402, status.target))
                    elif isinstance(status, api.SSuccess):
                        self.log.info(codes.Code(20401, status.result))
                        success_hashes[time.time() + storage.collector.success_hashes_time] = target.content_hash()
                        script_manager.event_handler.success_status(status)
                    elif isinstance(status, api.SFail):
                        self.log.warn(codes.Code(30401, status))
                        script_manager.event_handler.fail_status(status)
                        return False
                    else:
                        self.log.debug(codes.Code(10402, status))
                        if storage.main.production:
                            self.log.error(codes.Code(40401, target))
                        else:
                            self.log.fatal(CollectorError(codes.Code(40401, target)))
                        return True
                    return True
                else:
                    self.log.error(codes.Code(40402, target))
                    return False
            except scripts.ScriptManagerError:
                self.log.error(codes.Code(40403, target))

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
                        self.idle += 1
                    if target:
                        self.execute(target.content)
                        self.idle = 0
                except Exception as e:
                    self.throw(codes.Code(50401, f'While working: {e.__class__.__name__}: {e.__str__()}'))
                    break
            elif self.state == 2:  # Pausing state
                self.log.info(codes.Code(20002))
                self.state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self.log.info(codes.Code(20003))
                self.state = 1
            elif self.state == 5:  # Stopping state
                self.log.info(codes.Code(20005))
                break
            delta: float = time.time() - start
            self.speed = round(1 / delta, 3) if self.idle == 0 else 0
            time.sleep(storage.worker.worker_tick - delta if storage.worker.worker_tick - delta >= 0 else 0)


class ThreadManager(ThreadClass):
    workers_increment_id: int
    workers: Dict[int, Worker]
    collector: Collector

    def __init__(self) -> None:
        super().__init__('ThreadManager', ThreadManagerError)
        self.workers_increment_id = 0
        self.workers = {}
        self.collector = None

    def workers_count(self, additional: bool = False) -> int:
        count = 0
        for i in self.workers.values():
            if i.additional == additional:
                count += 1
        return count

    def check_collector(self) -> None:
        if not self.collector:
            self.collector = Collector()
            self.log.info(codes.Code(20201))
        if not self.collector.is_alive():
            try:
                self.collector.start()
                self.log.info(codes.Code(20202))
            except RuntimeError:
                self.log.warn(codes.Code(30201))
                self.collector = None

    def check_workers(self) -> None:
        if self.workers_count() < storage.worker.workers_count:
            self.workers[self.workers_increment_id] = Worker(self.workers_increment_id)
            self.log.info(codes.Code(20203, f'Worker-{self.workers_increment_id}'))
            self.workers_increment_id += 1
        for v in tuple(self.workers.values()):
            if not v.is_alive():
                try:
                    v.start()
                    self.log.info(codes.Code(20204, f'{v.name}'))
                except RuntimeError:
                    self.log.warn(codes.Code(30202, f'{v.name}'))
                    del self.workers[v.id]

    def stop_threads(self) -> None:
        for i in self.workers.values():
            i.state = 5
        for i in tuple(self.workers):
            self.workers[i].join(storage.worker.worker_wait)
            del self.workers[i]
        self.collector.state = 5
        self.collector.join(storage.collector.collector_wait)
        self.collector = None

    def run(self) -> None:
        self.state = 1
        while True:
            try:
                start: float = time.time()
                if self.state == 1:
                    self.check_collector()
                    self.check_workers()
                elif self.state == 2:  # Pausing state
                    self.log.info(codes.Code(20002))
                    self.state = 3
                elif self.state == 3:  # Paused state
                    pass
                elif self.state == 4:  # Resuming state
                    self.log.info(codes.Code(20003))
                    self.state = 1
                elif self.state == 5:  # Stopping state
                    self.log.info(codes.Code(20004))
                    self.stop_threads()
                    self.log.info(codes.Code(20005))
                    break
                delta: float = time.time() - start
                time.sleep(storage.thread_manager.thread_manager_tick - delta if
                           storage.thread_manager.thread_manager_tick - delta >= 0 else 0)
            except Exception as e:
                self.log.fatal_msg(codes.Code(50201, f'{e.__class__.__name__}: {e.__str__()}'), traceback.format_exc())
                self.stop_threads()
                self.throw(codes.Code(50201, f'While working: {e.__class__.__name__}: {e.__str__()}'))
                break

    def close(self) -> float:
        self.state = 5
        return storage.collector.collector_wait + self.workers.__len__() * (storage.worker.worker_wait + 1)


class Main:
    state: int
    log: logger.Logger
    thread_manager: ThreadManager

    def __init__(self, config_file_: str = None):
        global config_file
        if config_file_:
            config_file = config_file_
        refresh()
        self.state = 0
        self.log = logger.Logger('Core')
        self.thread_manager = ThreadManager()

    def turn_on(self) -> bool:
        if storage.main.production:
            self.log.info(codes.Code(20101))
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
        self.state = 1
        commands.Commands(server, self)

        analytics.analytics.dump(0)
        self.turn_on()
        server.run()

        self.thread_manager.start()

        script_manager.event_handler.monitor_turned_on()

        try:
            while 0 < self.state < 2:
                try:
                    if self.thread_manager.is_alive():
                        time.sleep(1)
                    else:
                        self.log.fatal(MonitorError(codes.Code(50101)))
                except KeyboardInterrupt:
                    self.log.info(codes.Code(20102))
                    self.state = 2
                except MonitorError:
                    self.state = 2
        finally:
            self.log.info(codes.Code(20103))
            self.turn_off()
            self.thread_manager.join(self.thread_manager.close())
            self.log.info(codes.Code(20104))
            cache.dump_success_hashes(success_hashes)
            self.log.info(codes.Code(20105))
            script_manager.event_handler.monitor_turned_off()
            analytics.analytics.stop()
            script_manager.unload_all()
            script_manager.del_()
            self.log.info(codes.Code(20106))
