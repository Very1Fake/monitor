import queue
import threading
import time
import traceback
from typing import Tuple, Dict, Type, List

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


class ResolverError(Exception):
    pass


class ThreadManagerError(Exception):
    pass


class CollectorError(Exception):
    pass


class PipeError(Exception):
    pass


class WorkerError(Exception):
    pass


class IndexWorkerError(Exception):
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


class Resolver:
    _log: logger.Logger
    _insert_target_lock: threading.Lock

    indices: library.Schedule
    targets: library.UniqueSchedule

    def __init__(self):
        self._log = logger.Logger('R')
        self._insert_target_lock = threading.Lock()

        self.indices = library.Schedule()
        self.targets = library.UniqueSchedule()

    @staticmethod
    def index_priority(index: api.IndexType) -> int:
        if isinstance(index, api.IOnce):
            return storage.api.priority_IOnce
        elif isinstance(index, api.IInterval):
            return storage.api.priority_IInterval
        else:
            return storage.api.priority_interval_default

    @staticmethod
    def target_priority(target: api.TargetType) -> int:
        if isinstance(target, api.TSmart):
            return storage.api.priority_TSmart[0] + target.reuse(storage.api.priority_TSmart[1])
        elif isinstance(target, api.TScheduled):
            return storage.api.priority_TScheduled[0] + target.reuse(storage.api.priority_TScheduled[1])
        elif isinstance(target, api.TInterval):
            return storage.api.priority_TInterval[0] + target.reuse(storage.api.priority_TInterval[1])
        else:
            return storage.api.priority_target_default

    def get_targets(self) -> List[api.TargetType]:
        time_: float = time.time()
        targets: list = []
        if any(self.targets[:time_]):
            for k, v in self.targets[:time_]:
                if v.script in script_manager.scripts and v.script in script_manager.parsers:
                    targets.append(v)
                else:
                    self._log.warn(codes.Code(30901, v))
            del self.targets[:time_]
        return targets

    def insert_target(self, target: api.TargetType) -> None:
        with self._insert_target_lock:
            if target.hash() not in success_hashes.values():
                try:
                    if isinstance(target, api.TSmart):
                        time_ = library.smart_extractor(
                            library.smart_gen(target.timestamp, target.length, target.scatter),
                            time.time()
                        )
                        if time_:  # TODO: Fix here (expired must be checked once)
                            self.targets[time_] = target
                        else:
                            self._log.warn(f'Smart target expired: {target}')
                    elif isinstance(target, api.TScheduled):
                        self.targets[target.timestamp] = target
                    elif isinstance(target, api.TInterval):
                        self.targets[time.time() + target.interval] = target
                    else:
                        self._log.error(codes.Code(40902, target))
                except ValueError:
                    self._log.fatal_msg(codes.Code(30301, target))
                except IndexError:
                    self._log.test(f'Inserting non-unique target')

    def execute_target(self) -> Tuple[int, str]:  # TODO: Combine in one function for new API
        try:
            target: api.TargetType = target_queue.get_nowait().content
            if target.hash() in success_hashes.values():
                raise ResolverError
        except (queue.Empty, ResolverError):
            return 0, ''

        self._log.debug(codes.Code(10901, str(target)), threading.current_thread().name)

        try:
            ok, result = script_manager.execute_parser(target.script, 'execute', (target,))
        except scripts.ScriptManagerError:
            self._log.warn(codes.Code(30902, str(target)), threading.current_thread().name)
            return 1, target.script

        if ok:
            if isinstance(result, api.SWaiting):
                self.insert_target(result.target)
                return 3, target.script
            elif isinstance(result, api.SSuccess):
                success_hashes[time.time() + storage.pipe.success_hashes_time] = target.hash()
                script_manager.event_handler.success_status(result)
                self._log.info(codes.Code(20901, str(result)), threading.current_thread().name)
                return 4, target.script
            elif isinstance(result, api.SFail):
                script_manager.event_handler.fail_status(result)
                self._log.warn(codes.Code(30903, target), threading.current_thread().name)
                return 5, target.script
            else:
                self._log.warn(codes.Code(30904, target), threading.current_thread().name)
                return 6, target.script
        else:
            if storage.main.production:
                self._log.error(codes.Code(40903, target), threading.current_thread().name)
            else:
                self._log.fatal(WorkerError(codes.Code(40903, target)), parent=threading.current_thread().name)
            return 2, target.script

    def insert_index(self, index: api.IndexType, force: bool = False) -> None:  # TODO: Make `now` argument here
        if isinstance(index, (api.IOnce, api.IInterval)):
            if force:
                self.indices[time.time()] = index
            else:
                if isinstance(index, api.IInterval):
                    self.indices[time.time() + index.interval] = index
        else:
            if storage.main.production:
                self._log.error(codes.Code(40901, index))
            else:
                self._log.fatal(CollectorError(codes.Code(40901, index)))

    def execute_index(self) -> Tuple[int, str]:  # TODO: Combine in one function for new API
        try:
            index: api.IndexType = index_queue.get_nowait().content
        except queue.Empty:
            return 0, ''

        self._log.debug(codes.Code(10902, index), threading.current_thread().name)

        try:
            ok, targets = script_manager.execute_parser(index.script, 'targets', ())
        except scripts.ScriptManagerError:
            self._log.warn(codes.Code(30905, index), threading.current_thread().name)
            return 1, index.script

        if ok:
            if isinstance(targets, (tuple, list)):
                for i in targets:
                    self.insert_target(i)
                self.insert_index(index)
                self._log.debug(codes.Code(20902, index), threading.current_thread().name)
                return 3, index.script
            else:
                self._log.warn(codes.Code(30906, index), threading.current_thread().name)
                return 4, index.script
        else:
            self._log.error(codes.Code(40904, index), threading.current_thread().name)
            return 2, index.script


index_queue: queue.PriorityQueue = queue.PriorityQueue(storage.queues.index_queue_size)
target_queue: queue.PriorityQueue = queue.PriorityQueue(storage.queues.target_queue_size)
resolver: Resolver = Resolver()
server = uctp.peer.Peer(
    'monitor',
    RSA.import_key(open('./monitor.pem').read()),
    '0.0.0.0',
    trusted=uctp.peer.Trusted(*yaml.safe_load(open('authority.yaml'))['trusted']),
    aliases=uctp.peer.Aliases(yaml.safe_load(open('authority.yaml'))['aliases']),
    auth_timeout=4,
    buffer=8192
)


class Pipe(ThreadClass):
    parsers_hash: str

    def __init__(self):
        super().__init__('P', PipeError)
        self.parsers_hash = ''

    def run(self) -> None:
        self.state = 1
        while True:
            start: float = time.time()
            if self.state == 1:  # Active state
                try:
                    del success_hashes[:time.time()]  # Cleanup expired hashes

                    if script_manager.hash() != self.parsers_hash:  # Check for scripts (loaded/unloaded)
                        self.log.info('Parsers reindexing started')
                        for i in script_manager.parsers:
                            ok, index = script_manager.execute_parser(i, 'index', ())
                            if ok:
                                resolver.insert_index(index, True)
                            else:
                                self.log.error(f'Reindexing failed ({i})')
                        self.parsers_hash = script_manager.hash()
                        self.log.info('Parsers reindexing complete')

                    for i in resolver.indices.pop(slice(time.time())):  # Send indices
                        try:
                            index_queue.put(
                                library.PrioritizedItem(resolver.index_priority(i[1]), i[1]),
                                timeout=storage.queues.index_queue_size
                            )
                        except queue.Full:  # TODO: Fix (index can't be lost)
                            self.log.warn(codes.Code(30303, i))

                    for i in resolver.get_targets():  # Send targets
                        try:
                            target_queue.put(
                                library.PrioritizedItem(resolver.target_priority(i), i),
                                timeout=storage.queues.target_queue_put_wait
                            )
                        except queue.Full:
                            self.log.warn(codes.Code(30302, i))

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
            time.sleep(storage.pipe.tick - delta if storage.pipe.tick - delta >= 0 else 0)


class Worker(ThreadClass):
    id: int
    start_time: float
    speed: float
    idle: int
    last_tick: float

    def __init__(self, id_: int):
        super().__init__(f'W-{id_}', WorkerError)
        self.id = id_
        self.speed = .0
        self.idle = 0
        self.start_time = time.time()
        self.last_tick = 0

    def run(self) -> None:
        self.state = 1
        while True:
            start = self.last_tick = time.time()
            if self.state == 1:
                try:
                    resolver.execute_target()
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
            self.speed = 0 if self.idle else round(1 / delta, 3)
            time.sleep(storage.worker.tick - delta if storage.worker.tick - delta >= 0 else 0)


class IndexWorker(ThreadClass):
    id: int
    start_time: float
    speed: float
    idle: int
    last_tick: float

    def __init__(self, id_: int):
        super().__init__(f'IW-{id_}', IndexWorkerError)
        self.id = id_
        self.speed = .0
        self.idle = 0
        self.start_time = time.time()
        self.last_tick = 0

    def run(self):
        self.state = 1
        while True:
            start = self.last_tick = time.time()
            if self.state == 1:
                try:
                    if resolver.execute_index()[0] < 2:
                        resolver.execute_target()
                except Exception as e:
                    self.throw(codes.Code(51001, f'While working: {e.__class__.__name__}: {e.__str__()}'))
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
            self.speed = 0 if self.idle else round(1 / delta, 3)
            time.sleep(storage.index_worker.tick - delta if storage.index_worker.tick - delta >= 0 else 0)


class ThreadManager(ThreadClass):
    workers_increment_id: int
    index_workers_increment_id: int
    workers: Dict[int, Worker]
    index_workers: Dict[int, IndexWorker]
    pipe: Pipe

    def __init__(self) -> None:
        super().__init__('TM', ThreadManagerError)
        self.workers_increment_id = 0
        self.index_workers_increment_id = 0
        self.workers = {}
        self.index_workers = {}
        self.pipe = None

    def check_pipe(self) -> None:
        if not self.pipe:
            self.pipe = Pipe()
            self.log.info(codes.Code(20201))
        if not self.pipe.is_alive():
            try:
                self.pipe.start()
                self.log.info(codes.Code(20202))
            except RuntimeError:
                self.log.warn(codes.Code(30201))
                self.pipe = None

    def check_workers(self) -> None:
        if len(self.workers) < storage.worker.count:
            self.workers[self.workers_increment_id] = Worker(self.workers_increment_id)
            self.log.info(codes.Code(20203, f'W-{self.workers_increment_id}'))
            self.workers_increment_id += 1
        for v in tuple(self.workers.values()):
            if not v.is_alive():
                try:
                    v.start()
                    self.log.info(codes.Code(20204, f'{v.name}'))
                except RuntimeError:
                    self.log.warn(codes.Code(30202, f'{v.name}'))
                    del self.workers[v.id]

    def check_index_workers(self) -> None:
        if len(self.index_workers) < storage.index_worker.count:
            self.index_workers[self.index_workers_increment_id] = IndexWorker(self.index_workers_increment_id)
            self.log.info(codes.Code(20205, f'IW-{self.index_workers_increment_id}'))
            self.index_workers_increment_id += 1
        for v in tuple(self.index_workers.values()):
            if not v.is_alive():
                try:
                    v.start()
                    self.log.info(codes.Code(20206, f'{v.name}'))
                except RuntimeError:
                    self.log.warn(codes.Code(30203, f'{v.name}'))
                    del self.index_workers[v.id]

    def stop_threads(self) -> None:
        for i in self.workers.values():
            i.state = 5
        for i in tuple(self.workers):
            self.workers[i].join(storage.worker.wait)
            del self.workers[i]
        for i in self.index_workers.values():
            i.state = 5
        for i in tuple(self.index_workers):
            self.index_workers[i].join(storage.index_worker.wait)
            del self.index_workers[i]
        self.pipe.state = 5
        self.pipe.join(storage.pipe.wait)
        self.pipe = None

    def run(self) -> None:
        self.state = 1
        while True:
            try:
                start: float = time.time()
                if self.state == 1:
                    self.check_pipe()
                    self.check_workers()
                    self.check_index_workers()
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
                time.sleep(storage.thread_manager.tick - delta if
                           storage.thread_manager.tick - delta >= 0 else 0)
            except Exception as e:
                self.log.fatal_msg(codes.Code(50201, f'{e.__class__.__name__}: {e.__str__()}'), traceback.format_exc())
                self.stop_threads()
                self.throw(codes.Code(50201, f'While working: {e.__class__.__name__}: {e.__str__()}'))
                break

    def close(self) -> float:
        self.state = 5
        return storage.pipe.wait + len(self.workers) * (
                storage.worker.wait + 1) + len(self.index_workers) * (storage.index_worker.wait + 1)


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
        self.log = logger.Logger('C')
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
