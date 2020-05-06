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


class Resolver:
    _log: logger.Logger
    _insert_target_lock: threading.Lock

    indices: library.Schedule
    targets: library.UniqueSchedule

    def __init__(self):
        self._log = logger.Logger('Resolver')
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

        try:
            ok, result = script_manager.execute_parser(target.script, 'execute', (target,))
        except scripts.ScriptManagerError:
            return 1, target.script

        if ok:
            if isinstance(result, api.SWaiting):
                self.insert_target(result.target)
                return 3, target.script
            elif isinstance(result, api.SSuccess):
                success_hashes[time.time() + storage.collector.success_hashes_time] = target.hash()
                script_manager.event_handler.success_status(result)
                return 4, target.script
            elif isinstance(result, api.SFail):
                script_manager.event_handler.fail_status(result)
                return 5, target.script
            else:
                return 6, target.script
        else:
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

        ok, targets = script_manager.execute_parser(index.script, 'targets', ())
        if ok:
            if isinstance(targets, (tuple, list)):
                for i in targets:
                    self.insert_target(i)
                self.insert_index(index)
                return 2, index.script
            else:
                return 3, index.script
        else:
            return 1, index.script


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


class Collector(ThreadClass):
    log: logger.Logger
    parsers_hash: str

    def __init__(self):
        super().__init__('Collector', CollectorError)
        self.parsers_hash = ''

    def step_parsers_check(self) -> None:
        if script_manager.hash() != self.parsers_hash:
            self.log.info(codes.Code(20301))
            for i in script_manager.parsers:
                ok, index = script_manager.execute_parser(i, 'index', ())
                if ok:
                    resolver.insert_index(index, True)
                else:
                    self.log.error(codes.Code(40302, i))
            self.parsers_hash = script_manager.hash()
            self.log.info(codes.Code(20302))

    def run(self) -> None:
        self.state = 1
        while True:
            start: float = time.time()
            if self.state == 1:  # Active state
                try:
                    del success_hashes[:time.time()]
                    self.step_parsers_check()

                    for i in resolver.indices.pop(slice(time.time())):
                        try:
                            index_queue.put(
                                library.PrioritizedItem(resolver.index_priority(i[1]), i[1]),
                                timeout=storage.queues.index_queue_size
                            )
                        except queue.Full:
                            self.log.warn(codes.Code(30302, i))

                    for i in resolver.get_targets():
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
            time.sleep(storage.collector.tick - delta if storage.collector.tick - delta >= 0 else 0)


class Worker(ThreadClass):
    _name: str = 'Worker'

    id: int
    additional: bool
    speed: float
    idle: int
    start_time: float

    def __init__(self, id_: int, name: str = ''):
        super().__init__(f'{name if name else self._name}-{id_}', WorkerError)
        self.id = id_
        self.speed = .0
        self.idle = 0
        self.start_time = time.time()

    def log_target_exec(self, status: int, script: str) -> None:
        if status == 1:
            self.log.error(f'Target lost in pipeline while executing (script "{script}" not found)')
        elif status == 2:
            if storage.main.production:
                self.log.error(f'Target execution failed ({script})')
            else:
                self.log.fatal(WorkerError(f'Target execution failed ({script})'))
        elif status == 3:
            self.log.debug(f'Target executed ({script})')
        elif status == 4:
            self.log.info(f'Successful target execution ({script})')
        elif status == 5:
            self.log.warn(f'Failed target execution ({script})')
        elif status == 6:
            self.log.warn(f'Unknown status received while executing target ({script})')

    def run(self) -> None:
        self.state = 1
        while True:
            start: float = time.time()
            if self.state == 1:
                try:
                    self.log_target_exec(*resolver.execute_target())
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


class IndexWorker(Worker):
    _name: str = 'IndexWorker'

    def __init__(self, id_: int, name: str = ''):
        super().__init__(id_, name if name else self._name)

    def run(self):
        self.state = 1
        while True:
            start: float = time.time()
            if self.state == 1:
                try:
                    status, script = resolver.execute_index()

                    if status == 1:
                        if storage.main.production:
                            self.log.error(f'Catalog execution failed ({script})')
                        else:
                            self.log.fatal(WorkerError(f'Catalog execution failed ({script})'))
                    elif status == 2:
                        self.log.info(f'Catalog updated ({script})')
                    elif status == 3:
                        if storage.main.production:
                            self.log.error(f'Catalog execution failed ({script})')
                        else:
                            self.log.fatal(CollectorError(f'Catalog execution failed ({script})'))

                    if status < 2:
                        status, script = resolver.execute_target()
                        self.log_target_exec(status, script)
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
            time.sleep(storage.index_worker.tick - delta if storage.index_worker.tick - delta >= 0 else 0)


class ThreadManager(ThreadClass):
    workers_increment_id: int
    index_workers_increment_id: int
    workers: Dict[int, Worker]
    index_workers: Dict[int, Worker]
    collector: Collector

    def __init__(self) -> None:
        super().__init__('ThreadManager', ThreadManagerError)
        self.workers_increment_id = 0
        self.index_workers_increment_id = 0
        self.workers = {}
        self.index_workers = {}
        self.collector = None

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
        if len(self.workers) < storage.worker.count:
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

    def check_index_workers(self) -> None:
        if len(self.index_workers) < storage.index_worker.count:
            self.index_workers[self.index_workers_increment_id] = IndexWorker(self.index_workers_increment_id)
            self.log.info(codes.Code(20205, f'IndexWorker-{self.index_workers_increment_id}'))
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
        self.collector.state = 5
        self.collector.join(storage.collector.wait)
        self.collector = None

    def run(self) -> None:
        self.state = 1
        while True:
            try:
                start: float = time.time()
                if self.state == 1:
                    self.check_collector()
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
        return storage.collector.wait + len(self.workers) * (storage.worker.wait + 1) + len(self.index_workers) * (storage.index_worker.wait + 1)


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
