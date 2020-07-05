import queue
import random
import threading
import time
import traceback
from typing import Tuple, Dict, Type, List, Union

import uctp
import yaml
from Crypto.PublicKey import RSA

from . import analytics
from . import api
from . import codes
from . import commands
from . import logger
from . import scripts
from . import storage
from . import tools
from .cache import UniquenessError, HashStorage
from .library import PrioritizedItem, UniqueSchedule, Provider


# TODO: throw() for state setters


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


class CatalogWorkerError(Exception):
    pass


class StateError(Exception):
    pass


class ThreadClass(threading.Thread):
    _exception: Type[Exception]
    _log: logger.Logger
    _state: int

    def __init__(self, name: str, exception: Type[Exception]):
        if not isinstance(name, str):
            raise TypeError('name must be str')

        super().__init__(name=name, daemon=True)

        self._exception = exception
        self._log = logger.Logger(name)
        self._state = 0

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, value: int) -> None:  # TODO: Set 5 from any state
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
            self._log.fatal(self._exception(code))


class Resolver:
    _log: logger.Logger = logger.Logger('R')
    _catalog_lock: threading.RLock = threading.RLock()
    _target_lock: threading.RLock = threading.RLock()

    catalog_queue: queue.PriorityQueue = queue.PriorityQueue(storage.queues.catalog_queue_size)
    target_queue: queue.PriorityQueue = queue.PriorityQueue(storage.queues.target_queue_size)

    catalogs: UniqueSchedule = UniqueSchedule()
    targets: UniqueSchedule = UniqueSchedule()

    @staticmethod
    def catalog_priority(catalog: api.CatalogType) -> int:
        if isinstance(catalog, api.CSmart):
            return storage.priority.CSmart
        elif isinstance(catalog, api.CScheduled):
            return storage.priority.CScheduled
        elif isinstance(catalog, api.CInterval):
            return storage.priority.CInterval
        else:
            return storage.priority.catalog_default

    @staticmethod
    def target_priority(target: Union[api.TargetType, api.RestockTargetType]) -> int:
        if isinstance(target, api.TSmart):
            return storage.priority.TSmart[0] + target.reuse(storage.priority.TSmart[1])
        elif isinstance(target, api.TScheduled):
            return storage.priority.TScheduled[0] + target.reuse(storage.priority.TScheduled[1])
        elif isinstance(target, api.TInterval):
            return storage.priority.TInterval[0] + target.reuse(storage.priority.TInterval[1])
        elif isinstance(target, api.RTSmart):
            return storage.priority.RTSmart[0] + target.reuse(storage.priority.RTSmart[1])
        elif isinstance(target, api.RTScheduled):
            return storage.priority.RTScheduled[0] + target.reuse(storage.priority.RTScheduled[1])
        elif isinstance(target, api.RTInterval):
            return storage.priority.RTInterval[0] + target.reuse(storage.priority.RTInterval[1])
        else:
            return storage.priority.target_default

    @classmethod
    def insert_catalog(cls, catalog: api.CatalogType, force: bool = False) -> None:
        with cls._catalog_lock:
            try:
                if issubclass(type(catalog), api.Catalog):
                    if force:
                        cls.catalogs[time.time()] = catalog
                    else:
                        if isinstance(catalog, api.CSmart):
                            if catalog.expired:
                                cls._log.warn(codes.Code(30911, str(catalog)))
                            else:
                                if (time_ := catalog.gen.extract()) == catalog.gen.time:
                                    catalog.expired = True

                                for i in range(100):  # TODO: Optimize
                                    if time_ in cls.catalogs:
                                        time_ += .0001
                                    else:
                                        cls.catalogs[time_] = catalog
                                        break
                                else:
                                    cls._log.error(f'Smart catalog lost (calibration not passed): {catalog}')
                        elif isinstance(catalog, api.CScheduled):
                            cls.catalogs[catalog.timestamp] = catalog
                        elif isinstance(catalog, api.CInterval):
                            cls.catalogs[time.time() + catalog.interval] = catalog
                else:
                    if storage.main.production:
                        cls._log.error(codes.Code(40901, catalog), threading.current_thread().name)
                    else:
                        cls._log.fatal(CollectorError(codes.Code(40901, catalog)),
                                       parent=threading.current_thread().name)
            except IndexError:
                cls._log.test(f'Inserting non-unique catalog')

    @classmethod
    def insert_target(cls, target: api.TargetType) -> None:
        with cls._target_lock:
            if HashStorage.check_target(target.hash()):
                try:
                    if isinstance(target, api.TSmart):
                        if target.expired:
                            cls._log.warn(codes.Code(30912, str(target)))
                        else:
                            if (time_ := target.gen.extract()) == target.gen.time:
                                target.expired = True

                            for i in range(100):  # TODO: Optimize
                                if time_ in cls.targets:
                                    time_ += .0001
                                else:
                                    cls.targets[time_] = target
                                    break
                            else:
                                cls._log.error(f'Smart target lost (calibration not passed): {target}')
                    elif isinstance(target, api.TScheduled):
                        cls.targets[target.timestamp] = target
                    elif isinstance(target, api.TInterval):
                        cls.targets[time.time() + target.interval] = target
                    else:
                        cls._log.error(codes.Code(40902, target), threading.current_thread().name)
                except IndexError:
                    cls._log.test(f'Inserting non-unique target')

    @classmethod
    def remove_catalog(cls, script: str):
        if not isinstance(script, str):
            raise TypeError('script must be str')

        with cls._catalog_lock, cls.catalog_queue.mutex:
            schedule_id = -1
            queue_id = -1

            for k, v in cls.catalogs.items():
                if v.script == script:
                    schedule_id = k

            for i, v in enumerate(cls.catalog_queue.queue):
                if v.content.script == script:
                    queue_id = i

            cls.catalogs.pop(schedule_id) if schedule_id > -1 else ...
            cls.catalog_queue.queue.pop(queue_id) if queue_id > -1 else ...

    @classmethod
    def remove_targets(cls, script: str):  # TODO: Check from core
        if not isinstance(script, str):
            raise TypeError('script must be str')

        with cls._target_lock, cls.target_queue.mutex:
            schedule_ids = []
            queue_ids = []

            for k, v in cls.targets.items():
                if v.script == script:
                    schedule_ids.append(k)

            for i, v in enumerate(cls.target_queue.queue):
                if v.content.script == script:
                    queue_ids.append(i)

            for i in schedule_ids:
                cls.targets.pop(i)

            for i in queue_ids:
                cls.target_queue.queue.pop(i)

    @classmethod
    def get_catalogs(cls) -> List[api.CatalogType]:
        with cls._catalog_lock:
            time_ = time.time()
            catalogs = []
            if any(cls.catalogs[:time_]):
                for k, v in cls.catalogs[:time_]:
                    if v.script in script_manager.scripts and v.script in script_manager.parsers:
                        catalogs.append(v)
                    elif v.script not in script_manager.scripts:
                        cls._log.warn(codes.Code(30901, v), threading.current_thread().name)
                    elif v.script not in script_manager.parsers:
                        cls._log.warn(codes.Code(30902, v), threading.current_thread().name)
                del cls.catalogs[:time_]
            return catalogs

    @classmethod
    def get_targets(cls) -> List[Union[api.TargetType, api.RestockTargetType]]:
        with cls._target_lock:
            time_ = time.time()
            targets = []
            if any(cls.targets[:time_]):
                for k, v in cls.targets[:time_]:
                    if v.script in script_manager.scripts and v.script in script_manager.parsers:
                        targets.append(v)
                    elif v.script not in script_manager.scripts:
                        cls._log.warn(codes.Code(30903, v), threading.current_thread().name)
                    elif v.script not in script_manager.parsers:
                        cls._log.warn(codes.Code(30904, v), threading.current_thread().name)
                del cls.targets[:time_]
            return targets

    @classmethod
    def execute(cls, mode: int = 0) -> Tuple[int, str]:
        try:
            if mode == 0:
                task: api.CatalogType = cls.catalog_queue.get_nowait().content
            elif mode == 1:
                task: api.TargetType = cls.target_queue.get_nowait().content
            else:
                raise ValueError(f'Unknown mode ({mode})')
        except queue.Empty:
            return 0, ''

        if mode == 0:
            cls._log.debug(codes.Code(10901, task), threading.current_thread().name)
        elif mode == 1:
            cls._log.debug(codes.Code(10902, task), threading.current_thread().name)

        try:
            result: list = script_manager.execute_parser(task.script, 'execute', (mode, task))

            if not isinstance(result, list):
                if mode == 0:
                    cls._log.error(codes.Code(30907, task), threading.current_thread().name)
                elif mode == 1:
                    cls._log.error(codes.Code(30910, task), threading.current_thread().name)
                script_manager.parser_error(task.script)
                return 1, task.script
        except scripts.ScriptNotFound:
            if mode == 0:
                cls._log.warn(codes.Code(30905, task), threading.current_thread().name)
            elif mode == 1:
                cls._log.warn(codes.Code(30908, task), threading.current_thread().name)
            return 2, task.script
        except scripts.ParserImplementationError:
            if mode == 0:
                cls._log.warn(codes.Code(30906, task), threading.current_thread().name)
            elif mode == 1:
                cls._log.warn(codes.Code(30909, task), threading.current_thread().name)
            return 3, task.script
        except Exception as e:
            if mode == 0:
                code = 40903
            elif mode == 1:
                code = 40904

            cls._log.fatal_msg(
                codes.Code(code, f'{task.script}: {e.__class__.__name__}: {str(e)}'),
                traceback.format_exc(),
                threading.current_thread().name
            )
            script_manager.event_handler.alert(codes.Code(code, f'{task.script}: {e.__class__.__name__}: {str(e)}'),
                                               threading.current_thread().name)
            return 4, task.script

        catalog: api.CatalogType = None
        targets: List[api.TargetType] = []

        for i in result:
            if issubclass(type(i), api.Item):
                if isinstance(i, api.IAnnounce):
                    if HashStorage.check_item(i.hash(3), True):
                        HashStorage.add_announced_item(i.hash(3))
                        script_manager.event_handler.item_announced(i)
                elif isinstance(i, api.IRelease):
                    if HashStorage.check_item(i.hash(4)):
                        id_ = HashStorage.add_item(i)
                        script_manager.event_handler.item_released(i)

                        if i.restock:
                            i.restock.id = id_
                            targets.append(i.restock)
                elif isinstance(i, api.IRestock):
                    if HashStorage.check_item_id(i.id):
                        HashStorage.add_item(i, True)
                        script_manager.event_handler.item_restock(i)
            elif issubclass(type(i), api.Catalog):
                if not catalog:
                    catalog = i
            elif issubclass(type(i), (api.Target, api.RestockTarget)):
                targets.append(i)
            elif issubclass(type(i), api.TargetEnd):
                try:
                    HashStorage.add_target(i.target.hash())
                except UniquenessError:
                    pass

                if isinstance(i, api.TEFail):
                    script_manager.event_handler.target_end_failed(i)
                elif isinstance(i, api.TESoldOut):
                    script_manager.event_handler.target_end_sold_out(i)
                elif isinstance(i, api.TESuccess):
                    script_manager.event_handler.target_end_success(i)

        if catalog:
            cls.remove_catalog(catalog.script)
            cls.insert_catalog(catalog)

        for i in targets:
            cls.insert_target(i)

        if mode == 0:
            cls._log.debug(codes.Code(10903), threading.current_thread().name)
        elif mode == 1:
            cls._log.debug(codes.Code(10904), threading.current_thread().name)

        return 5, task.script


class Pipe(ThreadClass):
    parsers_hashes: Dict[str, str]

    def __init__(self):
        super().__init__('P', PipeError)
        self.parsers_hashes = {}

    @staticmethod
    def _compare_parsers(old: Dict[str, str], new: Dict[str, str]):
        different = []
        for i in new:
            if i in old:
                if new[i] != old[i]:
                    different.append(i)
            else:
                different.append(i)
        return different

    def run(self) -> None:
        self.state = 1
        while True:
            start: float = time.time()
            if self.state == 1:  # Active state
                try:
                    HashStorage.cleanup()  # Cleanup expired hashes

                    if different := self._compare_parsers(
                            self.parsers_hashes, script_manager.hash()):  # Check for scripts (loaded/unloaded)
                        with script_manager.lock:
                            self._log.info(codes.Code(20301))
                            for i in different:
                                self._log.debug(codes.Code(10301, i))
                                try:
                                    if issubclass(type(catalog := script_manager.parsers[i].catalog), api.Catalog):
                                        resolver.insert_catalog(catalog, True)
                                        self._log.info(codes.Code(20303, i))
                                    else:
                                        self._log.error(codes.Code(40301, i))
                                except Exception as e:
                                    self._log.warn(codes.Code(30301, f'{i}: {e.__class__.__name__}: {str(e)}'))
                            self.parsers_hashes = script_manager.hash()
                            self._log.info(codes.Code(20302))
                    elif self.parsers_hashes != script_manager.hash():
                        self.parsers_hashes = script_manager.hash()

                    for i in resolver.get_catalogs():  # Send catalogs
                        try:
                            resolver.catalog_queue.put(
                                PrioritizedItem(resolver.catalog_priority(i), i),
                                timeout=storage.queues.catalog_queue_size
                            )
                        except queue.Full:  # TODO: Fix (catalog can't be lost)
                            self._log.warn(codes.Code(30302, i))

                    for i in resolver.get_targets():  # Send targets
                        try:
                            resolver.target_queue.put(
                                PrioritizedItem(resolver.target_priority(i), i),
                                timeout=storage.queues.target_queue_put_wait
                            )
                        except queue.Full:
                            self._log.warn(codes.Code(30303, i))

                except Exception as e:
                    self.throw(codes.Code(50301, f'While working: {e.__class__.__name__}: {str(e)}'))
                    break
            elif self.state == 2:  # Pausing state
                self._log.info(codes.Code(20002))
                self._state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self._log.info(codes.Code(20003))
                self._state = 1
            elif self.state == 5:  # Stopping state
                self._log.info(codes.Code(20005))
                break
            delta: float = time.time() - start
            time.sleep(storage.pipe.tick - delta if storage.pipe.tick - delta > 0 else 0)


class Worker(ThreadClass):
    id: int
    start_time: float
    speed: float
    idle: bool
    last_tick: float

    def __init__(self, id_: int):
        super().__init__(f'W-{id_}', WorkerError)
        self.id = id_
        self.speed = .0
        self.idle = True
        self.start_time = time.time()
        self.last_tick = 0

    def run(self) -> None:
        self.state = 1
        while True:
            start = self.last_tick = time.time()
            if self.state == 1:
                try:
                    if resolver.execute(1)[0] > 1:
                        self.idle = False
                    else:
                        self.idle = True
                except Exception as e:
                    self.throw(codes.Code(50401, f'While working: {e.__class__.__name__}: {str(e)}'))
                    break
            elif self.state == 2:  # Pausing state
                self._log.info(codes.Code(20002))
                self._state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self._log.info(codes.Code(20003))
                self._state = 1
            elif self.state == 5:  # Stopping state
                self._log.info(codes.Code(20005))
                break
            delta: float = time.time() - start
            self.speed = 0 if self.idle else round(1 / delta, 3)
            time.sleep(storage.worker.tick - delta if storage.worker.tick - delta > 0 else 0)


class CatalogWorker(ThreadClass):
    id: int
    start_time: float
    speed: float
    idle: bool
    last_tick: float

    def __init__(self, id_: int):
        super().__init__(f'CW-{id_}', CatalogWorkerError)
        self.id = id_
        self.speed = .0
        self.idle = True
        self.start_time = time.time()
        self.last_tick = 0

    def run(self):
        self._state = 1
        while True:
            start = self.last_tick = time.time()
            if self.state == 1:
                try:
                    if resolver.execute()[0] < 1:
                        if resolver.execute(1)[0] > 1:  # TODO: Switcher for assistance
                            self.idle = False
                        else:
                            self.idle = True
                    else:
                        self.idle = False
                except Exception as e:
                    self.throw(codes.Code(51001, f'While working: {e.__class__.__name__}: {str(e)}'))
                    break
            elif self.state == 2:  # Pausing state
                self._log.info(codes.Code(20002))
                self._state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self._log.info(codes.Code(20003))
                self._state = 1
            elif self.state == 5:  # Stopping state
                self._log.info(codes.Code(20005))
                break
            delta: float = time.time() - start
            self.speed = 0 if self.idle else round(1 / delta, 3)
            time.sleep(storage.catalog_worker.tick - delta if storage.catalog_worker.tick - delta > 0 else 0)


class ThreadManager(ThreadClass):
    _lock_ticks: int

    lock: threading.RLock
    catalog_workers_increment_id: int
    catalog_workers: Dict[int, CatalogWorker]
    workers_increment_id: int
    workers: Dict[int, Worker]
    pipe: Pipe

    def __init__(self) -> None:
        super().__init__('TM', ThreadManagerError)
        self._lock_ticks = 0

        self.lock = threading.RLock()
        self.catalog_workers_increment_id = 0
        self.catalog_workers = {}
        self.workers_increment_id = 0
        self.workers = {}
        self.pipe = Pipe()

    def check_pipe(self) -> None:
        with self.lock:
            if not self.pipe.is_alive():
                try:
                    self.pipe.start()
                    self._log.info(codes.Code(20202))
                except RuntimeError:
                    if self.pipe.state == 5:
                        self._log.warn(codes.Code(30201))
                    else:
                        self._log.error(codes.Code(40201))
                    self.pipe = Pipe()
                    self._log.info(codes.Code(20201))

    def check_workers(self) -> None:
        with self.lock:
            if len(self.workers) < storage.worker.count:
                self.workers[self.workers_increment_id] = Worker(self.workers_increment_id)
                self._log.info(codes.Code(20203, f'W-{self.workers_increment_id}'))
                self.workers_increment_id += 1
            elif len(self.workers) > storage.worker.count:
                try:
                    self.stop_worker()
                except StateError:
                    pass

            for v in list(self.workers.values()):
                if not v.is_alive():
                    try:
                        v.start()
                        self._log.info(codes.Code(20204, str(v.id)))
                    except RuntimeError:
                        if v.state == 5:
                            self._log.warn(codes.Code(30202, str(v.id)))
                        else:
                            self._log.error(codes.Code(40202, str(v.id)))
                        del self.workers[v.id]

    def check_catalog_workers(self) -> None:
        with self.lock:
            if len(self.catalog_workers) < storage.catalog_worker.count:
                self.catalog_workers[self.catalog_workers_increment_id] = CatalogWorker(
                    self.catalog_workers_increment_id
                )
                self._log.info(codes.Code(20205, f'CW-{self.catalog_workers_increment_id}'))
                self.catalog_workers_increment_id += 1
            elif len(self.catalog_workers) > storage.catalog_worker.count:
                try:
                    self.stop_catalog_worker()
                except StateError:
                    pass

            for v in list(self.catalog_workers.values()):
                if not v.is_alive():
                    try:
                        v.start()
                        self._log.info(codes.Code(20206, str(v.id)))
                    except RuntimeError:
                        if v.state == 5:
                            self._log.warn(codes.Code(30203, str(v.id)))
                        else:
                            self._log.error(codes.Code(40203, str(v.id)))
                        del self.catalog_workers[v.id]

    def stop_worker(self, id_: int = -1, blocking: bool = False) -> int:
        with self.lock:
            if id_ < 0:
                id_ = random.choice(list(self.workers))
            self.workers[id_].state = 5

            if blocking:
                self.workers[id_].join(storage.worker.wait)

            return id_

    def stop_catalog_worker(self, id_: int = -1, blocking: bool = False) -> int:
        with self.lock:
            if id_ < 0:
                id_ = random.choice(list(self.catalog_workers))
            self.catalog_workers[id_].state = 5

            if blocking:
                self.catalog_workers[id_].join(storage.catalog_worker.wait)

            return id_

    def stop_threads(self) -> None:
        with self.lock:
            for i in self.workers.values():
                try:
                    i.state = 5
                except StateError:
                    continue
            for i in tuple(self.workers):
                self.workers[i].join(storage.worker.wait)
                del self.workers[i]

            for i in self.catalog_workers.values():
                try:
                    i.state = 5
                except StateError:
                    continue
            for i in tuple(self.catalog_workers):
                self.catalog_workers[i].join(storage.catalog_worker.wait)
                del self.catalog_workers[i]

            try:
                self.pipe.state = 5
            except StateError:
                pass
            self.pipe.join(storage.pipe.wait)
            self.pipe = None

    def run(self) -> None:
        self.state = 1
        while True:
            try:
                start: float = time.time()
                if self.state == 1:
                    if self.lock.acquire(False):
                        self.check_pipe()
                        self.check_workers()
                        self.check_catalog_workers()
                        try:
                            self._lock_ticks = 0
                            self.lock.release()
                        except RuntimeError:
                            pass
                    else:
                        if self._lock_ticks == storage.thread_manager.lock_ticks:
                            self._log.warn(codes.Code(30204))
                            try:
                                self.lock.release()
                            except RuntimeError:
                                pass
                        else:
                            self._lock_ticks += 1
                elif self.state == 2:  # Pausing state
                    self._log.info(codes.Code(20002))
                    self.state = 3
                elif self.state == 3:  # Paused state
                    pass
                elif self.state == 4:  # Resuming state
                    self._log.info(codes.Code(20003))
                    self.state = 1
                elif self.state == 5:  # Stopping state
                    self._log.info(codes.Code(20004))
                    self.stop_threads()
                    self._log.info(codes.Code(20005))
                    break
                delta: float = time.time() - start
                time.sleep(storage.thread_manager.tick - delta if
                           storage.thread_manager.tick - delta > 0 else 0)
            except Exception as e:
                self._log.fatal_msg(codes.Code(50201, f'{e.__class__.__name__}: {str(e)}'), traceback.format_exc())
                self.stop_threads()
                self.throw(codes.Code(50201, f'While working: {e.__class__.__name__}: {str(e)}'))
                break

    def close(self) -> float:
        self.state = 5
        return storage.pipe.wait + len(self.workers) * (
                storage.worker.wait + 1) + len(self.catalog_workers) * (storage.catalog_worker.wait + 1)


class Core:
    state: int
    log: logger.Logger
    thread_manager: ThreadManager

    def __init__(self):
        self.state = 0
        self.log = logger.Logger('M')
        self.thread_manager = ThreadManager()

    def start(self):
        self.state = 1

        # Staring
        HashStorage.load()  # Load success hashes from cache
        storage.config_load()  # Load ./config.yaml
        script_manager.index.config_load()  # Load ./scripts/config.yaml
        script_manager.event_handler.start()  # Start event loop

        if storage.main.production:  # Notify about production mode
            self.log.info(codes.Code(20101))

        script_manager.index.reindex()  # Index scripts
        script_manager.load_all()  # Load scripts

        script_manager.event_handler.monitor_starting()

        commands.Commands()  # Initialize UCTP commands
        server.run()  # Run UCTP server

        analytic.dump(0)  # Create startup report

        self.thread_manager.start()  # Start pipeline
        script_manager.event_handler.monitor_started()
        # Starting end

        try:  # Waiting loop
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
        finally:  # Stopping
            self.log.info(codes.Code(20103))

            server.stop()  # Stop UCTP server
            storage.config_dump()

            script_manager.event_handler.monitor_stopping()

            self.thread_manager.join(self.thread_manager.close())  # Stop pipeline and wait

            provider.proxy_dump()  # Save proxies to ./proxy.json
            analytic.dump(2)  # Create stop report

            script_manager.event_handler.monitor_stopped()

            script_manager.event_handler.stop()  # Stop event loop
            script_manager.unload_all()  # Unload scripts
            script_manager.del_()  # Delete all data about scripts (index, parsers, etc.)

            self.log.info(codes.Code(20104))
            HashStorage.unload()  # Dump success hashes
            self.log.info(codes.Code(20105))

            self.log.info(codes.Code(20106))


if __name__ == 'source.core':
    script_manager: scripts.ScriptManager = scripts.ScriptManager()

    analytic: analytics.Analytics = analytics.Analytics()
    resolver: Resolver = Resolver()
    provider: Provider = Provider()
    server = uctp.peer.Peer(
        'monitor',
        RSA.import_key(open('./monitor.pem').read()),
        '0.0.0.0',
        trusted=uctp.peer.Trusted(*yaml.safe_load(open('authority.yaml'))['trusted']),
        aliases=uctp.peer.Aliases(yaml.safe_load(open('authority.yaml'))['aliases']),
        auth_timeout=4,
        buffer=8192
    )

    monitor: Core = Core()
