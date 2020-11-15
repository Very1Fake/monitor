from queue import Empty, PriorityQueue
from threading import current_thread, RLock
from time import time
from traceback import format_exc
from typing import List, Optional, Tuple, Union

from src.models.api.catalog import Catalog, CatalogType, CInterval, CScheduled, CSmart
from src.models.api.item import Item, IAnnounce, IRelease, IRestock
from src.models.api.message import Message
from src.models.api.target import Target, RestockTarget, TargetEnd, TargetType, RestockTargetType, TInterval, \
    TScheduled, TSmart, RTInterval, RTScheduled, RTSmart
from src.models.cache import HashStorage, UniquenessError
from src.utils import store
from src.utils.log import Logger
from src.utils.protocol import Code
from src.utils.schedule import UniqueSchedule
from .scripter import ScriptManager, ScriptNotFoundError, ParserImplementationError


class ResolverError(Exception):
    pass


class Resolver:
    log: Logger
    catalog_lock: RLock
    target_lock: RLock

    catalog_queue: PriorityQueue
    catalogs: UniqueSchedule
    target_queue: PriorityQueue
    targets: UniqueSchedule

    script_manager: ScriptManager

    def __init__(self, script_manager: ScriptManager):
        self.log = Logger('R')
        self.catalog_lock = RLock()
        self.target_lock = RLock()

        self.catalog_queue = PriorityQueue(store.queues.catalog_queue_size)
        self.catalogs = UniqueSchedule()
        self.target_queue = PriorityQueue(store.queues.target_queue_size)
        self.targets = UniqueSchedule()

        self.script_manager = script_manager

    @staticmethod
    def catalog_priority(catalog: CatalogType) -> int:
        if isinstance(catalog, CSmart):
            return store.priority.CSmart
        elif isinstance(catalog, CScheduled):
            return store.priority.CScheduled
        elif isinstance(catalog, CInterval):
            return store.priority.CInterval
        else:
            return store.priority.catalog_default

    @staticmethod
    def target_priority(target: Union[TargetType, RestockTargetType]) -> int:
        if isinstance(target, TSmart):
            return store.priority.TSmart[0] + target.reuse(store.priority.TSmart[1])
        elif isinstance(target, TScheduled):
            return store.priority.TScheduled[0] + target.reuse(store.priority.TScheduled[1])
        elif isinstance(target, TInterval):
            return store.priority.TInterval[0] + target.reuse(store.priority.TInterval[1])
        elif isinstance(target, RTSmart):
            return store.priority.RTSmart[0] + target.reuse(store.priority.RTSmart[1])
        elif isinstance(target, RTScheduled):
            return store.priority.RTScheduled[0] + target.reuse(store.priority.RTScheduled[1])
        elif isinstance(target, RTInterval):
            return store.priority.RTInterval[0] + target.reuse(store.priority.RTInterval[1])
        else:
            return store.priority.target_default

    def insert_catalog(self, catalog: CatalogType, force: bool = False) -> None:
        with self.catalog_lock:
            try:
                if issubclass(type(catalog), Catalog):
                    if force:
                        self.catalogs[time()] = catalog
                    else:
                        if isinstance(catalog, CSmart):
                            if catalog.expired:
                                self.log.warn(Code(30911, str(catalog)), current_thread().name)
                            else:
                                if (time_ := catalog.gen.extract()) == catalog.gen.time:
                                    catalog.expired = True

                                for i in range(100):  # TODO: Optimize
                                    if time_ in self.catalogs:
                                        time_ += .0001
                                    else:
                                        self.catalogs[time_] = catalog
                                        break
                                else:
                                    self.log.error(
                                        f'Smart catalog lost (calibration not passed): {catalog}',
                                        current_thread().name
                                    )
                        elif isinstance(catalog, CScheduled):
                            self.catalogs[catalog.timestamp] = catalog
                        elif isinstance(catalog, CInterval):
                            self.catalogs[time() + catalog.interval] = catalog
                else:
                    if store.main.production:
                        self.log.error(Code(40901, catalog), current_thread().name)
                    else:
                        self.log.fatal(ResolverError(Code(40901, catalog)),
                                       parent=current_thread().name)
            except IndexError:
                self.log.test(f'Inserting non-unique catalog', current_thread().name)

    def insert_target(self, target: TargetType) -> None:
        with self.target_lock:
            if HashStorage.check_target(target.hash()):
                try:
                    if isinstance(target, TSmart):
                        if target.expired:
                            self.log.warn(Code(30912, str(target)), current_thread().name)
                        else:
                            if (time_ := target.gen.extract()) == target.gen.time:
                                target.expired = True

                            for i in range(100):  # TODO: Optimize
                                if time_ in self.targets:
                                    time_ += .0001
                                else:
                                    self.targets[time_] = target
                                    break
                            else:
                                self.log.error(f'Smart target lost (calibration not passed): {target}')
                    elif isinstance(target, TScheduled):
                        self.targets[target.timestamp] = target
                    elif isinstance(target, TInterval):
                        self.targets[time() + target.interval] = target
                    else:
                        self.log.error(Code(40902, target), current_thread().name)
                except IndexError:
                    self.log.test(f'Inserting non-unique target', current_thread().name)

    def remove_catalog(self, script: str):
        if not isinstance(script, str):
            raise TypeError('script must be str')

        with self.catalog_lock, self.catalog_queue.mutex:
            schedule_id = -1
            queue_id = -1

            for k, v in self.catalogs.items():
                if v.script == script:
                    schedule_id = k

            for i, v in enumerate(self.catalog_queue.queue):
                if v.content.script == script:
                    queue_id = i

            self.catalogs.pop(schedule_id) if schedule_id > -1 else ...
            self.catalog_queue.queue.pop(queue_id) if queue_id > -1 else ...

    def remove_targets(self, script: str):  # TODO: Check from core
        if not isinstance(script, str):
            raise TypeError('script must be str')

        with self.target_lock, self.target_queue.mutex:
            schedule_ids = []
            queue_ids = []

            for k, v in self.targets.items():
                if v.script == script:
                    schedule_ids.append(k)

            for i, v in enumerate(self.target_queue.queue):
                if v.content.script == script:
                    queue_ids.append(i)

            for i in schedule_ids:
                self.targets.pop(i)

            for i in queue_ids:
                self.target_queue.queue.pop(i)

    def get_catalogs(self) -> List[CatalogType]:
        with self.catalog_lock:
            time_ = time()
            catalogs = []
            if any(self.catalogs[:time_]):
                for k, v in self.catalogs[:time_]:
                    if v.script in self.script_manager.scripts and v.script in self.script_manager.parsers:
                        catalogs.append(v)
                    elif v.script not in self.script_manager.scripts:
                        self.log.warn(Code(30901, v), current_thread().name)
                    elif v.script not in self.script_manager.parsers:
                        self.log.warn(Code(30902, v), current_thread().name)
                del self.catalogs[:time_]
            return catalogs

    def get_targets(self) -> List[Union[TargetType, RestockTargetType]]:
        with self.target_lock:
            time_ = time()
            targets = []
            if any(self.targets[:time_]):
                for k, v in self.targets[:time_]:
                    if v.script in self.script_manager.scripts and v.script in self.script_manager.parsers:
                        targets.append(v)
                    elif v.script not in self.script_manager.scripts:
                        self.log.warn(Code(30903, v), current_thread().name)
                    elif v.script not in self.script_manager.parsers:
                        self.log.warn(Code(30904, v), current_thread().name)
                del self.targets[:time_]
            return targets

    def execute(self, mode: int = 0) -> Tuple[int, str]:
        try:
            if mode == 0:
                task: CatalogType = self.catalog_queue.get_nowait().content
            elif mode == 1:
                task: TargetType = self.target_queue.get_nowait().content
            else:
                raise ValueError(f'Unknown mode ({mode})')
        except Empty:
            return 0, ''

        if mode == 0:
            self.log.debug(Code(10901, task), current_thread().name)
        elif mode == 1:
            self.log.debug(Code(10902, task), current_thread().name)

        try:
            result: list = self.script_manager.execute_parser(task.script, 'execute', (mode, task))

            if not isinstance(result, list):
                if mode == 0:
                    self.log.error(Code(30907, task), current_thread().name)
                elif mode == 1:
                    self.log.error(Code(30910, task), current_thread().name)
                self.script_manager.parser_error(task.script)
                return 1, task.script
        except ScriptNotFoundError:
            if mode == 0:
                self.log.warn(Code(30905, task), current_thread().name)
            elif mode == 1:
                self.log.warn(Code(30908, task), current_thread().name)
            return 2, task.script
        except ParserImplementationError:
            if mode == 0:
                self.log.warn(Code(30906, task), current_thread().name)
            elif mode == 1:
                self.log.warn(Code(30909, task), current_thread().name)
            return 3, task.script
        except Exception as e:
            if mode == 0:
                code = 40903
            elif mode == 1:
                code = 40904
            else:
                code = 40000

            self.log.fatal_msg(
                Code(code, f'{task.script}: {e.__class__.__name__}: {e!s}'),
                format_exc(),
                current_thread().name
            )
            self.script_manager.event_handler.alert(
                Code(code, f'{task.script}: {e.__class__.__name__}: {e!s}'), current_thread().name)
            return 4, task.script

        catalog: Optional[CatalogType] = None
        targets: List[TargetType] = []

        for i in result:
            if issubclass(type(i), Item):
                if isinstance(i, IAnnounce):
                    if HashStorage.check_item(i.hash(3), True):
                        HashStorage.add_announced_item(i.hash(3))
                        self.script_manager.event_handler.item(i)
                elif isinstance(i, IRelease):
                    if HashStorage.check_item(i.hash(4)):
                        id_ = HashStorage.add_item(i)
                        self.script_manager.event_handler.item(i)

                        if i.restock:
                            i.restock.id = id_
                            targets.append(i.restock)
                elif isinstance(i, IRestock):
                    if HashStorage.check_item_id(i.id):
                        HashStorage.add_item(i, True)
                        self.script_manager.event_handler.item(i)
            elif issubclass(type(i), Catalog):
                if not catalog:
                    catalog = i
            elif issubclass(type(i), (Target, RestockTarget)):
                targets.append(i)
            elif issubclass(type(i), TargetEnd):
                try:
                    HashStorage.add_target(i.target.hash())
                except UniquenessError:
                    pass
                else:
                    self.script_manager.event_handler.target_end(i)
            elif issubclass(type(i), Message):
                self.script_manager.event_handler.message(i)

        if catalog:
            self.remove_catalog(catalog.script)
            self.insert_catalog(catalog)

        for i in targets:
            self.insert_target(i)

        if mode == 0:
            self.log.debug(Code(10903), current_thread().name)
        elif mode == 1:
            self.log.debug(Code(10904), current_thread().name)

        return 5, task.script
