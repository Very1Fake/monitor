from random import choice
from threading import RLock
from time import sleep, time
from typing import Dict, Optional

from src.utils import store
from src.utils.protocol import Code
from src.utils.thread import ThreadClass, StateError
from .pipe import Pipe
from .resolver import Resolver
from .scripter import ScriptManager
from .worker import CatalogWorker, Worker


class ThreadManagerError(Exception):
    pass


class ThreadManager(ThreadClass):
    _lock_ticks: int

    lock: RLock
    catalog_workers_id: int
    catalog_workers: Dict[int, CatalogWorker]
    workers_id: int
    workers: Dict[int, Worker]
    pipe: Optional[Pipe]

    script_manager: ScriptManager
    resolver: Resolver

    def __init__(self, resolver: Resolver, script_manager: ScriptManager) -> None:
        super().__init__('TM', ThreadManagerError, script_manager.event_handler)
        self._lock_ticks = 0

        self.lock = RLock()
        self.catalog_workers_id = 0
        self.catalog_workers = {}
        self.workers_id = 0
        self.workers = {}
        self.pipe = None

        self.script_manager = script_manager
        self.resolver = resolver

    def check_pipe(self) -> None:
        with self.lock:
            try:
                if not self.pipe.is_alive():
                    self.pipe.start()
                    self._log.info(Code(20202))
            except (AttributeError, RuntimeError) as e:
                if isinstance(e, RuntimeError):
                    if self.pipe.state == 5:
                        self._log.warn(Code(30201))
                    else:
                        self._log.error(Code(40201))
                self.pipe = Pipe(self.resolver, self.script_manager)
                self._log.info(Code(20201))

    def check_workers(self) -> None:
        with self.lock:
            if len(self.workers) < store.worker.count:
                self.workers[self.workers_id] = Worker(
                    self.workers_id,
                    self.script_manager.event_handler,
                    self.resolver
                )
                self._log.info(Code(20203, f'W-{self.workers_id}'))
                self.workers_id += 1
            elif len(self.workers) > store.worker.count:
                try:
                    self.stop_worker()
                except StateError:
                    pass

            for v in list(self.workers.values()):
                if not v.is_alive():
                    try:
                        v.start()
                        self._log.info(Code(20204, str(v.id)))
                    except RuntimeError:
                        if v.state == 5:
                            self._log.warn(Code(30202, str(v.id)))
                        else:
                            self._log.error(Code(40202, str(v.id)))
                        del self.workers[v.id]

    def check_catalog_workers(self) -> None:
        with self.lock:
            if len(self.catalog_workers) < store.catalog_worker.count:
                self.catalog_workers[self.catalog_workers_id] = CatalogWorker(
                    self.catalog_workers_id,
                    self.script_manager.event_handler,
                    self.resolver
                )
                self._log.info(Code(20205, f'CW-{self.catalog_workers_id}'))
                self.catalog_workers_id += 1
            elif len(self.catalog_workers) > store.catalog_worker.count:
                try:
                    self.stop_catalog_worker()
                except StateError:
                    pass

            for v in list(self.catalog_workers.values()):
                if not v.is_alive():
                    try:
                        v.start()
                        self._log.info(Code(20206, str(v.id)))
                    except RuntimeError:
                        if v.state == 5:
                            self._log.warn(Code(30203, str(v.id)))
                        else:
                            self._log.error(Code(40203, str(v.id)))
                        del self.catalog_workers[v.id]

    def stop_worker(self, id_: int = -1, blocking: bool = False) -> int:
        with self.lock:
            if id_ < 0:
                id_ = choice(list(self.workers))
            self.workers[id_].state = 5

            if blocking:
                self.workers[id_].join(store.worker.wait)

            return id_

    def stop_catalog_worker(self, id_: int = -1, blocking: bool = False) -> int:
        with self.lock:
            if id_ < 0:
                id_ = choice(list(self.catalog_workers))
            self.catalog_workers[id_].state = 5

            if blocking:
                self.catalog_workers[id_].join(store.catalog_worker.wait)

            return id_

    def stop_threads(self) -> None:
        with self.lock:
            for i in self.workers.values():
                try:
                    i.state = 5
                except StateError:
                    continue
            for i in tuple(self.workers):
                self.workers[i].join(store.worker.wait)
                del self.workers[i]

            for i in self.catalog_workers.values():
                try:
                    i.state = 5
                except StateError:
                    continue
            for i in tuple(self.catalog_workers):
                self.catalog_workers[i].join(store.catalog_worker.wait)
                del self.catalog_workers[i]

            if self.pipe:
                try:
                    self.pipe.state = 5
                except StateError:
                    pass
                self.pipe.join(store.pipe.wait)

    def run(self) -> None:
        self.state = 1
        while True:
            try:
                start: float = time()
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
                        if self._lock_ticks == store.thread_manager.lock_ticks:
                            self._log.warn(Code(30204))
                            try:
                                self.lock.release()
                            except RuntimeError:
                                pass
                        else:
                            self._lock_ticks += 1
                elif self.state == 2:  # Pausing state
                    self._log.info(Code(20002))
                    self.state = 3
                elif self.state == 3:  # Paused state
                    pass
                elif self.state == 4:  # Resuming state
                    self._log.info(Code(20003))
                    self.state = 1
                elif self.state == 5:  # Stopping state
                    self._log.info(Code(20004))
                    self.stop_threads()
                    self._log.info(Code(20005))
                    break
                delta: float = time() - start
                sleep(store.thread_manager.tick - delta if
                      store.thread_manager.tick - delta > 0 else 0)
            except Exception as e:
                try:
                    self.lock.release()
                except RuntimeError:
                    pass

                self.stop_threads()
                self.throw(Code(50201, f'While working: {e.__class__.__name__}: {e!s}'))
                break

    def close(self) -> float:
        self.state = 5
        return store.pipe.wait + len(self.workers) * (
                store.worker.wait + 1) + len(self.catalog_workers) * (store.catalog_worker.wait + 1)
