from time import sleep, time

from src.utils import store
from src.utils.protocol import Code
from src.utils.thread import ThreadClass
from .resolver import Resolver
from .scripter import EventHandler


# Exceptions


class WorkerError(Exception):
    pass


class CatalogWorkerError(Exception):
    pass


# Classes


class Worker(ThreadClass):
    id: int
    start_time: float
    speed: float
    idle: bool
    last_tick: float

    resolver: Resolver

    def __init__(self, id_: int, event_handler: EventHandler, resolver: Resolver):
        super().__init__(f'W-{id_}', WorkerError, event_handler)
        self.id = id_
        self.speed = .0
        self.idle = True
        self.start_time = time()
        self.last_tick = 0

        self.resolver = resolver

    def run(self) -> None:
        self.state = 1
        while True:
            start = self.last_tick = time()
            if self.state == 1:
                try:
                    if self.resolver.execute(1)[0] > 1:
                        self.idle = False
                    else:
                        self.idle = True
                except Exception as e:
                    self.throw(Code(50401, f'While working: {e.__class__.__name__}: {e!s}'))
                    break
            elif self.state == 2:  # Pausing state
                self._log.info(Code(20002))
                self._state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self._log.info(Code(20003))
                self._state = 1
            elif self.state == 5:  # Stopping state
                self._log.info(Code(20005))
                break
            delta: float = time() - start
            self.speed = 0 if self.idle else round(1 / delta, 3)
            sleep(store.worker.tick - delta if store.worker.tick - delta > 0 else 0)


class CatalogWorker(ThreadClass):
    id: int
    start_time: float
    speed: float
    idle: bool
    last_tick: float

    resolver: Resolver

    def __init__(self, id_: int, event_handler: EventHandler, resolver: Resolver):
        super().__init__(f'CW-{id_}', CatalogWorkerError, event_handler)
        self.id = id_
        self.speed = .0
        self.idle = True
        self.start_time = time()
        self.last_tick = 0

        self.resolver = resolver

    def run(self):
        self._state = 1
        while True:
            start = self.last_tick = time()
            if self.state == 1:
                try:
                    if self.resolver.execute()[0] < 1:
                        if self.resolver.execute(1)[0] > 1:  # TODO: Switcher for assistance
                            self.idle = False
                        else:
                            self.idle = True
                    else:
                        self.idle = False
                except Exception as e:
                    self.throw(Code(51001, f'While working: {e.__class__.__name__}: {e!s}'))
                    break
            elif self.state == 2:  # Pausing state
                self._log.info(Code(20002))
                self._state = 3
            elif self.state == 3:  # Paused state
                pass
            elif self.state == 4:  # Resuming state
                self._log.info(Code(20003))
                self._state = 1
            elif self.state == 5:  # Stopping state
                self._log.info(Code(20005))
                break
            delta: float = time() - start
            self.speed = 0 if self.idle else round(1 / delta, 3)
            sleep(store.catalog_worker.tick - delta if store.catalog_worker.tick - delta > 0 else 0)
