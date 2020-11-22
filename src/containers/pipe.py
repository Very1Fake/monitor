from queue import Full
from time import sleep, time
from typing import Dict

from src.models.api.catalog import Catalog
from src.models.cache import HashStorage
from src.store import pipe, queue
from src.utils.protocol import Code
from src.utils.schedule import PrioritizedItem
from src.utils.thread import ThreadClass
from .resolver import Resolver
from .scripter import ScriptManager


class PipeError(Exception):
    pass


class Pipe(ThreadClass):
    hashes: Dict[str, str]

    resolver: Resolver
    script_manager: ScriptManager

    def __init__(self, resolver: Resolver, script_manager: ScriptManager):
        super().__init__('P', PipeError, script_manager.event_handler)
        self.hashes = {}
        self.resolver = resolver
        self.script_manager = script_manager

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
            start: float = time()
            if self.state == 1:  # Active state
                try:
                    HashStorage.cleanup()  # Cleanup expired hashes

                    if different := self._compare_parsers(
                            self.hashes, self.script_manager.hash()):  # Check for scripts (loaded/unloaded)
                        with self.script_manager.lock:
                            self._log.info(Code(20301))
                            for i in different:
                                self._log.debug(Code(10301, i))
                                try:
                                    if issubclass(type(catalog := self.script_manager.parsers[i].catalog), Catalog):
                                        self.resolver.insert_catalog(catalog, True)
                                        self._log.info(Code(20303, i))
                                    else:
                                        self._log.error(Code(40301, i))
                                except Exception as e:
                                    self._log.warn(Code(30301, f'{i}: {e.__class__.__name__}: {e!s}'))
                            self.hashes = self.script_manager.hash()
                            self._log.info(Code(20302))
                    elif self.hashes != self.script_manager.hash():
                        self.hashes = self.script_manager.hash()

                    for i in self.resolver.get_catalogs():  # Send catalogs
                        try:
                            self.resolver.catalog_queue.put(
                                PrioritizedItem(self.resolver.catalog_priority(i), i),
                                timeout=queue.catalog_queue_size
                            )
                        except Full:  # TODO: Fix (catalog can't be lost)
                            self._log.warn(Code(30302, i))

                    for i in self.resolver.get_targets():  # Send targets
                        try:
                            self.resolver.target_queue.put(
                                PrioritizedItem(self.resolver.target_priority(i), i),
                                timeout=queue.target_queue_put_wait
                            )
                        except Full:
                            self._log.warn(Code(30303, i))

                except Exception as e:
                    self.throw(Code(50301, f'While working: {e.__class__.__name__}: {e!s}'))
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
            sleep(pipe.tick - delta if pipe.tick - delta > 0 else 0)
