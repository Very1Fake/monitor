import inspect
from typing import Any

from uctp.peer import Peer

from src.containers.scripter import ScriptManager
from src.containers.thread_manager import ThreadManager
from src.models.cache import HashStorage
from src.models.provider import Provider
from src.utils import log
from src.utils import store
from src.models.keywords import Keywords
from src.utils.log import Logger
from src.utils.protocol import Code
from .analytics import Analytics


class Commands:
    __slots__ = ['log', 'analytics', 'kernel', 'provider', 'script_manager', 'thread_manager']
    log: Logger

    analytics: Analytics
    provider: Provider
    script_manager: ScriptManager
    thread_manager: ThreadManager

    def __init__(
            self,
            analytics: Analytics,
            kernel,
            provider: Provider,
            script_manager: ScriptManager,
            thread_manager: ThreadManager
    ):
        self.log = Logger('C')

        self.analytics = analytics
        self.kernel = kernel
        self.provider = provider
        self.script_manager = script_manager
        self.thread_manager = thread_manager

    def bind(self, server: Peer):
        server.commands.add_(self.analytics_dump)
        server.commands.add_(self.analytics_snapshot)
        server.commands.add_(self.analytics_proxy)
        server.commands.add_(self.analytics_proxies)
        server.commands.add_(self.analytics_worker)
        server.commands.add_(self.analytics_catalog_worker)
        server.commands.add_(self.config)
        server.commands.add_(self.config_categories)
        server.commands.add_(self.config_dump)
        server.commands.add_(self.config_load)
        server.commands.add_(self.config_get)
        server.commands.add_(self.config_set)
        server.commands.add_(self.hash_storage_defrag)
        server.commands.add_(self.hash_storage_dump)
        server.commands.add_(self.hash_storage_backup)
        server.commands.add_(self.hash_storage_stats)
        server.commands.add_(self.catalog_worker_stop)
        server.commands.add_(self.catalog_worker_list)
        server.commands.add_(self.catalog_worker_pause)
        server.commands.add_(self.catalog_worker_resume)
        server.commands.add_(self.keywords)
        server.commands.add_(self.keywords_dump)
        server.commands.add_(self.keywords_sync)
        server.commands.add_(self.keywords_clear)
        server.commands.add_(self.keywords_load)
        server.commands.add_(self.keywords_add)
        server.commands.add_(self.keywords_remove)
        server.commands.add_(self.log_file_reset)
        server.commands.add_(self.proxy)
        server.commands.add_(self.proxies)
        server.commands.add_(self.proxy_dump)
        server.commands.add_(self.proxy_load)
        server.commands.add_(self.proxy_add)
        server.commands.add_(self.proxy_remove)
        server.commands.add_(self.proxy_reset)
        server.commands.add_(self.proxy_clear)
        server.commands.add_(self.script)
        server.commands.add_(self.scripts)
        server.commands.add_(self.script_load)
        server.commands.add_(self.script_unload)
        server.commands.add_(self.script_reload)
        server.commands.add_(self.scripts_load_all)
        server.commands.add_(self.scripts_unload_all)
        server.commands.add_(self.scripts_reload_all)
        server.commands.add_(self.scripts_config)
        server.commands.add_(self.scripts_config_dump)
        server.commands.add_(self.scripts_config_load)
        server.commands.add_(self.scripts_index)
        server.commands.add_(self.scripts_reindex)
        server.commands.add_(self.scripts_blacklist_add)
        server.commands.add_(self.scripts_blacklist_remove)
        server.commands.add_(self.scripts_whitelist_add)
        server.commands.add_(self.scripts_whitelist_remove)
        server.commands.add_(self.pipe_pause)
        server.commands.add_(self.pipe_resume)
        server.commands.add_(self.worker_stop)
        server.commands.add_(self.worker_list)
        server.commands.add_(self.worker_pause)
        server.commands.add_(self.worker_resume)
        server.commands.add_(self.stop)

        server.commands.alias('a-dump', 'analytics_dump')
        server.commands.alias('a-snapshot', 'analytics_snapshot')
        server.commands.alias('a-proxy', 'analytics_proxy')
        server.commands.alias('a-proxies', 'analytics_proxies')
        server.commands.alias('a-worker', 'analytics_worker')
        server.commands.alias('a-c-worker', 'analytics_catalog_worker')
        server.commands.alias('c-cat', 'config_categories')
        server.commands.alias('c-dump', 'config_dump')
        server.commands.alias('c-load', 'config_load')
        server.commands.alias('c-get', 'config_get')
        server.commands.alias('c-set', 'config_set')
        server.commands.alias('hs-defrag', 'hash_storage_defrag')
        server.commands.alias('hs-dump', 'hash_storage_dump')
        server.commands.alias('hs-backup', 'hash_storage_backup')
        server.commands.alias('hs-stats', 'hash_storage_stats')
        server.commands.alias('cw-stop', 'catalog_worker_stop')
        server.commands.alias('cw-list', 'catalog_worker_list')
        server.commands.alias('cw-pause', 'catalog_worker_pause')
        server.commands.alias('cw-resume', 'catalog_worker_resume')
        server.commands.alias('kw-dump', 'keywords_dump')
        server.commands.alias('kw-sync', 'keywords_sync')
        server.commands.alias('kw-clear', 'keywords_clear')
        server.commands.alias('kw-load', 'keywords_load')
        server.commands.alias('kw-add', 'keywords_add')
        server.commands.alias('kw-remove', 'keywords_remove')
        server.commands.alias('l-file-reset', 'log_file_reset')
        server.commands.alias('p-dump', 'proxy_dump')
        server.commands.alias('p-load', 'proxy_load')
        server.commands.alias('p-add', 'proxy_add')
        server.commands.alias('p-remove', 'proxy_remove')
        server.commands.alias('p-reset', 'proxy_reset')
        server.commands.alias('p-clear', 'proxy_clear')
        server.commands.alias('s-load', 'script_load')
        server.commands.alias('s-unload', 'script_unload')
        server.commands.alias('s-reload', 'script_reload')
        server.commands.alias('s-load-all', 'scripts_load_all')
        server.commands.alias('s-unload-all', 'scripts_unload_all')
        server.commands.alias('s-reload-all', 'scripts_reload_all')
        server.commands.alias('s-config', 'scripts_config')
        server.commands.alias('sc-dump', 'scripts_config_dump')
        server.commands.alias('sc-load', 'scripts_config_load')
        server.commands.alias('s-index', 'scripts_index')
        server.commands.alias('s-reindex', 'scripts_reindex')
        server.commands.alias('sb-add', 'scripts_blacklist_add')
        server.commands.alias('sb-remove', 'scripts_blacklist_remove')
        server.commands.alias('sw-add', 'scripts_whitelist_add')
        server.commands.alias('sw-remove', 'scripts_whitelist_remove')
        server.commands.alias('p-pause', 'pipe_pause')
        server.commands.alias('p-resume', 'pipe_resume')
        server.commands.alias('w-stop', 'worker_stop')
        server.commands.alias('w-list', 'worker_list')
        server.commands.alias('w-pause', 'worker_pause')
        server.commands.alias('w-resume', 'worker_resume')

    def analytics_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.analytics.dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def analytics_snapshot(self, peer: Peer, type_: int = 1) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.analytics.snapshot(type_)

    def analytics_proxy(self, peer: Peer, proxy: str):
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.analytics.proxy(proxy)

    def analytics_proxies(self, peer: Peer):
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.analytics.proxies()

    def analytics_worker(self, peer: Peer, id_: int) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.analytics.info_worker(id_)

    def analytics_catalog_worker(self, peer: Peer, id_: int) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.analytics.info_catalog_worker(id_)

    def config(self, peer: Peer) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return store.snapshot()

    def config_categories(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(store.categories)

    def config_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        store.config_dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def config_load(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        store.config_load()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def config_get(self, peer: Peer, namespace: str, key: str) -> Any:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        if not isinstance(namespace, str):
            raise TypeError('namespace must be str')
        elif not isinstance(key, str):
            raise TypeError('key must be str')

        if namespace in store.categories:
            if hasattr(getattr(store, namespace), key):
                self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
                return getattr(getattr(store, namespace), key)
            else:
                raise IndexError(f'"{key}" not found in "{namespace}"')
        else:
            raise IndexError(f'Namespace "{namespace}" not found')

    def config_set(self, peer: Peer, namespace: str, key: str, value: Any) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        if not isinstance(namespace, str):
            raise TypeError('namespace must be str')
        elif not isinstance(key, str):
            raise TypeError('key must be str')

        if namespace in store.categories:
            if hasattr(getattr(store, namespace), key):
                try:
                    setattr(store, namespace, getattr(store, namespace)._replace(
                        **{key: type(getattr(getattr(store, namespace), key))(value)}))
                    self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
                    return True
                except ValueError:
                    raise TypeError(f'value type must be "{type(getattr(getattr(store, namespace), key))}"')
            else:
                raise IndexError(f'"{key}" not found in "{namespace}"')
        else:
            raise IndexError(f'Namespace "{namespace}" not found')

    def hash_storage_defrag(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        HashStorage.defrag()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def hash_storage_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        HashStorage.dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def hash_storage_backup(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        HashStorage.backup()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def hash_storage_stats(self, peer: Peer) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return HashStorage.stats()

    def catalog_worker_stop(self, peer: Peer, id_: int = -1) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.thread_manager.stop_catalog_worker(id_)

    def catalog_worker_list(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(self.thread_manager.catalog_workers)

    def catalog_worker_pause(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.thread_manager.lock:
            self.thread_manager.catalog_workers[id_].state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def catalog_worker_resume(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.thread_manager.lock:
            self.thread_manager.catalog_workers[id_].state = 4
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def keywords(self, peer: Peer) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return Keywords.export()

    def keywords_dump(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return Keywords.dump()

    def keywords_sync(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return Keywords.sync()

    def keywords_clear(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return Keywords.clear()

    def keywords_load(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return Keywords.load()

    def keywords_add(self, peer: Peer, type_: str, kw: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        if type_ == 'abs':
            return Keywords.add_abs(kw)
        elif type_ == 'pos':
            return Keywords.add_pos(kw)
        elif type_ == 'neg':
            return Keywords.add_neg(kw)
        else:
            raise

    def keywords_remove(self, peer: Peer, type_: str, kw: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        if type_ == 'abs':
            return Keywords.remove_abs(kw)
        elif type_ == 'pos':
            return Keywords.remove_pos(kw)
        elif type_ == 'neg':
            return Keywords.remove_neg(kw)
        else:
            raise

    def log_file_reset(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        log.reset_file()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy(self, peer: Peer, url: str) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.provider.lock:
            return self.provider.proxies[url].export()

    def proxies(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.provider.lock:
            return [i.address for i in self.provider.proxies.values()]

    def proxy_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.provider.proxy_dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy_load(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(self.provider.proxy_load())

    def proxy_add(self, peer: Peer, address: str, login: str = None, password: str = None) -> bool:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.provider.proxy_add(address, login, password)

    def proxy_remove(self, peer: Peer, address: str) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.provider.proxy_remove(address)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy_reset(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.provider.proxy_reset()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy_clear(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.provider.proxy_clear()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def script(self, peer: Peer, name: str) -> dict:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        if name in self.script_manager.scripts:
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return {k: v for k, v in self.script_manager.scripts[name].items() if not k.startswith('__')}
        else:
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return {}

    def scripts(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(self.script_manager.scripts)

    def script_load(self, peer: Peer, name: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.load(name)[1]

    def script_unload(self, peer: Peer, name: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.unload(name)[1]

    def script_reload(self, peer: Peer, name: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.reload(name)[1]

    def scripts_load_all(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.load_all()[1]

    def scripts_unload_all(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.unload_all()[1]

    def scripts_reload_all(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.reload_all()[1]

    def scripts_config(self, peer: Peer) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.index.config

    def scripts_config_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.script_manager.index.config_dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_config_load(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.script_manager.index.config_load()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_index(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.index.index

    def scripts_reindex(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.script_manager.index.reindex()

    def scripts_blacklist_add(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.script_manager.index.blacklist_add(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_blacklist_remove(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.script_manager.index.blacklist_remove(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_whitelist_add(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.script_manager.index.whitelist_add(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_whitelist_remove(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        self.script_manager.index.whitelist_remove(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def pipe_pause(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.thread_manager.lock:
            self.thread_manager.pipe.state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def pipe_resume(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.thread_manager.lock:
            self.thread_manager.pipe.state = 4
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def worker_stop(self, peer: Peer, id_: int = -1) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return self.thread_manager.stop_worker(id_)

    def worker_list(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(self.thread_manager.workers)

    def worker_pause(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.thread_manager.lock:
            self.thread_manager.workers[id_].state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def worker_resume(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with self.thread_manager.lock:
            self.thread_manager.workers[id_].state = 4
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def stop(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        if self.kernel.state == 1:
            self.kernel.state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True
        else:
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return False
