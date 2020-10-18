import inspect
from typing import Any

from uctp.peer import Peer

from .codes import Code
from . import core
from . import logger
from . import storage
from .cache import HashStorage
from .library import Keywords


class Commands:
    logger: logger.Logger

    def __init__(self):
        self.log = logger.Logger('C')

        core.server.commands.add_(self.analytics_dump)
        core.server.commands.add_(self.analytics_snapshot)
        core.server.commands.add_(self.analytics_proxy)
        core.server.commands.add_(self.analytics_proxies)
        core.server.commands.add_(self.analytics_worker)
        core.server.commands.add_(self.analytics_index_worker)
        core.server.commands.add_(self.config)
        core.server.commands.add_(self.config_categories)
        core.server.commands.add_(self.config_dump)
        core.server.commands.add_(self.config_load)
        core.server.commands.add_(self.config_get)
        core.server.commands.add_(self.config_set)
        core.server.commands.add_(self.hash_storage_defrag)
        core.server.commands.add_(self.hash_storage_dump)
        core.server.commands.add_(self.hash_storage_backup)
        core.server.commands.add_(self.hash_storage_stats)
        core.server.commands.add_(self.index_worker_stop)
        core.server.commands.add_(self.index_worker_list)
        core.server.commands.add_(self.index_worker_pause)
        core.server.commands.add_(self.index_worker_resume)
        core.server.commands.add_(self.keywords)
        core.server.commands.add_(self.keywords_dump)
        core.server.commands.add_(self.keywords_sync)
        core.server.commands.add_(self.keywords_clear)
        core.server.commands.add_(self.keywords_load)
        core.server.commands.add_(self.keywords_add)
        core.server.commands.add_(self.keywords_remove)
        core.server.commands.add_(self.log_file_reset)
        core.server.commands.add_(self.proxy)
        core.server.commands.add_(self.proxies)
        core.server.commands.add_(self.proxy_dump)
        core.server.commands.add_(self.proxy_load)
        core.server.commands.add_(self.proxy_add)
        core.server.commands.add_(self.proxy_remove)
        core.server.commands.add_(self.proxy_reset)
        core.server.commands.add_(self.proxy_clear)
        core.server.commands.add_(self.script)
        core.server.commands.add_(self.scripts)
        core.server.commands.add_(self.script_load)
        core.server.commands.add_(self.script_unload)
        core.server.commands.add_(self.script_reload)
        core.server.commands.add_(self.scripts_load_all)
        core.server.commands.add_(self.scripts_unload_all)
        core.server.commands.add_(self.scripts_reload_all)
        core.server.commands.add_(self.scripts_config)
        core.server.commands.add_(self.scripts_config_dump)
        core.server.commands.add_(self.scripts_config_load)
        core.server.commands.add_(self.scripts_index)
        core.server.commands.add_(self.scripts_reindex)
        core.server.commands.add_(self.scripts_blacklist_add)
        core.server.commands.add_(self.scripts_blacklist_remove)
        core.server.commands.add_(self.scripts_whitelist_add)
        core.server.commands.add_(self.scripts_whitelist_remove)
        core.server.commands.add_(self.pipe_pause)
        core.server.commands.add_(self.pipe_resume)
        core.server.commands.add_(self.worker_stop)
        core.server.commands.add_(self.worker_list)
        core.server.commands.add_(self.worker_pause)
        core.server.commands.add_(self.worker_resume)
        core.server.commands.add_(self.stop)

        core.server.commands.alias('a-dump', 'analytics_dump')
        core.server.commands.alias('a-snapshot', 'analytics_snapshot')
        core.server.commands.alias('a-proxy', 'analytics_proxy')
        core.server.commands.alias('a-proxies', 'analytics_proxies')
        core.server.commands.alias('a-worker', 'analytics_worker')
        core.server.commands.alias('a-i-worker', 'analytics_index_worker')
        core.server.commands.alias('c-cat', 'config_categories')
        core.server.commands.alias('c-dump', 'config_dump')
        core.server.commands.alias('c-load', 'config_load')
        core.server.commands.alias('c-get', 'config_get')
        core.server.commands.alias('c-set', 'config_set')
        core.server.commands.alias('hs-defrag', 'hash_storage_defrag')
        core.server.commands.alias('hs-dump', 'hash_storage_dump')
        core.server.commands.alias('hs-backup', 'hash_storage_backup')
        core.server.commands.alias('hs-stats', 'hash_storage_stats')
        core.server.commands.alias('iw-stop', 'index_worker_stop')
        core.server.commands.alias('iw-list', 'index_worker_list')
        core.server.commands.alias('iw-pause', 'index_worker_pause')
        core.server.commands.alias('iw-resume', 'index_worker_resume')
        core.server.commands.alias('kw-dump', 'keywords_dump')
        core.server.commands.alias('kw-sync', 'keywords_sync')
        core.server.commands.alias('kw-clear', 'keywords_clear')
        core.server.commands.alias('kw-load', 'keywords_load')
        core.server.commands.alias('kw-add', 'keywords_add')
        core.server.commands.alias('kw-remove', 'keywords_remove')
        core.server.commands.alias('l-file-reset', 'log_file_reset')
        core.server.commands.alias('p-dump', 'proxy_dump')
        core.server.commands.alias('p-load', 'proxy_load')
        core.server.commands.alias('p-add', 'proxy_add')
        core.server.commands.alias('p-remove', 'proxy_remove')
        core.server.commands.alias('p-reset', 'proxy_reset')
        core.server.commands.alias('p-clear', 'proxy_clear')
        core.server.commands.alias('s-load', 'script_load')
        core.server.commands.alias('s-unload', 'script_unload')
        core.server.commands.alias('s-reload', 'script_reload')
        core.server.commands.alias('s-load-all', 'scripts_load_all')
        core.server.commands.alias('s-unload-all', 'scripts_unload_all')
        core.server.commands.alias('s-reload-all', 'scripts_reload_all')
        core.server.commands.alias('s-config', 'scripts_config')
        core.server.commands.alias('sc-dump', 'scripts_config_dump')
        core.server.commands.alias('sc-load', 'scripts_config_load')
        core.server.commands.alias('s-index', 'scripts_index')
        core.server.commands.alias('s-reindex', 'scripts_reindex')
        core.server.commands.alias('sb-add', 'scripts_blacklist_add')
        core.server.commands.alias('sb-remove', 'scripts_blacklist_remove')
        core.server.commands.alias('sw-add', 'scripts_whitelist_add')
        core.server.commands.alias('sw-remove', 'scripts_whitelist_remove')
        core.server.commands.alias('p-pause', 'pipe_pause')
        core.server.commands.alias('p-resume', 'pipe_resume')
        core.server.commands.alias('w-stop', 'worker_stop')
        core.server.commands.alias('w-list', 'worker_list')
        core.server.commands.alias('w-pause', 'worker_pause')
        core.server.commands.alias('w-resume', 'worker_resume')

    def analytics_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.analytic.dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def analytics_snapshot(self, peer: Peer, type_: int = 1) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.analytic.snapshot(type_)

    def analytics_proxy(self, peer: Peer, proxy: str):
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.analytic.proxy(proxy)

    def analytics_proxies(self, peer: Peer):
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.analytic.proxies()

    def analytics_worker(self, peer: Peer, id_: int) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.analytic.info_worker(id_)

    def analytics_index_worker(self, peer: Peer, id_: int) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.analytic.info_index_worker(id_)

    def config(self, peer: Peer) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return storage.snapshot()

    def config_categories(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(storage.categories)

    def config_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        storage.config_dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def config_load(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        storage.config_load()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def config_get(self, peer: Peer, namespace: str, key: str) -> Any:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        if not isinstance(namespace, str):
            raise TypeError('namespace must be str')
        elif not isinstance(key, str):
            raise TypeError('key must be str')

        if namespace in storage.categories:
            if hasattr(getattr(storage, namespace), key):
                self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
                return getattr(getattr(storage, namespace), key)
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

        if namespace in storage.categories:
            if hasattr(getattr(storage, namespace), key):
                try:
                    setattr(storage, namespace, getattr(storage, namespace)._replace(
                        **{key: type(getattr(getattr(storage, namespace), key))(value)}))
                    self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
                    return True
                except ValueError:
                    raise TypeError(f'value type must be "{type(getattr(getattr(storage, namespace), key))}"')
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

    def index_worker_stop(self, peer: Peer, id_: int = -1) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.monitor.thread_manager.stop_catalog_worker(id_)

    def index_worker_list(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(core.monitor.thread_manager.catalog_workers)

    def index_worker_pause(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.catalog_workers[id_].state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def index_worker_resume(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.catalog_workers[id_].state = 4
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
        with logger.write_lock:
            logger.file = None
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy(self, peer: Peer, url: str) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.provider.lock:
            return core.provider.proxies[url].export()

    def proxies(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.provider.lock:
            return [i.address for i in core.provider.proxies.values()]

    def proxy_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.provider.proxy_dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy_load(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(core.provider.proxy_load())

    def proxy_add(self, peer: Peer, address: str, login: str = None, password: str = None) -> bool:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.provider.proxy_add(address, login, password)

    def proxy_remove(self, peer: Peer, address: str) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.provider.proxy_remove(address)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy_reset(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.provider.proxy_reset()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def proxy_clear(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.provider.proxy_clear()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def script(self, peer: Peer, name: str) -> dict:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        if name in core.script_manager.scripts:
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return {k: v for k, v in core.script_manager.scripts[name].items() if not k.startswith('__')}
        else:
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return {}

    def scripts(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(core.script_manager.scripts)

    def script_load(self, peer: Peer, name: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.load(name)[1]

    def script_unload(self, peer: Peer, name: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.unload(name)[1]

    def script_reload(self, peer: Peer, name: str) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.reload(name)[1]

    def scripts_load_all(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.load_all()[1]

    def scripts_unload_all(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.unload_all()[1]

    def scripts_reload_all(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.reload_all()[1]

    def scripts_config(self, peer: Peer) -> dict:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.index.config

    def scripts_config_dump(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.script_manager.index.config_dump()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_config_load(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.script_manager.index.config_load()
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_index(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.index.index

    def scripts_reindex(self, peer: Peer) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.script_manager.index.reindex()

    def scripts_blacklist_add(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.script_manager.index.blacklist_add(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_blacklist_remove(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.script_manager.index.blacklist_remove(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_whitelist_add(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.script_manager.index.whitelist_add(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def scripts_whitelist_remove(self, peer: Peer, name: str, folder: bool = False) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        core.script_manager.index.whitelist_remove(name, folder)
        self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
        return True

    def pipe_pause(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.pipe.state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def pipe_resume(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.pipe.state = 4
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def worker_stop(self, peer: Peer, id_: int = -1) -> int:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return core.monitor.thread_manager.stop_worker(id_)

    def worker_list(self, peer: Peer) -> list:
        self.log.info(Code(21103, f'{peer.name}: {inspect.stack()[0][3]}'))
        return list(core.monitor.thread_manager.workers)

    def worker_pause(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.workers[id_].state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def worker_resume(self, peer: Peer, id_: int) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.workers[id_].state = 4
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True

    def stop(self, peer: Peer) -> bool:
        self.log.info(Code(21101, f'{peer.name}: {inspect.stack()[0][3]}'))
        if core.monitor.state == 1:
            core.monitor.state = 2
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return True
        else:
            self.log.info(Code(21102, f'{peer.name}: {inspect.stack()[0][3]}'))
            return False
