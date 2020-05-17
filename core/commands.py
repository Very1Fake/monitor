from typing import Any

from core import core
from . import logger
from . import storage


class Commands:
    logger: logger.Logger

    def __init__(self):
        self.log = logger.Logger('Commands')
        self.core = core
        core.server.commands.add_(self.analytics_dump)
        core.server.commands.add_(self.analytics_snapshot)
        core.server.commands.add_(self.analytics_worker)
        core.server.commands.add_(self.analytics_index_worker)
        core.server.commands.add_(self.config)
        core.server.commands.add_(self.config_categories)
        core.server.commands.add_(self.config_dump)
        core.server.commands.add_(self.config_load)
        core.server.commands.add_(self.config_set)
        core.server.commands.add_(self.script)
        core.server.commands.add_(self.script_load)
        core.server.commands.add_(self.script_unload)
        core.server.commands.add_(self.script_reload)
        core.server.commands.add_(self.scripts_load_all)
        core.server.commands.add_(self.scripts_unload_all)
        core.server.commands.add_(self.scripts_reload_all)
        core.server.commands.add_(self.scripts_reindex)
        core.server.commands.add_(self.pipe_pause)
        core.server.commands.add_(self.pipe_resume)
        core.server.commands.add_(self.worker_stop)
        core.server.commands.add_(self.worker_list)
        core.server.commands.add_(self.worker_pause)
        core.server.commands.add_(self.worker_resume)
        core.server.commands.add_(self.index_worker_stop)
        core.server.commands.add_(self.index_worker_list)
        core.server.commands.add_(self.index_worker_pause)
        core.server.commands.add_(self.index_worker_resume)
        core.server.commands.add_(self.stop)

        core.server.commands.alias('a-dump', 'analytics_dump')
        core.server.commands.alias('a-snapshot', 'analytics_snapshot')
        core.server.commands.alias('a-worker', 'analytics_worker')
        core.server.commands.alias('a-i-worker', 'analytics_index_worker')
        core.server.commands.alias('c-cat', 'config_categories')
        core.server.commands.alias('c-dump', 'config_dump')
        core.server.commands.alias('c-load', 'config_load')
        core.server.commands.alias('c-set', 'config_set')
        core.server.commands.alias('s-load', 'script_load')
        core.server.commands.alias('s-unload', 'script_unload')
        core.server.commands.alias('s-reload', 'script_reload')
        core.server.commands.alias('s-load-all', 'scripts_load_all')
        core.server.commands.alias('s-unload-all', 'scripts_unload_all')
        core.server.commands.alias('s-reload-all', 'scripts_reload_all')
        core.server.commands.alias('s-reindex', 'scripts_reindex')
        core.server.commands.alias('p-pause', 'pipe_pause')
        core.server.commands.alias('p-resume', 'pipe_resume')
        core.server.commands.alias('w-stop', 'worker_stop')
        core.server.commands.alias('w-list', 'worker_list')
        core.server.commands.alias('w-pause', 'worker_pause')
        core.server.commands.alias('w-resume', 'worker_resume')
        core.server.commands.alias('iw-stop', 'index_worker_stop')
        core.server.commands.alias('iw-list', 'index_worker_list')
        core.server.commands.alias('iw-pause', 'index_worker_pause')
        core.server.commands.alias('iw-resume', 'index_worker_resume')

    @staticmethod
    def analytics_dump() -> bool:
        core.analytic.dump()
        return True

    @staticmethod
    def analytics_snapshot(type_: int = 1) -> dict:
        return core.analytic.snapshot(type_)

    @staticmethod
    def analytics_worker(id_: int) -> dict:
        return core.analytic.info_worker(id_)

    @staticmethod
    def analytics_index_worker(id_: int) -> dict:
        return core.analytic.info_index_worker(id_)

    @staticmethod
    def config() -> dict:
        return storage.snapshot()

    @staticmethod
    def config_categories() -> list:
        return list(storage.categories)

    @staticmethod
    def config_dump() -> bool:
        storage.config_dump()
        return True

    @staticmethod
    def config_load() -> bool:
        storage.config_load()
        return True

    @staticmethod
    def config_set(namespace: str, key: str, value: Any) -> bool:
        if not isinstance(namespace, str):
            raise TypeError('namespace must be str')
        elif not isinstance(key, str):
            raise TypeError('key must be str')

        if namespace in storage.categories:
            if hasattr(getattr(storage, namespace), key):
                try:
                    setattr(storage, namespace, getattr(storage, namespace)._replace(
                        **{key: type(getattr(getattr(storage, namespace), key))(value)}))
                    return True
                except ValueError:
                    raise TypeError(f'value type must be "{type(getattr(getattr(storage, namespace), key))}"')
            else:
                raise IndexError(f'"{key}" not found in "{namespace}"')
        else:
            raise IndexError(f'Namespace "{namespace}" not found')

    @staticmethod
    def script(name: str) -> dict:
        if name in core.script_manager.scripts:
            return {k: v for k, v in core.script_manager.scripts[name].items() if not k.startswith('__')}
        else:
            return {}

    @staticmethod
    def script_load(name: str) -> int:
        return core.script_manager.load(name)[1]

    @staticmethod
    def script_unload(name: str) -> int:
        return core.script_manager.unload(name)[1]

    @staticmethod
    def script_reload(name: str) -> int:
        return core.script_manager.reload(name)[1]

    @staticmethod
    def scripts_load_all() -> int:
        return core.script_manager.load_all()[1]

    @staticmethod
    def scripts_unload_all() -> int:
        return core.script_manager.unload_all()[1]

    @staticmethod
    def scripts_reload_all() -> int:
        return core.script_manager.reload_all()[1]

    @staticmethod
    def scripts_reindex() -> int:
        return core.script_manager.index.reindex()

    @staticmethod
    def pipe_pause() -> bool:
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.pipe.state = 2
            return True

    @staticmethod
    def pipe_resume() -> bool:
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.pipe.state = 4
            return True

    @staticmethod
    def worker_stop(id_: int = -1) -> int:
        return core.monitor.thread_manager.stop_worker(id_)

    @staticmethod
    def worker_list() -> list:
        return list(core.monitor.thread_manager.workers)

    @staticmethod
    def worker_pause(id_: int) -> bool:
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.workers[id_].state = 2
            return True

    @staticmethod
    def worker_resume(id_: int) -> bool:
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.workers[id_].state = 4
            return True

    @staticmethod
    def index_worker_stop(id_: int = -1) -> int:
        return core.monitor.thread_manager.stop_index_worker(id_)

    @staticmethod
    def index_worker_list() -> list:
        return list(core.monitor.thread_manager.workers)

    @staticmethod
    def index_worker_pause(id_: int) -> bool:
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.index_workers[id_].state = 2
            return True

    @staticmethod
    def index_worker_resume(id_: int) -> bool:
        with core.monitor.thread_manager.lock:
            core.monitor.thread_manager.index_workers[id_].state = 4
            return True

    @staticmethod
    def stop() -> bool:
        if core.monitor.state == 1:
            core.monitor.state = 2
            return True
        else:
            return False
