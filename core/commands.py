import uctp

from core import core
from . import logger


class Commands:
    logger: logger.Logger

    def __init__(self):
        self.log = logger.Logger('Commands')
        self.core = core
        core.server.commands.add_(self.analytics_dump)
        core.server.commands.add_(self.analytics_snapshot)
        core.server.commands.add_(self.analytics_worker)
        core.server.commands.add_(self.analytics_index_worker)
        core.server.commands.add_(self.script)
        core.server.commands.add_(self.script_load)
        core.server.commands.add_(self.script_unload)
        core.server.commands.add_(self.script_reload)
        core.server.commands.add_(self.scripts_load_all)
        core.server.commands.add_(self.scripts_unload_all)
        core.server.commands.add_(self.scripts_reload_all)
        core.server.commands.add_(self.scripts_reindex)
        core.server.commands.add_(self.stop)

        core.server.commands.alias('a_dump', 'analytics_dump')
        core.server.commands.alias('a_snapshot', 'analytics_snapshot')
        core.server.commands.alias('a_worker', 'analytics_worker')
        core.server.commands.alias('a_i_worker', 'analytics_index_worker')
        core.server.commands.alias('s_load', 'script_load')
        core.server.commands.alias('s_unload', 'script_unload')
        core.server.commands.alias('s_reload', 'script_reload')
        core.server.commands.alias('s_reindex', 'scripts_reindex')

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
    def stop() -> bool:
        if core.monitor.state == 1:
            core.monitor.state = 2
            return True
        else:
            return False
