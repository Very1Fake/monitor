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
        core.server.commands.add_(self.stop)

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
    def stop() -> bool:
        if core.monitor.state == 1:
            core.monitor.state = 2
            return True
        else:
            return False
