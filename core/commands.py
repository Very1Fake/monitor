import uctp

from . import logger


class Commands:
    logger: logger.Logger

    def __init__(
            self,
            server: uctp.peer.Peer,
            core
    ):
        self.log = logger.Logger('Commands')
        self.core = core
        server.commands.add_(self.stop)

    def stop(self) -> bool:
        if self.core.state == 1:
            self.core.state = 2
            return True
        else:
            return False
