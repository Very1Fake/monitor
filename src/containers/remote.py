from traceback import format_exc

from uctp.peer import Peer, ErrorHandler

from src.store import main
from src.utils.log import Logger
from src.utils.protocol import Code
from .scripter import EventHandler


class RemoteThreadError(Exception):
    pass


class RemoteThreadHandler(ErrorHandler):
    _log: Logger

    event_handler: EventHandler

    def __init__(self, event_handler: EventHandler):
        self._log = Logger('RT')
        self.event_handler = event_handler

    def handle(self, peer: Peer, exception: Exception):
        self.event_handler.alert(Code(51401), 'RT')
        if main.production:
            self._log.fatal_msg(Code(51401), format_exc())
        else:
            self._log.fatal(RemoteThreadError(Code(51401, f'{exception.__class__.__name__}: {exception!s}')))
