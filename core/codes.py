import struct

_10001 = 'Thread started'
_10002 = 'Thread paused'
_10003 = 'Thread resumed'
_10004 = 'Thread closing'
_10005 = 'Thread closed'
_11001 = 'Production mode enabled'
_11002 = 'Signal Interrupt'
_12001 = 'Collector initialized'
_12002 = 'Collector started'
_12003 = 'Collector was unexpectedly stopped'
_12004 = 'Worker initialized'
_12005 = 'Worker started'
_12006 = 'Worker was unexpectedly stopped'
_14001 = 'Executing target'
_34001 = 'Target lost'
_41001 = 'ThreadManager unexpectedly has turned off'
_52001 = 'Exception raised, emergency stop initiated'


class CodeError(Exception):
    pass


class Code:
    code: int
    digest: bytes
    title: str
    message: str

    def __init__(self, code: int, message: str = ''):
        if isinstance(code, int):
            self.code = code
            if f'_{code}' in globals():
                self.title = globals()[f'_{code}']
            else:
                raise CodeError('Code does not exist')
        else:
            raise CodeError('Code must be int')
        self.digest = struct.pack('>H', code)
        self.message = message

    def __str__(self) -> str:
        return f'C{self.code}'

    def __repr__(self) -> str:
        return f'Code({self.code}, {self.digest}, {self.title})'

    def format(self, mode: int = 1) -> str:
        if mode == 1 and self.message:
            return f'C{self.code}: {self.message}'
        elif mode == 2:
            return f'C{self.code} {self.title}' + (f': {self.message}' if self.message else '')
        else:
            return f'C{self.code}'
