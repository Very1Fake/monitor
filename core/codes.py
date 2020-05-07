from typing import Dict

_codes: Dict[int, str] = {
    # Debug (1xxxx)
    # System (100xx)
    10000: 'Test debug',

    # Resolver (109xx)
    10901: 'Executing target',
    10902: 'Executing catalog',

    # Information (2xxxx)
    # System (200xx)
    20000: 'Test information',
    20001: 'Thread started',
    20002: 'Thread paused',
    20003: 'Thread resumed',
    20004: 'Thread closing',
    20005: 'Thread closed',

    # Core (201xx)
    20101: 'Production mode enabled',
    20102: 'Signal Interrupt',
    20103: 'Turning off',
    20104: 'Saving success hashes started',
    20105: 'Saving success hashes complete',
    20106: 'Offline',

    # ThreadManager (202xx)
    20201: 'Pipe initialized',
    20202: 'Pipe started',
    20203: 'Worker initialized',
    20204: 'Worker started',
    20205: 'IndexWorker initialized',
    20206: 'IndexWorker started',

    # Pipe (203xx)
    20301: 'Reindexing parsers started',
    20302: 'Reindexing parsers complete',

    # ScriptManager (205xx)
    20501: 'Script loaded',
    20502: 'Script unloaded',
    20503: 'Script reloaded',
    20504: 'Loading all indexed scripts',
    20505: 'Loading all indexed scripts complete',
    20506: 'Unloading all scripts',
    20507: 'Unloading all scripts complete',
    20508: 'Reloading all scripts',
    20509: 'Reloading all scripts complete',

    # ScriptIndex (206xx)
    20601: 'Config loaded',
    20602: 'Config dumped',
    20603: 'Config does not loaded (must be dict)',
    20604: 'Skipping script (config not detected)',
    20605: 'Skipping script (bad config)',
    20606: 'Skipping script (script incompatible with core)',
    20607: 'Skipping script (ignored by config)',
    20608: 'Skipping script (script with this name is already indexed)',
    20609: 'N script(s) indexed',

    # Logger (208xx)
    20801: 'Log level changed',
    20802: 'Log mode changed',
    20803: 'Time changed to UTC',
    20804: 'Time changed to local',

    # Resolver (209xx)
    20901: 'Successful target execution',
    20902: 'Catalog updated',

    # Warning (3xxxx)
    # System (300xx)
    30000: 'Test warning',

    # ThreadManager (302xx)
    30201: 'Pipe was unexpectedly stopped',
    30202: 'Worker was unexpectedly stopped',
    30203: 'IndexWorker was unexpectedly stopped',

    # Pipe (303xx)
    30301: 'Target lost while inserting in schedule',
    30302: 'Target lost in pipeline',
    30303: 'Catalog lost in pipeline',

    # ScriptManager (305xx)
    30501: 'Module not loaded',
    30502: 'Nothing to import in script',
    30503: 'Script cannot be unloaded (_unload)',
    30504: 'Script cannot be unloaded (_reload)',
    30505: 'Script not indexed but still loaded',
    30506: 'Script already loaded',
    30507: 'Max errors for script reached unloading',

    # Logger (308xx)
    30801: 'Meaningless level change (changing to the same value)',
    30802: 'Meaningless mode change (changing to the same value)',
    30803: 'Meaningless time change (changing to the same value)',

    # Resolver (309xx)
    30901: 'Target lost (script not loaded)',
    30902: 'Target lost while executing (script not loaded)',
    30903: 'Target failed',
    30904: 'Unknown status received while executing target',
    30905: 'Catalog lost while executing (script not loaded)',
    30906: 'Wrong target list received while updating catalog',

    # Error (4xxxx)
    # System (400xx)
    40000: 'Test error',

    # Pipe (403xx)
    40301: 'Unknown index',
    40302: 'Wrong target list received from script',
    40303: 'Parser execution failed',
    40304: 'Target lost in pipeline (script unloaded)',

    # Worker (404xx)
    40401: 'Unknown status received while executing',
    40402: 'Parser execution failed',
    40403: 'Target lost in pipeline (script unloaded)',

    # ScriptsManager (405xx)
    40501: 'Can\'t load script (ImportError)',
    40502: 'Can\'t load script (script not indexed)',
    40503: 'Can\'t unload script (script isn\'t loaded)',
    40504: 'Can\'t reload script (script isn\'t loaded)',

    # Logger (408xx)
    40801: 'Can\'t change level (possible values (0, 1, 2, 3, 4, 5))',
    40802: 'Can\'t change mode (possible values (0, 1, 2, 3))',

    # Resolver (409xx)
    40901: 'Unknown index type (while inserting)',
    40902: 'Unknown target type (while inserting)',
    40903: 'Target execution failed',
    40904: 'Catalog execution failed',

    # Fatal (5xxxx)
    # System (500xx)
    50000: 'Test fatal',

    # Core (501xx)
    50101: 'ThreadManager unexpectedly has turned off',

    # ThreadManager (502xx)
    50201: 'Exception raised, emergency stop initiated',

    # Pipe (503xx)
    50301: 'Unexpectedly has turned off',

    # Worker (504xx)
    50401: 'Unexpectedly has turned off',

    # IndexWorker (510xx)
    51001: 'Unexpectedly has turned off'

}


class CodeError(Exception):
    pass


class Code:
    __slots__ = ('code', 'title', 'digest', 'hexdigest', 'message')
    code: int
    title: str
    message: str

    def __init__(self, code: int, message: str = ''):
        if isinstance(code, int) and len(str(code)) == 5:
            self.code = code
            if code in _codes:
                self.title = _codes[code]
            else:
                raise CodeError('Code does not exist')
        else:
            raise CodeError('Code must be int in range (10000 - 65535)')
        self.message = message

    def __str__(self) -> str:
        return self.format()

    def __repr__(self) -> str:
        return f'Code({self.code}, {self.title})'

    def format(self, mode: int = 1) -> str:
        if mode == 1 and self.message:
            return f'C{self.code}: {self.message}'
        elif mode == 2:
            return f'C{self.code} {self.title}' + (f': {self.message}' if self.message else '')
        else:
            return f'C{self.code}'
