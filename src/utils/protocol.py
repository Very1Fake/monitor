from datetime import datetime
from typing import Dict

_codes: Dict[int, str] = {
    # Debug (1xxxx)
    # System (100xx)
    10000: 'Test debug',

    # Pipe (103xx)
    10301: 'Reindexing parser',

    # Resolver (109xx)
    10901: 'Executing catalog',
    10902: 'Executing target',
    10903: 'Catalog executed',
    10904: 'Target executed',

    # SubProvider (113xx)
    11301: 'Common exception while sending request',

    # Information (2xxxx)
    # System (200xx)
    20000: 'Test information',
    20001: 'Thread started',
    20002: 'Thread paused',
    20003: 'Thread resumed',
    20004: 'Thread closing',
    20005: 'Thread closed',

    # Kernel (201xx)
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
    20205: 'CatalogWorker initialized',
    20206: 'CatalogWorker started',

    # Pipe (203xx)
    20301: 'Reindexing parsers started',
    20302: 'Reindexing parsers complete',
    20303: 'Parser reindexing complete',

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
    20606: 'Skipping script (script incompatible with monitor)',
    20607: 'Skipping script (script in blacklist)',
    20608: 'Skipping script (script with this name is already indexed)',
    20609: 'N script(s) indexed',
    20610: 'Skipping config (script not in whitelist)',

    # EventHandler (207xx)
    20701: 'Starting loop',
    20702: 'Loop started',
    20703: 'Stopping loop',
    20704: 'Loop stopped',

    # Logger (208xx)
    20801: 'Log level changed',
    20802: 'Log mode changed',

    # Resolver (209xx)
    20901: 'Successful target execution',
    20902: 'Catalog updated',

    # Commands (211xx)
    21101: 'Command executing',
    21102: 'Command executed',
    21103: 'Command execute',

    # Provider (212xx)
    21201: 'Proxies dumped',
    21202: 'Checking proxy',
    21203: 'Checking proxy (OK)',

    # Keywords (215xx)
    21501: 'Dumping keywords(started)',
    21502: 'Dumping keywords(complete)',
    21503: 'Clearing keywords(started)',
    21504: 'Clearing keywords(complete)',
    21505: 'Syncing keywords(started)',
    21506: 'Syncing keywords(complete)',
    21507: 'Loading keywords(started)',
    21508: 'Loading keywords(complete)',

    # Warning (3xxxx)
    # System (300xx)
    30000: 'Test warning',

    # ThreadManager (302xx)
    30201: 'Pipe was stopped',
    30202: 'Worker was stopped',
    30203: 'CatalogWorker was stopped',
    30204: 'Lock forced released',

    # Pipe (303xx)
    30301: 'Parser reindexing failed',
    30302: 'Catalog lost while sending (queue full)',
    30303: 'Target lost while sending (queue full)',

    # ScriptManager (305xx)
    30501: 'Module not loaded',
    30502: 'Nothing to import in script',
    30503: 'Script cannot be unloaded (_unload)',
    30504: 'Script cannot be unloaded (_reload)',
    30505: 'Script not indexed but still loaded',
    30506: 'Script already loaded',
    30507: 'Max errors for script reached, unloading',

    # EventHandler (307xx)
    30701: 'Loop already started',
    30702: 'Loop already stopped',

    # Logger (308xx)
    30801: 'Meaningless level change (changing to the same value)',
    30802: 'Meaningless mode change (changing to the same value)',

    # Resolver (309xx)
    30901: 'Catalog lost while retrieving (script not loaded)',
    30902: 'Catalog lost while retrieving (script has no Parser)',
    30903: 'Target lost while retrieving (script not loaded)',
    30904: 'Target lost while retrieving (script has no Parser)',
    30905: 'Catalog lost while executing (script unloaded)',
    30906: 'Catalog lost while executing (script has no parser)',
    30907: 'Catalog lost while executing (bad result)',
    30908: 'Target lost while executing (script unloaded)',
    30909: 'Target lost while executing (script has no parser)',
    30910: 'Target lost while executing (bad result)',
    30911: 'Smart catalog expired',
    30912: 'Smart target expired',

    # Provider (312xx)
    31201: 'Proxy added',
    31202: 'Proxy removed',
    31203: 'Proxies list changed',
    31204: 'Proxies statistics reset',
    31205: 'Proxies list cleared',

    # Keywords (315xx)
    31501: 'Keywords file not found',
    31511: 'Absolute keyword not loaded (TypeError)',
    31512: 'Absolute keyword not loaded (UniquenessError)',
    31521: 'Positive keyword not loaded (TypeError)',
    31522: 'Positive keyword not loaded (UniquenessError)',
    31531: 'Negative keyword not loaded (TypeError)',
    31532: 'Negative keyword not loaded (UniquenessError)',

    # Error (4xxxx)
    # System (400xx)
    40000: 'Unknown error',

    # ThreadManager (402xx)
    40201: 'Pipe was unexpectedly stopped',
    40202: 'Worker was unexpectedly stopped',
    40203: 'CatalogWorker was unexpectedly stopped',

    # Pipe (403xx)
    40301: 'Wrong catalog received from script',

    # Worker (404xx)
    40401: 'Unknown status received while executing',
    40402: 'Parser execution failed',
    40403: 'Target lost in pipeline (script unloaded)',

    # ScriptsManager (405xx)
    40501: 'Can\'t load script (ImportError)',
    40502: 'Can\'t load script (script not indexed)',
    40503: 'Can\'t unload script (script isn\'t loaded)',
    40504: 'Can\'t reload script (script isn\'t loaded)',
    40505: 'Script cannot be reloaded (folder not found)',
    40506: 'Script cannot be reloaded (script not in index)',

    # EventHandler (407xx)
    40701: 'Event execution failed',

    # Logger (408xx)
    40801: 'Can\'t change level (possible values (0, 1, 2, 3, 4, 5))',
    40802: 'Can\'t change mode (possible values (0, 1, 2, 3))',

    # Resolver (409xx)
    40901: 'Unknown index type (while inserting)',
    40902: 'Unknown target type (while inserting)',
    40903: 'Catalog execution failed',
    40904: 'Target execution failed',

    # Provider (412xx)
    41201: 'Bad proxy',
    41202: 'Checking proxy (FAILED)',

    # SubProvider (413xx)
    41301: 'Severe exception while sending request',

    # Keywords (415xx)
    41501: 'Loading keywords (Failed)',

    # Fatal (5xxxx)
    # System (500xx)
    50000: 'Test fatal',

    # Kernel (501xx)
    50101: 'ThreadManager unexpectedly has turned off',

    # ThreadManager (502xx)
    50201: 'Exception raised, emergency stop initiated',

    # Pipe (503xx)
    50301: 'Unexpectedly has turned off',

    # Worker (504xx)
    50401: 'Unexpectedly has turned off',

    # CatalogWorker (510xx)
    51001: 'Unexpectedly has turned off',

    # RemoteThread (514xx)
    51401: 'Unknown fatal error'
}


class CodeError(Exception):
    pass


class Code:
    __slots__ = ('code', 'title', 'message')
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


def get_time(name: bool = False) -> str:
    return datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S' if name else '%Y-%m-%d %H:%M:%S')
