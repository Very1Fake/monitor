import struct

# Debug (1xxxx)

# System (100xx)
_10000 = 'Test debug'

# Collector (103xx)
_10301 = 'N targets recived form script'

# Worker (104xx)
_10401 = 'Executing target'
_10402 = 'Target\'s execution result'

# Information (2xxxx)

# System (200xx)
_20000 = 'Test information'
_20001 = 'Thread started'
_20002 = 'Thread paused'
_20003 = 'Thread resumed'
_20004 = 'Thread closing'
_20005 = 'Thread closed'

# Core (201xx)
_20101 = 'Production mode enabled'
_20102 = 'Signal Interrupt'
_20103 = 'Turning off'
_20104 = 'Saving success hashes started'
_20105 = 'Saving success hashes complete'
_20106 = 'Offline'

# ThreadManager (202xx)
_20201 = 'Collector initialized'
_20202 = 'Collector started'
_20203 = 'Worker initialized'
_20204 = 'Worker started'
_20205 = 'IndexWorker initialized'
_20206 = 'IndexWorker started'

# Collector (203xx)
_20301 = 'Reindexing parsers started'
_20302 = 'Reindexing parsers complete'

# Worker (204xx)
_20401 = 'Success target execution'

# ScriptManager (205xx)
_20501 = 'Script loaded'
_20502 = 'Script unloaded'
_20503 = 'Script reloaded'
_20504 = 'Loading all indexed scripts'
_20505 = 'Loading all indexed scripts complete'
_20506 = 'Unloading all scripts'
_20507 = 'Unloading all scripts complete'
_20508 = 'Reloading all scripts'
_20509 = 'Reloading all scripts complete'

# ScriptIndex (206xx)
_20601 = 'Config loaded'
_20602 = 'Config dumped'
_20603 = 'Config does not loaded (must be dict)'
_20604 = 'Skipping script (config not detected)'
_20605 = 'Skipping script (bad config)'
_20606 = 'Skipping script (script incompatible with core)'
_20607 = 'Skipping script (ignored by config)'
_20608 = 'Skipping script (script with this name is already indexed)'
_20609 = 'N script(s) indexed'

# Logger (208xx)
_20801 = 'Log level changed'
_20802 = 'Log mode changed'
_20803 = 'Time changed to UTC'
_20804 = 'Time changed to local'

# Warnings (3xxxx)

# System (300xx)
_30000 = 'Test warning'

# ThreadManager (302xx)
_30201 = 'Collector was unexpectedly stopped'
_30202 = 'Worker was unexpectedly stopped'
_30203 = 'IndexWorker was unexpectedly stopped'

# Collector (303xx)
_30301 = 'Target lost while inserting in schedule'
_30302 = 'Target lost in pipeline'

# Worker (304xx)
_30401 = 'Target lost'
_30402 = 'Target lost in pipeline'
_30403 = 'Target execution failed'

# ScriptManager (305xx)
_30501 = 'Module not loaded'
_30502 = 'Nothing to import in script'
_30503 = 'Script cannot be unloaded (_unload)'
_30504 = 'Script cannot be unloaded (_reload)'
_30505 = 'Script not indexed but still loaded'
_30506 = 'Script already loaded'
_30507 = 'Max errors for script reached unloading'

# Logger (308xx)
_30801 = 'Meaningless level change (changing to the same value)'
_30802 = 'Meaningless mode change (changing to the same value)'
_30803 = 'Meaningless time change (changing to the same value)'

# Resolver (309xx)
_30901 = 'Target lost (script not found)'

# Errors (4xxxx)

# System (400xx)
_40000 = 'Test error'

# Collector (403xx)
_40301 = 'Unknown index'
_40302 = 'Wrong target list received from script'
_40303 = 'Parser execution failed'
_40304 = 'Target lost in pipeline (script unloaded)'

# Worker (404xx)
_40401 = 'Unknown status received while executing'
_40402 = 'Parser execution failed'
_40403 = 'Target lost in pipeline (script unloaded)'

# ScriptsManager (405xx)
_40501 = 'Can\'t load script (ImportError)'
_40502 = 'Can\'t load script (script not indexed)'
_40503 = 'Can\'t unload script (script isn\'t loaded)'
_40504 = 'Can\'t reload script (script isn\'t loaded)'

# Logger (408xx)
_40801 = 'Can\'t change level (possible values (0, 1, 2, 3, 4, 5))'
_40802 = 'Can\'t change mode (possible values (0, 1, 2, 3))'

# Resolver (409xx)
_40901 = 'Unknown index type (while inserting)'
_40902 = 'Unknown target type (while inserting)'
_40903 = 'Target lost in pipeline while executing'
_40904 = 'Target execution failed'
_40905 = 'Unknown status received while executing target'

# Fatals (5xxxx)

# System (500xx)
_50000 = 'Test fatal'

# Core (501xx)
_50101 = 'ThreadManager unexpectedly has turned off'

# ThreadManager (502xx)
_50201 = 'Exception raised, emergency stop initiated'

# Collector (503xx)
_50301 = 'Unexpectedly has turned off'

# Worker (504xx)
_50401 = 'Unexpectedly has turned off'


class CodeError(Exception):
    pass


class Code:
    __slots__ = ('code', 'title', 'digest', 'hexdigest', 'message')
    code: int
    title: str
    digest: bytes
    hexdigest: str
    message: str

    def __init__(self, code: int, message: str = ''):
        if isinstance(code, int) and len(str(code)) == 5:
            self.code = code
            if f'_{code}' in globals():
                self.title = globals()[f'_{code}']
            else:
                raise CodeError('Code does not exist')
        else:
            raise CodeError('Code must be int in range (10000 - 65535)')
        self.digest = struct.pack('>H', code)
        self.hexdigest = hex(code)
        self.message = message

    def __str__(self) -> str:
        return self.format()

    def __repr__(self) -> str:
        return f'Code({self.code}, {self.digest}, {self.title})'

    def format(self, mode: int = 1) -> str:
        if mode == 1 and self.message:
            return f'C{self.code}: {self.message}'
        elif mode == 2:
            return f'C{self.code} {self.title}' + (f': {self.message}' if self.message else '')
        else:
            return f'C{self.code}'
