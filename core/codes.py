import struct

# Debug (1xxxx)
_13001 = 'N targets recived form script'
_14001 = 'Executing target'
_14002 = 'Target\'s execution result'

# Information (2xxxx)
_20001 = 'Thread started'
_20002 = 'Thread paused'
_20003 = 'Thread resumed'
_20004 = 'Thread closing'
_20005 = 'Thread closed'
_21001 = 'Production mode enabled'
_21002 = 'Signal Interrupt'
_21003 = 'Turning off'
_21004 = 'Saving success hashes started'
_21005 = 'Saving success hashes complete'
_21006 = 'Offline'
_22001 = 'Collector initialized'
_22002 = 'Collector started'
_22003 = 'Collector was unexpectedly stopped'
_22004 = 'Worker initialized'
_22005 = 'Worker started'
_22006 = 'Worker was unexpectedly stopped'
_23001 = 'Reindexing parsers started'
_23002 = 'Reindexing parsers complete'
_24001 = 'Item available'
_25001 = 'Script loaded'
_25002 = 'Script unloaded'
_25003 = 'Script reloaded'
_25004 = 'Loading all indexed scripts'
_25005 = 'Loading all indexed scripts complete'
_25006 = 'Unloading all scripts'
_25007 = 'Unloading all scripts complete'
_25008 = 'Reloading all scripts'
_25009 = 'Reloading all scripts complete'
_26001 = 'Config loaded'
_26002 = 'Config dumped'
_26003 = 'Config does not loaded (must be dict)'
_26004 = 'Skipping config (config not detected)'
_26005 = 'Skipping config (bad config)'
_26006 = 'Skipping config (script incompatible with core)'
_26007 = 'Skipping config (ignored by config)'
_26008 = 'Skipping config (script with this name is already indexed)'
_26009 = 'N script(s) indexed'
_28001 = 'Log level changed'
_28002 = 'Log mode changed'
_28003 = 'Time changed to UTC'
_28004 = 'Time changed to local'

# Warnings (3xxxx)
_33001 = 'Target lost while inserting in schedule'
_34001 = 'Target lost'
_34002 = 'Target lost in pipeline'
_35001 = 'Module not loaded'
_35002 = 'Nothing to import in script'
_35003 = 'Script cannot be unloaded (_unload)'
_35004 = 'Script cannot be unloaded (_reload)'
_35005 = 'Script not indexed but still loaded'
_35006 = 'Script already loaded'
_35007 = 'Max errors for script reached unloading'
_38001 = 'Meaningless level change (changing to the same value)'
_38002 = 'Meaningless mode change (changing to the same value)'
_38003 = 'Meaningless time change (changing to the same value)'

# Errors (4xxxx)
_43001 = 'Unknown index'
_43002 = 'Wrong target list received from script'
_43003 = 'Parser execution failed'
_43004 = 'Target lost in pipeline (script unloaded)'
_44001 = 'Unknown status received while executing'
_44002 = 'Parser execution failed'
_44003 = 'Target lost in pipeline (script unloaded)'
_45001 = 'Can\'t load script (ImportError)'
_45002 = 'Can\'t load script (script not indexed)'
_45003 = 'Can\'t unload script (script isn\'t loaded)'
_45004 = 'Can\'t reload script (script isn\'t loaded)'
_48001 = 'Can\'t change level (possible values (0, 1, 2, 3, 4, 5))'
_48002 = 'Can\'t change mode (possible values (0, 1, 2, 3))'

# Fatals (5xxxx)
_51001 = 'ThreadManager unexpectedly has turned off'
_52001 = 'Exception raised, emergency stop initiated'


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
        if isinstance(code, int):
            self.code = code
            if f'_{code}' in globals():
                self.title = globals()[f'_{code}']
            else:
                raise CodeError('Code does not exist')
        else:
            raise CodeError('Code must be int')
        self.digest = struct.pack('>H', code)
        self.hexdigest = hex(code)
        self.message = message

    def __str__(self) -> str:
        return self.format(1)

    def __repr__(self) -> str:
        return f'Code({self.code}, {self.digest}, {self.title})'

    def format(self, mode: int = 1) -> str:
        if mode == 1 and self.message:
            return f'C{self.code}: {self.message}'
        elif mode == 2:
            return f'C{self.code} {self.title}' + (f': {self.message}' if self.message else '')
        else:
            return f'C{self.code}'
