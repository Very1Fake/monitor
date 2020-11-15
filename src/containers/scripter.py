from importlib import import_module
from os import makedirs, path, walk
from queue import Empty, Queue
from sys import modules
from threading import Lock, RLock, Thread
from time import sleep, time
from traceback import format_exc
from types import ModuleType
from typing import Any, Dict, List, Tuple, Type

from checksumdir import dirhash
from packaging.version import Version, InvalidVersion
from yaml import safe_dump, safe_load

from src import version
from src.models.api.impl import EventsExecutor, Parser
from src.models.api.item import ItemType
from src.models.api.message import MessageType
from src.models.api.target import TargetEndType
from src.models.provider import SubProvider
from src.utils import store
from src.utils.log import Logger
from src.utils.protocol import Code
from src.utils.storage import MainStorage, ScriptStorage


# Exceptions


class ScriptNotFoundError(Exception):
    pass


class ParserImplementationError(Exception):
    pass


# Classes


class ScriptIndex:
    lock: RLock
    log: Logger
    config: dict
    index: list

    def __init__(self):
        self.lock = RLock()
        self.log = Logger('SI')

        if not path.isdir('scripts/'):
            makedirs('scripts/')
        self.config = {'blacklist': {'folders': [], 'scripts': []}, 'whitelist': {'folders': [], 'scripts': []}}
        self.log.info(Code(20601))
        self.index = []

    def __repr__(self) -> str:
        return f'ScriptIndex({len(self.index)} script(s) indexed)'

    def del_(self):
        self.config_dump()
        self.log.info(Code(20602))
        del self

    def config_check(self) -> None:
        with self.lock:
            if MainStorage().file('scripts.yaml'):
                conf = safe_load(MainStorage().file('scripts.yaml'))
                if isinstance(conf, dict):
                    different = False
                    for k, v in self.config.items():
                        if k not in conf or not isinstance(conf[k], type(v)):
                            different = True
                            conf[k] = v
                        elif isinstance(self.config[k], dict):
                            for k2, v2 in self.config[k].items():
                                if k2 not in conf[k] or not isinstance(conf[k][k2], type(v2)):
                                    different = True
                                    conf[k][k2] = v2
                    if different:
                        safe_dump(conf, MainStorage().file('scripts.yaml', 'w+'))
                    else:
                        return
            safe_dump(self.config, MainStorage().file('scripts.yaml', 'w+'))

    def config_load(self) -> None:
        with self.lock:
            self.config_check()
            self.config = safe_load(MainStorage().file('scripts.yaml'))

    def config_dump(self) -> None:
        with self.lock:
            safe_dump(self.config, MainStorage().file('scripts.yaml', 'w+'))

    @staticmethod
    def check_dependency_version_style(version_: str) -> bool:
        if version_.startswith(('^', '_', '=')):
            try:
                Version(version_[1:])
                return True
            except InvalidVersion:
                return False
        return False

    @staticmethod
    def check_dependency_version(version_: str) -> bool:
        if version_.startswith('=') and Version(version_[1:]) == version or \
                version_.startswith('^') and Version(version_[1:]) <= version or \
                version_.startswith('_') and Version(version_[1:]) >= version:
            return True
        else:
            return False

    def check_config(self, file: str) -> bool:
        good = True
        config: dict = safe_load(open(file))
        if isinstance(config, dict):
            if 'name' in config:
                if not isinstance(config['name'], str):
                    self.log.debug('"name" must be str in ' + file)
                    good = False
            else:
                self.log.debug('"name" not specified in ' + file)
                good = False
            if 'kernel' in config:
                if not isinstance(config['kernel'], str):
                    self.log.debug('"kernel" must be version(str) in ' + file)
                    good = False
                else:
                    if not self.check_dependency_version_style(config['kernel']):
                        self.log.debug('Wrong style of version in "kernel" in ' + file)
                        good = False
            else:
                self.log.debug('"kernel" not specified in ' + file)
                good = False
            if 'version' in config:
                if not isinstance(config['version'], str):
                    self.log.debug('"version" must be version(str) in ' + file)
                    good = False
                else:
                    try:
                        Version(config['version'])
                    except InvalidVersion:
                        self.log.debug('Wrong version style of "version" in ' + file)
                        good = False
            else:
                self.log.debug('"version" not specified in ' + file)
                good = False
            if good:
                return True
        return False

    @staticmethod
    def get_config(file: str) -> dict:
        raw: dict = safe_load(open(file))
        config: dict = {
            'name': raw['name'],
            'kernel': raw['kernel'],
            'version': raw['version'],
            '_path': path.abspath(path.dirname(file))
        }
        if 'description' in raw:
            config['description'] = raw['description']
        if 'can_be_unloaded' in raw:
            config['can_be_unloaded'] = raw['can_be_unloaded']
        else:
            config['can_be_unloaded'] = True
        if 'keep' in raw:
            config['keep'] = raw['keep']
        else:
            config['keep'] = False
        if 'max-errors' in raw:
            config['max-errors'] = raw['max-errors'] if config['can_be_unloaded'] else -1
        else:
            config['max-errors'] = -1
        return config

    def reindex(self) -> int:
        folders: list = list(
            i for i in next(walk('scripts/'))[1]
            if not (
                    i in self.config['blacklist']['folders'] or
                    (self.config['whitelist']['folders'] and i not in self.config['whitelist']['folders'])
            )
        )  # Get all script folders
        names: List[str] = []
        self.index.clear()
        for i in folders:
            file = f'scripts/{i}/config.yaml'
            if not path.isfile(file):  # Check for config.yaml
                self.log.info(Code(20604, f'"{i}/"'))
                continue
            if not self.check_config(file):  # Check for config.yaml
                self.log.info(Code(20605, f'"{i}/"'))
                continue
            config: dict = self.get_config(file)  # Get config from config.yaml
            if not self.check_dependency_version(config['kernel']):
                self.log.info(Code(20606, f'"{i}/"'))
                continue
            if config['name'] in self.config['blacklist']['scripts']:
                self.log.info(Code(20607, f'"{i}/"'))
                continue
            if self.config['whitelist']['scripts'] and config['name'] not in self.config['whitelist']['scripts']:
                self.log.info(Code(20610, f'"{i}/"'))
                continue
            if config['name'] in names:
                self.log.info(Code(20608, f'"{i}/"'))
                continue
            names.append(config['name'])  # Save name to check on uniqueness
            self.index.append(config)
        self.log.info(Code(20609, str(len(self.index))))
        return 20609

    def blacklist_add(self, name: str, folder: bool = False) -> None:
        if isinstance(name, str):
            if folder:
                if name not in self.config['blacklist']['folders']:
                    self.config['blacklist']['folders'].append(name)
                else:
                    raise NameError('This name already in folders blacklist')
            else:
                if name not in self.config['blacklist']['scripts']:
                    self.config['blacklist']['scripts'].append(name)
                else:
                    raise NameError('This name already in scripts blacklist')
        else:
            raise TypeError('name must be str')

    def blacklist_remove(self, name: str, folder: bool = False) -> None:
        if isinstance(name, str):
            if folder:
                try:
                    self.config['blacklist']['folders'].remove(name)
                except ValueError:
                    raise NameError('This name not in folders blacklist')
            else:
                try:
                    self.config['blacklist']['scripts'].remove(name)
                except ValueError:
                    raise NameError('This name not in scripts blacklist')
        else:
            raise TypeError('name must be str')

    def whitelist_add(self, name: str, folder: bool = False) -> None:
        if isinstance(name, str):
            if folder:
                if name not in self.config['whitelist']['folders']:
                    self.config['whitelist']['folders'].append(name)
                else:
                    raise NameError('This name already in folders whitelist')
            else:
                if name not in self.config['whitelist']['scripts']:
                    self.config['whitelist']['scripts'].append(name)
                else:
                    raise NameError('This name already in folders whitelist')
        else:
            raise TypeError('name must be str')

    def whitelist_remove(self, name: str, folder: bool = False) -> None:
        if isinstance(name, str):
            if folder:
                try:
                    self.config['whitelist']['folders'].remove(name)
                except ValueError:
                    raise NameError('This name not in folders whitelist')
            else:
                try:
                    self.config['whitelist']['scripts'].remove(name)
                except ValueError:
                    raise NameError('This name not in scripts whitelist')
        else:
            raise TypeError('name must be str')

    def get_script(self, name: str) -> dict:
        for i in self.index:
            if i['name'] == name:
                return i
        else:
            raise IndexError(f'Script {name}" not found')


class EventHandler:
    _active: bool
    _lock: Lock
    _log: Logger

    executors: Dict[str, EventsExecutor]
    pool: Queue
    thread: Thread

    def __init__(self):
        self._active = False
        self._lock = Lock()
        self._log = Logger('EH')

        self.executors = {}
        self.pool = Queue()
        self.thread = Thread(target=self.loop, daemon=True)

    def loop(self):
        while True:
            start = time()

            try:
                task: Tuple[str, list] = self.pool.get_nowait()

                with self._lock:
                    for k, v in self.executors.items():
                        try:
                            getattr(v, task[0])(*task[1])
                        except AttributeError:
                            pass
                        except Exception as e:
                            if store.main.production:
                                self._log.error(Code(40701, f'{k}: {e.__class__.__name__}: {e!s}'))
                            else:
                                self._log.fatal_msg(Code(40701, k), format_exc())
            except Empty:
                if not self._active:
                    break

            delta = time() - start
            sleep(store.event_handler.tick - delta if store.event_handler.tick - delta > 0 else 0)

    def start(self):
        self._log.info(Code(20701))
        if not self.thread.is_alive():
            self._active = True
            try:
                self.thread.start()
            except RuntimeError:
                self.thread = Thread(target=self.loop, daemon=True)
                self.thread.start()
            self._log.info(Code(20702))
        else:
            self._log.warn(Code(30701))

    def stop(self):
        self._log.info(Code(20703))
        if self.thread.is_alive():
            self._active = False
            self.thread.join(store.event_handler.wait)
            self._log.info(Code(20704))
        else:
            self._log.warn(Code(30702))

    def add(self, name: str, executor: Type[EventsExecutor]) -> bool:
        with self._lock:
            self.executors[name] = executor(name, Logger('EE/' + name), ScriptStorage(name))
            return True

    def delete(self, name: str) -> bool:
        with self._lock:
            if name in self.executors:
                del self.executors[name]
            return True

    def monitor_starting(self) -> None:
        self.pool.put(('e_monitor_starting', ()))

    def monitor_started(self) -> None:
        self.pool.put(('e_monitor_started', ()))

    def monitor_stopping(self) -> None:
        self.pool.put(('e_monitor_stopping', ()))

    def monitor_stopped(self) -> None:
        self.pool.put(('e_monitor_stopped', ()))

    def alert(self, code: Code, thread: str) -> None:
        self.pool.put(('e_alert', (code, thread)))

    def item(self, item: ItemType) -> None:
        self.pool.put(('e_item', (item,)))

    def target_end(self, target_end: TargetEndType) -> None:
        self.pool.put(('e_target_end', (target_end,)))

    def message(self, message: MessageType) -> None:
        self.pool.put(('e_message', (message,)))


class ScriptManager:
    log: Logger
    lock: RLock
    scripts: Dict[str, dict]
    parsers: Dict[str, Parser]

    index: ScriptIndex
    event_handler: EventHandler

    def __init__(self):
        self.log = Logger('SM')
        self.lock = RLock()
        self.scripts = {}
        self.parsers = {}

        self.index = ScriptIndex()
        self.event_handler = EventHandler()

    def del_(self):
        del self.parsers
        del self.event_handler
        del self.scripts
        del self.index
        del self

    def _destroy(self, name: str) -> bool:  # Memory leak patch
        try:
            del modules[name]
            return True
        except KeyError:
            self.log.warn(Code(30501, name))
            return False

    def _scan(self, script: dict, module: ModuleType) -> bool:
        success = False
        if 'Parser' in module.__dict__ and issubclass(getattr(module, 'Parser'), Parser):
            if script['keep']:
                self.parsers[script['name']] = getattr(module, 'Parser')(
                    script['name'],
                    Logger('Parser/' + script['name']),
                    SubProvider(script['name']),
                    ScriptStorage(script['name'])
                )
            else:
                self.parsers[script['name']] = getattr(module, 'Parser')
            success = True
        if 'EventsExecutor' in module.__dict__ and issubclass(getattr(module, 'EventsExecutor'), EventsExecutor):
            self.event_handler.add(script['name'], getattr(module, 'EventsExecutor'))
            success = True
        return success

    def _load(self, script) -> bool:
        with self.lock, self.index.lock:
            if isinstance(script, str):
                script: dict = self.index.get_script(script)
            module: ModuleType = import_module(path.relpath(script['_path']).replace('/', '.'))
            if success := self._scan(script, module):
                self.scripts[script['name']] = script.copy()
                self.scripts[script['name']]['_hash'] = dirhash(script['_path'], 'sha1')
                self.scripts[script['name']]['_errors'] = 0
                self.scripts[script['name']]['_module'] = module.__name__
            else:
                self.log.warn(Code(30502, script['name']))
            return success

    def _unload(self, name: str) -> bool:
        with self.lock, self.index.lock:
            if self.scripts[name]['can_be_unloaded']:
                self.event_handler.delete(name)
                if name in self.parsers:
                    del self.parsers[name]
                self._destroy(self.scripts[name]['_module'])
                del self.scripts[name]
                return True
            else:
                self.log.warn(Code(30503, name))
                return False

    def _reload(self, name: str) -> Tuple[bool, int]:
        with self.lock, self.index.lock:
            if self.scripts[name]['can_be_unloaded']:
                try:
                    script: dict = self.scripts[name]
                    script.update(self.index.get_script(name))
                    script['_hash'] = dirhash(script['_path'], 'sha1')
                    if script:
                        module: ModuleType = import_module(path.relpath(script['_path']).replace('/', '.'))
                        self._destroy(module.__name__)
                        if self._scan(script, module):
                            return True, 0
                        else:
                            self.log.warn(Code(30502, name))
                            return False, 30502
                    else:
                        self.log.warn(Code(30504, name))
                        return False, 30504
                except StopIteration:
                    self.log.error(Code(40506, name))
                    return False, 40506
                except TypeError:
                    self.log.error(Code(40505, name))
                    return False, 40505
            else:
                self.log.warn(Code(30505, name))
                return False, 30505

    def load(self, script: str) -> Tuple[bool, int]:
        script: dict = self.index.get_script(script)
        if script and script['name'] in self.scripts:
            self.log.warn(Code(30506, script['name']))
            return True, 30506
        elif script and script['name'] not in self.scripts:
            try:
                if self._load(script):
                    self.log.info(Code(20501, script['name']))
                    return True, 20501
            except ImportError as e:
                if store.main.production:
                    self.log.error(Code(40501, script['name']))
                else:
                    self.log.fatal(e)
        else:
            self.log.error(Code(40502, script['name']))
        return False, 40502

    def unload(self, name: str) -> Tuple[bool, int]:
        if name in self.scripts:
            if self._unload(name):
                self.log.info(Code(20502, name))
                return True, 20502
        else:
            self.log.error(Code(40503, name))
        return False, 40503

    def reload(self, name: str) -> Tuple[bool, int]:
        if name in self.scripts:
            if (result := self._reload(name))[0]:
                self.log.info(Code(20503, name))
                return True, 20503
            else:
                return False, result[1]
        else:
            self.log.error(Code(40504, name))
        return False, 40504

    def load_all(self) -> Tuple[bool, int]:
        self.log.info(Code(20504))
        for i in [i for i in [i['name'] for i in self.index.index] if i not in self.scripts]:
            try:
                if self._load(i):
                    self.log.info(Code(20501, i))
            except ImportError as e:
                if store.main.production:
                    self.log.error(Code(40501, i))
                else:
                    self.log.fatal(e)
        self.log.info(Code(20505))
        return True, 20505

    def unload_all(self) -> Tuple[bool, int]:
        self.log.info(Code(20506))
        for i in self.scripts.copy():
            if self._unload(i):
                self.log.info(Code(20502, i))
        self.log.info(Code(20507))
        return True, 20507

    def reload_all(self) -> Tuple[bool, int]:
        self.log.info(Code(20508))
        for i in self.scripts:
            if self._reload(i):
                self.log.info(Code(20503, i))
        self.log.info(Code(20509))
        return True, 20509

    def hash(self) -> Dict[str, str]:
        return {i: self.scripts[i]['_hash'] for i in self.parsers if i in self.scripts}

    def parser_error(self, name: str):
        if not isinstance(name, str):
            raise TypeError('name must be str')

        with self.lock:
            try:
                self.scripts[name]['_errors'] += 1
            except KeyError:
                raise ScriptNotFoundError

    def get_parser(self, name: str):
        with self.lock:
            try:
                keep = self.scripts[name]['keep']
            except KeyError:
                raise ScriptNotFoundError

            try:
                parser = self.parsers[name]
            except KeyError:
                raise ParserImplementationError
        if keep:
            return parser
        else:
            return parser.__init__(
                name,
                Logger(f'parser/{name}'),
                SubProvider(name),
                ScriptStorage(name)
            )

    def execute_parser(self, name: str, func: str, args: tuple) -> Any:
        try:
            return getattr(self.get_parser(name), func)(*args)
        except (ParserImplementationError, ScriptNotFoundError) as e:
            raise e
        except Exception as e:
            with self.lock:
                scripts = self.scripts[name]
                scripts['_errors'] += 1
                if scripts['max-errors'] < 0 or \
                        scripts['_errors'] >= scripts['max-errors']:
                    self.log.warn(Code(30507, name))
                    self.unload(name)
                raise e
