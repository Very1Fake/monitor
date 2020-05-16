import hashlib
import importlib
import json
import os
import sys
import threading
from types import ModuleType
from typing import Dict, Any, Tuple, List

import yaml
from checksumdir import dirhash
from packaging.version import Version, InvalidVersion

from . import api
from . import codes
from . import storage
from . import version
from . import logger


# TODO: Status codes


class ScriptIndexError(Exception):
    pass


class ScriptManagerError(Exception):
    pass


class ScriptIndex:
    log: logger.Logger
    config: dict
    index: list
    path: str = 'scripts'

    def __init__(self):
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
        self.log = logger.Logger('SI')
        self.config = self.load_config()
        self.log.info(codes.Code(20601))
        self.index = []

    def __repr__(self) -> str:
        return f'ScriptIndex({len(self.index)} script(s) indexed, path="{self.path}")'

    def del_(self):
        self.dump_config()
        self.log.info(codes.Code(20602))
        del self

    def load_config(self) -> dict:
        config: dict = {'ignore': {'folders': [], 'scripts': []}}
        if os.path.isfile(self.path + '/config.yaml'):
            raw = yaml.safe_load(open(self.path + '/config.yaml'))
            if isinstance(raw, dict):
                if 'ignore' in raw and isinstance(raw['ignore'], dict):
                    if 'folders' in raw['ignore'] and isinstance(raw['ignore']['folders'], list):
                        config['ignore']['folders'] = raw['ignore']['folders']
                    else:
                        config['ignore']['folders'] = []
                    if 'scripts' in raw['ignore'] and isinstance(raw['ignore']['scripts'], list):
                        config['ignore']['scripts'] = raw['ignore']['scripts']
                    else:
                        config['ignore']['scripts'] = []
                return config
            else:
                self.log.info(codes.Code(20603))
        return config

    def dump_config(self) -> True:
        if os.path.isfile(self.path + '/config.yaml'):
            config_: dict = yaml.safe_load(open(self.path + '/config.yaml'))
            if isinstance(config_, dict):
                if json.dumps(config_) != json.dumps(self.config):
                    yaml.safe_dump(self.config, open(self.path + '/config.yaml', 'w+'))
                    return True
                return False
        yaml.safe_dump(self.config, open(self.path + '/config.yaml', 'w+'))
        return True

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
        config: dict = yaml.safe_load(open(file))
        if isinstance(config, dict):
            if 'name' in config:
                if not isinstance(config['name'], str):
                    self.log.debug('"name" must be str in ' + file)
                    good = False
            else:
                self.log.debug('"name" not specified in ' + file)
                good = False
            if 'core' in config:
                if not isinstance(config['core'], str):
                    self.log.debug('"core" must be version(str) in ' + file)
                    good = False
                else:
                    if not self.check_dependency_version_style(config['core']):
                        self.log.debug('Wrong style of version in "core" in ' + file)
                        good = False
            else:
                self.log.debug('"core" not specified in ' + file)
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
            if 'important' in config:
                if not isinstance(config['important'], bool):
                    self.log.debug('"important" must be bool in ' + file)
                    good = False
            else:
                self.log.debug('"important" not specified in ' + file)
                good = False
            if good:
                return True
        return False

    @staticmethod
    def get_config(file: str) -> dict:
        raw: dict = yaml.safe_load(open(file))
        config: dict = {
            'name': raw['name'],
            'core': raw['core'],
            'version': raw['version'],
            'important': raw['important'],
            '_path': os.path.abspath(os.path.dirname(file))
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
        folders: tuple = tuple(
            i for i in next(os.walk(self.path))[1] if i not in self.config['ignore']['folders']
        )  # Get all script folders
        names: List[str] = []
        self.index.clear()
        for i in folders:
            file = f'{self.path}/{i}/config.yaml'
            if not os.path.isfile(file):  # Check for config.yaml
                self.log.info(codes.Code(20604, f'"{i}"/'))
                continue
            if not self.check_config(file):  # Check for config.yaml
                self.log.info(codes.Code(20605, f'"{i}"/'))
                continue
            config: dict = self.get_config(file)  # Get config from config.yaml
            if not self.check_dependency_version(config['core']):
                self.log.info(codes.Code(20606, f'"{i}"/'))
                continue
            if 'ignore' in self.config and config['name'] in self.config['ignore']['scripts']:
                self.log.info(codes.Code(20607, f'"{i}"/'))
                continue
            if config['name'] in names:
                self.log.info(codes.Code(20608, f'"{i}"/'))
                continue
            names.append(config['name'])  # Save name to check on uniqueness
            self.index.append(config)
        self.log.info(codes.Code(20609, str(len(self.index))))
        return 20609

    def ignore(self, name: str, folder: bool = False) -> bool:
        if isinstance(name, str):
            if folder:
                if name not in self.config['ignore']['folders']:
                    self.config['ignore']['folders'].append(name)
                    return True
            else:
                if name not in self.config['ignore']['scripts']:
                    self.config['ignore']['scripts'].append(name)
                    return True
        return False

    def watch(self, name: str, folder: bool = False) -> bool:
        if isinstance(name, str):
            if folder:
                if name not in self.config['ignore']['folders']:
                    self.config['ignore']['folders'].remove(name)
                    return True
            else:
                if name not in self.config['ignore']['scripts']:
                    self.config['ignore']['scripts'].remove(name)
                    return True
        return False

    def get_script(self, name: str) -> dict:
        try:
            return next(i for i in self.index if i['name'] == name)
        except StopIteration:
            return {}


class EventHandler:  # TODO: unload protection
    log: logger.Logger
    executors: Dict[str, api.EventsExecutor]

    def __init__(self):
        self.log = logger.Logger('EventHandler')
        self.executors = {}

    def add(self, name: str, executor: api.EventsExecutor) -> bool:
        self.executors[name] = executor(name, logger.Logger('EventsExecutor/' + name))
        return True

    def delete(self, name: str) -> bool:
        if name in self.executors:
            del self.executors[name]
        return True

    def exec(self, event: str, args: tuple):
        for i in self.executors.__iter__():
            try:
                getattr(self.executors[i], event)(*args)
            except Exception as e:
                if storage.main.production:
                    self.log.error(f'{e.__str__()} while executing {event} with "{i}" executor')
                else:
                    self.log.fatal(e)

    def monitor_starting(self) -> None:
        self.exec('e_monitor_starting', ())

    def monitor_started(self) -> None:
        self.exec('e_monitor_started', ())

    def monitor_stopping(self) -> None:
        self.exec('e_monitor_stopping', ())

    def monitor_stopped(self) -> None:
        self.exec('e_monitor_stopped', ())

    def alert(self, code: codes.Code, thread: str) -> None:
        self.exec('e_alert', (code, thread))

    def success_status(self, status: api.SSuccess) -> None:
        self.exec('e_success_status', (status,))

    def fail_status(self, status: api.SFail) -> None:
        self.exec('e_fail_status', (status,))


class ScriptManager:
    log: logger.Logger
    lock: threading.Lock
    index: ScriptIndex
    scripts: Dict[str, dict]
    parsers: Dict[str, api.Parser]
    event_handler: EventHandler

    def __init__(self):
        self.log = logger.Logger('SM')
        self.lock = threading.Lock()
        self.index = ScriptIndex()
        self.scripts = {}
        self.parsers = {}
        self.event_handler = EventHandler()

    def del_(self):
        del self.parsers
        del self.event_handler
        del self.scripts
        del self.index
        del self

    def _destroy(self, name: str) -> bool:  # Memory leak patch
        try:
            del sys.modules[name]
            return True
        except KeyError:
            self.log.warn(codes.Code(30501, name))
            return False

    def _scan(self, script: dict, module: ModuleType) -> bool:
        success = False
        if 'Parser' in module.__dict__ and issubclass(getattr(module, 'Parser'), api.Parser):
            if script['keep']:
                self.parsers[script['name']] = getattr(module, 'Parser')(
                    script['name'],
                    logger.Logger('Parser/' + script['name'])
                )
            else:
                self.parsers[script['name']] = getattr(module, 'Parser')
            success = True
        if 'EventsExecutor' in module.__dict__ and issubclass(getattr(module, 'EventsExecutor'), api.EventsExecutor):
            self.event_handler.add(script['name'], getattr(module, 'EventsExecutor'))
            success = True
        return success

    def _load(self, script) -> bool:
        if isinstance(script, str):
            script: dict = self.index.get_script(script)
        module: ModuleType = importlib.import_module(os.path.relpath(script['_path']).replace('/', '.'))
        self._destroy(module.__name__)
        if success := self._scan(script, module):
            self.scripts[script['name']] = {k: v for k, v in script.items()}
            self.scripts[script['name']]['_hash'] = dirhash(script['_path'], 'sha1')
            self.scripts[script['name']]['_errors'] = 0
            del self.scripts[script['name']]['name']
        else:
            self.log.warn(codes.Code(30502, script['name']))
        return success

    def _unload(self, name: str) -> bool:
        if self.scripts[name]['can_be_unloaded']:
            self.event_handler.delete(name)
            if name in self.parsers:
                del self.parsers[name]
            del self.scripts[name]
            return True
        else:
            self.log.warn(codes.Code(30503, name))
            return False

    def _reload(self, name: str) -> Tuple[bool, int]:
        if self.scripts[name]['can_be_unloaded']:
            script: dict = self.scripts[name]
            script['name'] = name
            script['_hash'] = dirhash(script['_path'], 'sha1')
            if script:
                module: ModuleType = importlib.import_module(os.path.relpath(script['_path']).replace('/', '.'))
                self._destroy(module.__name__)
                if self._scan(script, module):
                    return True, 0
                else:
                    self.log.warn(codes.Code(30502, name))
                    return False, 30502
            else:
                self.log.warn(codes.Code(30504, name))
                return False, 30504
        else:
            self.log.warn(codes.Code(30505, name))
            return False, 30505

    def load(self, script: str) -> Tuple[bool, int]:
        with self.lock:
            script: dict = self.index.get_script(script)
            if script and script['name'] in self.scripts:
                self.log.warn(codes.Code(30506, script['name']))
                return True, 30506
            elif script and script['name'] not in self.scripts:
                try:
                    if self._load(script):
                        self.log.info(codes.Code(20501, script['name']))
                        return True, 20501
                except ImportError as e:
                    if storage.main.production:
                        self.log.error(codes.Code(40501, script['name']))
                    else:
                        self.log.fatal(e)
            else:
                self.log.error(codes.Code(40502, script['name']))
            return False, 40502

    def unload(self, name: str) -> Tuple[bool, int]:
        with self.lock:
            if name in self.scripts:
                if self._unload(name):
                    self.log.info(codes.Code(20502, name))
                    return True, 20502
            else:
                self.log.error(codes.Code(40503, name))
            return False, 40503

    def reload(self, name: str) -> Tuple[bool, int]:
        with self.lock:
            if name in self.scripts:
                if (result := self._reload(name))[0]:
                    self.log.info(codes.Code(20503, name))
                    return True, 20503
                else:
                    return False, result[1]
            else:
                self.log.error(codes.Code(40504, name))
            return False, 40504

    def load_all(self) -> Tuple[bool, int]:
        with self.lock:
            self.log.info(codes.Code(20504))
            for i in set().union(self.scripts, [i['name'] for i in self.index.index]):  # TODO: Fix here
                try:
                    if self._load(i):
                        self.log.info(codes.Code(20501, i))
                except ImportError as e:
                    if storage.main.production:
                        self.log.error(codes.Code(40501, i))
                    else:
                        self.log.fatal(e)
            self.log.info(codes.Code(20505))
            return True, 20505

    def unload_all(self) -> Tuple[bool, int]:
        with self.lock:
            self.log.info(codes.Code(20506))
            for i in self.scripts.copy():
                if self._unload(i):
                    self.log.info(codes.Code(20502, i))
            self.log.info(codes.Code(20507))
            return True, 20507

    def reload_all(self) -> Tuple[bool, int]:
        with self.lock:
            self.log.info(codes.Code(20508))
            for i in self.scripts:
                if self._reload(i):
                    self.log.info(codes.Code(20503, i))
            self.log.info(codes.Code(20509))
            return True, 20509

    def hash(self) -> Dict[str, str]:
        return {i: self.scripts[i]['_hash'] for i in self.parsers if i in self.scripts}

    def get_parser(self, name: str):
        try:
            if self.scripts[name]['keep']:
                return self.parsers[name]
            else:
                return self.parsers[name](name, logger.Logger(f'parser/{name}'))
        except KeyError:
            raise ScriptManagerError(f'Script "{name}" not loaded')

    def execute_parser(self, name: str, func: str, args: tuple) -> Tuple[bool, Any]:
        try:
            return True, getattr(self.get_parser(name), func)(*args)
        except ScriptManagerError as e:
            raise e
        except Exception as e:
            try:
                if self.scripts[name]['important']:
                    if self.scripts[name]['max-errors'] == -1 or \
                            self.scripts[name]['errors'] < self.scripts[name]['max-errors']:
                        self.scripts[name]['errors'] += 1
                    else:
                        self.log.warn(codes.Code(30507, name))
                        self.unload(name)
                    return False, None
                else:
                    raise e
            except KeyError:
                raise ScriptManagerError(f'Script "{name}" not loaded')
