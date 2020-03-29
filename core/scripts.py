import json
import os
import sys
from _hashlib import HASH as Hash
from hashlib import sha1
from importlib import import_module
from types import ModuleType
from typing import Dict, Any, Tuple

import yaml
from checksumdir import dirhash
from packaging.version import Version, InvalidVersion

from . import api
from . import storage
from . import version
from .logger import Logger


# TODO: Status codes


class ScriptIndexError(Exception):
    pass


class ScriptManagerError(Exception):
    pass


class ScriptIndex:
    path = 'scripts'

    def __init__(self):
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
        self.log: Logger = Logger('ScriptIndex')
        self.config = self.load_config()
        self.log.info('Config loaded')
        self.index: list = []

    def __repr__(self) -> str:
        return f'ScriptIndex({len(self.index)} script(s) indexed, path="{self.path}")'

    def del_(self):
        self.dump_config()
        self.log.info('Config dumped')
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
                self.log.debug('Config doens\'t loaded (must be dict)')
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
            if 'name' not in config:
                self.log.debug('"name" not specified in ' + file)
                good = False
            else:
                if not isinstance(config['name'], str):
                    self.log.debug('"name" must be str in ' + file)
                    good = False
            if 'core' not in config:
                self.log.debug('"core" not specified in ' + file)
                good = False
            else:
                if not isinstance(config['core'], str):
                    self.log.debug('"core" must be version(str) in ' + file)
                    good = False
                else:
                    if not self.check_dependency_version_style(config['core']):
                        self.log.debug('Wrong style of version in "core" in ' + file)
                        good = False
            if 'version' not in config:
                self.log.debug('"version" not specified in ' + file)
                good = False
            else:
                if not isinstance(config['version'], str):
                    self.log.debug('"version" must be version(str) in ' + file)
                    good = False
                else:
                    try:
                        Version(config['version'])
                    except InvalidVersion:
                        self.log.debug('Wrong version style of "version" in ' + file)
                        good = False
            if 'important' not in config:
                self.log.debug('"important" not specified in ' + file)
                good = False
            else:
                if not isinstance(config['important'], bool):
                    self.log.debug('"important" must be bool in ' + file)
                    good = False
            if good:
                return True
        return False

    @staticmethod
    def get_config(file: str) -> dict:
        raw: dict = yaml.safe_load(open(file))
        config: dict = {
            'name': raw['name'],
            'path': os.path.dirname(file),
            'core': raw['core'],
            'version': raw['version'],
            'important': raw['important']
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
        config['hash'] = dirhash(os.path.dirname(file), 'sha1')
        return config

    def reindex(self) -> None:
        folders: tuple = tuple(
            i for i in next(os.walk(self.path))[1] if i not in self.config['ignore']['folders']
        )  # Get all script folders
        names: Tuple[str] = ()
        self.index.clear()
        for i in folders:
            file = f'{self.path}/{i}/config.yaml'
            if not os.path.isfile(file):  # Check for config.yaml
                self.log.info(f'Skipping "{i}/" (config not detected)')
                continue
            if not self.check_config(file):  # Check for config.yaml
                self.log.info(f'Skipping "{i}/" (bad config)')
                continue
            config: dict = self.get_config(file)  # Get config from config.yaml
            if not self.check_dependency_version(config['core']):
                self.log.info(f'Skipping "{i}/" (script incompatible with core)')
                continue
            if 'ignore' in self.config and config['name'] in self.config['ignore']['scripts']:
                self.log.info(f'Skipping "{i}/" (ignored by config)')
                continue
            if config['name'] in names:
                self.log.info(f'Skipping "{i}/" (script with this name already indexed)')
                continue
            names += (config['name'],)  # Save name to check on uniqueness
            self.index.append(config)
        self.log.debug(f'{len(self.index)} script(s) indexed')

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
    def __init__(self):
        self.log = Logger('EventHandler')
        self.executors: Dict[str, api.EventsExecutor] = {}

    def add(self, name: str, executor: api.EventsExecutor) -> bool:
        self.executors[name] = executor(name, Logger('EventsExecutor/' + name))
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
                if storage.production:
                    self.log.error(f'{e.__str__()} while executing {event} with "{i}" executor')
                else:
                    self.log.fatal(e)

    def monitor_turning_on(self) -> None:
        self.exec('e_monitor_turning_on', ())

    def monitor_turned_on(self) -> None:
        self.exec('e_monitor_turned_on', ())

    def monitor_turning_off(self) -> None:
        self.exec('e_monitor_turning_off', ())

    def monitor_turned_off(self) -> None:
        self.exec('e_monitor_turned_off', ())

    def error(self, message: str, thread: str) -> None:
        self.exec('e_error', (message, thread))

    def fatal(self, e: Exception, thread: str) -> None:
        self.exec('e_fatal', (e, thread))

    def success_status(self, status: api.SSuccess) -> None:
        self.exec('e_success_status', (status,))

    def fail_status(self, status: api.SFail) -> None:
        self.exec('e_fail_status', (status,))


class ScriptManager:
    def __init__(self):
        self.log = Logger('ScriptManager')
        self.index: ScriptIndex = ScriptIndex()
        self.scripts: dict = {}
        self.parsers: dict = {}
        self.event_handler: EventHandler = EventHandler()

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
            self.log.warn(f'Module "{name}" not loaded')
            return False

    def _scan(self, script: dict, module: ModuleType) -> bool:
        success = False
        if 'Parser' in module.__dict__ and issubclass(getattr(module, 'Parser'), api.Parser):
            if script['keep']:
                self.parsers[script['name']] = getattr(module, 'Parser')(
                    script['name'],
                    Logger('Parser/' + script['name'])
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
        module: ModuleType = import_module(script['path'].replace('/', '.'))
        success = self._scan(script, module)
        self._destroy(module.__name__)
        if success:
            self.scripts[script['name']] = {k: v for k, v in script.items() if k != 'name'}
            self.scripts[script['name']]['errors'] = -1
        else:
            self.log.warn(f'Nothing to import in "{script["name"]}"')
        return success

    def _unload(self, name: str) -> bool:
        if self.scripts[name]['can_be_unloaded']:
            self.event_handler.delete(name)
            if name in self.parsers:
                del self.parsers[name]
            del self.scripts[name]
            return True
        else:
            self.log.warn(f'{name} can\'t be unloaded (unload)')
            return False

    def _reload(self, name: str) -> bool:
        if self.scripts[name]['can_be_unloaded']:
            script: dict = self.scripts[name]
            if script:
                module: ModuleType = import_module(script['path'].replace('/', '.'))
                self._scan(script, module)
                self._destroy(module.__name__)
            else:
                self.log.warn(f'"{name} not indexed but still loaded"')
                return False
            return True
        else:
            self.log.warn(f'{name} can\'t be unloaded (reload)')
            return False

    def load(self, script: str) -> bool:
        script: dict = self.index.get_script(script)
        if script and script['name'] in self.scripts:
            self.log.warn(f'"{script["name"]}" already loaded')
            return True
        elif script and script['name'] not in self.scripts:
            try:
                if self._load(script):
                    self.log.debug(f'"{script["name"]}" loaded')
                    return True
            except ImportError as e:
                if storage.production:
                    self.log.error(f'Can\'t load "{script["name"]}" (Exception: ImportError)')
                else:
                    self.log.fatal(e)
        else:
            self.log.error(f'Can\'t load "{script["name"]}" (script not indexed)')
        return False

    def unload(self, name: str) -> bool:
        if name in self.scripts:
            if self._unload(name):
                self.log.debug(f'"{name}" unloaded')
                return True
        else:
            self.log.error(f'Can\'t unload "{name}" (script isn\'t loaded)')
        return False

    def reload(self, name: str) -> bool:
        if name in self.scripts:
            if self._reload(name):
                self.log.debug(f'"{name}" reloaded')
                return True
        else:
            self.log.error(f'Can\'t reload "{name}" (script isn\'t loaded)')
        return False

    def load_all(self) -> bool:
        self.log.info(f'Loading all scripts')
        for i in set().union(self.scripts, [i['name'] for i in self.index.index]):  # TODO: Fix here
            try:
                if self._load(i):
                    self.log.debug(f'"{i}" loaded')
            except ImportError as e:
                if storage.production:
                    self.log.error(f'Can\'t load "{i}" (Exception: ImportError)')
                else:
                    self.log.fatal(e)
        self.log.info(f'Loading all scripts complete')
        return True

    def unload_all(self) -> bool:
        self.log.info(f'Unloading all scripts')
        for i in self.scripts.copy():
            if self._unload(i):
                self.log.debug(f'"{i}" unloaded')
        self.log.info(f'Unloading all scripts complete')
        return True

    def reload_all(self) -> bool:
        self.log.info(f'Reloading all scripts')
        for i in self.scripts:
            if self._reload(i):
                self.log.debug(f'"{i}" reloaded')
        self.log.info(f'Reloading all scripts complete')
        return True

    def hash(self) -> str:
        hash_: Hash = sha1(b'')
        for i in [i['hash'] for i in self.scripts.values()]:
            hash_.update(i.encode())
        return hash_.hexdigest()

    def get_parser(self, name: str):
        try:
            if self.scripts[name]['keep']:
                return self.parsers[name]
            else:
                return self.parsers[name](name, Logger(f'parser/{name}'))
        except KeyError:
            raise ScriptManagerError(f'Script "{name}" not loaded')

    def execute_parser(self, name: str, func: str, args: tuple) -> Tuple[bool, Any]:
        try:
            return True, getattr(self.get_parser(name), func)(*args)
        except ScriptManagerError as e:
            raise e
        except Exception as e:
            if self.scripts[name]['important']:
                if self.scripts[name]['max-errors'] == -1 or \
                        self.scripts[name]['errors'] < self.scripts[name]['max-errors']:
                    self.scripts[name]['errors'] += 1
                else:
                    self.log.warn(f'Max errors for "{name}" reached unloading...')
                    self.unload(name)
                return False, None
            else:
                raise e
