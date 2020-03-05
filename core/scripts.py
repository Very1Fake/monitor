import os
import sys
from hashlib import sha1
from importlib import import_module
from platform import python_implementation
from types import ModuleType

if python_implementation() == 'CPython':
    from _hashlib import HASH as Hash
else:
    from _hashlib import Hash as Hash
from checksumdir import dirhash
from yaml import safe_load

from . import api
from . import storage
from .logger import Logger


# TODO: Status codes
# TODO: Ignore list to ScriptIndex


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
        self.index: list = []

    def __repr__(self) -> str:
        return f'ScriptIndex({len(self.index)} script(s) indexed, path="{self.path}")'

    @staticmethod
    def check_config(file: str) -> bool:
        config: dict = safe_load(open(file))
        if isinstance(config, dict):
            if 'name' in config and 'version' in config:
                return True
        return False

    @staticmethod
    def get_config(file: str) -> dict:
        raw: dict = safe_load(open(file))
        config: dict = {'name': raw['name'], 'path': os.path.dirname(file), 'version': raw['version']}
        if 'description' in raw:
            config['description'] = raw['description']
        config['hash'] = dirhash(os.path.dirname(file), 'sha1')
        return config

    def reindex(self) -> None:
        folders: list = next(os.walk(self.path))[1]  # Get all script folders
        names: list = []
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
            if config['name'] in names:
                self.log.info(f'Skipping "{i}/" (script with this name already indexed)')
                continue
            names.append(config['name'])  # Save name to check on uniqueness
            self.index.append(config)
        self.log.debug(f'{len(self.index)} script(s) indexed')

    def get_script(self, name: str) -> dict:
        try:
            return next(i for i in self.index if i['name'] == name)
        except StopIteration:
            return {}


class EventHandler:
    def __init__(self):
        self.success_execs: dict = {}
        self.fail_execs: dict = {}
        self.lost_target_execs: dict = {}

    def execute_success(self) -> None:
        ...

    def execute_fail(self) -> None:
        ...

    def execute_lost_target(self) -> None:
        ...

    def delete(self, name: str) -> None:
        if name in self.success_execs:
            del self.success_execs[name]
        if name in self.fail_execs:
            del self.fail_execs[name]
        if name in self.lost_target_execs:
            del self.lost_target_execs[name]


class ScriptManager:
    def __init__(self):
        self.log = Logger('ScriptManager')
        self.index: ScriptIndex = ScriptIndex()
        self.index.reindex()
        self.scripts: dict = {}  # TODO: optimize
        self.parsers: dict = {}
        self.event_handler: EventHandler = EventHandler()

    def get_scripts(self) -> dict:
        return self.scripts

    def get_parsers(self) -> dict:
        return self.parsers

    def get_parser(self, name: str):
        return self.parsers[name]

    def get_index(self) -> ScriptIndex:
        return self.index

    def _destroy(self, name: str) -> bool:  # Memory leak patch
        try:
            del sys.modules[name]
            return True
        except KeyError:
            self.log.warn(f'Module "{name}" not loaded')
            return False

    def _import(self, script) -> bool:
        if isinstance(script, str):
            script: dict = self.index.get_script(script)
        success: bool = False
        module: ModuleType = import_module(script['path'].replace('/', '.'))
        if 'Parser' in module.__dict__ and issubclass(getattr(module, 'Parser'), api.Parser):
            self.parsers[script['name']] = getattr(module, 'Parser')
            success = True
        if 'SuccessEvent' in module.__dict__ and issubclass(getattr(module, 'SuccessEvent'), api.SuccessEvent):
            self.event_handler.success_execs[script['name']] = getattr(module, 'SuccessEvent')
            success = True
        if 'FailEvent' in module.__dict__ and issubclass(getattr(module, 'FailEvent'), api.FailEvent):
            self.event_handler.fail_execs[script['name']] = getattr(module, 'FailEvent')
            success = True
        if 'LostTargetEvent' in module.__dict__ and issubclass(getattr(module, 'LostTargetEvent'), api.LostTargetEvent):
            self.event_handler.lost_target_execs[script['name']] = getattr(module, 'LostTargetEvent')
            success = True
        self._destroy(module.__name__)
        if success:
            self.scripts[script['name']] = {k: v for k, v in script.items() if k != 'name'}
        else:
            self.log.warn(f'Nothing to import in "{script["name"]}"')
        return success

    def _unload(self, name: str) -> bool:  # TODO: Optimize
        self.event_handler.delete(name)
        del self.parsers[name]
        del self.scripts[name]
        return True

    def _reload(self, name: str) -> bool:
        script: dict = self.scripts[name]
        if script:
            module: ModuleType = import_module(script['path'].replace('/', '.'))
            if 'Parser' in module.__dict__ and issubclass(getattr(module, 'Parser'), api.Parser):
                self.parsers[script['name']] = getattr(module, 'Parser')
            else:
                if script['name'] in self.parsers:
                    del self.parsers[script['name']]
            if 'SuccessEvent' in module.__dict__ and issubclass(getattr(module, 'SuccessEvent'), api.SuccessEvent):
                self.event_handler.success_execs[script['name']] = getattr(module, 'SuccessEvent')
            else:
                if script['name'] in self.event_handler.success_execs:
                    del self.event_handler.success_execs[script['name']]
            if 'FailEvent' in module.__dict__ and issubclass(getattr(module, 'FailEvent'), api.FailEvent):
                self.event_handler.fail_execs[script['name']] = getattr(module, 'FailEvent')
            else:
                if script['name'] in self.event_handler.fail_execs:
                    del self.event_handler.fail_execs[script['name']]
            if 'LostTargetEvent' in module.__dict__ and issubclass(getattr(module, 'LostTargetEvent'),
                                                                   api.LostTargetEvent):
                self.event_handler.lost_target_execs[script['name']] = getattr(module, 'LostTargetEvent')
            else:
                if script['name'] in self.event_handler.lost_target_execs:
                    del self.event_handler.lost_target_execs[script['name']]
            self._destroy(module.__name__)
        else:
            self.log.warn(f'"{name} not indexed but still loaded"')
            return False
        return True

    def load(self, script: str) -> bool:
        script: dict = self.index.get_script(script)
        if script and script['name'] in self.scripts:
            self.log.warn(f'"{script["name"]}" already loaded')
            return True
        elif script and script['name'] not in self.scripts:
            try:
                if self._import(script):
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
                if self._import(i):
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
        _hash: Hash = sha1(b'')
        for i in [i['hash'] for i in self.scripts.values()]:
            _hash.update(i.encode())
        return _hash.hexdigest()
