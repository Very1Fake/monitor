from abc import ABC
from os import makedirs
from os.path import abspath, isdir, isfile
from typing import TextIO

from src.store import main


class Storage(ABC):
    __slots__ = 'path'
    path: str

    def check_path(self) -> None:
        if not isdir(self.path):
            makedirs(self.path, mode=0o750)

    def check(self, name: str) -> bool:
        self.check_path()
        return isfile(self.path + '/' + name)

    def file(
            self,
            name: str,
            mode: str = 'r',
            buffering=-1,
            encoding=None,
            errors=None,
            newline=None,
            closefd=True,
            opener=None
    ) -> TextIO:
        self.check_path()
        return open(self.path + '/' + name, mode,
                    buffering, encoding, errors, newline, closefd, opener)


class KernelStorage(Storage):
    """ Main storage where monitor's configs and keys are stored """

    def __init__(self):
        self.path = abspath(main.mount.rstrip('/') + '/kernel')


class LogStorage(Storage):
    """ A storage where .log files are stored """

    def __init__(self):
        self.path = abspath(main.mount.rstrip('/') + '/logs')


class CacheStorage(Storage):
    """
    WILL BE DEPRECATED SOON

    A storage where cache files are stored (e.g. HashStorage dumps)
    """

    def __init__(self):
        self.path = abspath(main.mount.rstrip('/') + '/cache')


class ReportStorage(Storage):
    """
    WILL BE DEPRECATED SOON

    A storage where analytics reports are stored
    """

    def __init__(self):
        self.path = abspath(main.mount.rstrip('/') + '/report')


class ScriptStorage(Storage):
    """ A storage where scripts can store their own files (e.g. secret.yaml) """

    def __init__(self, script: str):
        self.path = abspath(f'{main.mount.rstrip("/")}/scripts/{script}')
