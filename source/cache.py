import os
import sqlite3
import _blake2
import threading
import time
from typing import Union

from . import library
from . import storage


# TODO: Compress success hashes
# TODO: Backup


class UniquenessError(Exception):
    pass


def check():
    if not os.path.isdir(storage.main.cache_path):
        os.makedirs(storage.main.cache_path)


class HashStorage:
    __db: sqlite3.Connection = sqlite3.connect(':memory:', 1, check_same_thread=False)
    _lock: threading.Lock = threading.RLock()

    table: str

    def __init__(self, table: str):
        self.table = table
        self.check_table(table)

    def __contains__(self, item: Union[bytes, _blake2.blake2s]) -> bool:
        return self.contains(item)

    def __setitem__(self, key: Union[float, int], value: Union[bytes, _blake2.blake2s]):
        self.add(value, key)

    @classmethod
    def check_table(cls, name: str) -> None:
        with cls._lock, cls.__db as c:
            c.executescript(f'CREATE TABLE IF NOT EXISTS {name} (time real, hash blob);'
                            f'CREATE UNIQUE INDEX IF NOT EXISTS "{name}_uindex" ON {name}(hash);')

    def _clear(self) -> None:
        with self._lock, self.__db as c:
            for i in c.execute('SELECT `name` FROM sqlite_master WHERE `type`="table"').fetchall():
                c.execute(f'DROP TABLE `{i[0]}`;')

    def unload(self) -> None:
        with self._lock, self.__db as c:
            check()
            f = open(f'{storage.main.cache_path}/hash.sql', 'w+')
            for i in c.iterdump():
                f.write(i + '\n')
                f.flush()

    def load(self) -> bool:
        with self._lock, self.__db as c:
            check()
            if os.path.isfile(f'{storage.main.cache_path}/hash.sql'):
                f = open(f'{storage.main.cache_path}/hash.sql')
                self._clear()

                while line := f.readline():
                    c.execute(line)
                return True
            else:
                return False

    def dump(self) -> None:
        with self._lock, self.__db as c:
            check()
            f = open(f'{storage.main.cache_path}/hash_{library.get_time(name=True)}.sql', 'w+')
            for i in c.iterdump():
                f.write(i + '\n')
                f.flush()

    def backup(self) -> None:
        with self._lock, self.__db as c:
            check()
            c.backup(sqlite3.connect(f'{storage.main.cache_path}/hash_{library.get_time(name=True)}.db.backup'))

    def delete(self, table: str) -> None:
        if isinstance(table, str):
            with self._lock, self.__db as c:
                try:
                    c.execute(f'DROP TABLE {table}')
                except sqlite3.OperationalError:
                    pass
        else:
            raise TypeError('table must be str')

    def cleanup(self, time_: Union[float, int] = None) -> None:
        if time_:
            if not isinstance(time_, (float, int)):
                raise TypeError('time_ must be float or int')
        else:
            time_ = time.time()

        with self._lock, self.__db as c:
            for i in c.execute('SELECT `name` FROM sqlite_master WHERE `type`="table"').fetchall():
                c.execute(f'DELETE FROM {i[0]} WHERE `time`<=?', (time_,))

    def contains(self, hash_: Union[bytes, _blake2.blake2s], table: str = '') -> bool:
        if isinstance(hash_, _blake2.blake2s):
            hash_: bytes = hash_.digest()
        elif isinstance(hash_, bytes):
            pass
        else:
            raise TypeError('hash_ must be bytes or _blake2.blake2s')

        if table:
            if not isinstance(table, str):
                raise TypeError('table must be str')
        else:
            table = self.table

        with self._lock, self.__db as c:
            self.check_table(table)

            if c.execute(f'SELECT * FROM {table} WHERE `hash`=?', (hash_,)).fetchone():
                return True
            else:
                return False

    def add(self, hash_: Union[bytes, _blake2.blake2s], time_: Union[float, int] = None, table: str = '') -> None:
        if isinstance(hash_, _blake2.blake2s):
            hash_: bytes = hash_.digest()
        elif isinstance(hash_, bytes):
            pass
        else:
            raise TypeError('hash_ must be bytes or _blake2.blake2s')

        if time_:
            if not isinstance(time_, (float, int)):
                raise TypeError('time_ must be float or int')
        else:
            time_ = time.time()

        if table:
            if not isinstance(table, str):
                raise TypeError('table must be str')
        else:
            table = self.table

        with self._lock, self.__db as c:
            self.check_table(table)

            try:
                c.execute(f'INSERT INTO {table} VALUES (?, ?)', (time_, hash_))
            except sqlite3.IntegrityError as e:
                if str(e).startswith('UNIQUE'):
                    raise UniquenessError
                else:
                    raise e
