import json
import os
import sqlite3
import threading
import time
from typing import Union

from . import storage
from . import tools
from .api import Size, Sizes, Item, ItemType


class UniquenessError(Exception):
    pass


def check():
    if not os.path.isdir(storage.cache.path):
        os.makedirs(storage.cache.path)


class HashStorage:
    __db: sqlite3.Connection = sqlite3.connect(':memory:', 1, check_same_thread=False)
    _lock: threading.Lock = threading.RLock()

    __db.execute('PRAGMA foreign_keys = ON')

    @classmethod
    def check(cls) -> None:
        with cls._lock, cls.__db as c:
            c.executescript('''CREATE TABLE IF NOT EXISTS Targets (hash BLOB NOT NULL PRIMARY KEY, time REAL NOT NULL);
CREATE TABLE IF NOT EXISTS AnnouncedItems (hash BLOB NOT NULL PRIMARY KEY, time REAL NOT NULL);
CREATE TABLE IF NOT EXISTS Items (id INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE , time REAL NOT NULL);
CREATE TABLE IF NOT EXISTS RestockItems (id INTEGER PRIMARY KEY REFERENCES Items(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS Sizes (item INTEGER PRIMARY KEY NOT NULL REFERENCES RestockItems(id) ON DELETE CASCADE, type INTEGER NOT NULL, list TEXT NOT NULL);
''')

    @classmethod
    def _clear(cls) -> None:
        with cls._lock, cls.__db as c:
            for i in c.execute('SELECT name FROM sqlite_master WHERE type="table"').fetchall():
                c.execute(f'DROP TABLE {i[0]}')

    @classmethod
    def defrag(cls) -> None:
        with cls._lock, cls.__db as c:
            c.execute('VACUUM')

    @classmethod
    def unload(cls) -> None:
        with cls._lock, cls.__db as c:
            check()
            cls.__db.backup(sqlite3.connect(f'{storage.cache.path}/hash.db'))

    @classmethod
    def load(cls) -> bool:
        with cls._lock, cls.__db as c:
            check()
            if os.path.isfile(f'{storage.cache.path}/hash.db'):
                cls._clear()

                sqlite3.connect(f'{storage.cache.path}/hash.db').backup(cls.__db)
                return True
            else:
                return False

    @classmethod
    def dump(cls) -> None:
        with cls._lock, cls.__db as c:
            check()
            f = open(f'{storage.cache.path}/hash_{tools.get_time(name=True)}.sql', 'w+')
            for i in c.iterdump():
                f.write(i + '\n')
                f.flush()

    @classmethod
    def backup(cls) -> None:
        with cls._lock, cls.__db as c:
            check()
            c.backup(sqlite3.connect(f'{storage.cache.path}/hash_{tools.get_time(name=True)}.db.backup'))

    @classmethod
    def delete(cls, table: str) -> None:
        if isinstance(table, str):
            with cls._lock, cls.__db as c:
                try:
                    c.execute(f'DROP TABLE {table}')
                except sqlite3.OperationalError:
                    pass
        else:
            raise TypeError('table must be str')

    @classmethod
    def cleanup(cls) -> None:
        with cls._lock, cls.__db as c:
            cls.check()
            c.execute('DELETE FROM Targets WHERE time<=?', (time.time() - storage.cache.target_time,))
            c.execute('DELETE FROM AnnouncedItems WHERE time<=?', (time.time() - storage.cache.item_time,))
            c.execute('DELETE FROM Items WHERE time<=?', (time.time() - storage.cache.item_time,))

    @classmethod
    def contains(cls, hash_: bytes, item: bool = True) -> bool:
        if not isinstance(hash_, bytes):
            raise TypeError('hash_ must be bytes or _blake2.blake2s')

        if not isinstance(item, bool):
            raise TypeError('item must be bool')

        with cls._lock, cls.__db as c:
            cls.check()

            if c.execute(f'SELECT * FROM {"items" if item else "targets"} WHERE hash=?', (hash_,)).fetchone():
                return True
            else:
                return False

    @classmethod
    def add_target(cls, hash_: bytes) -> None:
        if not isinstance(hash_, bytes):
            raise TypeError('hash_ must be bytes')

        with cls._lock, cls.__db as c:
            cls.check()

            try:
                c.execute('INSERT INTO Targets VALUES (?, ?)', (hash_, time.time()))
            except sqlite3.IntegrityError as e:
                if str(e).startswith('UNIQUE'):
                    raise UniquenessError
                else:
                    raise e

    @classmethod
    def check_target(cls, hash_: bytes) -> bool:
        if not isinstance(hash_, bytes):
            raise TypeError('hash_ must be bytes')

        with cls._lock, cls.__db as c:
            cls.check()

            return not c.execute('SELECT time FROM Targets WHERE hash=?', (hash_,)).fetchone()

    @classmethod
    def add_announced_item(cls, hash_: bytes) -> None:
        if not isinstance(hash_, bytes):
            raise TypeError('hash_ must be bytes')

        with cls._lock, cls.__db as c:
            cls.check()

            try:
                c.execute('INSERT INTO AnnouncedItems VALUES (?, ?)', (hash_, time.time()))
            except sqlite3.IntegrityError as e:
                if str(e).startswith('UNIQUE'):
                    raise UniquenessError
                else:
                    raise e

    @classmethod
    def add_item(cls, item: ItemType, restock: bool = False) -> int:
        if not issubclass(type(item), Item):
            raise TypeError('item must be Item')

        if not isinstance(restock, bool):
            raise TypeError('restock must be bool')

        with cls._lock, cls.__db as c:
            cls.check()

            try:
                id_ = c.execute('INSERT INTO Items VALUES (NULL, ?, ?)', (item.hash(4), time.time())).lastrowid
                if restock:
                    c.execute(f'INSERT INTO RestockItems VALUES ({id_})')
                    c.execute(
                        f'INSERT INTO Sizes VALUES ({id_}, ?, ?)',
                        (item.sizes.type, json.dumps(item.sizes.export(), separators=(',', ':')))
                    )
                return id_
            except sqlite3.IntegrityError as e:
                if str(e).startswith('UNIQUE'):
                    raise UniquenessError
                else:
                    raise e

    @classmethod
    def check_item(cls, hash_: bytes, announced: bool = False) -> bool:
        if not isinstance(hash_, bytes):
            raise TypeError('hash_ must be bytes')

        if not isinstance(announced, bool):
            raise TypeError('announced must be bool')

        with cls._lock, cls.__db as c:
            cls.check()

            if c.execute(f'SELECT time FROM {"AnnouncedItems" if announced else "Items"} WHERE hash=?',
                         (hash_,)).fetchone():
                return False
            else:
                return True

    @classmethod
    def check_item_id(cls, id_: int, restock: bool = True) -> bool:
        if not isinstance(id_, int):
            raise TypeError('id_ must be int')

        if not isinstance(restock, bool):
            raise TypeError('restock must be bool')

        with cls._lock, cls.__db as c:
            cls.check()

            return not c.execute(f'SELECT id FROM {"RestockItems" if restock else "Items"} WHERE id={id_}').fetchone()

    @classmethod
    def update_size(cls, id_: int, sizes: Sizes) -> None:
        if not isinstance(id_, int):
            raise TypeError('id_ must be int')

        if not isinstance(sizes, Sizes):
            raise TypeError('sizes must be api.Sizes')

        with cls._lock, cls.__db as c:
            cls.check()

            if c.execute(f'SELECT item FROM sizes WHERE item={id_}').fecthone():
                c.execute(
                    f'UPDATE sizes SET type=?, list=? WHERE item={id_}',
                    (sizes.type, json.dumps(sizes.export(), separators=(',', ':')))
                )
            else:
                raise IndexError(f'Sizes for this item ({id_}) not found')

    @classmethod
    def get_size(cls, id_: int) -> Union[None, Sizes]:
        if not isinstance(id_, int):
            raise TypeError('id_ must be int')

        with cls._lock, cls.__db as c:
            cls.check()

            if sizes := c.execute(f'SELECT type, list FROM sizes WHERE item={id_}').fetchone():
                return Sizes(sizes[0], (Size(*i) for i in json.loads(sizes[1])))
            else:
                raise IndexError(f'Sizes for this item ({id_}) not found')

    @classmethod
    def stats(cls) -> dict:
        return dict(zip(
            ('targets', 'announced_items', 'items', 'restock_items', 'sizes'),
            sqlite3.connect('cache/hash.db').execute('SELECT (SELECT COUNT(hash) FROM Targets),'
                                                     '(SELECT COUNT(hash) FROM AnnouncedItems),'
                                                     '(SELECT COUNT(id) FROM Items),'
                                                     '(SELECT COUNT(id) FROM RestockItems),'
                                                     '(SELECT COUNT(item) FROM Sizes)').fetchone())
        )
