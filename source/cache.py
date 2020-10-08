"""Cache tools

:platform: Unix

"""

import os
import sqlite3
import threading
import time
from typing import Optional

import ujson

from . import storage
from . import tools
from .api import Size, Sizes, Item, ItemType


class UniquenessError(Exception):
    pass


def check() -> None:
    """Create cache/ if not exists

    Returns:
        None
    """
    if not os.path.isdir(storage.cache.path):
        os.makedirs(storage.cache.path)


class HashStorage:
    __db: sqlite3.Connection = sqlite3.connect(':memory:', 1, check_same_thread=False)
    __db.execute('PRAGMA foreign_keys = ON')

    _lock: threading.Lock = threading.RLock()

    @classmethod
    def check(cls) -> None:
        """Check database tables and create ones that not exists

        Returns:
            None
        """
        with cls._lock, cls.__db as c:
            c.executescript('''CREATE TABLE IF NOT EXISTS Targets (hash BLOB NOT NULL PRIMARY KEY, time REAL NOT NULL);
CREATE TABLE IF NOT EXISTS AnnouncedItems (hash BLOB NOT NULL PRIMARY KEY, time REAL NOT NULL);
CREATE TABLE IF NOT EXISTS Items (id INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE , time REAL NOT NULL);
CREATE TABLE IF NOT EXISTS RestockItems (id INTEGER PRIMARY KEY REFERENCES Items(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS Sizes (item INTEGER PRIMARY KEY NOT NULL REFERENCES RestockItems(id) ON DELETE CASCADE,
type INTEGER NOT NULL, list TEXT NOT NULL);''')

    @classmethod
    def _clear(cls) -> None:
        """Drop all tables from database

        Returns:
            None
        """
        with cls._lock, cls.__db as c:
            for i in c.execute('SELECT name FROM sqlite_master WHERE type="table"').fetchall():
                c.execute(f'DROP TABLE {i[0]}')

    @classmethod
    def defrag(cls) -> None:
        """Free space from database

        Returns:
            None
        """
        with cls._lock, cls.__db as c:
            c.execute('VACUUM')

    @classmethod
    def unload(cls) -> None:
        with cls._lock, cls.__db as c:
            check()
            c.backup(sqlite3.connect(f'{storage.cache.path}/hash.db'))

    @classmethod
    def load(cls) -> bool:
        """Load database from ``cache/hash.db``

        Returns:
            :obj:`bool`: ``True`` if successful load, otherwise ``False`` if ``cache/hash.db`` not exists
        """
        with cls._lock, cls.__db as c:
            check()
            if os.path.isfile(f'{storage.cache.path}/hash.db'):
                cls._clear()

                sqlite3.connect(f'{storage.cache.path}/hash.db').backup(c)
                return True
            else:
                return False

    @classmethod
    def dump(cls) -> None:
        """Create SQLite dump file in ``cache/``

        Returns:
            None
        """
        with cls._lock, cls.__db as c:
            check()
            f = open(f'{storage.cache.path}/hash_{tools.get_time(name=True)}.sql', 'w+')
            for i in c.iterdump():
                f.write(i + '\n')
                f.flush()

    @classmethod
    def backup(cls) -> None:
        """Create backup file of database in ``cache/``

        Returns:
            None
        """
        with cls._lock, cls.__db as c:
            check()
            c.backup(sqlite3.connect(f'{storage.cache.path}/hash_{tools.get_time(name=True)}.db.backup'))

    @classmethod
    def delete(cls, table: str) -> None:
        """Drop table from database

        Args:
            table: Table name

        Returns:
            None

        Raises:
            TypeError: If ``table`` type not int
        """
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
        """Delete expired rows from all tables

        Returns:
            None
        """
        with cls._lock, cls.__db as c:
            cls.check()
            c.execute('DELETE FROM Targets WHERE time<=?', (time.time() - storage.cache.target_time,))
            c.execute('DELETE FROM AnnouncedItems WHERE time<=?', (time.time() - storage.cache.item_time,))
            c.execute('DELETE FROM Items WHERE time<=?', (time.time() - storage.cache.item_time,))

    @classmethod
    def add_target(cls, hash_: bytes) -> None:
        """Add target hash to database

        Args:
            hash_: Target hash

        Returns:
            None

        Raises:
            TypeError: If ``hash_`` type not bytes
        """
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
        """Check :class:`source.api.Target` (only for subclasses) existence at database by its hash

        Args:
            hash_: Target hash

        Returns:
            :obj:`bool`: ``True`` if target not found, otherwise ``False``

        Raises:
            TypeError: If ``hash_`` type not bytes
        """
        if not isinstance(hash_, bytes):
            raise TypeError('hash_ must be bytes')

        with cls._lock, cls.__db as c:
            cls.check()

            return not c.execute('SELECT time FROM Targets WHERE hash=?', (hash_,)).fetchone()

    @classmethod
    def add_announced_item(cls, hash_: bytes) -> None:
        """Add :class:`source.api.IAnnounce` hash to database

        Args:
            hash_: Announced item hash

        Returns:
            None

        Raises:
            TypeError: If ``hash_`` type not bytes
        """
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
        """Add :class:`source.api.IRelease` or :class:`source.api.IRestock` hash to database

        Note:
            If ``restock`` is ``True`` item will be also saved to ``RestockItems`` table and sizes of item will be saved
            to ``Sizes`` table

        Args:
            item: Item that need to be save
            restock: Optional bool, defaults to ``False``. If ``True`` ``item`` will be saved to ``RestockItems``
                table, otherwise if ``False`` will be saved to ``Items`` table

        Returns:
            :obj:`int`: Item id (can be used for :func:`check_item_id` and :func:`get_size`)

        Raises:
            TypeError: If one of the arguments has wrong type
            :class:`UniquenessError`: If item hash already in database
        """
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
                        (item.sizes.type, ujson.dumps(item.sizes.export(), separators=(',', ':')))
                    )
                return id_
            except sqlite3.IntegrityError as e:
                if str(e).startswith('UNIQUE'):
                    raise UniquenessError
                else:
                    raise e

    @classmethod
    def remove_item(cls, hash_: bytes) -> None:
        """Remove :class:`source.api.IRelease` hash from database

        Note:
            Works only for :class:`source.api.IRelease`!!!

        Args:
            hash_: Item hash

        Returns:
            None

        Raises:
            TypeError: If ``hash_`` type not bytes
        """
        if not isinstance(hash_, bytes):
            raise TypeError('hash_ must be bytes')

        with cls._lock, cls.__db as c:
            cls.check()

            c.execute('DELETE FROM Items WHERE hash=?', (hash_,))

    @classmethod
    def check_item(cls, hash_: bytes, announced: bool = False) -> bool:
        """Check :class:`source.api.IRelease` or :class:`source.api.IRestock` existence at database by its hash
        
        Args:
            hash_: Item hash
            announced: Optional bool, defaults to ``False``. If ``True`` hash will be checked from ``AnnouncedItems``
                table, otherwise if ``False`` from ``Items`` table

        Returns:
            :obj:`bool`: ``True`` if target found, otherwise ``False``

        Raises:
            TypeError: If one of the arguments has wrong type
        """
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
        """Check if :class:`source.api.IRelease` or :class:`source.api.IRestock` with id exists

        Args:
            id_: Id of item to check
            restock: Optional bool, defaults to ``True``. If ``True`` id will be checked from ``AnnouncedItems``
                table, otherwise if ``False`` from ``Items`` table

        Returns:
            :obj:`bool`: ``True`` if item not exists, otherwise ``False``

        Raises:
            TypeError: If one of the arguments has wrong type
        """
        if not isinstance(id_, int):
            raise TypeError('id_ must be int')

        if not isinstance(restock, bool):
            raise TypeError('restock must be bool')

        with cls._lock, cls.__db as c:
            cls.check()

            return not c.execute(f'SELECT id FROM {"RestockItems" if restock else "Items"} WHERE id={id_}').fetchone()

    @classmethod
    def update_size(cls, id_: int, sizes: Sizes) -> None:
        """Update sizes of item

        Notes:
            The item must already has sizes

        Args:
            id_: Item id that has sizes
            sizes: :class:`source.api.Sizes` object that will be saved

        Returns:
            None

        Raises:
            TypeError: If one of the arguments has wrong type
            IndexError: If sizes for item not found
        """
        if not isinstance(id_, int):
            raise TypeError('id_ must be int')

        if not isinstance(sizes, Sizes):
            raise TypeError('sizes must be api.Sizes')

        with cls._lock, cls.__db as c:
            cls.check()

            if c.execute(f'SELECT item FROM sizes WHERE item={id_}').fetchone():
                c.execute(
                    f'UPDATE sizes SET type=?, list=? WHERE item={id_}',
                    (sizes.type, ujson.dumps(sizes.export(), separators=(',', ':')))
                )
            else:
                raise IndexError(f'Sizes for this item ({id_}) not found')

    @classmethod
    def get_size(cls, id_: int) -> Optional[Sizes]:
        """Get sizes of item

        Args:
            id_: Id of item that has sizes

        Returns:
            :class:`source.api.Sizes`

        Raises:
            TypeError: If ``id_`` type not int
            IndexError: If sizes not found for specified ``id_``
        """
        if not isinstance(id_, int):
            raise TypeError('id_ must be int')

        with cls._lock, cls.__db as c:
            cls.check()

            if sizes := c.execute(f'SELECT type, list FROM sizes WHERE item={id_}').fetchone():
                return Sizes(sizes[0], (Size(*i) for i in ujson.loads(sizes[1])))
            else:
                raise IndexError(f'Sizes for this item ({id_}) not found')

    @classmethod
    def stats(cls) -> dict:
        """Get items count of each tables of HashStorage

        Returns:
            :obj:`dict`

            Example output::

                {
                    'targets': 8,
                    'announced_items': 3,
                    'items': 6,
                    'restock_items': 0,
                    'sizes': 0
                }
        """

        with cls._lock, cls.__db as c:
            cls.check()

            return dict(zip(
                ('targets', 'announced_items', 'items', 'restock_items', 'sizes'),
                c.execute('SELECT (SELECT COUNT(hash) FROM Targets), (SELECT COUNT(hash) FROM AnnouncedItems),'
                          '(SELECT COUNT(id) FROM Items), (SELECT COUNT(id) FROM RestockItems),'
                          '(SELECT COUNT(item) FROM Sizes)').fetchone())
            )
