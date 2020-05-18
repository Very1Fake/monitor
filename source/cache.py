import os

from . import library
from . import storage


# TODO: Compress success hashes
# TODO: Backup


def check():
    if not os.path.isdir(storage.main.cache_path):
        os.makedirs(storage.main.cache_path)


def load_success_hashes(force: bool = True) -> library.Schedule:
    check()
    hashes: library.Schedule = library.Schedule()
    if os.path.isfile(storage.main.cache_path + '/success_hashes.cache'):
        with open(storage.main.cache_path + '/success_hashes.cache', 'r+') as file:
            for i in file.readlines():
                j = i.split(':')
                try:
                    if j.__len__() == 2 and j[1] != '\n':
                        hashes[float(j[0])] = bytes(bytearray.fromhex(j[1][:-1]))
                except ValueError:
                    continue
            if force:
                file.seek(0)
        os.rename(
            storage.main.cache_path + '/success_hashes.cache',
            storage.main.cache_path + '/success_hashes.cache.old'
        )
    return hashes


def dump_success_hashes(hashes: library.Schedule) -> bool:
    check()
    with open(storage.main.cache_path + '/success_hashes.cache', 'w+') as file:
        for i in hashes:
            file.write(f'{i}:{hashes[i].hex()}\n')
            file.flush()
    return True
