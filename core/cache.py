import os

from . import library
from . import storage


# TODO: Compress success hashes


def check():
    if not os.path.isdir(storage.cache_folder):
        os.makedirs(storage.cache_folder)


def load_success_hashes(force: bool = True) -> library.Schedule:
    check()
    hashes: library.Schedule = library.Schedule()
    if os.path.isfile(storage.cache_folder + '/success_hashes.cache'):
        with open(storage.cache_folder + '/success_hashes.cache', 'r+') as file:
            for i in file.readlines():
                j = i.split(':')
                try:
                    if 1 < j.__len__() < 3 and j[1] != '\n':
                        hashes[float(j[0])] = int(j[1][:-1], 16)
                except ValueError:
                    continue
            if force:
                file.seek(0)
                file.truncate()
    return hashes


def dump_success_hashes(hashes: library.Schedule) -> bool:
    check()
    with open(storage.cache_folder + '/success_hashes.cache', 'w+') as file:
        for i in hashes:
            file.write(f'{i}:{hex(hashes[i])[2:]}\n')
            file.flush()
    return True
