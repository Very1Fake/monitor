import collections
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Tuple, Generator, Iterator
import time


class LibraryError(Exception):
    pass


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    content: Any = field(compare=False)


class Schedule(dict):
    def __setitem__(self, time_: float, value):
        if isinstance(time_, float) or isinstance(time_, int):
            super().__setitem__(round(time_, 7), value)
        else:
            raise KeyError('Key must be int or float')

    def get_slice(self, time_) -> List[Tuple[float, Any]]:
        return [i for i in self.items() if i[0] <= time_]

    def get_slice_gen(self, time_) -> Generator[Tuple[float, Any], Any, None]:
        return (i for i in self.items() if i[0] <= time_)

    def pop_slice(self, time_) -> List[Tuple[str, dict]]:
        result: List[Tuple[str, dict]] = [i for i in super().items() if i[0] <= time_]
        for i in result:
            self.__delitem__(i[0])
        return result

    def pop_item(self, time_) -> None:
        if isinstance(time_, (float, int)):
            self.__delitem__(time_)
        elif isinstance(time_, (tuple, list)):
            for i in time_:
                if isinstance(i, float) or isinstance(i, int):
                    self.__delitem__(i)
                else:
                    continue
        else:
            raise ValueError('Item must be float or tuple[float]')

    def del_slice(self, time_) -> None:
        for i in tuple(k for k in super().__iter__() if k <= time_):
            self.__delitem__(i)


class UniqueSchedule(Schedule):
    def __init__(self, length: int = 1024):
        super().__init__()
        self.hashes: collections.deque = collections.deque(maxlen=length)

    def __setitem__(self, key, value):
        if not hash(value) in self.hashes:
            self.hashes.append(hash(value))
        else:
            raise IndexError('Non-unique value')
        super().__setitem__(key, value)

    def pop_slice(self, key) -> List[Tuple[str, dict]]:
        result: List[Tuple[str, dict]] = [i for i in super().items() if i[0] <= key]
        for i in result:
            self.hashes.remove(self.__getitem__(i[0]))
            self.__delitem__(i[0])
        return result

    def pop_item(self, key) -> None:
        if isinstance(key, float) or isinstance(key, float):
            self.__delitem__(key)
        elif isinstance(key, tuple) or isinstance(key, list):
            for i in key:
                if isinstance(i, float) or isinstance(i, int):
                    self.hashes.remove(hash(self.__getitem__(i)))
                    self.__delitem__(i)
                else:
                    continue
        else:
            raise ValueError('Item must be float or tuple[float]')


time_format: str = "%Y-%m-%d %H:%M:%S"


def get_time(global_: bool = True) -> str:
    if global_:
        time_ = datetime.utcnow()
    else:
        time_ = datetime.now()
    return datetime.strftime(time_, time_format)


def smart_gen(time_: float, length: int, scatter: int = 1) -> Iterator[float]:
    if length < 0:
        raise ValueError('length cannot be less than 0')
    if scatter < 1:
        raise ValueError('scatter cannot be less than 1')

    for i in range(length - 1, -1, -scatter):
        yield time_ - 2 ** i
    else:
        yield time_


def smart_extractor(gen: Iterator[float], now: float = None) -> float:
    if not now:
        now = time
    for i in gen:
        if i > now:
            return i
