import collections
import time
from collections import MutableMapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Tuple, Generator, Iterator, Union


class LibraryError(Exception):
    pass


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    content: Any = field(compare=False)


class Schedule(dict):
    def __setitem__(self, time_: Union[float, int], value):
        if isinstance(time_, float) or isinstance(time_, int):
            super().__setitem__(round(time_, 7), value)
        else:
            raise KeyError('Key must be int or float')

    def __getitem__(self, time_: Union[float, int, slice]):
        if isinstance(time_, slice):
            if time_.start:
                return (i for i in self.items() if i[0] >= time_.start)
            elif time_.stop:
                return (i for i in self.items() if i[0] <= time_.stop)
            else:
                return []
        else:
            return super().__getitem__(time_)

    def __delitem__(self, time_: Union[float, int, slice]):
        if isinstance(time_, slice):
            for i in self.key_list(time_):
                super().__delitem__(i)
        else:
            super().__delitem__(time_)

    def pop(self, time_: Union[float, int, slice]) -> List[Any]:
        items = list(self.__getitem__(time_))
        del self[time_]
        return items

    def pop_first(self, time_: slice) -> Any:
        try:
            item = next(self.__getitem__(time_))
            del self[item[0]]
            return item
        except StopIteration:
            return ()

    def key_list(self, time_: slice) -> list:
        if time_.start:
            return list(i for i in self if i >= time_.start)
        elif time_.stop:
            return list(i for i in self if i <= time_.stop)
        else:
            return []


class UniqueSchedule(Schedule):
    def __setitem__(self, time_: Union[float, int], value):
        if value not in self.values():
            super().__setitem__(time_, value)
        else:
            raise IndexError('Non-unique value')


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
