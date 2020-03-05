from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Tuple, Generator


class LibraryError(Exception):
    pass


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    content: Any = field(compare=False)


class Schedule(dict):
    def __setitem__(self, key: float, value):
        if isinstance(key, float) or isinstance(key, int):
            super().__setitem__(round(key, 5), value)
        else:
            raise KeyError('Key must be int or float')

    def get_slice(self, key) -> List[Tuple[float, Any]]:
        return [i for i in self.items() if i[0] <= key]

    def get_slice_gen(self, key) -> Generator[Tuple[float, Any], Any, None]:
        return (i for i in self.items() if i[0] <= key)

    def pop_slice(self, key) -> List[Tuple[str, dict]]:
        result: List[Tuple[str, dict]] = [i for i in super().items() if i[0] <= key]
        for i in result:
            self.__delitem__(i[0])
        return result

    def pop_item(self, key) -> None:
        if isinstance(key, float) or isinstance(key, float):
            self.__delitem__(key)
        elif isinstance(key, tuple) or isinstance(key, list):
            for i in key:
                if isinstance(i, float) or isinstance(i, int):
                    self.__delitem__(i)
                else:
                    continue
        else:
            raise ValueError('Item must be float or tuple[float]')


class UniqueSchedule(Schedule):
    def __init__(self, length: int = 1024):
        super().__init__()
        self.hashes: deque = deque(maxlen=length)

    def __setitem__(self, key, value):
        if not hash(value) in self.hashes:
            self.hashes.append(hash(value))
        else:
            raise ValueError('Non-unique value')
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


def get_time(_global: bool = True) -> str:
    if _global:
        _time = datetime.utcnow()
    else:
        _time = datetime.now()
    return datetime.strftime(_time, time_format)
