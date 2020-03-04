from datetime import datetime
from typing import Any, List, Tuple, Generator

from dataclasses import dataclass, field


class LibraryError(Exception):
    pass


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    content: Any = field(compare=False)


class Schedule(dict):
    def __init__(self):
        super().__init__()

    def __setitem__(self, key: float, value):
        if isinstance(key, float) or isinstance(key, int):
            super().__setitem__(round(key, 5), value)
        else:
            raise KeyError('Key must be int or float')

    def get_slice(self, item) -> List[Tuple[float, Any]]:
        return [i for i in self.items() if i[0] <= item]

    def get_slice_gen(self, item) -> Generator[Tuple[float, Any], Any, None]:
        return (i for i in self.items() if i[0] <= item)

    def pop_slice(self, item) -> List[Tuple[str, dict]]:
        result: List[Tuple[str, dict]] = [i for i in super().items() if i[0] <= item]
        for i in result:
            self.__delitem__(i[0])
        return result

    def pop_item(self, item) -> None:
        if isinstance(item, float) or isinstance(item, float):
            self.__delitem__(item)
        elif isinstance(item, tuple) or isinstance(item, list):
            for i in item:
                if isinstance(i, float) or isinstance(i, int):
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
