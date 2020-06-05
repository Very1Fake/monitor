import time
from datetime import datetime
from typing import Iterator

time_format: str = "%Y-%m-%d %H:%M:%S"


def get_time(global_: bool = True, name: bool = False) -> str:
    if global_:
        time_ = datetime.utcnow()
    else:
        time_ = datetime.now()
    return datetime.strftime(time_, time_format.replace(' ', '_') if name else time_format)


class SmartGen:
    time: float
    length: int
    scatter: int
    exp: float

    def __init__(self, time_: float, length: int, scatter: int = 1, exp: float = 2.) -> None:
        if isinstance(time_, float):
            self.time = time_
        else:
            raise TypeError('time_ must be float')

        if isinstance(length, int):
            if length < 0:
                raise ValueError('length cannot be less than 0')
            else:
                self.length = length
        else:
            raise TypeError('length must be int')

        if isinstance(scatter, int):
            if scatter < 1:
                raise ValueError('scatter cannot be less than 1')
            else:
                self.scatter = scatter
        else:
            raise TypeError('scatter must be int')

        if isinstance(exp, float):
            if exp <= 1:
                raise ValueError('exp cannot be more than 1')
            else:
                self.exp = exp
        else:
            raise TypeError('exp must be float')

    def __iter__(self) -> Iterator[float]:
        return self.generator()

    def generator(self) -> Iterator[float]:
        for i in range(self.length - 1, -1, -self.scatter):
            yield self.time - self.exp ** i
        else:
            yield self.time

    def extract(self, now: float = None):
        if now:
            if not isinstance(now, float):
                raise TypeError('time_ must be float')
        else:
            now = time.time()

        for i in self:
            if i > now:
                return i
        return self.time
