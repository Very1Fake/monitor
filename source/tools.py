import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator, TypeVar

time_format: str = "%Y-%m-%d %H:%M:%S"


def get_time(global_: bool = True, name: bool = False) -> str:
    if global_:
        time_ = datetime.utcnow()
    else:
        time_ = datetime.now()
    return datetime.strftime(time_, time_format.replace(' ', '_') if name else time_format)


class SmartGen(ABC):
    time: float

    def __iter__(self) -> Iterator[float]:
        return self.generator()

    @abstractmethod
    def generator(self) -> Iterator[float]:
        raise NotImplementedError

    @abstractmethod
    def extract(self, now: float = None) -> float:
        raise NotImplementedError


SmartGenType = TypeVar('SmartGenType', bound=SmartGen)


class ExponentialSmart(SmartGen):
    length: int
    scatter: int
    exp: float

    def __init__(self, time_: float, length: int, scatter: int = 1, exp: float = 2.) -> None:
        if isinstance(time_, float):
            self.time = time_
        else:
            raise TypeError('time_ must be float (ExponentialSmart)')

        if isinstance(length, int):
            if length < 0:
                raise ValueError('length cannot be less than 0 (ExponentialSmart)')
            else:
                self.length = length
        else:
            raise TypeError('length must be int (ExponentialSmart)')

        if isinstance(scatter, int):
            if scatter < 1:
                raise ValueError('scatter cannot be less than 1 (ExponentialSmart)')
            else:
                self.scatter = scatter
        else:
            raise TypeError('scatter must be int (ExponentialSmart)')

        if isinstance(exp, float):
            if exp <= 1:
                raise ValueError('exp cannot be more than 1 (ExponentialSmart)')
            else:
                self.exp = exp
        else:
            raise TypeError('exp must be float (ExponentialSmart)')

    def generator(self) -> Iterator[float]:
        for i in range(self.length - 1, -1, -self.scatter):
            yield self.time - self.exp ** i
        else:
            yield self.time

    def extract(self, now: float = None):
        if now:
            if not isinstance(now, float):
                raise TypeError('now must be float (ExponentialSmart)')
        else:
            now = time.time()

        for i in self.generator():
            if i > now:
                return i
        return self.time


class LinearSmart(SmartGen):
    length: int
    step: float

    def __init__(self, time_: float, length: int, step: float):
        if isinstance(time_, (float, int)):
            self.time = time_
        else:
            raise TypeError('time_ must be float or int (LinearSmart)')

        if isinstance(length, int):
            if length < 1:
                raise ValueError('length must be more than 0')
            else:
                self.length = length
        else:
            raise TypeError('length must be int (LinearSmart)')

        if isinstance(step, (float, int)):
            self.step = step
        else:
            raise TypeError('step must be float or int (LinearSmart)')

    def generator(self) -> Iterator[float]:
        for i in range(self.length, -1, -1):
            yield self.time - self.step * i

    def extract(self, now: float = None) -> float:
        if now:
            if not isinstance(now, float):
                raise TypeError('now must be float (LinearSmart)')
        else:
            now = time.time()

        for i in self.generator():
            if i > now:
                return i
        else:
            return self.time
