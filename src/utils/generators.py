from abc import abstractmethod, ABC
from time import time
from typing import Iterator, TypeVar


# Smart generator classes


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
            now = time()

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
            now = time()

        for i in self.generator():
            if i > now:
                return i
        else:
            return self.time
