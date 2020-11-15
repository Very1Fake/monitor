from abc import ABC
from dataclasses import dataclass, field
from hashlib import blake2s
from typing import TypeVar, Union

from src.utils.schedule import Interval, Scheduled, Smart


# Ordinary target base class & subclasses


@dataclass
class Target(ABC):
    name: str
    script: str
    data: Union[str, bytes, int, float, list, tuple, dict] = field(repr=False)
    reused: int = field(init=False, compare=False, default=-1)

    def __post_init__(self):
        if not isinstance(self.name, str):
            raise TypeError('name must be str')
        if not isinstance(self.script, str):
            raise TypeError('scripts must be str')
        if not isinstance(self.data, (str, bytes, int, float, list, tuple, dict)):
            raise TypeError('data must be (str bytes, int, float, list, tuple, dict)')

    def __eq__(self, other):
        if issubclass(type(other), Target):
            if other.name == self.name and other.script == self.script and other.data == self.data:
                return True
            else:
                return False
        else:
            return False

    def reuse(self, max_: int) -> int:
        if max_ > 0:
            if self.reused >= max_:
                self.reused = 0
            else:
                self.reused += 1
        return self.reused

    def hash(self) -> bytes:
        return blake2s(
            self.name.encode() +
            (self.data.encode() if isinstance(self.data, (str, bytes)) else str(self.data).encode()) +
            self.script.encode()
        ).digest()

    def __hash__(self) -> int:
        return hash(self.hash())


TargetType = TypeVar('TargetType', bound=Target)


@dataclass
class TInterval(Interval, Target):
    def __eq__(self, other):
        return Target.__eq__(self, other)


@dataclass
class TScheduled(Scheduled, Target):
    def __eq__(self, other):
        return Target.__eq__(self, other)


@dataclass
class TSmart(Smart, Target):
    def __eq__(self, other):
        return Target.__eq__(self, other)


# Restock target base class & subclasses


@dataclass
class RestockTarget(ABC):
    script: str
    data: Union[str, bytes, int, float, list, tuple, dict] = field(repr=False)
    item: int = field(init=False, default=-1)
    reused: int = field(init=False, default=-1)

    def __post_init__(self):
        if not isinstance(self.script, str):
            raise TypeError('scripts must be str')
        if not isinstance(self.data, (str, bytes, int, float, list, tuple, dict)):
            raise TypeError('data must be (str bytes, int, float, list, tuple, dict)')

    def __eq__(self, other):
        if issubclass(type(other), RestockTarget):
            if other.script == self.script and other.data == self.data and other.item == self.item:
                return True
            else:
                return False
        else:
            return False

    def reuse(self, max_: int) -> int:
        if max_ > 0:
            if self.reused >= max_:
                self.reused = 0
            else:
                self.reused += 1
        return self.reused

    def hash(self) -> bytes:
        return blake2s(
            self.script.encode() +
            (self.data.encode() if isinstance(self.data, (str, bytes)) else str(self.data).encode()) +
            str(self.item).encode()
        ).digest()

    def __hash__(self):
        return hash(self.hash())


RestockTargetType = TypeVar('RestockTargetType', bound=RestockTarget)


@dataclass
class RTInterval(Interval, RestockTarget):
    def __eq__(self, other):
        return RestockTarget.__eq__(self, other)


@dataclass
class RTScheduled(Scheduled, RestockTarget):
    def __eq__(self, other):
        return RestockTarget.__eq__(self, other)


@dataclass
class RTSmart(Smart, RestockTarget):
    def __eq__(self, other):
        return RestockTarget.__eq__(self, other)


# Target EOF base class & subclasses


@dataclass
class TargetEnd(ABC):
    target: TargetType
    description: str = ''

    def __post_init__(self):
        if not issubclass(type(self.target), Target):
            raise TypeError('target must be Target\'s subclass')
        if not isinstance(self.description, str):
            raise TypeError('description must be str')


TargetEndType = TypeVar('TargetEndType', bound=TargetEnd)


class TEFail(TargetEnd):
    pass


class TESoldOut(TargetEnd):
    pass


class TESuccess(TargetEnd):
    pass
