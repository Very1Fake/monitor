from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha1
from typing import Tuple, TypeVar, List, Any

# TODO: Logger for Parser


# Result classes


PriceType = TypeVar('PriceType', float, Tuple[float, float])
SizeType = TypeVar('SizeType', Tuple, Tuple[str], Tuple[str, str])


@dataclass
class Result:
    __slots__ = ('name', 'url', 'image', 'description', 'price', 'sizes')
    name: str
    url: str
    image: str
    description: str
    price: PriceType
    sizes: SizeType


# Indexing


class Index(ABC):
    pass


IndexType = TypeVar('IndexType', bound=Index)


@dataclass
class IndexOnce(Index):
    __slots__ = ('script',)
    script: str


@dataclass
class IndexInterval(Index):
    __slots__ = ('script', 'interval')
    script: str
    interval: float


# Target classes


class Target(ABC):
    pass


TargetType = TypeVar('TargetType', bound=Target)


@dataclass
class IntervalTarget(Target):
    __slots__ = ('script', 'data', 'interval', 'name')
    script: str
    data: Any
    interval: float
    name: str

    def hash(self) -> int:
        try:
            return int(sha1(
                self.script.encode() + str(hash(self.data)).encode() + str(self.interval).encode() + self.name.encode()
            ).hexdigest(), 16)
        except TypeError:
            return int(sha1(self.script.encode() + str(self.interval).encode() + self.name.encode()).hexdigest(), 16)

    def __hash__(self) -> int:
        return hash(self.hash())


@dataclass
class ScheduledTarget(Target):  # DON'T USE
    __slots__ = ('script', 'data', 'timestamp', 'name')
    script: str
    data: Any
    timestamp: float
    name: str


@dataclass
class CompletedTarget(Target):
    pass


@dataclass
class LostTarget(Target):
    message: str = ''

    def hash(self) -> int:
        return int(sha1(self.message).hexdigest(), 16)

    def __hash__(self) -> int:
        return hash(self.hash())


# Status classes


class Status(ABC):
    pass


StatusType = TypeVar('StatusType', bound=Status)


@dataclass
class StatusSuccess(Status):
    result: Result


@dataclass
class StatusWaiting(Status):
    target: TargetType


@dataclass
class StatusFail(Status):
    message: str = ''


# Parser classes


class Parser(ABC):  # Class to implement parsers
    @abstractmethod
    def index(self) -> IndexType: ...

    @abstractmethod
    def targets(self) -> List[TargetType]: ...  # TODO: Fix this annotation

    @abstractmethod
    def execute(self, data: Any) -> StatusType: ...


# Event classes


class SuccessEvent(ABC):  # Class to implement success event (after success parsing)
    @abstractmethod
    def execute(self) -> None: ...


class FailEvent(ABC):  # Class to implement fail event (after unsuccessful parsing)
    @abstractmethod
    def execute(self) -> None: ...


class CompletedTargetEvent(ABC):
    @abstractmethod
    def execute(self) -> None: ...


class LostTargetEvent(ABC):  # Class to implement lost target event
    @abstractmethod
    def execute(self) -> None: ...
