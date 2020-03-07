from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha1
from typing import Tuple, TypeVar, List, Any

# TODO: Logger for Parser


# Result classes


PriceType = TypeVar('PriceType', float, Tuple[float, float])
SizeType = TypeVar('SizeType', Tuple, Tuple[str], Tuple[str, str])
FooterType = TypeVar('FooterType', Tuple, Tuple[str, str])


@dataclass
class Result:
    __slots__ = ('name', 'url', 'image', 'description', 'price', 'sizes', 'footer')
    name: str
    url: str
    image: str
    description: str
    price: PriceType
    sizes: SizeType
    footer: FooterType


# Indexing


class Index(ABC):
    pass


IndexType = TypeVar('IndexType', bound=Index)


@dataclass
class IOnce(Index):
    __slots__ = ('script',)
    script: str


@dataclass
class IInterval(Index):
    __slots__ = ('script', 'interval')
    script: str
    interval: float


# Target classes


@dataclass
class Target(ABC):
    name: str
    script: str
    data: Any

    def content_hash(self) -> int:
        return int(sha1(
            self.script.encode() + self.data.__repr__().encode() + self.name.encode()
        ).hexdigest(), 16)


TargetType = TypeVar('TargetType', bound=Target)


@dataclass
class TInterval(Target):
    __slots__ = ('script', 'data', 'interval', 'name')
    interval: float

    def hash(self) -> int:
        return int(sha1(
            self.script.encode() + self.data.__repr__().encode() + str(self.interval).encode() + self.name.encode()
        ).hexdigest(), 16)

    def __hash__(self) -> int:
        return hash(self.hash())


@dataclass
class TScheduled(Target):  # DON'T USE
    __slots__ = ('script', 'data', 'timestamp', 'name')
    timestamp: float
    name: str


@dataclass
class TSmart(Target):  # DON'T USE
    __slots__ = ('script', 'data', 'accuracy', 'timestamp', 'name')
    accuracy: int
    timestamp: float


# Status classes


class Status(ABC):
    pass


StatusType = TypeVar('StatusType', bound=Status)


@dataclass
class SSuccess(Status):
    __slots__ = ('script', 'result')
    script: str
    result: Result


@dataclass
class SWaiting(Status):
    __slots__ = ('target',)
    target: TargetType


@dataclass
class SFail(Status):
    __slots__ = ('script', 'message')
    script: str
    message: str


# Parser classes


class Parser(ABC):  # Class to implement parsers
    @abstractmethod
    def index(self) -> IndexType: ...

    @abstractmethod
    def targets(self) -> List[TargetType]: ...  # TODO: Fix this annotation

    @abstractmethod
    def execute(self, target: TargetType) -> StatusType: ...


# Event classes


class EventsExecutor(ABC):
    def e_monitor_turning_on(self) -> None: ...

    def e_monitor_turned_on(self) -> None: ...

    def e_monitor_turning_off(self) -> None: ...

    def e_error(self, message: str, thread: str) -> None: ...

    def e_fatal(self, e: Exception, thread: str) -> None: ...

    def e_success_status(self, status: SSuccess) -> None: ...

    def e_fail_status(self, status: SFail) -> None: ...
