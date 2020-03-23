from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha1
from typing import Tuple, TypeVar, List, Any, Union, Dict

from .logger import Logger


# Error classes


class ScriptError(Exception):
    pass


class ParserError(Exception):
    pass


class EventsExecutorError(Exception):
    pass


# Result classes


PriceType = TypeVar('PriceType', Tuple[int, Union[float, int]], Tuple[int, Union[float, int], Union[float, int]])
SizeType = TypeVar('SizeType', Tuple, Tuple[str], Tuple[str, str])
FooterType = TypeVar('FooterType', Tuple, Tuple[str, str])

currencies = {
    'ruble': 3,
    'euro': 2,
    'dollar': 1,
    'pound': 0
}


@dataclass
class Result:
    __slots__ = ('name', 'url', 'channel', 'image', 'description', 'price', 'fields', 'sizes', 'footer')
    name: str
    url: str
    channel: str
    image: str
    description: str
    price: PriceType
    fields: Dict[str, Union[str, int, float]]
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
    __slots__ = ('name', 'script', 'data', 'interval')
    interval: float

    def hash(self) -> int:
        return int(sha1(
            self.script.encode() + self.data.__repr__().encode() + str(self.interval).encode() + self.name.encode()
        ).hexdigest(), 16)

    def __hash__(self) -> int:
        return hash(self.hash())


@dataclass
class TScheduled(Target):
    __slots__ = ('name', 'script', 'data', 'timestamp')
    timestamp: float

    def hash(self) -> int:
        return int(sha1(
            self.script.encode() + self.data.__repr__().encode() + str(self.timestamp).encode() + self.name.encode()
        ).hexdigest(), 16)

    def __hash__(self) -> int:
        return hash(self.hash())


@dataclass
class TSmart(Target):  # DON'T USE
    __slots__ = ('name', 'script', 'data', 'accuracy', 'timestamp')
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
    def __init__(self, name: str, log: Logger):
        self.name = name
        self.log = log

    @abstractmethod
    def index(self) -> IndexType: ...

    @abstractmethod
    def targets(self) -> List[TargetType]: ...  # TODO: Fix this annotation

    @abstractmethod
    def execute(self, target: TargetType) -> StatusType: ...


# Event classes


class EventsExecutor(ABC):
    def __init__(self, name: str, log: Logger):
        self.name = name
        self.log = log

    def e_monitor_turning_on(self) -> None: ...

    def e_monitor_turned_on(self) -> None: ...

    def e_monitor_turning_off(self) -> None: ...

    def e_monitor_turned_off(self) -> None: ...

    def e_error(self, message: str, thread: str) -> None: ...

    def e_fatal(self, e: Exception, thread: str) -> None: ...

    def e_success_status(self, status: SSuccess) -> None: ...

    def e_fail_status(self, status: SFail) -> None: ...
