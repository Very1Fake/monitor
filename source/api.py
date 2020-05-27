import abc
import hashlib
from dataclasses import dataclass, field
from types import GeneratorType
from typing import TypeVar, List, Any, Union, Dict, Generator

from . import codes
from . import logger
from .library import Interval, Scheduled, Smart, SubProvider


# Constants


CURRENCIES = {
    'AUD': 12,
    'CAD': 11,
    'HKD': 10,
    'PLN': 9,
    'BYN': 8,
    'UAH': 7,
    'NOK': 6,
    'CNY': 5,
    'RUB': 4,
    'EUR': 3,
    'USD': 2,
    'GBP': 1,
    '': 0
}


SIZE_TYPES = {
    'S-JP-W': 8,
    'S-JP-M': 7,
    'S-EU-W': 6,
    'S-EU-M': 5,
    'S-UK-W': 4,
    'S-UK-M': 3,
    'S-US-W': 2,
    'S-US-M': 1,
    '': 0
}


# Error classes


class ParserError(Exception):
    pass


class EventsExecutorError(Exception):
    pass


# Indexing


@dataclass
class Catalog(abc.ABC):
    script: str

    def __post_init__(self):
        if not isinstance(self.script, str):
            raise TypeError('script must be str')

    def hash(self) -> bytes:
        return hashlib.blake2s(self.script.encode()).digest()

    def __hash__(self) -> int:
        return hash(self.hash())


CatalogType = TypeVar('CatalogType', bound=Catalog)


@dataclass
class CInterval(Interval, Catalog):
    pass


@dataclass
class CScheduled(Scheduled, Catalog):
    pass


@dataclass
class CSmart(Smart, Catalog):
    pass


# Target classes


@dataclass
class Target(abc.ABC):
    name: str
    script: str
    data: Any = field(repr=False)
    reused: int = field(init=False, compare=False, default=-1)

    def __post_init__(self):
        if isinstance(self.name, str):
            raise TypeError('name must be str')
        if not isinstance(self.script, str):
            raise TypeError('scripts must be str')

    def reuse(self, max_: int) -> int:
        if max_ > 0:
            if self.reused >= max_:
                self.reused = 0
            else:
                self.reused += 1
        return self.reused

    def hash(self) -> bytes:
        return hashlib.blake2s(self.name.encode() + self.script.encode()).digest()

    def __hash__(self) -> int:
        return hash(self.hash())


TargetType = TypeVar('TargetType', bound=Target)


@dataclass
class TInterval(Interval, Target):
    pass


@dataclass
class TScheduled(Scheduled, Target):
    pass


@dataclass
class TSmart(Smart, Target):
    pass


# Restock Target classes


@dataclass
class RestockTarget(abc.ABC):
    script: str
    item: int = field(init=False, default=-1)
    reused: int = field(init=False, default=-1)

    def __post_init__(self):
        if not isinstance(self.script, str):
            raise TypeError('scripts must be str')

    def reuse(self, max_: int) -> int:
        if max_ > 0:
            if self.reused >= max_:
                self.reused = 0
            else:
                self.reused += 1
        return self.reused

    def hash(self) -> bytes:
        return hashlib.blake2s(self.script.encode() + str(self.item).encode()).digest()

    def __hash__(self):
        return hash(self.hash())


RestockTargetType = TypeVar('RestockTargetType', bound=RestockTarget)


@dataclass
class RTInterval(Interval, RestockTarget):
    pass


@dataclass
class RTScheduled(Scheduled, RestockTarget):
    pass


@dataclass
class RTSmart(Smart, RestockTarget):
    pass


# Target EOF classes


@dataclass
class TargetEnd(abc.ABC):
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


# Item classes


@dataclass
class Price:
    currency: int
    current: float
    old: float = 0.

    def __post_init__(self):
        if isinstance(self.currency, int):
            if self.currency not in CURRENCIES.values():
                raise IndexError(f'Currency ({self.currency}) does not exist')
        else:
            raise TypeError('currency must be int')

        if not isinstance(self.current, float):
            raise TypeError('current must be float')

        if not isinstance(self.old, float):
            raise TypeError('old must be float')

    def hash(self) -> bytes:
        return hashlib.blake2s(bytes(self.currency) + str(self.current).encode() + str(self.old).encode()).digest()


@dataclass
class Size:
    size: float
    url: str = ''

    def __post_init__(self):
        if not isinstance(self.size, float):
            raise TypeError('size must be float')
        if not isinstance(self.url, str):
            raise TypeError('size must be str')

    def hash(self) -> bytes:
        return hashlib.blake2s(str(self.size).encode() + self.url.encode()).digest()

    def export(self) -> list:
        return [self.size, self.url]


class Sizes(list):
    type: int

    def __init__(self, type_: int, values: Union[List[Size], Generator] = None):
        if isinstance(type_, int):
            if type_ not in SIZE_TYPES.values():
                raise IndexError(f'Size type ({type_}) does not exist')
            self.type = type_
        else:
            raise TypeError('type_ must be int')

        if values:
            if isinstance(values, (list, GeneratorType)):
                super().__init__()
                for i in values:
                    if isinstance(i, Size):
                        super().append(i)
                    else:
                        raise ValueError('All items of Sizes must be Size')
            else:
                raise TypeError('values must be iterable type (like list or generator)')

    def __setitem__(self, index: int, value: Size) -> None:
        if isinstance(value, Size):
            super().__setitem__(index, value)
        else:
            raise TypeError('value must be Size')

    def append(self, value: Size) -> None:
        if isinstance(value, Size):
            super().append(value)
        else:
            raise TypeError('Only Size can be appended')

    def extend(self, value) -> None:
        if isinstance(value, Sizes):
            super().extend(value)
        else:
            raise TypeError('Sizes can be extended only by Sizes')

    def hash(self) -> bytes:
        hash_ = hashlib.blake2s(str(self.type).encode())
        for i in self:
            hash_.update(i.hash())
        return hash_.digest()

    def export(self) -> List[list]:
        return [i.export() for i in self]


@dataclass
class FooterItem:
    text: str
    url: str

    def __post_init__(self):
        if not isinstance(self.text, str):
            raise TypeError('text must be str')
        if not isinstance(self.url, str):
            raise TypeError('url must be str')

    def hash(self) -> bytes:
        return hashlib.blake2s(self.text.encode() + self.url.encode()).digest()


class Item(abc.ABC):
    url: str
    channel: str
    name: str
    image: str = ''
    description: str = ''
    price: Price = None
    sizes: Sizes = None
    footer: List[FooterItem] = None
    fields: Dict[str, str] = None

    def __init__(
            self,
            url: str,
            channel: str,
            name: str,
            image: str = '',
            description: str = '',
            price: Price = None,
            sizes: Sizes = None,
            footer: List[FooterItem] = None,
            fields: Dict[str, str] = None
    ):
        if isinstance(url, str):
            self.url = url
        else:
            raise TypeError('url must be str')
        if isinstance(channel, str):
            self.channel = channel
        else:
            raise TypeError('channel must be str')
        if isinstance(name, str):
            self.name = name
        else:
            raise TypeError('name must be str')

        if isinstance(image, str):
            self.image = image
        else:
            raise TypeError('image must be str')
        if isinstance(description, str):
            self.description = description
        else:
            raise TypeError('description must be str')

        if price:
            if isinstance(price, Price):
                self.price = price
            else:
                raise TypeError('price must be Price')
        else:
            self.price = Price(CURRENCIES[''], 0.)

        if sizes:
            if isinstance(sizes, Sizes):
                self.sizes = sizes
            else:
                raise TypeError('sizes must be Sizes')
        else:
            self.sizes = Sizes(SIZE_TYPES[''])

        if footer:
            if isinstance(footer, list):
                for i in footer:
                    if not isinstance(i, FooterItem):
                        raise ValueError('All items in footer must be FooterItem')
                self.footer = footer
            else:
                raise TypeError('footer must be list')
        else:
            self.footer = []

        if fields:
            if isinstance(fields, dict):
                for k, v in fields.items():
                    if not isinstance(k, str):
                        raise ValueError('All keys in fields must be str')
                    if not isinstance(v, str):
                        raise ValueError('All values in fields must be str')
                self.fields = fields
            else:
                raise TypeError('fields must be dict')
        else:
            self.fields = {}

    def __repr__(self):
        return f'Item({self.url=}, {self.channel=}, {self.name=})'

    def hash(self, level: int = 2) -> bytes:
        if isinstance(level, int):
            if not 0 <= level <= 6:
                raise ValueError('level must be 0 <= level <= 5')
        else:
            raise TypeError('level must be int')

        hash_ = hashlib.blake2s(self.url.encode() + self.channel.encode())

        if level > 0:
            hash_.update(self.name.encode())
            if level > 1:
                hash_.update(self.image.encode())
                if level > 2:
                    hash_.update(self.description.encode())
                    if level > 3:
                        hash_.update(self.price.hash())
                        if level > 4:
                            for i in self.footer:
                                hash_.update(i.hash())
        return hash_.digest()


ItemType = TypeVar('ItemType', bound=Item)


class IAnnounce(Item):
    pass


class IRelease(Item):
    restock: RestockTargetType

    def __init__(
            self,
            url: str,
            channel: str,
            name: str,
            image: str = '',
            description: str = '',
            price: Price = None,
            sizes: Sizes = None,
            footer: List[FooterItem] = None,
            fields: Dict[str, str] = None,
            restock: RestockTargetType = None
    ):
        if restock:
            if issubclass(type(restock), RestockTarget):
                self.restock = restock
            else:
                raise TypeError('restock must be subclass of RestockTarget')
        else:
            self.restock = None

        super().__init__(name, channel, url, image, description, price, sizes, footer, fields)


class IRestock(Item):
    id: int

    def __init__(
            self,
            id_: int,
            url: str,
            channel: str,
            name: str,
            image: str = '',
            description: str = '',
            price: Price = None,
            sizes: Sizes = None,
            footer: List[FooterItem] = None,
            fields: Dict[str, str] = None
    ):
        if isinstance(id_, int):
            self.id = id_
        else:
            raise TypeError('id_ must be int')

        super().__init__(name, channel, url, image, description, price, sizes, footer, fields)


# Script classes


class Parser(abc.ABC):  # Class to implement by scripts with parser
    name: str
    log: logger.Logger
    provider: SubProvider

    def __init__(self, name: str, log: logger.Logger, provider_: SubProvider):
        self.name = name
        self.log = log
        self.provider = provider_

    @property
    def catalog(self) -> CatalogType:
        raise NotImplementedError

    @abc.abstractmethod
    def execute(
            self,
            mode: int,
            content: Union[CatalogType, TargetType]
    ) -> List[Union[CatalogType, TargetType, RestockTargetType, ItemType, TargetEndType]]:
        raise NotImplementedError


class EventsExecutor(abc.ABC):
    def __init__(self, name: str, log: logger.Logger):
        self.name = name
        self.log = log

    def e_monitor_starting(self) -> None: ...

    def e_monitor_started(self) -> None: ...

    def e_monitor_stopping(self) -> None: ...

    def e_monitor_stopped(self) -> None: ...

    def e_alert(self, code: codes.Code, thread: str) -> None: ...

    def e_item_announced(self, item: IAnnounce) -> None: ...

    def e_item_released(self, item: IRelease) -> None: ...

    def e_item_restock(self, item: IRestock) -> None: ...

    def e_target_end_fail(self, target_end: TEFail) -> None: ...

    def e_target_end_sold_out(self, target_end: TESoldOut) -> None: ...

    def e_target_end_success(self, target_end: TESuccess) -> None: ...