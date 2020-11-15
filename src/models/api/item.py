from abc import ABC
from dataclasses import dataclass
from hashlib import blake2s
from time import time
from types import GeneratorType
from typing import Dict, Generator, Optional, List, TypeVar, Union

from .target import RestockTarget, RestockTargetType

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
    'P-W-W': 14,
    'P-M-W': 13,
    'P-U-W': 12,
    'C-W-W': 11,
    'C-M-W': 10,
    'C-U-W': 9,
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
            if isinstance(self.current, int):
                self.current = float(self.current)
            else:
                raise TypeError('current must be float')

        if not isinstance(self.old, float):
            if isinstance(self.old, int):
                self.old = float(self.old)
            else:
                raise TypeError('old must be float')

    def hash(self) -> bytes:
        return blake2s(bytes(self.currency) + str(self.current).encode() + str(self.old).encode()).digest()


@dataclass
class Size:
    size: str
    url: str = ''

    def __post_init__(self):
        if not isinstance(self.size, str):
            raise TypeError('size must be str')
        if not isinstance(self.url, str):
            raise TypeError('size must be str')

    def hash(self) -> bytes:
        return blake2s(self.size.encode() + self.url.encode()).digest()

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
        hash_ = blake2s(str(self.type).encode())
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
        return blake2s(self.text.encode() + self.url.encode()).digest()


class Item(ABC):
    url: str
    channel: str
    name: str
    image: str = ''
    description: str = ''
    price: Price = None
    sizes: Sizes = None
    footer: List[FooterItem] = None
    fields: Dict[str, str] = None
    publish_date: float
    timestamp: float

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
            publish_date: float = -1.
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

        if isinstance(publish_date, float):
            self.publish_date = publish_date
        else:
            raise TypeError('publish_date must be float')

        self.timestamp = time()

    def __repr__(self):
        return f'Item({self.url=}, {self.channel=}, {self.name=})'

    def hash(self, level: int = 2) -> bytes:
        if isinstance(level, int):
            if not 0 <= level <= 6:
                raise ValueError('level must be 0 <= level <= 5')
        else:
            raise TypeError('level must be int')

        hash_ = blake2s(self.url.encode() + self.channel.encode())

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


class IRelease(Item):  # TODO: Change RestockTarget creation mechanism (from kernel)
    restock: Optional[RestockTargetType]

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
            publish_date: float = -1.,
            restock: RestockTargetType = None
    ):
        if restock:
            if issubclass(type(restock), RestockTarget):
                self.restock = restock
            else:
                raise TypeError('restock must be subclass of RestockTarget')
        else:
            self.restock = None

        super().__init__(url, channel, name, image, description, price, sizes, footer, fields, publish_date)


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
            fields: Dict[str, str] = None,
            publish_time: float = -1.
    ):
        if isinstance(id_, int):
            self.id = id_
        else:
            raise TypeError('id_ must be int')

        super().__init__(name, channel, url, image, description, price, sizes, footer, fields, publish_time)
