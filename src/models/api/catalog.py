from abc import ABC
from dataclasses import dataclass
from hashlib import blake2s
from typing import TypeVar

from src.utils.schedule import Interval, Scheduled, Smart


# Tasks classes


@dataclass
class Catalog(ABC):
    script: str

    def __post_init__(self):
        if not isinstance(self.script, str):
            raise TypeError('script must be str')

    def __eq__(self, other):
        if issubclass(type(other), Catalog):
            if other.script == self.script:
                return True
            else:
                return False
        else:
            return False

    def hash(self) -> bytes:
        return blake2s(self.script.encode()).digest()

    def __hash__(self) -> int:
        return hash(self.hash())


CatalogType = TypeVar('CatalogType', bound=Catalog)


@dataclass
class CInterval(Interval, Catalog):
    def __eq__(self, other):
        return Catalog.__eq__(self, other)


@dataclass
class CScheduled(Scheduled, Catalog):
    def __eq__(self, other):
        return Catalog.__eq__(self, other)


@dataclass
class CSmart(Smart, Catalog):
    def __eq__(self, other):
        return Catalog.__eq__(self, other)
