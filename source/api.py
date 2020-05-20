import abc
import hashlib
import json
import os
import threading
from dataclasses import dataclass, field
from typing import Tuple, TypeVar, List, Any, Union, Dict

import cfscrape
import requests
import yaml

from . import storage
from . import codes
from . import logger


# Error classes


class ScriptError(Exception):
    pass


class ParserError(Exception):
    pass


class EventsExecutorError(Exception):
    pass


class ProviderError(Exception):
    pass


class SubProviderError(Exception):
    pass


# Result classes


PriceType = TypeVar('PriceType', Tuple[int, float], Tuple[int, float, float])
SizeType = TypeVar('SizeType', Tuple, Tuple[str], Tuple[Tuple[str]])
FooterType = TypeVar('FooterType', Tuple, Tuple[str])

currencies = {
    'AUD': 11,
    'CAD': 10,
    'HKD': 9,
    'PLN': 8,
    'BYN': 7,
    'UAH': 6,
    'NOK': 5,
    'CNY': 4,
    'RUB': 3,
    'EUR': 2,
    'USD': 1,
    'GBP': 0
}


@dataclass
class Proxy:
    url: str
    bad: int = field(init=False, default=0)

    def export(self) -> dict:
        return {'url': self.url, 'bad': self.bad}

    def use(self) -> Dict[str, str]:
        if self.url:
            return {'http': f'http://{self.url}/', 'https': f'https://{self.url}/'}
        else:
            return {}

    def __str__(self) -> str:
        return f'Proxy({self.url}, bad={self.bad})'


class ProviderCore:
    _proxies: Dict[str, Proxy] = {}


class Provider(ProviderCore):
    log: logger.Logger

    def __init__(self) -> None:
        self.log = logger.Logger('P')
        self.proxy_load()

    @property
    def proxies(self) -> Dict[str, Proxy]:
        return self._proxies

    def _test(self, url: str, force: bool = False) -> bool:
        try:
            self.log.info(codes.Code(21202, url))
            if (code := requests.get(storage.provider.test_url, proxies=Proxy(url).use(),
                                     timeout=storage.provider.timeout).status_code) != 200:
                self.log.info(codes.Code(41202, url))
                if force:
                    return False
                else:
                    raise ProviderError(f'Bad proxy ({code})')
        except (requests.ConnectionError, requests.Timeout):
            self.log.info(codes.Code(41202, url))
            if force:
                return False
            else:
                raise ProviderError(f'Proxy not available or too slow (timeout: {storage.provider.timeout})')
        self.log.info(codes.Code(21203, url))
        return True

    @staticmethod
    def proxy_check() -> None:
        temp = yaml.safe_load(open('proxy.yaml', 'r' if os.path.isfile('proxy.yaml') else 'w+'))
        if not isinstance(temp, list):
            if temp:
                raise TypeError('Bad proxy.yaml')
            else:
                yaml.safe_dump([], open('proxy.yaml', 'w+'))

    def proxy_dump(self) -> None:
        yaml.safe_dump([i.url for i in self._proxies.values()], open('proxy.yaml', 'w+'))
        self.log.info(codes.Code(21201))

    def proxy_load(self) -> None:
        self.proxy_check()
        self._proxies.clear()

        proxy = yaml.safe_load(open('proxy.yaml'))
        count = 0
        for i in proxy:
            if isinstance(i, str):
                if i in self._proxies:
                    continue

                if self._test(i, True):
                    self._proxies[i] = Proxy(i)
                    count += 1
                else:
                    self.log.error(codes.Code(41201, proxy))
        if count:
            self.log.warn(codes.Code(31203, str(count)))

    def proxy_add(self, url: str) -> None:
        if not isinstance(url, str):
            raise TypeError('url must be str')
        else:
            if url in self._proxies:
                raise KeyError('Proxy with this url already specified')
            else:
                if self._test(url):
                    self._proxies[url] = Proxy(url)
                    self.log.warn(codes.Code(31201))

    def proxy_remove(self, url: str) -> None:
        if url in self._proxies:
            del self._proxies[url]
            self.log.warn(codes.Code(31202))
        else:
            raise KeyError('Proxy with this url not specified')


class SubProvider(ProviderCore):
    _log: logger.Logger
    _scripts: str
    pos: int

    def __init__(self, script: str):
        self._log = logger.Logger('SP')
        self._script = script
        self.pos = 0

    def _proxy(self) -> Proxy:
        valid = [k for k, v in self._proxies.items() if v.bad < storage.provider.max_bad]

        if valid:
            if self.pos >= len(valid) - 1:
                self.pos = 0
            else:
                self.pos += 1
            return self._proxies[valid[self.pos]]
        else:
            return Proxy('')

    def _get(self, func, *args, **kwargs):
        try:
            return func.get(*args, **kwargs)
        except Exception as e:
            if isinstance(e, requests.ConnectionError):
                self._log.warn(codes.Code(31201), self._script)
            elif isinstance(e, requests.Timeout):
                self._log.warn(codes.Code(31202, str(kwargs['timeout'])), self._script)

            raise SubProviderError

    def get(
            self,
            url: str,
            mode: int = 0,
            proxy: bool = False,
            timeout: int = None,
            *,
            params: Dict[str, str] = None,
            headers: Dict[str, str] = None,
            cookies: Dict[str, Any] = None,
            **kwargs
    ) -> str:
        if not isinstance(url, str):
            raise TypeError('url must be str')
        if not isinstance(mode, int):
            raise TypeError('mode must be int')
        if not isinstance(proxy, bool):
            raise TypeError('proxy must be bool')
        else:
            if proxy:
                proxy_ = self._proxy()
        if timeout:
            if not isinstance(timeout, int):
                raise TypeError('int must be int')
        else:
            timeout = storage.provider.timeout
        if params:
            if not isinstance(params, dict):
                raise TypeError('params must be dict')
        else:
            params: Dict[str, str] = {}
        if headers:
            if not isinstance(headers, dict):
                raise TypeError('header must be dict')
        else:
            headers: Dict[str, str] = {}
        if cookies:
            if not isinstance(cookies, dict):
                raise TypeError('cookies must be dict')
        else:
            cookies: Dict[str, Any] = {}

        try:
            if mode == 0:
                return self._get(
                    requests,
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    proxies=proxy_.use() if proxy else {},
                    timeout=timeout
                ).text
            elif mode == 1:
                return self._get(
                    cfscrape.create_scraper(delay=kwargs['delay'] if 'delay' in kwargs else storage.provider.delay),
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    proxies=proxy_.use() if proxy else {},
                    timeout=timeout
                ).text
            else:
                raise ValueError(f'Unknown mode, {mode}')
        except SubProviderError as e:
            if proxy:
                proxy_.bad += 1
            raise e


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

    def __post_init__(self):
        if not isinstance(self.name, str):
            raise ValueError('name must be str')
        if not isinstance(self.url, str):
            raise ValueError('url must be str')
        if not isinstance(self.channel, str):
            raise ValueError('channel must be str')
        if not isinstance(self.image, str):
            raise ValueError('image must be str')
        if not isinstance(self.description, str):
            raise ValueError('description must be str')
        if not isinstance(self.price, (tuple, list)):
            raise ValueError('price must be tuple or list')
        else:
            if (self.price.__len__() < 2 or not isinstance(self.price[0], int) or
                not isinstance(self.price[1], (int, float))) or \
                    (self.price.__len__() == 3 and not isinstance(self.price[2], (int, float))):
                raise ValueError('price must be tuple with (int, [int, float], *[int, float])')
        if not isinstance(self.fields, dict):
            raise ValueError('fields must be dict')
        else:
            for i in self.fields:
                if not isinstance(i, str):
                    raise KeyError('all keys in fields must be str')
            for i in self.fields.values():
                if not isinstance(i, (str, int, float)):
                    raise ValueError('all values in fields must be str, int or float')
        if not isinstance(self.sizes, (tuple, list)):
            raise ValueError('sizes must be tuple or list')
        else:
            for i in self.sizes:
                if not isinstance(i, (str, tuple, list)):
                    raise ValueError('sizes must contain tuple of tuples of str, tuple of str or be empty')
                else:
                    if isinstance(i, (tuple, list)) and \
                            (
                                    not i.__len__() == 2 or
                                    (i.__len__() == 2 and not isinstance(i[0], str) or not isinstance(i[1], str))
                            ):
                        raise ValueError('sizes items must be tuple or list of two str')
        if not isinstance(self.footer, (tuple, list)):
            raise ValueError('footer must be tuple or list')
        else:
            for i in self.footer:
                if i.__len__() == 0 or (i.__len__() == 1 and not isinstance(i[0], str)) or \
                        (i.__len__() == 2 and not isinstance(i[1], str)):
                    raise ValueError('footer item must be tuple with (str, *str)')


# Indexing


@dataclass
class Index(abc.ABC):
    script: str

    def hash(self) -> bytes:
        return hashlib.blake2s(self.script.encode()).digest()

    def __hash__(self) -> int:
        return hash(self.hash())


IndexType = TypeVar('IndexType', bound=Index)


@dataclass
class IOnce(Index):
    pass


@dataclass
class IInterval(Index):
    interval: float


# Target classes


@dataclass
class Target(abc.ABC):
    name: str
    script: str
    data: Any = field(repr=False)
    reused: int = field(init=False, default=-1)

    def reuse(self, max_: int) -> int:
        if max_ > 0:
            if self.reused >= max_:
                self.reused = 0
            else:
                self.reused += 1
        return self.reused

    def hash(self) -> bytes:
        return hashlib.blake2s(
            self.script.encode() +
            (self.data.encode() if isinstance(self.data, (str, bytes, bytearray)) else self.data.__repr__().encode()) +
            self.name.encode()
        ).digest()

    def __hash__(self) -> int:
        return hash(self.hash())


TargetType = TypeVar('TargetType', bound=Target)


@dataclass
class TInterval(Target):
    interval: int


@dataclass
class TScheduled(Target):
    timestamp: float


@dataclass
class TSmart(Target):
    length: int
    scatter: int
    timestamp: float


# Status classes


class Status(abc.ABC):
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


class Parser(abc.ABC):  # Class to implement parsers
    def __init__(self, name: str, log: logger.Logger, provider_: SubProvider):
        self.name = name
        self.log = log
        self.provider = provider_

    @abc.abstractmethod
    def index(self) -> IndexType:
        raise NotImplementedError

    @abc.abstractmethod
    def targets(self) -> List[TargetType]:
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, target: TargetType) -> StatusType:
        raise NotImplementedError


# Event classes


class EventsExecutor(abc.ABC):
    def __init__(self, name: str, log: logger.Logger):
        self.name = name
        self.log = log

    def e_monitor_starting(self) -> None: ...

    def e_monitor_started(self) -> None: ...

    def e_monitor_stopping(self) -> None: ...

    def e_monitor_stopped(self) -> None: ...

    def e_alert(self, code: codes.Code, thread: str) -> None: ...

    def e_success_status(self, status: SSuccess) -> None: ...

    def e_fail_status(self, status: SFail) -> None: ...


if __name__ == 'source.api':
    provider: Provider = Provider()
