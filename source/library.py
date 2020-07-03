import collections
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, List, Dict, Union, Tuple

import ujson
import cfscrape
import requests

from . import codes
from . import logger
from . import storage


# TODO: Add export/import of Proxy.bad


# Exception classes


class ProxyError(Exception):
    pass


class ProviderError(Exception):
    pass


class SubProviderError(Exception):
    pass


# Type classes


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    content: Any = field(compare=False)


class Schedule(dict):
    def __setitem__(self, time_: Union[float, int], value):
        if isinstance(time_, float) or isinstance(time_, int):
            super().__setitem__(round(time_, 7), value)
        else:
            raise KeyError('Key must be int or float')

    def __getitem__(self, time_: Union[float, int, slice]):
        if isinstance(time_, slice):
            if time_.start:
                return (i for i in self.items() if i[0] >= time_.start)
            elif time_.stop:
                return (i for i in self.items() if i[0] <= time_.stop)
            else:
                return []
        else:
            return super().__getitem__(time_)

    def __delitem__(self, time_: Union[float, int, slice]):
        if isinstance(time_, slice):
            for i in self.key_list(time_):
                super().__delitem__(i)
        else:
            super().__delitem__(time_)

    def __contains__(self, time_) -> bool:
        if isinstance(time_, float):
            time_ = round(time_, 7)
        return super().__contains__(time_)

    def pop_time(self, time_: Union[float, int, slice]) -> List[Any]:
        items = list(self.__getitem__(time_))
        del self[time_]
        return items

    def pop_first(self, time_: slice) -> Any:
        try:
            item = next(self.__getitem__(time_))
            del self[item[0]]
            return item
        except StopIteration:
            return ()

    def key_list(self, time_: slice) -> list:
        if time_.start:
            return list(i for i in self if i >= time_.start)
        elif time_.stop:
            return list(i for i in self if i <= time_.stop)
        else:
            return []


class UniqueSchedule(Schedule):
    def __setitem__(self, time_: Union[float, int], value):
        if value not in self.values():
            super().__setitem__(time_, value)
        else:
            raise IndexError('Non-unique value')


@dataclass
class Interval:
    interval: float = field(compare=False)

    def __post_init__(self):
        if not isinstance(self.interval, float):
            if isinstance(self.interval, int):
                self.interval = float(self.interval)
            else:
                raise TypeError('interval must be float')


@dataclass
class Scheduled:
    timestamp: float = field(compare=False)

    def __post_init__(self):
        if not isinstance(self.timestamp, float):
            if isinstance(self.timestamp, int):
                self.timestamp = float(self.timestamp)
            else:
                raise TypeError('timestamp must be float')


@dataclass
class Smart:
    timestamp: float = field(compare=False)
    length: int = field(compare=False)
    scatter: int = field(compare=False, default=1)
    exp: float = field(compare=False, default=2.)
    expired: bool = field(init=False)

    def __post_init__(self):
        if not isinstance(self.timestamp, float):
            if isinstance(self.timestamp, int):
                self.timestamp = float(self.timestamp)
            else:
                raise TypeError('timestamp must be float')

        if isinstance(self.length, int):
            if self.length < 0:
                raise ValueError('length cannot be less than 0')
        else:
            raise TypeError('length must be int')

        if isinstance(self.scatter, int):
            if self.scatter < 1:
                raise ValueError('scatter cannot be less than 1')
        else:
            raise TypeError('scatter must be int')

        if isinstance(self.exp, float):
            if self.exp <= 1:
                raise ValueError('exp cannot be more than 1')
        else:
            raise TypeError('exp must be float')

        self.expired = False


@dataclass
class Proxy:
    address: str = None
    login: str = None
    password: str = None
    bad: int = field(compare=False, default=0)
    _stats: collections.deque = field(compare=False, init=False, default_factory=lambda: collections.deque(maxlen=30))

    def __post_init__(self):
        if self.address:
            if isinstance(self.address, str):
                pass
            else:
                raise TypeError('address must be string')

        if self.login:
            if isinstance(self.login, str):
                if self.password:
                    if not isinstance(self.password, str):
                        raise TypeError(f'password must be string ({self.address})')
                else:
                    raise ProxyError(f'If login is specified, password must be specified too ({self.address})')
            else:
                raise TypeError(f'login must be string ({self.address})')

        if not isinstance(self.bad, int):
            raise TypeError(f'bad must be int ({self.address})')

    @property
    def stats(self) -> list:
        return list(self._stats)

    @stats.setter
    def stats(self, elapsed: Union[float, int]) -> None:
        self._stats.appendleft(round(elapsed, 5))

    def use(self) -> Dict[str, str]:
        if self.address:
            return {
                'http': f'http://{f"{self.login}:{self.password}@" if self.login else ""}{self.address}/',
                'https': f'https://{f"{self.login}:{self.password}@" if self.login else ""}{self.address}/'
            }
        else:
            return {}

    def export(self) -> dict:
        return {
            'address': self.address,
            'login': self.login,
            'password': self.password,
            'bad': self.bad,
            'stats': self.stats
        }

    def __str__(self) -> str:
        return f'Proxy({self.address}{f", l={self.login}" if self.login else ""}, bad={self.bad})'

    def __repr__(self) -> str:
        return f'{f"{self.login}:{self.password}@" if self.login else ""}{self.address}'


# Storage classes


class ProviderCore:
    lock: threading.RLock = threading.RLock()
    _proxies: Dict[str, Proxy] = {}


# Functional classes


class Provider(ProviderCore):
    log: logger.Logger

    def __init__(self) -> None:
        self.log = logger.Logger('PR')
        self.proxy_load()

    @property
    def proxies(self) -> Dict[str, Proxy]:
        return self._proxies

    def proxy_test(self, proxy: Proxy, force: bool = False) -> bool:
        try:
            self.log.info(codes.Code(21202, repr(proxy)))
            if (code := requests.get(storage.provider.test_url, proxies=proxy.use(),
                                     timeout=storage.provider.timeout).status_code) != 200:
                self.log.info(codes.Code(41202, repr(proxy)))
                if force:
                    return False
                else:
                    raise ProviderError(f'Bad proxy ({code})')
        except (requests.ConnectionError, requests.Timeout):
            self.log.info(codes.Code(41202, repr(proxy)))
            if force:
                return False
            else:
                raise ProviderError(f'Proxy not available or too slow (timeout: {storage.provider.timeout})')
        self.log.info(codes.Code(21203, repr(proxy)))
        return True

    @staticmethod
    def proxy_file_check(path: str) -> None:
        if os.path.isfile(path):
            file = open('proxy.json', 'r' if os.path.isfile('proxy.json') else 'w+').read()

            if file:
                try:
                    if isinstance(file := ujson.loads(file), dict):
                        for k, v in file.items():
                            if not isinstance(k, str):
                                raise TypeError(f'Proxy address must be string ({k})')

                            if isinstance(v, dict):
                                if 'login' in v:
                                    if isinstance(v['login'], str):
                                        if 'password' in v:
                                            if not isinstance(v['password'], str):
                                                raise TypeError(f'Proxy password must be string ({k})')
                                        else:
                                            raise IndexError(
                                                f'If proxy password specified, password must be specified too ({k})')
                                    else:
                                        raise TypeError(f'Proxy login must be string ({k})')
                            else:
                                raise TypeError(f'Proxy content must be object')
                    else:
                        raise TypeError('Bad proxy file (proxy file must be object)')
                except ValueError:
                    raise SyntaxError('Non JSON file')
            else:
                ujson.dump({}, open(path, 'w+'), indent=4)
        else:
            if path == 'proxy.json':
                ujson.dump({}, open(path, 'w+'), indent=4)
            else:
                raise FileNotFoundError(path)

    def proxy_dump(self, path: str = '') -> None:
        with open(path if path else 'proxy.yaml', 'w+') as f:
            proxies = {}

            for i in self._proxies.values():
                proxies[i.address] = {}

                if i.login:
                    proxies[i.address] = {'login': i.login, 'password': i.password}

            ujson.dump(proxies, f, indent=4)

        self.log.info(codes.Code(21201))

    def proxy_load(self, path: str = None) -> Tuple[int, int, int]:
        if not path:
            path = 'proxy.json'

        self.proxy_file_check(path)

        proxy = ujson.load(open(path))
        edited, new = 0, 0

        for k, v in proxy.items():
            if 'login' in v:
                p = Proxy(k, v['login'], v['password'])
            else:
                p = Proxy(k)

            if k in self._proxies and self._proxies[k] != p:
                continue

            if self.proxy_test(p, True):
                with self.lock:
                    self._proxies[k] = p
                if k in self._proxies:
                    edited += 1
                else:
                    new += 1
            else:
                self.log.error(codes.Code(41201, repr(p)))

        if edited or new:
            self.log.warn(codes.Code(31203, str([edited, new])))

        return len(self._proxies), edited, new

    def proxy_add(self, address: str, login: str = None, password: str = None) -> bool:
        if address in self._proxies:
            raise KeyError('Proxy with this address already specified')
        else:
            if self.proxy_test(p := Proxy(address, login, password)):
                with self.lock:
                    self._proxies[address] = p
                self.log.warn(codes.Code(31201))
                return True
            else:
                return False

    def proxy_remove(self, address: str) -> None:
        if address in self._proxies:
            with self.lock:
                del self._proxies[address]
            self.log.warn(codes.Code(31202))
        else:
            raise KeyError('Proxy with this address not specified')

    def proxy_reset(self):
        with self.lock:
            for i in self._proxies.values():
                i.bad = 0
        self.log.warn(codes.Code(31204))

    def proxy_clear(self):
        with self.lock:
            self._proxies.clear()
        self.log.warn(codes.Code(31205))


class SubProvider(ProviderCore):
    _log: logger.Logger
    _script: str
    pos: int

    def __init__(self, script: str):
        self._log = logger.Logger('SPR')
        self._script = script
        self.pos = 0

    def _proxy(self) -> Proxy:
        with self.lock:
            valid = [k for k, v in self._proxies.items() if v.bad < storage.provider.max_bad]

            if valid:
                if self.pos >= len(valid) - 1:
                    self.pos = 0
                else:
                    self.pos += 1
                return self._proxies[valid[self.pos]]
            else:
                return Proxy('')

    def request(
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
    ) -> requests.Response:
        if not isinstance(url, str):
            raise TypeError('url must be str')
        if not isinstance(mode, int):
            raise TypeError('mode must be int')
        if not isinstance(proxy, bool):
            raise TypeError('proxy must be bool')
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

        proxy_ = self._proxy() if proxy else Proxy('')
        start = time.time()

        try:
            if mode == 0:
                if 'type' not in kwargs:
                    raise KeyError('type must be specified for ')

                response = requests.request(
                    kwargs['type'],
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    proxies=proxy_.use(),
                    timeout=timeout
                )
            elif mode == 1:
                response = cfscrape.create_scraper(
                    delay=kwargs['delay'] if 'delay' in kwargs else storage.provider.delay
                ).request(
                    url,
                    params=params,
                    headers=headers,
                    cookies=cookies,
                    proxies=proxy_,
                    timeout=timeout
                )
            else:
                raise ValueError(f'Unknown mode, {mode}')
        except requests.exceptions.ProxyError:
            proxy_.bad += 1
            raise ProxyError
        except (ValueError, KeyError) as e:
            raise e
        except Exception as e:
            proxy_.bad += 1

            if isinstance(e, requests.ConnectionError):
                self._log.warn(codes.Code(31301), self._script)
            elif isinstance(e, requests.Timeout):
                self._log.warn(codes.Code(31302, str(kwargs['timeout'])), self._script)

            if proxy:
                raise type(e)(f'{e} (proxy {proxy_.address})')
            else:
                raise e
        else:
            proxy_.stats = time.time() - start
            return response
