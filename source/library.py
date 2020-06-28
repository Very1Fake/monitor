import os
from dataclasses import dataclass, field
from typing import Any, List, Dict, Union

import cfscrape
import requests
import yaml

from . import codes
from . import logger
from . import storage


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


# Storage classes


class ProviderCore:
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
                    self.log.error(codes.Code(41201, i))
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
    _script: str
    pos: int

    def __init__(self, script: str):
        self._log = logger.Logger('SPR')
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
        except requests.exceptions.ProxyError:
            raise ProxyError
        except Exception as e:
            if isinstance(e, requests.ConnectionError):
                self._log.warn(codes.Code(31301), self._script)
            elif isinstance(e, requests.Timeout):
                self._log.warn(codes.Code(31302, str(kwargs['timeout'])), self._script)

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

        for i in range(5):  # Optimize (now 5-times loop if bad proxy)
            if proxy:
                proxy_ = self._proxy()
            else:
                proxy_ = Proxy('')

            try:
                if mode == 0:
                    return self._get(
                        requests,
                        url,
                        params=params,
                        headers=headers,
                        cookies=cookies,
                        proxies=proxy_.use(),
                        timeout=timeout
                    ).text
                elif mode == 1:
                    return self._get(
                        cfscrape.create_scraper(delay=kwargs['delay'] if 'delay' in kwargs else storage.provider.delay),
                        url,
                        params=params,
                        headers=headers,
                        cookies=cookies,
                        proxies=proxy_.use(),
                        timeout=timeout
                    ).text
                else:
                    raise ValueError(f'Unknown mode, {mode}')
            except ProxyError:
                proxy_.bad += 1
            except SubProviderError as e:
                proxy_.bad += 1

                if proxy and proxy_.url:  # Make optimization
                    raise type(e)(f'{e} (proxy {proxy_.url})')
                else:
                    raise e
        else:
            if proxy and proxy_.url:
                raise ProxyError(proxy_.url)
