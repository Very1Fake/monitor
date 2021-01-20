import collections
import threading
from dataclasses import dataclass, field
from typing import Any, List, Dict, Union, Tuple

import requests
import ujson
import urllib3
from requests import adapters, exceptions
from requests.cookies import cookiejar_from_dict

from . import logger
from . import storage
from .codes import Code
from .tools import SmartGen, SmartGenType, MainStorage, ScriptStorage

# TODO: Add export/import of Proxy.bad


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    gen: SmartGenType = field(compare=False, repr=False)
    expired: bool = field(init=False)

    def __post_init__(self):
        if not issubclass(type(self.gen), SmartGen):
            raise TypeError('gen type must be subclass of SmartGen')

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
            self.log.info(Code(21202, repr(proxy)))
            if (code := requests.get(storage.provider.test_url, proxies=proxy.use(),
                                     timeout=storage.provider.proxy_timeout).status_code) != 200:
                self.log.info(Code(41202, repr(proxy)))
                if force:
                    return False
                else:
                    raise ProviderError(f'Bad proxy ({code})')
        except (requests.ConnectionError, requests.Timeout):
            self.log.info(Code(41202, repr(proxy)))
            if force:
                return False
            else:
                raise ProviderError(f'Proxy not available or too slow (timeout: {storage.provider.proxy_timeout})')
        self.log.info(Code(21203, repr(proxy)))
        return True

    @staticmethod
    def proxy_file_check() -> None:
        if MainStorage().check('proxy.json'):
            file = MainStorage().file('proxy.json', 'r' if MainStorage().check('proxy.json') else 'w+').read()

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
                ujson.dump({}, MainStorage().file('proxy.json', 'w+'), indent=4)
        else:
            ujson.dump({}, MainStorage().file('proxy.json', 'w+'), indent=4)

    def proxy_dump(self) -> None:
        with MainStorage().file('proxy.json', 'w+') as f:
            proxies = {}

            for i in self._proxies.values():
                proxies[i.address] = {}

                if i.login:
                    proxies[i.address] = {'login': i.login, 'password': i.password}

            ujson.dump(proxies, f, indent=4)

        self.log.info(Code(21201))

    def proxy_load(self) -> Tuple[int, int, int]:
        self.proxy_file_check()

        proxy = ujson.load(MainStorage().file('proxy.json'))
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
                self.log.error(Code(41201, repr(p)))

        if edited or new:
            self.log.warn(Code(31203, str([edited, new])))

        return len(self._proxies), edited, new

    def proxy_add(self, address: str, login: str = None, password: str = None) -> bool:
        if address in self._proxies:
            raise KeyError('Proxy with this address already specified')
        else:
            if self.proxy_test(p := Proxy(address, login, password)):
                with self.lock:
                    self._proxies[address] = p
                self.log.warn(Code(31201))
                return True
            else:
                return False

    def proxy_remove(self, address: str) -> None:
        if address in self._proxies:
            with self.lock:
                del self._proxies[address]
            self.log.warn(Code(31202))
        else:
            raise KeyError('Proxy with this address not specified')

    def proxy_reset(self):
        with self.lock:
            for i in self._proxies.values():
                i.bad = 0
        self.log.warn(Code(31204))

    def proxy_clear(self):
        with self.lock:
            self._proxies.clear()
        self.log.warn(Code(31205))


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
            proxy: bool = False,
            *,
            params: Dict[str, str] = None,
            headers: Dict[str, str] = None,
            cookies: Dict[str, Any] = None,
            data: Union[str, bytes] = '',
            method: str = 'get'
    ) -> Tuple[bool, Union[requests.Response, Exception]]:
        if not isinstance(url, str):
            raise TypeError('url must be str')
        if not isinstance(proxy, bool):
            raise TypeError('proxy must be bool')
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
        if not isinstance(data, (str, bytes)):
            raise TypeError('data must be str or bytes')
        if not isinstance(method, str):
            raise TypeError('method must be str')

        proxy_ = self._proxy() if proxy else Proxy('')

        with requests.Session() as sess:
            sess.cookies = cookiejar_from_dict(cookies)
            sess.headers = headers
            sess.max_redirects = storage.sub_provider.max_redirects
            sess.params = params
            sess.verify = storage.sub_provider.verify
            sess.mount('http://', adapters.HTTPAdapter(pool_maxsize=1, pool_connections=1))

            if storage.sub_provider.compression:
                sess.headers.update({'Accept-Encoding': storage.sub_provider.comp_type})

            for i in range(storage.sub_provider.max_retries):
                try:
                    resp = sess.request(method, url, data=data)
                except (exceptions.Timeout, exceptions.ProxyError, exceptions.ChunkedEncodingError) as e:
                    if proxy:
                        proxy_.bad += 1
                        proxy_.stats = -1
                    self._log.test(Code(11301, f'{type(e)}: {e!s}'), threading.current_thread().name)
                    continue
                except exceptions.RequestException as e:
                    self._log.error(Code(41301, f'{type(e)}: {e!s}'), threading.current_thread().name)
                    return False, e
                else:
                    proxy_.stats = resp.elapsed.microseconds * 1000000
                    return True, resp


class Keywords:
    __slots__ = ['abs', 'pos', 'neg', 'store', '_log', '_lock']
    _log: logger.Logger
    _lock: threading.RLock

    abs: list
    pos: list
    neg: list

    store: ScriptStorage

    def __init__(self, script: str):
        self._log = logger.Logger(f'KW/{script}')
        self._lock = threading.RLock()

        self.abs = []
        self.pos = []
        self.neg = []

        self.store = ScriptStorage(script)

        self.load()

    def export(self) -> dict:
        with self._lock:
            return {'absolute': self.abs, 'positive': self.pos, 'negative': self.neg}

    def check(self, s: str, div: str = ' ') -> bool:
        has_pos: bool = False
        for i in s.split(div):
            if i.lower() in self.abs:
                return True
            elif i.lower() in self.neg:
                return False
            elif not has_pos and i.lower() in self.pos:
                has_pos = True
        else:
            return has_pos

    def dump(self) -> int:
        with self._lock:
            self._log.info(Code(21501))
            if self.store.check('keywords.json'):
                kw = ujson.load(self.store.file('keywords.json'))

                if isinstance(kw, dict) and 'absolute' in kw and isinstance(kw['absolute'], list):
                    self.abs.sort()
                    kw['absolute'].sort()
                    if self.abs == kw['absolute'] and 'positive' in kw and isinstance(kw['positive'], list):
                        self.pos.sort()
                        kw['positive'].sort()
                        if self.pos == kw['positive'] and 'negative' in kw and isinstance(kw['negative'], list):
                            self.neg.sort()
                            kw['negative'].sort()
                            if self.neg == kw['negative']:
                                self._log.info(Code(21504))
                                return 1

            ujson.dump(self.export(), self.store.file('keywords.json', 'w+'), indent=2)
            self._log.info(Code(21502))
            return 0

    def sync(self) -> int:
        with self._lock:
            self._log.info(Code(21505))
            if self.store.check('keywords.json'):
                kw = ujson.load(self.store.file('keywords.json'))

                if isinstance(kw, dict):
                    if 'absolute' in kw and isinstance(kw['absolute'], list):
                        for i in kw['absolute']:
                            self.add_abs(i)
                    if 'positive' in kw and isinstance(kw['positive'], list):
                        for i in kw['positive']:
                            self.add_pos(i)
                    if 'negative' in kw and isinstance(kw['negative'], list):
                        for i in kw['negative']:
                            self.add_neg(i)
                    self._log.info(Code(21506))
                    return 0
                else:
                    raise TypeError('keywords.json must be object')
            else:
                self._log.warn(Code(31501))
                ujson.dump({'absolute': [], 'positive': [], 'negative': []},
                           self.store.file('keywords.json', 'w+'), indent=2)
                return 1

    def clear(self) -> int:
        with self._lock:
            self._log.info(Code(21503))
            self.abs.clear()
            self.pos.clear()
            self.neg.clear()
            self._log.info(Code(21504))
            return 0

    def load(self) -> int:
        with self._lock:
            self._log.info(Code(21507))
            self.clear()
            self.sync()
            self._log.info(Code(21508))
            return 0

    def add_abs(self, kw: str) -> int:
        with self._lock:
            if isinstance(kw, str):
                if kw in self.abs:
                    self._log.warn(Code(31512, kw))
                    return 2
                else:
                    self.abs.append(kw)
                    return 0
            else:
                self._log.warn(Code(31511, kw))
                return 1

    def add_pos(self, kw: str) -> int:
        with self._lock:
            if isinstance(kw, str):
                if kw in self.pos:
                    self._log.warn(Code(31522, kw))
                    return 2
                else:
                    self.pos.append(kw)
                    return 0
            else:
                self._log.warn(Code(31521, kw))
                return 1

    def add_neg(self, kw: str) -> int:
        with self._lock:
            if isinstance(kw, str):
                if kw in self.neg:
                    self._log.warn(Code(31532, kw))
                    return 2
                else:
                    self.neg.append(kw)
                    return 0
            else:
                self._log.warn(Code(31531, kw))
                return 1

    def remove_abs(self, kw: str) -> int:
        with self._lock:
            if kw in self.abs:
                self.abs.remove(kw)
                return 0
            else:
                return 1

    def remove_pos(self, kw: str) -> int:
        with self._lock:
            if kw in self.pos:
                self.pos.remove(kw)
                return 0
            else:
                return 1

    def remove_neg(self, kw: str) -> int:
        with self._lock:
            if kw in self.neg:
                self.neg.remove(kw)
                return 0
            else:
                return 1
