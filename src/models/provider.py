from threading import current_thread, RLock
from typing import Any, Dict, Tuple, Union

import urllib3
from requests import get, Response, Session
from requests.adapters import HTTPAdapter
from requests.cookies import cookiejar_from_dict
from requests.exceptions import ChunkedEncodingError, ConnectionError, ProxyError, RequestException, Timeout
from ujson import dump, load, loads

from src.models.proxy import Proxy
from src.store import provider, sub_provider
from src.utils.log import Logger
from src.utils.protocol import Code
from src.utils.storage import KernelStorage

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# TODO: Optimize project restructuring
# TODO: Add export/import of Proxy.bad


# Exceptions


class ProviderError(Exception):
    pass


class SubProviderError(Exception):
    pass


# Classes


class ProviderCore:
    lock: RLock = RLock()
    _proxies: Dict[str, Proxy] = {}


class Provider(ProviderCore):
    log: Logger

    def __init__(self) -> None:
        self.log = Logger('PR')
        self.proxy_load()

    @property
    def proxies(self) -> Dict[str, Proxy]:
        return self._proxies

    def proxy_test(self, proxy: Proxy, force: bool = False) -> bool:
        try:
            self.log.info(Code(21202, repr(proxy)))
            if (code := get(provider.test_url, proxies=proxy.use(),
                            timeout=provider.proxy_timeout).status_code) != 200:
                self.log.info(Code(41202, repr(proxy)))
                if force:
                    return False
                else:
                    raise ProviderError(f'Bad proxy ({code})')
        except (ConnectionError, Timeout):
            self.log.info(Code(41202, repr(proxy)))
            if force:
                return False
            else:
                raise ProviderError(f'Proxy not available or too slow (timeout: {provider.proxy_timeout})')
        self.log.info(Code(21203, repr(proxy)))
        return True

    @staticmethod
    def proxy_file_check() -> None:
        if KernelStorage().check('proxy.json'):
            file = KernelStorage().file('proxy.json', 'r' if KernelStorage().check('proxy.json') else 'w+').read()

            if file:
                try:
                    if isinstance(file := loads(file), dict):
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
                dump({}, KernelStorage().file('proxy.json', 'w+'), indent=4)
        else:
            dump({}, KernelStorage().file('proxy.json', 'w+'), indent=4)

    def proxy_dump(self) -> None:
        with KernelStorage().file('proxy.json', 'w+') as f:
            proxies = {}

            for i in self._proxies.values():
                proxies[i.address] = {}

                if i.login:
                    proxies[i.address] = {'login': i.login, 'password': i.password}

            dump(proxies, f, indent=4)

        self.log.info(Code(21201))

    def proxy_load(self) -> Tuple[int, int, int]:
        self.proxy_file_check()

        proxy = load(KernelStorage().file('proxy.json'))
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
    _log: Logger
    _script: str
    pos: int

    def __init__(self, script: str):
        self._log = Logger('SPR')
        self._script = script
        self.pos = 0

    def _proxy(self) -> Proxy:
        with self.lock:
            valid = [k for k, v in self._proxies.items() if v.bad < provider.max_bad]

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
    ) -> Tuple[bool, Union[Response, Exception]]:
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

        with Session() as sess:
            sess.cookies = cookiejar_from_dict(cookies)
            sess.headers = headers
            sess.max_redirects = sub_provider.max_redirects
            sess.params = params
            sess.verify = sub_provider.verify
            sess.mount('http://', HTTPAdapter(pool_maxsize=1, pool_connections=1))

            if sub_provider.compression:
                sess.headers.update({'Accept-Encoding': sub_provider.comp_type})

            for i in range(sub_provider.max_retries):
                self._log.debug(f'Try #{i}')
                try:
                    resp = sess.request(method, url, data=data)
                except (Timeout, ProxyError, ChunkedEncodingError) as e:
                    if proxy:
                        proxy_.bad += 1
                        proxy_.stats = -1
                    self._log.test(Code(11301, f'{type(e)}: {e!s}'), current_thread().name)
                    continue
                except RequestException as e:
                    self._log.error(Code(41301, f'{type(e)}: {e!s}'), current_thread().name)
                    return False, e
                else:
                    proxy_.stats = resp.elapsed.microseconds * 1000000
                    return True, resp
