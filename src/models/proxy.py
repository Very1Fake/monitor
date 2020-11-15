from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Union


class ProxyError(Exception):
    pass


@dataclass
class Proxy:
    address: str = None
    login: str = None
    password: str = None
    bad: int = field(compare=False, default=0)
    _stats: deque = field(compare=False, init=False, default_factory=lambda: deque(maxlen=30))

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
