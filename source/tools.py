import time
from datetime import datetime
from typing import Iterator

time_format: str = "%Y-%m-%d %H:%M:%S"


def get_time(global_: bool = True, name: bool = False) -> str:
    if global_:
        time_ = datetime.utcnow()
    else:
        time_ = datetime.now()
    return datetime.strftime(time_, time_format.replace(' ', '_') if name else time_format)


def smart_gen(time_: float, length: int, scatter: int = 1) -> Iterator[float]:
    if length < 0:
        raise ValueError('length cannot be less than 0')
    if scatter < 1:
        raise ValueError('scatter cannot be less than 1')

    for i in range(length - 1, -1, -scatter):
        yield time_ - 2 ** i
    else:
        yield time_


def smart_extractor(gen: Iterator[float], now: float = None) -> float:
    if not now:
        now = time.time()
    for i in gen:
        if i > now:
            return i

