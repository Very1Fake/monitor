import os
from typing import NamedTuple

import yaml


def _is_namedtuple(obj: object) -> bool:
    if hasattr(obj, '_asdict') and hasattr(obj, '_fields'):
        return True
    else:
        return False


def config_check() -> None:
    if os.path.isfile('./config.yaml'):
        conf: dict = yaml.safe_load(open('./config.yaml'))
        if isinstance(conf, dict):
            different = False
            snapshot_: dict = snapshot()
            for k, v in snapshot_.items():
                if k not in conf or not isinstance(v, type(conf[k])):
                    different = True
                    conf[k] = snapshot_[k]
                else:
                    if isinstance(snapshot_[k], dict):
                        for i in snapshot_[k]:
                            if i not in conf[k]:
                                different = True
                                conf[k][i] = snapshot_[k][i]
            if not different:
                return
        yaml.safe_dump(snapshot(), open('./config.yaml', 'w+'))
    else:
        yaml.safe_dump(snapshot(), open('./config.yaml', 'w+'))


def config_load() -> None:
    config_check()
    for k, v in yaml.safe_load(open('./config.yaml')).items():
        if k in categories:
            globals().update({k: globals()[k]._replace(**v)})
        else:
            globals().update({k: v})


def config_dump() -> None:
    config_check()
    yaml.safe_dump(snapshot(), open('./config.yaml', 'w+'))


class Main(NamedTuple):
    production: bool = False  # If True monitor will try to avoid fatal errors as possible
    logs_path: str = 'logs'
    cache_path: str = 'cache'


class Analytics(NamedTuple):
    path: str = 'reports'
    datetime: bool = True  # True - all output time will presented as , False - all output time as timestamps
    datetime_format: str = '%Y-%m-%d %H:%M:%S.%f'
    beautify: bool = False


class ThreadManager(NamedTuple):
    tick: float = 1.
    lock_ticks: int = 16  # How much ticks lock can be acquired, then it will released


class Pipe(NamedTuple):
    tick: float = .5  # Delta time for queue manage (in seconds)
    wait: float = 10.  # Timeout to join() when turning off monitor (in seconds)
    success_hashes_time: int = 172800  # How long save hashes of success targets


class Worker(NamedTuple):
    count: int = 5  # Max workers count in normal condition
    tick: float = 1.  # Delta time for worker run loop
    wait: float = 5.


class IndexWorker(NamedTuple):
    count: int = 5
    tick: float = 1.
    wait: int = 7


class Queues(NamedTuple):
    index_queue_size: int = 256  # Size for index_queue (will be waiting if full)
    index_queue_put_wait: float = 8.
    target_queue_size: int = 512  # Size for target_queue (will be waiting if full)
    target_queue_put_wait: float = 8.


class Logger(NamedTuple):
    level: int = 4  # (5 - Test, 4 - Debug, 3 - Info, 2 - Warn, 1 - Error, 0 - Fatal)
    mode: int = 1  # (0 - off, 1 - Console only, 2 - File only, 3 - Console & File)
    utc_time: bool = True  # (True - UTC (Coordinated Universal Time), False - local time)
    message_content: int = 1  # (0 - Only code, 1 - Code & Message, 2 - Code & Title & Message


class API(NamedTuple):
    priority_IOnce: int = 10
    priority_IInterval: int = 50
    priority_TSmart: list = [10, 0]
    priority_TScheduled: list = [50, 0]
    priority_TInterval: list = [100, 100]  # First value is base priority, second value is range (0 for static priority)
    priority_interval_default: int = 100
    priority_target_default: int = 1001


categories: tuple = (
    'main',
    'analytics',
    'thread_manager',
    'pipe',
    'worker',
    'index_worker',
    'queues',
    'logger',
    'api'
)

# Global variables
main: Main = Main()
analytics: Analytics = Analytics()
thread_manager: ThreadManager = ThreadManager()
pipe: Pipe = Pipe()
worker: Worker = Worker()
index_worker: IndexWorker = IndexWorker()
queues: Queues = Queues()
logger: Logger = Logger()
api: API = API()


def snapshot() -> dict:
    snapshot_: dict = {}
    for k, v in globals().items():
        if not k.startswith('__') and not k.startswith('_') and \
                k not in ('categories', 'config_check', 'config_load', 'config_dump', 'snapshot', 'os', 'yaml') and \
                not k[0].isupper():
            if _is_namedtuple(v):
                snapshot_[k] = v._asdict()
            else:
                snapshot_[k] = v
    return snapshot_


if __name__ == 'core.storage':
    config_check()
