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
                if k not in conf or not isinstance(conf[k], type(v)):
                    different = True
                    conf[k] = v
                elif isinstance(snapshot_[k], dict):
                    for k2, v2 in snapshot_[k].items():
                        if k2 not in conf[k] or not isinstance(conf[k][k2], type(v2)):
                            different = True
                            conf[k][k2] = v2
            if different:
                yaml.safe_dump(conf, open('./config.yaml', 'w+'))
            return
    yaml.safe_dump(snapshot(), open('./config.yaml', 'w+'))


def config_load() -> None:
    config_check()
    for k, v in yaml.safe_load(open('./config.yaml')).items():
        if k in categories:
            globals().update({k: globals()[k]._replace(**v)})
        else:
            globals().update({k: v})


def config_dump() -> None:
    yaml.safe_dump(snapshot(), open('./config.yaml', 'w+'))


class Main(NamedTuple):
    production: bool = False  # If True monitor will try to avoid fatal errors as possible
    logs_path: str = 'logs'


class Cache(NamedTuple):
    path: str = 'cache'
    item_time: int = 1209600
    target_time: int = 604800  # How long save hashes of success & failed targets


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


class Worker(NamedTuple):
    count: int = 5  # Max workers count in normal condition
    tick: float = 1.  # Delta time for worker run loop
    wait: float = 5.


class CatalogWorker(NamedTuple):
    count: int = 5
    tick: float = 1.
    wait: int = 7


class Queues(NamedTuple):
    catalog_queue_size: int = 256  # Size for catalog_queue (will be waiting if full)
    catalog_queue_put_wait: float = 8.
    target_queue_size: int = 512  # Size for target_queue (will be waiting if full)
    target_queue_put_wait: float = 8.


class Logger(NamedTuple):
    level: int = 4  # (5 - Test, 4 - Debug, 3 - Info, 2 - Warn, 1 - Error, 0 - Fatal)
    mode: int = 1  # (0 - off, 1 - Console only, 2 - File only, 3 - Console & File)
    utc_time: bool = True  # (True - UTC (Coordinated Universal Time), False - local time)
    message_content: int = 1  # (0 - Only code, 1 - Code & Message, 2 - Code & Title & Message


class Priority(NamedTuple):
    CSmart: int = 10
    CScheduled: int = 50
    CInterval: int = 100
    TSmart: list = [10, 0]
    TScheduled: list = [50, 0]
    TInterval: list = [100, 100]  # First value is base priority, second value is range (0 for static priority)
    RTSmart: list = [10, 0]
    RTScheduled: list = [50, 0]
    RTInterval: list = [100, 100]  # First value is base priority, second value is range (0 for static priority)
    catalog_default: int = 100
    target_default: int = 1001


class Provider(NamedTuple):
    max_bad: int = 25
    test_url: str = 'http://google.com/'
    proxy_timeout: float = 3.0


class SubProvider(NamedTuple):
    max_redirects: int = 5
    max_retries: int = 3
    connect_timeout: float = 1.
    read_timeout: float = 2.


class EventHandler(NamedTuple):
    tick: float = .1
    wait: float = 3.


categories: tuple = (
    'main',
    'cache',
    'analytics',
    'thread_manager',
    'pipe',
    'worker',
    'catalog_worker',
    'queues',
    'logger',
    'priority',
    'provider',
    'sub_provider',
    'event_handler'
)

# Global variables
main: Main = Main()
cache: Cache = Cache()
analytics: Analytics = Analytics()
thread_manager: ThreadManager = ThreadManager()
pipe: Pipe = Pipe()
worker: Worker = Worker()
catalog_worker: CatalogWorker = CatalogWorker()
queues: Queues = Queues()
logger: Logger = Logger()
priority: Priority = Priority()
provider: Provider = Provider()
sub_provider: SubProvider = SubProvider()
event_handler: EventHandler = EventHandler()


def snapshot() -> dict:
    snapshot_: dict = {}
    for k, v in globals().items():
        if k in categories:
            snapshot_[k] = v._asdict()
    return snapshot_


if __name__ == 'storage.storage':
    config_check()
