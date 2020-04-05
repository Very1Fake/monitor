import os
from typing import NamedTuple

import yaml


def _is_namedtuple(obj: object) -> bool:
    if hasattr(obj, '_asdict') and hasattr(obj, '_fields'):
        return True
    else:
        return False


def check_config(config_file: str) -> None:
    if os.path.isfile(config_file):
        cache: dict = yaml.safe_load(open(config_file))
        if isinstance(cache, dict):
            different = False
            snapshot_: dict = snapshot()
            for k, v in snapshot_.items():
                if k not in cache or type(v) != type(cache[k]):
                    different = True
                    cache[k] = snapshot_[k]
                else:
                    if isinstance(snapshot_[k], dict):
                        for i in snapshot_[k]:
                            if i not in cache[k]:
                                different = True
                                cache[k][i] = snapshot_[k][i]
            if different:
                yaml.safe_dump(cache, open(config_file, 'w+'))
            return
    yaml.safe_dump(snapshot(), open(config_file, 'w+'))


def reload_config(config_file: str = 'core/config.yaml') -> None:
    check_config(config_file)
    for k, v in yaml.safe_load(open(config_file)).items():
        if k.title().replace('_', '') in categories:
            globals().update({k: globals()[k.title().replace('_', '')]()._replace(**v)})
        elif k.upper() in categories:
            globals().update({k: globals()[k.upper()]()._replace(**v)})
        else:
            globals().update({k: v})


class Main(NamedTuple):
    production: bool = False  # If True monitor will try to avoid fatal errors as possible
    logs_path: str = 'logs'
    cache_path: str = '.cache'


class Analytics(NamedTuple):
    path: str = 'reports'
    interval: int = 300  # Interval between report files creation (in seconds)
    datetime: bool = True  # True - all output time will presented as , False - all output time as timestamps
    datetime_format: str = '%Y-%m-%d %H:%M:%S.%f'
    beautify: bool = False


class ThreadManager(NamedTuple):
    thread_manager_tick: float = 1


class Collector(NamedTuple):
    collector_tick: float = .5  # Delta time for queue manage (in seconds)
    collector_wait: float = 10  # Timeout to join() when turning off monitor (in seconds)
    targets_hashes_size: int = 1024
    success_hashes_time: int = 172800  # How long save hashes of success targets


class Worker(NamedTuple):
    workers_count: int = 3  # Max workers count in normal condition
    workers_proportion: float = 1.5  # Max workers count in overload condition (ceil(workers_count * workers_proportion))
    worker_tick: float = 1  # Delta time for worker run loop
    worker_wait: float = 5


class Queues(NamedTuple):
    task_queue_size: int = 512  # Size for task_queue (will be waiting if full)
    task_queue_put_wait: float = 8
    target_queue_size: int = 512  # Size for target_queue (will be waiting if full)
    target_queue_put_wait: float = 8


class Logger(NamedTuple):
    log_level: int = 4  # (5 - Test, 4 - Debug, 3 - Info, 2 - Warn, 1 - Error, 0 - Fatal)
    log_mode: int = 1  # (0 - off, 1 - Console only, 2 - File only, 3 - Console & File)
    log_utc_time: bool = True  # (True - UTC (Coordinated Universal Time), False - local time)
    log_message_content: int = 1  # (0 - Only code, 1 - Code & Message, 2 - Code & Title & Message


class API(NamedTuple):
    priority_TSmart: list = [10, 0]
    priority_TScheduled: list = [50, 0]
    priority_TInterval: list = [100, 100]  # First value is base priority, second value is range (0 for static priority)


categories: tuple = ('Main', 'Analytics', 'ThreadManager', 'Collector', 'Worker', 'Queues', 'Logger', 'API')

# Global variables
main: Main = Main()
analytics: Analytics = Analytics()
thread_manager: ThreadManager = ThreadManager()
collector: Collector = Collector()
worker: Worker = Worker()
queues: Queues = Queues()
logger: Logger = Logger()
api: API = API()


def snapshot() -> dict:
    snapshot_: dict = {}
    for k, v in globals().items():
        if not k.startswith('__') and not k.startswith('_') and \
                k not in ('categories', 'check_config', 'reload_config', 'snapshot', 'os', 'yaml') and \
                not k[0].isupper():
            if _is_namedtuple(v):
                snapshot_[k] = v._asdict()
            else:
                snapshot_[k] = v
    return snapshot_


if __name__ == 'core.storage':
    reload_config()
