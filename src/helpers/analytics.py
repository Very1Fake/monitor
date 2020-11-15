import os
import time
from datetime import datetime
from statistics import mean

import ujson

from src import __version__
from src.containers.scripter import ScriptManager
from src.containers.thread_manager import ThreadManager
from src.models.provider import Provider
from src.utils import store
from src.utils.storage import ReportStorage


class Analytics:
    start_time: datetime = datetime.utcnow()

    provider: Provider
    script_manager: ScriptManager
    thread_manager: ThreadManager

    def __init__(self, provider: Provider, script_manager: ScriptManager, thread_manager: ThreadManager):
        self.provider = provider
        self.script_manager = script_manager
        self.thread_manager = thread_manager

    @staticmethod
    def _check() -> None:
        if not os.path.isdir(store.analytics.path):
            os.makedirs(store.analytics.path)

    def info_workers(self) -> dict:
        with self.thread_manager.lock:
            return {
                'count': self.thread_manager.workers.__len__(),
                'speed': round(
                    sum([i.speed for i in self.thread_manager.workers.values()]),
                    3
                ),
                'list': [
                    {
                        'name': i.name,
                        'speed': i.speed,
                        'start_time': datetime.utcfromtimestamp(i.start_time).strftime(
                            store.analytics.datetime_format
                        ) if store.analytics.datetime else str(i.start_time),
                        'uptime': (datetime.utcnow() - datetime.utcfromtimestamp(i.start_time)).total_seconds() if
                        store.analytics.datetime else str(time.time() - i.start_time)
                    } for i in self.thread_manager.workers.values()
                ]
            }

    def info_worker(self, id_: int) -> dict:
        if isinstance(id_, int):
            with self.thread_manager.lock:
                if id_ in self.thread_manager.workers:
                    worker = self.thread_manager.workers[id_]
                    return {
                        'id': worker.id,
                        'name': worker.name,
                        'start_time': worker.start_time,
                        'uptime': round(time.time() - worker.start_time, 3),
                        'state': worker.state,
                        'speed': worker.speed,
                        'last_tick': worker.last_tick,
                        'freeze_time': time.time() - worker.last_tick
                    }
                else:
                    return {}
        else:
            raise TypeError('id_ must be int')

    def info_catalog_worker(self, id_: int) -> dict:
        if isinstance(id_, int):
            with self.thread_manager.lock:
                if id_ in self.thread_manager.catalog_workers:
                    worker = self.thread_manager.catalog_workers[id_]
                    return {
                        'id': worker.id,
                        'name': worker.name,
                        'start_time': worker.start_time,
                        'uptime': round(time.time() - worker.start_time, 3),
                        'state': worker.state,
                        'speed': worker.speed,
                        'last_tick': worker.last_tick,
                        'freeze_time': time.time() - worker.last_tick
                    }
                else:
                    return {}
        else:
            raise TypeError('id_ must be int')

    def proxy(self, proxy: str) -> dict:
        with self.provider.lock:
            if proxy in self.provider._proxies:
                proxy = self.provider._proxies[proxy]
                stats = [i for i in proxy.stats if i > 0]
                return {
                    'address': proxy.address,
                    'bad': proxy.bad,
                    'min': min(stats) if stats else 0,
                    'avg': mean(stats) if stats else 0,
                    'max': max(stats) if stats else 0
                }
            else:
                return {}

    def proxies(self) -> dict:
        with self.provider.lock:
            proxies = [self.proxy(i) for i in self.provider._proxies]

        return {
            'count': len(proxies),
            'bad': sum([i['bad'] for i in proxies]),
            'min': min([i['min'] for i in proxies]),
            'avg': mean([i['avg'] for i in proxies]),
            'max': max([i['max'] for i in proxies]),
            'proxies': proxies
        }

    def snapshot(self, type_: int = 1) -> dict:
        end_time = datetime.utcnow()
        return {
            'main': {
                'start_time': self.start_time.strftime(store.analytics.datetime_format) if
                store.analytics.datetime else str(self.start_time.timestamp()),
                'uptime': str(end_time - self.start_time) if
                store.analytics.datetime else (end_time - self.start_time).total_seconds(),
                'end_time': end_time.strftime(store.analytics.datetime_format) if
                store.analytics.datetime else end_time.timestamp(),
                'type': type_,
            },
            'scripts': {
                'indexed': self.script_manager.index.index.__len__(),
                'loaded': self.script_manager.scripts.__len__(),
                'parsers': self.script_manager.parsers.__len__(),
                'event_executors': self.script_manager.event_handler.executors.__len__()
            },
            'workers': self.info_workers(),
            'catalog_workers': self.info_workers(),
            'system': {
                'version': __version__,
                'analytics_version': 2
            }
        }

    def dump(self, type_: int = 1) -> None:
        self._check()

        suffix = ''
        if type_ == 0:
            suffix = 'first_'
        elif type_ == 2:
            suffix = 'final_'

        ujson.dump(self.snapshot(type_), ReportStorage().file(
            f'/report_{suffix}_{datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")}.json',
            'w+'
        ), indent=2 if store.analytics.beautify else 0)
