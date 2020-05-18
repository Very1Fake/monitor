import json
import os
import time
from datetime import datetime

from . import storage, core, __version__


class Analytics:
    def __init__(self):
        self.start_time: datetime = datetime.utcnow()
        self.active: bool = True

    @staticmethod
    def _check() -> None:
        if not os.path.isdir(storage.analytics.path):
            os.makedirs(storage.analytics.path)

    @staticmethod
    def info_workers() -> dict:
        with core.monitor.thread_manager.lock:
            return {
                'count': core.monitor.thread_manager.workers.__len__(),
                'speed': round(
                    sum([i.speed for i in core.monitor.thread_manager.workers.values()]),
                    3
                ),
                'list': [
                    {
                        'name': i.name,
                        'speed': i.speed,
                        'start_time': datetime.utcfromtimestamp(i.start_time).strftime(
                            storage.analytics.datetime_format
                        ) if storage.analytics.datetime else str(i.start_time),
                        'uptime': (datetime.utcnow() - datetime.utcfromtimestamp(i.start_time)).total_seconds() if
                        storage.analytics.datetime else str(time.time() - i.start_time)
                    } for i in core.monitor.thread_manager.workers.values()
                ]
            }

    @staticmethod
    def info_worker(id_: int) -> dict:
        if isinstance(id_, int):
            with core.monitor.thread_manager.lock:
                if id_ in core.monitor.thread_manager.workers:
                    worker = core.monitor.thread_manager.workers[id_]
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

    @staticmethod
    def info_index_worker(id_: int) -> dict:
        if isinstance(id_, int):
            with core.monitor.thread_manager.lock:
                if id_ in core.monitor.thread_manager.index_workers:
                    worker = core.monitor.thread_manager.index_workers[id_]
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

    def snapshot(self, type_: int = 1) -> dict:
        end_time = datetime.utcnow()
        return {
            'main': {
                'start_time': self.start_time.strftime(storage.analytics.datetime_format) if
                storage.analytics.datetime else str(self.start_time.timestamp()),
                'uptime': str(end_time - self.start_time) if
                storage.analytics.datetime else (end_time - self.start_time).total_seconds(),
                'end_time': end_time.strftime(storage.analytics.datetime_format) if
                storage.analytics.datetime else end_time.timestamp(),
                'type': type_,
            },
            'scripts': {
                'indexed': core.script_manager.index.index.__len__(),
                'loaded': core.script_manager.scripts.__len__(),
                'parsers': core.script_manager.parsers.__len__(),
                'event_executors': core.script_manager.event_handler.executors.__len__()
            },
            'workers': self.info_workers(),
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

        json.dump(self.snapshot(type_), open(
            storage.analytics.path + f'/report_{suffix}'
                                     f'{datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")}.json',
            'w+'
        ), indent=4 if storage.analytics.beautify else None)
