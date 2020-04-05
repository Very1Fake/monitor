import json
import os
import sys
import threading
import time
from datetime import datetime

from core import storage, core


class Analytics:
    def __init__(self):
        self.start_time: datetime = datetime.utcnow()
        self.active: bool = True
        self.thread: threading.Thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    @staticmethod
    def _check() -> None:
        if not os.path.isdir(storage.analytics.path):
            os.makedirs(storage.analytics.path)

    def loop(self):
        last: float = time.time()
        while self.active:
            if storage.analytics.interval != 0 and last + storage.analytics.interval < time.time():
                self.dump()
                last: float = time.time()
            time.sleep(1)

    def snapshot(self, type_: int = 1) -> dict:
        end_time = datetime.utcnow()
        return {
            'main': {
                'version': sys.modules['core'].__version__,
                'start_time': self.start_time.strftime(storage.analytics.datetime_format) if
                storage.analytics.datetime else str(self.start_time.timestamp()),
                'uptime': str(end_time - self.start_time) if
                storage.analytics.datetime else (end_time - self.start_time).total_seconds(),
                'end_time': end_time.strftime(storage.analytics.datetime_format) if
                storage.analytics.datetime else end_time.timestamp(),
                'type': type_,
                'workers': sys.modules['__main__'].monitor.thread_manager.workers.__len__()
            },
            'scripts': {
                'indexed': core.script_manager.index.index.__len__(),
                'loaded': core.script_manager.scripts.__len__(),
                'parsers': core.script_manager.parsers.__len__(),
                'event_executors': core.script_manager.event_handler.executors.__len__()
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
        ))

    def stop(self):
        self.active = False
        self.thread.join()
        self.dump(2)


if __name__ == 'core.analytics':
    analytics: Analytics = Analytics()
