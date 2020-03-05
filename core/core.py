import os
import threading
from queue import Queue, PriorityQueue, Empty, Full
from time import sleep, time
from typing import Tuple

import yaml

from . import api
from . import library
from . import logger
from . import scripts
from . import storage


# TODO: FIX target_done in queue


class MonitorError(Exception):
    pass


class CollectorError(Exception):
    pass


class WorkerError(Exception):
    pass


class Collector(threading.Thread):
    def __init__(
            self,
            script_manager: scripts.ScriptManager,
            storage: dict,
            task_queue: PriorityQueue,
            target_queue: Queue
    ):
        super().__init__(name='Collector', daemon=True)
        self.log: logger.Logger = logger.Logger(self.name)
        self.lock: threading.Lock = threading.Lock()
        self.script_manager: scripts.ScriptManager = script_manager
        self.storage: dict = storage
        self.task_queue: PriorityQueue = task_queue
        self.target_queue: Queue = target_queue
        self.schedule_indices: library.Schedule = library.Schedule()
        self.schedule_targets: library.UniqueSchedule = library.UniqueSchedule(self.storage['targets_hashes_size'])

    def reset_index(self):
        self.schedule_indices.clear()
        for i in self.script_manager.parsers:
            parser = self.script_manager.parsers[i]()
            index = parser.index()
            if isinstance(index, api.IndexInterval):
                self.schedule_indices[time()] = index
            else:
                if self.storage['production']:
                    self.log.error(f'Unknown index while resetting index "{i}"')
                else:
                    self.log.fatal(CollectorError(f'Unknown index while resetting index "{i}"'))

    def insert_index(self, index: api.IndexType):
        if isinstance(index, api.IndexInterval):
            self.schedule_indices[time() + index.interval] = index
        else:
            if self.storage['production']:
                self.log.error(f'Unknown index "{index}"')
            else:
                self.log.fatal(CollectorError(f'Unknown index "{index}"'))

    def reindex(self, start: float):
        to_delete: Tuple[float] = ()
        to_insert: Tuple[api.TargetType] = ()
        for k, v in self.schedule_indices.get_slice_gen(start):  # TODO: Script unload protection
            targets: Tuple[api.TargetType] = self.script_manager.parsers[v.script]().targets()
            if isinstance(targets, tuple) or isinstance(targets, list):
                self.log.debug(f'{len(targets)} targets received from "{v.script}"')
                self.insert_targets(targets)
            else:
                if self.storage['production']:
                    self.log.error(f'Wrong target list received from "{v.script}"')
                else:
                    self.log.fatal(CollectorError(f'Wrong target list received from "{v.script}"'))
            if isinstance(v, api.IndexInterval):
                to_insert += (v,)
                to_delete += (k,)
        for i in to_insert:
            self.insert_index(i)
        self.schedule_indices.pop_item(to_delete)

    def insert_targets(self, targets: Tuple[api.TargetType]):
        for i in targets:
            if isinstance(i, api.IntervalTarget):
                try:
                    self.schedule_targets[time() + i.interval] = i
                except ValueError:
                    pass
            elif isinstance(i, api.CompletedTarget):
                pass  # TODO: CompletedTargetEvent
            elif isinstance(i, api.LostTarget):
                pass  # TODO: LostTarget

    def send_tasks(self, start: float):
        ids: Tuple[float] = ()
        for k, v in self.schedule_targets.get_slice_gen(start):  # TODO: Script unload protection
            try:
                self.task_queue.put(library.PrioritizedItem(100, v), timeout=self.storage['task_queue_put_wait'])
            except Full as e:
                if self.storage['production']:
                    self.log.error(f'Target lost: {v}')
                else:
                    self.log.fatal(CollectorError(f'Target lost: {v}'), e)
            ids += (k,)
        self.schedule_targets.pop_item(ids)

    def run(self) -> None:
        parsers_hash: str = self.script_manager.hash()
        self.script_manager.load_all()
        while True:
            start: float = time()
            if self.storage['state'] == 1:
                if self.script_manager.hash() != parsers_hash:
                    self.log.info('Reindexing parsers')
                    self.reset_index()
                    parsers_hash = self.script_manager.hash()
                    self.log.info('Reindexing parsers complete')
                if any(self.schedule_indices.get_slice_gen(start)):  # TODO: One reindex per tick
                    self.reindex(start)
                if not self.target_queue.empty():
                    targets: Tuple[api.TargetType] = ()
                    try:
                        while True:
                            targets += (self.target_queue.get_nowait(),)
                    except Empty:
                        pass
                    self.insert_targets(targets)
                if any(self.schedule_targets.get_slice_gen(start)):
                    self.send_tasks(start)
            elif self.storage['state'] == 3:
                self.log.info('Closing thread')
                break
            delta: float = time() - start
            sleep(self.storage['collector_tick'] - delta if self.storage['collector_tick'] - delta >= 0 else 0)


class Worker(threading.Thread):
    def __init__(
            self,
            script_manager: scripts.ScriptManager,
            storage: dict,
            task_queue: PriorityQueue,
            target_queue: Queue,
    ):
        super().__init__(name='Worker', daemon=True)
        self.log: logger.Logger = logger.Logger(self.name)
        self.lock: threading.Lock = threading.Lock()
        self.script_manager: scripts.ScriptManager = script_manager
        self.storage: dict = storage
        self.task_queue: PriorityQueue = task_queue
        self.target_queue: Queue = target_queue

    def execute(self, target: api.TargetType) -> bool:
        self.log.debug(f'Executing: {target}')
        status: api.StatusType = self.script_manager.parsers[target.script]().execute(target.data)
        if isinstance(status, api.StatusWaiting):
            self.log.debug(f'Result: {status}')
            try:
                self.target_queue.put(status.target, timeout=self.storage['target_queue_put_wait'])
            except Full as e:
                if self.storage['production']:
                    self.log.error(f'Target lost: {status.target}')
                else:
                    self.log.fatal(WorkerError(f'Target lost: {status.target}'), e)
        elif isinstance(status, api.StatusSuccess):
            self.log.info(f'Item now available: {status.result}')
        elif isinstance(status, api.StatusFail):
            self.log.warn(f'Target lost: {status}')  # TODO: LostTargetEvent here
            return False
        else:
            self.log.debug(f'Result: {status}')
            if self.storage['production']:
                self.log.error(f'Unknown status received while executing "{target}"')
            else:
                self.log.fatal(CollectorError(f'Unknown status received while executing "{target}"'))
            return True
        return True

    def run(self) -> None:
        i = 0
        while True:
            start: float = time()
            if self.storage['state'] == 1:
                target: library.PrioritizedItem = None
                try:
                    target = self.task_queue.get(timeout=self.storage['task_queue_get_wait'])
                except Empty:
                    pass
                if target:
                    self.execute(target.content)
                i += 1
            elif self.storage['state'] == 3:
                self.log.info('Closing thread')
                break
            delta: float = time() - start
            sleep(self.storage['worker_tick'] - delta if self.storage['worker_tick'] - delta >= 0 else 0)


class Main:
    def __init__(self, config_file: str = None):
        if config_file:
            self.config_file = config_file
        else:
            self.config_file = 'core/config.yaml'
        self.lock: threading.Lock = threading.Lock()
        self.storage: dict = {'state': 0}
        self.check_config()
        self.update_storage()
        self.log: logger.Logger = logger.Logger('Core')
        self.script_manager: scripts.ScriptManager = scripts.ScriptManager()
        self.task_queue: PriorityQueue = PriorityQueue(storage.task_queue_size)
        self.target_queue: Queue = Queue(storage.target_queue_size)

    def check_config(self) -> None:
        if os.path.isfile(self.config_file):
            config: dict = yaml.safe_load(open(self.config_file))
            if isinstance(config, dict):
                different = False
                snapshot: dict = storage.snapshot()
                for k in snapshot:
                    if k not in config:
                        different = True
                        config[k] = snapshot[k]
                if different:
                    yaml.safe_dump(config, open(self.config_file, 'w+'))
                return
        yaml.safe_dump(storage.snapshot(), open(self.config_file, 'w+'))

    def update_storage(self, **kwargs) -> None:
        self.lock.acquire()
        for k, v in kwargs.items():
            self.storage[k] = v
        storage.reload_config(self.config_file)
        a = storage.snapshot()
        self.storage.update(a)
        self.lock.release()

    def turn_off(self) -> bool:
        self.update_storage(state=3)
        return True

    def turn_on(self) -> bool:
        self.update_storage(state=1)
        return True

    def start(self):
        self.turn_on()

        collector = Collector(self.script_manager, self.storage, self.task_queue, self.target_queue)
        worker = Worker(self.script_manager, self.storage, self.task_queue, self.target_queue)

        collector.start()
        worker.start()

        try:
            collector.join()
            self.log.fatal(MonitorError('Collector unexpected has turned off'))
        except KeyboardInterrupt:
            self.log.info('Signal Interrupt!')
        finally:
            self.log.info('Turning off...')
            self.turn_off()
            worker.join(self.storage['collector_wait'])
            collector.join(self.storage['worker_wait'])
            self.log.info('Done')
