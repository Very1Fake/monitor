from os.path import isfile

from yaml import safe_dump, safe_load


def check_config(config_file: str) -> None:
    if isfile(config_file):
        cache: dict = safe_load(open(config_file))
        if isinstance(cache, dict):
            different = False
            temp_snapshot: dict = snapshot()
            for k in temp_snapshot:
                if k not in cache:
                    different = True
                    cache[k] = temp_snapshot[k]
            if different:
                safe_dump(cache, open(config_file, 'w+'))
            return
    safe_dump(snapshot(), open(config_file, 'w+'))


def reload_config(config_file: str = 'core/config.yaml') -> None:
    check_config(config_file)
    globals().update(safe_load(open(config_file)))


# Main
production: bool = False  # If True monitor will try to avoid fatal errors as possible
logs_folder: str = 'logs'
cache_folder: str = '.cache'

# ThreadManager
thread_manager_tick: float = 1

# Collector
collector_tick: float = .5  # Delta time for queue manage (in seconds)
collector_wait: float = 10  # Timeout to join() when turning off monitor (in seconds)
targets_hashes_size: int = 1024
success_hashes_time: int = 172800  # How long save hashes of success targets

# Worker
workers_count: int = 3  # Max workers count in normal condition
workers_proportion: float = 1.5  # Max workers count in overload condition (ceil(workers_count * workers_proportion))
worker_tick: float = 1  # Delta time for worker run loop
worker_wait: float = 5

# Queues
task_queue_size: int = 512  # Size for task_queue (will be waiting if full)
task_queue_put_wait: float = 8
target_queue_size: int = 512  # Size for target_queue (will be waiting if full)
target_queue_put_wait: float = 8

# Logger
log_level: int = 4  # (5 - Test, 4 - Debug, 3 - Info, 2 - Warn, 1 - Error, 0 - Fatal)
log_mode: int = 1  # (0 - off, 1 - Console only, 2 - File only, 3 - Console & File)
log_utc_time: bool = True  # (True - UTC (Coordinated Universal Time), False - local time)
log_message_content: int = 1  # (0 - Only code, 1 - Code & Message, 2 - Code & Title & Message

# API
priority_TSmart: list = [10, 0]
priority_TScheduled: list = [50, 0]
priority_TInterval: list = [100, 100]  # First value is base priority, second value is range (0 for static priority)


def snapshot() -> dict:
    return {k: v for k, v in globals().items() if
            not k.startswith('__') and not k.startswith('_') and k not in (
                'check_config', 'isfile', 'reload_config', 'safe_dump', 'safe_load', 'snapshot'
            )}


if __name__ == 'core.storage':
    reload_config()
