from yaml import safe_load

# Main
production: bool = False  # If True monitor will try to avoid fatal errors as possible

# Core
producer_tick: float = 1  # Delta time for queue manage (in seconds)
consumer_tick: float = .5  # Delta time for worker run loop

# Queues
task_queue_size: int = 64  # Size for task_queue (will be waiting if full)
task_queue_get_wait: float = 1  # Time for wait for get() (in seconds)
target_queue_size: int = 32  # Size for target_queue (will be waiting if full)

# Logger
log_level: int = 5  # (5 - Test, 4 - Debug, 3 - Info, 2 - Warn, 1 - Error, 0 - Fatal)
log_mode: int = 0  # (0 - off, 1 - Console only, 2 - File only, 3 - Console & File)
log_time: bool = True  # (True - UTC (Coordinated Universal Time), False - local time)


def reload_config(config_file: str = 'core/config.yaml') -> None:
    globals().update(safe_load(open(config_file)))


def snapshot() -> dict:
    return {k: v for k, v in globals().items() if
            not k.startswith('__') and not k.startswith('_') and k not in ('reload_config', 'snapshot', 'safe_load')}
