from termcolor import colored

from . import library as lib
from . import storage


# TODO: Log to file (for mode 2 and 3)


class LoggerError(Exception):
    pass


class Logger:
    def __init__(self, name: str):
        self.name = name
        self.mode = 1

    def test(self, msg: str):
        if storage.log_level >= 5:
            if self.mode == 1 or self.mode == 3:
                print(f"[{lib.get_time(storage.log_time)}] [{colored('TEST', 'magenta')}] [{self.name}]: {msg}")

    def debug(self, msg: str):
        if storage.log_level >= 4:
            if self.mode == 1 or self.mode == 3:
                print(f"[{lib.get_time(storage.log_time)}] [{colored('DEBUG', 'blue')}] [{self.name}]: {msg}")

    def info(self, msg: str):
        if storage.log_level >= 3:
            if self.mode == 1 or self.mode == 3:
                print(f"[{lib.get_time(storage.log_time)}] [{colored('INFO', 'green')}] [{self.name}]: {msg}")

    def warn(self, msg: str):
        if storage.log_level >= 2:
            if self.mode == 1 or self.mode == 3:
                print(f"[{lib.get_time(storage.log_time)}] [{colored('WARN', 'yellow')}] [{self.name}]: {msg}")

    def error(self, msg: str):
        if storage.log_level >= 1:
            if self.mode == 1 or self.mode == 3:
                print(f"[{lib.get_time(storage.log_time)}] [{colored('ERROR', 'red')}] [{self.name}]: {msg}")

    def fatal(self, e: Exception):
        if storage.log_level >= 0:
            if self.mode == 1 or self.mode == 3:
                print(colored(f"[{lib.get_time(storage.log_time)}] [FATAL] [{self.name}]: {e.__str__()}", 'red',
                              attrs=['reverse']) + "\n\n")
            raise e


def change_level(level: int):
    log = Logger('Logger')
    if level in (0, 1, 2, 3, 4, 5):
        if storage.log_level == level:
            log.warn('Meaningless level change (changing to the same value)')
        else:
            log.info(f'Level changed from {storage.log_level} to {level}')
            storage.log_level = level
    else:
        if storage.production:
            log.error('Can\'t change level (possible values (0, 1, 2, 3, 4, 5))')
        else:
            log.fatal(LoggerError('Can\'t change level (possible values (0, 1, 2, 3, 4, 5))'))


def change_mode(mode: int):
    log = Logger('Logger')
    if mode in (0, 1, 2, 3):
        if storage.log_mode == mode:
            log.warn('Meaningless mode change (changing to the same value)')
        else:
            log.info(f'Mode changed from {storage.log_mode} to {mode}')
            storage.log_mode = mode
    else:
        if storage.production:
            log.error('Can\'t change mode (possible values (0, 1, 2, 3))')
        else:
            log.fatal(LoggerError('Can\'t change mode (possible values (0, 1, 2, 3))'))


def change_time(_global: bool):
    log = Logger('Logger')
    if storage.log_time == _global:
        log.warn('Meaningless time change (changing to the same value)')
    else:
        log.info(f'Time changed to {"UTC" if _global else "local"}')
        storage.log_time = _global
