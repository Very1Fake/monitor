import os

from termcolor import colored

from . import library as lib
from . import storage


if not os.path.isdir(storage.logs_folder):
    os.makedirs(storage.logs_folder)
log_file = open(f'{storage.logs_folder}{lib.get_time(storage.log_utc_time)}.log', 'w+')  # TODO: Fix here


class LoggerError(Exception):
    pass


class Logger:
    def __init__(self, name: str):
        self.name = name

    def test(self, msg: str) -> bool:
        if storage.log_level >= 5:
            if storage.log_mode == 1 or storage.log_mode == 3:
                print(f"[{lib.get_time(storage.log_utc_time)}] [{colored('TEST', 'magenta')}] [{self.name}]: {msg}")
            if storage.log_mode == 2 or storage.log_mode == 3:
                log_file.write(f"[{lib.get_time(storage.log_utc_time)}] [TEST] [{self.name}]: {msg}\n")
                log_file.flush()
            return True
        return False

    def debug(self, msg: str) -> bool:
        if storage.log_level >= 4:
            if storage.log_mode == 1 or storage.log_mode == 3:
                print(f"[{lib.get_time(storage.log_utc_time)}] [{colored('DEBUG', 'blue')}] [{self.name}]: {msg}")
            if storage.log_mode == 2 or storage.log_mode == 3:
                log_file.write(f"[{lib.get_time(storage.log_utc_time)}] [DEBUG] [{self.name}]: {msg}\n")
                log_file.flush()
            return True
        return False

    def info(self, msg: str) -> bool:
        if storage.log_level >= 3:
            if storage.log_mode == 1 or storage.log_mode == 3:
                print(f"[{lib.get_time(storage.log_utc_time)}] [{colored('INFO', 'green')}] [{self.name}]: {msg}")
            if storage.log_mode == 2 or storage.log_mode == 3:
                log_file.write(f"[{lib.get_time(storage.log_utc_time)}] [INFO] [{self.name}]: {msg}\n")
                log_file.flush()
            return True
        return False

    def warn(self, msg: str) -> bool:
        if storage.log_level >= 2:
            if storage.log_mode == 1 or storage.log_mode == 3:
                print(f"[{lib.get_time(storage.log_utc_time)}] [{colored('WARN', 'yellow')}] [{self.name}]: {msg}")
            if storage.log_mode == 2 or storage.log_mode == 3:
                log_file.write(f"[{lib.get_time(storage.log_utc_time)}] [WARN] [{self.name}]: {msg}\n")
                log_file.flush()
            return True
        return False

    def error(self, msg: str) -> bool:
        if storage.log_level >= 1:
            if storage.log_mode == 1 or storage.log_mode == 3:
                print(f"[{lib.get_time(storage.log_utc_time)}] [{colored('ERROR', 'red')}] [{self.name}]: {msg}")
            if storage.log_mode == 2 or storage.log_mode == 3:
                log_file.write(f"[{lib.get_time(storage.log_utc_time)}] [ERROR] [{self.name}]: {msg}\n")
                log_file.flush()
            return True
        return False

    def fatal(self, e: Exception, _from: Exception = None):
        if storage.log_mode == 1 or storage.log_mode == 3:
            print(colored(
                f"[{lib.get_time(storage.log_utc_time)}] [FATAL] [{self.name}]:   "
                f"{e.__class__.__name__}: {e.__str__()}", 'red', attrs=['reverse']
            ))
        if storage.log_mode == 2 or storage.log_mode == 3:
            log_file.write(f"[{lib.get_time(storage.log_utc_time)}] [FATAL] [{self.name}]:   "
                           f"{e.__class__.__name__}: {e.__str__()}\n")
            log_file.flush()
        if _from:
            raise e from _from
        else:
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
    if storage.log_utc_time == _global:
        log.warn('Meaningless time change (changing to the same value)')
    else:
        log.info(f'Time changed to {"UTC" if _global else "local"}')
        storage.log_utc_time = _global
