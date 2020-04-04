import os
from typing import Union

from termcolor import colored

from . import codes
from . import library as lib
from . import storage

if storage.log_mode == 2 or storage.log_mode == 3:
    if not os.path.isdir(storage.logs_folder):
        os.makedirs(storage.logs_folder)
    log_file = open(f'{storage.logs_folder}/{lib.get_time(storage.log_utc_time)}.log', 'w+')


class LoggerError(Exception):
    pass


class Logger:
    types: tuple = (
        ('FATAL', 'red'),
        ('ERROR', 'red'),
        ('WARN', 'yellow'),
        ('INFO', 'green'),
        ('DEBUG', 'blue'),
        ('TEST', 'magenta')
    )

    def __init__(self, name: str):
        self.name = name

    @staticmethod
    def format_msg(msg: Union[str, codes.Code]) -> str:
        if isinstance(msg, codes.Code):
            return msg.format(storage.log_message_content)
        else:
            return msg

    def print(self, type_: int, msg: Union[str, codes.Code]) -> None:
        print(
            f"[{lib.get_time(storage.log_utc_time)}] [{colored(*self.types[type_])}] "
            f"[{self.name}]: {self.format_msg(msg)}"
        )

    def write(self, type_: int, msg: Union[str, codes.Code]) -> None:
        log_file.write(
            f"[{lib.get_time(storage.log_utc_time)}] [{self.types[type_][0]}] "
            f"[{self.name}]: {self.format_msg(msg)}\n"
        )
        log_file.flush()

    def test(self, msg: Union[str, codes.Code]) -> bool:
        if storage.log_level >= 5:
            if storage.log_mode == 1 or storage.log_mode == 3:
                self.print(5, msg)
            if storage.log_mode == 2 or storage.log_mode == 3:
                self.write(5, msg)
            return True
        return False

    def debug(self, msg: Union[str, codes.Code]) -> bool:
        if storage.log_level >= 4:
            if storage.log_mode == 1 or storage.log_mode == 3:
                self.print(4, msg)
            if storage.log_mode == 2 or storage.log_mode == 3:
                self.write(4, msg)
            return True
        return False

    def info(self, msg: Union[str, codes.Code]) -> bool:
        if storage.log_level >= 3:
            if storage.log_mode == 1 or storage.log_mode == 3:
                self.print(3, msg)
            if storage.log_mode == 2 or storage.log_mode == 3:
                self.write(3, msg)
            return True
        return False

    def warn(self, msg: Union[str, codes.Code]) -> bool:
        if storage.log_level >= 2:
            if storage.log_mode == 1 or storage.log_mode == 3:
                self.print(2, msg)
            if storage.log_mode == 2 or storage.log_mode == 3:
                self.write(2, msg)
            return True
        return False

    def error(self, msg: Union[str, codes.Code]) -> bool:
        if storage.log_level >= 1:
            if storage.log_mode == 1 or storage.log_mode == 3:
                self.print(1, msg)
            if storage.log_mode == 2 or storage.log_mode == 3:
                self.write(1, msg)
            return True
        return False

    def fatal_msg(self, msg: Union[str, codes.Code]) -> bool:
        if storage.log_mode == 1 or storage.log_mode == 3:
            print(colored(
                f"[{lib.get_time(storage.log_utc_time)}] [FATAL] [{self.name}]: {self.format_msg(msg)}",
                'red',
                attrs=['reverse']
            ))
        if storage.log_mode == 2 or storage.log_mode == 3:
            self.write(0, msg)
        return True

    def fatal(self, e: Exception, from_: Exception = None):
        if storage.log_mode == 1 or storage.log_mode == 3:
            print(colored(
                f"[{lib.get_time(storage.log_utc_time)}] [FATAL] [{self.name}]:   "
                f"{e.__class__.__name__}: {e.__str__()}",
                'red',
                attrs=['reverse']
            ))
        if storage.log_mode == 2 or storage.log_mode == 3:
            self.write(0, f"  {e.__class__.__name__}: {e.__str__()}\n")
            log_file.flush()
        if from_:
            raise e from from_
        else:
            raise e


def change_level(level: int):
    log = Logger('Logger')
    if level in (0, 1, 2, 3, 4, 5):
        if storage.log_level == level:
            log.warn(codes.Code(38001))
        else:
            log.info(codes.Code(28001, f'From {storage.log_level} to {level}'))
            storage.log_level = level
    else:
        if storage.production:
            log.error(codes.Code(48001))
        else:
            log.fatal(LoggerError(codes.Code(48001)))


def change_mode(mode: int):
    log = Logger('Logger')
    if mode in (0, 1, 2, 3):
        if storage.log_mode == mode:
            log.warn(codes.Code(38002))
        else:
            log.info(codes.Code(28002, f'From {storage.log_mode} to {mode}'))
            storage.log_mode = mode
    else:
        if storage.production:
            log.error(codes.Code(48002))
        else:
            log.fatal(LoggerError(codes.Code(48002)))


def change_time(global_: bool):
    log = Logger('Logger')
    if storage.log_utc_time == global_:
        log.warn(codes.Code(38003))
    else:
        log.info(codes.Code(28003) if global_ else codes.Code(28004))
        storage.log_utc_time = global_
