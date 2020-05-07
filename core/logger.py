import os
import traceback
from typing import Union

from termcolor import colored

from . import codes
from . import library as lib
from . import storage

if storage.logger.mode == 2 or storage.logger.mode == 3:
    if not os.path.isdir(storage.main.logs_path):
        os.makedirs(storage.main.logs_path)
    log_file = open(f'{storage.main.logs_path}/{lib.get_time(storage.logger.utc_time)}.log', 'w+')


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
            return msg.format(storage.logger.message_content)
        else:
            return msg

    def print(self, type_: int, msg: Union[str, codes.Code], parent: str = '') -> None:
        print(
            f"[{lib.get_time(storage.logger.utc_time)}] [{colored(*self.types[type_])}] "
            f"[{f'{parent}>' if parent else ''}{self.name}]: {self.format_msg(msg)}"
        )

    def write(self, type_: int, msg: Union[str, codes.Code], parent: str = '') -> None:
        log_file.write(
            f"[{lib.get_time(storage.logger.utc_time)}] [{self.types[type_][0]}] "
            f"[{f'{parent}>' if parent else ''}{self.name}]: {self.format_msg(msg)}\n"
        )
        log_file.flush()

    def test(self, msg: Union[str, codes.Code], parent: str = '') -> bool:
        if storage.logger.level >= 5:
            if storage.logger.mode == 1 or storage.logger.mode == 3:
                self.print(5, msg, parent)
            if storage.logger.mode == 2 or storage.logger.mode == 3:
                self.write(5, msg, parent)
            return True
        return False

    def debug(self, msg: Union[str, codes.Code], parent: str = '') -> bool:
        if storage.logger.level >= 4:
            if storage.logger.mode == 1 or storage.logger.mode == 3:
                self.print(4, msg, parent)
            if storage.logger.mode == 2 or storage.logger.mode == 3:
                self.write(4, msg, parent)
            return True
        return False

    def info(self, msg: Union[str, codes.Code], parent: str = '') -> bool:
        if storage.logger.level >= 3:
            if storage.logger.mode == 1 or storage.logger.mode == 3:
                self.print(3, msg, parent)
            if storage.logger.mode == 2 or storage.logger.mode == 3:
                self.write(3, msg, parent)
            return True
        return False

    def warn(self, msg: Union[str, codes.Code], parent: str = '') -> bool:
        if storage.logger.level >= 2:
            if storage.logger.mode == 1 or storage.logger.mode == 3:
                self.print(2, msg, parent)
            if storage.logger.mode == 2 or storage.logger.mode == 3:
                self.write(2, msg, parent)
            return True
        return False

    def error(self, msg: Union[str, codes.Code], parent: str = '') -> bool:
        if storage.logger.level >= 1:
            if storage.logger.mode == 1 or storage.logger.mode == 3:
                self.print(1, msg, parent)
            if storage.logger.mode == 2 or storage.logger.mode == 3:
                self.write(1, msg, parent)
            return True
        return False

    def fatal_msg(self, msg: Union[str, codes.Code], traceback_: str = '', parent: str = '') -> bool:
        if storage.logger.mode == 1 or storage.logger.mode == 3:
            print(colored(
                f"[{lib.get_time(storage.logger.utc_time)}] [FATAL] [{f'{parent}>' if parent else ''}{self.name}]: "
                f"{self.format_msg(msg)}",
                'red',
                attrs=['reverse']
            ) + f"\n{'=' * 32}\n{traceback_}\n{'=' * 32}" if traceback_ else '')
        if storage.logger.mode == 2 or storage.logger.mode == 3:
            self.write(0, self.format_msg(msg) + f"\n{'=' * 32}\n{traceback_}\n{'=' * 32}" if traceback_ else '',
                       parent)
        return True

    def fatal(self, e: Exception, from_: Exception = None, parent: str = ''):
        if storage.logger.mode == 1 or storage.logger.mode == 3:
            print(colored(
                f"[{lib.get_time(storage.logger.utc_time)}] [FATAL] [{f'{parent}>' if parent else ''}{self.name}]: "
                f"{e.__class__.__name__}: {e.__str__()}",
                'red',
                attrs=['reverse']
            ) + f"\n{'=' * 32}\n{traceback.format_exc()}\n{'=' * 32}")
        if storage.logger.mode == 2 or storage.logger.mode == 3:
            self.write(0, f"  {e.__class__.__name__}: {e.__str__()}\n{'=' * 32}\n{traceback.format_exc()}\n{'=' * 32}",
                       parent)
            log_file.flush()
        if from_:
            raise e from from_
        else:
            raise e


def change_level(level: int):
    log = Logger('Logger')
    if level in (0, 1, 2, 3, 4, 5):
        if storage.logger.level == level:
            log.warn(codes.Code(30801))
        else:
            log.info(codes.Code(20801, f'From {storage.logger.level} to {level}'))
            storage.logger.level = level
    else:
        if storage.main.production:
            log.error(codes.Code(40801))
        else:
            log.fatal(LoggerError(codes.Code(40801)))


def change_mode(mode: int):
    log = Logger('Logger')
    if mode in (0, 1, 2, 3):
        if storage.logger.mode == mode:
            log.warn(codes.Code(30802))
        else:
            log.info(codes.Code(20802, f'From {storage.logger.mode} to {mode}'))
            storage.logger.mode = mode
    else:
        if storage.main.production:
            log.error(codes.Code(40802))
        else:
            log.fatal(LoggerError(codes.Code(40802)))


def change_time(global_: bool):
    log = Logger('Logger')
    if storage.logger.utc_time == global_:
        log.warn(codes.Code(30803))
    else:
        log.info(codes.Code(20803) if global_ else codes.Code(20804))
        storage.logger.utc_time = global_
