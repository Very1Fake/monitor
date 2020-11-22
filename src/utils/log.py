import threading
import traceback
from typing import Union, Optional, TextIO

from termcolor import colored

from src.store import log, main
from .protocol import get_time, Code
from .storage import LogStorage

print_lock: threading.Lock = threading.Lock()
write_lock: threading.Lock = threading.Lock()

file: Optional[TextIO] = None


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
    def format_msg(msg) -> str:
        if isinstance(msg, Code):
            return msg.format(log.content)
        else:
            return str(msg)

    def print(self, type_: int, msg: Union[str, Code], parent: str = '') -> None:
        with print_lock:
            print(
                f"[{get_time()}] [{colored(*self.types[type_])}] "
                f"[{f'{parent}>' if parent else ''}{self.name}]: {self.format_msg(msg)}"
            )

    def write(self, type_: int, msg: Union[str, Code], parent: str = '') -> None:
        with write_lock:
            global file
            try:
                if not LogStorage().check(file.name):
                    raise NameError
            except (NameError, AttributeError):
                file = LogStorage().file(get_time(True) + '.log', 'w+')
            finally:
                file.write(
                    f"[{get_time()}] [{self.types[type_][0]}] "
                    f"[{f'{parent}>' if parent else ''}{self.name}]: {self.format_msg(msg)}\n"
                )
                file.flush()

    def test(self, msg: Union[str, Code], parent: str = '') -> bool:
        if log.level >= 5:
            if log.mode == 1 or log.mode == 3:
                self.print(5, msg, parent)
            if log.mode == 2 or log.mode == 3:
                self.write(5, msg, parent)
            return True
        return False

    def debug(self, msg: Union[str, Code], parent: str = '') -> bool:
        if log.level >= 4:
            if log.mode == 1 or log.mode == 3:
                self.print(4, msg, parent)
            if log.mode == 2 or log.mode == 3:
                self.write(4, msg, parent)
            return True
        return False

    def info(self, msg: Union[str, Code], parent: str = '') -> bool:
        if log.level >= 3:
            if log.mode == 1 or log.mode == 3:
                self.print(3, msg, parent)
            if log.mode == 2 or log.mode == 3:
                self.write(3, msg, parent)
            return True
        return False

    def warn(self, msg: Union[str, Code], parent: str = '') -> bool:
        if log.level >= 2:
            if log.mode == 1 or log.mode == 3:
                self.print(2, msg, parent)
            if log.mode == 2 or log.mode == 3:
                self.write(2, msg, parent)
            return True
        return False

    def error(self, msg: Union[str, Code], parent: str = '') -> bool:
        if log.level >= 1:
            if log.mode == 1 or log.mode == 3:
                self.print(1, msg, parent)
            if log.mode == 2 or log.mode == 3:
                self.write(1, msg, parent)
            return True
        return False

    def fatal_msg(self, msg: Union[str, Code], traceback_: str = '', parent: str = '') -> bool:
        if log.mode == 1 or log.mode == 3:
            with print_lock:
                print(colored(
                    f'[{get_time()}] [FATAL] '
                    f'[{f"{parent}>" if parent else ""}{self.name}]: {self.format_msg(msg)}',
                    'red',
                    attrs=['reverse']
                ) + f"\n{'=' * 32}\n{traceback_}\n{'=' * 32}" if traceback_ else '')

        if log.mode == 2 or log.mode == 3:
            self.write(0, self.format_msg(msg) + f"\n{'=' * 32}\n{traceback_}\n{'=' * 32}" if traceback_ else '',
                       parent)

        return True

    def fatal(self, e: Exception, from_: Exception = None, parent: str = ''):
        if log.mode == 1 or log.mode == 3:
            with print_lock:
                print(colored(
                    f'[{get_time()}] [FATAL] '
                    f'[{f"{parent}>" if parent else ""}{self.name}]: '
                    f"{e.__class__.__name__}: {self.format_msg(e.args[0]) if len(e.args) else e!s}",
                    'red',
                    attrs=['reverse']
                ) + f"\n{'=' * 32}\n{traceback.format_exc()}\n{'=' * 32}")

        if log.mode == 2 or log.mode == 3:
            self.write(
                0,
                f"{e.__class__.__name__}: {self.format_msg(e) if len(e.args) else e!s}\n"
                f"{'=' * 32}\n{traceback.format_exc()}\n{'=' * 32}",
                parent
            )

        if from_:
            raise e from from_
        else:
            raise e


def change_level(level: int):
    if level in (0, 1, 2, 3, 4, 5):
        if log.level == level:
            log.warn(Code(30801))
        else:
            log.info(Code(20801, f'From {log.level} to {level}'))
            log.level = level
    else:
        if main.production:
            log.error(Code(40801))
        else:
            log.fatal(LoggerError(Code(40801)))


def change_mode(mode: int):
    if mode in (0, 1, 2, 3):
        if log.mode == mode:
            log.warn(Code(30802))
        else:
            log.info(Code(20802, f'From {log.mode} to {mode}'))
            log.mode = mode
    else:
        if main.production:
            log.error(Code(40802))
        else:
            log.fatal(LoggerError(Code(40802)))


def reset_file():
    global file
    with write_lock:
        file = None
