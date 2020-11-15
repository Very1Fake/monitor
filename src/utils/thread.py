from threading import Thread
from typing import Type

from src.containers.scripter import EventHandler
from .log import Logger
from .protocol import Code


class StateError(Exception):
    pass


class ThreadClass(Thread):
    _exception: Type[Exception]
    _log: Logger
    _state: int

    event_handler: EventHandler

    def __init__(self, name: str, exception: Type[Exception], event_handler: EventHandler):
        if not isinstance(name, str):
            raise TypeError('name must be str')

        super().__init__(name=name, daemon=True)

        self._exception = exception
        self._log = Logger(name)
        self._state = 0

        self.event_handler = event_handler

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, value: int) -> None:  # TODO: Set 5 from any state
        if isinstance(value, int):
            if self._state == 1 and value not in (2, 5):
                raise StateError('In this state, you can change state only to 2 or 5')
            elif self._state in (2, 4, 5):
                raise StateError('State locked')
            elif self._state == 3 and value not in (4, 5):
                raise StateError('In this state, you can change state only to 4 or 5')
            else:
                self._state = value
        else:
            ValueError('state must be int')

    def throw(self, code: Code) -> None:
        self.event_handler.alert(code, self.name)
        self._log.fatal(self._exception(code))
