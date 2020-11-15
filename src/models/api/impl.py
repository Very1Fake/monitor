from abc import abstractmethod, ABC
from typing import List, Union

from src.utils.log import Logger
from src.utils.protocol import Code
from src.utils.storage import ScriptStorage
from .catalog import CatalogType
from .item import ItemType
from .message import MessageType
from .target import TargetType, RestockTargetType, TargetEndType
from ..provider import SubProvider


class EventsExecutor(ABC):  # Class to implement by scripts with events
    name: str
    log: Logger
    storage: ScriptStorage

    def __init__(self, name: str, log: Logger, storage: ScriptStorage):
        self.name = name
        self.log = log
        self.storage = storage

    def e_monitor_starting(self) -> None: ...

    def e_monitor_started(self) -> None: ...

    def e_monitor_stopping(self) -> None: ...

    def e_monitor_stopped(self) -> None: ...

    def e_alert(self, code: Code, thread: str) -> None: ...

    def e_item(self, item: ItemType) -> None: ...

    def e_target_end(self, target_end: TargetEndType) -> None: ...

    def e_message(self, message: MessageType) -> None: ...


class Parser(ABC):  # Class to implement by scripts with parser
    name: str
    log: Logger
    provider: SubProvider
    storage: ScriptStorage

    def __init__(self, name: str, log: Logger, provider_: SubProvider, storage: ScriptStorage):
        self.name = name
        self.log = log
        self.provider = provider_
        self.storage = storage

    @property
    def catalog(self) -> CatalogType:
        raise NotImplementedError

    @abstractmethod
    def execute(
            self,
            mode: int,
            content: Union[CatalogType, TargetType]
    ) -> List[Union[CatalogType, TargetType, RestockTargetType, TargetEndType, ItemType, MessageType]]:
        raise NotImplementedError
