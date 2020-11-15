from abc import ABC
from typing import TypeVar


class Message(ABC):
    __slots__ = ['channel', 'script', 'text']
    channel: str
    script: str
    text: str

    def __init__(self, text: str, script: str, channel: str = ''):
        self.text = text
        self.script = script
        self.channel = channel


MessageType = TypeVar('MessageType', bound=Message)


class MInfo(Message):
    pass


class MAlert(Message):
    pass
