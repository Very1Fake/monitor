from threading import RLock

from ujson import dump, load

from src.utils.log import Logger
from src.utils.protocol import Code
from src.utils.storage import MainStorage


class Keywords:
    _lock: RLock = RLock()
    _log: Logger = Logger('KW')

    abs: list = []
    pos: list = []
    neg: list = []

    @classmethod
    def export(cls) -> dict:
        with cls._lock:
            return {'absolute': cls.abs, 'positive': cls.pos, 'negative': cls.neg}

    @classmethod
    def check(cls, string: str, divider: str = ' ') -> bool:
        has_pos: bool = False
        for i in string.split(divider):
            if i.lower() in cls.abs:
                return True
            elif i.lower() in cls.neg:
                return False
            elif not has_pos and i.lower() in cls.pos:
                has_pos = True
        else:
            return has_pos

    @classmethod
    def dump(cls) -> int:
        with cls._lock:
            cls._log.info(Code(21501))
            if MainStorage().check('keywords.json'):
                kw = load(MainStorage().file('keywords.json'))

                if isinstance(kw, dict) and 'absolute' in kw and isinstance(kw['absolute'], list):
                    cls.abs.sort()
                    kw['absolute'].sort()
                    if cls.abs == kw['absolute'] and 'positive' in kw and isinstance(kw['positive'], list):
                        cls.pos.sort()
                        kw['positive'].sort()
                        if cls.pos == kw['positive'] and 'negative' in kw and isinstance(kw['negative'], list):
                            cls.neg.sort()
                            kw['negative'].sort()
                            if cls.neg == kw['negative']:
                                cls._log.info(Code(21504))
                                return 1

            dump(cls.export(), MainStorage().file('keywords.json', 'w+'), indent=2)
            cls._log.info(Code(21502))
            return 0

    @classmethod
    def sync(cls) -> int:
        with cls._lock:
            cls._log.info(Code(21503))
            if MainStorage().check('keywords.json'):
                kw = load(MainStorage().file('keywords.json'))

                if isinstance(kw, dict):
                    if 'absolute' in kw and isinstance(kw['absolute'], list):
                        for i in kw['absolute']:
                            cls.add_abs(i)
                    if 'positive' in kw and isinstance(kw['positive'], list):
                        for i in kw['positive']:
                            cls.add_pos(i)
                    if 'negative' in kw and isinstance(kw['negative'], list):
                        for i in kw['negative']:
                            cls.add_neg(i)
                    cls._log.info(Code(21504))
                    return 0
                else:
                    raise TypeError('keywords.json must be object')
            else:
                cls._log.warn(Code(31501))
                dump({'absolute': [], 'positive': [], 'negative': []},
                     MainStorage().file('keywords.json'), indent=2)
                return 1

    @classmethod
    def clear(cls) -> int:
        with cls._lock:
            cls._log.info(Code(21505))
            cls.abs.clear()
            cls.pos.clear()
            cls.neg.clear()
            cls._log.info(Code(21506))
            return 0

    @classmethod
    def load(cls) -> int:
        with cls._lock:
            cls._log.info(Code(21507))
            cls.clear()
            cls.sync()
            cls._log.info(Code(21508))
            return 0

    @classmethod
    def add_abs(cls, kw: str) -> int:
        with cls._lock:
            if isinstance(kw, str):
                if kw in cls.abs:
                    cls._log.warn(Code(31512, kw))
                    return 2
                else:
                    cls.abs.append(kw)
                    return 0
            else:
                cls._log.warn(Code(31511, kw))
                return 1

    @classmethod
    def add_pos(cls, kw: str) -> int:
        with cls._lock:
            if isinstance(kw, str):
                if kw in cls.pos:
                    cls._log.warn(Code(31522, kw))
                    return 2
                else:
                    cls.pos.append(kw)
                    return 0
            else:
                cls._log.warn(Code(31521, kw))
                return 1

    @classmethod
    def add_neg(cls, kw: str) -> int:
        with cls._lock:
            if isinstance(kw, str):
                if kw in cls.neg:
                    cls._log.warn(Code(31532, kw))
                    return 2
                else:
                    cls.neg.append(kw)
                    return 0
            else:
                cls._log.warn(Code(31531, kw))
                return 1

    @classmethod
    def remove_abs(cls, kw: str) -> int:
        with cls._lock:
            if kw in cls.abs:
                cls.abs.remove(kw)
                return 0
            else:
                return 1

    @classmethod
    def remove_pos(cls, kw: str) -> int:
        with cls._lock:
            if kw in cls.pos:
                cls.pos.remove(kw)
                return 0
            else:
                return 1

    @classmethod
    def remove_neg(cls, kw: str) -> int:
        with cls._lock:
            if kw in cls.neg:
                cls.neg.remove(kw)
                return 0
            else:
                return 1
