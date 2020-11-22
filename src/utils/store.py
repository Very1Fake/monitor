from io import TextIOWrapper
from json import dumps as j_dump, loads as j_load
from typing import Dict, Optional, Type

from toml import dumps as t_dump, loads as t_load
from yaml import safe_dump as y_dump, safe_load as y_load

from src.utils.env import get_namespace


class SectionMeta(type):
    def __new__(mcs, name, base, dct, **kwargs):
        abc = kwargs.get('abc', False)

        if not abc:
            if '__annotations__' in dct:
                dct['_fields'] = {}
                if '__slots__' not in dct:
                    dct['__slots__'] = []

                for k, t in dct['__annotations__'].items():
                    if k[0] != '_' and t in (bool, int, float, str, list, dict):
                        j = dct.get(k, None)
                        if j is None:
                            raise ValueError('Section variable must have default value')
                        elif isinstance(j, t):
                            dct['__slots__'].append(k)
                            dct['_fields'][k] = (j, t)
                            del dct[k]
                        else:
                            raise TypeError('Section variable value does not match its type')
                    else:
                        raise TypeError(f'Section variable can be only (bool, int, float, str, list, dict, Field)')
            else:
                raise LookupError('Section must have at least one annotation')

        try:
            del dct['__annotations__']
        except KeyError:
            pass

        return super().__new__(mcs, name, base, dct)


class Section(metaclass=SectionMeta, abc=True):
    __slots__ = ['_fields', '_name']
    _fields: Dict[str, tuple]
    _name: str

    def __init__(self, **kwargs):
        for k, v in self._fields.items():
            setattr(self, k, kwargs.get(k, v[0]))

    def __setattr__(self, key, value):
        if key in self._fields:
            if isinstance(value, self._fields[key][1]):
                super().__setattr__(key, value)
            else:
                raise TypeError(f'Value does not match section variable type ({self._fields[key][1]})')
        else:
            raise AttributeError(f'Section has not attribute {key}')

    @property
    def section(self):
        if hasattr(self, '_name'):
            return getattr(self, '_name')
        else:
            return self.__class__.__name__.lower()

    @property
    def fields(self):
        return self._fields

    @property
    def has_secret(self) -> bool:
        for i in self._fields.values():
            if i[2]:
                return True
        else:
            return False

    @property
    def asdict(self) -> dict:
        d = {}
        for i in self._fields:
            d[i] = getattr(self, i)
        return d

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({", ".join([f"{k}={v!r}" for k, v in self.asdict.items()])})'

    def loads(self, source: Optional[str] = None, method: int = 1, **kwargs):
        if method not in range(4):
            raise IndexError('Method does not exist')

        if method == 0:
            source = {self.section: get_namespace(self.section, **kwargs)}
        else:
            if isinstance(source, str):
                if method == 1:
                    source = t_load(source, **kwargs)
                elif method == 2:
                    source = j_load(source, **kwargs)
                elif method == 3:
                    source = y_load(source)
            else:
                raise ValueError('Source must be str (for non-environment method)')

        if self.section in source:
            if isinstance(source[self.section], dict):
                for k, v in source[self.section].items():
                    try:
                        setattr(self, k, v)
                    except AttributeError:
                        pass
            else:
                raise TypeError('Section in source must be dict')
        else:
            raise KeyError('Section not found in source')

    def dumps(self, method: int = 1, **kwargs) -> str:
        if method not in range(4):
            raise IndexError('Method does not exist')

        if method == 0:
            raise NotImplementedError('Environment does not support export')

        e = {self.section: self.asdict}

        if method == 1:
            return t_dump(e, **kwargs)
        elif method == 2:
            return j_dump(e, **kwargs)
        elif method == 3:
            return y_dump(e, **kwargs)


class StoreMeta(type):
    def __new__(mcs, name, base, dct, **kwargs):
        abc = kwargs.get('abc', False)

        if not abc:
            if '__annotations__' in dct:
                dct['_sections'] = {}
                if '__slots__' not in dct:
                    dct['__slots__'] = []

                for k, t in dct['__annotations__'].items():
                    if k[0] != '_' and issubclass(t, Section):
                        if k in dct:
                            raise AttributeError('Store section can be class variable')
                        else:
                            dct['__slots__'].append(k)
                            dct['_sections'][k] = t
                    else:
                        raise TypeError(f'Store section must be Section')
            else:
                raise LookupError('Store must have at least one section')

        try:
            del dct['__annotations__']
        except KeyError:
            pass

        return super().__new__(mcs, name, base, dct)


class Store(metaclass=StoreMeta, abc=True):
    __slots__ = ['_sections']
    _sections: Dict[str, Type[Section]]

    def __init__(self):
        for k, t in self._sections.items():
            setattr(self, k, t())

    @property
    def sections(self):
        return self._sections

    def loads(self, source: str = None, method: int = 1, **kwargs):
        if method not in range(4):
            raise IndexError('Method does not exist')
        if method != 0 and not isinstance(source, str):
            raise ValueError('Source must be specified (for non-environment method)')

        for i in self._sections:
            getattr(self, i).loads(source, method, **kwargs)

    def load(self, source: TextIOWrapper, method: int = 1, **kwargs):
        if method not in range(4):
            raise IndexError('Method does not exist')
        if method != 0 and not isinstance(source, TextIOWrapper):
            raise ValueError('Source must be specified (for non-environment method)')

        if isinstance(source, TextIOWrapper):
            source = source.read()
        else:
            raise TypeError('Target must be opened file')

        for i in self._sections:
            getattr(self, i).loads(source, method, **kwargs)

    def dumps(self, method: int = 1, **kwargs) -> str:
        if method not in range(4):
            raise IndexError('Method does not exist')
        if method == 0:
            raise NotImplementedError('Environment does not support export')

        e = {i: getattr(self, i).asdict for i in self._sections}

        if method == 1:
            return t_dump(e, **kwargs)
        elif method == 2:
            return j_dump(e, **kwargs)
        elif method == 3:
            return y_dump(e, **kwargs)

    def dump(self, target: TextIOWrapper, method: int = 1, **kwargs):
        if isinstance(target, TextIOWrapper):
            target.write(self.dumps(method, **kwargs))
        else:
            raise TypeError('Target must be opened file with write access')
