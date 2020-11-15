from typing import List, Union, Optional, Tuple


# TODO: Global module storage


class Version:
    __slots__ = ['major', 'minor', 'patch', 'numbers']
    major: int
    minor: int
    patch: int
    numbers: tuple

    @property
    def version(self) -> str:
        return f'{self.major}.{self.minor}{f".{self.patch}" if self.patch else ""}'

    def __init__(self, major: int = 0, minor: int = 0, patch: int = 0):
        if isinstance(major, int):
            if major < 0:
                raise ValueError('Major number must be greater than 0')
            else:
                self.major = major
        else:
            raise TypeError('Major number must be int')
        if isinstance(minor, int):
            if minor < 0:
                raise ValueError('Minor number must be greater than 0')
            else:
                self.minor = minor
        else:
            raise TypeError('Minor number must be int')
        if isinstance(patch, int):
            if patch < 0:
                raise ValueError('Patch number must be greater than 0')
            else:
                self.patch = patch
        else:
            raise TypeError('Patch number must be int')
        self.numbers = (major, minor, patch)

    @classmethod
    def from_str(cls, v: str):
        if isinstance(v, str):
            return cls(*cls.parse(v)[:3])
        else:
            raise TypeError('Version string must be str')

    @staticmethod
    def parse(base: str) -> Tuple[int, int, int, int]:
        if not isinstance(base, str):
            raise TypeError('base must be str')

        if base[0] == '^':
            req = 1
        elif base[0] == '~':
            req = 2
        elif base.startswith('>='):
            req = 4
        elif base.startswith('<='):
            req = 6
        elif base[0] == '>':
            req = 3
        elif base[0] == '<':
            req = 5
        else:
            req = 0

        try:  # Parsing numbers from string
            numbers = tuple((int(i) for i in base[2 if req in (4, 6) else 1 if req else 0:].split('.')))
        except ValueError:
            raise ValueError('Version must contain only integers')

        if len(numbers) > 0:
            major = numbers[0]

            if len(numbers) > 1:
                if numbers[1] < 0:
                    raise ValueError('Minor number must be greater than 0')
                else:
                    minor = numbers[1]
            else:
                minor = 0

            if len(numbers) > 2:
                if numbers[2] < 0:
                    raise ValueError('Patch number must greater than than 0')
                else:
                    patch = numbers[2]
            else:
                patch = 0

            return major, minor, patch, req
        else:
            raise ValueError('Version numbers not specified')

    def __eq__(self, other):  # Operator: `==`
        if isinstance(other, Version):
            return self.patch == other.patch and self.minor == other.minor and self.major == other.major
        else:
            raise TypeError('Version can\'t be compared to other types')

    def __ne__(self, other):  # Operator: `!=`
        if isinstance(other, Version):
            return self.patch != other.patch or self.minor != other.minor or self.major != other.major
        else:
            raise TypeError('Version can\'t be compared to other types')

    def __gt__(self, other):  # Operator: `>`
        if isinstance(other, Version):
            if self.major > other.major:
                return True
            elif self.major == other.major:
                if self.minor > other.minor:
                    return True
                elif self.minor == other.minor:
                    if self.patch > other.patch:
                        return True
            return False
        else:
            raise TypeError('Version can\'t be compared to other types')

    def __lt__(self, other):  # Operator `<`
        if isinstance(other, Version):
            if self.major < other.major:
                return True
            elif self.major == other.major:
                if self.minor < other.minor:
                    return True
                elif self.minor == other.minor:
                    if self.patch < other.patch:
                        return True
            return False
        else:
            raise TypeError('Version can\'t be compared to other types')

    def __ge__(self, other):  # Operator `>=`
        if isinstance(other, Version):
            return self > other or self == other
        else:
            raise TypeError('Version can\'t be compared to other types')

    def __le__(self, other):  # Operator `<=`
        if isinstance(other, Version):
            return self < other or self == other
        else:
            raise TypeError('Version can\'t be compared to other types')

    def __bool__(self):
        return bool(self.major or self.minor or self.patch)

    def __str__(self) -> str:
        return f'Version({self.version})'


class VersionRequirement:
    __slots__ = ['numbers', 'req']
    numbers: Tuple[int, int, int]
    req: int

    def __init__(self, vr: str):
        *self.numbers, self.req = Version.parse(vr)
        self.numbers = tuple(self.numbers)

    def match(self, version: Version) -> bool:
        if self.req == 0:  # No requirement
            return version == Version(*self.numbers)
        elif self.req == 1:  # Caret requirement
            if self.numbers[0]:
                return Version(*self.numbers) <= version < Version(self.numbers[0] + 1)
            elif self.numbers[1]:
                return Version(*self.numbers) <= version < Version(0, self.numbers[1] + 1)
            elif self.numbers[2]:
                return Version(*self.numbers) <= version < Version(0, 0, self.numbers[2] + 1)
            else:
                return Version() <= version < Version(1)
        elif self.req == 2:
            if self.numbers[1]:
                return Version(*self.numbers) <= version < Version(self.numbers[0], self.numbers[1] + 1)
            else:
                return Version(*self.numbers) <= version < Version(self.numbers[0] + 1)
        elif self.req == 3:
            return version > Version(*self.numbers)
        elif self.req == 4:
            return version >= Version(*self.numbers)
        elif self.req == 5:
            return version < Version(*self.numbers)
        elif self.req == 6:
            return version <= Version(*self.numbers)


class Dependency:
    name: str
    version: Version

    def __str__(self) -> str:
        return f"Dependency('{self.name}', '')"

    def export(self) -> dict:
        return {'name': self.name, 'version': str(Version)}


class Module:
    id: str
    name: str
    version: Version
    authors: Optional[Union[str, List[str]]]
    description: str
    group: str
    dependencies: List[Dependency]


class Group:
    name: str
    modules: List[Module]
