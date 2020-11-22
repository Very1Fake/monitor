from os import environ


def get_namespace(name: str, divider: str = '.'):
    if not isinstance(divider, str):
        raise TypeError('Divider must be str')
    if isinstance(name, str):
        space = {}

        for i in environ:
            if (j := i.partition(divider))[0] == name:
                space[j[2]] = environ[i]

        return space
    else:
        raise TypeError('Namespace must be str')
