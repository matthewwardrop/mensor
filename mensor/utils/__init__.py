import functools
import warnings
from collections import OrderedDict


def startseq_match(A, B):
    '''
    Checks whether sequence a starts sequence b.
    For example: startseq_match([1,2], [1,2,3]) == True.
    '''
    for i, a in enumerate(A):
        if i == len(B) or a != B[i]:
            return False
    return True


class AttrDict(dict):

    def __dir__(self):
        return dict.__dir__(self) + self.keys()

    def __getattr__(self, key):
        return self[key]

    def map(self, names):
        return [self[name] for name in names]


def deprecated(version_flagged=None, version_removal=None):
    """
    This is a decorator to use when deprecating functions or methods. The
    decorator should be called first with the version at which it will be
    deprecated.
    """
    def deprecated(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.simplefilter('always', DeprecationWarning)  # turn off filter
            warnings.warn(
                "{}() is deprecated{}{}.".format(
                    func.__name__,
                    " as of version {}".format(version_flagged) if version_flagged else "",
                    " and will be removed in version {}".format(version_removal) if version_removal else ""
                ),
                category=DeprecationWarning,
                stacklevel=2
            )
            warnings.simplefilter('default', DeprecationWarning)  # reset filter
            return func(*args, **kwargs)
        return wrapper
    return deprecated


def nested_dict_copy(d):
    d = d.copy()
    for key, value in d.items():
        if isinstance(value, dict):
            d[key] = nested_dict_copy(value)
    return d


class SequenceMap(object):

    def __init__(self, sequence=None):
        self.__values = OrderedDict([(v, v) for v in sequence]) if sequence else OrderedDict()

    def prepend(self, value):
        self.append(value)
        self.__values.move_to_end(value, last=False)

    def append(self, value):
        self.__values[value] = value

    def extend(self, values):
        for value in values:
            self.append(value)

    def copy(self):
        return self.__class__(self.__values.keys())

    def get(self, value, default=None):
        return self.__values.get(value, default)

    def pop(self, value, default=None):
        return self.__values.pop(value, default)

    @property
    def first(self):
        return next(iter(self))

    def __getitem__(self, item):
        return self.__values[item]

    def __setitem__(self, item, value):
        self.__values[item] = value

    def __iter__(self):
        return self.__values.__iter__()

    def __reversed__(self):
        return self.__values.__reversed__()

    def __len__(self):
        return len(self.__values)

    def __repr__(self):
        return '{{{}}}'.format(', '.join([v.__repr__() for v in self]))
