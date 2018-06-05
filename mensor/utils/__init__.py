import copy
import functools
import inspect
import logging
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
        if value in self.__values:
            raise ValueError("'{}' is already in SequenceMap.".format(value))
        self.__values[value] = value

    def extend(self, values):
        for value in values:
            self.append(value)

    def copy(self):
        return self.__class__(self.__values.values())

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
        return iter(self.__values.values())

    def __len__(self):
        return len(self.__values)

    def __eq__(self, other):
        if not isinstance(other, SequenceMap):
            return False
        return set(self) == set(other)

    def __repr__(self):
        return '{{{}}}'.format(', '.join([v.__repr__() for v in self]))


class Options(object):

    def __init__(self, options=None):
        self._options = options or {}

    def add_option(self, name, desc, required, default=None, parser=None):
        if name in self._options:
            raise ValueError("Option of name `{}` already exists.".format(name))
        self._options[name] = {"desc": desc, "required": required, "default": default, "parser": parser, "pinned": False}
        return self

    def process(self, **opts):
        params = {}
        ignoring = []
        for key, value in opts.items():
            if key not in self._options or self._options[key].get('pinned'):
                ignoring.append(key)
            else:
                param_schema = self._options[key]
                params[key] = param_schema['parser'](value) if param_schema['parser'] else value

        if len(ignoring) > 0:
            logging.warning("Ignoring invalid keys: '{}'".format("', '".join(ignoring)))

        for name, schema in self._options.items():
            if name not in params:
                if schema['required'] and not schema['pinned']:
                    raise RuntimeError("A value for parameter `{}` ({}) was not provided.".format(name, schema['desc']))
                else:
                    params[name] = schema['default']
        return params

    def show(self):
        for name in sorted(self._options):
            schema = self._options[name]
            print("{}:".format(name))
            for field in ['desc', 'required', 'default']:
                print("    {}: {}".format(field, schema[field]))
            if schema['parser']:
                print("    Note: Inputs are parsed and/or validated.")

    def copy(self):
        return self.__class__(options=copy.deepcopy(self._options))

    def update(self, opts, pinned=False):
        for name, value in opts.items():
            if name in self._options:
                self._options[name]['pinned'] = pinned
                self._options[name]['required'] = False
                self._options[name]['default'] = value
        return self

    def with_options(self, **opts):
        return self.copy().update(opts)

    def with_pinned(self, **opts):
        return self.copy().update(opts, pinned=True)

    def __getattr__(self, name):
        if name in self._options:
            if self._options[name]['required']:
                raise RuntimeError("No value specified for option '{}'".format(name))
            return self._options[name]['default']
        raise AttributeError

    def __setattr__(self, name, value):
        if name in ['_options']:
            object.__setattr__(self, name, value)
        elif name in self._options:
            self._options[name]['default'] = value
        else:
            raise AttributeError

    def __getitem__(self, name):
        if name in self._options:
            if self._options[name]['required']:
                raise RuntimeError("No value specified for option '{}'".format(name))
            return self._options[name]['default']
        raise KeyError

    def __setitem__(self, name, value):
        if name in self._options:
            self._options[name]['default'] = value
        raise KeyError


class OptionsMixin(object):

    @property
    def opts(self):
        if not hasattr(self, '_OptionsMixin__opts'):
            self.__opts = Options()
        return self.__opts

    @opts.setter
    def opts(self, opts):
        if isinstance(opts, dict):
            self.opts.pin(**opts)
        else:
            if not isinstance(opts, Options):
                raise ValueError("`opts` must be of type `mensor.utils.Options`.")
            self.__opts = opts


def with_opts_processed(f):
    signature = inspect.getfullargspec(f).args

    def wrapped(self, *args, **opts):
        base_args = {}
        for opt in list(opts):
            if opt in signature:
                base_args[opt] = opts.pop(opt)
        opts = self.opts.process(**opts)
        return f(self, *args, **base_args, **opts)

    return wrapped
