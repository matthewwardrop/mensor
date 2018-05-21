import logging
from abc import ABCMeta


class SubclassRegisteringABCMeta(ABCMeta):
    """
    This metaclass provides automatic registration of the subclasses associated
    with classes that use this metaclass. In particular, it allows for a
    dynamic configuration driven lookup of subclasses based on the subclasses
    defined in memory at the time of lookup.
    """

    def __init__(cls, name, bases, dct):
        super(SubclassRegisteringABCMeta, cls).__init__(name, bases, dct)

        if not hasattr(cls, '_registry'):
            cls._registry = {}

        registry_keys = getattr(cls, 'REGISTRY_KEYS', [])
        if registry_keys:
            for key in registry_keys:
                if key in cls._registry and cls.__name__ != cls._registry[key].__name__:
                    logging.info("Ignoring attempt by class `{}` to register key '{}', which is already registered for class `{}`.".format(cls.__name__, key, cls._registry[key].__name__))
                else:
                    cls._registry[key] = cls

    def for_kind(cls, key):
        return cls._registry[key]
