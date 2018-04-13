import re
from abc import ABCMeta, abstractmethod, abstractproperty

import six

from .types import *


class BaseConstraint(metaclass=ABCMeta):

    @classmethod
    def from_operands(cls, *operands, resolvable=True):
        if len(operands) == 0:
            return None
        return cls(*operands, resolvable=resolvable)

    def __init__(self, *operands, resolvable=True):
        self.operands = list(op for op in operands if op is not None)
        if len(operands) == 0:
            raise RuntimeError()
        self._resolvable = resolvable

    # @abstractmethod
    # def __invert__(self):
    #     raise NotImplementedError

    @abstractmethod
    def __or__(self, other):
        raise NotImplementedError

    @abstractmethod
    def __and__(self, other):
        raise NotImplementedError

    @abstractproperty
    def by_scoping(self):
        raise NotImplementedError

    @property
    def scoped(self):
        for operand in self.operands:
            if operand.scoped:
                return True
        return False

    def add_operand(self, other):
        if other is None:
            return self
        return self.from_operands(other, *self.operands, resolvable=self.resolvable and other.resolvable)

    def add_operands(self, others):
        if other is None:
            return self
        return self.from_operands(*self.operands, *other, resolvable=self.resolvable and other.resolvable)

    @property
    def resolvable(self):
        return self._resolvable and all(op.resolvable for op in self.operands)

    @property
    def dimensions(self):
        return sum([op.dimensions for op in self.operands], [])

    def via_next(self, foreign_key):
        n = self.from_operands(*[op.via_next(foreign_key) for op in self.operands])
        if n is None:
            return None
        if len(n.operands) < len(self.operands):
            n._resolvable = False
        return n

    @property
    def depth(self):
        return min(op.depth for op in self.operands)


class And(BaseConstraint):

    def __and__(self, other):
        if isinstance(other, And):
            return self.add_operands(other.operands)
        return self.add_operand(other)

    def __or__(self, other):
        if isinstance(other, Or):
            return other.add_operand(self)
        return Or(self, other)

    def __repr__(self):
        return "[ {} ]".format(" & ".join(str(op) for op in self.operands))

    @property
    def by_scoping(self):
        scoped = []
        unscoped = []
        for constraint in self.operands:
            if constraint.scoped:
                if not isinstance(constraint, Constraint):
                    raise RuntimeError("Nested scoping is not supported.")
                scoped.append(constraint)
            else:
                unscoped.append(constraint)
        return self.__class__.from_operands(*unscoped), self.__class__.from_operands(*scoped)


class Or(BaseConstraint):

    def __and__(self, other):
        if isinstance(other, And):
            return other.add_operand(self)
        return And(self, other)

    def __or__(self, other):
        if isinstance(other, Or):
            return self.add_operands(other.operands)
        return self.add_operand(other)

    def __repr__(self):
        return "( {} )".format(" | ".join(str(op) for op in self.operands))

    @property
    def by_scoping(self):
        for constraint in self.operands:
            if constraint.scoped:
                raise RuntimeError("Scoping only allowed for top-level features.")
                scoped.append(constraint)
        return self, None


class Constraint(BaseConstraint):

    @classmethod
    def from_spec(cls, spec):
        if spec is None:
            return None
        elif isinstance(spec, list):
            return And(*[cls.from_spec(s) for s in spec])
        elif isinstance(spec, tuple):
            return Or(*[cls.from_spec(s) for s in spec])
        elif isinstance(spec, six.string_types):
            # TODO: Support non-equi expressions
            scoped = False
            if spec.startswith('*/'):
                scoped = True
                spec = spec[2:]
            m = re.match(r'^([a-zA-Z0-9_/:]+)(=)(.*)$', spec)
            if m is None:
                raise ValueError('Constraint expression does not satisfy for "<field>=<value>".')
            return cls(*m.groups(), scoped=scoped)
        raise ValueError("Invalid constraint specification: {} ({})".format(spec, type(spec)))

    def __init__(self, expr, relation, rhs, scoped=False):
        self.expr = expr
        self.relation = relation
        self.rhs = rhs
        self._scoped = scoped

    @property
    def scoped(self):
        return self._scoped

    def __invert__(self):
        raise NotImplementedError

    def __or__(self, other):
        if isinstance(other, Or):
            return other.add_operand(self)
        return Or(self, other)

    def __and__(self, other):
        if isinstance(other, And):
            return other.add_operand(self)
        return And(self, other)

    def __repr__(self):
        return str(self.expr) + str(self.relation) + str(self.rhs) + ('(s)' if self.scoped else '')

    @property
    def resolvable(self):
        return True

    @property
    def depth(self):
        return len(self.expr.split('/')) - 1

    @property
    def dimensions(self):
        return [self.expr]

    def via_next(self, foreign_key):
        s = self.expr.split('/')
        if len(s) > 1 and s[0] == foreign_key:
            return self.__class__('/'.join(s[1:]), self.relation, self.rhs)

    @property
    def by_scoping(self):
        if self.scoped:
            return None, self
        return self, None


class EvaluationContext(object):

    @classmethod
    def from_spec(cls, name=None, unit_type=None, spec=None):
        # TODO: Scoped constraints from "*/ds" notation
        if isinstance(spec, EvaluationContext):
            return spec
        constraints = Constraint.from_spec(spec)
        if constraints is None:
            scoped_constraints = None
        else:
            constraints, scoped_constraints = Constraint.from_spec(spec).by_scoping
        return cls(name=name, unit_type=unit_type, constraints=constraints, scoped_constraints=scoped_constraints)

    def __init__(self, name=None, unit_type=None, constraints=None, scoped_constraints=None):
        self.name = name
        self.unit_type = unit_type
        self.constraints = constraints
        self.scoped_constraints = scoped_constraints

    def add_constraint(self, constraint, scoped=False):
        assert isinstance(constraint, BaseConstraint)
        if scoped:
            if self.scoped_constraints is not None:
                self.scoped_constraints &= constraint
            else:
                self.scoped_constraints = constraint
        else:
            if self.constraints is not None:
                self.constraints &= constraint
            else:
                self.constraints = constraint

    @property
    def dimensions(self):
        if self.constraints is None:
            return []
        return self.constraints.dimensions

    @property
    def scoped_dimensions(self):
        if self.scoped_constraints is None:
            return []
        return self.scoped_constraints.dimensions

    def via_next(self, foreign_key):
        if not isinstance(foreign_key, (type(None),) + six.string_types):
            foreign_key = foreign_key.name
        return self.__class__(
            name=self.name,
            unit_type=foreign_key,
            constraints=self.constraints.via_next(foreign_key) if self.constraints else None,
            scoped_constraints=self.scoped_constraints
        )

    @property
    def resolvable(self):
        resolvables = []
        if isinstance(self.constraints, And):  # Since there is automatic suppression of nested `And`s, this one check if sufficient for generality
            for op in self.constraints.operands:
                if op.resolvable and op.depth == 0:
                    resolvables.append(op)
        elif self.constraints is not None:
            if self.constraints.resolvable and self.constraints.depth == 0:
                resolvables.append(self.constraints)
        return resolvables

    @property
    def scoped_resolvable(self):
        # TODO: enforce the below elsewhere
        assert not isinstance(self.scoped_constraints, Or)

        resolvables = []
        if isinstance(self.scoped_constraints, And):  # Since there is automatic suppression of nested `And`s, this one check if sufficient for generality
            resolvables.extend(self.scoped_constraints.operands)
        elif self.scoped_constraints is not None:
            resolvables.append(self.scoped_constraints)
        return resolvables
