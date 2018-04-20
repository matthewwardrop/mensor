import re
from abc import ABCMeta, abstractmethod, abstractproperty

import six


class BaseConstraint(metaclass=ABCMeta):
    """
    This abstract class defines the API contract to which all constraints in
    mensor conform.

    Terminology
    -----------
    A constraint is "scoped" if it is only to be applied to MeasureProviders
    joined in on specific unit_types. A constraint is "generic" if it applies to
    all MeasureProviders that provide all of the dimensions in `.dimensions`.

    A child constraint is called an "operand" to prevent confusing API
    such as ".constraint.constraints".

    The depth of a constraint is the number of joins away from being relevant
    a particular constraint is. For example, a constraint of
    "account/address/country='Australia'" would have a depth of 2.

    A constraint is resolvable if all of its components as originally specified
    are reachable in the current context (e.g. `.via_next` may cause some
    constraints to go out of scope in an Or statement).
    """

    # Specification of features affected by this constraint

    @abstractproperty
    def dimensions(self):
        raise NotImplementedError

    @abstractproperty
    def depth(self):
        raise NotImplementedError

    @abstractmethod
    def via_next(self, foreign_key):
        raise NotImplementedError

    # Extraction of generic and scoped constraints

    @abstractproperty
    def resolvable(self):
        raise NotImplementedError

    @abstractproperty
    def has_generic(self):
        raise NotImplementedError

    @abstractproperty
    def has_scoped(self):
        raise NotImplementedError

    @abstractproperty
    def generic(self):
        raise NotImplementedError

    @abstractproperty
    def scoped(self):
        raise NotImplementedError

    # Mathematical operations on constraints

    @abstractmethod
    def __and__(self, other):
        raise NotImplementedError

    @abstractmethod
    def __or__(self, other):
        raise NotImplementedError

    @abstractmethod
    def __invert__(self):
        raise NotImplementedError


class ContainerConstraint(BaseConstraint):
    """
    All composite constraints are subclasses of `ContainerConstraint`.

    All child `BaseConstraint` instances are called "operands" to avoid
    confusing API conventions like "constraint.constraints".
    """

    # Definition methods

    @classmethod
    def from_operands(cls, *operands, resolvable=True, simplify=True):
        ops = []
        for operand in operands:
            if isinstance(operand, list):
                ops.extend(operand)
            elif operand is None:
                continue
            elif isinstance(operand, ContainerConstraint):
                operand = operand.simplify()
            elif not isinstance(operand, BaseConstraint):
                raise ValueError("All children of a `ContainerConstraint` must be instances of subclasses of `BaseConstraint`.")
            else:
                ops.append(operand)
        if len(ops) == 0:
            return None
        print(ops)
        constraint = cls(ops, resolvable=resolvable)
        if simplify:
            constraint = constraint.simplify()
        return constraint

    def __init__(self, operands, resolvable=True):
        self.operands = operands
        self._resolvable = resolvable
        if len(self.operands) == 0:
            raise RuntimeError("Attempted to create an empty constraint container.")

    def add_operand(self, other):
        if other is None:
            return self
        return self.from_operands(other, *self.operands)

    def add_operands(self, others):
        if others is None:
            return self
        return self.from_operands(*self.operands, *others)

    def simplify(self):
        if self.resolvable and len(self.operands) == 1:
            return self.operands[0]
        return self

    # Specification of features affected by this constraint

    @property
    def dimensions(self):
        return sum([op.dimensions for op in self.operands], [])

    @property
    def depth(self):
        return min(op.depth for op in self.operands)

    def via_next(self, foreign_key):
        # Any None's in this list will cause the new parent object to be unresolvable.
        n = self.from_operands(*[op.via_next(foreign_key) for op in self.operands], simplify=False)
        if n is None:
            return None
        if isinstance(n, Or) and len(n.operands) < len(self.operands):
            n._resolvable = False
        return n

    # Extraction of generic and scoped constraints

    @property
    def resolvable(self):
        return self._resolvable and all(op.resolvable for op in self.operands)

    @property
    def has_generic(self):
        return any(operand.has_generic for operand in self.operands)

    @property
    def has_scoped(self):
        return any(operand.has_scoped for operand in self.operands)

    @property
    def generic(self):
        return self.__class__.from_operands([operand for operand in self.operands if operand.has_generic])

    @property
    def scoped(self):
        return self.__class__.from_operands([operand for operand in self.operands if operand.has_scoped])


class And(ContainerConstraint):

    @property
    def operands(self):
        return self._operands

    @operands.setter
    def operands(self, operands):
        print(operands)
        if any(operand.has_generic and isinstance(operand, ContainerConstraint) for operand in operands):
            raise ValueError("Generic constraints cannot be nested.")
        self._operands = operands

    @property
    def resolvable(self):
        # And results are not dependent on having all members present, so ignore self._resolvable.
        return all(op.resolvable for op in self.operands)

    def __and__(self, other):
        if isinstance(other, And):
            return self.add_operands(other.operands)
        return self.add_operand(other)

    def __or__(self, other):
        if isinstance(other, Or):
            return other.add_operand(self)
        return Or.from_operands(self, other)

    def __invert__(self):
        raise NotImplementedError

    def __repr__(self):
        return "[ {} ]".format(" & ".join(str(op) for op in self.operands))


class Or(ContainerConstraint):

    @property
    def operands(self):
        return self._operands

    @operands.setter
    def operands(self, operands):
        if any(operand.has_generic for operand in operands):
            raise ValueError("Generic constraints cannot be nested.")
        self._operands = operands

    def __and__(self, other):
        if isinstance(other, And):
            return other.add_operand(self)
        return And.from_operands(self, other)

    def __or__(self, other):
        if isinstance(other, Or):
            return self.add_operands(other.operands)
        return self.add_operand(other)

    def __invert__(self):
        raise NotImplementedError

    def __repr__(self):
        return "( {} )".format(" | ".join(str(op) for op in self.operands))


class Constraint(BaseConstraint):

    # Definition methods

    @classmethod
    def from_spec(cls, spec):
        if spec is None:
            return None
        elif isinstance(spec, list):
            return And.from_operands(*[cls.from_spec(s) for s in spec])
        elif isinstance(spec, tuple):
            return Or.from_operands(*[cls.from_spec(s) for s in spec])
        elif isinstance(spec, six.string_types):
            # TODO: Support non-equi expressions
            generic = False
            if spec.startswith('*/'):
                generic = True
                spec = spec[2:]
            m = re.match(r'^([a-zA-Z0-9_/:]+)(=)(.*)$', spec)
            if m is None:
                raise ValueError('Constraint expression does not satisfy for "<field>=<value>".')
            return cls(*m.groups(), generic=generic)
        raise ValueError("Invalid constraint specification: {} ({})".format(spec, type(spec)))

    def __init__(self, expr, relation, rhs, generic=False):
        self.expr = expr
        self.relation = relation
        self.rhs = rhs
        self._generic = generic

    # Specification of features affected by this constraint

    @property
    def dimensions(self):
        return [self.expr]

    @property
    def depth(self):
        return len(self.expr.split('/')) - 1

    def via_next(self, foreign_key):
        s = self.expr.split('/')
        if len(s) > 1 and s[0] == foreign_key:
            return self.__class__('/'.join(s[1:]), self.relation, self.rhs)

    # Extraction of generic and scoped constraints

    @property
    def resolvable(self):
        return True

    @property
    def has_generic(self):
        return self._generic

    @property
    def has_scoped(self):
        return not self._generic

    @property
    def generic(self):
        if self._generic:
            return self

    @property
    def scoped(self):
        if not self._generic:
            return self

    # Mathematical operations on constraints

    def __and__(self, other):
        if isinstance(other, And):
            return other.add_operand(self)
        return And.from_operands(self, other)

    def __or__(self, other):
        if isinstance(other, Or):
            return other.add_operand(self)
        return Or.from_operands(self, other)

    def __invert__(self):
        raise NotImplementedError

    def __repr__(self):
        return str(self.expr) + str(self.relation) + str(self.rhs) + ('(g)' if self._generic else '')


class EvaluationContext(object):

    @classmethod
    def from_spec(cls, name=None, unit_type=None, spec=None):
        if isinstance(spec, EvaluationContext):
            return spec
        constraints = Constraint.from_spec(spec)
        if constraints is None:
            generic_constraints = scoped_constraints = None
        else:
            generic_constraints = constraints.generic
            scoped_constraints = constraints.scoped
        return cls(name=name, unit_type=unit_type, scoped_constraints=scoped_constraints, generic_constraints=generic_constraints)

    def __init__(self, name=None, unit_type=None, scoped_constraints=None, generic_constraints=None):
        self.name = name
        self.unit_type = unit_type
        self.generic_constraints = generic_constraints
        self.scoped_constraints = scoped_constraints

    def add_constraint(self, constraint, generic=False):
        assert isinstance(constraint, BaseConstraint)
        if generic:
            if self.generic_constraints is not None:
                self.generic_constraints &= constraint
            else:
                self.generic_constraints = constraint
        else:
            if self.scoped_constraints is not None:
                self.scoped_constraints &= constraint
            else:
                self.scoped_constraints = constraint

    @property
    def generic_dimensions(self):
        if self.generic_constraints is None:
            return []
        return self.generic_constraints.dimensions

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
            scoped_constraints=self.scoped_constraints.via_next(foreign_key) if self.scoped_constraints else None,
            generic_constraints=self.generic_constraints
        )

    @property
    def scoped_resolvable(self):
        resolvables = []
        if isinstance(self.scoped_constraints, And):  # Since there is automatic suppression of nested `And`s, this one check if sufficient for generality
            for op in self.scoped_constraints.operands:
                if op.resolvable and op.depth == 0:
                    resolvables.append(op)
        elif self.scoped_constraints is not None:
            if self.scoped_constraints.resolvable and self.scoped_constraints.depth == 0:
                resolvables.append(self.scoped_constraints)
        return resolvables

    @property
    def generic_resolvable(self):
        resolvables = []
        if isinstance(self.generic_constraints, And):  # Since there is automatic suppression of nested `And`s, this one check if sufficient for generality
            resolvables.extend(self.generic_constraints.operands)
        elif self.generic_constraints is not None:
            resolvables.append(self.generic_constraints)
        return resolvables
