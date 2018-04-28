import itertools
import re
from enum import Enum
from abc import ABCMeta, abstractmethod, abstractproperty

import six


class CONSTRAINTS(Enum):
    AND = 'and'
    OR = 'or'
    IN = 'in'
    EQUALITY = 'eq'
    INEQUALITY_GT = 'ineq_gt'
    INEQUALITY_GTE = 'ineq_gte'
    INEQUALITY_LT = 'ineq_lt'
    INEQUALITY_LTE = 'ineq_lte'


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
    "account/address/country='Australia'" would have a depth of 2. An Or
    constraint with between "account/address/country .." and "account/transactions/..."
    would have depth 1.

    A constraint is resolvable if all of its components as originally specified
    are reachable in the current context (e.g. `.via_next` may cause some
    constraints to go out of scope in an Or statement).

    A constraint is applicable if it has depth 0 and is resolvable.
    """

    @abstractproperty
    def kind(self):
        raise NotImplementedError

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

    @property
    def applicable(self):
        return self.depth == 0 and self.resolvable

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
            if operand is None:
                continue
            elif isinstance(operand, list):
                ops.extend(operand)
            elif isinstance(operand, cls):
                ops.extend(operand.operands)
            elif isinstance(operand, BaseConstraint):
                ops.append(operand)
            else:
                raise ValueError("All children of a `ContainerConstraint` must be instances of subclasses of `BaseConstraint`.")
        if len(ops) == 0:
            return None
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
        n = self.from_operands(*[op.via_next(foreign_key) for op in self.operands], simplify=False, resolvable=self.resolvable)
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
    def kind(self):
        return CONSTRAINTS.AND

    @property
    def operands(self):
        return self._operands

    @operands.setter
    def operands(self, operands):
        if any(operand.has_generic and operand.has_scoped and isinstance(operand, ContainerConstraint) for operand in operands):
            raise ValueError("Generic constraints cannot be nested with non-generic constraints.")
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
    def kind(self):
        return CONSTRAINTS.OR

    @property
    def operands(self):
        return self._operands

    @operands.setter
    def operands(self, operands):
        if any(operand.has_generic and operand.has_scoped for operand in operands):
            raise ValueError("Generic constraints cannot be nested with non-generic constraints.")
        self._operands = operands

    @property
    def depth(self):
        """
        Or statements can only be evaluated together, an so depth is minimum
        depth of shared prefix.
        """
        # Get common prefix of all operands
        common_prefix = ''.join(
            c[0] for c in itertools.takewhile(
                lambda x: all(x[0] == y for y in x),
                zip(*[op.field for op in self.operands if isinstance(op, Constraint)])
            )
        )

        return min([
            len(common_prefix.split('/')) - 1,
            min(op.depth for op in self.operands)
        ])

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
        elif isinstance(spec, BaseConstraint):
            return spec
        elif isinstance(spec, list):
            r = And.from_operands(*[cls.from_spec(s) for s in spec])
            return r
        elif isinstance(spec, tuple):
            return Or.from_operands(*[cls.from_spec(s) for s in spec])
        elif isinstance(spec, dict):
            constraints = []
            for field, value in spec.items():
                generic = False
                if field.startswith('*/'):
                    generic = True
                    field = field[2:]
                constraints.append(cls._get_constraint(field, value, generic=generic))
            return And.from_operands(*constraints)
        raise ValueError("Invalid constraint specification: {} ({})".format(spec, type(spec)))

    @classmethod
    def _get_constraint(cls, field, value, generic=False):
        if isinstance(value, str):
            m = re.match('^[<>][=]?', value)
            if m:
                relation = m.group(0)
                return cls(field, relation=relation, value=value[len(relation):], generic=generic)
            return Constraint(field, '==', value, generic=generic)
        elif isinstance(value, list):
            return cls.from_spec([{('*/' if generic else '') + field: v} for v in value])
        elif isinstance(value, set):
            if any(isinstance(v, tuple) for v in value) or all(isinstance(v, str) and re.match('^[<>][=]?', v) for v in value):
                return cls.from_spec(tuple({('*/' if generic else '') + field: v} for v in value))
            return Constraint(field, 'in', value, generic=generic)
        elif isinstance(value, tuple):
            assert len(value) == 2, "All explicit relations must be of length two."
            return Constraint(field, value[0], value[1], generic=generic)

        return cls(field, '==', value, generic=generic)

    def __init__(self, field, relation, value, generic=False):
        assert relation in ('==', '<', '<=', '>', '>=', 'in'), "Invalid relation specified in constraint."
        self.field = field
        self.relation = relation
        self.value = value
        self._generic = generic

    @property
    def kind(self):
        if self.relation == '==':
            return CONSTRAINTS.EQUALITY
        elif self.relation == '<':
            return CONSTRAINTS.INEQUALITY_LT
        elif self.relation == '<=':
            return CONSTRAINTS.INEQUALITY_LTE
        elif self.relation == '>':
            return CONSTRAINTS.INEQUALITY_GT
        elif self.relation == '>=':
            return CONSTRAINTS.INEQUALITY_GTE
        elif self.relation == 'in':
            return CONSTRAINTS.IN
        raise RuntimeError("Invalid relation detected {}.".format(self.relation))

    # Specification of features affected by this constraint

    @property
    def dimensions(self):
        return [self.field]

    @property
    def depth(self):
        return len(self.field.split('/')) - 1

    def via_next(self, foreign_key):
        s = self.field.split('/')
        if len(s) > 1 and s[0] == foreign_key:
            return self.__class__('/'.join(s[1:]), self.relation, self.value)

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
        return "{}{}{}".format(('*/' if self.generic else '') + self.field, self.relation if self.relation is not 'in' else ' ∈ ', self.value)


class EvaluationContext(object):

    @classmethod
    def from_spec(cls, name=None, unit_type=None, spec=None):
        if isinstance(spec, EvaluationContext):
            return spec
        constraint = Constraint.from_spec(spec)
        if constraint is None:
            generic_constraint = scoped_constraint = None
        else:
            generic_constraint = constraint.generic
            scoped_constraint = constraint.scoped
        return cls(name=name, unit_type=unit_type, scoped_constraint=scoped_constraint, generic_constraint=generic_constraint)

    def __init__(self, name=None, unit_type=None, scoped_constraint=None, generic_constraint=None):
        self.name = name
        self.unit_type = unit_type
        self.generic_constraint = generic_constraint
        self.scoped_constraint = scoped_constraint

    def add_constraint(self, constraint, generic=False):
        assert isinstance(constraint, BaseConstraint)
        if generic:
            if self.generic_constraint is not None:
                self.generic_constraint &= constraint
            else:
                self.generic_constraint = constraint
        else:
            if self.scoped_constraint is not None:
                self.scoped_constraint &= constraint
            else:
                self.scoped_constraint = constraint

    def via_next(self, foreign_key):
        if not isinstance(foreign_key, (type(None),) + six.string_types):
            foreign_key = foreign_key.name
        return self.__class__(
            name=self.name,
            unit_type=foreign_key,
            scoped_constraint=self.scoped_constraint.via_next(foreign_key) if self.scoped_constraint else None,
            generic_constraint=self.generic_constraint
        )

    # Convenience methods to help with building evaluation strategies

    @property
    def scoped_applicable(self):
        if self.scoped_constraint is None:
            return []
        elif self.scoped_constraint.kind is CONSTRAINTS.AND:  # Since there is automatic suppression of nested `And`s, this one check if sufficient for generality
            return [
                op for op in self.scoped_constraint.operands if op.applicable
            ]
        elif self.scoped_constraint.applicable:
            return [self.scoped_constraint]
        return []

    @property
    def generic_applicable(self):
        if self.generic_constraint is None:
            return []
        elif self.generic_constraint.kind is CONSTRAINTS.AND:  # Since there is automatic suppression of nested `And`s, this one check if sufficient for generality
            return [
                op for op in self.generic_constraint.operands if op.applicable
            ]
        elif self.generic_constraint.applicable:
            return [self.generic_constraint]
        return []

    @property
    def scoped_applicable_dimensions(self):
        return list(itertools.chain(
            *[op.dimensions for op in self.scoped_applicable]
        ))

    @property
    def generic_applicable_dimensions(self):
        return list(itertools.chain(
            *[op.dimensions for op in self.generic_applicable]
        ))

    @property
    def dimensions(self):
        dimensions = []
        if self.scoped_constraint:
            dimensions.extend(self.scoped_constraint.dimensions)
        if self.generic_constraint:
            dimensions.extend(self.generic_constraint.dimensions)
        return dimensions
