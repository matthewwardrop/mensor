import itertools
import re
from abc import ABCMeta, abstractmethod, abstractproperty
from enum import Enum

import six


class CONSTRAINTS(Enum):
    NULL = 'null'
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
    def via_next(self, foreign_key, include_generic=False):
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
    def __eq__(self, other):
        raise NotImplementedError

    @abstractmethod
    def __and__(self, other):
        raise NotImplementedError

    @abstractmethod
    def __or__(self, other):
        raise NotImplementedError

    @abstractmethod
    def __invert__(self):
        raise NotImplementedError

    # Convenience methods to help with building evaluation strategies

    @property
    def scoped_applicable(self):
        if self.scoped.kind is CONSTRAINTS.AND:  # Since there is automatic suppression of nested `And`s, this one check if sufficient for generality
            applicable = [
                op for op in self.scoped.operands if op.applicable
            ]
        elif self.scoped.applicable:
            applicable = [self.scoped]
        else:
            applicable = []
        return And.from_operands(applicable)

    def scoped_for_unit_type(self, unit_type):
        return And.from_operands(self.scoped, self.generic.via_next(unit_type if isinstance(unit_type, str) else unit_type.name, include_generic=True))

    def generic_for_provider(self, provider):
        if self.generic.kind is CONSTRAINTS.AND:
            generic_constraints = self.generic.operands[:]
        elif self.generic:
            generic_constraints = [self.generic]
        else:
            generic_constraints = []

        provider_features = (
            set(identifier.name for identifier in provider.identifiers)
            .union(dimension.name for dimension in provider.dimensions)
            .union(measure.name for measure in provider.measures)
        )

        applicable = []
        for constraint in generic_constraints:
            if len(set(constraint.dimensions).difference(provider_features)) == 0:
                applicable.append(constraint)

        return And.from_operands(applicable)


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
            if not operand:
                continue
            elif isinstance(operand, list):
                ops.extend([op for op in operand if op])
            elif isinstance(operand, cls):
                ops.extend(operand.operands)
            elif isinstance(operand, BaseConstraint):
                ops.append(operand)
            else:
                raise ValueError("All children of a `ContainerConstraint` must be instances of subclasses of `BaseConstraint`.")
        if len(ops) == 0:
            return NullConstraint()
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
        if not other:
            return self
        return self.from_operands(other, *self.operands)

    def add_operands(self, others):
        return self.from_operands(*self.operands, others)

    def simplify(self):
        if self.resolvable and len(self.operands) == 1:
            return self.operands[0]
        return self

    # Specification of features affected by this constraint

    @property
    def dimensions(self):
        return list(itertools.chain(*[op.dimensions for op in self.operands]))

    @property
    def depth(self):
        return min(op.depth for op in self.operands)

    def via_next(self, foreign_key, include_generic=False):
        # Any None's in this list will cause the new parent object to be unresolvable.
        n = self.from_operands(*[op.via_next(foreign_key, include_generic=include_generic) for op in self.operands], simplify=False, resolvable=self.resolvable)
        if not n:
            return n
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
        return self.__class__.from_operands([operand.generic for operand in self.operands if operand.has_generic])

    @property
    def scoped(self):
        return self.__class__.from_operands([operand.scoped for operand in self.operands if operand.has_scoped])

    # Mathematical operations
    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        for operand in self.operands:
            if operand not in other.operands:
                return False
        for operand in other.operands:
            if operand not in self.operands:
                return False
        return True


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
        if any(operand.has_generic for operand in operands) and any(operand.has_scoped for operand in operands):
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


class NullConstraint(BaseConstraint):

    @property
    def kind(self):
        return CONSTRAINTS.NULL

    # Specification of features affected by this constraint

    @property
    def dimensions(self):
        return []

    @property
    def depth(self):
        return -1

    def via_next(self, foreign_key, include_generic=False):
        return self

    # Extraction of generic and scoped constraints

    @property
    def resolvable(self):
        return False

    @property
    def has_generic(self):
        return False

    @property
    def has_scoped(self):
        return False

    @property
    def generic(self):
        return self

    @property
    def scoped(self):
        return self

    # Mathematical operations on constraints

    def __eq__(self, other):
        return other.kind == self.kind

    def __and__(self, other):
        return other

    def __or__(self, other):
        return other

    def __invert__(self):
        raise NotImplementedError

    def __nonzero__(self):  # Python 2
        return False

    def __bool__(self):  # Python 3
        return False

    def __repr__(self):
        return "Ø"


class Constraint(BaseConstraint):

    # Definition methods
    @classmethod
    def from_spec(cls, spec):
        if not spec:
            return NullConstraint()
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
        if relation not in ('==', '<', '<=', '>', '>=', 'in'):
            raise ValueError("Invalid relation specified in constraint.")
        if generic and field.count('/') > 1:
            raise ValueError("Generic field name cannot consist of more than one '/'.")
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
        if self.generic:
            return 0
        return len(self.field.split('/')) - 1

    def via_next(self, foreign_key, include_generic=False):
        if not include_generic and self.generic:
            return self
        s = self.field.split('/')
        if len(s) > 1 and s[0] == foreign_key:
            return self.__class__('/'.join(s[1:]), self.relation, self.value, generic=self.generic)
        return NullConstraint()

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
        if self.has_generic:
            return self
        return NullConstraint()

    @property
    def scoped(self):
        if self.has_scoped:
            return self
        return NullConstraint()

    # Mathematical operations on constraints
    def __eq__(self, other):
        if not isinstance(other, Constraint):
            return False
        if self.field != other.field:
            return False
        if self.relation != other.relation:
            return False
        if self.value != other.value:
            return False
        return True

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
        return "{}{}{}".format(('*/' if self.generic else '') + self.field, self.relation if self.relation is not 'in' else ' ∈ ', self.value.__repr__())
