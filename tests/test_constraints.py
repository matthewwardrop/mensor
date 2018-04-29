import unittest

from mensor.measures.context import Constraint, And, Or, EvaluationContext


class PandasMeasureProviderTests(unittest.TestCase):

    def test_constraint(self):

        valid_ops = {
            '==': 100,
            '<': "string",
            '<=': 100,
            '>': "string",
            '>=': 100,
            'in': [1, 2, 3, 4, 5]
        }
        invalid_ops = {
            '': 100,
            None: 'string',
            'my_op': 100
        }

        for rel, value in valid_ops.items():
            c = Constraint('a', rel, value)
            self.assertEquals(c.field, 'a')
            self.assertEquals(c.relation, rel)
            self.assertEquals(c.value, value)
            self.assertFalse(c.generic)

        for rel, value in invalid_ops.items():
            self.assertRaises(ValueError, Constraint, 'a', rel, value)

    def test_constraint_specs(self):
        c = Constraint.from_spec(spec={'a': 10})
        self.assertIsInstance(c, Constraint)
        self.assertEqual(c.field, 'a')
        self.assertEqual(c.value, 10)
        self.assertEqual(c.relation, '==')
        self.assertFalse(c.generic)

        c = Constraint.from_spec(spec={'a': "10"})
        self.assertIsInstance(c, Constraint)
        self.assertEqual(c.field, 'a')
        self.assertEqual(c.value, "10")
        self.assertEqual(c.relation, '==')
        self.assertFalse(c.generic)

        c = Constraint.from_spec(spec={'*/a': ('>', 10)})
        self.assertIsInstance(c, Constraint)
        self.assertEqual(c.field, 'a')
        self.assertEqual(c.value, 10)
        self.assertEqual(c.relation, '>')
        self.assertTrue(c.generic)

        c = Constraint.from_spec(spec={'a': '>10'})
        self.assertIsInstance(c, Constraint)
        self.assertEqual(c.field, 'a')
        self.assertEqual(c.value, '10')
        self.assertEqual(c.relation, '>')
        self.assertFalse(c.generic)

        c = Constraint.from_spec(spec={'a': {1, 2, 3}})
        self.assertIsInstance(c, Constraint)
        self.assertEqual(c.field, 'a')
        self.assertEqual(c.value, {1, 2, 3})
        self.assertEqual(c.relation, 'in')
        self.assertFalse(c.generic)

        c = Constraint.from_spec(spec={'*/a': [1, 2, "10"]})
        self.assertIsInstance(c, And)
        self.assertEqual(len(c.operands), 3)

        c = Constraint.from_spec(spec={'a': {('<', 10), ('>', 11)}})
        self.assertIsInstance(c, Or)
        self.assertEqual(len(c.operands), 2)

        c = Constraint.from_spec(spec=[{'a': 10, 'field': 11}])
        self.assertIsInstance(c, And)
        self.assertEqual(len(c.operands), 2)

        c = Constraint.from_spec(spec=({'a': 10}, {'field': 11}))
        self.assertIsInstance(c, Or)
        self.assertEqual(len(c.operands), 2)

    def test_resolvability(self):
        c = Constraint.from_spec({'unit/a': 1, 'unit/b': 2, 'type/c': 3})
        self.assertTrue(c.via_next('unit').resolvable)

        c = Constraint.from_spec(({'unit/a': 1, 'unit/b': 2}, {'type/c': 3}))
        self.assertFalse(c.via_next('unit').resolvable)

    def test_constraint_arithmetic(self):
        c1 = Constraint.from_spec({'a': 10})
        c2 = Constraint.from_spec(({'b': 20}, {'c': 30}))
        c3 = Constraint.from_spec({'d': 40, 'e': 50})

        c = c1 & c2
        self.assertIsInstance(c, And)

        c = c1 | c2
        self.assertIsInstance(c, Or)

        c = c1 | c2 & c3
        self.assertIsInstance(c, Or)

        # Commutativity
        self.assertEqual(c1 & c2, c2 & c1)
        self.assertEqual(c1 | c2, c2 | c1)

        # Associativity
        self.assertEqual((c1 & c2) & c3, c1 & (c2 & c3))
        self.assertEqual((c1 | c2) | c3, c1 | (c2 | c3))

    def test_generic_scoped(self):
        c = Constraint.from_spec({'a': 10})
        self.assertTrue(c.has_scoped)
        self.assertFalse(c.has_generic)
        self.assertEqual(c.scoped, c)

        c = Constraint.from_spec({'*/a': 10})
        self.assertFalse(c.has_scoped)
        self.assertTrue(c.has_generic)
        self.assertEqual(c.generic, c)

        c = Constraint.from_spec({'a': 10, '*/b': 20})
        self.assertTrue(c.has_scoped)
        self.assertTrue(c.has_generic)
        self.assertEqual(c.scoped, Constraint.from_spec({'a': 10}))
        self.assertEqual(c.generic, Constraint.from_spec({'b': 20}))

        self.assertRaises(ValueError, Constraint.from_spec, ({'*/b': 20}, {'c': 30}))

    def test_evaluation_context(self):
        ev = EvaluationContext.from_spec(name='test', unit_type='test', spec={'a': 10})

        constraint = Constraint.from_spec({'a': 10})

        self.assertEquals(ev.scoped_constraint, constraint)
        self.assertIsNone(ev.generic_constraint)
