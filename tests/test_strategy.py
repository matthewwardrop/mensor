import os
import unittest

from mensor.backends.pandas import PandasMeasureProvider
from mensor.constraints import Constraint
from mensor.measures import MetaMeasureProvider


class EvaluationStrategyTests(unittest.TestCase):
    def setUp(self):
        self.registry = MetaMeasureProvider()

        data_dir = os.path.join(os.path.dirname(__file__), "data")

        people = (
            PandasMeasureProvider(
                name="people", data=os.path.join(data_dir, "people.csv")
            )
            .add_identifier("person", expr="id", role="primary")
            .add_dimension("name")
            .add_measure("age")
            .add_partition("ds")
        )
        self.registry.register(people)

        people2 = (
            PandasMeasureProvider(
                name="people2", data=os.path.join(data_dir, "people.csv")
            )
            .add_identifier("person", expr="id", role="unique")
            .add_identifier("geography", expr="id_geography", role="foreign")
            .add_partition("ds")
        )
        self.registry.register(people2)

        geographies = (
            PandasMeasureProvider(
                name="geographies", data=os.path.join(data_dir, "geographies.csv")
            )
            .add_identifier("geography", expr="id_geography", role="primary")
            .add_dimension("name")
            .add_measure("population")
            .add_partition("ds")
        )
        self.registry.register(geographies)

        transactions = (
            PandasMeasureProvider(
                name="transactions", data=os.path.join(data_dir, "transactions.csv")
            )
            .add_identifier("transaction", expr="id", role="primary")
            .add_identifier("person:buyer", expr="id_buyer", role="foreign")
            .add_identifier("person:seller", expr="id_seller", role="foreign")
            .add_measure("value")
            .add_partition("ds", requires_constraint=True)
        )
        self.registry.register(transactions)

    def test_simple(self):
        es = self.registry.get_strategy("person", measures=["age"], segment_by=["name"])

        self.assertIn("age", es.measures)
        self.assertIn("name", es.segment_by)

    def test_simple_where(self):
        es = self.registry.get_strategy(
            "person", measures=["age"], segment_by=["name"], where={"ds": "2018-01-01"}
        )

        self.assertIn("age", es.measures)
        self.assertIn("name", es.segment_by)
        self.assertIn("ds", es.segment_by)
        self.assertTrue(es.segment_by["ds"].private)
        self.assertTrue(es.segment_by["ds"].implicit)
        self.assertIsInstance(es.where, Constraint)
        self.assertEqual(es.where.field, "ds")
        self.assertEqual(es.where.value, "2018-01-01")
        self.assertEqual(es.where.relation, "==")

    def test_requires_constraint_enforced(self):
        es = self.registry.get_strategy("transaction", measures=["value"])
        self.assertRaises(RuntimeError, es._check_constraints)

        es = self.registry.get_strategy(
            "transaction", measures=["value"], where={"ds": "2018-01-01"}
        )
        es._check_constraints()

    def test_multiple_providers_for_unit_type(self):
        es = self.registry.get_strategy(
            "person:seller", measures=["age"], segment_by=["geography"]
        )

        self.assertIn("age", es.measures)
        self.assertIn("geography", es.segment_by)
        self.assertFalse(es.segment_by["geography"].private)
        self.assertTrue(es.segment_by["geography"].external)
        self.assertIn("person:seller", es.segment_by)
        self.assertTrue(es.segment_by["person:seller"].private)
        self.assertIn("ds", es.segment_by)

        self.assertEqual(es.joins[0].provider.name, "people2")

        self.assertEqual(set(es.joins[0].join_on_left), set(["person:seller", "ds"]))
        self.assertEqual(set(es.joins[0].join_on_right), set(["person:seller", "ds"]))

        self.assertIsNone(es.joins[0].join_prefix)

    def test_primary_key_requirement(self):
        es = self.registry.get_strategy("person", segment_by=["geography"])

        self.assertEqual(len(es.joins), 1)
        self.assertEqual(es.joins[0].unit_type, "person")
        self.assertEqual(set(es.joins[0].segment_by), {"person", "geography", "ds"})

    def test_forward_joins(self):
        es = self.registry.get_strategy(
            "transaction",
            measures=["person:seller/age"],
            segment_by=["person:buyer/name"],
            where={"*/ds": "2018-01-01"},
        )

        # Top-level strategy check
        self.assertIn("person:seller/age", es.measures)
        self.assertFalse(es.measures["person:seller/age"].private)
        self.assertIn("person:buyer/name", es.segment_by)
        self.assertTrue(es.segment_by["person:buyer/name"].external)
        self.assertFalse(es.segment_by["person:buyer/name"].private)
        self.assertIn("person:seller", es.segment_by)
        self.assertTrue(es.segment_by["person:seller"].private)
        self.assertIn("person:buyer", es.segment_by)
        self.assertTrue(es.segment_by["person:buyer"].private)
        self.assertIn("ds", es.segment_by)
        self.assertTrue(es.segment_by["ds"].private)

        # Joins check
        self.assertEqual(len(es.joins), 2)

        for join in es.joins:
            if join.unit_type == "person:seller":
                self.assertEqual({"person:seller", "ds"}, set(join.join_on_right))
                self.assertEqual({"person:seller", "ds"}, set(join.join_on_left))
            elif join.unit_type == "person:buyer":
                self.assertEqual({"person:buyer", "ds"}, set(join.join_on_right))
                self.assertEqual({"person:buyer", "ds"}, set(join.join_on_left))
            else:
                raise ValueError("Invalid unit type detected.")

    def test_reverse_joins(self):
        es = self.registry.get_strategy(
            "person:seller",
            measures=["transaction/value"],
            segment_by=["name", "person:seller"],
            where={"*/ds": "2018-01-01"},
        )

        self.assertEqual(es.strategy_type, es.Type.REGULAR)
        self.assertIn("transaction/value", es.measures)
        self.assertFalse(es.measures["transaction/value"].private)
        self.assertTrue(es.measures["transaction/value"].external)
        self.assertIn("name", es.segment_by)
        self.assertFalse(es.segment_by["name"].private)
        self.assertIn("person:seller", es.segment_by)
        self.assertFalse(es.segment_by["person:seller"].private)

        # Join information
        rjoin = es.joins[0]
        self.assertEqual(rjoin.join_prefix, "transaction")
        self.assertEqual(rjoin.unit_type, "person:seller")
        self.assertEqual(rjoin.strategy_type, es.Type.UNIT_REBASE)
        self.assertIn("person:seller", rjoin.segment_by)
        self.assertIn("ds", rjoin.segment_by)
        self.assertEqual({"person:seller", "ds"}, set(rjoin.join_on_left))
        self.assertEqual({"person:seller", "ds"}, set(rjoin.join_on_right))

    def test_automatic_masking(self):
        es = self.registry.get_strategy(
            "person:seller",
            measures=["transaction/value"],
            segment_by=["person:seller"],
        )

        self.assertIn("person:seller", es.segment_by)

        person_dimension = es.segment_by["person:seller"]
        self.assertEqual(person_dimension.mask, "person:seller")
        self.assertEqual(person_dimension.name, "person")
