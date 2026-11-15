#!/usr/bin/env python3
"""Tests for asyncapi_breaking.py: no network, no repo state beyond apis/asyncapi.yaml."""
import importlib.util
import os
import unittest

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("aab", os.path.join(HERE, "asyncapi_breaking.py"))
aab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(aab)

# The real envelope shape: payload.allOf[CloudEvent, {properties.data: $ref}].
BASE = """
components:
  messages:
    OrderPlaced:
      name: OrderPlaced
      payload:
        allOf:
          - $ref: "#/components/schemas/CloudEvent"
          - type: object
            properties:
              data: {$ref: "#/components/schemas/OrderPlacedData"}
  schemas:
    CloudEvent:
      type: object
      properties: {specversion: {type: string}}
    OrderPlacedData:
      type: object
      required: [order_id, promise_date]
      properties:
        order_id: {type: string}
        promise_date: {type: [string, "null"]}
        site_id: {type: string}
"""


def mutated(**changes):
    doc = yaml.safe_load(BASE)
    data = doc["components"]["schemas"]["OrderPlacedData"]
    if changes.get("drop"):
        data["properties"].pop(changes["drop"], None)
        data["required"] = [f for f in data.get("required") or [] if f != changes["drop"]]
    if "rename" in changes:
        data["properties"][changes["rename"]] = data["properties"].pop("order_id")
        data["required"] = [changes["rename"] if f == "order_id" else f for f in data["required"]]
    if "narrow" in changes:
        data["properties"]["promise_date"]["type"] = changes["narrow"]
    if changes.get("drop_message"):
        del doc["components"]["messages"]["OrderPlaced"]
    if "add_message" in changes:
        doc["components"]["messages"]["OrderCancelled"] = {"name": "OrderCancelled", "payload": {}}
    if "add_field" in changes:
        data["properties"][changes["add_field"]] = {"type": "string"}
    if "widen" in changes:
        data["properties"]["order_id"]["type"] = changes["widen"]
    return doc


class Breaking(unittest.TestCase):
    def setUp(self):
        self.base = yaml.safe_load(BASE)

    def test_identical_specs_are_silent(self):
        self.assertEqual(aab.breaking(self.base, yaml.safe_load(BASE)), [])

    def test_a_removed_message_is_breaking(self):
        self.assertTrue(any("REMOVED MESSAGE" in f and "OrderPlaced" in f
                            for f in aab.breaking(self.base, mutated(drop_message=True))))

    def test_a_removed_required_field_is_breaking(self):
        self.assertTrue(any("REMOVED REQUIRED data FIELD OrderPlaced.data.promise_date" in f
                            for f in aab.breaking(self.base, mutated(drop="promise_date"))))

    def test_a_renamed_required_field_is_breaking(self):
        self.assertTrue(any("REMOVED REQUIRED data FIELD OrderPlaced.data.order_id" in f
                            for f in aab.breaking(self.base, mutated(rename="orderRef"))))

    def test_a_narrowed_type_is_breaking(self):
        self.assertTrue(any("NARROWED data FIELD TYPE OrderPlaced.data.promise_date" in f
                            for f in aab.breaking(self.base, mutated(narrow="string"))))

    def test_a_type_swap_is_breaking(self):
        self.assertTrue(any("NARROWED data FIELD TYPE OrderPlaced.data.order_id" in f
                            for f in aab.breaking(self.base, mutated(widen="integer"))))

    def test_additive_changes_pass(self):
        head = mutated(add_message=True, add_field="fulfillment_class", widen=["string", "null"])
        self.assertEqual(aab.breaking(self.base, head), [])

    def test_a_required_field_becoming_optional_is_not_flagged(self):
        head = yaml.safe_load(BASE)
        head["components"]["schemas"]["OrderPlacedData"]["required"] = ["order_id"]
        self.assertEqual(aab.breaking(self.base, head), [])


class RealSpec(unittest.TestCase):
    """The gate must read THIS repo's real catalog through its allOf/$ref shape."""

    def real(self):
        with open(os.path.join(HERE, "..", "..", "apis", "asyncapi.yaml"), encoding="utf-8") as fh:
            return yaml.safe_load(fh)

    def test_every_real_message_resolves_a_data_schema(self):
        doc = self.real()
        messages = doc["components"]["messages"]
        for name, message in messages.items():
            data = aab.data_schema(doc, message)
            self.assertIsInstance(data.get("properties"), dict,
                                  f"{name}: data schema did not resolve through allOf/$ref")

    def test_the_real_spec_against_itself_is_silent(self):
        doc = self.real()
        self.assertEqual(aab.breaking(doc, doc), [])


class Main(unittest.TestCase):
    def test_an_unreadable_spec_is_exit_2_not_a_pass(self):
        self.assertEqual(aab.main([os.path.join(HERE, "nope.yaml"), os.path.join(HERE, "nope.yaml")]), 2)


if __name__ == "__main__":
    unittest.main()
