"""Tests for Databricks table name resolution."""

from __future__ import annotations

import unittest

from pentaho_converter.generation_config import GenerationConfig
from pentaho_converter.table_names import qualify_table_name, resolve_target_schema, table_write_lines


class TestTableNames(unittest.TestCase):
    def test_public_schema_maps_to_default(self):
        self.assertEqual(resolve_target_schema("PUBLIC", "default"), "default")
        self.assertEqual(resolve_target_schema("dbo", "default"), "default")

    def test_meaningful_schema_preserved(self):
        self.assertEqual(resolve_target_schema("analytics", "default"), "analytics")

    def test_qualify_table_name(self):
        cfg = GenerationConfig(catalog="main", schema="default")
        self.assertEqual(
            qualify_table_name("sales_by_country", "PUBLIC", config=cfg),
            "main.default.sales_by_country",
        )
        self.assertEqual(
            qualify_table_name("dim_customer", "analytics", config=cfg),
            "main.analytics.dim_customer",
        )

    def test_table_write_uses_target_vars(self):
        lines = table_write_lines(
            out_var="df_out",
            in_df="df_in",
            table="sales_by_country",
            source_schema="PUBLIC",
            step_name="Table output",
        )
        code = "\n".join(lines)
        self.assertIn("TARGET_CATALOG", code)
        self.assertIn("TARGET_SCHEMA", code)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS", code)
        self.assertIn("sales_by_country", code)


if __name__ == "__main__":
    unittest.main()
