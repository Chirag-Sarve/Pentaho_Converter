"""Tests for Pentaho path resolution in generated PySpark."""

from __future__ import annotations

import unittest

from pentaho_converter.path_utils import spark_load_path_expr, uses_pentaho_directory_variable


class TestPathUtils(unittest.TestCase):
    def test_internal_directory_uses_data_dir(self):
        raw = "${Internal.Transformation.Filename.Directory}/customers.csv"
        self.assertTrue(uses_pentaho_directory_variable(raw))
        self.assertEqual(
            spark_load_path_expr(raw),
            "f'{PENTAHO_DATA_DIR}/customers.csv'",
        )

    def test_absolute_path_unchanged(self):
        raw = "/data/inbound/customers.csv"
        self.assertFalse(uses_pentaho_directory_variable(raw))
        self.assertEqual(spark_load_path_expr(raw), "'/data/inbound/customers.csv'")


if __name__ == "__main__":
    unittest.main()
