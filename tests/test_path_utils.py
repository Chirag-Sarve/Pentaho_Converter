"""Tests for Pentaho path resolution in generated PySpark."""

from __future__ import annotations

import unittest

from pentaho_converter.path_utils import (
    is_local_machine_path,
    normalize_text_file_basename,
    spark_load_path_expr,
    spark_save_path_expr,
    spark_text_file_path_expr,
    uses_pentaho_directory_variable,
)


class TestPathUtils(unittest.TestCase):
    def test_internal_directory_uses_data_dir(self):
        raw = "${Internal.Transformation.Filename.Directory}/customers.csv"
        self.assertTrue(uses_pentaho_directory_variable(raw))
        self.assertEqual(
            spark_load_path_expr(raw),
            "f'{PENTAHO_DATA_DIR}/customers.csv'",
        )

    def test_absolute_path_unchanged_on_load(self):
        raw = "/data/inbound/customers.csv"
        self.assertFalse(uses_pentaho_directory_variable(raw))
        self.assertEqual(spark_load_path_expr(raw), "'/data/inbound/customers.csv'")

    def test_windows_local_path_remapped(self):
        raw = r"C:\Users\me\data\out.csv"
        self.assertTrue(is_local_machine_path(raw))
        self.assertEqual(
            spark_load_path_expr(raw),
            "f'{PENTAHO_DATA_DIR}/out.csv'",
        )
        self.assertEqual(
            spark_save_path_expr(raw),
            "f'{PENTAHO_DATA_DIR}/out.csv'",
        )

    def test_save_uses_basename_under_data_dir(self):
        self.assertEqual(
            spark_save_path_expr("/data/out/customers.csv"),
            "f'{PENTAHO_DATA_DIR}/customers.csv'",
        )

    def test_save_missing_name_uses_placeholder(self):
        self.assertEqual(
            spark_save_path_expr(""),
            "f'{PENTAHO_DATA_DIR}/<output_name>'",
        )

    def test_volumes_path_preserved(self):
        raw = "/Volumes/main/default/data/out"
        self.assertEqual(spark_save_path_expr(raw), repr(raw))

    def test_text_file_write_read_basename_continuity(self):
        self.assertEqual(
            normalize_text_file_basename(r"C:\data\orders", "csv"),
            r"C:\data\orders".replace("\\", "/") + ".csv",
        )
        write_expr = spark_text_file_path_expr(r"C:\data\orders", extension="csv")
        read_expr = spark_text_file_path_expr(
            "${Internal.Transformation.Filename.Directory}/orders.csv"
        )
        self.assertEqual(write_expr, read_expr)


if __name__ == "__main__":
    unittest.main()
