"""Tests for Databricks notebook source conversion."""

from __future__ import annotations

import ast
import unittest

from databricks.databricks_client import to_notebook_source


_SAMPLE_CODE = '''"""
Auto-generated PySpark code
"""

TARGET_CATALOG = 'main'
TARGET_SCHEMA = 'default'

from pyspark.sql import SparkSession

def run_process_sales(spark):
    """Execute transformation: process_sales"""

    # Step: CSV file input (CsvInput) [converted]
    df_CSV_file_input = spark.read.csv("customers.csv")

    # Step: Table output (TableOutput) [converted]
    df_Table_output = df_CSV_file_input
    df_Table_output.write.saveAsTable("sales_by_country")

    return df_Table_output


def run_workflow(spark):
    """Run all Pentaho transformations for job: sales_etl_job

    Databricks notebook: run_workflow(spark)
    """
    print('Running transformation: process_sales')
    result_process_sales = run_process_sales(spark)
    return result_process_sales


def main():
    _owns_spark_session = False
    _spark = globals().get("spark")
    if _spark is None:
        _spark = SparkSession.getActiveSession()
    if _spark is None:
        _spark = SparkSession.builder.getOrCreate()
        _owns_spark_session = True
    try:
        run_process_sales(_spark)
    finally:
        if _owns_spark_session:
            _spark.stop()


if __name__ == "__main__":
    main()
'''


class TestDatabricksNotebook(unittest.TestCase):
    def test_cells_do_not_orphan_return(self):
        cells = [
            c.strip()
            for c in to_notebook_source(_SAMPLE_CODE).split("# COMMAND ----------")
            if c.strip()
        ]
        self.assertGreaterEqual(len(cells), 2)

        for cell in cells:
            if "return " not in cell:
                continue
            self.assertIn("def run_", cell, "return must stay inside a function cell")
            wrapped = cell.replace("# Databricks notebook source\n", "")
            ast.parse(wrapped)

    def test_many_steps_stay_in_one_run_cell(self):
        many_steps = _SAMPLE_CODE.replace(
            "    # Step: CSV file input (CsvInput) [converted]",
            "    # Step: CSV file input (CsvInput) [converted]\n"
            "    # Step: Filter rows (FilterRows) [converted]\n"
            "    # Step: Group by (GroupBy) [converted]\n"
            "    # Step: Sort rows (SortRows) [converted]",
        )
        cells = [
            c.strip()
            for c in to_notebook_source(many_steps).split("# COMMAND ----------")
            if c.strip()
        ]
        run_cells = [c for c in cells if "def run_process_sales" in c]
        self.assertEqual(len(run_cells), 1)
        self.assertIn("return df_Table_output", run_cells[0])

    def test_notebook_omits_main_and_uses_spark_runner(self):
        cells = [
            c.strip()
            for c in to_notebook_source(_SAMPLE_CODE).split("# COMMAND ----------")
            if c.strip()
        ]
        combined = "\n\n".join(cells)
        self.assertNotIn("def main():", combined)
        self.assertNotIn('if __name__ == "__main__"', combined)

        runner = cells[-1]
        self.assertIn("run_workflow(spark)", runner)
        self.assertIn("Do not call main()", runner)


if __name__ == "__main__":
    unittest.main()
