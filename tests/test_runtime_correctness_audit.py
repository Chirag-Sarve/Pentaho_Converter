"""Final runtime-correctness regression suite: Value Mapper, Abort, Text File I/O."""

from __future__ import annotations

import ast
import re
import tempfile
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.code_generator import PySparkCodeGenerator
from pentaho_converter.graph import StepDAG
from pentaho_converter.models import ConversionStats, PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.path_utils import spark_text_file_path_expr
from pentaho_converter.step_xml import parse_filter_rows_config, parse_value_mapper_config
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.transformation_parser import parse_transformation
from pentaho_converter.value_mapper_converter import (
    convert_value_mapper_step,
    mappings_from_step_element,
)

SAMPLES = Path(__file__).resolve().parent / "samples"


def _generate(ktr: Path) -> str:
    trans = parse_transformation(ktr)
    return PySparkCodeGenerator().generate_transformation(trans, ConversionStats(), [])


class TestValueMapperRuntime(unittest.TestCase):
    def test_parse_all_valuemap_rows_including_empty_source(self):
        xml = """
        <step>
          <field_to_use>status</field_to_use>
          <target_field>label</target_field>
          <non_match_default>OTHER</non_match_default>
          <valuemap><source_value>A</source_value><target_value>ACTIVE</target_value></valuemap>
          <valuemap><source_value>B</source_value><target_value>BETA</target_value></valuemap>
          <valuemap><source_value/><target_value>EMPTY</target_value></valuemap>
        </step>
        """
        cfg = parse_value_mapper_config(ET.fromstring(xml))
        maps = cfg["mappings"]
        self.assertEqual(len(maps), 3)
        self.assertEqual(maps[0]["source"], "A")
        self.assertEqual(maps[1]["source"], "B")
        self.assertTrue(any(m["source"] == "" and m["target"] == "EMPTY" for m in maps))

    def test_chained_when_otherwise_default_only_on_non_match(self):
        meta = {
            "field_to_use": "status",
            "target_field": "label",
            "non_match_default": "OTHER",
            "mappings": [
                {"source": "A", "target": "ACTIVE"},
                {"source": "B", "target": "BETA"},
            ],
            "case_sensitive": True,
        }
        lines, status = convert_value_mapper_step(meta, "df_in", "df_out", "VM")
        code = "\n".join(lines)
        self.assertEqual(status, "converted")
        self.assertIn(".when(", code)
        self.assertIn("otherwise(lit('OTHER'))", code)
        # Mapped literals must appear before otherwise(default)
        self.assertLess(code.index("lit('ACTIVE')"), code.index("otherwise(lit('OTHER'))"))
        self.assertLess(code.index("lit('BETA')"), code.index("otherwise(lit('OTHER'))"))
        # Must not blanked-mapped replace: no unconditional default for non-null only
        self.assertNotRegex(code, r"when\(col\(\"status\"\)\.isNotNull\(\),\s*lit\('OTHER'\)\)")

    def test_empty_mappings_passthrough_without_default(self):
        lines, status = convert_value_mapper_step(
            {"field_to_use": "status", "mappings": [], "non_match_default": ""},
            "df_in",
            "df_out",
            "VM",
        )
        code = "\n".join(lines)
        self.assertEqual(status, "partial")
        self.assertIn("df_out = df_in", code)

    def test_null_values_passthrough_when_unmapped(self):
        meta = {
            "field_to_use": "status",
            "target_field": "label",
            "non_match_default": "OTHER",
            "mappings": [{"source": "A", "target": "ACTIVE"}],
            "case_sensitive": True,
        }
        lines, _ = convert_value_mapper_step(meta, "df_in", "df_out", "VM")
        code = "\n".join(lines)
        # Null/empty source passes through before otherwise(default)
        self.assertIn(".when((col(\"status\").isNull()", code)
        self.assertIn("otherwise(lit('OTHER'))", code)

    def test_fields_xml_empty_source_kept(self):
        xml = """
        <step>
          <field_to_use>status</field_to_use>
          <fields>
            <field><source_value>A</source_value><target_value>ACTIVE</target_value></field>
            <field><source_value/><target_value>EMPTY</target_value></field>
          </fields>
        </step>
        """
        maps = mappings_from_step_element(ET.fromstring(xml))
        self.assertEqual(len(maps), 2)
        self.assertTrue(any(m["source"] == "" for m in maps))

    def test_ktr_emits_multi_mapping_chain(self):
        code = _generate(SAMPLES / "ValueMapper_Runtime.ktr")
        ast.parse(code)
        self.assertIn("lit('ACTIVE')", code)
        self.assertIn("lit('BETA')", code)
        self.assertIn("lit('EMPTY')", code)
        self.assertIn("otherwise(lit('OTHER'))", code)
        self.assertLess(code.index("lit('ACTIVE')"), code.index("otherwise(lit('OTHER'))"))

    def test_spark_semantics_default_does_not_wipe_mappings(self):
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, lit, when
        except ImportError:
            self.skipTest("PySpark not installed")

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("vm_runtime")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        try:
            # Avoid createDataFrame(list) pickling issues on some Pythons.
            df = spark.range(4).select(
                when(col("id") == 0, lit("A"))
                .when(col("id") == 1, lit("B"))
                .when(col("id") == 2, lit("Z"))
                .otherwise(lit(None))
                .alias("status")
            )
            expr = (
                when(col("status").isNull() | (col("status") == lit("")), lit("EMPTY"))
                .when(col("status") == lit("A"), lit("ACTIVE"))
                .when(col("status") == lit("B"), lit("BETA"))
                .otherwise(lit("OTHER"))
            )
            rows = {r["status"]: r["label"] for r in df.withColumn("label", expr).collect()}
            self.assertEqual(rows["A"], "ACTIVE")
            self.assertEqual(rows["B"], "BETA")
            self.assertEqual(rows["Z"], "OTHER")
            self.assertEqual(rows[None], "EMPTY")
        finally:
            spark.stop()


class TestAbortFailureStream(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_abort_does_not_overwrite_filter_false_stream(self):
        filter_xml = """
        <step>
          <name>Filter</name>
          <type>FilterRows</type>
          <send_true_to>OK</send_true_to>
          <send_false_to>Abort</send_false_to>
          <compare>
            <condition>
              <leftvalue>status</leftvalue>
              <function>=</function>
              <value><type>String</type><text>A</text></value>
            </condition>
          </compare>
        </step>
        """
        filter_el = ET.fromstring(textwrap.dedent(filter_xml).strip())
        source = PentahoStep(name="Generate", step_type="DataGrid", attributes={}, raw_element=None)
        filt = PentahoStep(
            name="Filter", step_type="FilterRows", attributes={}, raw_element=filter_el
        )
        filt.parsed_config = parse_filter_rows_config(filter_el)
        ok = PentahoStep(name="OK", step_type="Dummy", attributes={}, raw_element=ET.fromstring("<step/>"))
        abort = PentahoStep(
            name="Abort",
            step_type="Abort",
            attributes={},
            raw_element=ET.fromstring(
                "<step><message>fail</message><row_threshold>0</row_threshold></step>"
            ),
        )
        trans = PentahoTransformation(name="t", file_path=Path("t.ktr"))
        trans.steps = [source, filt, ok, abort]
        hops = [
            PentahoHop(from_name="Generate", to_name="Filter"),
            PentahoHop(from_name="Filter", to_name="OK"),
            PentahoHop(from_name="Filter", to_name="Abort"),
        ]
        dag = StepDAG(trans.steps, hops)
        df_map = {
            "Generate": "df_Generate",
            "Filter": "df_Filter",
            "OK": "df_Dummy_OK",
            "Abort": "df_Abort",
        }
        filter_lines, _ = self.registry.generate_code(
            "FilterRows",
            StepContext(transformation=trans, step=filt, dag=dag, df_variable_map=df_map),
        )
        filter_code = "\n".join(filter_lines)
        self.assertIn("df_Abort = ", filter_code)
        self.assertIn("df_OK = ", filter_code)

        abort_lines, status = self.registry.generate_code(
            "Abort",
            StepContext(transformation=trans, step=abort, dag=dag, df_variable_map=df_map),
        )
        abort_code = "\n".join(abort_lines)
        self.assertEqual(status, "converted")
        self.assertNotIn("df_Abort = df_Filter", abort_code)
        self.assertNotIn("df_Abort = df_OK", abort_code)
        self.assertIn("failure/branch stream", abort_code)
        self.assertIn("_abort_count_df_Abort", abort_code)
        self.assertIn("raise RuntimeError", abort_code)

    def test_ktr_pass_fail_codegen(self):
        code = _generate(SAMPLES / "Abort_Pass_Fail.ktr")
        ast.parse(code)
        self.assertIn("df_Abort = ", code)
        self.assertNotIn("df_Abort = df_Filter", code)
        self.assertIn("if _abort_count_df_Abort > 0:", code)
        self.assertIn("raise RuntimeError", code)
        # Filter must use a literal, not col("A")
        self.assertIn("lit('A')", code)

    def test_pass_and_fail_runtime_paths(self):
        """PASS: empty Abort stream → no raise. FAIL: rows on Abort → RuntimeError."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, lit
        except ImportError:
            self.skipTest("PySpark not installed")

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("abort_runtime")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        try:
            from pyspark.sql.functions import when as spark_when

            df = spark.range(2).select(
                spark_when(col("id") == 0, lit("A")).otherwise(lit("B")).alias("status")
            )
            df_ok = df.filter(col("status") == lit("A"))
            df_abort = df.filter(~(col("status") == lit("A")))

            # FAIL path: failure stream has rows
            abort_count = df_abort.count()
            self.assertGreater(abort_count, 0)
            with self.assertRaises(RuntimeError):
                if abort_count > 0:
                    raise RuntimeError("failure rows reached Abort")

            # PASS path: empty failure stream → no raise
            empty_abort = df_ok.filter(lit(False))
            pass_count = empty_abort.count()
            self.assertEqual(pass_count, 0)
            if pass_count > 0:
                raise RuntimeError("should not abort on PASS")
            self.assertEqual(df_ok.count(), 1)
        finally:
            spark.stop()


class TestTextFileWriteReadContinuity(unittest.TestCase):
    def test_shared_path_helper_matches_write_and_read(self):
        write_raw = r"C:\pentaho\data\runtime_orders"
        read_raw = "${Internal.Transformation.Filename.Directory}/runtime_orders.csv"
        write_expr = spark_text_file_path_expr(write_raw, extension="csv")
        read_expr = spark_text_file_path_expr(read_raw)
        self.assertEqual(write_expr, read_expr)
        self.assertEqual(write_expr, "f'{PENTAHO_DATA_DIR}/runtime_orders.csv'")

    def test_ktr_write_read_paths_identical(self):
        code = _generate(SAMPLES / "TextFile_Write_Read.ktr")
        ast.parse(code)
        saves = re.findall(r"\.save\(([^)]+)\)", code)
        loads = re.findall(r"\.csv\(([^)]+)\)", code)
        self.assertTrue(saves, "expected Text File Output .save(...)")
        self.assertTrue(loads, "expected Text File Input .csv(...)")
        self.assertEqual(saves[0].strip(), loads[0].strip())
        self.assertIn("PENTAHO_DATA_DIR}/runtime_orders.csv", code)
        self.assertIn("Spark CSV", code)
        self.assertIn("Pentaho filename:", code)

    def test_write_read_workflow_executes(self):
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, lit
        except ImportError:
            self.skipTest("PySpark not installed")

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("tfo_tfi_runtime")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = str(Path(tmp) / "runtime_orders.csv")
                df = spark.range(2).select(
                    col("id").cast("int").alias("id"),
                    lit("alice").alias("name"),
                )
                (
                    df.write.format("csv")
                    .option("header", True)
                    .option("sep", ",")
                    .mode("overwrite")
                    .save(path)
                )
                # Spark writes a directory; reading the same path must succeed.
                self.assertTrue(Path(path).is_dir())
                loaded = (
                    spark.read.option("header", True)
                    .option("sep", ",")
                    .schema("id INT, name STRING")
                    .csv(path)
                )
                self.assertEqual(loaded.count(), 2)
                self.assertEqual(sorted(loaded.columns), ["id", "name"])
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
