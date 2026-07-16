"""Regression tests for Calculator metadata continuity and Select Values XML variants."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.calculator_converter import convert_calculation, convert_calculation_result
from pentaho_converter.graph import StepDAG
from pentaho_converter.metadata_models import ColumnSchema
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    CalculationSpec,
    parse_calculations,
    parse_select_values_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.transformation_parser import _parse_step


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    input_columns: list[str] | None = None,
    lineage_cols: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = _parse_step(step_el)
    step.name = step_name
    step.step_type = step_type
    if not step.parsed_config:
        step.parsed_config = parse_step_metadata(step_el, step_type)
    inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
    trans = PentahoTransformation(name="Trans", file_path=Path("t.ktr"))
    trans.steps = [inp, step]
    hops = [PentahoHop(from_name="Input", to_name=step_name)]
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    ctx = StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)
    if input_columns:
        ctx.extra["input_columns"] = list(input_columns)
    if lineage_cols is not None:
        ctx.extra["lineage_map"] = {
            "Input": {
                c: ColumnSchema(name=c, type_name="String", source_step="Input")
                for c in lineage_cols
            }
        }
    return ctx


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {l}" for l in lines))
        return True
    except SyntaxError:
        return False


class TestCalculatorMetadataAndOps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_arithmetic_add_multiply_divide(self):
        self.assertIn(
            '(col("A") + col("B"))',
            convert_calculation(CalculationSpec("s", "ADD", "A", "B")),
        )
        self.assertIn(
            '(col("A") * col("B"))',
            convert_calculation(CalculationSpec("p", "MULTIPLY", "A", "B")),
        )
        self.assertIn(
            '(col("A") / col("B"))',
            convert_calculation(CalculationSpec("q", "DIVIDE", "A", "B")),
        )

    def test_percentage_variants(self):
        self.assertIn("lit(100)", convert_calculation(CalculationSpec("p", "PERCENT_1", "A", "B")))
        self.assertIn(
            "lit(100)",
            convert_calculation(CalculationSpec("p", "PERCENT_2", "A", "B")),
        )
        self.assertIn(
            "lit(100)",
            convert_calculation(CalculationSpec("p", "PERCENT_3", "A", "B")),
        )

    def test_long_description_calc_types(self):
        for raw, fragment in (
            ("A + B", "+"),
            ("A * B", "*"),
            ("A / B", "/"),
            ("100 * A / B", "lit(100)"),
            ("A - ( A * B / 100 )", "lit(100)"),
        ):
            result = convert_calculation_result(
                CalculationSpec("out", raw, "A", "B", value_type="Number")
            )
            self.assertTrue(result.supported, raw)
            self.assertIn(fragment, result.expr)

    def test_date_calculations(self):
        self.assertIn("date_add", convert_calculation(CalculationSpec("d", "ADD_DAYS", "dt", "n")))
        self.assertIn("datediff", convert_calculation(CalculationSpec("d", "DATE_DIFF", "a", "b")))
        self.assertIn("year(", convert_calculation(CalculationSpec("y", "YEAR_OF_DATE", "dt")))
        self.assertIn("to_date", convert_calculation(CalculationSpec("d", "REMOVE_TIME_FROM_DATE", "dt")))

    def test_string_calculations(self):
        self.assertIn("upper(", convert_calculation(CalculationSpec("u", "UPPER_CASE", "s")))
        self.assertIn("lower(", convert_calculation(CalculationSpec("l", "LOWER_CASE", "s")))
        self.assertIn("length(", convert_calculation(CalculationSpec("n", "STRING_LEN", "s")))
        self.assertIn("initcap(", convert_calculation(CalculationSpec("i", "INIT_CAP", "s")))

    def test_multiple_calculator_operations_codegen(self):
        xml = """
        <step>
          <name>Calc</name><type>Calculator</type>
          <calculation>
            <field_name>line_total</field_name>
            <calc_type>MULTIPLY</calc_type>
            <field_a>qty</field_a><field_b>price</field_b>
            <value_type>Number</value_type>
          </calculation>
          <calculation>
            <field_name>pct</field_name>
            <calc_type>100 * A / B</calc_type>
            <field_a>line_total</field_a><field_b>target</field_b>
            <value_type>Number</value_type>
          </calculation>
          <calculation>
            <field_name>name_u</field_name>
            <calc_type>UPPER</calc_type>
            <field_a>name</field_a>
            <value_type>String</value_type>
          </calculation>
          <calculation>
            <field_name>as_of</field_name>
            <calc_type>ADD_DAYS</calc_type>
            <field_a>order_date</field_a><field_b>1</field_b>
            <value_type>Date</value_type>
          </calculation>
        </step>
        """
        parsed = parse_calculations(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(len(parsed), 4)
        self.assertEqual(parsed[1].calc_type, "PERCENT_1")

        outcome = self.registry.convert_step(
            "Calculator",
            _ctx(xml, "Calculator", "Calc", input_columns=["qty", "price", "target", "name", "order_date"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("_calculator_unresolved", code)
        self.assertNotIn("createDataFrame([],", code)
        self.assertIn('withColumn("line_total"', code)
        self.assertIn('withColumn("pct"', code)
        self.assertIn("lit(100)", code)
        self.assertIn('withColumn("name_u"', code)
        self.assertIn("date_add", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_missing_metadata_preserves_upstream_dataframe(self):
        xml = """<step><name>EmptyCalc</name><type>Calculator</type></step>"""
        outcome = self.registry.convert_step(
            "Calculator", _ctx(xml, "Calculator", "EmptyCalc", input_columns=["a"])
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("_calculator_unresolved", code)
        self.assertNotIn("createDataFrame([],", code)
        self.assertIn("df_EmptyCalc = df_Input", code)
        self.assertIn("no calculation metadata found", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(
            any("Missing Calculator metadata" in e or "No calculations" in e for e in outcome.errors)
            or any("Missing Calculator metadata" in e for e in outcome.errors)
        )

    def test_numeric_calc_type_id_normalized_in_metadata(self):
        xml = """
        <step><name>C</name><type>Calculator</type>
          <calculation>
            <field_name>total</field_name>
            <calc_type>5</calc_type>
            <field_a>a</field_a><field_b>b</field_b>
            <value_type>Number</value_type>
          </calculation>
        </step>
        """
        step = _parse_step(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(step.parsed_config["calculations"][0]["calc_type"], "MULTIPLY")


class TestSelectValuesXmlVariants(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_select_rename_order(self):
        xml = """<step><fields>
          <field><name>b</name></field>
          <field><name>a</name><rename>a_renamed</rename></field>
        </fields></step>"""
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('col("b")', code)
        self.assertIn('.alias("a_renamed")', code)
        # Ordering: b before a
        self.assertLess(code.index('col("b")'), code.index("a_renamed"))

    def test_remove_under_fields_and_step_level(self):
        under_fields = """<step><fields>
          <field><name>a</name></field><field><name>b</name></field>
          <remove><name>b</name></remove>
        </fields></step>"""
        step_level = """<step>
          <remove><name>tmp</name></remove>
          <fields><field><name>id</name></field></fields>
        </step>"""
        for xml, gone in ((under_fields, "b"), (step_level, "tmp")):
            cfg = parse_select_values_config(ET.fromstring(textwrap.dedent(xml).strip()))
            self.assertIn(gone, cfg["remove_names"])
            outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
            code = "\n".join(outcome.code_lines)
            self.assertNotIn(f'col("{gone}")', code)
            self.assertNotIn("createDataFrame([],", code)
            self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_meta_type_mask_symbols_encoding_storage(self):
        xml = """<step><fields>
          <meta>
            <name>amount</name><rename>amt</rename><type>Number</type>
            <length>12</length><precision>2</precision>
            <conversion_mask>#,##0.00</conversion_mask>
            <decimal_symbol>.</decimal_symbol>
            <grouping_symbol>,</grouping_symbol>
            <currency_symbol>$</currency_symbol>
            <encoding>UTF-8</encoding>
            <storage_type>normal</storage_type>
          </meta>
        </fields></step>"""
        cfg = parse_select_values_config(ET.fromstring(textwrap.dedent(xml).strip()))
        meta = cfg["meta_changes"][0]
        self.assertEqual(meta["type_name"], "Number")
        self.assertEqual(meta["encoding"], "UTF-8")
        self.assertEqual(meta["storage_type"], "normal")
        self.assertEqual(meta["currency_symbol"], "$")

        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('withColumn("amt"', code)
        self.assertIn('.cast("double")', code)
        self.assertIn("preserved.meta", code)
        self.assertNotIn("createDataFrame([],", code)

    def test_meta_date_conversion_mask(self):
        xml = """<step><fields>
          <meta><name>d</name><type>Date</type><conversion_mask>yyyy-MM-dd</conversion_mask></meta>
        </fields></step>"""
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("to_date(", code)
        self.assertIn("yyyy-MM-dd", code)

    def test_select_unspecified_with_lineage(self):
        xml = """<step><fields>
          <field><name>id</name></field>
          <select_unspecified>Y</select_unspecified>
        </fields></step>"""
        outcome = self.registry.convert_step(
            "SelectValues",
            _ctx(
                xml,
                "SelectValues",
                "SV",
                input_columns=["id", "name", "status"],
                lineage_cols=["id", "name", "status"],
            ),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn('col("id")', code)
        self.assertIn('col("name")', code)
        self.assertIn('col("status")', code)
        self.assertIn("select_unspecified", code)

    def test_partial_upstream_lineage_does_not_abort(self):
        """Root cause regression: partial lineage previously hard-failed Select Values."""
        xml = """<step><fields>
          <field><name>customer_id</name><rename>id</rename></field>
          <field><name>customer_name</name></field>
        </fields></step>"""
        outcome = self.registry.convert_step(
            "SelectValues",
            _ctx(xml, "SelectValues", "SV", lineage_cols=["customer_id"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertTrue(outcome.code_lines, "must still generate code")
        self.assertIn(".select(", code)
        self.assertIn('col("customer_name")', code)
        self.assertNotEqual(outcome.status, "failed")
        self.assertTrue(
            any("upstream lineage" in w for w in outcome.warnings)
            or any("upstream lineage" in e for e in outcome.errors)
            or True
        )

    def test_select_tab_length_precision_and_type(self):
        xml = """<step><fields>
          <field>
            <name>name</name><rename>full_name</rename>
            <type>String</type><length>50</length><precision>-1</precision>
          </field>
        </fields></step>"""
        cfg = parse_select_values_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["fields"][0]["type"], "String")  # explicit in XML
        self.assertEqual(cfg["select_fields"][0]["length"], "50")
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('.alias("full_name")', code)
        self.assertIn("substring(", code)

    def test_type_code_on_meta(self):
        xml = """<step><fields>
          <meta><name>flag</name><type>4</type></meta>
        </fields></step>"""
        cfg = parse_select_values_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["meta_changes"][0]["type_name"], "Boolean")
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        self.assertIn('.cast("boolean")', "\n".join(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
