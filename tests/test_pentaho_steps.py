"""Tests for Pentaho step converters (RowGenerator, Constant, Calculator, FilterRows, StringOperations)."""

from __future__ import annotations

import ast
import textwrap
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.calculator_converter import convert_calculation
from pentaho_converter.filter_converter import convert_filter_condition, convert_simple_condition
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.pipeline import convert_pentaho_project
from pentaho_converter.step_xml import CalculationSpec, parse_constant_fields, parse_data_grid_rows
from pentaho_converter.graph import StepDAG
from pentaho_converter.steps.base import StepContext, build_default_registry


SAMPLES_DIR = Path(__file__).resolve().parent / "samples"


def _parse_step_xml(xml: str) -> ET.Element:
    return ET.fromstring(textwrap.dedent(xml).strip())


def _build_context(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = _parse_step_xml(step_xml)
    step = PentahoStep(
        name=step_name,
        step_type=step_type,
        attributes={},
        raw_element=step_el,
    )
    trans = PentahoTransformation(name="test_trans", file_path=Path("test.ktr"))
    if with_input:
        input_step = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [input_step, step]
        hops = [PentahoHop(from_name="Input", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _extract_generated_function(code: str) -> str:
    """Return the body of run_test_trans for syntax checking."""
    start = code.find("def run_test_trans")
    if start == -1:
        return code
    return code[start:]


def _syntax_ok(code: str) -> bool:
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False


class TestRowGenerator(unittest.TestCase):
    def test_data_grid_multiple_rows(self):
        xml = """
        <step>
          <fields>
            <field><name>id</name><type>Integer</type></field>
            <field><name>name</name><type>String</type></field>
            <field><name>age</name><type>Integer</type></field>
          </fields>
          <data>
            <line><item>1</item><item>John</item><item>20</item></line>
            <line><item>2</item><item>David</item><item>30</item></line>
          </data>
        </step>
        """
        step_el = _parse_step_xml(xml)
        cols, rows = parse_data_grid_rows(step_el)
        self.assertEqual(cols, ["id", "name", "age"])
        self.assertEqual(len(rows), 2)

        ctx = _build_context(xml, "DataGrid", "Generate Rows", with_input=False)
        lines, status = build_default_registry().generate_code("DataGrid", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("'John'", code)
        self.assertIn("'David'", code)
        self.assertIn("spark.createDataFrame", code)

    def test_row_generator_with_limit(self):
        xml = """
        <step>
          <limit>3</limit>
          <fields>
            <field><name>id</name><type>Integer</type><nullif>1</nullif></field>
            <field><name>country</name><type>String</type><nullif>India</nullif></field>
          </fields>
        </step>
        """
        ctx = _build_context(xml, "RowGenerator", "Generate Rows", with_input=False)
        lines, status = build_default_registry().generate_code("RowGenerator", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("* 3", code)
        self.assertIn("'India'", code)


class TestConstant(unittest.TestCase):
    def test_multiple_constants(self):
        xml = """
        <step>
          <fields>
            <field><name>Country</name><type>String</type><value>India</value></field>
            <field><name>Active</name><type>Boolean</type><value>Y</value></field>
            <field><name>Amount</name><type>Integer</type><value>100</value></field>
          </fields>
        </step>
        """
        fields = parse_constant_fields(_parse_step_xml(xml))
        self.assertEqual(len(fields), 3)

        ctx = _build_context(xml, "Constant", "Add constants")
        lines, status = build_default_registry().generate_code("Constant", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn('withColumn("Country", lit(\'India\'))', code)
        self.assertIn('withColumn("Active", lit(True))', code)
        self.assertIn('withColumn("Amount", lit(100))', code)


class TestCalculator(unittest.TestCase):
    def test_arithmetic_and_string_functions(self):
        cases = [
            (CalculationSpec("sum", "ADD", "A", "B"), "(col(\"A\") + col(\"B\"))"),
            (CalculationSpec("diff", "SUBTRACT", "A", "B"), "(col(\"A\") - col(\"B\"))"),
            (CalculationSpec("prod", "MULTIPLY", "A", "B"), "(col(\"A\") * col(\"B\"))"),
            (CalculationSpec("quot", "DIVIDE", "A", "B"), "(col(\"A\") / col(\"B\"))"),
            (CalculationSpec("mod", "REMAINDER", "A", "B"), "(col(\"A\") % col(\"B\"))"),
            (CalculationSpec("abs_val", "ABS", "A"), "abs(col(\"A\"))"),
            (CalculationSpec("rounded", "ROUND_1", "A"), "round(col(\"A\"))"),
            (CalculationSpec("sqrt_val", "SQUARE_ROOT", "A"), "sqrt(col(\"A\"))"),
            (CalculationSpec("upper_name", "UPPER_CASE", "Name"), "upper(col(\"Name\"))"),
            (CalculationSpec("lower_name", "LOWER_CASE", "Name"), "lower(col(\"Name\"))"),
            (CalculationSpec("name_len", "STRING_LEN", "Name"), "length(col(\"Name\"))"),
        ]
        for calc, expected in cases:
            self.assertIn(expected, convert_calculation(calc))

    def test_calculator_step_code_generation(self):
        xml = """
        <step>
          <calculation>
            <field_name>total</field_name>
            <calc_type>ADD</calc_type>
            <field_a>price</field_a>
            <field_b>tax</field_b>
            <value_type>Number</value_type>
          </calculation>
          <calculation>
            <field_name>discount</field_name>
            <calc_type>MULTIPLY</calc_type>
            <field_a>total</field_a>
            <field_b>rate</field_b>
            <value_type>Number</value_type>
          </calculation>
        </step>
        """
        ctx = _build_context(xml, "Calculator", "Calculator")
        lines, status = build_default_registry().generate_code("Calculator", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn('withColumn("total"', code)
        self.assertIn('withColumn("discount"', code)


class TestFilterRows(unittest.TestCase):
    def test_and_condition(self):
        xml = """
        <condition>
          <negated>N</negated>
          <conditions>
            <condition>
              <negated>N</negated>
              <leftvalue>age</leftvalue>
              <function>></function>
              <rightvalue/>
              <value><name>constant</name><type>Integer</type><text>18</text><isnull>N</isnull></value>
            </condition>
            <condition>
              <negated>N</negated>
              <operator>AND</operator>
              <leftvalue>country</leftvalue>
              <function>=</function>
              <rightvalue/>
              <value><name>constant</name><type>String</type><text>India</text><isnull>N</isnull></value>
            </condition>
          </conditions>
        </condition>
        """
        expr = convert_filter_condition(_parse_step_xml(xml))
        self.assertIn('col("age")', expr)
        self.assertIn('col("country")', expr)
        self.assertIn("&", expr)
        self.assertIn("lit(18)", expr)
        self.assertIn("lit('India')", expr)

    def test_in_list_and_null(self):
        in_xml = """
        <condition>
          <negated>N</negated>
          <leftvalue>status</leftvalue>
          <function>IN LIST</function>
          <value><type>String</type><text>ACTIVE;PENDING</text></value>
        </condition>
        """
        expr = convert_filter_condition(_parse_step_xml(in_xml))
        self.assertIn(".isin([", expr)
        self.assertIn("'ACTIVE'", expr)

        null_xml = """
        <condition>
          <negated>N</negated>
          <leftvalue>email</leftvalue>
          <function>IS NOT NULL</function>
        </condition>
        """
        expr_null = convert_filter_condition(_parse_step_xml(null_xml))
        self.assertIn('.isNotNull()', expr_null)

    def test_filter_rows_handler(self):
        xml = """
        <step>
          <compare>
            <condition>
              <negated>N</negated>
              <leftvalue>age</leftvalue>
              <function>></function>
              <value><type>Integer</type><text>18</text></value>
            </condition>
          </compare>
        </step>
        """
        ctx = _build_context(xml, "FilterRows", "Filter rows")
        lines, status = build_default_registry().generate_code("FilterRows", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn(".filter(", code)
        self.assertIn('col("age")', code)

    def test_simple_condition_fallback(self):
        expr = convert_simple_condition("(age > 18 AND country == 'India')")
        self.assertIn("col(", expr)
        self.assertIn("&", expr)
        self.assertIn("(col(", expr)

    def test_filter_rightvalue_numeric_literal(self):
        from pentaho_converter.filter_converter import convert_filter_condition_from_metadata

        expr = convert_filter_condition_from_metadata(
            {"leftvalue": "age", "function": ">", "rightvalue": "100"}
        ).expr
        self.assertIn("lit(100", expr)
        self.assertNotIn('col("100")', expr)


class TestStringOperations(unittest.TestCase):
    def test_upper_trim_substring(self):
        xml = """
        <step>
          <fields>
            <field>
              <in_stream_name>Name</in_stream_name>
              <out_stream_name>Name_clean</out_stream_name>
              <trim_type>both</trim_type>
              <lower_upper>upper</lower_upper>
            </field>
            <field>
              <in_stream_name>Code</in_stream_name>
              <out_stream_name>Code_sub</out_stream_name>
              <cut_from>2</cut_from>
              <cut_to>5</cut_to>
            </field>
          </fields>
        </step>
        """
        ctx = _build_context(xml, "StringOperations", "String ops")
        lines, status = build_default_registry().generate_code("StringOperations", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("trim(", code)
        self.assertIn("upper(", code)
        self.assertIn("substring(", code)

    def test_replace_in_string(self):
        xml = """
        <step>
          <in_stream_name>desc</in_stream_name>
          <out_stream_name>desc_clean</out_stream_name>
          <search>foo</search>
          <replace>bar</replace>
        </step>
        """
        ctx = _build_context(xml, "ReplaceInString", "Replace")
        lines, status = build_default_registry().generate_code("ReplaceInString", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("regexp_replace", code)


class TestEndToEndSyntax(unittest.TestCase):
    def test_sample_zip_generates_valid_python(self):
        if not SAMPLES_DIR.exists():
            self.skipTest("sample KTR files not present")
        buf = BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for p in SAMPLES_DIR.rglob("*"):
                if p.is_file():
                    zf.write(p, p.relative_to(SAMPLES_DIR))
        result = convert_pentaho_project(buf.getvalue(), "step_tests")
        self.assertTrue(result.files)
        for path, content in result.files.items():
            if path.endswith(".py"):
                self.assertTrue(
                    _syntax_ok(content),
                    f"Syntax error in generated file: {path}",
                )


if __name__ == "__main__":
    unittest.main()
