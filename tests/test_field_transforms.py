"""Tests for Select Values, Constants, Set Value*, Concat Fields, Add XML, Replace in String."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_add_xml_config,
    parse_concat_fields_config,
    parse_replace_in_string_config,
    parse_select_values_config,
    parse_set_value_constant_config,
    parse_set_value_field_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    # Seed parsed_config like the real parser would
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="Trans", file_path=Path("t.ktr"))
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Input", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {l}" for l in lines))
        return True
    except SyntaxError:
        return False


class TestSelectValuesComplete(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_select_rename(self):
        xml = """<step><fields>
          <field><name>id</name><rename>customer_id</rename></field>
          <field><name>name</name></field>
        </fields></step>"""
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('.alias("customer_id")', code)
        self.assertIn('col("name")', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")

    def test_meta_and_remove_under_fields(self):
        xml = """<step><fields>
          <field><name>a</name></field>
          <field><name>b</name></field>
          <field><name>c</name></field>
          <remove><name>b</name></remove>
          <meta><name>a</name><rename>a_new</rename><type>Integer</type></meta>
        </fields></step>"""
        cfg = parse_select_values_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["remove_names"], ["b"])
        self.assertEqual(cfg["meta_changes"][0]["type_name"], "Integer")

        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('.alias("a_new")', code)
        self.assertIn('.cast("bigint")', code)
        self.assertNotIn('col("b")', code)
        self.assertIn('col("c")', code)

    def test_meta_only_type_cast(self):
        xml = """<step><fields>
          <meta><name>amount</name><type>Number</type></meta>
        </fields></step>"""
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('withColumn("amount"', code)
        self.assertIn('.cast("double")', code)

    def test_remove_only_emits_drop(self):
        xml = """<step><fields>
          <remove><name>tmp_col</name></remove>
          <remove><name>debug</name></remove>
        </fields></step>"""
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".drop(", code)
        self.assertIn('"tmp_col"', code)
        self.assertIn('"debug"', code)
        self.assertEqual(outcome.status, "converted")


class TestAddConstants(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_structured_metadata(self):
        xml = """<step><fields>
          <field><name>Country</name><type>String</type><value>India</value></field>
          <field><name>Flag</name><type>Boolean</type><value>Y</value></field>
        </fields></step>"""
        meta = parse_step_metadata(ET.fromstring(textwrap.dedent(xml).strip()), "Constant")
        self.assertEqual(len(meta["constants"]), 2)
        outcome = self.registry.convert_step("Constant", _ctx(xml, "Constant", "AddC"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('withColumn("Country", lit(\'India\'))', code)
        self.assertIn('withColumn("Flag", lit(True))', code)
        self.assertEqual(outcome.status, "converted")

    def test_nullif_and_format(self):
        """Official Pentaho exports store the constant in nullif + format."""
        xml = """<step><fields>
          <field>
            <name>Amount</name><type>Number</type><format>#,##0.00</format>
            <currency/><decimal>,</decimal><group>.</group>
            <nullif>1.234,56</nullif><length>9</length><precision>2</precision>
            <set_empty_string>N</set_empty_string>
          </field>
          <field>
            <name>AsOf</name><type>Date</type><format>yyyy-MM-dd</format>
            <nullif>2024-01-15</nullif><set_empty_string>N</set_empty_string>
          </field>
        </fields></step>"""
        outcome = self.registry.convert_step("Constant", _ctx(xml, "Constant", "AddC2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('withColumn("Amount", lit(1234.56))', code)
        self.assertIn('to_date(lit(\'2024-01-15\'), \'yyyy-MM-dd\')', code)
        self.assertIn("preserved.Amount", code)


class TestSetValueConstant(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_set_values(self):
        xml = """
        <step>
          <usevar>N</usevar>
          <fields>
            <field><name>status</name><value>ACTIVE</value><mask/><set_empty_string>N</set_empty_string></field>
            <field><name>note</name><value/><mask/><set_empty_string>Y</set_empty_string></field>
            <field><name>qty</name><value>10</value><mask/><set_empty_string>N</set_empty_string></field>
          </fields>
        </step>
        """
        cfg = parse_set_value_constant_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(len(cfg["fields"]), 3)
        outcome = self.registry.convert_step(
            "SetValueConstant", _ctx(xml, "SetValueConstant", "SVC")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn('withColumn("status", lit(\'ACTIVE\'))', code)
        self.assertIn('withColumn("note", lit(""))', code)
        self.assertIn('withColumn("qty", lit(10))', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")
        self.assertGreaterEqual(outcome.semantic_score, 0.7)

    def test_usevar_substitutes_parameters(self):
        xml = """
        <step>
          <usevar>Y</usevar>
          <fields>
            <field><name>env</name><value>${ENV_NAME}</value><mask/><set_empty_string>N</set_empty_string></field>
          </fields>
        </step>
        """
        ctx = _ctx(xml, "SetValueConstant", "SVC2")
        ctx.transformation.parameters = {"ENV_NAME": "prod"}
        outcome = self.registry.convert_step("SetValueConstant", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("lit('prod')", code)
        self.assertIn("usevar=True", code)


class TestSetValueField(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_copy_field(self):
        xml = """
        <step>
          <fields>
            <field><name>old_status</name><replaceby>new_status</replaceby></field>
            <field><name>backup</name><replaceby>name</replaceby></field>
          </fields>
        </step>
        """
        cfg = parse_set_value_field_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["fields"][0]["replace_by"], "new_status")
        outcome = self.registry.convert_step("SetValueField", _ctx(xml, "SetValueField", "SVF"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('withColumn("old_status", col("new_status"))', code)
        self.assertIn('withColumn("backup", col("name"))', code)
        self.assertEqual(outcome.status, "converted")


class TestConcatFields(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_concat_ws(self):
        xml = """
        <step>
          <separator>;</separator>
          <enclosure/>
          <fields>
            <field><name>first</name><type>String</type></field>
            <field><name>last</name><type>String</type></field>
          </fields>
          <ConcatFields>
            <targetFieldName>full_name</targetFieldName>
            <targetFieldLength>100</targetFieldLength>
            <removeSelectedFields>Y</removeSelectedFields>
          </ConcatFields>
        </step>
        """
        cfg = parse_concat_fields_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["target_field_name"], "full_name")
        self.assertTrue(cfg["remove_selected_fields"])
        outcome = self.registry.convert_step("ConcatFields", _ctx(xml, "ConcatFields", "CF"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("concat_ws(';',", code)
        self.assertIn('withColumn("full_name"', code)
        self.assertIn(".drop(", code)
        self.assertIn("substring(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")

    def test_concat_with_enclosure(self):
        xml = """
        <step>
          <separator>,</separator>
          <enclosure>"</enclosure>
          <fields>
            <field><name>a</name></field>
            <field><name>b</name></field>
          </fields>
          <ConcatFields>
            <targetFieldName>csv_row</targetFieldName>
            <targetFieldLength>0</targetFieldLength>
            <removeSelectedFields>N</removeSelectedFields>
          </ConcatFields>
        </step>
        """
        outcome = self.registry.convert_step("ConcatFields", _ctx(xml, "ConcatFields", "CF2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("concat(", code)
        self.assertIn('lit(\'"\')', code)


class TestAddXml(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_add_xml_elements(self):
        xml = """
        <step>
          <encoding>UTF-8</encoding>
          <valueName>xml_data</valueName>
          <xml_repeat_element>Person</xml_repeat_element>
          <file>
            <omitXMLheader>Y</omitXMLheader>
            <omitNullValues>Y</omitNullValues>
          </file>
          <fields>
            <field>
              <name>id</name><element>id</element><type>Integer</type>
              <attribute>N</attribute><attributeParentName/>
            </field>
            <field>
              <name>name</name><element>name</element><type>String</type>
              <attribute>N</attribute><attributeParentName/>
            </field>
            <field>
              <name>active</name><element>active</element><type>String</type>
              <attribute>Y</attribute><attributeParentName/>
            </field>
          </fields>
        </step>
        """
        cfg = parse_add_xml_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["value_name"], "xml_data")
        self.assertEqual(cfg["root_node"], "Person")
        self.assertEqual(len(cfg["fields"]), 3)
        outcome = self.registry.convert_step("AddXML", _ctx(xml, "AddXML", "AX"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('withColumn("xml_data"', code)
        self.assertIn("<Person", code)
        self.assertIn("<id>", code)
        self.assertIn("active=", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")
        self.assertGreaterEqual(outcome.semantic_score, 0.7)


class TestReplaceInString(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_field_level_replace(self):
        xml = """
        <step>
          <fields>
            <field>
              <in_stream_name>desc</in_stream_name>
              <out_stream_name>desc_clean</out_stream_name>
              <use_regex>no</use_regex>
              <replace_string>foo</replace_string>
              <replace_by_string>bar</replace_by_string>
              <set_empty_string>N</set_empty_string>
              <replace_field_by_string/>
              <whole_word>yes</whole_word>
              <case_sensitive>no</case_sensitive>
              <is_unicode>no</is_unicode>
            </field>
          </fields>
        </step>
        """
        cfg = parse_replace_in_string_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertTrue(cfg["operations"][0]["whole_word"])
        self.assertFalse(cfg["operations"][0]["case_sensitive"])
        outcome = self.registry.convert_step(
            "ReplaceInString", _ctx(xml, "ReplaceInString", "RIS")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("regexp_replace", code)
        self.assertIn("desc_clean", code)
        self.assertIn("(?i)", code)
        self.assertIn(r"\b", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")

    def test_legacy_step_level(self):
        xml = """
        <step>
          <in_stream_name>desc</in_stream_name>
          <out_stream_name>desc_clean</out_stream_name>
          <search>foo</search>
          <replace>bar</replace>
        </step>
        """
        outcome = self.registry.convert_step(
            "ReplaceInString", _ctx(xml, "ReplaceInString", "RIS2")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("regexp_replace", code)
        self.assertIn("foo", code)

    def test_replace_field_by_string(self):
        xml = """
        <step>
          <fields>
            <field>
              <in_stream_name>msg</in_stream_name>
              <out_stream_name>msg</out_stream_name>
              <use_regex>no</use_regex>
              <replace_string>TOKEN</replace_string>
              <replace_by_string/>
              <set_empty_string>N</set_empty_string>
              <replace_field_by_string>token_value</replace_field_by_string>
              <whole_word>no</whole_word>
              <case_sensitive>yes</case_sensitive>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "ReplaceInString", _ctx(xml, "ReplaceInString", "RIS3")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("token_value", code)
        self.assertIn("replace(", code.lower())
        self.assertTrue(_syntax_ok(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
