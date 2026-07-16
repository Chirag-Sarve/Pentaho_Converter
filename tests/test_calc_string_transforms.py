"""Tests for Calculator, Checksum, Number Range, String Ops/Cut, Value Mapper, Sequences."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.calculator_converter import convert_calculation_result
from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    CalculationSpec,
    parse_checksum_config,
    parse_fields_change_sequence_config,
    parse_number_range_config,
    parse_sequence_config,
    parse_string_cut_config,
    parse_string_operations_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    input_columns: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
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
    return ctx


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {l}" for l in lines))
        return True
    except SyntaxError:
        return False


class TestParsersCalcGroup(unittest.TestCase):
    def test_checksum_parse(self):
        xml = """<step>
          <checksumtype>SHA-256</checksumtype>
          <resultfieldName>hash</resultfieldName>
          <resultType>hex</resultType>
          <fields><field><name>a</name></field><field><name>b</name></field></fields>
        </step>"""
        cfg = parse_checksum_config(ET.fromstring(xml))
        self.assertEqual(cfg["checksum_type"], "SHA-256")
        self.assertEqual(cfg["fields"], ["a", "b"])

    def test_number_range_parse(self):
        xml = """<step>
          <inputField>score</inputField>
          <outputField>band</outputField>
          <fallBackValue>other</fallBackValue>
          <rules>
            <rule><lower_bound>0</lower_bound><upper_bound>50</upper_bound><value>low</value></rule>
            <rule><lower_bound>50</lower_bound><upper_bound>100</upper_bound><value>high</value></rule>
          </rules>
        </step>"""
        cfg = parse_number_range_config(ET.fromstring(xml))
        self.assertEqual(cfg["input_field"], "score")
        self.assertEqual(len(cfg["rules"]), 2)
        self.assertTrue(cfg["lower_inclusive"])
        self.assertFalse(cfg["upper_inclusive"])

    def test_string_cut_and_ops_parse(self):
        cut = parse_string_cut_config(ET.fromstring(
            """<step><fields>
              <field><in_stream_name>s</in_stream_name><out_stream_name>t</out_stream_name>
              <cut_from>1</cut_from><cut_to>4</cut_to></field>
            </fields></step>"""
        ))
        self.assertEqual(cut["fields"][0]["cut_from"], "1")
        ops = parse_string_operations_config(ET.fromstring(
            """<step><fields>
              <field><in_stream_name>s</in_stream_name><trim_type>both</trim_type>
              <padding_type>left</padding_type><pad_char>0</pad_char><pad_len>5</pad_len>
              <digits>only</digits></field>
            </fields></step>"""
        ))
        self.assertEqual(ops["fields"][0]["padding_type"], "left")
        self.assertEqual(ops["fields"][0]["digits_type"], "only")

    def test_sequence_wrap_and_change(self):
        seq = parse_sequence_config(ET.fromstring(
            """<step><valuename>id</valuename><start>1</start><increment>1</increment>
            <maxvalue>3</maxvalue><use_database>N</use_database></step>"""
        ))
        self.assertEqual(seq.max_value, 3)
        chg = parse_fields_change_sequence_config(ET.fromstring(
            """<step><resultfieldName>seq</resultfieldName><start>10</start><increment>5</increment>
            <fields><field><name>grp</name></field></fields></step>"""
        ))
        self.assertEqual(chg["start_at"], 10)
        self.assertEqual(chg["group_fields"], ["grp"])


class TestHandlersCalcGroup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_calculator_crc32(self):
        result = convert_calculation_result(
            CalculationSpec(field_name="c", calc_type="CRC32", field_a="s", value_type="Integer")
        )
        self.assertTrue(result.supported)
        self.assertIn("crc32", result.expr)

    def test_checksum_sha256(self):
        xml = """<step>
          <checksumtype>SHA-256</checksumtype>
          <resultfieldName>hash</resultfieldName>
          <fields><field><name>a</name></field></fields>
        </step>"""
        outcome = self.registry.convert_step(
            "CheckSum", _ctx(xml, "CheckSum", "CS", input_columns=["a"])
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("sha2(", code)
        self.assertNotIn("concat_ws", code)
        self.assertIn('coalesce(col("a")', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_calculator_mask_xml_escapes(self):
        result = convert_calculation_result(
            CalculationSpec(field_name="x", calc_type="MASK_XML", field_a="s", value_type="String")
        )
        self.assertTrue(result.supported)
        self.assertIn("&amp;", result.expr)
        self.assertNotIn("[&<>", result.expr)

    def test_number_range_open_bounds(self):
        xml = """<step>
          <inputField>score</inputField>
          <outputField>band</outputField>
          <fallBackValue>other</fallBackValue>
          <rules>
            <rule><lower_bound></lower_bound><upper_bound></upper_bound><value>all</value></rule>
          </rules>
        </step>"""
        outcome = self.registry.convert_step(
            "NumberRange", _ctx(xml, "NumberRange", "NR2", input_columns=["score"])
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("isNotNull()", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_fields_change_sequence_all_columns(self):
        xml = """<step>
          <resultfieldName>seq</resultfieldName>
          <start>1</start><increment>1</increment>
          <fields></fields>
        </step>"""
        outcome = self.registry.convert_step(
            "FieldsChangeSequence",
            _ctx(xml, "FieldsChangeSequence", "FCS2", input_columns=["a", "b"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn('col("a")', code)
        self.assertIn('col("b")', code)
        self.assertIn("all upstream columns", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_number_range(self):
        xml = """<step>
          <inputField>score</inputField>
          <outputField>band</outputField>
          <fallBackValue>other</fallBackValue>
          <rules>
            <rule><lower_bound>0</lower_bound><upper_bound>50</upper_bound><value>low</value></rule>
          </rules>
        </step>"""
        outcome = self.registry.convert_step(
            "NumberRange", _ctx(xml, "NumberRange", "NR", input_columns=["score"])
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn(">= lit(0.0)", code)
        self.assertIn("< lit(50.0)", code)
        self.assertIn("other", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_string_operations_padding(self):
        xml = """<step><fields>
          <field><in_stream_name>s</in_stream_name><out_stream_name>s2</out_stream_name>
          <trim_type>both</trim_type><padding_type>left</padding_type>
          <pad_char>0</pad_char><pad_len>5</pad_len><digits>only</digits>
          </field>
        </fields></step>"""
        outcome = self.registry.convert_step(
            "StringOperations",
            _ctx(xml, "StringOperations", "SO", input_columns=["s"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("lpad(", code)
        self.assertIn("regexp_replace", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_string_cut_zero_based(self):
        xml = """<step><fields>
          <field><in_stream_name>s</in_stream_name><out_stream_name>t</out_stream_name>
          <cut_from>1</cut_from><cut_to>4</cut_to></field>
        </fields></step>"""
        outcome = self.registry.convert_step(
            "StringCut", _ctx(xml, "StringCut", "SC", input_columns=["s"])
        )
        code = "\n".join(outcome.code_lines)
        # Java substring(1,4) → Spark substring(s, 2, 3)
        self.assertIn("substring(", code)
        self.assertIn(", 2, 3)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_value_mapper_non_empty(self):
        xml = """<step>
          <field_to_use>status</field_to_use>
          <target_field>label</target_field>
          <non_match_default>X</non_match_default>
          <non_empty>Y</non_empty>
          <fields><field><source_value>A</source_value><target_value>Active</target_value></field></fields>
        </step>"""
        outcome = self.registry.convert_step(
            "ValueMapper",
            _ctx(xml, "ValueMapper", "VM", input_columns=["status"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("when(", code)
        self.assertIn("non_empty=Y", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_sequence_wrap(self):
        xml = """<step>
          <valuename>id</valuename><start>1</start><increment>1</increment><maxvalue>3</maxvalue>
        </step>"""
        outcome = self.registry.convert_step(
            "Sequence", _ctx(xml, "Sequence", "Seq", input_columns=["x"])
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("%", code)
        self.assertIn("max_value=3", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_fields_change_sequence(self):
        xml = """<step>
          <resultfieldName>seq</resultfieldName>
          <start>1</start><increment>1</increment>
          <fields><field><name>customer</name></field></fields>
        </step>"""
        outcome = self.registry.convert_step(
            "FieldsChangeSequence",
            _ctx(xml, "FieldsChangeSequence", "FCS", input_columns=["customer", "val"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("lag(", code)
        self.assertIn('_sum(', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
