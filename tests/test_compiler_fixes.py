"""Tests for compiler-grade pipeline fixes: lineage, variables, XML parsing."""

from __future__ import annotations

import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.lineage import substitute_pentaho_variables
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import parse_row_generator_fields, parse_sequence_config
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.transformation_parser import parse_transformation, _parse_step


def _ctx(xml: str, stype: str, name: str, inputs: list[str] | None = None) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(xml).strip())
    step = PentahoStep(name=name, step_type=stype, attributes={}, raw_element=step_el)
    trans = PentahoTransformation(name="T", file_path=Path("t.ktr"), parameters={"BATCH_DATE": "2026-01-01"})
    ins = inputs or ["Input"]
    input_steps = [
        PentahoStep(name=n, step_type="RowGenerator", attributes={}, raw_element=None) for n in ins
    ]
    trans.steps = input_steps + [step]
    hops = [PentahoHop(from_name=n, to_name=name) for n in ins]
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(
        transformation=trans, step=step, dag=dag, df_variable_map=df_map,
        extra={"input_columns": ["id", "name"]},
    )


class TestCompilerFixes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_row_generator_default_values(self):
        xml = """
        <step><limit>1</limit><fields>
          <field><name>id</name><type>Integer</type><default>42</default></field>
          <field><name>country</name><type>String</type><default>India</default></field>
        </fields></step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        fields = parse_row_generator_fields(el)
        self.assertEqual(fields[0].value, "42")
        self.assertEqual(fields[1].value, "India")
        outcome = self.registry.convert_step("RowGenerator", _ctx(xml, "RowGenerator", "RG", []))
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("None, None", code)
        self.assertIn("42", code)
        self.assertIn("'India'", code)

    def test_sequence_valuename_tags(self):
        xml = """
        <step><valuename>order_id</valuename><start>10</start><increment>2</increment></step>
        """
        cfg = parse_sequence_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg.field_name, "order_id")
        self.assertEqual(cfg.start_at, 10)
        self.assertEqual(cfg.increment_by, 2)
        outcome = self.registry.convert_step("Sequence", _ctx(xml, "Sequence", "Seq"))
        self.assertIn("order_id", "\n".join(outcome.code_lines))

    def test_nested_file_tag_parsed(self):
        xml = """
        <step><name>Out</name><type>TextFileOutput</type>
          <file><name>/data/out/sales.csv</name></file>
          <separator>|</separator></step>
        """
        step = _parse_step(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(step.attributes.get("filename"), "/data/out/sales.csv")
        self.assertNotIn("<file>", step.attributes.get("file", "<"))

    def test_table_input_variable_substitution(self):
        xml = "<step><sql>SELECT * FROM t WHERE dt = '${BATCH_DATE}'</sql></step>"
        outcome = self.registry.convert_step("TableInput", _ctx(xml, "TableInput", "TI", []))
        code = "\n".join(outcome.code_lines)
        self.assertIn("2026-01-01", code)
        self.assertNotIn("${BATCH_DATE}", code)

    def test_merge_join_no_keys_no_cartesian(self):
        xml = "<step><join_type>INNER</join_type></step>"
        outcome = self.registry.convert_step(
            "MergeJoin", _ctx(xml, "MergeJoin", "MJ", ["L", "R"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn(".join(df_R, how=", code)
        self.assertIn("no join keys", code.lower())

    def test_merge_join_keys_1_keys_2_xml(self):
        xml = """
        <step><join_type>INNER</join_type>
          <keys_1><key>left_id</key></keys_1>
          <keys_2><key>right_id</key></keys_2>
        </step>
        """
        outcome = self.registry.convert_step(
            "MergeJoin", _ctx(xml, "MergeJoin", "MJ", ["L", "R"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn('df_L["left_id"] == df_R["right_id"]', code)

    def test_compare_value_string_literal(self):
        from pentaho_converter.filter_converter import convert_simple_condition

        expr = convert_simple_condition("status = 'ACTIVE'")
        self.assertEqual(expr, 'col("status") == lit(\'ACTIVE\')')

    def test_substitute_variables_helper(self):
        self.assertEqual(
            substitute_pentaho_variables("x=${A}", {"A": "1"}),
            "x=1",
        )


if __name__ == "__main__":
    unittest.main()
