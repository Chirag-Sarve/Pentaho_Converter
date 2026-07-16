"""Tests for Pentaho Join step migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_join_rows_config,
    parse_merge_rows_config,
    parse_multiway_merge_join_config,
    parse_sorted_merge_config,
    parse_step_metadata,
    parse_xml_join_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.join_handlers import JOIN_HANDLERS
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    input_names: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    hops: list[PentahoHop] = []
    steps: list[PentahoStep] = []
    for name in input_names or ["Left", "Right"]:
        steps.append(PentahoStep(name=name, step_type="RowGenerator", attributes={}, raw_element=None))
        hops.append(PentahoHop(from_name=name, to_name=step_name))
    steps.append(step)
    trans = PentahoTransformation(name="JoinTrans", file_path=Path("join.ktr"), steps=steps)
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestJoinParsers(unittest.TestCase):
    def test_join_rows_parse(self):
        cfg = parse_join_rows_config(ET.fromstring("""
        <step>
          <directory>%%java.io.tmpdir%%</directory>
          <prefix>jr</prefix>
          <cache_size>250</cache_size>
          <main>MainStream</main>
          <compare>
            <condition>
              <conditions>
                <condition>
                  <leftvalue>a</leftvalue>
                  <function>=</function>
                  <rightvalue>b</rightvalue>
                </condition>
              </conditions>
            </condition>
          </compare>
        </step>
        """))
        self.assertEqual(cfg["directory"], "%%java.io.tmpdir%%")
        self.assertEqual(cfg["prefix"], "jr")
        self.assertEqual(cfg["cache_size"], 250)
        self.assertEqual(cfg["main_step"], "MainStream")
        self.assertIsNotNone(cfg["condition"])

    def test_merge_rows_parse(self):
        cfg = parse_merge_rows_config(ET.fromstring("""
        <step>
          <keys><key>id</key></keys>
          <values><value>name</value><value>amount</value></values>
          <flag_field>diff_flag</flag_field>
          <reference>Ref</reference>
          <compare>Cmp</compare>
        </step>
        """))
        self.assertEqual(cfg["flag_field"], "diff_flag")
        self.assertEqual(cfg["key_fields"], ["id"])
        self.assertEqual(cfg["value_fields"], ["name", "amount"])
        self.assertEqual(cfg["reference"], "Ref")
        self.assertEqual(cfg["compare"], "Cmp")
        self.assertEqual(cfg["keys"][0]["left"], "id")

    def test_multiway_parse(self):
        cfg = parse_multiway_merge_join_config(ET.fromstring("""
        <step>
          <join_type>FULL OUTER</join_type>
          <number_input>3</number_input>
          <step0>A</step0><step1>B</step1><step2>C</step2>
          <keys><key>id</key><key>id</key><key>id</key></keys>
        </step>
        """))
        self.assertEqual(cfg["join_type"], "FULL OUTER")
        self.assertEqual(cfg["input_steps"], ["A", "B", "C"])
        self.assertEqual(cfg["key_fields"], ["id", "id", "id"])

    def test_sorted_merge_parse(self):
        cfg = parse_sorted_merge_config(ET.fromstring("""
        <step>
          <fields>
            <field><name>ts</name><ascending>Y</ascending></field>
            <field><name>id</name><ascending>N</ascending></field>
          </fields>
        </step>
        """))
        self.assertEqual(cfg["sort_fields"][0]["name"], "ts")
        self.assertTrue(cfg["sort_fields"][0]["ascending"])
        self.assertFalse(cfg["sort_fields"][1]["ascending"])

    def test_xml_join_parse(self):
        cfg = parse_xml_join_config(ET.fromstring("""
        <step>
          <valueXMLfield>result</valueXMLfield>
          <targetXMLstep>Target</targetXMLstep>
          <targetXMLfield>doc</targetXMLfield>
          <sourceXMLstep>Source</sourceXMLstep>
          <sourceXMLfield>frag</sourceXMLfield>
          <targetXPath>/root</targetXPath>
          <complexJoin>Y</complexJoin>
          <joinCompareField>grp</joinCompareField>
          <omitXMLHeader>Y</omitXMLHeader>
          <omitNullValues>N</omitNullValues>
        </step>
        """))
        self.assertEqual(cfg["value_xml_field"], "result")
        self.assertEqual(cfg["target_xpath"], "/root")
        self.assertTrue(cfg["complex_join"])
        self.assertEqual(cfg["join_compare_field"], "grp")

    def test_parse_step_metadata_aliases(self):
        el = ET.fromstring(
            "<step><directory>/tmp</directory><cache_size>10</cache_size><main>M</main></step>"
        )
        self.assertEqual(parse_step_metadata(el, "JoinRows")["main_step"], "M")
        self.assertIn("key_fields", parse_step_metadata(
            ET.fromstring("<step><keys><key>id</key></keys><flag_field>f</flag_field></step>"),
            "MergeRows",
        ))


class TestJoinConverters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_join_rows_cross_join(self):
        xml = """
        <step>
          <directory>%%java.io.tmpdir%%</directory>
          <prefix>out</prefix>
          <cache_size>500</cache_size>
          <main>A</main>
        </step>
        """
        outcome = self.registry.convert_step(
            "JoinRows", _ctx(xml, "JoinRows", "JR", ["A", "B"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("crossJoin", code)
        self.assertIn("Cartesian", code)
        self.assertIn("preserved.cache_size", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_merge_join_still_works(self):
        xml = """
        <step>
          <join_type>LEFT OUTER</join_type>
          <step1>L</step1><step2>R</step2>
          <keys_1><key>id</key></keys_1>
          <keys_2><key>id</key></keys_2>
        </step>
        """
        outcome = self.registry.convert_step(
            "MergeJoin", _ctx(xml, "MergeJoin", "MJ", ["L", "R"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".join(", code)
        self.assertIn("left", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_merge_rows_changed_flag(self):
        xml = """
        <step>
          <keys><key>id</key></keys>
          <values><value>name</value></values>
          <flag_field>diff_flag</flag_field>
          <reference>Ref</reference>
          <compare>Cmp</compare>
        </step>
        """
        outcome = self.registry.convert_step(
            "MergeRows", _ctx(xml, "MergeRows", "MR", ["Ref", "Cmp"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("full_outer", code)
        self.assertIn("deleted", code)
        self.assertIn("new", code)
        self.assertIn("changed", code)
        self.assertIn("identical", code)
        self.assertIn("diff_flag", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_multiway_chained_joins(self):
        xml = """
        <step>
          <join_type>INNER</join_type>
          <number_input>3</number_input>
          <step0>A</step0><step1>B</step1><step2>C</step2>
          <keys><key>id</key><key>id</key><key>id</key></keys>
        </step>
        """
        outcome = self.registry.convert_step(
            "MultiwayMergeJoin",
            _ctx(xml, "MultiwayMergeJoin", "MW", ["A", "B", "C"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertGreaterEqual(code.count(".join("), 2)
        self.assertIn("df_A", code)
        self.assertIn("df_C", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_sorted_merge_order(self):
        xml = """
        <step>
          <fields>
            <field><name>ts</name><ascending>Y</ascending></field>
            <field><name>id</name><ascending>N</ascending></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "SortedMerge", _ctx(xml, "SortedMerge", "SM", ["S1", "S2"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("unionByName", code)
        self.assertIn("orderBy", code)
        self.assertIn("col('ts').asc()", code)
        self.assertIn("col('id').desc()", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_xml_join_aggregation(self):
        xml = """
        <step>
          <valueXMLfield>result_xml</valueXMLfield>
          <targetXMLstep>Target</targetXMLstep>
          <targetXMLfield>doc</targetXMLfield>
          <sourceXMLstep>Source</sourceXMLstep>
          <sourceXMLfield>frag</sourceXMLfield>
          <targetXPath>/orders</targetXPath>
        </step>
        """
        outcome = self.registry.convert_step(
            "XMLJoin", _ctx(xml, "XMLJoin", "XJ", ["Target", "Source"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("collect_list", code)
        self.assertIn("result_xml", code)
        self.assertIn("preserved.target_xpath", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_xml_join_complex(self):
        xml = """
        <step>
          <valueXMLfield>out</valueXMLfield>
          <targetXMLstep>Target</targetXMLstep>
          <targetXMLfield>doc</targetXMLfield>
          <sourceXMLstep>Source</sourceXMLstep>
          <sourceXMLfield>frag</sourceXMLfield>
          <complexJoin>Y</complexJoin>
          <joinCompareField>grp</joinCompareField>
          <targetXPath>/root/item[?]</targetXPath>
        </step>
        """
        outcome = self.registry.convert_step(
            "XMLJoin", _ctx(xml, "XMLJoin", "XJC", ["Target", "Source"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("groupBy", code)
        self.assertIn("grp", code)
        self.assertEqual(outcome.status, "partial")

    def test_handler_type_coverage(self):
        types: set[str] = set()
        for handler in JOIN_HANDLERS:
            types |= set(getattr(handler, "_TYPES", set()))
        for required in (
            "joinrows", "mergerows", "multimergejoin", "sortedmerge", "xmljoin",
        ):
            self.assertIn(required, types)

    def test_validators_registered(self):
        for stype in (
            "joinrows", "mergerows", "multimergejoin", "sortedmerge", "xmljoin",
        ):
            self.assertIsNotNone(get_validator(stype), msg=stype)

    def test_merge_join_preserves_sort_requirement(self):
        xml = """
        <step>
          <join_type>INNER</join_type>
          <keys_1><key>id</key></keys_1>
          <keys_2><key>id</key></keys_2>
        </step>
        """
        outcome = self.registry.convert_step(
            "MergeJoin", _ctx(xml, "MergeJoin", "MJ", ["L", "R"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("pre-sorted", code)
        self.assertIn("null join keys", code)
        self.assertIn("preserved.join_type", code)

    def test_join_rows_single_stream_edge(self):
        outcome = self.registry.convert_step(
            "JoinRows",
            _ctx(
                "<step><cache_size>10</cache_size><main>Only</main></step>",
                "JoinRows",
                "JR1",
                ["Only"],
            ),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("only one input stream", code)
        self.assertEqual(outcome.status, "partial")

    def test_merge_rows_null_safe_changed(self):
        xml = """
        <step>
          <keys><key>id</key></keys>
          <values><value>amount</value></values>
          <flag_field>flag</flag_field>
        </step>
        """
        outcome = self.registry.convert_step(
            "MergeRows", _ctx(xml, "MergeRows", "MR", ["Ref", "Cmp"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("eqNullSafe", code)
        self.assertIn("null join keys", code)

    def test_sorted_merge_missing_order(self):
        outcome = self.registry.convert_step(
            "SortedMerge",
            _ctx("<step><fields/></step>", "SortedMerge", "SM0", ["A", "B"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("no ordering fields", code)
        self.assertEqual(outcome.status, "partial")

    def test_xml_join_missing_fields(self):
        outcome = self.registry.convert_step(
            "XMLJoin",
            _ctx("<step><targetXMLstep>T</targetXMLstep></step>", "XMLJoin", "X0", ["T", "S"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("missing target/source XML field", code)
        self.assertEqual(outcome.status, "partial")


if __name__ == "__main__":
    unittest.main()
