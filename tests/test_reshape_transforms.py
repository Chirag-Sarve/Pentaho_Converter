"""Tests for reshape / sort / unique Transform steps."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_denormaliser_config,
    parse_flattener_config,
    parse_normaliser_config,
    parse_sort_rows_config,
    parse_split_field_to_rows_config,
    parse_split_fields_config,
    parse_step_metadata,
    parse_unique_rows_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    input_columns: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
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


class TestParsers(unittest.TestCase):
    def test_normaliser_parse(self):
        xml = """<step>
          <typefield>period</typefield>
          <fields>
            <field><name>jan</name><value>January</value><norm>sales</norm></field>
            <field><name>feb</name><value>February</value><norm>sales</norm></field>
          </fields>
        </step>"""
        cfg = parse_normaliser_config(ET.fromstring(xml))
        self.assertEqual(cfg["type_field"], "period")
        self.assertEqual(len(cfg["fields"]), 2)
        self.assertEqual(cfg["fields"][0]["norm"], "sales")

    def test_denormaliser_parse(self):
        xml = """<step>
          <key_field>month</key_field>
          <group><field><name>product</name></field></group>
          <fields>
            <field>
              <field_name>amount</field_name>
              <key_value>Jan</key_value>
              <target_name>jan_amt</target_name>
              <target_type>Number</target_type>
              <target_aggregation_type>SUM</target_aggregation_type>
            </field>
          </fields>
        </step>"""
        cfg = parse_denormaliser_config(ET.fromstring(xml))
        self.assertEqual(cfg["key_field"], "month")
        self.assertEqual(cfg["group_fields"], ["product"])
        self.assertEqual(cfg["target_fields"][0]["target_name"], "jan_amt")

    def test_flattener_split_sort_unique_parse(self):
        flat = parse_flattener_config(ET.fromstring(
            "<step><field_name>val</field_name>"
            "<fields><field><name>v1</name></field><field><name>v2</name></field></fields></step>"
        ))
        self.assertEqual(flat["field_name"], "val")
        self.assertEqual(flat["target_fields"], ["v1", "v2"])

        s2r = parse_split_field_to_rows_config(ET.fromstring(
            "<step><splitfield>tags</splitfield><delimiter>,</delimiter>"
            "<newfield>tag</newfield><rownum>Y</rownum><rownum_field>nr</rownum_field></step>"
        ))
        self.assertEqual(s2r["split_field"], "tags")
        self.assertTrue(s2r["include_row_number"])

        sf = parse_split_fields_config(ET.fromstring(
            "<step><splitfield>csv</splitfield><delimiter>;</delimiter>"
            "<fields><field><name>a</name><type>Integer</type></field></fields></step>"
        ))
        self.assertEqual(sf["fields"][0]["name"], "a")

        sort_cfg = parse_sort_rows_config(ET.fromstring(
            "<step><unique_rows>Y</unique_rows><fields>"
            "<field><name>id</name><ascending>N</ascending><case_sensitive>N</case_sensitive></field>"
            "</fields></step>"
        ))
        self.assertTrue(sort_cfg["unique_rows"])
        self.assertFalse(sort_cfg["sort_fields"][0]["ascending"])
        self.assertFalse(sort_cfg["sort_fields"][0]["case_sensitive"])

        uniq = parse_unique_rows_config(ET.fromstring(
            "<step><count_rows>Y</count_rows><count_field>cnt</count_field>"
            "<fields><field><name>id</name><case_insensitive>Y</case_insensitive></field></fields></step>"
        ))
        self.assertTrue(uniq["count_rows"])
        self.assertTrue(uniq["compare_fields"][0]["case_insensitive"])


class TestReshapeHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_row_normaliser_union(self):
        xml = """<step>
          <typefield>period</typefield>
          <fields>
            <field><name>jan</name><value>January</value><norm>sales</norm></field>
            <field><name>feb</name><value>February</value><norm>sales</norm></field>
          </fields>
        </step>"""
        ctx = _ctx(xml, "RowNormaliser", "Norm", input_columns=["id", "jan", "feb"])
        outcome = self.registry.convert_step("RowNormaliser", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("unionByName", code)
        self.assertIn("January", code)
        self.assertIn('alias("sales")', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_row_denormaliser_groupby(self):
        xml = """<step>
          <key_field>month</key_field>
          <group><field><name>product</name></field></group>
          <fields>
            <field>
              <field_name>amount</field_name>
              <key_value>Jan</key_value>
              <target_name>jan_amt</target_name>
              <target_aggregation_type>SUM</target_aggregation_type>
            </field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "RowDenormaliser",
            _ctx(xml, "RowDenormaliser", "Denorm", input_columns=["product", "month", "amount"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("groupBy", code)
        self.assertIn("sum(", code)
        self.assertIn("jan_amt", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_row_flattener(self):
        xml = """<step>
          <field_name>val</field_name>
          <fields>
            <field><name>v1</name></field>
            <field><name>v2</name></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "Flattener",
            _ctx(xml, "Flattener", "Flat", input_columns=["id", "val"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("groupBy('_flat_grp')", code)
        self.assertIn('alias("v1")', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_split_field_to_rows(self):
        xml = """<step>
          <splitfield>tags</splitfield>
          <delimiter>,</delimiter>
          <newfield>tag</newfield>
          <rownum>Y</rownum>
          <rownum_field>nr</rownum_field>
        </step>"""
        outcome = self.registry.convert_step(
            "SplitFieldToRows",
            _ctx(xml, "SplitFieldToRows", "S2R", input_columns=["id", "tags"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("explode_outer", code)
        self.assertIn("split(", code)
        self.assertIn("nr", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_split_fields(self):
        xml = """<step>
          <splitfield>csv</splitfield>
          <delimiter>;</delimiter>
          <fields>
            <field><name>a</name><type>Integer</type><nullif>-</nullif></field>
            <field><name>b</name><type>String</type><trimtype>both</trimtype></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "FieldSplitter",
            _ctx(xml, "FieldSplitter", "SF", input_columns=["csv"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("element_at(", code)
        self.assertIn('"csv"', code)
        self.assertIn(".drop(", code)
        self.assertIn('.cast("bigint")', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_sort_rows_case_and_unique(self):
        xml = """<step>
          <unique_rows>Y</unique_rows>
          <fields>
            <field><name>name</name><ascending>Y</ascending><case_sensitive>N</case_sensitive></field>
            <field><name>id</name><ascending>N</ascending></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "SortRows",
            _ctx(xml, "SortRows", "Sort", input_columns=["id", "name"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("orderBy(", code)
        self.assertIn("lower(", code)
        self.assertIn('dropDuplicates(["name", "id"])', code)
        self.assertIn("asc_nulls_last", code)
        self.assertIn("preserved.directory=", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_split_fields_id_mode(self):
        xml = """<step>
          <splitfield>payload</splitfield>
          <delimiter>,</delimiter>
          <fields>
            <field><name>a</name><id>Sales1=</id><idrem>Y</idrem><type>Number</type></field>
            <field><name>b</name><id>Sales2=</id><idrem>Y</idrem><type>Number</type></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "FieldSplitter",
            _ctx(xml, "FieldSplitter", "SFID", input_columns=["payload"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("expr(", code)
        self.assertIn("startswith", code)
        self.assertIn("substring", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_split_fields_enclosure_from_csv(self):
        xml = """<step>
          <splitfield>csv</splitfield>
          <delimiter>,</delimiter>
          <enclosure>"</enclosure>
          <fields>
            <field><name>a</name><type>String</type></field>
            <field><name>b</name><type>String</type></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "FieldSplitter",
            _ctx(xml, "FieldSplitter", "SFEnc", input_columns=["csv"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("from_csv", code)
        self.assertIn('"quote": \'"\'', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_denormaliser_preserves_target_meta(self):
        xml = """<step>
          <key_field>month</key_field>
          <group><field><name>product</name></field></group>
          <fields>
            <field>
              <field_name>amount</field_name>
              <key_value>Jan</key_value>
              <target_name>jan_amt</target_name>
              <target_type>Number</target_type>
              <target_length>10</target_length>
              <target_precision>2</target_precision>
              <target_null_string>-</target_null_string>
              <target_aggregation_type>SUM</target_aggregation_type>
            </field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "RowDenormaliser",
            _ctx(xml, "RowDenormaliser", "D2", input_columns=["product", "month", "amount"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("preserved.target 'jan_amt'", code)
        self.assertIn("length='10'", code)
        self.assertIn("lit('-')", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_unique_rows_with_count(self):
        xml = """<step>
          <count_rows>Y</count_rows>
          <count_field>dup_cnt</count_field>
          <fields><field><name>id</name></field></fields>
        </step>"""
        outcome = self.registry.convert_step(
            "Unique",
            _ctx(xml, "Unique", "Uniq", input_columns=["id", "val"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("dup_cnt", code)
        self.assertIn("row_number()", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_unique_hashset_case_insensitive(self):
        xml = """<step>
          <store_values>Y</store_values>
          <fields>
            <field><name>code</name><case_insensitive>Y</case_insensitive></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "UniqueRowsByHashSet",
            _ctx(xml, "UniqueRowsByHashSet", "HashUniq", input_columns=["code"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("dropDuplicates", code)
        self.assertIn("lower(", code)
        self.assertIn("HashSet", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
