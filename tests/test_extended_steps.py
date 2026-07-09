"""Regression tests for extended Pentaho step converters (joins, formula, CSV output, DB ops)."""

from __future__ import annotations

import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx_multi(
    step_xml: str,
    step_type: str,
    step_name: str,
    input_names: list[str],
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    inputs = [
        PentahoStep(name=n, step_type="RowGenerator", attributes={}, raw_element=None)
        for n in input_names
    ]
    trans = PentahoTransformation(name="JoinTrans", file_path=Path("join.ktr"))
    trans.steps = inputs + [step]
    hops = [PentahoHop(from_name=n, to_name=step_name) for n in input_names]
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
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


class TestExtendedSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_merge_join(self):
        xml = """
        <step>
          <join_type>INNER</join_type>
          <key><name>id</name><field>id</field></key>
        </step>
        """
        outcome = self.registry.convert_step(
            "MergeJoin",
            _ctx_multi(xml, "MergeJoin", "Merge join", ["Left", "Right"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".join(", code)
        self.assertIn(outcome.status, ("converted", "partial"))
        self.assertGreaterEqual(outcome.semantic_score, 0.9)

    def test_join_rows(self):
        xml = """
        <step>
          <join_type>LEFT OUTER</join_type>
          <key><name>customer_id</name><field>customer_id</field></key>
        </step>
        """
        outcome = self.registry.convert_step(
            "JoinRows",
            _ctx_multi(xml, "JoinRows", "Join rows", ["A", "B"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".join(", code)
        self.assertIn("left", code.lower())

    def test_merge_rows(self):
        xml = """
        <step>
          <flag_field>diff_flag</flag_field>
          <key><name>id</name><field>id</field></key>
        </step>
        """
        outcome = self.registry.convert_step(
            "MergeRows",
            _ctx_multi(xml, "MergeRows", "Merge rows", ["Ref", "Compare"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertTrue(".join(" in code or "unionByName" in code)

    def test_formula_withColumn(self):
        xml = """
        <step>
          <field_name>total</field_name>
          <formula>[price] * [qty]</formula>
        </step>
        """
        outcome = self.registry.convert_step("Formula", _ctx(xml, "Formula", "Calc formula"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("withColumn", code)
        self.assertIn("total", code)
        self.assertGreaterEqual(outcome.semantic_score, 0.9)

    def test_csv_output(self):
        xml = """
        <step>
          <filename>/data/out/customers.csv</filename>
          <separator>;</separator>
          <header>Y</header>
        </step>
        """
        outcome = self.registry.convert_step(
            "CsvOutput", _ctx(xml, "CsvOutput", "Write CSV"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('csv')", code)
        self.assertIn("customers.csv", code)
        self.assertIn(";", code)

    def test_insert_update_merge(self):
        xml = """
        <step>
          <schema>analytics</schema>
          <table>dim_customer</table>
          <fields>
            <field><name>customer_id</name></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "InsertUpdate", _ctx(xml, "InsertUpdate", "Upsert"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("MERGE INTO", code)

    def test_execute_sql(self):
        xml = "<step><sql>SELECT 1 AS x</sql></step>"
        outcome = self.registry.convert_step(
            "ExecSQL", _ctx(xml, "ExecSQL", "Run SQL", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("spark.sql(", code)
        self.assertIn("SELECT 1", code)

    def test_parquet_output(self):
        xml = "<step><filename>/data/out/part</filename><compression>snappy</compression></step>"
        outcome = self.registry.convert_step(
            "ParquetOutput", _ctx(xml, "ParquetOutput", "Parquet out"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('parquet')", code)

    def test_stream_lookup(self):
        xml = """
        <step>
          <key><name>id</name><field>id</field></key>
        </step>
        """
        outcome = self.registry.convert_step(
            "StreamLookup",
            _ctx_multi(xml, "StreamLookup", "Lookup", ["Main", "Lookup"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("broadcast(", code)
        self.assertIn(".join(", code)

    def test_replace_null(self):
        xml = """
        <step>
          <replace_value>lit(0)</replace_value>
          <fields><field><name>amount</name></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "ReplaceNull", _ctx(xml, "ReplaceNull", "Fill nulls"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("isNull()", code)
        self.assertIn("when(", code)

    def test_merge_join_different_key_names(self):
        xml = """
        <step><join_type>LEFT OUTER</join_type>
          <key_1>left_id</key_1><value_1>right_id</value_1></step>
        """
        outcome = self.registry.convert_step(
            "MergeJoin",
            _ctx_multi(xml, "MergeJoin", "MJ", ["L", "R"]),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn('df_L["left_id"] == df_R["right_id"]', code)

    def test_value_mapper_fields_xml(self):
        xml = """
        <step>
          <field_to_use>status</field_to_use><target_field>status_label</target_field>
          <fields><field><source_value>A</source_value><target_value>Active</target_value></field></fields>
        </step>
        """
        outcome = self.registry.convert_step("ValueMapper", _ctx(xml, "ValueMapper", "VM"))
        self.assertIn("when(", "\n".join(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")

    def test_formula_bracket_fields(self):
        xml = "<step><field_name>total</field_name><formula>[price] * [qty]</formula></step>"
        outcome = self.registry.convert_step("Formula", _ctx(xml, "Formula", "F"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('col("price")', code)
        self.assertIn('col("qty")', code)

    def test_select_values_rename(self):
        xml = """<step><fields>
          <field><name>id</name><rename>customer_id</rename></field></fields></step>"""
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('.alias("customer_id")', code)

    def test_select_values_meta_and_remove(self):
        xml = """<step>
          <meta><name>a</name><rename>a_new</rename></meta>
          <fields><field><name>a</name></field><field><name>b</name></field></fields>
          <remove><name>b</name></remove>
        </step>"""
        outcome = self.registry.convert_step("SelectValues", _ctx(xml, "SelectValues", "SV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('.alias("a_new")', code)
        self.assertNotIn('col("b")', code)

    def test_value_mapper_source_target_tags(self):
        xml = """
        <step>
          <field_to_use>status</field_to_use><target_field>status_label</target_field>
          <fields><field><source>A</source><target>Active</target></field></fields>
        </step>
        """
        outcome = self.registry.convert_step("ValueMapper", _ctx(xml, "ValueMapper", "VM"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("lit('Active')", code)
        self.assertEqual(outcome.status, "converted")

    def test_group_by_valuefield_subject(self):
        xml = """
        <step>
          <group><field><name>region</name></field></group>
          <fields>
            <field><aggregate>SUM</aggregate><valuefield>amount</valuefield>
              <name>total_amount</name></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step("GroupBy", _ctx(xml, "GroupBy", "GB"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('col("amount")', code)
        self.assertNotIn('col("total_amount")', code)

    def test_group_by_subject_column(self):
        xml = """
        <step>
          <group><field><name>region</name></field></group>
          <fields>
            <field><aggregate>SUM</aggregate><subject>amount</subject>
              <name>total_amount</name></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step("GroupBy", _ctx(xml, "GroupBy", "GB"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('col("amount")', code)
        self.assertIn("total_amount", code)

    def test_calculator_remove_fields(self):
        xml = """
        <step><calculation>
          <field_name>total</field_name><calc_type>MULTIPLY</calc_type>
          <field_a>price</field_a><field_b>qty</field_b><remove>Y</remove>
        </calculation></step>
        """
        outcome = self.registry.convert_step("Calculator", _ctx(xml, "Calculator", "Calc"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".drop(", code)
        self.assertIn("price", code)

    def test_csv_input_file_tag(self):
        xml = "<step><file>/data/in.csv</file><separator>;</separator></step>"
        outcome = self.registry.convert_step("CsvInput", _ctx(xml, "CsvInput", "CSV", with_input=False))
        self.assertIn("/data/in.csv", "\n".join(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
