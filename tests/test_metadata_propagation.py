"""Tests for metadata propagation and column lineage."""

from __future__ import annotations

import json
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.metadata_propagation import (
    build_step_metadata_bundle,
    get_converter_metadata,
    propagate_step_metadata,
)
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.transformation_parser import _parse_step


def _ctx(xml: str, stype: str, name: str, inputs: list[str] | None = None) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(xml).strip())
    step = _parse_step(step_el)
    trans = PentahoTransformation(
        name="T", file_path=Path("t.ktr"), parameters={"BATCH_DATE": "2026-01-01"}
    )
    ins = inputs or ["Input"]
    input_steps = [
        PentahoStep(name=n, step_type="RowGenerator", attributes={}, raw_element=None)
        for n in ins
    ]
    trans.steps = input_steps + [step]
    hops = [PentahoHop(from_name=n, to_name=name) for n in ins]
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(
        transformation=trans,
        step=step,
        dag=dag,
        df_variable_map=df_map,
        extra={
            "input_columns": ["id", "price", "qty", "status"],
            "lineage_map": {
                "Input": {
                    "id": __import__("pentaho_converter.metadata_models", fromlist=["ColumnSchema"]).ColumnSchema("id", "Integer"),
                    "price": __import__("pentaho_converter.metadata_models", fromlist=["ColumnSchema"]).ColumnSchema("price", "Number"),
                    "qty": __import__("pentaho_converter.metadata_models", fromlist=["ColumnSchema"]).ColumnSchema("qty", "Integer"),
                    "status": __import__("pentaho_converter.metadata_models", fromlist=["ColumnSchema"]).ColumnSchema("status", "String"),
                }
            },
        },
    )


class TestMetadataPropagation(unittest.TestCase):
    def _show_propagation(self, label: str, ctx: StepContext) -> dict:
        before = build_step_metadata_bundle(ctx.step)
        bundle = propagate_step_metadata(ctx)
        received = get_converter_metadata(ctx)
        return {
            "label": label,
            "before_parsed_config": before.parsed_config,
            "after_converter_metadata_keys": sorted(received.keys()),
            "received": received,
            "trace": bundle.propagation_trace,
        }

    def test_calculator_metadata_propagation(self):
        xml = """
        <step><name>Calc</name><type>Calculator</type>
          <calculation>
            <field_name>total</field_name><calc_type>MULTIPLY</calc_type>
            <field_a>price</field_a><field_b>qty</field_b>
            <value_type>Number</value_type><remove>Y</remove>
          </calculation>
        </step>
        """
        ctx = _ctx(xml, "Calculator", "Calc")
        report = self._show_propagation("Calculator", ctx)
        received = report["received"]

        self.assertIn("calculations", report["before_parsed_config"])
        self.assertIn("calculations", received)
        self.assertEqual(received["calculations"][0]["field_a"], "price")
        self.assertTrue(received["calculations"][0]["remove"])
        self.assertIn("parsed_config.calculations", report["trace"])

    def test_merge_join_metadata_propagation(self):
        xml = """
        <step><name>MJ</name><type>MergeJoin</type>
          <join_type>INNER</join_type><step1>L</step1><step2>R</step2>
          <keys_1><key>left_id</key></keys_1>
          <keys_2><key>right_id</key></keys_2>
        </step>
        """
        ctx = _ctx(xml, "MergeJoin", "MJ", ["L", "R"])
        received = self._show_propagation("MergeJoin", ctx)["received"]

        self.assertEqual(received["join_type"], "INNER")
        self.assertEqual(received["step1"], "L")
        self.assertEqual(received["keys_1"], ["left_id"])
        self.assertEqual(received["join_keys"][0]["left"], "left_id")

    def test_group_by_metadata_propagation(self):
        xml = """
        <step><name>GB</name><type>GroupBy</type>
          <group><field><name>region</name></field></group>
          <fields>
            <field><aggregate>SUM</aggregate><subject>amount</subject>
              <name>total_amount</name><type>Number</type></field>
          </fields>
        </step>
        """
        ctx = _ctx(xml, "GroupBy", "GB")
        received = self._show_propagation("GroupBy", ctx)["received"]

        self.assertEqual(received["group_keys"], ["region"])
        self.assertEqual(received["aggregates"][0]["aggregate"], "SUM")
        self.assertEqual(received["aggregates"][0]["subject"], "amount")
        self.assertIn("aggregate_fields", received)

    def test_filter_rows_metadata_propagation(self):
        xml = """
        <step><name>F</name><type>FilterRows</type>
          <compare_value>status = 'ACTIVE'</compare_value>
          <send_true_to>Out</send_true_to><send_false_to>Reject</send_false_to>
          <compare><condition>
            <negated>N</negated><leftvalue>age</leftvalue><function>&gt;</function>
            <value><type>Integer</type><text>18</text></value>
          </condition></compare>
        </step>
        """
        ctx = _ctx(xml, "FilterRows", "F")
        received = self._show_propagation("FilterRows", ctx)["received"]

        self.assertEqual(received["compare_value"], "status = 'ACTIVE'")
        self.assertEqual(received["send_true_to"], "Out")
        self.assertEqual(received["condition"]["leftvalue"], "age")
        self.assertIn("filter_condition", received)

    def test_value_mapper_metadata_propagation(self):
        xml = """
        <step><name>VM</name><type>ValueMapper</type>
          <field_to_use>status</field_to_use><target_field>status_label</target_field>
          <non_match_default>Unknown</non_match_default>
          <fields><field><source_value>A</source_value><target_value>Active</target_value></field></fields>
        </step>
        """
        ctx = _ctx(xml, "ValueMapper", "VM")
        received = self._show_propagation("ValueMapper", ctx)["received"]

        self.assertEqual(received["field_to_use"], "status")
        self.assertEqual(received["mappings"][0]["source"], "A")
        self.assertEqual(received["non_match_default"], "Unknown")
        self.assertIn("value_mappings", received)

    def test_row_generator_metadata_propagation(self):
        xml = """
        <step><name>RG</name><type>RowGenerator</type><limit>3</limit>
          <fields>
            <field><name>id</name><type>Integer</type><string>1</string></field>
          </fields>
        </step>
        """
        ctx = _ctx(xml, "RowGenerator", "RG", [])
        received = self._show_propagation("RowGenerator", ctx)["received"]

        self.assertEqual(received["limit"], 3)
        self.assertEqual(received["fields"][0]["value"], "1")
        self.assertEqual(received["row_field_types"]["id"], "Integer")

    def test_sequence_metadata_propagation(self):
        xml = """
        <step><name>Seq</name><type>Sequence</type>
          <valuename>order_id</valuename><start>1</start><increment>1</increment>
        </step>
        """
        ctx = _ctx(xml, "Sequence", "Seq")
        received = self._show_propagation("Sequence", ctx)["received"]
        self.assertEqual(received["field_name"], "order_id")

    def test_text_file_output_metadata_propagation(self):
        xml = """
        <step><name>Out</name><type>TextFileOutput</type>
          <file><name>/out/data.csv</name><append>Y</append></file>
          <separator>|</separator><header>Y</header><encoding>UTF-8</encoding>
        </step>
        """
        ctx = _ctx(xml, "TextFileOutput", "Out")
        received = self._show_propagation("TextFileOutput", ctx)["received"]
        self.assertEqual(received["filename"], "/out/data.csv")
        self.assertEqual(received["separator"], "|")
        self.assertTrue(received["append"])

    def test_table_input_metadata_propagation(self):
        xml = """
        <step><name>TI</name><type>TableInput</type>
          <connection>DB</connection>
          <sql>SELECT * FROM t WHERE d='${BATCH_DATE}'</sql>
          <variables_active>Y</variables_active>
        </step>
        """
        ctx = _ctx(xml, "TableInput", "TI", [])
        received = self._show_propagation("TableInput", ctx)["received"]
        self.assertEqual(received["connection"], "DB")
        self.assertIn("2026-01-01", received["sql_resolved"])

    def test_database_lookup_metadata_propagation(self):
        xml = """
        <step><name>LKP</name><type>DatabaseLookup</type>
          <connection>WH</connection><schema>d</schema><table>dim</table>
          <lookup><key><name>id</name><field>pk</field></key></lookup>
          <value><name>name</name><rename>customer_name</rename></value>
        </step>
        """
        ctx = _ctx(xml, "DatabaseLookup", "LKP")
        received = self._show_propagation("DatabaseLookup", ctx)["received"]
        self.assertEqual(received["keys"][0]["stream_field"], "id")
        self.assertEqual(received["return_fields"][0]["rename"], "customer_name")
        self.assertEqual(received["join_keys"][0]["left"], "id")

    def test_lineage_validation_missing_column(self):
        """Missing upstream columns must warn, not abort step generation."""
        xml = """
        <step><name>SV</name><type>SelectValues</type>
          <fields><field><name>missing_col</name></field></fields>
        </step>
        """
        ctx = _ctx(xml, "SelectValues", "SV")
        outcome = build_default_registry().convert_step("SelectValues", ctx)
        # Schema tracking assists validation only — generation must continue.
        self.assertNotEqual(outcome.status, "failed")
        self.assertTrue(outcome.code_lines)
        self.assertTrue(
            any("missing_col" in w for w in outcome.warnings)
            or any("missing_col" in e for e in outcome.errors)
            or "missing_col" in "\n".join(outcome.code_lines)
        )

    def test_converter_receives_propagated_metadata(self):
        xml = """
        <step><name>Calc</name><type>Calculator</type>
          <calculation>
            <field_name>total</field_name><calc_type>ADD</calc_type>
            <field_a>price</field_a><field_b>qty</field_b>
          </calculation>
        </step>
        """
        ctx = _ctx(xml, "Calculator", "Calc")
        outcome = build_default_registry().convert_step("Calculator", ctx)
        self.assertIn("calculations", ctx.converter_metadata)
        self.assertIn("withColumn", "\n".join(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
