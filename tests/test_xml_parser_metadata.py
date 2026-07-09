"""Tests for XML parser structured metadata (before/after)."""

from __future__ import annotations

import json
import textwrap
import unittest
from xml.etree import ElementTree as ET

from pentaho_converter.step_xml import parse_step_metadata
from pentaho_converter.transformation_parser import (
    _parse_step,
    shallow_parse_step_attributes,
)


def _step(xml: str) -> ET.Element:
    return ET.fromstring(textwrap.dedent(xml).strip())


def _pretty(data: object) -> str:
    return json.dumps(data, indent=2, sort_keys=True)


class TestXmlParserMetadata(unittest.TestCase):
    """Verify nested XML is parsed into structured metadata."""

    def test_calculator_before_after(self):
        xml = """
        <step>
          <name>Calc</name><type>Calculator</type>
          <calculation>
            <field_name>total</field_name>
            <calc_type>5</calc_type>
            <field_a>price</field_a>
            <field_b>qty</field_b>
            <value_type>Number</value_type>
            <conversion_mask>#.#</conversion_mask>
            <decimal_symbol>.</decimal_symbol>
            <remove>Y</remove>
          </calculation>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        self.assertIn("<calculation>", before.get("calculation", ""))
        self.assertIn("calculation", before)
        self.assertEqual(after["calculations"][0]["calc_type"], "MULTIPLY")
        self.assertEqual(after["calculations"][0]["decimal_symbol"], ".")
        self.assertTrue(after["calculations"][0]["remove"])

    def test_merge_join_before_after(self):
        xml = """
        <step>
          <name>MJ</name><type>MergeJoin</type>
          <join_type>LEFT OUTER</join_type>
          <step1>Orders</step1><step2>Customers</step2>
          <keys_1><key>order_id</key></keys_1>
          <keys_2><key>customer_id</key></keys_2>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        self.assertIn("<keys_1>", before.get("keys_1", ""))
        self.assertEqual(after["join_type"], "LEFT OUTER")
        self.assertEqual(after["step1"], "Orders")
        self.assertEqual(after["keys_1"], ["order_id"])
        self.assertEqual(after["keys_2"], ["customer_id"])
        self.assertEqual(after["keys"][0]["left"], "order_id")
        self.assertEqual(after["keys"][0]["right"], "customer_id")

    def test_group_by_before_after(self):
        xml = """
        <step>
          <name>GB</name><type>GroupBy</type>
          <group><field><name>region</name></field></group>
          <fields>
            <field>
              <aggregate>SUM</aggregate>
              <subject>amount</subject>
              <name>total_amount</name>
              <type>Number</type>
              <valuefield>amount</valuefield>
            </field>
          </fields>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        self.assertIn("<group>", before.get("group", ""))
        self.assertEqual(after["group_keys"], ["region"])
        self.assertEqual(after["aggregates"][0]["aggregate"], "SUM")
        self.assertEqual(after["aggregates"][0]["subject"], "amount")
        self.assertEqual(after["aggregates"][0]["valuefield"], "amount")

    def test_value_mapper_before_after(self):
        xml = """
        <step>
          <name>VM</name><type>ValueMapper</type>
          <field_to_use>status</field_to_use>
          <target_field>status_label</target_field>
          <non_match_default>Unknown</non_match_default>
          <fields>
            <field><source_value>A</source_value><target_value>Active</target_value></field>
          </fields>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        # Legacy parser skipped <fields> container.
        self.assertEqual(before.get("field_to_use"), "status")
        self.assertNotIn("fields", before)
        self.assertEqual(after["field_to_use"], "status")
        self.assertEqual(after["mappings"][0]["source"], "A")
        self.assertEqual(after["mappings"][0]["target"], "Active")
        self.assertEqual(after["non_match_default"], "Unknown")

    def test_filter_rows_before_after(self):
        xml = """
        <step>
          <name>Filter</name><type>FilterRows</type>
          <compare_value>status = 'ACTIVE'</compare_value>
          <compare>
            <condition>
              <negated>N</negated>
              <leftvalue>age</leftvalue>
              <function>&gt;</function>
              <value><type>Integer</type><text>18</text></value>
            </condition>
          </compare>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        self.assertIn("<condition>", before.get("compare", ""))
        self.assertEqual(after["compare_value"], "status = 'ACTIVE'")
        self.assertEqual(after["condition"]["leftvalue"], "age")
        self.assertEqual(after["condition"]["function"], ">")
        self.assertEqual(after["condition"]["value"]["text"], "18")

    def test_row_generator_before_after(self):
        xml = """
        <step>
          <name>RG</name><type>RowGenerator</type>
          <limit>5</limit>
          <fields>
            <field><name>id</name><type>Integer</type><string>42</string></field>
            <field><name>country</name><type>String</type><default>India</default></field>
          </fields>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        # Legacy parser skipped <fields> and nested <file> children.
        self.assertEqual(before.get("limit"), "5")
        self.assertNotIn("fields", before)
        self.assertEqual(after["limit"], 5)
        self.assertEqual(after["fields"][0]["value"], "42")
        self.assertEqual(after["fields"][1]["value"], "India")

    def test_sequence_before_after(self):
        xml = """
        <step>
          <name>Seq</name><type>Sequence</type>
          <valuename>order_id</valuename>
          <start>10</start><increment>2</increment><max_value>1000</max_value>
        </step>
        """
        el = _step(xml)
        after = _parse_step(el).parsed_config
        self.assertEqual(after["field_name"], "order_id")
        self.assertEqual(after["start_at"], 10)
        self.assertEqual(after["increment_by"], 2)
        self.assertEqual(after["max_value"], 1000)

    def test_text_file_output_before_after(self):
        xml = """
        <step>
          <name>Out</name><type>TextFileOutput</type>
          <file>
            <name>/data/out/sales.csv</name>
            <extention>csv</extention>
            <append>Y</append>
            <create_parent_folder>Y</create_parent_folder>
          </file>
          <separator>|</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <compression>gzip</compression>
          <fields>
            <field><name>id</name><type>Integer</type></field>
          </fields>
        </step>
        """
        el = _step(xml)
        step = _parse_step(el)
        before = shallow_parse_step_attributes(el)
        after = step.parsed_config

        # Legacy parser kept only the file path, not nested file properties.
        self.assertEqual(before.get("filename"), "/data/out/sales.csv")
        self.assertNotIn("file_append", before)
        self.assertEqual(step.attributes["filename"], "/data/out/sales.csv")
        self.assertEqual(step.attributes["file_append"], "Y")
        self.assertEqual(after["filename"], "/data/out/sales.csv")
        self.assertEqual(after["separator"], "|")
        self.assertTrue(after["append"])
        self.assertTrue(after["create_parent_folder"])
        self.assertEqual(after["compression"], "gzip")
        self.assertEqual(after["output_fields"][0]["name"], "id")

    def test_table_input_before_after(self):
        xml = """
        <step>
          <name>TI</name><type>TableInput</type>
          <connection>MyDB</connection>
          <sql>SELECT * FROM t WHERE dt = '${BATCH_DATE}'</sql>
          <limit>100</limit>
          <variables_active>Y</variables_active>
          <execute_each_row>N</execute_each_row>
          <parameters>
            <parameter><name>BATCH_DATE</name><default>2026-01-01</default></parameter>
          </parameters>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        self.assertIn("<parameter>", before.get("parameters", ""))
        self.assertEqual(after["connection"], "MyDB")
        self.assertIn("${BATCH_DATE}", after["sql"])
        self.assertEqual(after["limit"], 100)
        self.assertTrue(after["variables_active"])
        self.assertEqual(after["parameters"][0]["name"], "BATCH_DATE")

    def test_database_lookup_before_after(self):
        xml = """
        <step>
          <name>LKP</name><type>DatabaseLookup</type>
          <connection>WH</connection>
          <schema>dim</schema>
          <table>customers</table>
          <cached>Y</cached><cache_size>5000</cache_size>
          <lookup>
            <key>
              <name>customer_id</name>
              <field>id</field>
            </key>
          </lookup>
          <value>
            <name>customer_name</name>
            <rename>name</rename>
            <default>Unknown</default>
            <type>String</type>
          </value>
          <fail_on_multiple>N</fail_on_multiple>
          <eat_row_on_failure>Y</eat_row_on_failure>
        </step>
        """
        el = _step(xml)
        before = shallow_parse_step_attributes(el)
        after = _parse_step(el).parsed_config

        self.assertIn("<key>", before.get("lookup", ""))
        self.assertIn("<name>", before.get("value", ""))
        self.assertEqual(after["connection"], "WH")
        self.assertEqual(after["table"], "customers")
        self.assertTrue(after["cached"])
        self.assertEqual(after["keys"][0]["stream_field"], "customer_id")
        self.assertEqual(after["keys"][0]["table_field"], "id")
        self.assertEqual(after["return_fields"][0]["name"], "customer_name")
        self.assertEqual(after["return_fields"][0]["rename"], "name")
        self.assertTrue(after["eat_row_on_failure"])

    def test_step_converter_test_ktr_filter_parsed(self):
        from pathlib import Path
        from pentaho_converter.transformation_parser import parse_transformation

        ktr = Path(__file__).resolve().parent / "samples" / "Step_Converter_Test.ktr"
        trans = parse_transformation(ktr)
        filt = next(s for s in trans.steps if s.step_type == "FilterRows")
        cond = filt.parsed_config["condition"]
        self.assertIsNotNone(cond)
        self.assertIn("conditions", cond)
        self.assertEqual(len(cond["conditions"]), 2)


if __name__ == "__main__":
    unittest.main()
