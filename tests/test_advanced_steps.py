"""Tests for advanced Pentaho step converters."""

from __future__ import annotations

import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
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
    df_map = {}
    for s in trans.steps:
        safe = s.name.replace(" ", "_")
        st = (
            (s.step_type or "")
            .strip()
            .lower()
            .replace(" ", "")
            .replace("(", "")
            .replace(")", "")
        )
        if st in {"dummy", "dummytrans", "dummydonothing"}:
            lower = safe.lower()
            if lower == "dummy" or lower.startswith("dummy_"):
                df_map[s.name] = f"df_{safe}"
            else:
                df_map[s.name] = f"df_Dummy_{safe}"
        else:
            df_map[s.name] = f"df_{safe}"
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


class TestAdvancedHandlers(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_unique_drop_duplicates(self):
        xml = """
        <step>
          <count_rows>N</count_rows>
          <fields><field><name>id</name></field></fields>
        </step>
        """
        lines, status = self.registry.generate_code("Unique", _ctx(xml, "Unique", "Unique rows"))
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("dropDuplicates", code)

    def test_value_mapper_when(self):
        xml = """
        <step>
          <field_to_use>status</field_to_use>
          <target_field>status_label</target_field>
          <non_match_default>UNKNOWN</non_match_default>
          <valuemap><source_value>A</source_value><target_value>Active</target_value></valuemap>
        </step>
        """
        lines, status = self.registry.generate_code("ValueMapper", _ctx(xml, "ValueMapper", "Map values"))
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("when(", code)
        self.assertIn("'Active'", code)

    def test_sequence_row_number(self):
        xml = """
        <step>
          <fieldname>seq_id</fieldname>
          <start_at>100</start_at>
          <increment_by>5</increment_by>
        </step>
        """
        lines, status = self.registry.generate_code("Sequence", _ctx(xml, "Sequence", "Add seq"))
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("row_number()", code)
        self.assertIn("100", code)

    def test_system_info_timestamp(self):
        xml = """
        <step>
          <fields>
            <field><name>run_ts</name><type>system datetime (variable)</type></field>
          </fields>
        </step>
        """
        lines, status = self.registry.generate_code("SystemInfo", _ctx(xml, "SystemInfo", "Sys info", False))
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("current_timestamp()", code)

    def test_abort_raises(self):
        xml = "<step><message>Stop pipeline</message></step>"
        lines, status = self.registry.generate_code("Abort", _ctx(xml, "Abort", "Abort step"))
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("raise RuntimeError", code)

    def test_rank_window(self):
        xml = """
        <step>
          <field_name>amount</field_name>
          <rank_name>rnk</rank_name>
          <sort_size>5</sort_size>
          <fields><field><name>amount</name><ascending>Y</ascending></field></fields>
        </step>
        """
        lines, status = self.registry.generate_code("Rank", _ctx(xml, "Rank", "Rank rows"))
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("rank()", code)

    def test_unknown_step_uses_fallback(self):
        lines, status = self.registry.generate_code(
            "TotallyUnknownStepXYZ",
            _ctx("<step/>", "TotallyUnknownStepXYZ", "Mystery", False),
        )
        self.assertIn(status, ("converted", "partial", "partially_supported", "partial"))
        self.assertTrue(lines)
        self.assertNotIn("unsupported", "\n".join(lines).lower())

    def test_dummy_passthrough(self):
        lines, status = self.registry.generate_code(
            "Dummy",
            _ctx("<step/>", "Dummy", "Rejected Records"),
        )
        code = "\n".join(lines)
        self.assertEqual(status, "converted")
        self.assertIn("# Dummy: Rejected Records", code)
        self.assertIn("# Pass-through step - DataFrame unchanged", code)
        # Hop input is the predecessor DataFrame, output uses Dummy_ prefix.
        self.assertIn("df_Dummy_Rejected_Records = df_Input", code)
        self.assertNotIn("df_Rejected_Records = df_Input", code)
        self.assertNotIn(".filter(", code)
        self.assertNotIn(".join(", code)
        self.assertNotIn(".select(", code)
        self.assertNotIn(".sort(", code)

    def test_dummy_do_nothing_alias(self):
        lines, status = self.registry.generate_code(
            "Dummy (do nothing)",
            _ctx("<step/>", "Dummy (do nothing)", "Dummy"),
        )
        code = "\n".join(lines)
        self.assertEqual(status, "converted")
        self.assertIn("# Dummy: Dummy", code)
        # Step name already starts with Dummy → df_Dummy (not df_Dummy_Dummy)
        self.assertIn("df_Dummy = df_Input", code)
        self.assertNotIn("df_Input =", code)

    def test_dummy_filter_false_branch_uses_hop_stream(self):
        """False hop from Filter must not be overwritten with Filter's primary DF."""
        filter_xml = """
        <step>
          <name>Filter rows</name>
          <type>FilterRows</type>
          <send_true_to>Sort Rows</send_true_to>
          <send_false_to>Rejected Records</send_false_to>
          <compare>
            <condition>
              <negated>N</negated>
              <leftvalue>status</leftvalue>
              <function>=</function>
              <value><type>String</type><text>OK</text></value>
            </condition>
          </compare>
        </step>
        """
        filter_el = ET.fromstring(textwrap.dedent(filter_xml).strip())
        source = PentahoStep(
            name="Source", step_type="RowGenerator", attributes={}, raw_element=None
        )
        filt = PentahoStep(
            name="Filter rows",
            step_type="FilterRows",
            attributes={},
            raw_element=filter_el,
        )
        from pentaho_converter.step_xml import parse_filter_rows_config

        filt.parsed_config = parse_filter_rows_config(filter_el)
        sort = PentahoStep(
            name="Sort Rows", step_type="SortRows", attributes={}, raw_element=None
        )
        dummy = PentahoStep(
            name="Rejected Records",
            step_type="Dummy",
            attributes={},
            raw_element=ET.fromstring("<step/>"),
        )
        trans = PentahoTransformation(name="t", file_path=Path("t.ktr"))
        trans.steps = [source, filt, sort, dummy]
        hops = [
            PentahoHop(from_name="Source", to_name="Filter rows"),
            PentahoHop(from_name="Filter rows", to_name="Sort Rows"),
            PentahoHop(from_name="Filter rows", to_name="Rejected Records"),
        ]
        dag = StepDAG(trans.steps, hops)
        df_map = {
            "Source": "df_Source",
            "Filter rows": "df_Filter_rows",
            "Sort Rows": "df_Sort_Rows",
            "Rejected Records": "df_Dummy_Rejected_Records",
        }

        filter_ctx = StepContext(
            transformation=trans, step=filt, dag=dag, df_variable_map=df_map
        )
        filter_lines, filter_status = self.registry.generate_code("FilterRows", filter_ctx)
        filter_code = "\n".join(filter_lines)
        self.assertIn(filter_status, ("converted", "partial", "partially_supported"))
        self.assertIn("df_Sort_Rows = df_Source.filter(", filter_code)
        self.assertIn("df_Rejected_Records = df_Source.filter(~(", filter_code)

        dummy_ctx = StepContext(
            transformation=trans, step=dummy, dag=dag, df_variable_map=df_map
        )
        dummy_lines, dummy_status = self.registry.generate_code("Dummy", dummy_ctx)
        dummy_code = "\n".join(dummy_lines)
        self.assertEqual(dummy_status, "converted")
        self.assertIn("# Pass-through step - DataFrame unchanged", dummy_code)
        self.assertIn("df_Dummy_Rejected_Records = df_Rejected_Records", dummy_code)
        # Must NOT overwrite the false-branch stream with Filter's primary output
        self.assertNotIn("df_Rejected_Records = df_Filter_rows", dummy_code)
        self.assertNotIn("df_Dummy_Rejected_Records = df_Filter_rows", dummy_code)


if __name__ == "__main__":
    unittest.main()
