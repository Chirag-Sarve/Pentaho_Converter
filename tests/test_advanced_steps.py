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
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
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


if __name__ == "__main__":
    unittest.main()
