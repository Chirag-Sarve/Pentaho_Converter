"""Coverage for previously untested canonical Spoon step types."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import parse_step_metadata
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="GapTrans", file_path=Path("gap.ktr"))
    if with_input:
        inp = PentahoStep(name="Upstream", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Upstream", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestGapSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_parquet_input(self):
        xml = "<step><filename>/data/events.parquet</filename></step>"
        outcome = self.registry.convert_step("ParquetInput", _ctx(xml, "ParquetInput", "PQ", with_input=False))
        code = "\n".join(outcome.code_lines)
        self.assertIn("parquet", code.lower())
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_orc_output(self):
        xml = "<step><filename>/data/out.orc</filename></step>"
        outcome = self.registry.convert_step("OrcOutput", _ctx(xml, "OrcOutput", "ORC"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("orc", code.lower())
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_hadoop_file_input(self):
        xml = "<step><filename>/warehouse/raw</filename><file_format>text</file_format></step>"
        outcome = self.registry.convert_step(
            "HadoopFileInput", _ctx(xml, "HadoopFileInput", "HDFS", with_input=False)
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".load(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_top_n_limit(self):
        xml = "<step><nr_lines>25</nr_lines></step>"
        outcome = self.registry.convert_step("Top", _ctx(xml, "Top", "TopN"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".limit(25)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_regex_replace(self):
        xml = """
        <step>
          <in_stream_name>phone</in_stream_name>
          <out_stream_name>phone_clean</out_stream_name>
          <search>[^0-9]</search>
          <replace></replace>
          <use_regex>Y</use_regex>
        </step>
        """
        outcome = self.registry.convert_step("RegexReplace", _ctx(xml, "RegexReplace", "RR"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("regexp_replace", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_splunk_input_unsupported_warning(self):
        xml = """
        <step>
          <host>splunk.example.com</host>
          <port>8089</port>
          <query>search index=main</query>
        </step>
        """
        outcome = self.registry.convert_step(
            "SplunkInput", _ctx(xml, "SplunkInput", "Splunk", with_input=False)
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.host=", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_replace_null_metadata_routed(self):
        xml = """
        <step>
          <fields>
            <field><name>city</name><value>UNKNOWN</value></field>
          </fields>
        </step>
        """
        meta = parse_step_metadata(ET.fromstring(textwrap.dedent(xml).strip()), "ReplaceNull")
        self.assertEqual(meta["fields"][0]["name"], "city")
        outcome = self.registry.convert_step("ReplaceNull", _ctx(xml, "ReplaceNull", "RN"))
        self.assertTrue(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
