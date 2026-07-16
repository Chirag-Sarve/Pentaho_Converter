"""Tests for Pentaho Mapping (sub-transformation) step migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_mapping_config,
    parse_mapping_input_config,
    parse_mapping_output_config,
    parse_simple_mapping_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.mapping_handlers import MAPPING_HANDLERS
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    extra_inputs: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="MappingTrans", file_path=Path("mapping_parent.ktr"))
    steps = []
    hops = []
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        steps.append(inp)
        hops.append(PentahoHop(from_name="Input", to_name=step_name))
    for name in extra_inputs or []:
        steps.append(PentahoStep(name=name, step_type="Dummy", attributes={}, raw_element=None))
        hops.append(PentahoHop(from_name=name, to_name=step_name))
    steps.append(step)
    trans.steps = steps
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


_MAPPING_XML = """
<step>
  <name>Call Sub</name>
  <type>Mapping</type>
  <specification_method>filename</specification_method>
  <filename>${Internal.Entry.Current.Directory}/child_map.ktr</filename>
  <allow_multiple_input>N</allow_multiple_input>
  <allow_multiple_output>N</allow_multiple_output>
  <mappings>
    <input>
      <mapping>
        <input_step>Input</input_step>
        <output_step>Mapping input</output_step>
        <main_path>Y</main_path>
        <rename_on_output>Y</rename_on_output>
        <description>main in</description>
        <connector><parent>id</parent><child>cust_id</child></connector>
        <connector><parent>name</parent><child>cust_name</child></connector>
      </mapping>
    </input>
    <output>
      <mapping>
        <input_step>Mapping output</input_step>
        <output_step></output_step>
        <main_path>Y</main_path>
        <rename_on_output>N</rename_on_output>
        <connector><parent>status</parent><child>out_status</child></connector>
      </mapping>
    </output>
    <parameters>
      <variablemapping><variable>THRESHOLD</variable><input>100</input></variablemapping>
      <variablemapping><variable>ENV</variable><input>${DEPLOY_ENV}</input></variablemapping>
      <inherit_all_vars>Y</inherit_all_vars>
    </parameters>
  </mappings>
</step>
"""

_SIMPLE_MAPPING_XML = """
<step>
  <name>Simple Call</name>
  <type>SimpleMapping</type>
  <specification_method>filename</specification_method>
  <filename>helpers/enrich.ktr</filename>
  <mappings>
    <input>
      <mapping>
        <main_path>Y</main_path>
        <connector><parent>sku</parent><child>product_sku</child></connector>
      </mapping>
    </input>
    <output>
      <mapping>
        <main_path>Y</main_path>
        <connector><parent>price</parent><child>list_price</child></connector>
      </mapping>
    </output>
    <parameters>
      <variablemapping><variable>REGION</variable><input>US</input></variablemapping>
      <inherit_all_vars>N</inherit_all_vars>
    </parameters>
  </mappings>
</step>
"""

_MAPPING_INPUT_XML = """
<step>
  <name>Mapping input</name>
  <type>MappingInput</type>
  <fields>
    <field>
      <name>cust_id</name>
      <type>Integer</type>
      <length>9</length>
      <precision>0</precision>
      <default>0</default>
    </field>
    <field>
      <name>cust_name</name>
      <type>String</type>
      <length>50</length>
      <precision>-1</precision>
      <required>Y</required>
    </field>
    <field>
      <name>note</name>
      <type>String</type>
      <length>100</length>
      <optional>Y</optional>
      <default_value></default_value>
    </field>
    <select_unspecified>Y</select_unspecified>
  </fields>
</step>
"""

_MAPPING_OUTPUT_XML = """
<step>
  <name>Mapping output</name>
  <type>MappingOutput</type>
  <fields>
    <field><name>out_status</name><type>String</type></field>
    <field><name>out_score</name><type>Number</type><rename>score</rename></field>
  </fields>
</step>
"""


class TestMappingParsers(unittest.TestCase):
    def test_mapping_parse(self):
        cfg = parse_mapping_config(ET.fromstring(_MAPPING_XML))
        self.assertIn("child_map.ktr", cfg["filename"])
        self.assertEqual(len(cfg["input_mappings"]), 1)
        self.assertEqual(len(cfg["output_mappings"]), 1)
        self.assertEqual(cfg["input_mappings"][0]["connectors"][0]["parent"], "id")
        self.assertEqual(cfg["input_mappings"][0]["connectors"][0]["child"], "cust_id")
        self.assertTrue(cfg["input_mappings"][0]["rename_on_output"])
        self.assertEqual(len(cfg["parameters"]), 2)
        self.assertEqual(cfg["parameters"][0]["variable"], "THRESHOLD")
        self.assertTrue(cfg["inherit_all_variables"])

    def test_simple_mapping_parse(self):
        cfg = parse_simple_mapping_config(ET.fromstring(_SIMPLE_MAPPING_XML))
        self.assertEqual(cfg["filename"], "helpers/enrich.ktr")
        self.assertEqual(cfg["input_mapping"]["connectors"][0]["child"], "product_sku")
        self.assertEqual(cfg["output_mapping"]["connectors"][0]["parent"], "price")
        self.assertFalse(cfg["inherit_all_variables"])
        self.assertEqual(cfg["parameters"][0]["variable"], "REGION")

    def test_mapping_input_parse(self):
        cfg = parse_mapping_input_config(ET.fromstring(_MAPPING_INPUT_XML))
        self.assertEqual(len(cfg["fields"]), 3)
        self.assertEqual(cfg["fields"][0]["name"], "cust_id")
        self.assertEqual(cfg["fields"][0]["type"], "Integer")
        self.assertEqual(cfg["fields"][0]["default_value"], "0")
        self.assertTrue(cfg["fields"][0]["required"])
        self.assertTrue(cfg["fields"][2]["optional"])
        self.assertTrue(cfg["select_unspecified"])
        self.assertEqual(cfg["field_names"], ["cust_id", "cust_name", "note"])

    def test_mapping_output_parse(self):
        cfg = parse_mapping_output_config(ET.fromstring(_MAPPING_OUTPUT_XML))
        self.assertEqual(cfg["field_names"], ["out_status", "out_score"])
        self.assertEqual(cfg["fields"][1]["rename"], "score")

    def test_legacy_connector_format(self):
        cfg = parse_mapping_config(ET.fromstring("""
        <step>
          <filename>legacy.ktr</filename>
          <input>
            <connector><field>a</field><mapping>b</mapping></connector>
          </input>
          <output>
            <connector><field>c</field><mapping>d</mapping></connector>
          </output>
        </step>
        """))
        self.assertEqual(cfg["input_mappings"][0]["connectors"][0]["parent"], "a")
        self.assertEqual(cfg["input_mappings"][0]["connectors"][0]["child"], "b")
        self.assertEqual(cfg["output_mappings"][0]["connectors"][0]["child"], "d")

    def test_parse_step_metadata_dispatch(self):
        mapping = parse_step_metadata(ET.fromstring(_MAPPING_XML), "Mapping")
        self.assertIn("child_map.ktr", mapping["filename"])
        simple = parse_step_metadata(ET.fromstring(_SIMPLE_MAPPING_XML), "SimpleMapping")
        self.assertEqual(simple["filename"], "helpers/enrich.ktr")
        inp = parse_step_metadata(ET.fromstring(_MAPPING_INPUT_XML), "MappingInput")
        self.assertEqual(len(inp["fields"]), 3)
        out = parse_step_metadata(ET.fromstring(_MAPPING_OUTPUT_XML), "MappingOutput")
        self.assertEqual(out["output_columns"][0], "out_status")


class TestMappingHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertEqual(len(MAPPING_HANDLERS), 4)
        for step_type in (
            "Mapping",
            "SimpleMapping",
            "MappingInput",
            "MappingOutput",
            "Mapping (sub-transformation)",
            "Simple Mapping (sub-transformation)",
            "Mapping Input Specification",
            "Mapping Output Specification",
        ):
            conv = self.registry.get_converter(step_type)
            self.assertIsNotNone(conv, msg=step_type)
            self.assertNotEqual(type(conv).__name__, "FallbackHandler", msg=step_type)

    def test_mapping_generates_reusable_invocation(self):
        ctx = _ctx(_MAPPING_XML, "Mapping", "Call Sub")
        outcome = self.registry.convert_step("Mapping", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), msg=code)
        self.assertIn("_invoke_mapping_", code)
        self.assertIn("run_child_map", code)
        self.assertIn("withColumnRenamed('id', 'cust_id')", code)
        self.assertIn("withColumnRenamed('name', 'cust_name')", code)
        self.assertIn("THRESHOLD", code)
        self.assertIn("_pentaho_mapping_stack", code)
        self.assertIn("Circular mapping reference", code)
        self.assertIn("createOrReplaceTempView('_pentaho_mapping_input')", code)
        self.assertIn("preserved.filename=", code)
        self.assertIn("inherit_all", code.lower().replace(" ", ""))
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_mapping_missing_child_warns(self):
        ctx = _ctx("""
        <step>
          <mappings>
            <input><mapping><main_path>Y</main_path></mapping></input>
            <output><mapping><main_path>Y</main_path></mapping></output>
            <parameters><inherit_all_vars>Y</inherit_all_vars></parameters>
          </mappings>
        </step>
        """, "Mapping", "Broken Map")
        outcome = self.registry.convert_step("Mapping", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: missing sub-transformation", code)
        self.assertEqual(outcome.status, "partial")

    def test_simple_mapping_generates_helper(self):
        ctx = _ctx(_SIMPLE_MAPPING_XML, "SimpleMapping", "Simple Call")
        outcome = self.registry.convert_step("SimpleMapping", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), msg=code)
        self.assertIn("_invoke_mapping_", code)
        self.assertIn("run_enrich", code)
        self.assertIn("withColumnRenamed('sku', 'product_sku')", code)
        self.assertIn("withColumnRenamed('list_price', 'price')", code)
        self.assertIn("REGION", code)
        self.assertIn("Variable inheritance disabled", code)
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_mapping_input_schema_validation(self):
        ctx = _ctx(_MAPPING_INPUT_XML, "MappingInput", "Mapping input", with_input=False)
        outcome = self.registry.convert_step("MappingInput", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), msg=code)
        self.assertIn("_pentaho_mapping_input", code)
        self.assertIn("_required_", code)
        self.assertIn("missing required fields", code)
        self.assertIn(".cast('int')", code)
        self.assertIn("lit('0')", code)
        self.assertIn("select_unspecified", code)
        self.assertIn("sorted(", code)
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_mapping_output_projection(self):
        ctx = _ctx(_MAPPING_OUTPUT_XML, "MappingOutput", "Mapping output")
        outcome = self.registry.convert_step("MappingOutput", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), msg=code)
        self.assertIn("_pentaho_mapping_output", code)
        self.assertIn("withColumnRenamed('out_score', 'score')", code)
        self.assertIn("select(", code)
        self.assertIn("schema mismatch", code)
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_mapping_input_empty_fields(self):
        ctx = _ctx("""
        <step>
          <fields><select_unspecified>N</select_unspecified></fields>
        </step>
        """, "MappingInput", "Empty Spec", with_input=False)
        outcome = self.registry.convert_step("MappingInput", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), msg=code)
        self.assertIn("_pentaho_mapping_input", code)


class TestMappingEdgeCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_missing_parameter_value_warning(self):
        ctx = _ctx("""
        <step>
          <filename>child.ktr</filename>
          <mappings>
            <input><mapping><main_path>Y</main_path></mapping></input>
            <output><mapping><main_path>Y</main_path></mapping></output>
            <parameters>
              <variablemapping><variable>NEED_ME</variable><input></input></variablemapping>
              <inherit_all_vars>Y</inherit_all_vars>
            </parameters>
          </mappings>
        </step>
        """, "Mapping", "Param Gap")
        outcome = self.registry.convert_step("Mapping", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("missing parameter values", code)
        self.assertIn("NEED_ME", code)

    def test_multi_input_limitation(self):
        ctx = _ctx("""
        <step>
          <filename>multi.ktr</filename>
          <allow_multiple_input>Y</allow_multiple_input>
          <mappings>
            <input>
              <mapping>
                <input_step>Input</input_step>
                <output_step>In1</output_step>
                <main_path>Y</main_path>
              </mapping>
              <mapping>
                <input_step>Side</input_step>
                <output_step>In2</output_step>
                <main_path>N</main_path>
              </mapping>
            </input>
            <output><mapping><main_path>Y</main_path></mapping></output>
            <parameters><inherit_all_vars>Y</inherit_all_vars></parameters>
          </mappings>
        </step>
        """, "Mapping", "Multi In", extra_inputs=["Side"])
        outcome = self.registry.convert_step("Mapping", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("Multi-input Mapping", code)
        self.assertIn("_pentaho_mapping_input_In2", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), msg=code)
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_repository_reference_and_extras(self):
        cfg = parse_mapping_config(ET.fromstring("""
        <step>
          <specification_method>rep_name</specification_method>
          <trans_name>ChildEnrich</trans_name>
          <directory_path>/home/etl</directory_path>
          <trans_object_id>abc-123</trans_object_id>
          <custom_flag>Y</custom_flag>
          <mappings>
            <input><mapping><main_path>Y</main_path></mapping></input>
            <output><mapping><main_path>Y</main_path></mapping></output>
            <parameters><inherit_all_vars>N</inherit_all_vars></parameters>
          </mappings>
        </step>
        """))
        self.assertEqual(cfg["specification_method"], "rep_name")
        self.assertEqual(cfg["trans_name"], "ChildEnrich")
        self.assertEqual(cfg["directory_path"], "/home/etl")
        self.assertEqual(cfg["trans_object_id"], "abc-123")
        self.assertEqual(cfg["extras"]["custom_flag"], "Y")
        self.assertFalse(cfg["inherit_all_variables"])

    def test_mapping_input_reads_named_secondary_view(self):
        ctx = _ctx(_MAPPING_INPUT_XML, "MappingInput", "Mapping input", with_input=False)
        outcome = self.registry.convert_step("MappingInput", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("_pentaho_mapping_input_Mapping_input", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), msg=code)

    def test_circular_guard_and_logging_helper(self):
        ctx = _ctx(_MAPPING_XML, "Mapping", "Call Sub")
        outcome = self.registry.convert_step("Mapping", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("_pentaho_mapping_stack", code)
        self.assertIn("Circular mapping reference", code)
        self.assertIn("globals().get('run_child_map')", code)
        self.assertEqual(outcome.status, "converted")
        self.assertGreaterEqual(outcome.semantic_score, 0.9)


if __name__ == "__main__":
    unittest.main()
