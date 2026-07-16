"""Tests for Closure Generator, Get ID from Slave Server, and XSL Transformation."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_closure_generator_config,
    parse_get_slave_sequence_config,
    parse_step_metadata,
    parse_xslt_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    input_columns: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
    trans = PentahoTransformation(name="Trans", file_path=Path("t.ktr"))
    trans.steps = [inp, step]
    hops = [PentahoHop(from_name="Input", to_name=step_name)]
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


class TestParsersSpecialTransforms(unittest.TestCase):
    def test_closure_parse(self):
        xml = """<step>
          <parent_id_field>parent_id</parent_id_field>
          <child_id_field>child_id</child_id_field>
          <distance_field>dist</distance_field>
          <is_root_zero>Y</is_root_zero>
        </step>"""
        cfg = parse_closure_generator_config(ET.fromstring(xml))
        self.assertEqual(cfg["parent_id_field"], "parent_id")
        self.assertEqual(cfg["child_id_field"], "child_id")
        self.assertEqual(cfg["distance_field"], "dist")
        self.assertTrue(cfg["is_root_zero"])
        self.assertEqual(cfg["max_depth"], 50)

    def test_get_slave_parse(self):
        xml = """<step>
          <valuename>slave_id</valuename>
          <slave>carte01</slave>
          <seqname>global_seq</seqname>
          <increment>500</increment>
        </step>"""
        cfg = parse_get_slave_sequence_config(ET.fromstring(xml))
        self.assertEqual(cfg["value_name"], "slave_id")
        self.assertEqual(cfg["slave_server"], "carte01")
        self.assertEqual(cfg["sequence_name"], "global_seq")
        self.assertEqual(cfg["increment"], 500)

    def test_xslt_parse(self):
        xml = """<step>
          <xslfilename>/data/transform.xsl</xslfilename>
          <fieldname>xml_payload</fieldname>
          <resultfieldname>xml_out</resultfieldname>
          <xslfilefielduse>N</xslfilefielduse>
          <xslfactory>JAXP</xslfactory>
          <parameters>
            <parameter><name>p1</name><field>f1</field></parameter>
          </parameters>
          <outputproperties>
            <outputproperty><name>indent</name><value>yes</value></outputproperty>
          </outputproperties>
        </step>"""
        cfg = parse_xslt_config(ET.fromstring(xml))
        self.assertEqual(cfg["xsl_filename"], "/data/transform.xsl")
        self.assertEqual(cfg["field_name"], "xml_payload")
        self.assertEqual(cfg["result_field"], "xml_out")
        self.assertFalse(cfg["xsl_file_field_use"])
        self.assertEqual(cfg["parameters"][0]["name"], "p1")
        self.assertEqual(cfg["output_properties"][0]["value"], "yes")


class TestHandlersSpecialTransforms(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_closure_generator_emits_hierarchy(self):
        ctx = _ctx(
            """
            <step>
              <name>CG</name>
              <type>ClosureGenerator</type>
              <parent_id_field>parent_id</parent_id_field>
              <child_id_field>child_id</child_id_field>
              <distance_field>distance</distance_field>
              <is_root_zero>Y</is_root_zero>
            </step>
            """,
            "ClosureGenerator",
            "CG",
            input_columns=["parent_id", "child_id"],
        )
        outcome = self.registry.convert_step("ClosureGenerator", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("_cg_edges_", code)
        self.assertIn("unionByName", code)
        self.assertIn("is_root_zero=Y", code)
        self.assertIn('alias("distance")', code)

    def test_closure_missing_keys_does_not_fail(self):
        ctx = _ctx(
            """
            <step>
              <name>CG</name>
              <type>ClosureGenerator</type>
              <distance_field>distance</distance_field>
            </step>
            """,
            "ClosureGenerator",
            "CG",
        )
        outcome = self.registry.convert_step("ClosureGenerator", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertIn(outcome.status, ("partial", "converted"))
        self.assertIn("WARNING: missing parent_id_field", code)

    def test_get_slave_sequence_preserves_and_warns(self):
        ctx = _ctx(
            """
            <step>
              <name>GID</name>
              <type>GetSlaveSequence</type>
              <valuename>id</valuename>
              <slave>${SLAVE}</slave>
              <seqname>seq_a</seqname>
              <increment>10000</increment>
            </step>
            """,
            "GetSlaveSequence",
            "GID",
            input_columns=["a"],
        )
        outcome = self.registry.convert_step("GetSlaveSequence", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertEqual(outcome.status, "partial")
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("slave='${SLAVE}'", code)
        self.assertIn('withColumn("id"', code)
        self.assertIn("row_number()", code)

    def test_get_id_from_slave_server_alias(self):
        ctx = _ctx(
            """
            <step>
              <name>GID2</name>
              <type>Get ID from Slave Server</type>
              <valuename>uid</valuename>
              <slave>srv</slave>
              <seqname>s</seqname>
              <increment>1</increment>
            </step>
            """,
            "Get ID from Slave Server",
            "GID2",
            input_columns=["x"],
        )
        outcome = self.registry.convert_step("Get ID from Slave Server", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertEqual(outcome.status, "partial")
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("slave='srv'", code)

    def test_xslt_static_stylesheet(self):
        ctx = _ctx(
            """
            <step>
              <name>X</name>
              <type>XSLT</type>
              <xslfilename>/dbfs/styles/a.xsl</xslfilename>
              <fieldname>xml_in</fieldname>
              <resultfieldname>xml_out</resultfieldname>
              <xslfilefielduse>N</xslfilefielduse>
              <xslfactory>JAXP</xslfactory>
              <parameters/>
              <outputproperties/>
            </step>
            """,
            "XSLT",
            "X",
            input_columns=["xml_in"],
        )
        outcome = self.registry.convert_step("XSLT", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertIn("lxml", code)
        self.assertIn("etree.XSLT", code)
        self.assertIn('/dbfs/styles/a.xsl', code)
        self.assertIn('withColumn("xml_out"', code)
        self.assertIn("preserved.xslfilename", code)

    def test_xslt_missing_path_partial(self):
        ctx = _ctx(
            """
            <step>
              <name>X</name>
              <type>XSLT</type>
              <fieldname>xml_in</fieldname>
              <resultfieldname>xml_out</resultfieldname>
              <xslfilefielduse>N</xslfilefielduse>
            </step>
            """,
            "XSLT",
            "X",
            input_columns=["xml_in"],
        )
        outcome = self.registry.convert_step("XSLT", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertEqual(outcome.status, "partial")
        self.assertIn("invalid/missing XSL path", code)

    def test_xslt_saxon_warns(self):
        ctx = _ctx(
            """
            <step>
              <name>X</name>
              <type>XSL Transformation</type>
              <xslfilename>/a.xsl</xslfilename>
              <fieldname>xml_in</fieldname>
              <resultfieldname>out</resultfieldname>
              <xslfactory>SAXON</xslfactory>
            </step>
            """,
            "XSL Transformation",
            "X",
            input_columns=["xml_in"],
        )
        outcome = self.registry.convert_step("XSL Transformation", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("SAXON", code)
        self.assertIn("libxslt/lxml", code)
        self.assertEqual(outcome.status, "partial")

    def test_xslt_field_stylesheet_and_params(self):
        ctx = _ctx(
            """
            <step>
              <name>X</name>
              <type>XSLT</type>
              <fieldname>xml_in</fieldname>
              <resultfieldname>xml_out</resultfieldname>
              <xslfilefielduse>Y</xslfilefielduse>
              <xslfilefield>xsl_path</xslfilefield>
              <xslfieldisafile>Y</xslfieldisafile>
              <xslfactory>JAXP</xslfactory>
              <parameters>
                <parameter><name>p1</name><field>f1</field></parameter>
              </parameters>
              <outputproperties>
                <outputproperty><name>indent</name><value>yes</value></outputproperty>
              </outputproperties>
            </step>
            """,
            "XSLT",
            "X",
            input_columns=["xml_in", "xsl_path", "f1"],
        )
        outcome = self.registry.convert_step("XSLT", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertEqual(outcome.status, "partial")
        self.assertIn('col("xsl_path")', code)
        self.assertIn('col("f1")', code)
        self.assertIn("pretty_print", code)
        self.assertIn("preserved.parameters", code)

    def test_closure_empty_stream(self):
        step_el = ET.fromstring(
            "<step><parent_id_field>p</parent_id_field>"
            "<child_id_field>c</child_id_field>"
            "<distance_field>d</distance_field></step>"
        )
        step = PentahoStep(name="CG", step_type="ClosureGenerator", attributes={}, raw_element=step_el)
        step.parsed_config = parse_step_metadata(step_el, "ClosureGenerator")
        trans = PentahoTransformation(name="Trans", file_path=Path("t.ktr"))
        trans.steps = [step]
        dag = StepDAG(trans.steps, [])
        ctx = StepContext(
            transformation=trans,
            step=step,
            dag=dag,
            df_variable_map={"CG": "df_CG"},
        )
        outcome = self.registry.convert_step("ClosureGenerator", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertIn("df_CG =", code)

    def test_display_names_resolve_to_handlers(self):
        mapping = {
            "Sort rows": "SortRows",
            "Select values": "SelectValues",
            "Unique rows (HashSet)": "Unique",
            "Add constants": "Constant",
            "Value Mapper": "ValueMapper",
            "Strings cut": "StringOperations",
            "Set field value to a constant": "SetValueConstant",
            "Closure Generator": "ClosureGenerator",
            "Get ID from slave server": "GetSlaveSequence",
            "XSL Transformation": "Xslt",
        }
        for display, expected in mapping.items():
            conv = self.registry.get_converter(display)
            name = getattr(conv, "converter_name", type(conv).__name__)
            self.assertEqual(
                name, expected, f"{display!r} resolved to {name}, expected {expected}"
            )


if __name__ == "__main__":
    unittest.main()
