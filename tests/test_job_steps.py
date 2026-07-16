"""Tests for Pentaho Job-category step migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_files_from_result_config,
    parse_files_to_result_config,
    parse_get_variable_config,
    parse_rows_from_result_config,
    parse_rows_to_result_config,
    parse_set_variable_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.job_handlers import JOB_HANDLERS
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    parameters: dict[str, str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="JobTrans", file_path=Path("job.ktr"))
    if parameters:
        trans.parameters = dict(parameters)
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


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestJobParsers(unittest.TestCase):
    def test_rows_to_result_parse(self):
        cfg = parse_rows_to_result_config(ET.fromstring("<step></step>"))
        self.assertEqual(cfg["result_buffer"], "rows")
        self.assertTrue(cfg["preserve_order"])

    def test_rows_from_result_parse(self):
        cfg = parse_rows_from_result_config(ET.fromstring("""
        <step>
          <fields>
            <field><name>id</name><type>Integer</type><length>9</length><precision>0</precision></field>
            <field><name>name</name><type>String</type><length>50</length><precision>-1</precision></field>
          </fields>
        </step>
        """))
        self.assertEqual(len(cfg["fields"]), 2)
        self.assertEqual(cfg["fields"][0]["name"], "id")
        self.assertEqual(cfg["output_columns"], ["id", "name"])

    def test_files_from_result_parse(self):
        cfg = parse_files_from_result_config(ET.fromstring("<step></step>"))
        self.assertIn("filename", cfg["output_columns"])
        self.assertEqual(cfg["result_buffer"], "files")

    def test_files_to_result_parse(self):
        cfg = parse_files_to_result_config(ET.fromstring("""
        <step>
          <filename_field>path_col</filename_field>
          <file_type>GENERAL</file_type>
        </step>
        """))
        self.assertEqual(cfg["filename_field"], "path_col")
        self.assertEqual(cfg["file_type"], "GENERAL")

    def test_set_variable_parse(self):
        cfg = parse_set_variable_config(ET.fromstring("""
        <step>
          <fields>
            <field>
              <field_name>status</field_name>
              <variable_name>VAR_STATUS</variable_name>
              <variable_type>PARENT_JOB</variable_type>
              <default_value>unknown</default_value>
            </field>
            <field>
              <field_name>cnt</field_name>
              <variable_name>VAR_CNT</variable_name>
              <variable_type>JVM</variable_type>
              <default_value>0</default_value>
            </field>
          </fields>
          <use_formatting>Y</use_formatting>
        </step>
        """))
        self.assertEqual(len(cfg["fields"]), 2)
        self.assertEqual(cfg["fields"][0]["variable_name"], "VAR_STATUS")
        self.assertEqual(cfg["fields"][0]["variable_type"], "PARENT_JOB")
        self.assertTrue(cfg["use_formatting"])

    def test_get_variable_parse(self):
        cfg = parse_get_variable_config(ET.fromstring("""
        <step>
          <fields>
            <field>
              <name>env_path</name>
              <variable>${Internal.Entry.Current.Directory}/data</variable>
              <type>String</type>
              <trim_type>both</trim_type>
            </field>
            <field>
              <name>threshold</name>
              <variable>${THRESHOLD}</variable>
              <type>Integer</type>
              <length>9</length>
            </field>
          </fields>
        </step>
        """))
        self.assertEqual(len(cfg["fields"]), 2)
        self.assertIn("${THRESHOLD}", cfg["fields"][1]["variable"])
        self.assertEqual(cfg["fields"][0]["trim_type"], "both")


class TestJobHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        types = set()
        for h in JOB_HANDLERS:
            types |= set(getattr(h, "_TYPES", set()))
        for required in (
            "rowstoresult", "copyrowstoresult",
            "rowsfromresult", "getrowsfromresult",
            "filesfromresult", "getfilesfromresult",
            "filestoresult", "setfilesinresult",
            "setvariable", "setvariables",
            "getvariable", "getvariables",
        ):
            self.assertIn(required, types)

    def test_copy_rows_to_result(self):
        outcome = self.registry.convert_step(
            "RowsToResult",
            _ctx("<step></step>", "RowsToResult", "CopyRows"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_pentaho_result_rows", code)
        self.assertIn("Copy Rows to Result", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial", "partially_supported"))

    def test_get_rows_from_result(self):
        xml = """
        <step>
          <fields>
            <field><name>id</name><type>Integer</type></field>
            <field><name>name</name><type>String</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "RowsFromResult",
            _ctx(xml, "RowsFromResult", "GetRows", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_pentaho_result_rows", code)
        self.assertIn("id", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_get_files_from_result(self):
        outcome = self.registry.convert_step(
            "FilesFromResult",
            _ctx("<step></step>", "FilesFromResult", "GetFiles", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_pentaho_result_files", code)
        self.assertIn("filename", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_set_files_in_result(self):
        xml = """
        <step>
          <filename_field>filepath</filename_field>
          <file_type>GENERAL</file_type>
        </step>
        """
        outcome = self.registry.convert_step(
            "FilesToResult",
            _ctx(xml, "FilesToResult", "SetFiles"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_pentaho_result_files", code)
        self.assertIn("filepath", code)
        self.assertIn("GENERAL", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_set_variables_jvm_and_parent_scope(self):
        xml = """
        <step>
          <fields>
            <field>
              <field_name>status</field_name>
              <variable_name>VAR_STATUS</variable_name>
              <variable_type>JVM</variable_type>
              <default_value>ok</default_value>
            </field>
            <field>
              <field_name>region</field_name>
              <variable_name>VAR_REGION</variable_name>
              <variable_type>PARENT_JOB</variable_type>
              <default_value>NA</default_value>
            </field>
          </fields>
          <use_formatting>N</use_formatting>
        </step>
        """
        outcome = self.registry.convert_step(
            "SetVariable",
            _ctx(xml, "SetVariable", "SetVars"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("spark.conf.set", code)
        self.assertIn("VAR_STATUS", code)
        self.assertIn("PARENT_JOB", code)
        self.assertIn("LIMITATION", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial", "partially_supported"))

    def test_get_variables_lookup_chain(self):
        xml = """
        <step>
          <fields>
            <field>
              <name>threshold</name>
              <variable>${THRESHOLD}</variable>
              <type>Integer</type>
            </field>
            <field>
              <name>datapath</name>
              <variable>${BASE}/in</variable>
              <type>String</type>
              <trim_type>both</trim_type>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "GetVariable",
            _ctx(xml, "GetVariable", "GetVars", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("withColumn", code)
        self.assertIn("THRESHOLD", code)
        self.assertIn("widgets", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_ui_aliases(self):
        aliases = [
            ("Copy Rows to Result", "<step></step>", True),
            ("Get Rows from Result", """
              <step><fields><field><name>a</name><type>String</type></field></fields></step>
            """, False),
            ("Get Files from Result", "<step></step>", False),
            ("Set Files in Result", """
              <step><filename_field>f</filename_field><file_type>GENERAL</file_type></step>
            """, True),
            ("Set Variables", """
              <step><fields><field>
                <field_name>x</field_name><variable_name>X</variable_name>
                <variable_type>JVM</variable_type><default_value>1</default_value>
              </field></fields></step>
            """, True),
            ("Get Variables", """
              <step><fields><field>
                <name>y</name><variable>${Y}</variable><type>String</type>
              </field></fields></step>
            """, False),
        ]
        for step_type, xml, with_input in aliases:
            with self.subTest(step_type=step_type):
                outcome = self.registry.convert_step(
                    step_type,
                    _ctx(xml, step_type, step_type.replace(" ", "_"), with_input=with_input),
                )
                self.assertTrue(outcome.code_lines)
                self.assertTrue(_syntax_ok(outcome.code_lines), msg=outcome.code_lines)

    def test_validators_present(self):
        for st in (
            "RowsToResult", "RowsFromResult", "FilesFromResult",
            "FilesToResult", "SetVariable", "GetVariable",
        ):
            self.assertIsNotNone(get_validator(st), msg=st)

    def test_empty_set_files_missing_field(self):
        outcome = self.registry.convert_step(
            "FilesToResult",
            _ctx("<step></step>", "FilesToResult", "EmptySetFiles"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_copy_rows_empty_input(self):
        outcome = self.registry.convert_step(
            "RowsToResult",
            _ctx("<step><custom_meta>Y</custom_meta></step>", "RowsToResult", "EmptyCopy", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_pentaho_result_rows", code)
        self.assertIn("preserved.extras.custom_meta", code)
        self.assertIn(outcome.status, ("partial", "partially_supported"))
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_get_rows_empty_result_schema(self):
        outcome = self.registry.convert_step(
            "RowsFromResult",
            _ctx("<step></step>", "RowsFromResult", "EmptyGetRows", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_get_files_dedup_and_order(self):
        outcome = self.registry.convert_step(
            "FilesFromResult",
            _ctx("<step></step>", "FilesFromResult", "DedupFiles", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_seen_", code)
        self.assertIn("preserving first-seen order", code)
        self.assertIn("isinstance(_f, dict)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_set_files_null_and_duplicate_paths(self):
        xml = """
        <step>
          <filename_field>filepath</filename_field>
          <file_type>ERRORLINE</file_type>
        </step>
        """
        outcome = self.registry.convert_step(
            "FilesToResult",
            _ctx(xml, "FilesToResult", "SetFilesNull"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("skip null / empty", code)
        self.assertIn("ERRORLINE", code)
        self.assertIn("_pentaho_result_files.append", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_set_variables_defaults_and_duplicate_names(self):
        xml = """
        <step>
          <fields>
            <field>
              <field_name>a</field_name>
              <variable_name>DUP</variable_name>
              <variable_type>JVM</variable_type>
              <default_value>first</default_value>
            </field>
            <field>
              <field_name>b</field_name>
              <variable_name>DUP</variable_name>
              <variable_type>ROOT_JOB</variable_type>
              <default_value>second</default_value>
            </field>
          </fields>
          <use_formatting>Y</use_formatting>
        </step>
        """
        outcome = self.registry.convert_step(
            "SetVariable",
            _ctx(xml, "SetVariable", "DupVars"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("duplicate variable", code)
        self.assertIn("ROOT_JOB", code)
        self.assertIn("LIMITATION", code)
        self.assertIn("use_formatting", code)
        self.assertIn(outcome.status, ("partial", "partially_supported"))
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_get_variables_missing_empty_and_template(self):
        xml = """
        <step>
          <fields>
            <field>
              <name>threshold</name>
              <variable>${THRESHOLD}</variable>
              <type>Integer</type>
              <length>9</length>
              <precision>0</precision>
            </field>
            <field>
              <name>datapath</name>
              <variable>${BASE}/data</variable>
              <type>String</type>
              <trim_type>both</trim_type>
              <currency>$</currency>
            </field>
            <field>
              <name>blank</name>
              <variable></variable>
              <type>String</type>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "GetVariable",
            _ctx(
                xml,
                "GetVariable",
                "GetEdge",
                with_input=False,
                parameters={"THRESHOLD": "42", "BASE": "/tmp"},
            ),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("THRESHOLD", code)
        self.assertIn("42", code)  # param default baked into resolve
        self.assertIn("WARNING: missing variable", code)
        self.assertIn("preserved.field.datapath.currency", code)
        self.assertIn("widgets", code)
        self.assertIn(outcome.status, ("partial", "partially_supported", "converted"))
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_structured_metadata_roundtrip(self):
        from pentaho_converter.step_xml import is_structured_step_type

        for st in (
            "RowsToResult", "RowsFromResult", "FilesFromResult",
            "FilesToResult", "SetVariable", "GetVariable",
        ):
            self.assertTrue(is_structured_step_type(st), msg=st)


if __name__ == "__main__":
    unittest.main()
