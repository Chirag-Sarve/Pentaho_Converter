"""Tests for Pentaho Experimental category: SFTP Put + Script."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_experimental_script_config,
    parse_sftp_put_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.experimental_handlers import (
    EXPERIMENTAL_HANDLERS,
    ExperimentalScriptHandler,
    SFTPPutHandler,
)
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
    trans = PentahoTransformation(name="Trans", file_path=Path("t.ktr"))
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


class TestSFTPPutVerification(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_property_matrix_and_secrets(self):
        xml = """
        <step>
          <servername>${HOST}</servername>
          <serverport>2222</serverport>
          <username>${USER}</username>
          <password>super-secret</password>
          <usekeyfilename>Y</usekeyfilename>
          <keyfilename>/keys/id_rsa</keyfilename>
          <keyfilepass>phrase-secret</keyfilepass>
          <sourceFileFieldName>local_path</sourceFileFieldName>
          <remoteDirectoryFieldName>remote_dir</remoteDirectoryFieldName>
          <remoteFilenameFieldName>remote_name</remoteFilenameFieldName>
          <createRemoteFolder>Y</createRemoteFolder>
          <compression>zlib</compression>
          <timeout>45</timeout>
          <overwrite>N</overwrite>
          <append>Y</append>
          <binary>N</binary>
          <inputIsStream>N</inputIsStream>
          <successWhenNoFile>Y</successWhenNoFile>
          <aftersftpput>nothing</aftersftpput>
        </step>
        """
        cfg = parse_sftp_put_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["host"], "${HOST}")
        self.assertEqual(cfg["port"], "2222")
        self.assertEqual(cfg["username"], "${USER}")
        self.assertEqual(cfg["authentication_method"], "private_key")
        self.assertTrue(cfg["password_configured"])
        self.assertTrue(cfg["passphrase_configured"])
        self.assertIn("private_key", cfg["private_key_secret_ref"])
        self.assertNotIn("super-secret", str(cfg))
        self.assertNotIn("phrase-secret", str(cfg))
        self.assertFalse(cfg["overwrite"])
        self.assertTrue(cfg["append"])
        self.assertEqual(cfg["transfer_mode"], "ascii")
        self.assertEqual(cfg["timeout"], "45")
        self.assertTrue(cfg["create_remote_directory"])

        ctx = _ctx(xml, "SFTPPut", "Put", parameters={"HOST": "sftp.local", "USER": "etl"})
        lines, status = SFTPPutHandler().generate_code(ctx)
        code = "\n".join(lines)
        self.assertEqual(status, "partial")
        self.assertIn("paramiko", code)
        self.assertIn("sftp.local", code)
        self.assertIn("etl", code)
        self.assertIn("_sftp_env_subst_", code)
        self.assertIn("put_mode = 'a'", code)
        self.assertIn("45.0", code)
        self.assertIn("zlib", code)
        self.assertIn("ASCII", code)
        self.assertIn("dbutils.secrets.get", code)
        self.assertIn("key='passphrase'", code)
        self.assertIn("key='private_key'", code)
        self.assertNotIn("super-secret", code)
        self.assertNotIn("phrase-secret", code)
        self.assertIn("logging.getLogger", code)
        self.assertIn("AuthenticationException", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_overwrite_disabled_and_missing_dir(self):
        ctx = _ctx(
            """
            <step>
              <servername>h</servername>
              <username>u</username>
              <password>x</password>
              <sourceFileFieldName>f</sourceFileFieldName>
              <remoteDirectoryFieldName>d</remoteDirectoryFieldName>
              <overwrite>N</overwrite>
              <createRemoteFolder>N</createRemoteFolder>
            </step>
            """,
            "SFTPPut",
            "Put2",
        )
        code = "\n".join(SFTPPutHandler().generate_code(ctx)[0])
        self.assertIn("put_mode = 'x'", code)
        self.assertIn("missing remote directory", code)
        self.assertIn("FileExistsError", code)

    def test_registry_and_validator(self):
        self.assertTrue(any(isinstance(h, SFTPPutHandler) for h in EXPERIMENTAL_HANDLERS))
        outcome = self.registry.convert_step(
            "SFTPPut",
            _ctx(
                """
                <step>
                  <servername>h</servername>
                  <username>u</username>
                  <password>x</password>
                  <localfilename>/tmp/a.csv</localfilename>
                  <sftpdirectory>/r</sftpdirectory>
                </step>
                """,
                "SFTPPut",
                "P",
                with_input=False,
            ),
        )
        validator = get_validator("SFTPPut")
        result = validator.validate(
            _ctx(
                "<step><servername>h</servername><username>u</username>"
                "<password>x</password><localfilename>/tmp/a.csv</localfilename></step>",
                "SFTPPut",
                "P",
                with_input=False,
            ),
            {},
            outcome.code_lines,
        )
        self.assertFalse(result.errors, result.errors)


class TestExperimentalScript(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_handler_registered_and_not_swallowed_by_scriptvaluemod(self):
        self.assertTrue(ExperimentalScriptHandler().can_handle("Script"))
        self.assertFalse(ExperimentalScriptHandler().can_handle("ScriptValueMod"))
        outcome = self.registry.convert_step(
            "Script",
            _ctx(
                """
                <step>
                  <name>Calc.py</name>
                  <type>Script</type>
                  <jsScripts>
                    <jsScript>
                      <jsScript_type>0</jsScript_type>
                      <jsScript_name>transform.py</jsScript_name>
                      <jsScript_script>df = df  # identity</jsScript_script>
                    </jsScript>
                  </jsScripts>
                  <fields>
                    <field>
                      <name>out_col</name>
                      <rename>out_col</rename>
                      <type>String</type>
                      <replace>N</replace>
                    </field>
                  </fields>
                </step>
                """,
                "Script",
                "Calc.py",
            ),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("Experimental Script", code)
        self.assertIn("python", code.lower())
        self.assertIn("out_col", code)
        self.assertIn("WARNING", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_parse_language_from_extension(self):
        cfg = parse_experimental_script_config(ET.fromstring(textwrap.dedent("""
        <step>
          <name>S1</name>
          <jsScripts>
            <jsScript>
              <jsScript_type>0</jsScript_type>
              <jsScript_name>body.groovy</jsScript_name>
              <jsScript_script>return 1</jsScript_script>
            </jsScript>
          </jsScripts>
          <fields><field><name>v</name><type>Integer</type><replace>Y</replace></field></fields>
        </step>
        """).strip()))
        self.assertEqual(cfg["script_language"], "groovy")
        self.assertEqual(len(cfg["fields"]), 1)
        meta = parse_step_metadata(
            ET.fromstring("<step><jsScripts><jsScript>"
                          "<jsScript_type>0</jsScript_type>"
                          "<jsScript_name>a.js</jsScript_name>"
                          "<jsScript_script>var x=1;</jsScript_script>"
                          "</jsScript></jsScripts></step>"),
            "Script",
        )
        self.assertEqual(meta.get("script_language"), "javascript")

    def test_javascript_reuses_approximate_conversion(self):
        ctx = _ctx(
            """
            <step>
              <name>J</name>
              <type>Script</type>
              <jsScripts>
                <jsScript>
                  <jsScript_type>0</jsScript_type>
                  <jsScript_name>t.js</jsScript_name>
                  <jsScript_script>var upper = name.toUpperCase();</jsScript_script>
                </jsScript>
              </jsScripts>
              <fields>
                <field><name>upper</name><rename>upper</rename><type>String</type></field>
              </fields>
            </step>
            """,
            "Script",
            "J",
        )
        code = "\n".join(ExperimentalScriptHandler().generate_code(ctx)[0])
        self.assertIn("javascript", code)
        self.assertIn("withColumn", code)
        self.assertTrue(_syntax_ok(ExperimentalScriptHandler().generate_code(ctx)[0]), code)

    def test_ruby_unsupported_preserved(self):
        ctx = _ctx(
            """
            <step>
              <jsScripts>
                <jsScript>
                  <jsScript_type>0</jsScript_type>
                  <jsScript_name>calc.rb</jsScript_name>
                  <jsScript_script>puts 1</jsScript_script>
                </jsScript>
              </jsScripts>
              <fields><field><name>x</name><type>String</type></field></fields>
            </step>
            """,
            "Script",
            "R",
        )
        code = "\n".join(ExperimentalScriptHandler().generate_code(ctx)[0])
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("ruby", code.lower())
        self.assertIn("puts 1", code)

    def test_validator(self):
        register_builtin_validators()
        self.assertIsNotNone(get_validator("Script"))


if __name__ == "__main__":
    unittest.main()
