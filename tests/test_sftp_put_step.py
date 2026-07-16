"""Tests for Experimental SFTP Put transformation migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import parse_sftp_put_config, parse_step_metadata
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.experimental_handlers import EXPERIMENTAL_HANDLERS, SFTPPutHandler
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str = "SFTPPut",
    step_name: str = "SFTPPut",
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


_FULL_XML = """
<step>
  <name>UploadFiles</name>
  <type>SFTPPut</type>
  <servername>${SFTP_HOST}</servername>
  <serverport>22</serverport>
  <username>sftpuser</username>
  <password>Encrypted 2be98afc86aa7f2e4bb18bd63c99dbdde</password>
  <sourceFileFieldName>local_path</sourceFileFieldName>
  <remoteDirectoryFieldName>remote_dir</remoteDirectoryFieldName>
  <remoteFilenameFieldName>remote_name</remoteFilenameFieldName>
  <inputIsStream>N</inputIsStream>
  <addFilenameResut>Y</addFilenameResut>
  <usekeyfilename>N</usekeyfilename>
  <keyfilename/>
  <keyfilepass/>
  <compression>zlib</compression>
  <proxyType>HTTP</proxyType>
  <proxyHost>proxy.example.com</proxyHost>
  <proxyPort>8080</proxyPort>
  <proxyUsername>proxyuser</proxyUsername>
  <proxyPassword>Encrypted proxypass</proxyPassword>
  <createRemoteFolder>Y</createRemoteFolder>
  <aftersftpput>nothing</aftersftpput>
  <destinationfolderFieldName/>
  <createdestinationfolder>N</createdestinationfolder>
  <timeout>60</timeout>
  <overwrite>Y</overwrite>
  <append>N</append>
  <binary>Y</binary>
  <successWhenNoFile>Y</successWhenNoFile>
  <loglevel>Basic</loglevel>
  <error>
    <target_step>ErrorHandler</target_step>
  </error>
</step>
"""


class TestSFTPPutParser(unittest.TestCase):
    def test_parse_full_config_and_redacts_secrets(self):
        cfg = parse_sftp_put_config(ET.fromstring(textwrap.dedent(_FULL_XML).strip()))
        self.assertEqual(cfg["host"], "${SFTP_HOST}")
        self.assertEqual(cfg["port"], "22")
        self.assertEqual(cfg["username"], "sftpuser")
        self.assertEqual(cfg["authentication_method"], "password")
        self.assertTrue(cfg["password_configured"])
        self.assertIn("dbutils.secrets.get", cfg["password_secret_ref"])
        self.assertNotIn("2be98afc", str(cfg))
        self.assertEqual(cfg["extras"].get("password"), "<redacted>")
        self.assertEqual(cfg["extras"].get("proxyPassword"), "<redacted>")
        self.assertEqual(cfg["local_filename_field"], "local_path")
        self.assertEqual(cfg["remote_directory_field"], "remote_dir")
        self.assertEqual(cfg["remote_filename_field"], "remote_name")
        self.assertTrue(cfg["create_remote_directory"])
        self.assertTrue(cfg["overwrite"])
        self.assertFalse(cfg["append"])
        self.assertEqual(cfg["transfer_mode"], "binary")
        self.assertEqual(cfg["timeout"], "60")
        self.assertEqual(cfg["proxy_host"], "proxy.example.com")
        self.assertTrue(cfg["proxy_password_configured"])
        self.assertEqual(cfg["compression"], "zlib")
        self.assertTrue(cfg["variable_substitution"])
        self.assertTrue(cfg["success_when_no_file"])
        self.assertEqual(cfg["error_target_step"], "ErrorHandler")
        self.assertEqual(cfg["logging_level"], "Basic")
        self.assertTrue(cfg["add_filename_to_result"])

    def test_parse_private_key_auth(self):
        xml = """
        <step>
          <servername>sftp.example.com</servername>
          <username>keyuser</username>
          <usekeyfilename>Y</usekeyfilename>
          <keyfilename>/secrets/id_rsa</keyfilename>
          <keyfilepass>passphrase-secret</keyfilepass>
          <sourceFileFieldName>path</sourceFileFieldName>
          <remoteDirectoryFieldName>rdir</remoteDirectoryFieldName>
        </step>
        """
        cfg = parse_sftp_put_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["authentication_method"], "private_key")
        self.assertTrue(cfg["use_private_key"])
        self.assertEqual(cfg["key_file"], "/secrets/id_rsa")
        self.assertTrue(cfg["passphrase_configured"])
        self.assertIn("passphrase", cfg["passphrase_secret_ref"])
        self.assertNotIn("passphrase-secret", str(cfg.values()))

    def test_parse_job_entry_aliases(self):
        xml = """
        <step>
          <servername>host</servername>
          <username>u</username>
          <localdirectory>/data/out</localdirectory>
          <sftpdirectory>/incoming</sftpdirectory>
          <wildcard>*.csv</wildcard>
          <createRemoteFolder>Y</createRemoteFolder>
          <aftersftpput>delete</aftersftpput>
          <successWhenNoFile>Y</successWhenNoFile>
        </step>
        """
        cfg = parse_sftp_put_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["local_directory"], "/data/out")
        self.assertEqual(cfg["remote_directory"], "/incoming")
        self.assertEqual(cfg["wildcard"], "*.csv")
        self.assertEqual(cfg["after_sftp_put"], "delete")

    def test_structured_metadata_dispatch(self):
        el = ET.fromstring(textwrap.dedent(_FULL_XML).strip())
        meta = parse_step_metadata(el, "SFTPPut")
        self.assertEqual(meta.get("local_filename_field"), "local_path")
        self.assertTrue(meta.get("password_configured"))


class TestSFTPPutHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_handler_registered(self):
        self.assertTrue(any(isinstance(h, SFTPPutHandler) for h in EXPERIMENTAL_HANDLERS))
        self.assertTrue(SFTPPutHandler().can_handle("SFTP Put"))
        self.assertTrue(SFTPPutHandler().can_handle("SFTPPut"))

    def test_generate_paramiko_with_secrets(self):
        ctx = _ctx(_FULL_XML, parameters={"SFTP_HOST": "sftp.prod.local"})
        outcome = self.registry.convert_step("SFTPPut", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("paramiko", code)
        self.assertIn("dbutils.secrets.get", code)
        self.assertIn("scope='sftp'", code)
        self.assertNotIn("2be98afc", code)
        self.assertNotIn("Encrypted proxypass", code)
        self.assertIn("local_path", code)
        self.assertIn("create_remote", code)
        self.assertIn("sftp.prod.local", code)  # variable substitution
        self.assertIn("WARNING", code)  # proxy / limitations
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertIn(outcome.status, ("converted", "partial", "partially_supported"))

    def test_missing_credentials_warning(self):
        ctx = _ctx(
            """
            <step>
              <servername>host</servername>
              <username>u</username>
              <sourceFileFieldName>f</sourceFileFieldName>
            </step>
            """
        )
        lines, status = SFTPPutHandler().generate_code(ctx)
        code = "\n".join(lines)
        self.assertIn("missing credentials", code.lower())
        self.assertEqual(status, "partial")

    def test_key_auth_code(self):
        ctx = _ctx(
            """
            <step>
              <servername>host</servername>
              <username>u</username>
              <usekeyfilename>Y</usekeyfilename>
              <keyfilename>/dbfs/keys/id_rsa</keyfilename>
              <sourceFileFieldName>path</sourceFileFieldName>
              <remoteDirectoryFieldName>rdir</remoteDirectoryFieldName>
              <createRemoteFolder>Y</createRemoteFolder>
            </step>
            """
        )
        lines, status = SFTPPutHandler().generate_code(ctx)
        code = "\n".join(lines)
        self.assertIn("use_key = True", code)
        self.assertIn("/dbfs/keys/id_rsa", code)
        self.assertIn("RSAKey.from_private_key_file", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_static_local_no_input(self):
        ctx = _ctx(
            """
            <step>
              <servername>host</servername>
              <username>u</username>
              <password>x</password>
              <localfilename>/tmp/a.csv</localfilename>
              <sftpdirectory>/remote</sftpdirectory>
              <remotefilename>a.csv</remotefilename>
            </step>
            """,
            with_input=False,
        )
        lines, status = SFTPPutHandler().generate_code(ctx)
        code = "\n".join(lines)
        self.assertIn("/tmp/a.csv", code)
        self.assertIn("createDataFrame", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_validator_accepts_generated_code(self):
        ctx = _ctx(_FULL_XML)
        outcome = self.registry.convert_step("SFTPPut", ctx)
        validator = get_validator("SFTPPut")
        self.assertIsNotNone(validator)
        result = validator.validate(ctx, ctx.step.parsed_config or {}, outcome.code_lines)
        self.assertFalse(result.errors, result.errors)
        self.assertIn("sftp_put", result.properties_converted)


if __name__ == "__main__":
    unittest.main()
