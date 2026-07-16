"""Tests for Pentaho Server (BA Server) transformation migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_call_endpoint_config,
    parse_get_session_variable_config,
    parse_set_session_variable_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.pentaho_server_handlers import (
    CallEndpointHandler,
    GetSessionVariablesHandler,
    PENTAHO_SERVER_HANDLERS,
    SetSessionVariablesHandler,
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
    return StepContext(transformation=trans, dag=dag, step=step, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


_CALL_XML = """
<step>
  <name>CallBA</name>
  <type>CallEndpointStep</type>
  <serverUrl>http://localhost:8080/pentaho</serverUrl>
  <userName>admin</userName>
  <password>Encrypted secretpwd</password>
  <isBypassingAuthentication>N</isBypassingAuthentication>
  <moduleName>platform</moduleName>
  <isModuleFromField>N</isModuleFromField>
  <endpointPath>/version/show</endpointPath>
  <httpMethod>GET</httpMethod>
  <isEndpointFromField>N</isEndpointFromField>
  <resultField>api_result</resultField>
  <statusCodeField>http_status</statusCodeField>
  <responseTimeField>elapsed_ms</responseTimeField>
  <timeout>15</timeout>
  <retries>2</retries>
  <retryDelay>1</retryDelay>
  <verifySSL>Y</verifySSL>
  <fields>
    <field>
      <fieldName>depth_field</fieldName>
      <parameter>depth</parameter>
      <defaultValue>1</defaultValue>
    </field>
  </fields>
  <headers>
    <header>
      <name>Accept</name>
      <value>application/json</value>
    </header>
  </headers>
</step>
"""

_GET_SESSION_XML = """
<step>
  <name>GetSess</name>
  <type>GetSessionVariableStep</type>
  <fields>
    <field>
      <name>folder</name>
      <variable>${scheduler_folder}</variable>
      <type>String</type>
      <format/>
      <currency/>
      <decimal/>
      <group/>
      <length>-1</length>
      <precision>-1</precision>
      <trim_type>both</trim_type>
      <default_value>/home</default_value>
    </field>
    <field>
      <name>show_dialog</name>
      <variable>showOverrideDialog</variable>
      <type>String</type>
      <default_value>N</default_value>
      <trim_type>none</trim_type>
    </field>
  </fields>
</step>
"""

_SET_SESSION_XML = """
<step>
  <name>SetSess</name>
  <type>SetSessionVariableStep</type>
  <use_formatting>Y</use_formatting>
  <fields>
    <field>
      <name>folder_path</name>
      <variable>scheduler_folder</variable>
      <default_value>/default</default_value>
    </field>
    <field>
      <name/>
      <variable>${showOverrideDialog}</variable>
      <default_value>Y</default_value>
    </field>
  </fields>
</step>
"""


class TestCallEndpointParser(unittest.TestCase):
    def test_parse_full_config_redacts_password(self):
        cfg = parse_call_endpoint_config(ET.fromstring(textwrap.dedent(_CALL_XML).strip()))
        self.assertEqual(cfg["server_url"], "http://localhost:8080/pentaho")
        self.assertEqual(cfg["username"], "admin")
        self.assertTrue(cfg["password_configured"])
        self.assertIn("dbutils.secrets.get", cfg["password_secret_ref"])
        self.assertNotIn("secretpwd", str(cfg))
        self.assertEqual(cfg["extras"].get("password"), "<redacted>")
        self.assertEqual(cfg["module_name"], "platform")
        self.assertEqual(cfg["endpoint_path"], "/version/show")
        self.assertEqual(cfg["http_method"], "GET")
        self.assertEqual(cfg["result_field"], "api_result")
        self.assertEqual(cfg["status_code_field"], "http_status")
        self.assertEqual(cfg["response_time_field"], "elapsed_ms")
        self.assertEqual(len(cfg["parameters"]), 1)
        self.assertEqual(cfg["parameters"][0]["parameter"], "depth")
        self.assertEqual(cfg["headers"][0]["name"], "Accept")
        self.assertEqual(cfg["timeout"], "15")
        self.assertEqual(cfg["retries"], "2")
        self.assertTrue(cfg["verify_ssl"])
        self.assertFalse(cfg["use_session_authentication"])

    def test_parse_session_auth_flag(self):
        xml = """
        <step>
          <serverUrl>https://ba.example/pentaho</serverUrl>
          <isBypassingAuthentication>Y</isBypassingAuthentication>
          <moduleName>platform</moduleName>
          <endpointPath>/session/userName</endpointPath>
          <httpMethod>GET</httpMethod>
        </step>
        """
        cfg = parse_call_endpoint_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertTrue(cfg["use_session_authentication"])


class TestSessionVariableParsers(unittest.TestCase):
    def test_parse_get_session_variables(self):
        cfg = parse_get_session_variable_config(
            ET.fromstring(textwrap.dedent(_GET_SESSION_XML).strip())
        )
        self.assertEqual(len(cfg["fields"]), 2)
        self.assertEqual(cfg["fields"][0]["name"], "folder")
        self.assertEqual(cfg["fields"][0]["variable"], "${scheduler_folder}")
        self.assertEqual(cfg["fields"][0]["default_value"], "/home")
        self.assertEqual(cfg["fields"][0]["trim_type"], "both")
        self.assertEqual(cfg["scope"], "BA_SESSION")
        self.assertFalse(cfg["variable_inheritance"])

    def test_parse_set_session_variables(self):
        cfg = parse_set_session_variable_config(
            ET.fromstring(textwrap.dedent(_SET_SESSION_XML).strip())
        )
        self.assertTrue(cfg["use_formatting"])
        self.assertTrue(cfg["overwrite"])
        self.assertEqual(cfg["fields"][0]["field_name"], "folder_path")
        self.assertEqual(cfg["fields"][0]["variable_name"], "scheduler_folder")
        self.assertIn("scheduler_folder", cfg["variable_names"])
        self.assertEqual(cfg["scope"], "BA_SESSION")


class TestPentahoServerConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()

    def test_handlers_registered(self):
        self.assertEqual(len(PENTAHO_SERVER_HANDLERS), 3)
        types = set()
        for h in PENTAHO_SERVER_HANDLERS:
            types |= set(h._TYPES)
        self.assertIn("callendpointstep", types)
        self.assertIn("getsessionvariablestep", types)
        self.assertIn("setsessionvariablestep", types)

    def test_call_endpoint_generates_requests(self):
        ctx = _ctx(_CALL_XML, "CallEndpointStep", "CallBA")
        lines, status = CallEndpointHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("requests", text)
        self.assertIn("api_result", text)
        self.assertIn("http_status", text)
        self.assertTrue("api" in text.lower())
        self.assertTrue(_syntax_ok(lines), text)
        self.assertIn(status, ("converted", "partial"))

    def test_call_endpoint_missing_url_partial(self):
        xml = """
        <step>
          <moduleName>platform</moduleName>
          <endpointPath>/x</endpointPath>
          <httpMethod>GET</httpMethod>
        </step>
        """
        ctx = _ctx(xml, "CallEndpointStep", "CallBA")
        lines, status = CallEndpointHandler().generate_code(ctx)
        self.assertEqual(status, "partial")
        self.assertIn("missing endpoint server URL", "\n".join(lines))

    def test_call_endpoint_session_auth_warning(self):
        xml = """
        <step>
          <serverUrl>http://localhost:8080/pentaho</serverUrl>
          <isBypassingAuthentication>Y</isBypassingAuthentication>
          <moduleName>platform</moduleName>
          <endpointPath>/version/show</endpointPath>
          <httpMethod>GET</httpMethod>
        </step>
        """
        ctx = _ctx(xml, "CallEndpointStep", "CallBA")
        lines, status = CallEndpointHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertEqual(status, "partial")
        self.assertIn("session auth", text.lower())
        self.assertTrue(_syntax_ok(lines), text)

    def test_call_endpoint_url_build_platform_and_plugin(self):
        from pentaho_converter.pentaho_server_converter import _build_endpoint_url

        self.assertEqual(
            _build_endpoint_url("http://h/pentaho", "platform", "/version/show"),
            "http://h/pentaho/api/version/show",
        )
        self.assertEqual(
            _build_endpoint_url("http://h/pentaho/", "scheduler", "jobs"),
            "http://h/pentaho/plugin/scheduler/api/jobs",
        )

    def test_call_endpoint_preserves_headers_timeout_retry_ssl_auth(self):
        ctx = _ctx(_CALL_XML, "CallEndpointStep", "CallBA")
        lines, _status = CallEndpointHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("Accept", text)
        self.assertIn("application/json", text)
        self.assertIn("_retries = 2", text)
        self.assertIn("_verify_ssl = True", text)
        self.assertIn("HTTPBasicAuth", text)
        self.assertIn("dbutils.secrets.get", text)
        self.assertIn("preserved.timeout", text)
        self.assertIn("depth", text)
        self.assertTrue(_syntax_ok(lines), text)

    def test_call_endpoint_post_body_and_invalid_url(self):
        xml = """
        <step>
          <serverUrl>localhost:8080/pentaho</serverUrl>
          <moduleName>platform</moduleName>
          <endpointPath>/repo/files</endpointPath>
          <httpMethod>POST</httpMethod>
          <requestBody>{"a":1}</requestBody>
          <contentType>application/json</contentType>
          <trustAllSSL>Y</trustAllSSL>
          <resultField>body</resultField>
        </step>
        """
        ctx = _ctx(xml, "CallEndpointStep", "CallBA", with_input=False)
        lines, status = CallEndpointHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertEqual(status, "partial")
        self.assertIn("missing scheme", text)
        self.assertIn('{"a":1}', text)
        self.assertIn("application/json", text)
        self.assertIn("_verify_ssl = False", text)
        self.assertTrue(_syntax_ok(lines), text)

    def test_call_endpoint_variable_substitution(self):
        xml = """
        <step>
          <serverUrl>${BA_URL}</serverUrl>
          <moduleName>platform</moduleName>
          <endpointPath>/version/show</endpointPath>
          <httpMethod>GET</httpMethod>
        </step>
        """
        ctx = _ctx(
            xml, "CallEndpointStep", "CallBA", with_input=False,
            parameters={"BA_URL": "https://ba.example/pentaho"},
        )
        lines, _status = CallEndpointHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("https://ba.example/pentaho", text)
        self.assertIn("_subst", text)
        self.assertTrue(_syntax_ok(lines), text)

    def test_call_endpoint_from_field_preserves_method_casing(self):
        xml = """
        <step>
          <serverUrl>http://localhost:8080/pentaho</serverUrl>
          <moduleName>module_col</moduleName>
          <endpointPath>path_col</endpointPath>
          <httpMethod>method_col</httpMethod>
          <isEndpointFromField>Y</isEndpointFromField>
          <isModuleFromField>Y</isModuleFromField>
        </step>
        """
        cfg = parse_call_endpoint_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["http_method"], "method_col")
        self.assertTrue(cfg["endpoint_from_field"])
        ctx = _ctx(xml, "CallEndpointStep", "CallBA")
        lines, _status = CallEndpointHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("method_col", text)
        self.assertIn("module_col", text)
        self.assertIn("path_col", text)
        self.assertTrue(_syntax_ok(lines), text)

    def test_get_session_variables_lookup(self):
        ctx = _ctx(_GET_SESSION_XML, "GetSessionVariableStep", "GetSess", with_input=False)
        lines, status = GetSessionVariablesHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("_pentaho_session_vars", text)
        self.assertIn("scheduler_folder", text)
        self.assertIn("withColumn", text)
        self.assertIn("LIMITATION", text)
        self.assertIn("preserved.fields", text)
        self.assertIn("BA_SESSION", text)
        self.assertEqual(status, "partial")
        self.assertTrue(_syntax_ok(lines), text)

    def test_get_session_missing_variable_uses_default(self):
        xml = """
        <step>
          <fields>
            <field>
              <name>x</name>
              <variable/>
              <default_value>fallback</default_value>
              <type>String</type>
            </field>
          </fields>
        </step>
        """
        ctx = _ctx(xml, "GetSessionVariableStep", "GetSess", with_input=False)
        lines, status = GetSessionVariablesHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("missing/undefined session variable", text)
        self.assertIn("fallback", text)
        self.assertIn("withColumn('x'", text.replace('"', "'"))
        self.assertIn("withColumn", text)
        self.assertEqual(status, "partial")
        self.assertTrue(_syntax_ok(lines), text)

    def test_set_session_variables_write(self):
        ctx = _ctx(_SET_SESSION_XML, "SetSessionVariableStep", "SetSess")
        lines, status = SetSessionVariablesHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("_pentaho_session_vars", text)
        self.assertIn("spark.conf.set", text)
        self.assertIn("os.environ", text)
        self.assertIn("scheduler_folder", text)
        self.assertIn("more than one row", text)
        self.assertIn("preserved.fields", text)
        self.assertIn("BA_SESSION", text)
        self.assertEqual(status, "partial")
        self.assertTrue(_syntax_ok(lines), text)

    def test_set_session_overwrite_false(self):
        xml = """
        <step>
          <overwrite>N</overwrite>
          <fields>
            <field>
              <name>v</name>
              <variable>my_sess</variable>
              <default_value>d</default_value>
            </field>
          </fields>
        </step>
        """
        cfg = parse_set_session_variable_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertFalse(cfg["overwrite"])
        ctx = _ctx(xml, "SetSessionVariableStep", "SetSess")
        lines, _status = SetSessionVariablesHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("overwrite=False", text)
        self.assertIn("not in _pentaho_session_vars", text)
        self.assertTrue(_syntax_ok(lines), text)

    def test_set_session_no_input_uses_defaults(self):
        ctx = _ctx(_SET_SESSION_XML, "SetSessionVariableStep", "SetSess", with_input=False)
        lines, status = SetSessionVariablesHandler().generate_code(ctx)
        text = "\n".join(lines)
        self.assertIn("no input row", text)
        self.assertIn("/default", text)
        self.assertEqual(status, "partial")
        self.assertTrue(_syntax_ok(lines), text)

    def test_display_name_aliases(self):
        for stype in ("Call Endpoint", "Get Session Variables", "Set Session Variables"):
            self.assertTrue(
                any(h.can_handle(stype) for h in PENTAHO_SERVER_HANDLERS),
                msg=stype,
            )

    def test_registry_round_trip(self):
        registry = build_default_registry()
        for xml, stype, name, handler_cls in (
            (_CALL_XML, "CallEndpointStep", "CallBA", CallEndpointHandler),
            (_GET_SESSION_XML, "GetSessionVariableStep", "GetSess", GetSessionVariablesHandler),
            (_SET_SESSION_XML, "SetSessionVariableStep", "SetSess", SetSessionVariablesHandler),
        ):
            ctx = _ctx(xml, stype, name, with_input=(stype != "GetSessionVariableStep"))
            outcome = registry.convert_step(stype, ctx)
            self.assertIsNotNone(outcome)
            self.assertTrue(outcome.code_lines, msg=stype)
            self.assertTrue(_syntax_ok(outcome.code_lines), "\n".join(outcome.code_lines))
            self.assertTrue(handler_cls().can_handle(stype))

    def test_validators(self):
        for stype in (
            "CallEndpointStep",
            "GetSessionVariableStep",
            "SetSessionVariableStep",
        ):
            v = get_validator(stype)
            self.assertIsNotNone(v, msg=stype)


if __name__ == "__main__":
    unittest.main()
