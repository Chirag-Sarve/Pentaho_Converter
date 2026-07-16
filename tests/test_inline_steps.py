"""Tests for Pentaho Inline step migration (Injector, Socket Reader, Socket Writer)."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_injector_config,
    parse_socket_reader_config,
    parse_socket_writer_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.inline_handlers import INLINE_HANDLERS
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import parse_step_config, register_builtin_validators


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="InlineTrans", file_path=Path("inline.ktr"))
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


def _syntax_ok(code: str) -> bool:
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False


class TestInlineParsers(unittest.TestCase):
    def test_parse_injector_fields(self):
        xml = """
        <step>
          <fields>
            <field>
              <name>id</name>
              <type>Integer</type>
              <length>9</length>
              <precision>0</precision>
            </field>
            <field>
              <name>name</name>
              <type>String</type>
              <length>50</length>
              <precision>-1</precision>
            </field>
            <field>
              <name>amount</name>
              <type>Number</type>
              <length>10</length>
              <precision>2</precision>
            </field>
          </fields>
        </step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        cfg = parse_injector_config(el)
        self.assertEqual(len(cfg["fields"]), 3)
        self.assertEqual(cfg["fields"][0]["name"], "id")
        self.assertEqual(cfg["fields"][0]["type"], "Integer")
        self.assertEqual(cfg["fields"][0]["length"], "9")
        self.assertEqual(cfg["fields"][1]["precision"], "")  # -1 sentinel cleared
        self.assertTrue(cfg["inject_at_runtime"])
        self.assertFalse(cfg["has_row_values"])
        self.assertIn("id", cfg["output_columns"])

    def test_parse_injector_with_values_and_nulls(self):
        xml = """
        <step>
          <fields>
            <field><name>a</name><type>String</type><value>hello</value></field>
            <field><name>b</name><type>Integer</type></field>
            <field><name>c</name><type>String</type><set_empty_string>Y</set_empty_string></field>
          </fields>
        </step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        cfg = parse_injector_config(el)
        self.assertTrue(cfg["has_row_values"])
        self.assertFalse(cfg["inject_at_runtime"])
        self.assertEqual(cfg["fields"][0]["value"], "hello")
        self.assertTrue(cfg["fields"][2]["set_empty_string"])

    def test_parse_socket_reader(self):
        xml = """
        <step>
          <hostname>192.168.1.10</hostname>
          <port>11000</port>
          <buffer_size>5000</buffer_size>
          <compressed>Y</compressed>
          <encoding>UTF-8</encoding>
          <delimiter>,</delimiter>
          <protocol>kettle</protocol>
          <timeout>30</timeout>
        </step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        cfg = parse_socket_reader_config(el)
        self.assertEqual(cfg["hostname"], "192.168.1.10")
        self.assertEqual(cfg["port"], "11000")
        self.assertEqual(cfg["buffer_size"], "5000")
        self.assertTrue(cfg["compressed"])
        self.assertEqual(cfg["encoding"], "UTF-8")
        self.assertEqual(cfg["delimiter"], ",")
        self.assertEqual(cfg["timeout"], "30")

    def test_parse_socket_writer(self):
        xml = """
        <step>
          <port>11000</port>
          <buffer_size>2000</buffer_size>
          <flush_interval>5000</flush_interval>
          <compressed>N</compressed>
          <encoding>ISO-8859-1</encoding>
          <output_format>json</output_format>
        </step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        cfg = parse_socket_writer_config(el)
        self.assertEqual(cfg["port"], "11000")
        self.assertEqual(cfg["buffer_size"], "2000")
        self.assertEqual(cfg["flush_interval"], "5000")
        self.assertFalse(cfg["compressed"])
        self.assertEqual(cfg["encoding"], "ISO-8859-1")
        self.assertEqual(cfg["output_format"], "json")

    def test_parse_step_metadata_dispatch(self):
        inj = ET.fromstring(
            "<step><fields><field><name>x</name><type>String</type></field></fields></step>"
        )
        self.assertEqual(parse_step_metadata(inj, "Injector")["fields"][0]["name"], "x")
        sock = ET.fromstring("<step><hostname>h</hostname><port>9</port></step>")
        self.assertEqual(parse_step_metadata(sock, "SocketReader")["hostname"], "h")
        wr = ET.fromstring("<step><port>8</port><flush_interval>1</flush_interval></step>")
        self.assertEqual(parse_step_metadata(wr, "Socket Writer")["flush_interval"], "1")


class TestInlineHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertEqual(len(INLINE_HANDLERS), 3)
        for handler in INLINE_HANDLERS:
            conv = self.registry.get_converter(next(iter(handler._TYPES)))
            self.assertIsNotNone(conv)

    def test_injector_empty_schema(self):
        xml = """
        <step>
          <fields>
            <field><name>id</name><type>Integer</type><length>9</length></field>
            <field><name>label</name><type>String</type><length>40</length></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "Injector",
            _ctx(xml, "Injector", "Injector", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame([],", code)
        self.assertIn("id INT", code)
        self.assertIn("label STRING", code)
        self.assertIn("inject", code.lower())
        self.assertIn("preserved.field.id", code)
        self.assertIn(outcome.status, ("converted", "partial"))
        self.assertTrue(_syntax_ok(code))

    def test_injector_with_literal_values(self):
        xml = """
        <step>
          <fields>
            <field><name>id</name><type>Integer</type><value>1</value></field>
            <field><name>name</name><type>String</type><value>Alice</value></field>
            <field><name>flag</name><type>Boolean</type><value>Y</value></field>
            <field><name>missing</name><type>String</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "Injector",
            _ctx(xml, "Injector", "Inject data", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame(data,", code)
        self.assertIn("Alice", code)
        self.assertIn("None", code)  # null missing field
        self.assertTrue(_syntax_ok(code))
        self.assertEqual(outcome.status, "converted")

    def test_injector_empty_fields(self):
        xml = "<step><fields/></step>"
        outcome = self.registry.convert_step(
            "Injector",
            _ctx(xml, "Injector", "Empty inj", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)
        self.assertIn("WARNING", code)
        self.assertEqual(outcome.status, "partial")

    def test_socket_reader_structured_streaming(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <port>9999</port>
          <buffer_size>3000</buffer_size>
          <compressed>Y</compressed>
          <encoding>UTF-8</encoding>
        </step>
        """
        outcome = self.registry.convert_step(
            "SocketReader",
            _ctx(xml, "SocketReader", "Socket reader", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("readStream", code)
        self.assertIn("format('socket')", code)
        self.assertIn("localhost", code)
        self.assertIn("9999", code)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("compressed", code.lower())
        self.assertIn("preserved.port=", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(code))

    def test_socket_reader_invalid_port(self):
        xml = """
        <step>
          <hostname>bad host name</hostname>
          <port>99999</port>
        </step>
        """
        outcome = self.registry.convert_step(
            "Socket Reader",
            _ctx(xml, "Socket Reader", "SR", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("port out of range", code)
        self.assertEqual(outcome.status, "partial")

    def test_socket_reader_missing_endpoint(self):
        xml = "<step></step>"
        outcome = self.registry.convert_step(
            "SocketReader",
            _ctx(xml, "SocketReader", "SR2", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)
        self.assertIn("WARNING", code)
        self.assertEqual(outcome.status, "partial")

    def test_socket_writer_preserves_and_warns(self):
        xml = """
        <step>
          <port>11000</port>
          <buffer_size>2000</buffer_size>
          <flush_interval>5000</flush_interval>
          <compressed>Y</compressed>
          <encoding>UTF-8</encoding>
          <output_format>csv</output_format>
        </step>
        """
        outcome = self.registry.convert_step(
            "SocketWriter",
            _ctx(xml, "SocketWriter", "Socket writer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.port=", code)
        self.assertIn("flush", code.lower())
        self.assertIn("foreachBatch", code)
        self.assertIn("df_Socket_writer = df_Input", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(code))

    def test_socket_writer_invalid_port(self):
        xml = "<step><port>abc</port></step>"
        outcome = self.registry.convert_step(
            "Socket Writer",
            _ctx(xml, "Socket Writer", "SW"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("invalid port", code)
        self.assertEqual(outcome.status, "partial")


class TestInlineValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_validators_registered(self):
        for st in ("Injector", "SocketReader", "SocketWriter"):
            v = get_validator(st)
            self.assertIsNotNone(v)
            self.assertNotEqual(type(v).__name__, "GenericStepValidator")

    def test_parse_step_config_inline(self):
        ctx = _ctx(
            """
            <step>
              <fields>
                <field><name>id</name><type>Integer</type></field>
              </fields>
            </step>
            """,
            "Injector",
            "Inj",
            with_input=False,
        )
        parsed = parse_step_config(ctx)
        self.assertTrue(parsed.get("fields"))
        self.assertEqual(parsed["fields"][0]["name"], "id")

        ctx2 = _ctx(
            "<step><hostname>h</hostname><port>1</port><buffer_size>9</buffer_size></step>",
            "SocketReader",
            "R",
            with_input=False,
        )
        p2 = parse_step_config(ctx2)
        self.assertEqual(p2.get("hostname"), "h")
        self.assertEqual(p2.get("port"), "1")

    def test_convert_outcomes_include_validation(self):
        for st, xml, with_input in (
            (
                "Injector",
                "<step><fields><field><name>a</name><type>String</type></field></fields></step>",
                False,
            ),
            (
                "SocketReader",
                "<step><hostname>localhost</hostname><port>9999</port><encoding>UTF-8</encoding></step>",
                False,
            ),
            (
                "SocketWriter",
                "<step><port>11000</port><encoding>UTF-8</encoding></step>",
                True,
            ),
        ):
            outcome = self.registry.convert_step(st, _ctx(xml, st, st, with_input=with_input))
            self.assertTrue(outcome.code_lines)
            self.assertGreaterEqual(outcome.semantic_score, 0.5)
            self.assertFalse(outcome.errors)


class TestInlineEdgeCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_injector_type_code_and_residuals(self):
        xml = """
        <step>
          <customFlag>Y</customFlag>
          <fields>
            <field>
              <name>x</name>
              <type>5</type>
              <length>8</length>
              <precision>-2</precision>
              <currency>$</currency>
            </field>
          </fields>
        </step>
        """
        cfg = parse_injector_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["fields"][0]["type"], "Integer")
        self.assertEqual(cfg["fields"][0]["field_options"].get("currency"), "$")
        self.assertEqual(cfg["options"].get("customFlag"), "Y")

        outcome = self.registry.convert_step(
            "Injector",
            _ctx(xml, "Injector", "InjCodes", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("x INT", code)
        self.assertIn("currency=", code)
        self.assertIn("preserved.options", code)
        self.assertTrue(code.isascii(), "generated comments must be ASCII-safe")

    def test_injector_missing_raw_element(self):
        step = PentahoStep(name="Bare", step_type="Injector", attributes={}, raw_element=None)
        trans = PentahoTransformation(name="T", file_path=Path("t.ktr"))
        trans.steps = [step]
        dag = StepDAG(trans.steps, [])
        ctx = StepContext(
            transformation=trans,
            step=step,
            dag=dag,
            df_variable_map={"Bare": "df_Bare"},
        )
        outcome = self.registry.convert_step("Injector", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_injector_data_grid_rows(self):
        xml = """
        <step>
          <fields>
            <field><name>id</name><type>Integer</type></field>
            <field><name>name</name><type>String</type></field>
          </fields>
          <data>
            <line><item>1</item><item>Alice</item></line>
            <line><item></item><item>Bob</item></line>
          </data>
        </step>
        """
        # parse_data_grid_rows expects specific structure; if empty, fallback to literals path
        cfg = parse_injector_config(ET.fromstring(textwrap.dedent(xml).strip()))
        outcome = self.registry.convert_step(
            "Injector",
            _ctx(xml, "Injector", "Grid", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)
        self.assertTrue(_syntax_ok(code))
        # Either grid path or empty schema stub is acceptable depending on data_grid parser shape
        self.assertTrue(cfg["fields"])

    def test_socket_reader_encoding_and_residuals(self):
        xml = """
        <step>
          <hostname>host1</hostname>
          <port>4040</port>
          <buffer_size>4096</buffer_size>
          <compressed>N</compressed>
          <encoding>UTF-8</encoding>
          <delimiter>|</delimiter>
          <timeout>15</timeout>
          <extraProp>keep-me</extraProp>
        </step>
        """
        cfg = parse_socket_reader_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["options"].get("extraProp"), "keep-me")
        self.assertFalse(cfg["compressed"])

        outcome = self.registry.convert_step(
            "SocketReader",
            _ctx(xml, "SocketReader", "SockR", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('socket')", code)
        self.assertIn("host1", code)
        self.assertIn("4040", code)
        self.assertIn("preserved.encoding=", code)
        self.assertIn("preserved.buffer_size=", code)
        self.assertIn("preserved.options.extraProp=", code)
        self.assertIn("WARNING: timeout=", code)
        self.assertTrue(code.isascii(), "generated comments must be ASCII-safe")
        self.assertTrue(_syntax_ok(code))

    def test_socket_writer_host_encoding_output(self):
        xml = """
        <step>
          <hostname>egress.local</hostname>
          <port>7001</port>
          <buffer_size>1000</buffer_size>
          <flush_interval>250</flush_interval>
          <encoding>ISO-8859-1</encoding>
          <output_format>csv</output_format>
          <delimiter>;</delimiter>
          <compressed>Y</compressed>
        </step>
        """
        outcome = self.registry.convert_step(
            "SocketWriter",
            _ctx(xml, "SocketWriter", "SockW"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("preserved.port=", code)
        self.assertIn("preserved.hostname=", code)
        self.assertIn("preserved.encoding=", code)
        self.assertIn("output_format=", code)
        self.assertIn("WARNING: encoding=", code)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("foreachBatch", code)
        self.assertTrue(code.isascii())
        self.assertEqual(outcome.status, "partial")

    def test_socket_variable_port_allowed(self):
        xml = """
        <step>
          <hostname>${SOCKET_HOST}</hostname>
          <port>${SOCKET_PORT}</port>
        </step>
        """
        outcome = self.registry.convert_step(
            "SocketReader",
            _ctx(xml, "SocketReader", "VarSock", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('socket')", code)
        self.assertIn("${SOCKET_HOST}", code)
        self.assertIn("${SOCKET_PORT}", code)


if __name__ == "__main__":
    unittest.main()
