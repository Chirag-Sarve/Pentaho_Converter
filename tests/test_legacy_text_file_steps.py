"""Tests for Pentaho Legacy Text File Input / Output migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_text_file_input_config,
    parse_text_file_output_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = False) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    # Populate parsed_config the same way the transformation parser does.
    from pentaho_converter.step_xml import parse_step_metadata

    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="LegacyTextTrans", file_path=Path("legacy_text.ktr"))
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


class TestOldTextFileInput(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_handler_registered(self):
        conv = self.registry.get_converter("OldTextFileInput")
        self.assertIsNotNone(conv)
        self.assertTrue(conv.can_handle("OldTextFileInput"))

    def test_parses_complete_legacy_xml(self):
        xml = """
        <step>
          <accept_filenames>Y</accept_filenames>
          <accept_field>path</accept_field>
          <separator>;</separator>
          <enclosure>"</enclosure>
          <escapechar>\\</escapechar>
          <header>Y</header>
          <nr_headerlines>2</nr_headerlines>
          <footer>Y</footer>
          <nr_footerlines>1</nr_footerlines>
          <noempty>Y</noempty>
          <encoding>ISO-8859-1</encoding>
          <error_ignored>Y</error_ignored>
          <buffer_size>50000</buffer_size>
          <lazy_conversion>Y</lazy_conversion>
          <file>
            <name>/data/in/</name>
            <filemask>.*\\.csv</filemask>
            <exclude_filemask>tmp.*</exclude_filemask>
            <include_subfolders>Y</include_subfolders>
            <type>CSV</type>
            <compression>GZip</compression>
          </file>
          <filters>
            <filter>
              <filter_string>#</filter_string>
              <filter_position>0</filter_position>
              <filter_is_last_line>N</filter_is_last_line>
              <filter_is_positive>N</filter_is_positive>
            </filter>
          </filters>
          <fields>
            <field>
              <name>amount</name>
              <type>Number</type>
              <format>#,##0.00</format>
              <currency>$</currency>
              <decimal>.</decimal>
              <group>,</group>
              <nullif></nullif>
              <ifnull>0</ifnull>
              <length>12</length>
              <precision>2</precision>
            </field>
            <field>
              <name>when</name>
              <type>Date</type>
              <format>yyyy-MM-dd</format>
            </field>
          </fields>
        </step>
        """
        cfg = parse_text_file_input_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["filename"], "/data/in/")
        self.assertEqual(cfg["separator"], ";")
        self.assertEqual(cfg["filemask"], r".*\.csv")
        self.assertEqual(cfg["include_subfolders"], "Y")
        self.assertEqual(cfg["compression"], "GZip")
        self.assertEqual(cfg["accept_field"], "path")
        self.assertEqual(len(cfg["filters"]), 1)
        self.assertEqual(cfg["fields"][0]["decimal"], ".")
        self.assertEqual(cfg["fields"][0]["default"], "0")
        self.assertEqual(cfg["fields"][1]["format"], "yyyy-MM-dd")

    def test_generates_csv_reader(self):
        xml = """
        <step>
          <filename>/data/legacy.csv</filename>
          <separator>,</separator>
          <header>Y</header>
          <enclosure>"</enclosure>
          <encoding>UTF-8</encoding>
          <fields>
            <field><name>id</name><type>Integer</type></field>
            <field><name>name</name><type>String</type><nullif>N/A</nullif></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "OldTextFileInput", _ctx(xml, "OldTextFileInput", "LegacyIn")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("type: OldTextFileInput", code)
        self.assertIn(".csv(", code)
        self.assertIn("legacy.csv", code)
        self.assertIn("nullValue", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_preserves_unsupported_accept_filenames(self):
        xml = """
        <step>
          <accept_filenames>Y</accept_filenames>
          <accept_field>filepath</accept_field>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <fields><field><name>a</name><type>String</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "OldTextFileInput", _ctx(xml, "OldTextFileInput", "AcceptIn")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("accept_filenames", code)
        self.assertEqual(outcome.status, "partial")

    def test_error_ignored_uses_permissive_mode(self):
        xml = """
        <step>
          <filename>/data/a.csv</filename>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <error_ignored>Y</error_ignored>
          <fields><field><name>a</name><type>String</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "OldTextFileInput", _ctx(xml, "OldTextFileInput", "ErrIn")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn('mode", "PERMISSIVE"', code.replace("'", '"'))
        self.assertIn("error", code.lower())

    def test_include_filename_and_rownum(self):
        xml = """
        <step>
          <filename>/data/a.csv</filename>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <include>Y</include>
          <include_field>src_file</include_field>
          <rownum>Y</rownum>
          <rownum_field>rn</rownum_field>
          <fields><field><name>a</name><type>String</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "OldTextFileInput", _ctx(xml, "OldTextFileInput", "MetaIn")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("src_file", code)
        self.assertIn("input_file_name()", code)
        self.assertIn("monotonically_increasing_id()", code)

    def test_compression_encoding_schema_and_field_formats(self):
        xml = """
        <step>
          <separator>,</separator>
          <header>Y</header>
          <encoding>ISO-8859-1</encoding>
          <file>
            <name>/data/legacy.csv.gz</name>
            <type>CSV</type>
            <compression>GZip</compression>
          </file>
          <fields>
            <field>
              <name>amt</name>
              <type>Number</type>
              <format>#,##0.00</format>
              <currency>$</currency>
              <decimal>.</decimal>
              <group>,</group>
              <precision>2</precision>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "OldTextFileInput", _ctx(xml, "OldTextFileInput", "CompIn")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("gzip", code)
        self.assertIn("ISO-8859-1", code)
        self.assertIn(".schema(", code)
        self.assertIn("amt DOUBLE", code.replace("'", ""))
        self.assertIn("preserved.field_format", code)
        self.assertIn("currency", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_variable_substitution_and_edge_notes(self):
        xml = """
        <step>
          <filename>${DATA_DIR}/rows.csv</filename>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <fields><field><name>a</name><type>String</type></field></fields>
        </step>
        """
        ctx = _ctx(xml, "OldTextFileInput", "VarIn")
        ctx.transformation.parameters = {"DATA_DIR": "/mnt/data"}
        outcome = self.registry.convert_step("OldTextFileInput", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertIn("rows.csv", code)
        self.assertIn("missing/empty/corrupt", code)

    def test_reuses_text_file_input_handler(self):
        from pentaho_converter.steps.input_handlers import TextFileInputHandler

        handler = TextFileInputHandler()
        self.assertTrue(handler.can_handle("TextFileInput"))
        self.assertTrue(handler.can_handle("OldTextFileInput"))
        # Single handler class — no duplicate Legacy converter type.
        modern = self.registry.get_converter("TextFileInput")
        legacy = self.registry.get_converter("OldTextFileInput")
        self.assertIs(type(modern), type(legacy))
        self.assertTrue(modern.can_handle("OldTextFileInput"))
        self.assertTrue(legacy.can_handle("TextFileInput"))


class TestTextFileOutputLegacy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_handler_registered(self):
        conv = self.registry.get_converter("TextFileOutputLegacy")
        self.assertIsNotNone(conv)
        self.assertTrue(conv.can_handle("TextFileOutputLegacy"))

    def test_parses_is_command_and_naming(self):
        xml = """
        <step>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <file>
            <name>/out/data</name>
            <extention>csv</extention>
            <append>Y</append>
            <is_command>Y</is_command>
            <add_date>Y</add_date>
            <add_time>N</add_time>
            <splitevery>1000</splitevery>
            <create_parent_folder>Y</create_parent_folder>
            <fast_dump>N</fast_dump>
          </file>
          <fields>
            <field>
              <name>amt</name>
              <type>Number</type>
              <format>0.00</format>
              <decimal>.</decimal>
              <group>,</group>
              <nullif></nullif>
            </field>
          </fields>
        </step>
        """
        cfg = parse_text_file_output_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["filename"], "/out/data")
        self.assertEqual(cfg["extension"], "csv")
        self.assertTrue(cfg["file_as_command"])
        self.assertTrue(cfg["append"])
        self.assertTrue(cfg["add_date"])
        self.assertEqual(cfg["split_every"], "1000")
        self.assertEqual(cfg["output_fields"][0]["decimal"], ".")

    def test_generates_csv_writer_with_command_warning(self):
        xml = """
        <step>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <enclosure>"</enclosure>
          <file>
            <name>/data/out/legacy</name>
            <extention>csv</extention>
            <is_command>Y</is_command>
            <append>N</append>
          </file>
          <fields>
            <field><name>id</name><type>Integer</type></field>
            <field><name>name</name><type>String</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "TextFileOutputLegacy",
            _ctx(xml, "TextFileOutputLegacy", "LegacyOut", with_input=True),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("type: TextFileOutputLegacy", code)
        self.assertIn(".csv(", code)
        self.assertIn("is_command", code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_modern_text_file_output_still_works(self):
        xml = """
        <step>
          <filename>/data/out/customers</filename>
          <extension>csv</extension>
          <separator>;</separator>
          <header>Y</header>
          <enclosure>"</enclosure>
          <encoding>UTF-8</encoding>
        </step>
        """
        outcome = self.registry.convert_step(
            "TextFileOutput",
            _ctx(xml, "TextFileOutput", "TFO", with_input=True),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".csv(", code)
        self.assertNotIn("is_command", code)

    def test_compression_footer_header_and_ended_line(self):
        xml = """
        <step>
          <separator>,</separator>
          <header>Y</header>
          <footer>Y</footer>
          <encoding>UTF-8</encoding>
          <compression>GZip</compression>
          <endedLine>EOF</endedLine>
          <file>
            <name>/out/legacy</name>
            <extention>csv</extention>
            <is_command>N</is_command>
            <create_parent_folder>Y</create_parent_folder>
          </file>
          <fields>
            <field>
              <name>amt</name>
              <type>Number</type>
              <format>0.00</format>
              <decimal>.</decimal>
              <nullif>NA</nullif>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "TextFileOutputLegacy",
            _ctx(xml, "TextFileOutputLegacy", "FmtOut", with_input=True),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("gzip", code)
        self.assertIn('option("header", True)', code)
        self.assertIn("footer lines are not written", code)
        self.assertIn("endedLine", code)
        self.assertIn("preserved.field_format", code)
        self.assertIn("create_parent_folder", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_reuses_text_file_output_handler(self):
        from pentaho_converter.steps.output_handlers import TextFileOutputHandler

        handler = TextFileOutputHandler()
        self.assertTrue(handler.can_handle("TextFileOutput"))
        self.assertTrue(handler.can_handle("TextFileOutputLegacy"))
        modern = self.registry.get_converter("TextFileOutput")
        legacy = self.registry.get_converter("TextFileOutputLegacy")
        self.assertIs(type(modern), type(legacy))


if __name__ == "__main__":
    unittest.main()
