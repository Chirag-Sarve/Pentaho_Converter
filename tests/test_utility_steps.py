"""Tests for Pentaho Utility step migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_change_file_encoding_config,
    parse_clone_row_config,
    parse_delay_row_config,
    parse_edi_to_xml_config,
    parse_exec_process_config,
    parse_ifnull_config,
    parse_mail_config,
    parse_meta_structure_config,
    parse_null_if_config,
    parse_process_files_config,
    parse_ssh_config,
    parse_step_metadata,
    parse_syslog_config,
    parse_table_compare_config,
    parse_write_to_log_config,
    parse_zip_file_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.utility_handlers import UTILITY_HANDLERS
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    input_columns: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="UtilityTrans", file_path=Path("utility.ktr"))
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Input", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    ctx = StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)
    if input_columns:
        ctx.extra["input_columns"] = list(input_columns)
    return ctx


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestUtilityParsers(unittest.TestCase):
    def test_clone_row_parse(self):
        cfg = parse_clone_row_config(ET.fromstring("""
        <step>
          <nrclones>3</nrclones>
          <addcloneflag>Y</addcloneflag>
          <cloneflagfield>is_clone</cloneflagfield>
          <addclonenum>Y</addclonenum>
          <clonenumfield>clone_idx</clonenumfield>
        </step>
        """))
        self.assertEqual(cfg["nr_clones"], 3)
        self.assertTrue(cfg["add_clone_flag"])
        self.assertEqual(cfg["clone_flag_field"], "is_clone")
        self.assertTrue(cfg["add_clone_num"])

    def test_null_if_parse(self):
        cfg = parse_null_if_config(ET.fromstring("""
        <step>
          <fields>
            <field><name>status</name><value>N/A</value></field>
            <field><name>qty</name><value>0</value></field>
          </fields>
        </step>
        """))
        self.assertEqual(len(cfg["fields"]), 2)
        self.assertEqual(cfg["fields"][0]["value"], "N/A")

    def test_ifnull_enhanced_parse(self):
        cfg = parse_ifnull_config(ET.fromstring("""
        <step>
          <selectFields>Y</selectFields>
          <selectValuesType>N</selectValuesType>
          <replaceAllByValue>UNKNOWN</replaceAllByValue>
          <valuetypes>
            <valuetype><name>String</name><value>-</value></valuetype>
          </valuetypes>
          <fields>
            <field><name>a</name><value>x</value><set_empty_string>N</set_empty_string></field>
          </fields>
        </step>
        """))
        self.assertTrue(cfg["select_fields"])
        self.assertEqual(cfg["replace_all"], "UNKNOWN")
        self.assertEqual(cfg["value_types"][0]["type"], "String")
        self.assertEqual(cfg["replacements"][0]["name"], "a")

    def test_delay_parse(self):
        cfg = parse_delay_row_config(ET.fromstring(
            "<step><timeout>500</timeout><scaletime>1</scaletime></step>"
        ))
        self.assertEqual(cfg["timeout"], 500)
        self.assertEqual(cfg["scale_time"], "seconds")

    def test_encoding_parse(self):
        cfg = parse_change_file_encoding_config(ET.fromstring("""
        <step>
          <sourcefilename>/data/in.txt</sourcefilename>
          <targetfilename>/data/out.txt</targetfilename>
          <sourceencoding>ISO-8859-1</sourceencoding>
          <targetencoding>UTF-8</targetencoding>
        </step>
        """))
        self.assertEqual(cfg["source_encoding"], "ISO-8859-1")
        self.assertEqual(cfg["target_encoding"], "UTF-8")

    def test_meta_structure_parse(self):
        cfg = parse_meta_structure_config(ET.fromstring(
            "<step><outputRowcount>Y</outputRowcount><rowcountField>rc</rowcountField></step>"
        ))
        self.assertTrue(cfg["output_rowcount"])
        self.assertEqual(cfg["rowcount_field"], "rc")

    def test_write_to_log_parse(self):
        cfg = parse_write_to_log_config(ET.fromstring("""
        <step>
          <loglevel>Detailed</loglevel>
          <limitRows>Y</limitRows>
          <limitRowsNumber>5</limitRowsNumber>
          <fields><field><name>id</name></field></fields>
        </step>
        """))
        self.assertEqual(cfg["log_level"], "Detailed")
        self.assertEqual(cfg["limit_rows_number"], 5)
        self.assertEqual(cfg["fields"], ["id"])

    def test_table_compare_parse(self):
        cfg = parse_table_compare_config(ET.fromstring("""
        <step>
          <reference_schema>db</reference_schema>
          <reference_table>t1</reference_table>
          <compare_schema>db</compare_schema>
          <compare_table>t2</compare_table>
          <key_fields>
            <key><name>id</name></key>
          </key_fields>
        </step>
        """))
        self.assertEqual(cfg["reference_table"], "t1")
        self.assertEqual(cfg["key_fields"], ["id"])

    def test_zip_process_exec_ssh_mail_syslog_edi_parse(self):
        zip_cfg = parse_zip_file_config(ET.fromstring("""
        <step>
          <zipfilename>/tmp/a.zip</zipfilename>
          <sourcefilenamefield>path</sourcefilenamefield>
          <compressionrate>DEFLATED</compressionrate>
        </step>
        """))
        self.assertEqual(zip_cfg["source_filename_field"], "path")

        pf = parse_process_files_config(ET.fromstring(
            "<step><operation_type>1</operation_type>"
            "<sourcefilenamefield>s</sourcefilenamefield>"
            "<targetfilenamefield>t</targetfilenamefield></step>"
        ))
        self.assertEqual(pf["operation"], "move")

        ep = parse_exec_process_config(ET.fromstring(
            "<step><processfield>cmd</processfield><failwhennonzero>Y</failwhennonzero></step>"
        ))
        self.assertEqual(ep["process_field"], "cmd")
        self.assertTrue(ep["fail_when_nonzero"])

        ssh = parse_ssh_config(ET.fromstring("""
        <step>
          <serverName>host</serverName>
          <userName>user</userName>
          <command>ls</command>
          <usePrivateKey>Y</usePrivateKey>
          <keyFileName>/key</keyFileName>
        </step>
        """))
        self.assertEqual(ssh["server"], "host")
        self.assertTrue(ssh["use_private_key"])

        mail = parse_mail_config(ET.fromstring("""
        <step>
          <server>smtp.example.com</server>
          <destination>a@b.com</destination>
          <subject>Hi</subject>
          <comment>Body</comment>
          <use_auth>Y</use_auth>
          <auth_user>u</auth_user>
          <auth_password>secret</auth_password>
        </step>
        """))
        self.assertEqual(mail["destination"], "a@b.com")
        self.assertEqual(mail["auth_password"], "secret")

        syslog = parse_syslog_config(ET.fromstring(
            "<step><serverName>loghost</serverName><facility>LOCAL0</facility>"
            "<priority>ERROR</priority><messageFieldName>msg</messageFieldName></step>"
        ))
        self.assertEqual(syslog["facility"], "LOCAL0")

        edi = parse_edi_to_xml_config(ET.fromstring(
            "<step><inputfield>edi</inputfield><outputfield>xml</outputfield></step>"
        ))
        self.assertEqual(edi["input_field"], "edi")


class TestUtilityHandlers(unittest.TestCase):
    def setUp(self):
        register_builtin_validators()
        self.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertGreaterEqual(len(UTILITY_HANDLERS), 14)
        for step_type in (
            "CloneRow", "NullIf", "Delay", "ChangeFileEncoding", "MetaStructure",
            "WriteToLog", "TableCompare", "ZipFile", "ProcessFiles", "ExecProcess",
            "SSH", "Mail", "SyslogMessage", "Edi2Xml",
        ):
            ctx = _ctx(
                f"<step><name>S</name><type>{step_type}</type></step>",
                step_type,
                "S",
            )
            outcome = self.registry.convert_step(step_type, ctx)
            self.assertNotEqual(outcome.status, "failed", step_type)
            self.assertTrue(_syntax_ok(outcome.code_lines), "\n".join(outcome.code_lines))

    def test_clone_row_duplicates(self):
        ctx = _ctx(
            """
            <step>
              <name>Clone</name>
              <type>CloneRow</type>
              <nrclones>2</nrclones>
              <addcloneflag>Y</addcloneflag>
              <cloneflagfield>is_clone</cloneflagfield>
            </step>
            """,
            "CloneRow",
            "Clone",
        )
        outcome = self.registry.convert_step("CloneRow", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertEqual(outcome.status, "converted")
        self.assertIn("unionByName", code)
        self.assertIn("is_clone", code)

    def test_clone_row_from_field(self):
        ctx = _ctx(
            """
            <step>
              <name>Clone</name>
              <type>CloneRow</type>
              <nrcloneinfield>Y</nrcloneinfield>
              <nrclonefield>n</nrclonefield>
            </step>
            """,
            "CloneRow",
            "Clone",
        )
        outcome = self.registry.convert_step("CloneRow", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("explode(sequence(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_null_if_sets_null(self):
        ctx = _ctx(
            """
            <step>
              <name>NI</name>
              <type>NullIf</type>
              <fields>
                <field><name>status</name><value>N/A</value></field>
              </fields>
            </step>
            """,
            "NullIf",
            "NI",
        )
        outcome = self.registry.convert_step("NullIf", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)
        self.assertIn("lit(None)", code)
        self.assertIn("when(", code)
        self.assertIn("N/A", code)

    def test_ifnull_still_works(self):
        ctx = _ctx(
            """
            <step>
              <name>IN</name>
              <type>IfNull</type>
              <selectFields>Y</selectFields>
              <fields>
                <field><name>a</name><value>0</value></field>
              </fields>
            </step>
            """,
            "IfNull",
            "IN",
        )
        outcome = self.registry.convert_step("IfNull", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("isNull()", code)
        self.assertIn("lit(0)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_ifnull_ui_display_name_matches(self):
        """UI title 'If Field Value is Null' must not fall through to FallbackHandler."""
        ctx = _ctx(
            """
            <step>
              <name>IN</name>
              <type>IfNull</type>
              <selectFields>Y</selectFields>
              <fields>
                <field><name>a</name><value>x</value></field>
              </fields>
            </step>
            """,
            "If Field Value is Null",
            "IN",
        )
        outcome = self.registry.convert_step("If Field Value is Null", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("isNull()", code)
        self.assertNotIn("Fallback", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_ifnull_replace_all_default(self):
        ctx = _ctx(
            """
            <step>
              <name>IN</name>
              <type>IfNull</type>
              <selectFields>N</selectFields>
              <selectValuesType>N</selectValuesType>
              <replaceAllByValue>DEFAULT</replaceAllByValue>
            </step>
            """,
            "IfNull",
            "IN",
            input_columns=["col_a", "col_b"],
        )
        outcome = self.registry.convert_step("IfNull", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("col_a", code)
        self.assertIn("DEFAULT", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_delay_documents_limitation(self):
        ctx = _ctx(
            """
            <step>
              <name>D</name>
              <type>Delay</type>
              <timeout>1000</timeout>
              <scaletime>milliseconds</scaletime>
            </step>
            """,
            "Delay",
            "D",
        )
        outcome = self.registry.convert_step("Delay", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("distributed", code.lower())
        self.assertIn(outcome.status, ("partial", "converted"))

    def test_meta_structure_emits_schema(self):
        ctx = _ctx(
            """
            <step>
              <name>MS</name>
              <type>MetaStructure</type>
              <outputRowcount>Y</outputRowcount>
            </step>
            """,
            "MetaStructure",
            "MS",
        )
        outcome = self.registry.convert_step("MetaStructure", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn(".schema", code)
        self.assertIn("createDataFrame", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_write_to_log_uses_logging(self):
        ctx = _ctx(
            """
            <step>
              <name>WL</name>
              <type>WriteToLog</type>
              <loglevel>Error</loglevel>
              <limitRows>Y</limitRows>
              <limitRowsNumber>3</limitRowsNumber>
              <fields><field><name>id</name></field></fields>
            </step>
            """,
            "WriteToLog",
            "WL",
        )
        outcome = self.registry.convert_step("WriteToLog", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("logging.getLogger", code)
        self.assertNotIn("print(", code.split("for _lr")[0])  # no print-based logging
        self.assertIn("ERROR", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_table_compare_join(self):
        ctx = _ctx(
            """
            <step>
              <name>TC</name>
              <type>TableCompare</type>
              <reference_schema>s</reference_schema>
              <reference_table>ref</reference_table>
              <compare_schema>s</compare_schema>
              <compare_table>cmp</compare_table>
              <key_fields><key><name>id</name></key></key_fields>
            </step>
            """,
            "TableCompare",
            "TC",
        )
        outcome = self.registry.convert_step("TableCompare", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("spark.table", code)
        self.assertIn(".join(", code)
        self.assertIn("_tc_diff", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_change_encoding(self):
        ctx = _ctx(
            """
            <step>
              <name>Enc</name>
              <type>ChangeFileEncoding</type>
              <sourcefilename>/data/a.txt</sourcefilename>
              <targetfilename>/data/b.txt</targetfilename>
              <sourceencoding>latin-1</sourceencoding>
              <targetencoding>utf-8</targetencoding>
            </step>
            """,
            "ChangeFileEncoding",
            "Enc",
        )
        outcome = self.registry.convert_step("ChangeFileEncoding", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("latin-1", code)
        self.assertIn("utf-8", code)
        self.assertIn("read_text", code)

    def test_zip_file(self):
        ctx = _ctx(
            """
            <step>
              <name>Z</name>
              <type>ZipFile</type>
              <zipfilename>/tmp/out.zip</zipfilename>
              <sourcefilenamefield>path</sourcefilenamefield>
              <compressionrate>DEFLATED</compressionrate>
            </step>
            """,
            "ZipFile",
            "Z",
        )
        outcome = self.registry.convert_step("ZipFile", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("zipfile.ZipFile", code)
        self.assertIn("ZIP_DEFLATED", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_process_files_copy(self):
        ctx = _ctx(
            """
            <step>
              <name>PF</name>
              <type>ProcessFiles</type>
              <operation_type>copy</operation_type>
              <sourcefilenamefield>src</sourcefilenamefield>
              <targetfilenamefield>dst</targetfilenamefield>
              <overwritetargetfile>Y</overwritetargetfile>
            </step>
            """,
            "ProcessFiles",
            "PF",
        )
        outcome = self.registry.convert_step("ProcessFiles", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("shutil.copy2", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_exec_process_warns(self):
        ctx = _ctx(
            """
            <step>
              <name>EP</name>
              <type>ExecProcess</type>
              <processfield>cmd</processfield>
              <outputfield>out</outputfield>
            </step>
            """,
            "ExecProcess",
            "EP",
        )
        outcome = self.registry.convert_step("ExecProcess", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.process_field", code)

    def test_ssh_redacts_password(self):
        ctx = _ctx(
            """
            <step>
              <name>SSH</name>
              <type>SSH</type>
              <serverName>h</serverName>
              <userName>u</userName>
              <password>secret</password>
              <command>uptime</command>
            </step>
            """,
            "SSH",
            "SSH",
        )
        outcome = self.registry.convert_step("SSH", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("<redacted>", code)
        self.assertNotIn("secret", code)

    def test_mail_stub(self):
        ctx = _ctx(
            """
            <step>
              <name>M</name>
              <type>Mail</type>
              <server>smtp</server>
              <destination>a@b.c</destination>
              <subject>Sub</subject>
              <comment>Body</comment>
            </step>
            """,
            "Mail",
            "M",
        )
        outcome = self.registry.convert_step("Mail", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("smtplib", code)
        self.assertIn("a@b.c", code)

    def test_syslog_warns(self):
        ctx = _ctx(
            """
            <step>
              <name>SL</name>
              <type>SyslogMessage</type>
              <serverName>log</serverName>
              <facility>USER</facility>
              <priority>INFO</priority>
            </step>
            """,
            "SyslogMessage",
            "SL",
        )
        outcome = self.registry.convert_step("SyslogMessage", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.facility", code)

    def test_edi_to_xml_stub(self):
        ctx = _ctx(
            """
            <step>
              <name>E</name>
              <type>Edi2Xml</type>
              <inputfield>edi</inputfield>
              <outputfield>xml</outputfield>
            </step>
            """,
            "Edi2Xml",
            "E",
        )
        outcome = self.registry.convert_step("Edi2Xml", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("CDATA", code)
        self.assertIn("withColumn", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_null_if_empty_config(self):
        ctx = _ctx(
            "<step><name>NI</name><type>NullIf</type></step>",
            "NullIf",
            "NI",
        )
        outcome = self.registry.convert_step("NullIf", ctx)
        self.assertIn(outcome.status, ("partial", "converted"))
        self.assertIn("WARNING", "\n".join(outcome.code_lines))

    def test_validators_registered(self):
        for st in ("clonerow", "nullif", "writetolog", "mail", "ssh", "iffieldvalueisnull"):
            v = get_validator(st)
            self.assertIsNotNone(v, st)

    def test_secrets_never_emitted(self):
        for step_type, xml, secret in (
            (
                "SSH",
                """
                <step><name>S</name><type>SSH</type>
                  <serverName>h</serverName><userName>u</userName>
                  <password>s3cret!</password><command>ls</command>
                </step>
                """,
                "s3cret!",
            ),
            (
                "Mail",
                """
                <step><name>M</name><type>Mail</type>
                  <server>smtp</server><destination>a@b.c</destination>
                  <auth_password>mail-pw</auth_password><use_auth>Y</use_auth><auth_user>u</auth_user>
                </step>
                """,
                "mail-pw",
            ),
        ):
            ctx = _ctx(xml, step_type, "X")
            code = "\n".join(self.registry.convert_step(step_type, ctx).code_lines)
            self.assertNotIn(secret, code)
            self.assertIn("<redacted>", code)

    def test_ui_aliases_resolve(self):
        aliases = [
            ("Clone Row", "CloneRow", "unionByName"),
            ("Null If", "NullIf", "lit(None)"),
            ("Delay Row", "Delay", "WARNING"),
            ("Change File Encoding", "ChangeFileEncoding", "encoding"),
            ("Metadata Structure of Stream", "MetaStructure", ".schema"),
            ("Write to Log", "WriteToLog", "logging.getLogger"),
            ("Table Compare", "TableCompare", "spark.table"),
            ("Zip File", "ZipFile", "zipfile"),
            ("Process Files", "ProcessFiles", "shutil"),
            ("Execute a Process", "ExecProcess", "UNSUPPORTED"),
            ("Run SSH Commands", "SSH", "UNSUPPORTED"),
            ("Send Message to Syslog", "SyslogMessage", "UNSUPPORTED"),
            ("EDI to XML", "Edi2Xml", "CDATA"),
        ]
        for ui_name, ktr_type, needle in aliases:
            xml = f"<step><name>S</name><type>{ktr_type}</type></step>"
            # Prefer richer XML when needed for meaningful codegen
            if ktr_type == "CloneRow":
                xml = "<step><name>S</name><type>CloneRow</type><nrclones>1</nrclones></step>"
            elif ktr_type == "NullIf":
                xml = (
                    "<step><name>S</name><type>NullIf</type>"
                    "<fields><field><name>a</name><value>x</value></field></fields></step>"
                )
            elif ktr_type == "ChangeFileEncoding":
                xml = (
                    "<step><name>S</name><type>ChangeFileEncoding</type>"
                    "<sourcefilename>a</sourcefilename><targetfilename>b</targetfilename>"
                    "<sourceencoding>utf-8</sourceencoding><targetencoding>utf-8</targetencoding>"
                    "</step>"
                )
            elif ktr_type == "TableCompare":
                xml = (
                    "<step><name>S</name><type>TableCompare</type>"
                    "<reference_table>t1</reference_table><compare_table>t2</compare_table>"
                    "<key_fields><key><name>id</name></key></key_fields></step>"
                )
            elif ktr_type == "ZipFile":
                xml = (
                    "<step><name>S</name><type>ZipFile</type>"
                    "<sourcefilenamefield>p</sourcefilenamefield></step>"
                )
            elif ktr_type == "ProcessFiles":
                xml = (
                    "<step><name>S</name><type>ProcessFiles</type>"
                    "<sourcefilenamefield>s</sourcefilenamefield>"
                    "<targetfilenamefield>t</targetfilenamefield></step>"
                )
            elif ktr_type == "Edi2Xml":
                xml = (
                    "<step><name>S</name><type>Edi2Xml</type>"
                    "<inputfield>edi</inputfield><outputfield>xml</outputfield></step>"
                )
            ctx = _ctx(xml, ui_name, "S")
            outcome = self.registry.convert_step(ui_name, ctx)
            code = "\n".join(outcome.code_lines)
            self.assertTrue(_syntax_ok(outcome.code_lines), f"{ui_name}: {code}")
            self.assertIn(needle, code, ui_name)


if __name__ == "__main__":
    unittest.main()
