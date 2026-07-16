"""Parser and edge-case tests for Deprecated Excel Output and SAP Input."""

from __future__ import annotations

import textwrap
import unittest
from xml.etree import ElementTree as ET

from pentaho_converter.step_xml import parse_excel_output_config, parse_sap_input_config
from pentaho_converter.steps.base import build_default_registry
from tests.test_all_input_steps import _ctx as _input_ctx
from tests.test_all_output_steps import _ctx as _output_ctx


class TestExcelOutputParser(unittest.TestCase):
    def test_parses_workbook_sheet_formatting_and_template(self):
        xml = """
        <step>
          <header>Y</header>
          <footer>N</footer>
          <encoding>UTF-8</encoding>
          <append>Y</append>
          <add_to_result_filenames>Y</add_to_result_filenames>
          <file>
            <name>/data/report</name>
            <extention>xls</extention>
            <sheetname>SheetA</sheetname>
            <do_not_open_newfile_init>Y</do_not_open_newfile_init>
            <create_parent_folder>Y</create_parent_folder>
            <split>Y</split>
            <add_date>Y</add_date>
            <add_time>N</add_time>
            <autosizecolums>Y</autosizecolums>
            <nullisblank>Y</nullisblank>
            <protect_sheet>Y</protect_sheet>
            <password>secret</password>
            <splitevery>1000</splitevery>
            <usetempfiles>Y</usetempfiles>
            <tempdirectory>/tmp</tempdirectory>
          </file>
          <template>
            <enabled>Y</enabled>
            <append>Y</append>
            <filename>/tpl/base.xls</filename>
          </template>
          <fields>
            <field><name>a</name><type>Number</type><format>0.00</format></field>
          </fields>
          <custom>
            <header_font_bold>Y</header_font_bold>
            <row_font_name>arial</row_font_name>
          </custom>
        </step>
        """
        cfg = parse_excel_output_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["filename"], "/data/report")
        self.assertEqual(cfg["sheetname"], "SheetA")
        self.assertEqual(cfg["extension"], "xls")
        self.assertEqual(cfg["header"], "Y")
        self.assertEqual(cfg["append"], "Y")
        self.assertEqual(cfg["nullisblank"], "Y")
        self.assertEqual(cfg["protect_sheet"], "Y")
        self.assertTrue(cfg["password_set"])
        self.assertEqual(cfg["template_enabled"], "Y")
        self.assertEqual(cfg["template_append"], "Y")
        self.assertEqual(cfg["template_filename"], "/tpl/base.xls")
        self.assertEqual(cfg["do_not_open_newfile_init"], "Y")
        self.assertEqual(cfg["add_to_result_filenames"], "Y")
        self.assertEqual(cfg["split"], "Y")
        self.assertEqual(cfg["fields"][0]["format"], "0.00")
        self.assertEqual(cfg["custom"]["header_font_bold"], "Y")

    def test_preserves_protect_and_template_in_codegen(self):
        xml = """
        <step>
          <file>
            <name>/data/out</name>
            <extention>xls</extention>
            <sheetname>Data</sheetname>
            <protect_sheet>Y</protect_sheet>
            <password>x</password>
            <split>Y</split>
            <splitevery>50</splitevery>
          </file>
          <template><enabled>Y</enabled><append>Y</append><filename>/t.xls</filename></template>
          <add_to_result_filenames>Y</add_to_result_filenames>
        </step>
        """
        registry = build_default_registry()
        outcome = registry.convert_step("ExcelOutput", _output_ctx(xml, "ExcelOutput", "XO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("com.crealytics.spark.excel", code)
        self.assertIn("preserved.protect_sheet", code)
        self.assertIn("password_set", code)
        self.assertNotIn("password='x'", code)
        self.assertIn("preserved.template_append", code)
        self.assertIn("preserved.add_to_result_filenames", code)
        self.assertIn("preserved.split", code)
        self.assertEqual(outcome.status, "partial")


class TestSapInputParser(unittest.TestCase):
    def test_parses_connection_function_fields_filters(self):
        xml = """
        <step>
          <connection>SAP_ERP</connection>
          <client>100</client>
          <system>00</system>
          <language>EN</language>
          <username>RFC</username>
          <password>secret</password>
          <host>sap.host</host>
          <function>
            <name>RFC_READ_TABLE</name>
            <group>BC</group>
          </function>
          <parameters>
            <parameter>
              <field_name>MARA</field_name>
              <sap_type>SINGLE</sap_type>
              <parameter_name>QUERY_TABLE</parameter_name>
              <target_type>String</target_type>
            </parameter>
            <parameter>
              <field_name>MATNR LIKE 'A%'</field_name>
              <sap_type>TABLE</sap_type>
              <table_name>OPTIONS</table_name>
              <parameter_name>TEXT</parameter_name>
              <target_type>String</target_type>
            </parameter>
            <parameter>
              <field_name>500</field_name>
              <sap_type>SINGLE</sap_type>
              <parameter_name>ROWCOUNT</parameter_name>
              <target_type>Integer</target_type>
            </parameter>
          </parameters>
          <fields>
            <field>
              <field_name>WA</field_name>
              <sap_type>TABLE</sap_type>
              <table_name>DATA</table_name>
              <new_name>WA</new_name>
              <target_type>String</target_type>
            </field>
          </fields>
        </step>
        """
        cfg = parse_sap_input_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["connection"], "SAP_ERP")
        self.assertEqual(cfg["client"], "100")
        self.assertEqual(cfg["system"], "00")
        self.assertEqual(cfg["language"], "EN")
        self.assertEqual(cfg["function_name"], "RFC_READ_TABLE")
        self.assertTrue(cfg["password_set"])
        self.assertEqual(cfg["sap_connection"]["password"], "***REDACTED***")
        self.assertEqual(cfg["batch_size"], "500")
        self.assertEqual(len(cfg["filters"]), 1)
        self.assertEqual(cfg["fields"][0]["new_name"], "WA")

    def test_placeholder_and_status_partial(self):
        xml = """
        <step>
          <connection>SAP</connection>
          <client>100</client>
          <function><name>BAPI_USER_GET_DETAIL</name></function>
        </step>
        """
        registry = build_default_registry()
        outcome = registry.convert_step("SapInput", _input_ctx(xml, "SapInput", "SAP"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("WARNING", code)
        self.assertIn("preserved.client", code)
        self.assertEqual(outcome.status, "partial")


if __name__ == "__main__":
    unittest.main()
