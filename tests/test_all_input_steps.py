"""Tests for all Pentaho Input transformation step converters."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.extra_input_handlers import EXTRA_INPUT_HANDLERS


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = False) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    trans = PentahoTransformation(name="InputTrans", file_path=Path("input.ktr"))
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


class TestExistingInputsUnchanged(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_table_input(self):
        xml = "<step><sql>SELECT id FROM customers</sql><connection>db</connection></step>"
        outcome = self.registry.convert_step("TableInput", _ctx(xml, "TableInput", "TI"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("spark.sql(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_csv_input(self):
        xml = """
        <step>
          <filename>/data/a.csv</filename><separator>,</separator><header>Y</header>
          <fields><field><name>id</name><type>Integer</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step("CsvInput", _ctx(xml, "CsvInput", "CSV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('csv')", code)
        self.assertIn("/data/a.csv", code)

    def test_data_grid(self):
        xml = """
        <step>
          <fields>
            <field><name>a</name><type>String</type></field>
            <field><name>b</name><type>Integer</type></field>
          </fields>
          <data><line><item>x</item><item>1</item></line></data>
        </step>
        """
        outcome = self.registry.convert_step("DataGrid", _ctx(xml, "DataGrid", "DG"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)

    def test_row_generator(self):
        xml = """
        <step><limit>5</limit>
          <fields><field><name>id</name><type>Integer</type><nullif/><ifnull>1</ifnull></field></fields>
        </step>
        """
        outcome = self.registry.convert_step("RowGenerator", _ctx(xml, "RowGenerator", "RG"))
        self.assertIn("createDataFrame", "\n".join(outcome.code_lines))

    def test_json_input(self):
        xml = "<step><filename>/data/a.json</filename></step>"
        outcome = self.registry.convert_step("JsonInput", _ctx(xml, "JsonInput", "JI"))
        self.assertIn("format('json')", "\n".join(outcome.code_lines))

    def test_xml_get_data(self):
        xml = "<step><filename>/data/a.xml</filename></step>"
        outcome = self.registry.convert_step("getXMLData", _ctx(xml, "getXMLData", "XML"))
        self.assertIn("format('xml')", "\n".join(outcome.code_lines))

    def test_excel_input_path_expr(self):
        xml = "<step><filename>/data/a.xlsx</filename><sheetname>Data</sheetname></step>"
        outcome = self.registry.convert_step("ExcelInput", _ctx(xml, "ExcelInput", "XL"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("com.crealytics.spark.excel", code)
        self.assertIn("Data", code)

    def test_text_file_input(self):
        xml = """
        <step>
          <filename>/data/a.txt</filename><separator>|</separator><header>Y</header>
          <enclosure>"</enclosure><encoding>UTF-8</encoding>
          <fields>
            <field><name>id</name><type>Integer</type></field>
            <field><name>name</name><type>String</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step("TextFileInput", _ctx(xml, "TextFileInput", "TFI"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".csv(", code)
        self.assertIn("|", code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertIn("a.txt", code)
        self.assertIn(".schema(", code)
        self.assertEqual(outcome.status, "converted")

    def test_text_file_input_tsv(self):
        xml = """
        <step>
          <filename>/data/rows.tsv</filename>
          <separator>&#9;</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <fields><field><name>col</name><type>String</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step("TextFileInput", _ctx(xml, "TextFileInput", "TFI_TSV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".csv(", code)
        self.assertIn(r"\t", code)

    def test_text_file_input_missing_filename_partial(self):
        xml = "<step><separator>,</separator><header>Y</header></step>"
        outcome = self.registry.convert_step("TextFileInput", _ctx(xml, "TextFileInput", "TFI2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: Input filename missing", code)
        self.assertIn("<input_file>", code)
        self.assertEqual(outcome.status, "partial")
        self.assertNotEqual(outcome.status, "failed")

    def test_text_file_input_missing_schema_still_converted(self):
        """inferSchema remains executable — locale/schema INFO must not force PARTIAL."""
        xml = """
        <step>
          <filename>/data/a.csv</filename>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
        </step>
        """
        outcome = self.registry.convert_step("TextFileInput", _ctx(xml, "TextFileInput", "TFI3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("Schema unavailable", code)
        self.assertIn("inferSchema", code)
        self.assertIn(".csv(", code)
        self.assertEqual(outcome.status, "converted")

    def test_system_info(self):
        xml = """
        <step>
          <fields><field><name>run_ts</name><type>system datetime (variable)</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step("SystemInfo", _ctx(xml, "SystemInfo", "SYS"))
        self.assertIn("current_timestamp()", "\n".join(outcome.code_lines))


class TestNewlyImplementedInputs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_fixed_file_input(self):
        xml = """
        <step>
          <filename>/data/fixed.txt</filename>
          <fields>
            <field><name>id</name><type>String</type><length>4</length></field>
            <field><name>name</name><type>String</type><length>10</length></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step("FixedInput", _ctx(xml, "FixedInput", "FIX"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("substring(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_gzip_csv_input(self):
        xml = "<step><filename>/data/a.csv.gz</filename><separator>,</separator><header>Y</header></step>"
        outcome = self.registry.convert_step("GZIPCSVInput", _ctx(xml, "GZIPCSVInput", "GZ"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("gzip", code)
        self.assertIn("format('csv')", code)

    def test_s3_csv_input(self):
        xml = "<step><bucket>my-bucket</bucket><filename>path/a.csv</filename><header>Y</header></step>"
        outcome = self.registry.convert_step("S3CSVINPUT", _ctx(xml, "S3CSVINPUT", "S3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('csv')", code)
        self.assertIn("s3a://", code)

    def test_property_input(self):
        xml = "<step><filename>/cfg/app.properties</filename></step>"
        outcome = self.registry.convert_step("PropertyInput", _ctx(xml, "PropertyInput", "PROP"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("read.text(", code)
        self.assertIn("split(", code)

    def test_yaml_input(self):
        xml = "<step><filename>/data/a.yaml</filename></step>"
        outcome = self.registry.convert_step("YamlInput", _ctx(xml, "YamlInput", "YML"))
        self.assertIn("read.text(", "\n".join(outcome.code_lines))

    def test_xml_input_stream(self):
        xml = "<step><filename>/data/stream.xml</filename><row_tag>record</row_tag></step>"
        outcome = self.registry.convert_step("XMLInputStream", _ctx(xml, "XMLInputStream", "STAX"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('xml')", code)
        self.assertIn("record", code)

    def test_load_file_content(self):
        xml = "<step><filename>/data/*.txt</filename></step>"
        outcome = self.registry.convert_step("LoadFileInput", _ctx(xml, "LoadFileInput", "LFC"))
        self.assertIn("wholeTextFiles(", "\n".join(outcome.code_lines))

    def test_access_input(self):
        xml = "<step><filename>/data/db.accdb</filename><table>Customers</table></step>"
        outcome = self.registry.convert_step("AccessInput", _ctx(xml, "AccessInput", "ACC"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("ucanaccess", code.lower())
        self.assertIn("Customers", code)

    def test_sas_input(self):
        xml = "<step><filename>/data/a.sas7bdat</filename></step>"
        outcome = self.registry.convert_step("SASInput", _ctx(xml, "SASInput", "SAS"))
        self.assertIn("sas", "\n".join(outcome.code_lines).lower())

    def test_xbase_input(self):
        xml = "<step><filename>/data/a.dbf</filename></step>"
        outcome = self.registry.convert_step("XBaseInput", _ctx(xml, "XBaseInput", "DBF"))
        self.assertIn("dbf", "\n".join(outcome.code_lines))

    def test_shapefile_input(self):
        xml = "<step><filename>/data/zones.shp</filename></step>"
        outcome = self.registry.convert_step("ShapeFileReader", _ctx(xml, "ShapeFileReader", "SHP"))
        code = "\n".join(outcome.code_lines)
        self.assertTrue("shapefile" in code.lower() or "Sedona" in code)

    def test_get_file_names(self):
        xml = "<step><filename>/mnt/data</filename></step>"
        outcome = self.registry.convert_step("GetFileNames", _ctx(xml, "GetFileNames", "GFN"))
        code = "\n".join(outcome.code_lines)
        self.assertTrue("dbutils.fs.ls" in code or "listStatus" in code)

    def test_get_subfolder_names(self):
        xml = "<step><directory>/mnt/data</directory></step>"
        outcome = self.registry.convert_step("GetSubFolders", _ctx(xml, "GetSubFolders", "GSF"))
        code = "\n".join(outcome.code_lines)
        self.assertTrue("isDir" in code or "isDirectory" in code)

    def test_get_files_rows_count(self):
        xml = "<step><filename>/data/a.txt</filename><rowsCountField>n</rowsCountField></step>"
        outcome = self.registry.convert_step("GetFilesRowsCount", _ctx(xml, "GetFilesRowsCount", "GFRC"))
        self.assertIn(".count()", "\n".join(outcome.code_lines))

    def test_get_table_names(self):
        xml = "<step><schemaname>default</schemaname></step>"
        outcome = self.registry.convert_step("GetTableNames", _ctx(xml, "GetTableNames", "GTN"))
        self.assertIn("listTables", "\n".join(outcome.code_lines))

    def test_random_value(self):
        xml = """
        <step>
          <fields>
            <field><name>r</name><type>number</type></field>
            <field><name>u</name><type>uuid</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step("RandomValue", _ctx(xml, "RandomValue", "RV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("rand()", code)
        self.assertIn("uuid()", code)

    def test_random_credit_card(self):
        xml = """
        <step>
          <field>card_number</field><card_type>Visa</card_type><length>16</length><limit>3</limit>
        </step>
        """
        outcome = self.registry.convert_step(
            "RandomCCNumberGenerator", _ctx(xml, "RandomCCNumberGenerator", "CC")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("card_number", code)
        self.assertIn("concat(", code)

    def test_salesforce_input(self):
        xml = "<step><module>Account</module><query>SELECT Id FROM Account</query></step>"
        outcome = self.registry.convert_step("SalesforceInput", _ctx(xml, "SalesforceInput", "SF"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("salesforce", code.lower())
        self.assertIn("SELECT Id FROM Account", code)

    def test_ldap_input_partial(self):
        xml = "<step><host>ldap.example.com</host><port>389</port><searchBase>dc=ex</searchBase></step>"
        outcome = self.registry.convert_step("LDAPInput", _ctx(xml, "LDAPInput", "LDAP"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("PARTIAL", code)
        self.assertIn("ldap.example.com", code)

    def test_ldif_input(self):
        xml = "<step><filename>/data/users.ldif</filename></step>"
        outcome = self.registry.convert_step("LDIFInput", _ctx(xml, "LDIFInput", "LDIF"))
        self.assertIn("ldif", "\n".join(outcome.code_lines).lower())

    def test_rss_input(self):
        xml = "<step><url>/data/feed.xml</url><row_tag>item</row_tag></step>"
        outcome = self.registry.convert_step("RssInput", _ctx(xml, "RssInput", "RSS"))
        self.assertIn("format('xml')", "\n".join(outcome.code_lines))

    def test_hl7_input_partial(self):
        xml = "<step><filename>/data/msg.hl7</filename></step>"
        outcome = self.registry.convert_step("HL7Input", _ctx(xml, "HL7Input", "HL7"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("hl7", code.lower())
        self.assertIn("PARTIAL", code)


class TestUnsupportedInputs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def _assert_unsupported(self, step_type: str, xml: str = "<step/>"):
        outcome = self.registry.convert_step(step_type, _ctx(xml, step_type, step_type))
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial", "partially_supported", "unsupported"))

    def test_get_repository_names(self):
        self._assert_unsupported("GetRepositoryNames")

    def test_email_messages_input(self):
        self._assert_unsupported("MailInput", "<step><server>imap.example.com</server></step>")

    def test_deserialize_from_file(self):
        self._assert_unsupported("CubeInput", "<step><file>/tmp/cube.ser</file></step>")

    def test_mondrian_input(self):
        self._assert_unsupported(
            "MondrianInput",
            "<step><catalog>/mondrian/schema.xml</catalog><query>SELECT ...</query></step>",
        )

    def test_olap_input(self):
        self._assert_unsupported("OlapInput", "<step><url>http://xmla</url></step>")

    def test_sap_input(self):
        xml = """
        <step>
          <connection>SAP_ERP</connection>
          <client>100</client>
          <system>00</system>
          <language>EN</language>
          <username>RFC_USER</username>
          <host>sap.example.com</host>
          <function>
            <name>RFC_READ_TABLE</name>
            <description>Read table</description>
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
              <field_name>100</field_name>
              <sap_type>SINGLE</sap_type>
              <parameter_name>ROWCOUNT</parameter_name>
              <target_type>Integer</target_type>
            </parameter>
            <parameter>
              <field_name>MATNR LIKE 'A%'</field_name>
              <sap_type>TABLE</sap_type>
              <table_name>OPTIONS</table_name>
              <parameter_name>TEXT</parameter_name>
              <target_type>String</target_type>
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
        outcome = self.registry.convert_step("SapInput", _ctx(xml, "SapInput", "SAP1"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.connection", code)
        self.assertIn("RFC_READ_TABLE", code)
        self.assertIn("preserved.client", code)
        self.assertIn("100", code)
        self.assertIn("preserved.parameters", code)
        self.assertIn("preserved.fields", code)
        self.assertIn("WARNING", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_sap_input_missing_connection(self):
        xml = "<step><function><name>BAPI_USER_GET_DETAIL</name></function></step>"
        outcome = self.registry.convert_step("SapInput", _ctx(xml, "SapInput", "SAP2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("Missing SAP connection", code)
        self.assertIn("UNSUPPORTED", code)


class TestInputHandlerCoverage(unittest.TestCase):
    """Ensure every required Input step type resolves to a dedicated handler."""

    CASES = [
        ("CsvInput", "<step><filename>/a.csv</filename></step>", "csv"),
        ("DataGrid", "<step><fields><field><name>a</name></field></fields></step>", "createDataFrame"),
        ("CubeInput", "<step><file>/cube.ser</file></step>", "UNSUPPORTED"),
        ("ShapeFileReader", "<step><filename>/a.shp</filename></step>", "shapefile"),
        ("MailInput", "<step><server>imap</server></step>", "UNSUPPORTED"),
        (
            "FixedInput",
            "<step><filename>/a.txt</filename><fields>"
            "<field><name>id</name><length>4</length></field></fields></step>",
            "substring",
        ),
        ("GZIPCSVInput", "<step><filename>/a.csv.gz</filename></step>", "gzip"),
        ("RandomCCNumberGenerator", "<step><field>cc</field></step>", "concat"),
        ("RandomValue", "<step><fields><field><name>r</name><type>number</type></field></fields></step>", "rand"),
        ("RowGenerator", "<step><limit>1</limit><fields><field><name>a</name></field></fields></step>", "createDataFrame"),
        ("getXMLData", "<step><filename>/a.xml</filename></step>", "xml"),
        ("GetFileNames", "<step><filename>/mnt</filename></step>", "ls"),
        ("GetFilesRowsCount", "<step><filename>/a.txt</filename></step>", "count"),
        ("GetRepositoryNames", "<step/>", "UNSUPPORTED"),
        ("GetSubFolders", "<step><directory>/mnt</directory></step>", "folder"),
        ("SystemInfo", "<step/>", "current_"),
        ("GetTableNames", "<step/>", "listTables"),
        ("HL7Input", "<step><filename>/a.hl7</filename></step>", "hl7"),
        ("JsonInput", "<step><filename>/a.json</filename></step>", "json"),
        ("LDAPInput", "<step><host>h</host></step>", "LDAP"),
        ("LDIFInput", "<step><filename>/a.ldif</filename></step>", "ldif"),
        ("LoadFileInput", "<step><filename>/a.txt</filename></step>", "wholeTextFiles"),
        ("AccessInput", "<step><filename>/a.accdb</filename><table>t</table></step>", "ucanaccess"),
        ("ExcelInput", "<step><filename>/a.xlsx</filename></step>", "excel"),
        ("MondrianInput", "<step><catalog>c</catalog></step>", "UNSUPPORTED"),
        ("OlapInput", "<step/>", "UNSUPPORTED"),
        ("SapInput", "<step><connection>SAP</connection><function><name>RFC_READ_TABLE</name></function></step>", "UNSUPPORTED"),
        ("PropertyInput", "<step><filename>/a.properties</filename></step>", "split"),
        ("RssInput", "<step><url>/feed.xml</url></step>", "xml"),
        ("S3CSVINPUT", "<step><bucket>b</bucket><filename>a.csv</filename></step>", "csv"),
        ("SASInput", "<step><filename>/a.sas7bdat</filename></step>", "sas"),
        ("SalesforceInput", "<step><module>Account</module></step>", "salesforce"),
        ("TableInput", "<step><sql>SELECT 1</sql></step>", "spark."),
        ("TextFileInput", "<step><filename>/a.txt</filename><separator>,</separator></step>", "csv"),
        ("XBaseInput", "<step><filename>/a.dbf</filename></step>", "dbf"),
        ("XMLInputStream", "<step><filename>/a.xml</filename></step>", "xml"),
        ("YamlInput", "<step><filename>/a.yaml</filename></step>", "yaml"),
    ]

    def test_all_inputs_have_dedicated_handlers(self):
        registry = build_default_registry()
        for step_type, xml, needle in self.CASES:
            outcome = registry.convert_step(step_type, _ctx(xml, step_type, step_type))
            code = "\n".join(outcome.code_lines)
            self.assertFalse(
                any("No dedicated converter" in e for e in outcome.errors),
                msg=f"{step_type} fell through to unsupported registry path: {outcome.errors}",
            )
            self.assertIn(
                needle.lower(),
                code.lower(),
                msg=f"{step_type} code missing {needle!r}:\n{code}",
            )

    def test_extra_handlers_registered(self):
        self.assertGreaterEqual(len(EXTRA_INPUT_HANDLERS), 20)


if __name__ == "__main__":
    unittest.main()
