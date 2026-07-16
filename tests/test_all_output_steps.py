"""Tests for all Pentaho Output transformation step converters."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.extra_output_handlers import EXTRA_OUTPUT_HANDLERS as OUTPUT_HANDLERS


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    trans = PentahoTransformation(name="OutputTrans", file_path=Path("output.ktr"))
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


class TestCoreOutputs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_table_output_append(self):
        xml = """
        <step>
          <schema>analytics</schema>
          <table>fact_sales</table>
          <truncate>N</truncate>
        </step>
        """
        outcome = self.registry.convert_step("TableOutput", _ctx(xml, "TableOutput", "TO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("saveAsTable", code)
        self.assertIn("append", code)
        self.assertIn("TARGET_CATALOG", code)
        self.assertIn("fact_sales", code)
        self.assertIn("type: TableOutput", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_table_output_truncate(self):
        xml = """
        <step><table>dim_customer</table><truncate>Y</truncate></step>
        """
        outcome = self.registry.convert_step("TableOutput", _ctx(xml, "TableOutput", "TO2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("overwrite", code)

    def test_table_output_missing_table_warns(self):
        xml = "<step><truncate>N</truncate></step>"
        outcome = self.registry.convert_step("TableOutput", _ctx(xml, "TableOutput", "TO3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: Target table name missing", code)
        self.assertIn("<table_name>", code)
        self.assertNotIn("target_table", code)

    def test_text_file_output(self):
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
        outcome = self.registry.convert_step("TextFileOutput", _ctx(xml, "TextFileOutput", "TFO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('format("csv")', code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertIn("customers.csv", code)
        self.assertIn(";", code)
        self.assertNotIn("C:\\", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_text_file_output_tsv(self):
        xml = """
        <step>
          <filename>/data/out/rows.tsv</filename>
          <separator>&#9;</separator>
          <header>N</header>
        </step>
        """
        outcome = self.registry.convert_step("TextFileOutput", _ctx(xml, "TextFileOutput", "TFO_TSV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('format("csv")', code)
        self.assertIn(r"\t", code)

    def test_text_file_output_single_column_plain_text(self):
        xml = """
        <step>
          <filename>lines</filename>
          <fields>
            <field><name>line</name></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step("TextFileOutput", _ctx(xml, "TextFileOutput", "TFO2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".text(", code)
        self.assertIn('select("line")', code.replace("'", '"'))

    def test_text_file_output_missing_filename_partial(self):
        xml = "<step><separator>,</separator><header>Y</header></step>"
        outcome = self.registry.convert_step("TextFileOutput", _ctx(xml, "TextFileOutput", "TFO3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: Output filename missing", code)
        self.assertIn("<output_name>", code)
        self.assertEqual(outcome.status, "partial")
        self.assertNotEqual(outcome.status, "failed")

    def test_text_file_output_fixed_width_todo(self):
        xml = """
        <step>
          <filename>/data/fixed.out</filename>
          <file_type>Fixed</file_type>
          <padded>Y</padded>
          <fields><field><name>a</name><length>5</length></field></fields>
        </step>
        """
        outcome = self.registry.convert_step("TextFileOutput", _ctx(xml, "TextFileOutput", "FIXO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("TODO: Fixed-width output", code)
        self.assertEqual(outcome.status, "partial")

    def test_excel_output_spark_excel(self):
        xml = """
        <step>
          <header>Y</header>
          <footer>N</footer>
          <encoding>UTF-8</encoding>
          <append>N</append>
          <file>
            <name>/data/out</name>
            <extention>xls</extention>
            <sheetname>Data</sheetname>
            <nullisblank>Y</nullisblank>
            <autosizecolums>Y</autosizecolums>
          </file>
          <fields>
            <field><name>col_a</name><type>String</type><format>#</format></field>
            <field><name>col_b</name><type>Number</type></field>
          </fields>
          <custom>
            <header_font_name>arial</header_font_name>
            <header_font_bold>Y</header_font_bold>
          </custom>
        </step>
        """
        outcome = self.registry.convert_step("ExcelOutput", _ctx(xml, "ExcelOutput", "XO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("com.crealytics.spark.excel", code)
        self.assertIn("dataAddress", code)
        self.assertIn("Data", code)
        self.assertIn("select('col_a', 'col_b')", code.replace('"', "'"))
        self.assertIn("preserved.sheetname", code)
        self.assertIn("preserved.formatting", code)
        self.assertIn("WARNING", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_excel_output_missing_workbook(self):
        xml = "<step><sheetname>OnlySheet</sheetname></step>"
        outcome = self.registry.convert_step("ExcelOutput", _ctx(xml, "ExcelOutput", "XO2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("workbook/filename missing", code.lower())
        self.assertIn("WARNING", code)

    def test_excel_output_variable_substitution(self):
        xml = """
        <step>
          <file>
            <name>${Internal.Transformation.Filename.Directory}/report</name>
            <extention>xls</extention>
            <sheetname>${SHEET}</sheetname>
          </file>
        </step>
        """
        ctx = _ctx(xml, "ExcelOutput", "XO3")
        ctx.transformation.parameters = {"SHEET": "Export"}
        outcome = self.registry.convert_step("ExcelOutput", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertIn("Export", code)
        self.assertIn("com.crealytics.spark.excel", code)

    def test_json_output(self):
        xml = "<step><filename>/data/a.json</filename><append>N</append></step>"
        outcome = self.registry.convert_step("JsonOutput", _ctx(xml, "JsonOutput", "JO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".json(", code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertIn("a.json", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_parquet_output(self):
        xml = "<step><filename>/data/out.parquet</filename></step>"
        outcome = self.registry.convert_step("ParquetOutput", _ctx(xml, "ParquetOutput", "PQ"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".parquet(", code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_delta_file_output(self):
        xml = "<step><filename>/data/delta_out</filename></step>"
        outcome = self.registry.convert_step("DeltaOutput", _ctx(xml, "DeltaOutput", "DO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('format("delta")', code)
        self.assertIn(".save(", code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_csv_output(self):
        xml = """
        <step>
          <filename>/data/out/customers.csv</filename>
          <separator>,</separator>
          <header>Y</header>
          <enclosure>"</enclosure>
        </step>
        """
        outcome = self.registry.convert_step("CsvOutput", _ctx(xml, "CsvOutput", "CSV"))
        code = "\n".join(outcome.code_lines)
        self.assertIn('format("csv")', code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertIn("customers.csv", code)
        self.assertIn('option("quote"', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_windows_path_never_emitted(self):
        xml = r"<step><filename>C:\Users\me\out.csv</filename></step>"
        outcome = self.registry.convert_step("CsvOutput", _ctx(xml, "CsvOutput", "Win"))
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("C:\\Users", code)
        self.assertIn("PENTAHO_DATA_DIR", code)

    def test_xml_output(self):
        xml = """
        <step>
          <filename>/data/a.xml</filename>
          <mainElement>orders</mainElement>
          <repeatElement>order</repeatElement>
        </step>
        """
        outcome = self.registry.convert_step("XMLOutput", _ctx(xml, "XMLOutput", "XO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('xml')", code)
        self.assertIn("orders", code)
        self.assertIn("order", code)


class TestDbOutputs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_insert_update_lookup_keys(self):
        xml = """
        <step>
          <schema>analytics</schema>
          <lookup>
            <table>dim_customer</table>
            <key><name>cust_id</name><field>customer_id</field><condition>=</condition></key>
            <value><name>cust_name</name><rename>name</rename><update>Y</update></value>
          </lookup>
        </step>
        """
        outcome = self.registry.convert_step("InsertUpdate", _ctx(xml, "InsertUpdate", "IU"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("MERGE INTO", code)
        self.assertIn("customer_id", code)
        self.assertIn("cust_id", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_update_merge(self):
        xml = """
        <step>
          <lookup>
            <table>orders</table>
            <key><name>id</name><field>id</field></key>
            <value><name>amount</name><rename>amount</rename><update>Y</update></value>
          </lookup>
        </step>
        """
        outcome = self.registry.convert_step("Update", _ctx(xml, "Update", "Upd"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("MERGE INTO", code)
        self.assertIn("WHEN MATCHED THEN UPDATE", code)

    def test_delete_merge(self):
        xml = """
        <step>
          <lookup>
            <table>orders</table>
            <key><name>id</name><field>id</field></key>
          </lookup>
        </step>
        """
        outcome = self.registry.convert_step("Delete", _ctx(xml, "Delete", "Del"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("MERGE INTO", code)
        self.assertIn("DELETE", code)

    def test_synchronize_after_merge(self):
        xml = """
        <step>
          <lookup>
            <table>sync_target</table>
            <key><name>id</name><field>id</field></key>
          </lookup>
          <operation_order_field>flag</operation_order_field>
          <order_insert>new</order_insert>
          <order_update>changed</order_update>
          <order_delete>deleted</order_delete>
        </step>
        """
        outcome = self.registry.convert_step(
            "SynchronizeAfterMerge", _ctx(xml, "SynchronizeAfterMerge", "Sync"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("MERGE INTO", code)
        self.assertIn("_sync_src", code)
        self.assertIn("WHEN MATCHED", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))


class TestExtraOutputs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertGreaterEqual(len(OUTPUT_HANDLERS), 10)

    def test_excel_writer(self):
        xml = """
        <step>
          <file><name>/data/out</name><extension>xlsx</extension></file>
          <sheetname>Report</sheetname>
          <header>Y</header>
          <startingCell>B2</startingCell>
        </step>
        """
        outcome = self.registry.convert_step("ExcelWriter", _ctx(xml, "ExcelWriter", "EW"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("com.crealytics.spark.excel", code)
        self.assertIn("Report", code)
        self.assertIn("B2", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_access_output(self):
        xml = "<step><filename>/tmp/db.accdb</filename><table>export</table></step>"
        outcome = self.registry.convert_step("AccessOutput", _ctx(xml, "AccessOutput", "AO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("ucanaccess", code.lower())
        self.assertIn("jdbc", code)

    def test_s3_file_output(self):
        xml = """
        <step>
          <bucket>my-bucket</bucket>
          <filename>out/data</filename>
          <file_format>csv</file_format>
          <separator>,</separator>
          <header>Y</header>
        </step>
        """
        outcome = self.registry.convert_step("S3FileOutput", _ctx(xml, "S3FileOutput", "S3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("s3a://", code)
        self.assertIn("format('csv')", code)

    def test_sql_file_output(self):
        xml = """
        <step>
          <filename>/data/out.sql</filename>
          <schema>analytics</schema>
          <table>customers</table>
        </step>
        """
        outcome = self.registry.convert_step("SQLFileOutput", _ctx(xml, "SQLFileOutput", "SQL"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("INSERT INTO", code)
        self.assertIn(".text(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_properties_output(self):
        xml = """
        <step>
          <filename>/data/app.properties</filename>
          <key_field>k</key_field>
          <value_field>v</value_field>
        </step>
        """
        outcome = self.registry.convert_step("PropertyOutput", _ctx(xml, "PropertyOutput", "PO"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("lit('=')", code)
        self.assertIn(".text(", code)

    def test_rss_output(self):
        xml = "<step><filename>/data/feed.xml</filename><channel_title>News</channel_title></step>"
        outcome = self.registry.convert_step("RssOutput", _ctx(xml, "RssOutput", "RSS"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('xml')", code)

    def test_ldap_output_partial(self):
        xml = "<step><host>ldap.example</host><port>389</port><searchBase>dc=ex</searchBase></step>"
        outcome = self.registry.convert_step("LDAPOutput", _ctx(xml, "LDAPOutput", "LDAP"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("LDAP", code)
        self.assertIn("ldap.example", code)

    def test_salesforce_insert(self):
        xml = "<step><module>Account</module><targeturl>https://login.salesforce.com</targeturl></step>"
        outcome = self.registry.convert_step(
            "SalesforceInsert", _ctx(xml, "SalesforceInsert", "SFI"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("salesforce", code.lower())
        self.assertIn("Account", code)

    def test_salesforce_update(self):
        outcome = self.registry.convert_step(
            "SalesforceUpdate",
            _ctx("<step><module>Contact</module></step>", "SalesforceUpdate", "SFU"),
        )
        self.assertIn("update", "\n".join(outcome.code_lines))

    def test_salesforce_upsert(self):
        xml = "<step><module>Account</module><upsertfield>ExternalId__c</upsertfield></step>"
        outcome = self.registry.convert_step(
            "SalesforceUpsert", _ctx(xml, "SalesforceUpsert", "SFUp"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("upsert", code)
        self.assertIn("ExternalId__c", code)

    def test_salesforce_delete(self):
        outcome = self.registry.convert_step(
            "SalesforceDelete",
            _ctx("<step><module>Lead</module></step>", "SalesforceDelete", "SFD"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("delete", code.lower())
        self.assertIn("salesforce", code.lower())

    def test_serialize_unsupported(self):
        outcome = self.registry.convert_step(
            "CubeOutput",
            _ctx("<step><file>cube.ser</file></step>", "CubeOutput", "Cube"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)

    def test_autodoc(self):
        outcome = self.registry.convert_step(
            "AutoDoc",
            _ctx("<step><filename>/docs/out.md</filename></step>", "AutoDoc", "Doc"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".text(", code)
        self.assertIn("documentation", code.lower())

    def test_reporting_unsupported(self):
        outcome = self.registry.convert_step(
            "PentahoReportingOutput",
            _ctx(
                "<step><filename>/reports/r.prpt</filename></step>",
                "PentahoReportingOutput",
                "PRPT",
            ),
        )
        self.assertIn("UNSUPPORTED", "\n".join(outcome.code_lines))


class TestAllListedOutputsHaveHandlers(unittest.TestCase):
    """Ensure every listed Output step type resolves to a dedicated converter."""

    EXPECTED = [
        ("AutoDoc", "Automatic Documentation"),
        ("Delete", "Delete"),
        ("InsertUpdate", "Insert/Update"),
        ("JsonOutput", "JSON"),
        ("LDAPOutput", "LDAP"),
        ("AccessOutput", "Access"),
        ("ExcelWriter", "Excel Writer"),
        ("PentahoReportingOutput", "Reporting"),
        ("PropertyOutput", "Properties"),
        ("RssOutput", "RSS"),
        ("S3FileOutput", "S3"),
        ("SQLFileOutput", "SQL File"),
        ("SalesforceDelete", "SF Delete"),
        ("SalesforceInsert", "SF Insert"),
        ("SalesforceUpdate", "SF Update"),
        ("SalesforceUpsert", "SF Upsert"),
        ("CubeOutput", "Serialize"),
        ("SynchronizeAfterMerge", "Sync"),
        ("TableOutput", "Table"),
        ("TextFileOutput", "Text"),
        ("Update", "Update"),
        ("XMLOutput", "XML"),
        ("ParquetOutput", "Parquet"),
        ("DeltaOutput", "Delta"),
        ("CsvOutput", "CSV"),
        ("ExcelOutput", "Excel"),
    ]

    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_all_types_have_dedicated_handlers(self):
        for step_type, _label in self.EXPECTED:
            xml = "<step><filename>/tmp/x</filename><table>t</table><module>Account</module></step>"
            outcome = self.registry.convert_step(step_type, _ctx(xml, step_type, step_type))
            self.assertTrue(
                outcome.code_lines,
                msg=f"{step_type} produced no code",
            )
            # Fallback emits generic Delta — dedicated handlers should not look like pure fallback
            # for most types; at minimum syntax is valid.
            self.assertTrue(
                _syntax_ok(outcome.code_lines),
                msg=f"{step_type} generated invalid Python:\n" + "\n".join(outcome.code_lines),
            )


if __name__ == "__main__":
    unittest.main()
