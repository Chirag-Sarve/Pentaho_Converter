"""Tests for Pentaho Lookup step migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_check_file_locked_config,
    parse_column_exists_config,
    parse_database_lookup_config,
    parse_db_join_config,
    parse_db_proc_config,
    parse_dynamic_sql_row_config,
    parse_file_exists_config,
    parse_fuzzy_match_config,
    parse_http_client_config,
    parse_http_post_config,
    parse_rest_client_config,
    parse_step_metadata,
    parse_table_exists_config,
    parse_web_services_lookup_config,
    parse_webservice_available_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.lookup_handlers import LOOKUP_HANDLERS
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    extra_inputs: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="LookupTrans", file_path=Path("lookup.ktr"))
    hops: list[PentahoHop] = []
    steps: list[PentahoStep] = []
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        steps.append(inp)
        hops.append(PentahoHop(from_name="Input", to_name=step_name))
    if extra_inputs:
        for name in extra_inputs:
            steps.append(PentahoStep(name=name, step_type="RowGenerator", attributes={}, raw_element=None))
            hops.append(PentahoHop(from_name=name, to_name=step_name))
    steps.append(step)
    trans.steps = steps
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestLookupParsers(unittest.TestCase):
    def test_db_proc_parse(self):
        cfg = parse_db_proc_config(ET.fromstring("""
        <step>
          <connection>dw</connection>
          <procedure>sp_enrich</procedure>
          <lookup>
            <argument><name>id</name><direction>IN</direction><type>Integer</type></argument>
            <argument><name>out_status</name><direction>OUT</direction><type>String</type></argument>
          </lookup>
          <result><name>ret</name><type>Integer</type></result>
        </step>
        """))
        self.assertEqual(cfg["procedure"], "sp_enrich")
        self.assertEqual(len(cfg["parameters"]), 2)
        self.assertEqual(cfg["results"][0]["name"], "ret")

    def test_db_join_parse(self):
        cfg = parse_db_join_config(ET.fromstring("""
        <step>
          <connection>dw</connection>
          <sql>SELECT * FROM dim WHERE id = ?</sql>
          <outerjoin>Y</outerjoin>
          <rowlimit>10</rowlimit>
          <parameter><name>id</name></parameter>
        </step>
        """))
        self.assertTrue(cfg["outer_join"])
        self.assertEqual(cfg["row_limit"], 10)
        self.assertEqual(cfg["parameters"][0]["name"], "id")

    def test_dynamic_sql_parse(self):
        cfg = parse_dynamic_sql_row_config(ET.fromstring("""
        <step>
          <sql>SELECT 1</sql>
          <sql_fieldname>dyn_sql</sql_fieldname>
          <queryOnlyOnChange>Y</queryOnlyOnChange>
        </step>
        """))
        self.assertEqual(cfg["sql_field"], "dyn_sql")
        self.assertTrue(cfg["query_only_on_change"])

    def test_exists_parsers(self):
        fe = parse_file_exists_config(ET.fromstring(
            "<step><filename>/data/a.csv</filename><resultfieldname>ok</resultfieldname></step>"
        ))
        self.assertEqual(fe["filename"], "/data/a.csv")
        te = parse_table_exists_config(ET.fromstring(
            "<step><schemaname>s</schemaname><tablename>t</tablename></step>"
        ))
        self.assertEqual(te["table"], "t")
        ce = parse_column_exists_config(ET.fromstring(
            "<step><tablename>t</tablename><columnname>c</columnname></step>"
        ))
        self.assertEqual(ce["column"], "c")
        fl = parse_check_file_locked_config(ET.fromstring(
            "<step><filenamefield>path</filenamefield></step>"
        ))
        self.assertEqual(fl["filename_field"], "path")

    def test_http_rest_ws_fuzzy_parse(self):
        ws = parse_webservice_available_config(ET.fromstring("""
        <step><url>https://api.example/health</url><connectTimeOut>5000</connectTimeOut></step>
        """))
        self.assertIn("example", ws["url"])
        http = parse_http_client_config(ET.fromstring("""
        <step>
          <url>https://api.example/items</url>
          <fieldName>body</fieldName>
          <argument><argumentField>id</argumentField><argumentParameter>item_id</argumentParameter></argument>
        </step>
        """))
        self.assertEqual(http["result_field"], "body")
        self.assertEqual(http["arguments"][0]["parameter"], "item_id")
        post = parse_http_post_config(ET.fromstring("""
        <step><url>https://api.example</url><requestEntity>payload</requestEntity></step>
        """))
        self.assertEqual(post["request_entity"], "payload")
        rest = parse_rest_client_config(ET.fromstring("""
        <step>
          <url>https://api.example/v1</url>
          <method>POST</method>
          <bodyField>json_body</bodyField>
          <applicationType>JSON</applicationType>
        </step>
        """))
        self.assertEqual(rest["method"], "POST")
        soap = parse_web_services_lookup_config(ET.fromstring("""
        <step>
          <wsdlUrl>https://example/svc?wsdl</wsdlUrl>
          <operationName>GetStatus</operationName>
          <soapAction>urn:GetStatus</soapAction>
        </step>
        """))
        self.assertEqual(soap["operation"], "GetStatus")
        fuzzy = parse_fuzzy_match_config(ET.fromstring("""
        <step>
          <algorithm>Levenshtein</algorithm>
          <mainstreamfield>name</mainstreamfield>
          <lookupfield>lkp_name</lookupfield>
          <minimalValue>0.7</minimalValue>
          <maximalValue>1.0</maximalValue>
          <outputmatchvalue>score</outputmatchvalue>
        </step>
        """))
        self.assertEqual(fuzzy["main_stream_field"], "name")
        self.assertAlmostEqual(fuzzy["minimal_value"], 0.7)

    def test_stream_lookup_return_fields_under_lookup(self):
        cfg = parse_database_lookup_config(ET.fromstring("""
        <step>
          <cache>Y</cache>
          <cache_size>500</cache_size>
          <lookup>
            <key><name>region_code</name><field>region_code</field></key>
            <value><name>region_name</name><rename>region</rename><default>UNK</default></value>
          </lookup>
        </step>
        """))
        self.assertTrue(cfg["cached"])
        self.assertEqual(cfg["cache_size"], 500)
        self.assertEqual(cfg["return_fields"][0]["rename"], "region")

    def test_parse_step_metadata_aliases(self):
        el = ET.fromstring("<step><procedure>p</procedure><connection>c</connection></step>")
        self.assertEqual(parse_step_metadata(el, "DBProc")["procedure"], "p")
        self.assertIn("sql", parse_step_metadata(
            ET.fromstring("<step><sql>SELECT 1</sql></step>"), "DBJoin"
        ))


class TestLookupHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_lookup_handlers_registered(self):
        self.assertGreaterEqual(len(LOOKUP_HANDLERS), 10)

    def test_db_proc_codegen(self):
        xml = """
        <step>
          <name>CallProc</name><type>DBProc</type>
          <connection>dw</connection>
          <procedure>sp_enrich</procedure>
          <lookup>
            <argument><name>id</name><direction>IN</direction></argument>
          </lookup>
          <result><name>ret_code</name></result>
        </step>
        """
        outcome = self.registry.convert_step("DBProc", _ctx(xml, "DBProc", "CallProc"))
        self.assertNotEqual(outcome.status, "failed")
        code = "\n".join(outcome.code_lines)
        self.assertIn("sp_enrich", code)
        self.assertIn("CALL", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_db_join_codegen(self):
        xml = """
        <step>
          <name>DBJ</name><type>DBJoin</type>
          <connection>dw</connection>
          <sql>SELECT id, name FROM dim_cust</sql>
          <outer_join>Y</outer_join>
          <parameter><name>id</name></parameter>
        </step>
        """
        outcome = self.registry.convert_step("DBJoin", _ctx(xml, "DBJoin", "DBJ"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("spark.sql(", code)
        self.assertIn(".join(", code)
        self.assertIn("left", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_db_join_parameterized_no_spark_sql_question_mark(self):
        xml = """
        <step>
          <name>DBJ</name><type>DBJoin</type>
          <sql>SELECT * FROM dim WHERE id = ?</sql>
          <parameter><name>id</name></parameter>
        </step>
        """
        outcome = self.registry.convert_step("DBJoin", _ctx(xml, "DBJoin", "DBJ"))
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("spark.sql(_sql_", code)
        self.assertTrue("prepared" in code.lower() or "JDBC" in code)
        self.assertIn("WARNING", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_dynamic_sql_codegen(self):
        xml = """
        <step>
          <name>Dyn</name><type>DynamicSQLRow</type>
          <sql_fieldname>q</sql_fieldname>
          <outer_join>Y</outer_join>
          <rowlimit>5</rowlimit>
        </step>
        """
        outcome = self.registry.convert_step("DynamicSQLRow", _ctx(xml, "DynamicSQLRow", "Dyn"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("SQL injection", code)
        self.assertIn("toLocalIterator", code)
        self.assertIn("limit(5)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_file_table_column_exists(self):
        for stype, xml, needle in (
            ("FileExists", """
            <step><name>FE</name><type>FileExists</type>
              <filename>/Volumes/data/a.csv</filename>
              <resultfieldname>exists_flag</resultfieldname>
            </step>""", "_file_exists"),
            ("TableExists", """
            <step><name>TE</name><type>TableExists</type>
              <schemaname>analytics</schemaname><tablename>dim_date</tablename>
            </step>""", "_table_exists"),
            ("ColumnExists", """
            <step><name>CE</name><type>ColumnExists</type>
              <tablename>dim_date</tablename><columnname>date_key</columnname>
            </step>""", "_column_exists"),
        ):
            name = xml.strip().split("<name>")[1].split("</name>")[0]
            outcome = self.registry.convert_step(stype, _ctx(xml, stype, name))
            code = "\n".join(outcome.code_lines)
            self.assertIn(needle, code, msg=stype)
            self.assertTrue(_syntax_ok(outcome.code_lines), msg=stype)

    def test_file_locked_and_ws_available(self):
        locked = self.registry.convert_step("CheckFileLocked", _ctx("""
        <step><name>L</name><type>CheckFileLocked</type>
          <filename>/tmp/a.dat</filename></step>
        """, "CheckFileLocked", "L"))
        self.assertIn("_file_is_locked", "\n".join(locked.code_lines))
        self.assertIn("WARNING", "\n".join(locked.code_lines))
        self.assertTrue(_syntax_ok(locked.code_lines))

        avail = self.registry.convert_step("WebServiceAvailable", _ctx("""
        <step><name>W</name><type>WebServiceAvailable</type>
          <url>https://api.example/health</url>
          <connectTimeOut>3000</connectTimeOut>
        </step>
        """, "WebServiceAvailable", "W"))
        code = "\n".join(avail.code_lines)
        self.assertIn("requests.get", code)
        self.assertTrue(_syntax_ok(avail.code_lines))

    def test_http_rest_codegen(self):
        http = self.registry.convert_step("HTTP", _ctx("""
        <step><name>H</name><type>HTTP</type>
          <url>https://api.example/items</url>
          <fieldName>resp</fieldName>
          <resultcodefieldname>code</resultcodefieldname>
        </step>
        """, "HTTP", "H"))
        hcode = "\n".join(http.code_lines)
        self.assertIn("requests.request", hcode)
        self.assertIn("GET", hcode)
        self.assertTrue(_syntax_ok(http.code_lines))

        post = self.registry.convert_step("HTTPPOST", _ctx("""
        <step><name>P</name><type>HTTPPOST</type>
          <url>https://api.example/items</url>
          <requestEntity>body</requestEntity>
        </step>
        """, "HTTPPOST", "P"))
        self.assertIn("POST", "\n".join(post.code_lines))
        self.assertTrue(_syntax_ok(post.code_lines))

        rest = self.registry.convert_step("Rest", _ctx("""
        <step><name>R</name><type>Rest</type>
          <url>https://api.example/v1</url>
          <method>PUT</method>
          <bodyField>payload</bodyField>
          <resultfield>resp</resultfield>
        </step>
        """, "Rest", "R"))
        rcode = "\n".join(rest.code_lines)
        self.assertIn("requests.request", rcode)
        self.assertIn("PUT", rcode)
        self.assertTrue(_syntax_ok(rest.code_lines))

    def test_web_services_lookup_stub(self):
        outcome = self.registry.convert_step("WebServiceLookup", _ctx("""
        <step><name>SOAP</name><type>WebServiceLookup</type>
          <wsdlUrl>https://example/a?wsdl</wsdlUrl>
          <operationName>Ping</operationName>
          <soapAction>urn:Ping</soapAction>
          <fields><field><name>status</name><rename>svc_status</rename></field></fields>
        </step>
        """, "WebServiceLookup", "SOAP"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("wsdl", code.lower())
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_fuzzy_match_enhanced(self):
        xml = """
        <step>
          <name>FZ</name><type>FuzzyMatch</type>
          <algorithm>Levenshtein</algorithm>
          <mainstreamfield>cust_name</mainstreamfield>
          <lookupfield>party_name</lookupfield>
          <minimalValue>0.8</minimalValue>
          <maximalValue>1.0</maximalValue>
          <outputmatchvalue>score</outputmatchvalue>
          <closervalue>Y</closervalue>
        </step>
        """
        ctx = _ctx(xml, "FuzzyMatch", "FZ", extra_inputs=["Lookup"])
        # Ensure Input + Lookup both hop into FZ
        outcome = self.registry.convert_step("FuzzyMatch", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("crossJoin", code)
        self.assertIn("levenshtein(", code)
        self.assertIn("0.8", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_stream_lookup_return_and_cache(self):
        xml = """
        <step>
          <name>SL</name><type>StreamLookup</type>
          <cache>Y</cache>
          <cache_size>100</cache_size>
          <lookup>
            <key><name>region_code</name><field>region_code</field></key>
            <value><name>region_name</name><rename>region</rename><default>UNK</default></value>
          </lookup>
        </step>
        """
        ctx = _ctx(xml, "StreamLookup", "SL", extra_inputs=["Regions"])
        outcome = self.registry.convert_step("StreamLookup", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("broadcast(", code)
        self.assertIn(".join(", code)
        self.assertIn("cache", code.lower())
        self.assertIn("region", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_dblookup_alias(self):
        xml = """
        <step>
          <name>DL</name><type>DBLookup</type>
          <connection>dw</connection>
          <table>dim_region</table>
          <lookup>
            <key><name>code</name><field>region_code</field></key>
          </lookup>
          <value><name>region_name</name><rename>region</rename></value>
        </step>
        """
        outcome = self.registry.convert_step("DBLookup", _ctx(xml, "DBLookup", "DL"))
        self.assertNotEqual(outcome.status, "failed")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_all_lookup_types_smoke(self):
        samples = [
            ("DBProc", "<step><name>A</name><type>DBProc</type><procedure>p</procedure></step>"),
            ("DBJoin", "<step><name>A</name><type>DBJoin</type><sql>SELECT 1 AS id</sql></step>"),
            ("DynamicSQLRow", "<step><name>A</name><type>DynamicSQLRow</type><sql>SELECT 1</sql></step>"),
            ("FileExists", "<step><name>A</name><type>FileExists</type><filename>/x</filename></step>"),
            ("TableExists", "<step><name>A</name><type>TableExists</type><tablename>t</tablename></step>"),
            ("ColumnExists",
             "<step><name>A</name><type>ColumnExists</type><tablename>t</tablename><columnname>c</columnname></step>"),
            ("CheckFileLocked", "<step><name>A</name><type>CheckFileLocked</type><filename>/x</filename></step>"),
            ("WebServiceAvailable",
             "<step><name>A</name><type>WebServiceAvailable</type><url>https://x</url></step>"),
            ("HTTP", "<step><name>A</name><type>HTTP</type><url>https://x</url></step>"),
            ("HTTPPOST", "<step><name>A</name><type>HTTPPOST</type><url>https://x</url></step>"),
            ("Rest", "<step><name>A</name><type>Rest</type><url>https://x</url><method>GET</method></step>"),
            ("WebServiceLookup",
             "<step><name>A</name><type>WebServiceLookup</type><wsdlUrl>https://x?wsdl</wsdlUrl></step>"),
        ]
        for stype, xml in samples:
            outcome = self.registry.convert_step(stype, _ctx(xml, stype, "A"))
            self.assertNotEqual(outcome.status, "failed", msg=stype)
            self.assertTrue(_syntax_ok(outcome.code_lines), msg=stype)

    def test_validators_registered(self):
        for stype in (
            "DBJoin", "DBProc", "DynamicSQLRow", "FileExists", "TableExists",
            "ColumnExists", "CheckFileLocked", "WebServiceAvailable",
            "WebServiceLookup", "HTTP", "HTTPPOST", "Rest", "FuzzyMatch",
            "StreamLookup", "DBLookup",
        ):
            v = get_validator(stype)
            self.assertIsNotNone(v, msg=stype)

    def test_edge_missing_url_and_connection(self):
        http = self.registry.convert_step("HTTP", _ctx(
            "<step><name>H</name><type>HTTP</type></step>", "HTTP", "H"
        ))
        self.assertIn("WARNING", "\n".join(http.code_lines))
        self.assertTrue(_syntax_ok(http.code_lines))

        tbl = self.registry.convert_step("TableExists", _ctx(
            "<step><name>T</name><type>TableExists</type><tablename>t</tablename></step>",
            "TableExists", "T",
        ))
        self.assertIn("missing database connection", "\n".join(tbl.code_lines))

        proc = self.registry.convert_step("DBProc", _ctx(
            "<step><name>P</name><type>DBProc</type><procedure>sp_x</procedure>"
            "<lookup><argument><name>id</name><direction>IN</direction></argument></lookup></step>",
            "DBProc", "P",
        ))
        self.assertIn("missing database connection", "\n".join(proc.code_lines))
        self.assertIn("_in_bindings_", "\n".join(proc.code_lines))

    def test_ws_available_url_in_field_parse(self):
        cfg = parse_webservice_available_config(ET.fromstring("""
        <step>
          <urlInField>Y</urlInField>
          <urlField>endpoint</urlField>
          <url>https://fallback</url>
        </step>
        """))
        self.assertTrue(cfg["url_in_field"])
        self.assertEqual(cfg["url_field"], "endpoint")

    def test_fuzzy_unsupported_algorithm_warning(self):
        xml = """
        <step>
          <name>FZ</name><type>FuzzyMatch</type>
          <algorithm>NeedlemanWunsch</algorithm>
          <mainstreamfield>a</mainstreamfield>
          <lookupfield>b</lookupfield>
          <minimalValue>0.5</minimalValue>
        </step>
        """
        outcome = self.registry.convert_step(
            "FuzzyMatch", _ctx(xml, "FuzzyMatch", "FZ", extra_inputs=["Lookup"])
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("_fm_main_key", code)
        self.assertIn("levenshtein(", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_stream_lookup_null_key_filter(self):
        xml = """
        <step>
          <name>SL</name><type>StreamLookup</type>
          <lookup>
            <key><name>k</name><field>k</field></key>
            <value><name>v</name><rename>out_v</rename></value>
          </lookup>
        </step>
        """
        outcome = self.registry.convert_step(
            "StreamLookup", _ctx(xml, "StreamLookup", "SL", extra_inputs=["Lkp"])
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("isNotNull()", code)
        self.assertIn("out_v", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_file_exists_with_filetype(self):
        outcome = self.registry.convert_step("FileExists", _ctx("""
        <step><name>FE</name><type>FileExists</type>
          <filename>/Volumes/data/a.csv</filename>
          <includefiletype>Y</includefiletype>
          <filetypefieldname>ft</filetypefieldname>
        </step>
        """, "FileExists", "FE"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("_file_type_of", code)
        self.assertIn("'ft'", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_dblookup_preserves_connection_warning(self):
        xml = """
        <step>
          <name>DL</name><type>DatabaseLookup</type>
          <connection>dw</connection>
          <table>dim_region</table>
          <cached>Y</cached>
          <cache_size>50</cache_size>
          <lookup>
            <key><name>code</name><field>region_code</field></key>
          </lookup>
          <value><name>region_name</name><rename>region</rename></value>
        </step>
        """
        outcome = self.registry.convert_step("DatabaseLookup", _ctx(xml, "DatabaseLookup", "DL"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("preserved.connection", code)
        self.assertIn("broadcast", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
