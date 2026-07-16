"""Tests for Pentaho Bulk Loading transformation migration."""

from __future__ import annotations

import ast
import logging
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import parse_bulk_loader_config, parse_step_metadata
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.bulk_loading_handlers import BULK_LOADING_HANDLERS
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import parse_step_config, register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="BulkTrans", file_path=Path("bulk.ktr"))
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


def _convert(registry, step_type: str, xml: str, name: str = "bulk", with_input: bool = True):
    outcome = registry.convert_step(step_type, _ctx(xml, step_type, name, with_input=with_input))
    code = "\n".join(outcome.code_lines)
    return outcome, code


_FULL_PG_XML = """
<step>
  <connection>PostgresConn</connection>
  <schema>public</schema>
  <table>orders</table>
  <loadMethod>AUTO_END</loadMethod>
  <loadAction>TRUNCATE</loadAction>
  <dbNameOverride></dbNameOverride>
  <enclosure>"</enclosure>
  <delimiter>;</delimiter>
  <escape>\\</escape>
  <nullif>\\\\N</nullif>
  <encoding>UTF-8</encoding>
  <stopOnError>Y</stopOnError>
  <maxErrors>50</maxErrors>
  <commit>10000</commit>
  <psqlPath>/usr/bin/psql</psqlPath>
  <eraseFiles>Y</eraseFiles>
  <mapping>
    <stream_name>order_id</stream_name>
    <field_name>ORDER_ID</field_name>
  </mapping>
  <mapping>
    <stream_name>amount</stream_name>
    <field_name>AMOUNT</field_name>
  </mapping>
  <customVendorFlag>Y</customVendorFlag>
</step>
"""


class TestBulkLoaderParsers(unittest.TestCase):
    def test_parse_postgresql_full(self):
        el = ET.fromstring(textwrap.dedent(_FULL_PG_XML).strip())
        cfg = parse_bulk_loader_config(el)
        self.assertEqual(cfg["connection"], "PostgresConn")
        self.assertEqual(cfg["schema"], "public")
        self.assertEqual(cfg["table"], "orders")
        self.assertEqual(cfg["load_method"], "AUTO_END")
        self.assertEqual(cfg["load_action"], "TRUNCATE")
        self.assertEqual(cfg["truncate"], "Y")
        self.assertEqual(cfg["delimiter"], ";")
        self.assertEqual(cfg["enclosure"], '"')
        self.assertEqual(cfg["escape_char"], "\\")
        self.assertEqual(cfg["null_string"], "\\\\N")
        self.assertEqual(cfg["encoding"], "UTF-8")
        self.assertEqual(cfg["commit_size"], "10000")
        self.assertEqual(cfg["max_errors"], "50")
        self.assertEqual(cfg["psql_path"], "/usr/bin/psql")
        self.assertEqual(len(cfg["fields"]), 2)
        self.assertEqual(cfg["fields"][0]["stream_field"], "order_id")
        self.assertEqual(cfg["fields"][0]["table_field"], "ORDER_ID")
        self.assertEqual(cfg["extras"].get("customVendorFlag"), "Y")

    def test_parse_oracle_sqlldr_paths(self):
        xml = """
        <step>
          <connection>Ora</connection>
          <schema>HR</schema>
          <table>EMPLOYEES</table>
          <sqlldr>/opt/oracle/bin/sqlldr</sqlldr>
          <controlFile>/tmp/emp.ctl</controlFile>
          <dataFile>/tmp/emp.dat</dataFile>
          <logFile>/tmp/emp.log</logFile>
          <badFile>/tmp/emp.bad</badFile>
          <discardFile>/tmp/emp.dsc</discardFile>
          <direct>Y</direct>
          <parallel>Y</parallel>
          <bindSize>256000</bindSize>
          <readSize>1048576</readSize>
          <maxErrors>0</maxErrors>
          <loadMethod>AUTOMATIC</loadMethod>
          <loadAction>APPEND</loadAction>
          <commit>5000</commit>
        </step>
        """
        cfg = parse_bulk_loader_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["sqlldr_path"], "/opt/oracle/bin/sqlldr")
        self.assertEqual(cfg["control_file"], "/tmp/emp.ctl")
        self.assertEqual(cfg["data_file"], "/tmp/emp.dat")
        self.assertEqual(cfg["bad_file"], "/tmp/emp.bad")
        self.assertEqual(cfg["direct"], "Y")
        self.assertEqual(cfg["parallel"], "Y")
        self.assertEqual(cfg["bind_size"], "256000")
        self.assertEqual(cfg["commit_size"], "5000")
        self.assertEqual(cfg["load_action"], "APPEND")
        self.assertEqual(cfg["truncate"], "N")

    def test_parse_mysql_fifo(self):
        xml = """
        <step>
          <connection>MySQL</connection>
          <schema>sales</schema>
          <table>facts</table>
          <fifoFileName>/tmp/mysql.fifo</fifoFileName>
          <delimiter>,</delimiter>
          <enclosure>"</enclosure>
          <escape>\\</escape>
          <charset>utf8mb4</charset>
          <bulkSize>50000</bulkSize>
          <localInfile>Y</localInfile>
          <replace>N</replace>
          <ignore>Y</ignore>
        </step>
        """
        cfg = parse_bulk_loader_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["fifo_file"], "/tmp/mysql.fifo")
        self.assertEqual(cfg["batch_size"], "50000")
        self.assertEqual(cfg["local_infile"], "Y")
        self.assertEqual(cfg["ignore_duplicates"], "Y")
        self.assertEqual(cfg["encoding"], "utf8mb4")

    def test_parse_greenplum_and_infobright(self):
        gp = parse_bulk_loader_config(
            ET.fromstring(
                textwrap.dedent("""
                <step>
                  <connection>GP</connection>
                  <schema>public</schema>
                  <table>gp_tgt</table>
                  <gploadPath>/usr/local/greenplum/bin/gpload</gploadPath>
                  <controlFile>/tmp/gp.ctrl</controlFile>
                  <maxErrors>10</maxErrors>
                  <eraseFiles>Y</eraseFiles>
                  <loadAction>TRUNCATE</loadAction>
                </step>
                """).strip()
            )
        )
        self.assertEqual(gp["gpload_path"], "/usr/local/greenplum/bin/gpload")
        self.assertEqual(gp["control_file"], "/tmp/gp.ctrl")
        self.assertEqual(gp["max_errors"], "10")

        ib = parse_bulk_loader_config(
            ET.fromstring(
                textwrap.dedent("""
                <step>
                  <connection>IB</connection>
                  <table>ib_fact</table>
                  <agentHost>ib-agent</agentHost>
                  <agentPort>5555</agentPort>
                  <dataFile>/tmp/ib.dat</dataFile>
                  <bulkSize>20000</bulkSize>
                </step>
                """).strip()
            )
        )
        self.assertEqual(ib["agent_host"], "ib-agent")
        self.assertEqual(ib["agent_port"], "5555")
        self.assertEqual(ib["batch_size"], "20000")

    def test_parse_tpt_operator_and_keys(self):
        xml = """
        <step>
          <connection>TD</connection>
          <table>dim_cust</table>
          <tptOperator>Update</tptOperator>
          <tbuildPath>/opt/teradata/client/tbuild</tbuildPath>
          <jobName>load_dim_cust</jobName>
          <loadAction>Upsert</loadAction>
          <packFactor>500</packFactor>
          <maxSessions>8</maxSessions>
          <errorTable>err_dim</errorTable>
          <logTable>log_dim</logTable>
          <keys>
            <key><name>CUST_ID</name></key>
          </keys>
          <mapping>
            <stream_name>cust_id</stream_name>
            <field_name>CUST_ID</field_name>
            <key>Y</key>
          </mapping>
          <mapping>
            <stream_name>name</stream_name>
            <field_name>CUST_NAME</field_name>
            <update>Y</update>
          </mapping>
        </step>
        """
        cfg = parse_bulk_loader_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["tpt_operator"], "Update")
        self.assertEqual(cfg["tbuild_path"], "/opt/teradata/client/tbuild")
        self.assertEqual(cfg["tpt_job_name"], "load_dim_cust")
        self.assertEqual(cfg["pack_factor"], "500")
        self.assertEqual(cfg["max_sessions"], "8")
        self.assertIn("CUST_ID", cfg["key_fields"])

    def test_parse_teradata_and_vertica(self):
        td = ET.fromstring(
            textwrap.dedent("""
            <step>
              <connection>TD</connection>
              <table>staging</table>
              <errorTable>err_staging</errorTable>
              <logTable>log_staging</logTable>
              <sessions>4</sessions>
              <fastloadPath>/opt/teradata/fastload</fastloadPath>
              <tptPath>/opt/teradata/tpt</tptPath>
              <packFactor>1000</packFactor>
            </step>
            """).strip()
        )
        cfg = parse_bulk_loader_config(td)
        self.assertEqual(cfg["error_table"], "err_staging")
        self.assertEqual(cfg["sessions"], "4")
        self.assertEqual(cfg["fastload_path"], "/opt/teradata/fastload")
        self.assertEqual(cfg["tpt_path"], "/opt/teradata/tpt")

        vert = ET.fromstring(
            "<step><connection>V</connection><schema>public</schema>"
            "<table>vfact</table><copyStatement>COPY vfact FROM STDIN</copyStatement>"
            "<streamName>bulk</streamName><batchSize>25000</batchSize></step>"
        )
        vcfg = parse_bulk_loader_config(vert)
        self.assertEqual(vcfg["copy_statement"], "COPY vfact FROM STDIN")
        self.assertEqual(vcfg["stream_name"], "bulk")
        self.assertEqual(vcfg["batch_size"], "25000")

    def test_parse_monetdb_and_vectorwise(self):
        monet = parse_bulk_loader_config(
            ET.fromstring(
                textwrap.dedent("""
                <step>
                  <connection>M</connection>
                  <schema>sys</schema>
                  <table>t</table>
                  <mclientPath>/usr/bin/mclient</mclientPath>
                  <copyStatement>COPY INTO t FROM STDIN</copyStatement>
                  <bufferSize>65536</bufferSize>
                  <encoding>UTF-8</encoding>
                </step>
                """).strip()
            )
        )
        self.assertEqual(monet["mclient_path"], "/usr/bin/mclient")
        self.assertEqual(monet["buffer_size"], "65536")

        vw = parse_bulk_loader_config(
            ET.fromstring(
                textwrap.dedent("""
                <step>
                  <connection>VW</connection>
                  <schema>ingres</schema>
                  <table>vw_tgt</table>
                  <vwloadPath>/opt/ingres/bin/vwload</vwloadPath>
                  <batchSize>1000</batchSize>
                  <bufferSize>8192</bufferSize>
                </step>
                """).strip()
            )
        )
        self.assertEqual(vw["connection"], "VW")
        self.assertEqual(vw["vwload_path"], "/opt/ingres/bin/vwload")
        self.assertEqual(vw["batch_size"], "1000")

    def test_parse_step_metadata_dispatch(self):
        el = ET.fromstring(
            "<step><connection>c</connection><schema>s</schema><table>t</table></step>"
        )
        for step_type in (
            "PGBulkLoader",
            "OracleBulkLoader",
            "MySQLBulkLoader",
            "GPBulkLoader",
            "VerticaBulkLoader",
            "MonetDBBulkLoader",
            "InfobrightLoader",
            "VectorWiseBulkLoader",
            "TeraFast",
            "TeraDataBulkLoader",
        ):
            meta = parse_step_metadata(el, step_type)
            self.assertEqual(meta.get("table"), "t", msg=step_type)
            self.assertEqual(meta.get("connection"), "c", msg=step_type)


class TestBulkLoaderHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertEqual(len(BULK_LOADING_HANDLERS), 10)
        for handler in BULK_LOADING_HANDLERS:
            self.assertTrue(handler._TYPES)

    def test_all_vendor_types_dispatch(self):
        cases = [
            ("PGBulkLoader", "PostgreSQL Bulk Loader"),
            ("OracleBulkLoader", "Oracle Bulk Loader"),
            ("MySQLBulkLoader", "MySQL Bulk Loader"),
            ("GPBulkLoader", "Greenplum Load"),
            ("VerticaBulkLoader", "Vertica Bulk Loader"),
            ("MonetDBBulkLoader", "MonetDB Bulk Loader"),
            ("InfobrightLoader", "Infobright Loader"),
            ("VectorWiseBulkLoader", "Ingres VectorWise Bulk Loader"),
            ("TeraFast", "Teradata FastLoad Bulk Loader"),
            ("TeraDataBulkLoader", "Teradata TPT Bulk Loader"),
            ("PostgreSQL Bulk Loader", "PostgreSQL Bulk Loader"),
        ]
        xml = """
        <step>
          <connection>db</connection>
          <schema>public</schema>
          <table>tgt</table>
          <loadAction>INSERT</loadAction>
          <batchSize>1000</batchSize>
        </step>
        """
        for step_type, _label in cases:
            outcome, code = _convert(self.registry, step_type, xml, f"step_{step_type}")
            self.assertIn("saveAsTable", code, msg=step_type)
            self.assertIn("format('jdbc')", code, msg=step_type)
            self.assertIn("JDBC FALLBACK", code, msg=step_type)
            self.assertIn("UNSUPPORTED", code, msg=step_type)
            self.assertIn("WARNING", code, msg=step_type)
            self.assertIn(outcome.status, ("converted", "partial"), msg=step_type)
            self.assertTrue(_syntax_ok(code), msg=f"{step_type}: {code}")

    def test_postgresql_field_mapping_jdbc_and_nulls(self):
        outcome, code = _convert(self.registry, "PGBulkLoader", _FULL_PG_XML, "PG load")
        self.assertIn("col('order_id').alias('ORDER_ID')", code)
        self.assertIn(".mode('overwrite')", code)
        self.assertIn("preserved.psql_path=", code)
        self.assertIn("preserved.connection=", code)
        self.assertIn("Reject limits", code)
        self.assertIn("org.postgresql.Driver", code)
        self.assertIn(".replace(", code)
        self.assertIn("format('jdbc')", code)
        self.assertTrue(_syntax_ok(code))

    def test_oracle_sqlldr_jdbc_fallback(self):
        xml = """
        <step>
          <connection>Ora</connection>
          <schema>HR</schema>
          <table>EMP</table>
          <dataFile>/tmp/emp.dat</dataFile>
          <sqlldr>/bin/sqlldr</sqlldr>
          <commit>2000</commit>
          <loadAction>APPEND</loadAction>
        </step>
        """
        _, code = _convert(self.registry, "OraBulkLoader", xml, "Ora load")
        self.assertIn("COPY INTO", code)
        self.assertIn("/tmp/emp.dat", code)
        self.assertIn(".mode('append')", code)
        self.assertIn("sqlldr", code.lower())
        self.assertIn("oracle.jdbc.OracleDriver", code)
        self.assertIn("batchsize", code)
        self.assertIn("2000", code)
        self.assertTrue(_syntax_ok(code))

    def test_mysql_load_data_jdbc_writer(self):
        xml = """
        <step>
          <connection>MySQL</connection>
          <schema>sales</schema>
          <table>facts</table>
          <fifoFileName>/tmp/mysql.fifo</fifoFileName>
          <localInfile>Y</localInfile>
          <bulkSize>50000</bulkSize>
          <charset>utf8mb4</charset>
        </step>
        """
        _, code = _convert(self.registry, "MySQLBulkLoader", xml, "MySQL load")
        self.assertIn("LOAD DATA", code)
        self.assertIn("fifo", code.lower())
        self.assertIn("com.mysql.cj.jdbc.Driver", code)
        self.assertIn("batchsize", code)
        self.assertIn("50000", code)
        self.assertIn("saveAsTable", code)
        self.assertTrue(_syntax_ok(code))

    def test_greenplum_jdbc_fallback(self):
        xml = """
        <step>
          <connection>GP</connection>
          <schema>public</schema>
          <table>gp_tgt</table>
          <gploadPath>/bin/gpload</gploadPath>
          <maxErrors>5</maxErrors>
        </step>
        """
        _, code = _convert(self.registry, "GPBulkLoader", xml, "GP load")
        self.assertIn("gpload", code.lower())
        self.assertIn("org.postgresql.Driver", code)
        self.assertIn("JDBC FALLBACK", code)
        self.assertTrue(_syntax_ok(code))

    def test_monetdb_copy_and_jdbc(self):
        xml = """
        <step>
          <connection>M</connection>
          <schema>sys</schema>
          <table>mt</table>
          <mclientPath>/usr/bin/mclient</mclientPath>
          <copyStatement>COPY INTO mt FROM STDIN</copyStatement>
          <bufferSize>4096</bufferSize>
        </step>
        """
        _, code = _convert(self.registry, "MonetDBBulkLoader", xml, "Monet load")
        self.assertIn("mclient", code.lower())
        self.assertIn("nl.cwi.monetdb.jdbc.MonetDriver", code)
        self.assertTrue(_syntax_ok(code))

    def test_vertica_copy_jdbc(self):
        xml = """
        <step>
          <connection>V</connection>
          <schema>public</schema>
          <table>vfact</table>
          <copyStatement>COPY vfact FROM STDIN</copyStatement>
          <streamName>bulk</streamName>
          <batchSize>25000</batchSize>
        </step>
        """
        _, code = _convert(self.registry, "VerticaBulkLoader", xml, "Vertica load")
        self.assertIn("VerticaCopyStream", code)
        self.assertIn("com.vertica.jdbc.Driver", code)
        self.assertIn("25000", code)
        self.assertTrue(_syntax_ok(code))

    def test_teradata_fastload_sessions_unsupported(self):
        xml = """
        <step>
          <connection>TD</connection>
          <table>stg</table>
          <fastloadPath>/opt/fastload</fastloadPath>
          <sessions>6</sessions>
          <errorTable>err_stg</errorTable>
        </step>
        """
        _, code = _convert(self.registry, "TeraFast", xml, "FL load")
        self.assertIn("FastLoad", code)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("sessions", code.lower())
        self.assertIn("err_stg", code)
        self.assertTrue(_syntax_ok(code))

    def test_teradata_tpt_operator_warnings(self):
        xml = """
        <step>
          <connection>TD</connection>
          <table>dim_cust</table>
          <tptOperator>Update</tptOperator>
          <tbuildPath>/opt/tbuild</tbuildPath>
          <jobName>job1</jobName>
          <loadAction>Upsert</loadAction>
          <packFactor>100</packFactor>
          <keys><key><name>ID</name></key></keys>
        </step>
        """
        _, code = _convert(self.registry, "TeraDataBulkLoader", xml, "TPT load")
        self.assertIn("TPT", code)
        self.assertIn("Update", code)
        self.assertIn("WARNING", code)
        self.assertIn("pack", code.lower())
        self.assertIn("'ID'", code)
        self.assertTrue(_syntax_ok(code))

    def test_infobright_and_vectorwise_preservation(self):
        ib_xml = """
        <step>
          <connection>IB</connection>
          <table>ib_t</table>
          <agentHost>h</agentHost>
          <agentPort>1</agentPort>
          <bulkSize>10</bulkSize>
        </step>
        """
        _, ib_code = _convert(self.registry, "InfobrightLoader", ib_xml, "IB")
        self.assertIn("Infobright", ib_code)
        self.assertIn("UNSUPPORTED", ib_code)
        self.assertIn("agent", ib_code.lower())

        vw_xml = """
        <step>
          <connection>VW</connection>
          <schema>ingres</schema>
          <table>vw_t</table>
          <vwloadPath>/bin/vwload</vwloadPath>
          <batchSize>99</batchSize>
        </step>
        """
        _, vw_code = _convert(self.registry, "VectorWiseBulkLoader", vw_xml, "VW")
        self.assertIn("VectorWise", vw_code)
        self.assertIn("vwload", vw_code.lower())
        self.assertIn("ingres.jdbc", vw_code)
        self.assertTrue(_syntax_ok(vw_code))

    def test_missing_input_empty_dataset(self):
        xml = """
        <step>
          <connection>db</connection>
          <table>tgt</table>
        </step>
        """
        outcome, code = _convert(
            self.registry, "MySQLBulkLoader", xml, "MySQL load", with_input=False
        )
        self.assertIn("empty dataset", code.lower())
        self.assertIn("createDataFrame", code)
        self.assertIn(outcome.status, ("partial", "converted"))
        self.assertTrue(_syntax_ok(code))

    def test_missing_table_warning(self):
        xml = """
        <step>
          <connection>db</connection>
          <schema>public</schema>
        </step>
        """
        _, code = _convert(self.registry, "VerticaBulkLoader", xml, "Vertica load")
        self.assertIn("Target table name missing", code)
        self.assertIn("saveAsTable", code)
        self.assertTrue(_syntax_ok(code))

    def test_error_handling_passthrough(self):
        """Forced handler error path should continue pipeline with partial status."""
        from pentaho_converter.steps import bulk_loading_handlers as mod

        handler = mod.PostgreSQLBulkLoaderHandler()
        ctx = _ctx(_FULL_PG_XML, "PGBulkLoader", "PG load")

        original = mod.convert_bulk_loader_step

        def boom(*_a, **_k):
            raise RuntimeError("forced failure")

        mod.convert_bulk_loader_step = boom  # type: ignore[assignment]
        try:
            with self.assertLogs(mod.logger, level=logging.ERROR):
                lines, status = handler.generate_code(ctx)
        finally:
            mod.convert_bulk_loader_step = original  # type: ignore[assignment]

        code = "\n".join(lines)
        self.assertEqual(status, "partial")
        self.assertIn("ERROR", code)
        self.assertIn("df_PG_load = df_Input", code)

    def test_validator_accepts_bulk_loader(self):
        register_builtin_validators()
        outcome, _ = _convert(self.registry, "PGBulkLoader", _FULL_PG_XML, "PG load")
        ctx = _ctx(_FULL_PG_XML, "PGBulkLoader", "PG load")
        parsed = parse_step_config(ctx)
        validator = get_validator("PGBulkLoader")
        self.assertIsNotNone(validator)
        result = validator.validate(ctx, parsed, outcome.code_lines)
        self.assertFalse(result.errors, msg=result.errors)
        self.assertIn("bulk_loader", result.properties_converted)

    def test_no_duplicate_handler_logic_in_registry(self):
        """Each bulk-loader alias should resolve to a dedicated converter."""
        aliases = {
            "pgbulkloader",
            "mysqlbulkloader",
            "orabulkloader",
            "gpbulkloader",
            "verticabulkloader",
            "monetdbbulkloader",
            "infobrightloader",
            "vectorwisebulkloader",
            "terafast",
            "teradatabulkloader",
        }
        for alias in aliases:
            conv = self.registry.get_converter(alias)
            self.assertIsNotNone(conv, msg=alias)
            self.assertNotEqual(getattr(conv, "converter_name", ""), "Fallback", msg=alias)


if __name__ == "__main__":
    unittest.main()
