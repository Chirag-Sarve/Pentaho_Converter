"""Unit tests for Bulk Loading Pentaho Job Entries.

Covers MYSQL_BULK_FILE, MYSQL_BULK_LOAD, MSSQL_BULK_LOAD —
parser coverage, Spark/JDBC mocked success/failure, warnings, variables.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import bulk_ops as bops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_mssql_bulk_load,
    handle_mysql_bulk_file,
    handle_mysql_bulk_load,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime


def _runtime(
    *,
    variables: dict | None = None,
    spark: object | None = None,
    connections: dict | None = None,
) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=spark,
        cfg={},
        entry_types={"MYSQL_BULK_FILE", "MYSQL_BULK_LOAD", "MSSQL_BULK_LOAD"},
        trans_runners={},
        child_job_modules={},
    )
    rt = JobRuntime(
        name="BulkTestJob",
        entries=[],
        hops=[],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )
    rt.spark = spark
    rt.connections = dict(connections or {})
    return rt


_BULK_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>BulkSample</name>
  <connection>
    <name>MySQL_Src</name>
    <server>${DB_HOST}</server>
    <type>MYSQL</type>
    <database>retail</database>
    <port>3306</port>
    <username>etl</username>
    <password>${DB_PASS}</password>
  </connection>
  <connection>
    <name>MSSQL_Tgt</name>
    <server>${MSSQL_HOST}</server>
    <type>MSSQLNATIVE</type>
    <database>dw</database>
    <port>1433</port>
    <username>sa</username>
    <password>x</password>
  </connection>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>Export</name>
      <type>MYSQL_BULK_FILE</type>
      <connection>MySQL_Src</connection>
      <schemaname>${SCHEMA}</schemaname>
      <tablename>customers</tablename>
      <filename>${OUT}/customers_${RUN_ID}.csv</filename>
      <separator>,</separator>
      <enclosed>"</enclosed>
      <optionenclosed>N</optionenclosed>
      <lineterminated>\\n</lineterminated>
      <limitlines>1000</limitlines>
      <listcolumn>id,name,email</listcolumn>
      <highpriority>Y</highpriority>
      <outdumpvalue>0</outdumpvalue>
      <iffileexists>3</iffileexists>
      <addfiletoresult>Y</addfiletoresult>
    </entry>
    <entry>
      <name>LoadMy</name>
      <type>MYSQL_BULK_LOAD</type>
      <connection>MySQL_Src</connection>
      <schemaname></schemaname>
      <tablename>staging_customers</tablename>
      <filename>${OUT}/customers_${RUN_ID}.csv</filename>
      <separator>,</separator>
      <enclosed>"</enclosed>
      <escaped>\\</escaped>
      <replacedata>Y</replacedata>
      <ignorelines>1</ignorelines>
      <listattribut>id,name,email</listattribut>
      <localinfile>Y</localinfile>
      <prorityvalue>0</prorityvalue>
      <addfiletoresult>N</addfiletoresult>
    </entry>
    <entry>
      <name>LoadMs</name>
      <type>MSSQL_BULK_LOAD</type>
      <connection>MSSQL_Tgt</connection>
      <schemaname>dbo</schemaname>
      <tablename>customers</tablename>
      <filename>${OUT}/customers_${RUN_ID}.csv</filename>
      <datafiletype>char</datafiletype>
      <fieldterminator>,</fieldterminator>
      <lineterminated>\\n</lineterminated>
      <codepage>ACP</codepage>
      <keepidentity>N</keepidentity>
      <tablock>Y</tablock>
      <truncate>Y</truncate>
      <batchsize>1000</batchsize>
      <maxerrors>0</maxerrors>
      <addfiletoresult>N</addfiletoresult>
    </entry>
  </entries>
  <hops>
    <hop><from>Start</from><to>Export</to><enabled>Y</enabled><unconditional>Y</unconditional></hop>
  </hops>
</job>
"""


class TestBulkParser(unittest.TestCase):
    def test_parses_all_bulk_entries_and_connections(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bulk.kjb"
            path.write_text(_BULK_KJB, encoding="utf-8")
            job = parse_job(path)

        self.assertIn("MySQL_Src", job.connections)
        self.assertEqual(job.connections["MySQL_Src"]["type"], "MYSQL")
        self.assertIn("MSSQL_Tgt", job.connections)

        by_name = {e.name: e for e in job.entries}
        exp = by_name["Export"]
        self.assertEqual(exp.entry_type, "MYSQL_BULK_FILE")
        self.assertEqual(exp.attributes["listcolumn"], "id,name,email")
        self.assertEqual(exp.attributes["iffileexists"], "3")
        self.assertEqual(exp.attributes["limitlines"], "1000")

        load = by_name["LoadMy"]
        self.assertEqual(load.entry_type, "MYSQL_BULK_LOAD")
        self.assertEqual(load.attributes["replacedata"], "Y")
        self.assertEqual(load.attributes["localinfile"], "Y")
        self.assertEqual(load.attributes["ignorelines"], "1")

        mssql = by_name["LoadMs"]
        self.assertEqual(mssql.entry_type, "MSSQL_BULK_LOAD")
        self.assertEqual(mssql.attributes["truncate"], "Y")
        self.assertEqual(mssql.attributes["batchsize"], "1000")
        self.assertEqual(mssql.attributes["tablock"], "Y")


class TestMysqlBulkFile(unittest.TestCase):
    def test_missing_table_fails(self):
        out = bops.mysql_bulk_file(
            connection_meta={},
            table="",
            filename="/tmp/x.csv",
        )
        self.assertFalse(out.success)

    def test_existing_file_fail_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            path.write_text("x", encoding="utf-8")
            out = bops.mysql_bulk_file(
                connection_meta={"type": "MYSQL", "server": "h", "database": "d", "port": "3306"},
                table="t",
                filename=str(path),
                iffileexists="2",
            )
            self.assertFalse(out.success)

    def test_spark_jdbc_export_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = Path(tmp) / "out.csv"
            spark = MagicMock()
            df = MagicMock()
            df.count.return_value = 2
            writer = MagicMock()
            df.write.mode.return_value = writer
            writer.option.return_value = writer
            # After csv write, create fake part file
            part_dir = Path(str(out_file) + ".spark_out")

            def _csv(path):
                p = Path(path)
                p.mkdir(parents=True, exist_ok=True)
                (p / "part-0000").write_text("1,a\n2,b\n", encoding="utf-8")

            writer.csv.side_effect = _csv
            spark.read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = df

            # Chain option calls for jdbc reader
            reader = MagicMock()
            spark.read.format.return_value = reader
            reader.option.return_value = reader
            reader.load.return_value = df

            conn = {
                "type": "MYSQL",
                "server": "localhost",
                "port": "3306",
                "database": "retail",
                "username": "u",
                "password": "p",
            }
            out = bops.mysql_bulk_file(
                connection_meta=conn,
                connection_name="MySQL_Src",
                schema="retail",
                table="customers",
                filename=str(out_file),
                separator=",",
                enclosed='"',
                listcolumn="id,name",
                limitlines="10",
                iffileexists="3",
                highpriority=True,
                spark=spark,
            )
            self.assertTrue(out.success)
            self.assertTrue(out_file.exists())
            self.assertEqual(out.row_count, 2)
            self.assertTrue(any("highpriority" in w for w in out.warnings))

    def test_handler_variable_substitution_and_result_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_file = Path(tmp) / "c_R1.csv"
            spark = MagicMock()
            df = MagicMock()
            df.count.return_value = 1
            reader = MagicMock()
            spark.read.format.return_value = reader
            reader.option.return_value = reader
            reader.load.return_value = df
            writer = MagicMock()
            df.write.mode.return_value = writer
            writer.option.return_value = writer

            def _csv(path):
                p = Path(path)
                p.mkdir(parents=True, exist_ok=True)
                (p / "part-0000").write_bytes(b"1\n")

            writer.csv.side_effect = _csv

            rt = _runtime(
                variables={"OUT": tmp, "RUN_ID": "R1", "SCHEMA": "retail"},
                spark=spark,
                connections={
                    "MySQL_Src": {
                        "type": "MYSQL",
                        "server": "localhost",
                        "port": "3306",
                        "database": "retail",
                        "username": "u",
                        "password": "p",
                    }
                },
            )
            res = handle_mysql_bulk_file(
                rt,
                JobEntry(
                    name="Export",
                    entry_type="MYSQL_BULK_FILE",
                    attributes={
                        "connection": "MySQL_Src",
                        "schemaname": "${SCHEMA}",
                        "tablename": "customers",
                        "filename": "${OUT}/c_${RUN_ID}.csv",
                        "separator": ",",
                        "iffileexists": "3",
                        "addfiletoresult": "Y",
                    },
                ),
            )
            self.assertTrue(res.success)
            self.assertTrue(out_file.exists())
            self.assertEqual(len(rt.result_filenames), 1)


class TestMysqlBulkLoad(unittest.TestCase):
    def test_missing_file_fails(self):
        out = bops.mysql_bulk_load(
            connection_meta={"type": "MYSQL", "server": "h", "database": "d"},
            table="t",
            filename=str(Path(tempfile.gettempdir()) / "no_such_bulk_file.csv"),
        )
        self.assertFalse(out.success)

    def test_spark_load_success_and_localinfile_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "in.csv"
            src.write_text("1,a\n2,b\n", encoding="utf-8")
            spark = MagicMock()
            df = MagicMock()
            df.columns = ["_c0", "_c1"]
            df.count.return_value = 2
            df.withColumnRenamed.return_value = df
            df.select.return_value = df
            reader = MagicMock()
            spark.read.format.return_value = reader
            reader.option.return_value = reader
            reader.load.return_value = df
            writer = MagicMock()
            df.write.format.return_value = writer
            writer.option.return_value = writer
            writer.mode.return_value = writer

            out = bops.mysql_bulk_load(
                connection_meta={
                    "type": "MYSQL",
                    "server": "localhost",
                    "port": "3306",
                    "database": "retail",
                    "username": "u",
                    "password": "p",
                },
                table="staging",
                filename=str(src),
                separator=",",
                enclosed='"',
                replacedata=True,
                localinfile=True,
                listattribut="id,name",
                ignorelines="0",
                spark=spark,
            )
            self.assertTrue(out.success)
            self.assertEqual(out.row_count, 2)
            self.assertTrue(any("localinfile" in w for w in out.warnings))
            writer.save.assert_called()


class TestMssqlBulkLoad(unittest.TestCase):
    def test_bcp_options_warn_and_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "in.csv"
            src.write_text("1,a\n", encoding="utf-8")
            spark = MagicMock()
            df = MagicMock()
            df.count.return_value = 1
            reader = MagicMock()
            spark.read.format.return_value = reader
            reader.option.return_value = reader
            reader.load.return_value = df
            writer = MagicMock()
            df.write.format.return_value = writer
            writer.option.return_value = writer
            writer.mode.return_value = writer
            spark.sql.side_effect = Exception("no truncate")

            out = bops.mssql_bulk_load(
                connection_meta={
                    "type": "MSSQLNATIVE",
                    "server": "localhost",
                    "port": "1433",
                    "database": "dw",
                    "username": "sa",
                    "password": "x",
                },
                schema="dbo",
                table="customers",
                filename=str(src),
                fieldterminator=",",
                truncate=True,
                tablock=True,
                keepidentity=True,
                firetriggers=True,
                batchsize="500",
                spark=spark,
            )
            self.assertTrue(out.success)
            self.assertTrue(any("tablock" in w for w in out.warnings))
            self.assertTrue(any("keepidentity" in w for w in out.warnings))
            self.assertTrue(any("firetriggers" in w for w in out.warnings))

    def test_handler_failure_without_spark(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "in.csv"
            src.write_text("1\n", encoding="utf-8")
            rt = _runtime(
                connections={
                    "MSSQL_Tgt": {
                        "type": "MSSQLNATIVE",
                        "server": "localhost",
                        "port": "1433",
                        "database": "dw",
                    }
                }
            )
            res = handle_mssql_bulk_load(
                rt,
                JobEntry(
                    name="LoadMs",
                    entry_type="MSSQL_BULK_LOAD",
                    attributes={
                        "connection": "MSSQL_Tgt",
                        "tablename": "t",
                        "filename": str(src),
                        "fieldterminator": ",",
                    },
                ),
            )
            self.assertFalse(res.success)


class TestRegistration(unittest.TestCase):
    def test_handlers_registered(self):
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types={"MYSQL_BULK_FILE", "MYSQL_BULK_LOAD", "MSSQL_BULK_LOAD"},
            trans_runners={},
            child_job_modules={},
        )
        self.assertEqual(handlers["MYSQL_BULK_FILE"].__name__, "handle_mysql_bulk_file")
        self.assertEqual(handlers["MYSQL_BULK_LOAD"].__name__, "handle_mysql_bulk_load")
        self.assertEqual(handlers["MSSQL_BULK_LOAD"].__name__, "handle_mssql_bulk_load")


if __name__ == "__main__":
    unittest.main()
