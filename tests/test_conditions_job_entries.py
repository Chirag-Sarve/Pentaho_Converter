"""Unit tests for Conditions-category Pentaho Job Entries."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import condition_ops as cops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_check_db_connections,
    handle_check_files_locked,
    handle_columns_exist,
    handle_delay,
    handle_eval_files_metrics,
    handle_eval_table_content,
    handle_file_exists,
    handle_files_exist,
    handle_folder_is_empty,
    handle_simple_eval,
    handle_table_exists,
    handle_wait_for_sql,
    handle_webservice_available,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry, JobHop
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime


def _runtime(
    entries: list[JobEntry],
    hops: list[JobHop] | None = None,
    *,
    variables: dict | None = None,
    spark: object | None = None,
    connections: dict | None = None,
) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=spark,
        cfg={},
        entry_types={e.entry_type.upper() for e in entries},
        trans_runners={},
        child_job_modules={},
    )
    rt = JobRuntime(
        name="CondTestJob",
        entries=entries,
        hops=hops or [],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )
    rt.spark = spark
    rt.connections = dict(connections or {})
    return rt


_COND_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>CondSample</name>
  <connection>
    <name>RetailDB</name>
    <server>${DB_HOST}</server>
    <type>POSTGRESQL</type>
    <access>Native</access>
    <database>retail</database>
    <port>5432</port>
    <username>etl</username>
    <password>${DB_PASS}</password>
  </connection>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>FileOk</name>
      <type>FILE_EXISTS</type>
      <filename>${BASE}/a.txt</filename>
    </entry>
    <entry>
      <name>FilesOk</name>
      <type>FILES_EXIST</type>
      <fields>
        <field><name>${BASE}/a.txt</name></field>
        <field><name>${BASE}/b.txt</name></field>
      </fields>
    </entry>
    <entry>
      <name>EmptyDir</name>
      <type>FOLDER_IS_EMPTY</type>
      <foldername>${BASE}/empty</foldername>
      <include_subfolders>N</include_subfolders>
    </entry>
    <entry>
      <name>Locked</name>
      <type>CHECK_FILES_LOCKED</type>
      <arg_from_previous>N</arg_from_previous>
      <include_subfolders>N</include_subfolders>
      <fields>
        <field>
          <name>${BASE}/a.txt</name>
          <filemask></filemask>
        </field>
      </fields>
    </entry>
    <entry>
      <name>Web</name>
      <type>WEBSERVICE_AVAILABLE</type>
      <url>${API_URL}</url>
      <connectTimeOut>5000</connectTimeOut>
      <readTimeOut>5000</readTimeOut>
    </entry>
    <entry>
      <name>Eval</name>
      <type>SIMPLE_EVAL</type>
      <valuetype>variable</valuetype>
      <variablename>RETRY_COUNT</variablename>
      <fieldtype>number</fieldtype>
      <comparevalue>${RETRY_MAX}</comparevalue>
      <successnumbercondition>smaller</successnumbercondition>
      <successcondition>equal</successcondition>
      <successbooleancondition>false</successbooleancondition>
      <successwhenvarset>N</successwhenvarset>
    </entry>
    <entry>
      <name>Wait</name>
      <type>DELAY</type>
      <maximumTimeout>1</maximumTimeout>
      <scaletime>0</scaletime>
    </entry>
    <entry>
      <name>Tbl</name>
      <type>TABLE_EXISTS</type>
      <connection>RetailDB</connection>
      <schemaname>${SCHEMA}</schemaname>
      <tablename>orders</tablename>
    </entry>
    <entry>
      <name>Cols</name>
      <type>COLUMNS_EXIST</type>
      <connection>RetailDB</connection>
      <schemaname>public</schemaname>
      <tablename>orders</tablename>
      <fields>
        <field><name>order_id</name></field>
        <field><name>amount</name></field>
      </fields>
    </entry>
    <entry>
      <name>RowCount</name>
      <type>EVAL_TABLE_CONTENT</type>
      <connection>RetailDB</connection>
      <tablename>orders</tablename>
      <success_condition>rows_count_greater</success_condition>
      <limit>0</limit>
      <is_custom_sql>N</is_custom_sql>
    </entry>
    <entry>
      <name>Metrics</name>
      <type>EVAL_FILES_METRICS</type>
      <source_filefolder>${BASE}</source_filefolder>
      <wildcard>.*\\.txt$</wildcard>
      <include_subFolders>N</include_subFolders>
      <evaluation_type>count</evaluation_type>
      <comparevalue>2</comparevalue>
      <successnumbercondition>equal</successnumbercondition>
      <source_files>files</source_files>
      <scale>bytes</scale>
    </entry>
    <entry>
      <name>WaitSql</name>
      <type>WAIT_FOR_SQL</type>
      <connection>RetailDB</connection>
      <tablename>orders</tablename>
      <success_condition>rows_count_greater</success_condition>
      <rows_count_value>0</rows_count_value>
      <maximum_timeout>1</maximum_timeout>
      <check_cycle_time>1</check_cycle_time>
      <success_on_timeout>Y</success_on_timeout>
    </entry>
    <entry>
      <name>CheckDb</name>
      <type>CHECK_DB_CONNECTIONS</type>
      <connections>
        <connection>
          <name>RetailDB</name>
          <waitfor>0</waitfor>
          <waittime>second</waittime>
        </connection>
      </connections>
    </entry>
  </entries>
  <hops>
    <hop><from>Start</from><to>FileOk</to><enabled>Y</enabled><unconditional>Y</unconditional></hop>
  </hops>
</job>
"""


class TestConditionsParser(unittest.TestCase):
    def test_parses_connections_and_condition_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cond.kjb"
            path.write_text(_COND_KJB, encoding="utf-8")
            job = parse_job(path)

        self.assertIn("RetailDB", job.connections)
        self.assertEqual(job.connections["RetailDB"]["type"], "POSTGRESQL")
        self.assertEqual(job.connections["RetailDB"]["server"], "${DB_HOST}")

        by_name = {e.name: e for e in job.entries}
        self.assertEqual(by_name["FilesOk"].entry_type, "FILES_EXIST")
        self.assertEqual(len(by_name["FilesOk"].attributes["fields"]), 2)
        self.assertEqual(by_name["Eval"].attributes["successnumbercondition"], "smaller")
        self.assertEqual(by_name["Web"].attributes["connectTimeOut"], "5000")
        self.assertEqual(by_name["Cols"].attributes["fields"][0]["name"], "order_id")
        self.assertEqual(
            by_name["CheckDb"].attributes["connections"][0]["name"], "RetailDB"
        )


class TestSimpleEvalOps(unittest.TestCase):
    def test_string_operators(self):
        self.assertTrue(cops.simple_eval(left="abc", compare="abc", successcondition="equal").success)
        self.assertTrue(cops.simple_eval(left="abc", compare="ab", successcondition="contains").success)
        self.assertTrue(cops.simple_eval(left="abc", compare="a", successcondition="startswith").success)
        self.assertTrue(cops.simple_eval(left="abc", compare="c", successcondition="endswith").success)
        self.assertTrue(cops.simple_eval(left="", compare="empty", successcondition="equal").success)
        self.assertTrue(
            cops.simple_eval(left="x", compare="a,x,b", successcondition="inlist").success
        )

    def test_number_and_legacy_smaller(self):
        out = cops.simple_eval(
            left="3",
            compare="5",
            fieldtype="NUMBER",
            successnumbercondition="SMALLER",
        )
        self.assertTrue(out.success)
        out2 = cops.simple_eval(
            left="5",
            compare="",
            minvalue="1",
            maxvalue="10",
            fieldtype="number",
            successnumbercondition="between",
        )
        self.assertTrue(out2.success)

    def test_boolean_and_varset(self):
        self.assertTrue(
            cops.simple_eval(
                left="Y", fieldtype="boolean", successbooleancondition="true"
            ).success
        )
        self.assertTrue(
            cops.simple_eval(left="", successwhenvarset=True, var_is_set=True).success
        )
        self.assertFalse(
            cops.simple_eval(left="", successwhenvarset=True, var_is_set=False).success
        )


class TestFileConditions(unittest.TestCase):
    def test_folder_empty_files_exist_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            empty = base / "empty"
            empty.mkdir()
            (base / "a.txt").write_text("a", encoding="utf-8")
            (base / "b.txt").write_text("b", encoding="utf-8")

            self.assertTrue(cops.folder_is_empty(str(empty)).success)
            self.assertFalse(cops.folder_is_empty(str(base)).success)

            self.assertTrue(
                cops.files_exist([str(base / "a.txt"), str(base / "b.txt")]).success
            )
            self.assertFalse(
                cops.files_exist([str(base / "a.txt"), str(base / "missing.txt")]).success
            )

            metrics = cops.eval_files_metrics(
                [str(base)],
                evaluation_type="count",
                comparevalue="2",
                successnumbercondition="equal",
                wildcard=r".*\.txt$",
            )
            self.assertTrue(metrics.success)

            locked = cops.check_files_locked([{"source": str(base / "a.txt"), "wildcard": ""}])
            self.assertTrue(locked.success)


class TestHandlers(unittest.TestCase):
    def test_file_exists_and_files_exist_and_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "a.txt").write_text("x", encoding="utf-8")
            (base / "empty").mkdir()
            rt = _runtime([], variables={"BASE": str(base)})

            fe = handle_file_exists(
                rt,
                JobEntry(
                    name="F",
                    entry_type="FILE_EXISTS",
                    attributes={"filename": str(base / "a.txt")},
                ),
            )
            self.assertTrue(fe.success)

            fs = handle_files_exist(
                rt,
                JobEntry(
                    name="Fs",
                    entry_type="FILES_EXIST",
                    attributes={
                        "fields": [
                            {"name": str(base / "a.txt")},
                            {"name": str(base / "missing.txt")},
                        ]
                    },
                ),
            )
            self.assertFalse(fs.success)

            empty = handle_folder_is_empty(
                rt,
                JobEntry(
                    name="E",
                    entry_type="FOLDER_IS_EMPTY",
                    attributes={"foldername": str(base / "empty")},
                ),
            )
            self.assertTrue(empty.success)

    def test_simple_eval_handler_with_variables_and_hops(self):
        entries = [
            JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
            JobEntry(
                name="Eval",
                entry_type="SIMPLE_EVAL",
                attributes={
                    "valuetype": "VARIABLE",
                    "variablename": "RETRY_COUNT",
                    "fieldtype": "NUMBER",
                    "comparevalue": "${RETRY_MAX}",
                    "successnumbercondition": "SMALLER",
                },
            ),
            JobEntry(name="Ok", entry_type="SUCCESS"),
            JobEntry(
                name="FailPath",
                entry_type="WRITE_TO_LOG",
                attributes={"logmessage": "no", "loglevel": "BASIC"},
            ),
        ]
        hops = [
            JobHop("Start", "Eval", unconditional=True),
            JobHop("Eval", "Ok", evaluation=True),
            JobHop("Eval", "FailPath", evaluation=False),
        ]
        rt = _runtime(entries, hops, variables={"RETRY_COUNT": "1", "RETRY_MAX": "3"})
        result = rt.run()
        self.assertTrue(result.success)
        self.assertIn("Ok", rt.executed)
        self.assertNotIn("FailPath", rt.executed)

    def test_delay_is_short(self):
        rt = _runtime([])
        entry = JobEntry(
            name="D",
            entry_type="DELAY",
            attributes={"maximumTimeout": "0", "scaletime": "0"},
        )
        res = handle_delay(rt, entry)
        self.assertTrue(res.success)

    def test_webservice_mocked(self):
        rt = _runtime([])
        entry = JobEntry(
            name="W",
            entry_type="WEBSERVICE_AVAILABLE",
            attributes={"url": "https://example.com", "connectTimeOut": "1000"},
        )
        with patch(
            "pentaho_converter.runtime_templates.engine.condition_ops.webservice_available",
            return_value=cops.CondOutcome(True, "HTTP 200", True, extra={"status_code": 200}),
        ):
            res = handle_webservice_available(rt, entry)
        self.assertTrue(res.success)

    def test_table_and_columns_with_spark_mock(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        df = MagicMock()
        df.columns = ["order_id", "amount", "extra"]
        spark.table.return_value = df

        rt = _runtime([], spark=spark, connections={})
        tbl = handle_table_exists(
            rt,
            JobEntry(
                name="T",
                entry_type="TABLE_EXISTS",
                attributes={"tablename": "orders", "schemaname": "public"},
            ),
        )
        self.assertTrue(tbl.success)

        cols = handle_columns_exist(
            rt,
            JobEntry(
                name="C",
                entry_type="COLUMNS_EXIST",
                attributes={
                    "tablename": "orders",
                    "schemaname": "public",
                    "fields": [{"name": "order_id"}, {"name": "amount"}],
                },
            ),
        )
        self.assertTrue(cols.success)

        missing = handle_columns_exist(
            rt,
            JobEntry(
                name="C2",
                entry_type="COLUMNS_EXIST",
                attributes={
                    "tablename": "orders",
                    "fields": [{"name": "nope"}],
                },
            ),
        )
        self.assertFalse(missing.success)

    def test_eval_table_and_wait_sql(self):
        spark = MagicMock()
        spark.table.return_value.count.return_value = 5
        rt = _runtime([], spark=spark)

        ev = handle_eval_table_content(
            rt,
            JobEntry(
                name="E",
                entry_type="EVAL_TABLE_CONTENT",
                attributes={
                    "tablename": "orders",
                    "success_condition": "rows_count_greater",
                    "limit": "0",
                },
            ),
        )
        self.assertTrue(ev.success)

        wait = handle_wait_for_sql(
            rt,
            JobEntry(
                name="W",
                entry_type="WAIT_FOR_SQL",
                attributes={
                    "tablename": "orders",
                    "success_condition": "rows_count_greater",
                    "rows_count_value": "100",
                    "maximum_timeout": "0",
                    "check_cycle_time": "1",
                    "success_on_timeout": "Y",
                },
            ),
        )
        self.assertTrue(wait.success)

    def test_check_db_connections_tcp(self):
        rt = _runtime(
            [],
            connections={
                "Local": {
                    "type": "POSTGRESQL",
                    "server": "127.0.0.1",
                    "port": "1",
                    "database": "x",
                }
            },
        )
        # Port 1 should fail quickly
        res = handle_check_db_connections(
            rt,
            JobEntry(
                name="Db",
                entry_type="CHECK_DB_CONNECTIONS",
                attributes={
                    "connections": [
                        {"name": "Local", "waitfor": "0", "waittime": "second"}
                    ]
                },
            ),
        )
        self.assertFalse(res.success)

    def test_eval_files_metrics_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "a.txt").write_text("hi", encoding="utf-8")
            rt = _runtime([])
            res = handle_eval_files_metrics(
                rt,
                JobEntry(
                    name="M",
                    entry_type="EVAL_FILES_METRICS",
                    attributes={
                        "source_filefolder": str(base),
                        "wildcard": r".*\.txt$",
                        "evaluation_type": "count",
                        "comparevalue": "1",
                        "successnumbercondition": "equal",
                        "source_files": "files",
                    },
                ),
            )
            self.assertTrue(res.success)

    def test_handlers_registered(self):
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types={
                "FILES_EXIST",
                "FOLDER_IS_EMPTY",
                "CHECK_FILES_LOCKED",
                "WEBSERVICE_AVAILABLE",
                "TABLE_EXISTS",
                "COLUMNS_EXIST",
                "EVAL_TABLE_CONTENT",
                "EVAL_FILES_METRICS",
                "WAIT_FOR_SQL",
                "CHECK_DB_CONNECTIONS",
                "WAIT_FOR",
            },
            trans_runners={},
            child_job_modules={},
        )
        for key in (
            "FILES_EXIST",
            "FOLDER_IS_EMPTY",
            "CHECK_FILES_LOCKED",
            "WEBSERVICE_AVAILABLE",
            "TABLE_EXISTS",
            "COLUMNS_EXIST",
            "EVAL_TABLE_CONTENT",
            "EVAL_FILES_METRICS",
            "WAIT_FOR_SQL",
            "CHECK_DB_CONNECTIONS",
            "WAIT_FOR",
        ):
            self.assertNotEqual(handlers[key].__name__, "handle_todo", msg=key)

    def test_check_files_locked_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "f.txt"
            path.write_text("x", encoding="utf-8")
            rt = _runtime([])
            res = handle_check_files_locked(
                rt,
                JobEntry(
                    name="L",
                    entry_type="CHECK_FILES_LOCKED",
                    attributes={"fields": [{"name": str(path), "filemask": ""}]},
                ),
            )
            self.assertTrue(res.success)


if __name__ == "__main__":
    unittest.main()
