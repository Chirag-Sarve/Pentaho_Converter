"""Unit tests for Scripting-category Pentaho Job Entries (Shell, SQL, JavaScript/EVAL)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import scripting_ops as sops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_eval,
    handle_shell,
    handle_sql,
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
        name="ScriptTestJob",
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


_SCRIPT_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>ScriptSample</name>
  <connection>
    <name>AuditDB</name>
    <server>localhost</server>
    <type>POSTGRESQL</type>
    <database>audit</database>
    <port>5432</port>
    <username>etl</username>
    <password>x</password>
  </connection>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>Echo</name>
      <type>SHELL</type>
      <insertScript>Y</insertScript>
      <script>echo HELLO %RUN_ID% DATE=${CURRENT_DATE}</script>
      <work_directory>${PROJECT_HOME}</work_directory>
      <arg_from_previous>N</arg_from_previous>
      <exec_per_row>N</exec_per_row>
      <set_logfile>N</set_logfile>
      <argument>arg1</argument>
      <argument>arg2</argument>
    </entry>
    <entry>
      <name>AuditSql</name>
      <type>SQL</type>
      <connection>AuditDB</connection>
      <sql>-- comment only
-- INSERT INTO t VALUES ('${RUN_ID}');</sql>
      <useVariableSubstitution>Y</useVariableSubstitution>
      <sqlfromfile>N</sqlfromfile>
      <sendOneStatement>N</sendOneStatement>
    </entry>
    <entry>
      <name>JsGate</name>
      <type>EVAL</type>
      <script>parent_job.getVariable("FLAG") == "Y"</script>
    </entry>
  </entries>
  <hops>
    <hop><from>Start</from><to>Echo</to><enabled>Y</enabled><unconditional>Y</unconditional></hop>
  </hops>
</job>
"""


class TestScriptingParser(unittest.TestCase):
    def test_parses_shell_sql_eval(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "script.kjb"
            path.write_text(_SCRIPT_KJB, encoding="utf-8")
            job = parse_job(path)

        by_name = {e.name: e for e in job.entries}
        shell = by_name["Echo"]
        self.assertEqual(shell.entry_type, "SHELL")
        self.assertEqual(shell.attributes["insertScript"], "Y")
        self.assertEqual(shell.attributes["arguments"], ["arg1", "arg2"])

        sql = by_name["AuditSql"]
        self.assertEqual(sql.entry_type, "SQL")
        self.assertEqual(sql.attributes["connection"], "AuditDB")
        self.assertEqual(sql.attributes["useVariableSubstitution"], "Y")
        self.assertIn("AuditDB", job.connections)

        js = by_name["JsGate"]
        self.assertEqual(js.entry_type, "EVAL")
        self.assertIn("getVariable", js.attributes["script"])


class TestShellOps(unittest.TestCase):
    def test_echo_python_path_and_percent_vars(self):
        out = sops.run_shell(
            script="echo HELLO %RUN_ID%",
            insert_script=True,
            variables={"RUN_ID": "batch-1"},
        )
        self.assertTrue(out.success)
        self.assertIn("HELLO batch-1", out.stdout)
        self.assertEqual(out.extra.get("mode"), "python_echo")

    def test_shell_handler_variable_substitution(self):
        with tempfile.TemporaryDirectory() as tmp:
            rt = _runtime(
                [],
                variables={"RUN_ID": "R1", "CURRENT_DATE": "2026-07-21", "PROJECT_HOME": tmp},
            )
            res = handle_shell(
                rt,
                JobEntry(
                    name="Echo",
                    entry_type="SHELL",
                    attributes={
                        "insertScript": "Y",
                        "script": "echo RUN=%RUN_ID% DATE=${CURRENT_DATE}",
                        "work_directory": "${PROJECT_HOME}",
                    },
                ),
            )
            self.assertTrue(res.success)
            self.assertIn("RUN=R1", res.result["stdout"])
            self.assertIn("DATE=2026-07-21", res.result["stdout"])

    def test_shell_failure_nonzero_exit(self):
        # `false` command on Unix; on Windows may differ — use python -c exit
        out = sops.run_shell(
            script='python -c "raise SystemExit(2)"',
            insert_script=True,
            variables={},
        )
        self.assertFalse(out.success)
        self.assertEqual(out.exit_code, 2)


class TestSqlOps(unittest.TestCase):
    def test_split_skips_comment_only(self):
        stmts = sops.split_sql_statements(
            "-- Placeholder\n-- INSERT INTO t VALUES (1);"
        )
        self.assertEqual(stmts, [])

    def test_split_multiple_and_one_statement(self):
        sql = "CREATE TABLE a (id INT);\nINSERT INTO a VALUES (1);"
        self.assertEqual(len(sops.split_sql_statements(sql)), 2)
        self.assertEqual(len(sops.split_sql_statements(sql, send_one_statement=True)), 1)

    def test_sql_comment_only_succeeds_without_spark(self):
        rt = _runtime([], variables={"RUN_ID": "x"})
        res = handle_sql(
            rt,
            JobEntry(
                name="Audit",
                entry_type="SQL",
                attributes={
                    "sql": "-- audit ${RUN_ID}\n-- nothing",
                    "useVariableSubstitution": "Y",
                    "sqlfromfile": "N",
                    "sendOneStatement": "N",
                },
            ),
        )
        self.assertTrue(res.success)

    def test_sql_executes_via_spark(self):
        spark = MagicMock()
        rt = _runtime([], spark=spark, connections={"DB": {"type": "POSTGRESQL"}})
        res = handle_sql(
            rt,
            JobEntry(
                name="Run",
                entry_type="SQL",
                attributes={
                    "connection": "DB",
                    "sql": "CREATE TABLE IF NOT EXISTS t (id INT); SELECT 1;",
                    "useVariableSubstitution": "Y",
                    "sendOneStatement": "N",
                },
            ),
        )
        self.assertTrue(res.success)
        self.assertEqual(spark.sql.call_count, 2)

    def test_sql_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "audit.sql"
            path.write_text("SELECT 1 AS x;", encoding="utf-8")
            spark = MagicMock()
            rt = _runtime([], spark=spark)
            res = handle_sql(
                rt,
                JobEntry(
                    name="FileSql",
                    entry_type="SQL",
                    attributes={
                        "sqlfromfile": "Y",
                        "sqlfilename": str(path),
                        "sql": "",
                        "useVariableSubstitution": "N",
                    },
                ),
            )
            self.assertTrue(res.success)
            spark.sql.assert_called_once()

    def test_sql_failure(self):
        spark = MagicMock()
        spark.sql.side_effect = Exception("boom")
        rt = _runtime([], spark=spark)
        res = handle_sql(
            rt,
            JobEntry(
                name="Bad",
                entry_type="SQL",
                attributes={"sql": "SELECT * FROM missing_table", "sendOneStatement": "Y"},
            ),
        )
        self.assertFalse(res.success)


class TestEvalJavascript(unittest.TestCase):
    def test_literal_true_false(self):
        self.assertTrue(sops.evaluate_javascript("true", variables={}).success)
        self.assertFalse(sops.evaluate_javascript("false", variables={}).success)

    def test_get_variable_comparison(self):
        out = sops.evaluate_javascript(
            'parent_job.getVariable("FLAG") == "Y"',
            variables={"FLAG": "Y"},
        )
        self.assertTrue(out.success)
        self.assertEqual(out.extra.get("mode"), "translated")

    def test_complex_js_todo(self):
        out = sops.evaluate_javascript(
            "function f(){ return true; } f();",
            variables={},
        )
        self.assertFalse(out.success)
        self.assertEqual(out.extra.get("mode"), "todo")
        self.assertIn("ORIGINAL_JAVASCRIPT_PRESERVED", out.warnings)

    def test_eval_handler_hops(self):
        entries = [
            JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
            JobEntry(
                name="Gate",
                entry_type="EVAL",
                attributes={"script": 'getVariable("FLAG") == "Y"'},
            ),
            JobEntry(name="Ok", entry_type="SUCCESS"),
            JobEntry(
                name="No",
                entry_type="WRITE_TO_LOG",
                attributes={"logmessage": "no", "loglevel": "BASIC"},
            ),
        ]
        hops = [
            JobHop("Start", "Gate", unconditional=True),
            JobHop("Gate", "Ok", evaluation=True),
            JobHop("Gate", "No", evaluation=False),
        ]
        rt = _runtime(entries, hops, variables={"FLAG": "Y"})
        result = rt.run()
        self.assertTrue(result.success)
        self.assertIn("Ok", rt.executed)
        self.assertNotIn("No", rt.executed)

    def test_eval_false_follows_failure_hop(self):
        entries = [
            JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
            JobEntry(
                name="Gate",
                entry_type="EVAL",
                attributes={"script": "false"},
            ),
            JobEntry(name="Ok", entry_type="SUCCESS"),
            JobEntry(
                name="No",
                entry_type="WRITE_TO_LOG",
                attributes={"logmessage": "fail path", "loglevel": "BASIC"},
            ),
        ]
        hops = [
            JobHop("Start", "Gate", unconditional=True),
            JobHop("Gate", "Ok", evaluation=True),
            JobHop("Gate", "No", evaluation=False),
        ]
        rt = _runtime(entries, hops)
        result = rt.run()
        self.assertIn("No", rt.executed)
        self.assertNotIn("Ok", rt.executed)
        self.assertTrue(result.success)


class TestRegistration(unittest.TestCase):
    def test_handlers_registered(self):
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types={"SHELL", "SQL", "EVAL"},
            trans_runners={},
            child_job_modules={},
        )
        self.assertEqual(handlers["SHELL"].__name__, "handle_shell")
        self.assertEqual(handlers["SQL"].__name__, "handle_sql")
        self.assertEqual(handlers["EVAL"].__name__, "handle_eval")


if __name__ == "__main__":
    unittest.main()
