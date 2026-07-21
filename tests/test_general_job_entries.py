"""Unit tests for General-category Pentaho Job Entries.

Covers: Start, Dummy, Job, Set Variables, Success, Transformation,
plus hop semantics and variable inheritance/override behaviour.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_set_variables,
    handle_special,
    handle_success,
    make_job_handler,
)
from pentaho_converter.runtime_templates.engine.job_models import (
    HopKind,
    JobEntry,
    JobHop,
    entries_from_defs,
    hops_from_defs,
)
from pentaho_converter.runtime_templates.engine.job_runtime import (
    JobExecutionError,
    JobRuntime,
)
from pentaho_converter.runtime_templates.engine.variables import substitute_variables


def _runtime(
    entries: list[JobEntry],
    hops: list[JobHop],
    *,
    handlers: dict | None = None,
    variables: dict | None = None,
    parent_variables: dict | None = None,
    root_variables: dict | None = None,
    variable_scopes: list | None = None,
    name: str = "TestJob",
) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    scopes = variable_scopes
    if scopes is None:
        scopes = [vars_]
        if parent_variables is not None and parent_variables not in scopes:
            scopes.append(parent_variables)
        if root_variables is not None and root_variables not in scopes:
            scopes.append(root_variables)
    built = handlers or {
        "SPECIAL": handle_special,
        "START": handle_special,
        "DUMMY": handle_special,
        "SUCCESS": handle_success,
        "SET_VARIABLES": handle_set_variables,
    }
    return JobRuntime(
        name=name,
        entries=entries,
        hops=hops,
        variables=vars_,
        handlers=built,
        parent_variables=parent_variables,
        root_variables=root_variables if root_variables is not None else vars_,
        variable_scopes=scopes,
    )


class TestHopSemantics(unittest.TestCase):
    def test_unconditional_always_fires(self):
        hop = JobHop("A", "B", unconditional=True, evaluation=False)
        self.assertEqual(hop.kind(from_entry_type="TRANS"), HopKind.UNCONDITIONAL)
        self.assertTrue(hop.fires(entry_succeeded=False, from_entry_type="TRANS"))
        self.assertTrue(hop.fires(entry_succeeded=True, from_entry_type="TRANS"))

    def test_success_and_failure_hops(self):
        ok = JobHop("A", "B", unconditional=False, evaluation=True)
        fail = JobHop("A", "C", unconditional=False, evaluation=False)
        self.assertEqual(ok.kind(from_entry_type="TRANS"), HopKind.ON_SUCCESS)
        self.assertEqual(fail.kind(from_entry_type="TRANS"), HopKind.ON_FAILURE)
        self.assertTrue(ok.fires(entry_succeeded=True, from_entry_type="TRANS"))
        self.assertFalse(ok.fires(entry_succeeded=False, from_entry_type="TRANS"))
        self.assertTrue(fail.fires(entry_succeeded=False, from_entry_type="TRANS"))
        self.assertFalse(fail.fires(entry_succeeded=True, from_entry_type="TRANS"))

    def test_start_and_dummy_default_unconditional(self):
        hop = JobHop("Start", "Next", unconditional=None, evaluation=None)
        self.assertEqual(hop.kind(from_entry_type="SPECIAL"), HopKind.UNCONDITIONAL)
        self.assertEqual(hop.kind(from_entry_type="DUMMY"), HopKind.UNCONDITIONAL)
        self.assertEqual(hop.kind(from_entry_type="START"), HopKind.UNCONDITIONAL)

    def test_disabled_hop_never_fires(self):
        hop = JobHop("A", "B", enabled=False, unconditional=True)
        self.assertFalse(hop.fires(entry_succeeded=True, from_entry_type="SPECIAL"))


class TestStartAndDummy(unittest.TestCase):
    def test_start_is_only_entry_with_is_start(self):
        start = JobEntry(name="Start", entry_type="SPECIAL", is_start=True)
        dummy = JobEntry(name="Gate", entry_type="SPECIAL", is_start=False)
        typed = JobEntry(name="Pass", entry_type="DUMMY", is_start=False)
        rt = _runtime(
            [start, dummy, typed, JobEntry(name="Success", entry_type="SUCCESS")],
            [
                JobHop("Start", "Gate"),
                JobHop("Gate", "Pass"),
                JobHop("Pass", "Success"),
            ],
        )
        result = rt.run()
        self.assertTrue(result.success)
        self.assertEqual(rt.executed, ["Start", "Gate", "Pass", "Success"])
        self.assertEqual(rt.results["Start"].result, "START")
        self.assertEqual(rt.results["Gate"].result, "DUMMY")
        self.assertEqual(rt.results["Pass"].result, "DUMMY")

    def test_missing_start_raises(self):
        rt = _runtime(
            [JobEntry(name="OnlyDummy", entry_type="DUMMY", is_start=False)],
            [],
        )
        with self.assertRaises(JobExecutionError) as ctx:
            rt.run()
        self.assertIn("no Start entry", str(ctx.exception))

    def test_start_scheduler_warns(self):
        start = JobEntry(
            name="Start",
            entry_type="SPECIAL",
            is_start=True,
            attributes={"schedulerType": "1", "intervalMinutes": "5", "repeat": "Y"},
        )
        with self.assertLogs(level="WARNING") as captured:
            handle_special(
                _runtime([start], [], variables={}),
                start,
            )
        self.assertTrue(any("schedulerType" in line for line in captured.output))


class TestSuccessEntry(unittest.TestCase):
    def test_success_is_terminal_result(self):
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(name="Success", entry_type="SUCCESS"),
            ],
            [JobHop("Start", "Success")],
        )
        result = rt.run()
        self.assertTrue(result.success)
        self.assertEqual(result.name, "Success")


class TestSetVariables(unittest.TestCase):
    def tearDown(self):
        for key in ("TEST_JVM_VAR", "TEST_ROOT_VAR", "TEST_PARENT_VAR", "TEST_CUR_VAR"):
            os.environ.pop(key, None)

    def test_current_job_scope(self):
        entry = JobEntry(
            name="Set",
            entry_type="SET_VARIABLES",
            attributes={
                "replace": "Y",
                "fields": [
                    {
                        "variable_name": "TEST_CUR_VAR",
                        "variable_string": "hello",
                        "variable_type": "CURRENT_JOB",
                    }
                ],
            },
        )
        parent = {"EXISTING": "1"}
        current: dict = {"EXISTING": "1"}
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                entry,
                JobEntry(name="Success", entry_type="SUCCESS"),
            ],
            [JobHop("Start", "Set"), JobHop("Set", "Success")],
            variables=current,
            parent_variables=parent,
            root_variables=parent,
            variable_scopes=[current, parent],
        )
        result = rt.run()
        self.assertTrue(result.success)
        self.assertEqual(current["TEST_CUR_VAR"], "hello")
        self.assertNotIn("TEST_CUR_VAR", parent)

    def test_root_job_scope_propagates(self):
        root: dict = {}
        child: dict = {}
        entry = JobEntry(
            name="Set",
            entry_type="SET_VARIABLES",
            attributes={
                "replace": "Y",
                "fields": [
                    {
                        "variable_name": "TEST_ROOT_VAR",
                        "variable_string": "from-child",
                        "variable_type": "ROOT_JOB",
                    }
                ],
            },
        )
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                entry,
            ],
            [JobHop("Start", "Set")],
            variables=child,
            parent_variables=root,
            root_variables=root,
            variable_scopes=[child, root],
        )
        rt.run()
        self.assertEqual(child["TEST_ROOT_VAR"], "from-child")
        self.assertEqual(root["TEST_ROOT_VAR"], "from-child")

    def test_parent_job_requires_parent(self):
        entry = JobEntry(
            name="Set",
            entry_type="SET_VARIABLES",
            attributes={
                "replace": "Y",
                "fields": [
                    {
                        "variable_name": "TEST_PARENT_VAR",
                        "variable_string": "x",
                        "variable_type": "PARENT_JOB",
                    }
                ],
            },
        )
        current: dict = {}
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                entry,
            ],
            [JobHop("Start", "Set")],
            variables=current,
            variable_scopes=[current],
        )
        with self.assertRaises(JobExecutionError) as ctx:
            rt.run()
        self.assertIn("PARENT_JOB", str(ctx.exception))

    def test_parent_job_writes_current_and_parent(self):
        parent: dict = {}
        current: dict = {}
        entry = JobEntry(
            name="Set",
            entry_type="SET_VARIABLES",
            attributes={
                "replace": "Y",
                "fields": [
                    {
                        "variable_name": "TEST_PARENT_VAR",
                        "variable_string": "up",
                        "variable_type": "PARENT_JOB",
                    }
                ],
            },
        )
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                entry,
            ],
            [JobHop("Start", "Set")],
            variables=current,
            parent_variables=parent,
            root_variables=parent,
            variable_scopes=[current, parent],
        )
        rt.run()
        self.assertEqual(current["TEST_PARENT_VAR"], "up")
        self.assertEqual(parent["TEST_PARENT_VAR"], "up")

    def test_jvm_scope_sets_environ(self):
        current: dict = {}
        entry = JobEntry(
            name="Set",
            entry_type="SET_VARIABLES",
            attributes={
                "replace": "Y",
                "fields": [
                    {
                        "variable_name": "TEST_JVM_VAR",
                        "variable_string": "jvm-val",
                        "variable_type": "JVM",
                    }
                ],
            },
        )
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                entry,
            ],
            [JobHop("Start", "Set")],
            variables=current,
            variable_scopes=[current],
        )
        rt.run()
        self.assertEqual(current["TEST_JVM_VAR"], "jvm-val")
        self.assertEqual(os.environ.get("TEST_JVM_VAR"), "jvm-val")

    def test_variable_substitution_and_increment(self):
        current = {"RETRY_COUNT": "2", "BASE": "path"}
        entry = JobEntry(
            name="Set",
            entry_type="SET_VARIABLES",
            attributes={
                "replace": "Y",
                "fields": [
                    {
                        "variable_name": "RETRY_COUNT",
                        "variable_string": "${RETRY_COUNT}+1",
                        "variable_type": "CURRENT_JOB",
                    },
                    {
                        "variable_name": "OUT",
                        "variable_string": "${BASE}/out",
                        "variable_type": "CURRENT_JOB",
                    },
                ],
            },
        )
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                entry,
            ],
            [JobHop("Start", "Set")],
            variables=current,
            variable_scopes=[current],
        )
        rt.run()
        self.assertEqual(current["RETRY_COUNT"], "3")
        self.assertEqual(current["OUT"], "path/out")

    def test_properties_file_loading(self):
        with tempfile.TemporaryDirectory() as tmp:
            props = Path(tmp) / "vars.properties"
            props.write_text("FROM_FILE=abc\n# comment\nOTHER=1\n", encoding="utf-8")
            current: dict = {}
            entry = JobEntry(
                name="Set",
                entry_type="SET_VARIABLES",
                filename=str(props),
                attributes={
                    "replace": "Y",
                    "filename": str(props),
                    "file_variable_type": "CURRENT_JOB",
                    "fields": [],
                },
            )
            rt = _runtime(
                [
                    JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                    entry,
                ],
                [JobHop("Start", "Set")],
                variables=current,
                variable_scopes=[current],
            )
            rt.run()
            self.assertEqual(current["FROM_FILE"], "abc")
            self.assertEqual(current["OTHER"], "1")

    def test_replace_false_skips_existing(self):
        current = {"KEEP": "old"}
        entry = JobEntry(
            name="Set",
            entry_type="SET_VARIABLES",
            attributes={
                "replacevars": "N",
                "fields": [
                    {
                        "variable_name": "KEEP",
                        "variable_value": "new",
                        "variable_type": "CURRENT_JOB",
                    }
                ],
            },
        )
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                entry,
            ],
            [JobHop("Start", "Set")],
            variables=current,
            variable_scopes=[current],
        )
        rt.run()
        self.assertEqual(current["KEEP"], "old")


class TestTransformationEntry(unittest.TestCase):
    def test_trans_success_and_variable_pass_through(self):
        seen = {}

        def runner(_spark, cfg):
            seen.update(cfg)
            return "df-ok"

        handlers = build_handlers(
            spark=MagicMock(),
            cfg={"base": "1"},
            entry_types={"SPECIAL", "TRANS", "SUCCESS"},
            trans_runners={"Load": runner},
            child_job_modules={},
        )
        vars_ = {"BATCH_ID": "B1", "Internal.Job.Name": "Master"}
        rt = JobRuntime(
            name="Master",
            entries=[
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(
                    name="Load",
                    entry_type="TRANS",
                    attributes={
                        "pass_all_parameters": "Y",
                        "parameters": [{"name": "EXTRA", "value": "${BATCH_ID}-x"}],
                    },
                ),
                JobEntry(name="Success", entry_type="SUCCESS"),
            ],
            hops=[
                JobHop("Start", "Load"),
                JobHop("Load", "Success"),
            ],
            variables=vars_,
            handlers=handlers,
            variable_scopes=[vars_],
        )
        result = rt.run()
        self.assertTrue(result.success)
        self.assertEqual(seen.get("BATCH_ID"), "B1")
        self.assertEqual(seen.get("EXTRA"), "B1-x")

    def test_trans_failure_follows_failure_hop(self):
        def boom(_spark, _cfg):
            raise ValueError("trans failed")

        handlers = build_handlers(
            spark=MagicMock(),
            cfg={},
            entry_types={"SPECIAL", "TRANS", "SUCCESS", "ABORT"},
            trans_runners={"Load": boom},
            child_job_modules={},
        )
        from pentaho_converter.runtime_templates.engine.handlers import handle_abort

        handlers["ABORT"] = handle_abort
        rt = JobRuntime(
            name="Master",
            entries=[
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(name="Load", entry_type="TRANS"),
                JobEntry(name="Success", entry_type="SUCCESS"),
                JobEntry(
                    name="Abort",
                    entry_type="ABORT",
                    attributes={"message": "failed"},
                ),
            ],
            hops=[
                JobHop("Start", "Load"),
                JobHop("Load", "Success", unconditional=False, evaluation=True),
                JobHop("Load", "Abort", unconditional=False, evaluation=False),
            ],
            handlers=handlers,
        )
        with self.assertRaises(JobExecutionError):
            rt.run()
        self.assertIn("Abort", rt.executed)
        self.assertNotIn("Success", rt.executed)


class TestJobEntry(unittest.TestCase):
    def test_nested_job_inherits_and_overrides(self):
        # Simulate nested execute via make_job_handler calling a stub module.run
        parent_vars = {"SHARED": "parent", "ONLY_PARENT": "p"}

        class FakeModule:
            @staticmethod
            def run(spark, cfg):
                assert cfg.get("SHARED") == "parent"
                parent_ref = cfg["__parent_variables__"]
                root_ref = cfg["__root_variables__"]
                parent_ref["FROM_CHILD_PARENT"] = "via-parent-scope"
                root_ref["FROM_CHILD_ROOT"] = "via-root-scope"
                return {
                    "success": True,
                    "variables": {"CHILD_LOCAL": "local"},
                    "executed": ["Start", "Success"],
                }

        import sys
        import types

        jobs_pkg = types.ModuleType("jobs")
        child_mod = types.ModuleType("jobs.ChildJob")
        child_mod.run = FakeModule.run
        sys.modules["jobs"] = jobs_pkg
        sys.modules["jobs.ChildJob"] = child_mod
        try:
            handler = make_job_handler(
                spark=MagicMock(),
                cfg={},
                child_job_modules={"Run Child": ("ChildJob", "ChildJob")},
            )
            rt = _runtime(
                [
                    JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                    JobEntry(
                        name="Run Child",
                        entry_type="JOB",
                        attributes={"pass_all_parameters": "Y"},
                    ),
                    JobEntry(name="Success", entry_type="SUCCESS"),
                ],
                [
                    JobHop("Start", "Run Child"),
                    JobHop("Run Child", "Success"),
                ],
                variables=parent_vars,
                variable_scopes=[parent_vars],
                handlers={
                    "SPECIAL": handle_special,
                    "SUCCESS": handle_success,
                    "JOB": handler,
                },
            )
            result = rt.run()
            self.assertTrue(result.success)
            self.assertEqual(parent_vars.get("FROM_CHILD_PARENT"), "via-parent-scope")
            self.assertEqual(parent_vars.get("FROM_CHILD_ROOT"), "via-root-scope")
        finally:
            sys.modules.pop("jobs.ChildJob", None)
            sys.modules.pop("jobs", None)

    def test_missing_child_mapping_fails(self):
        handler = make_job_handler(
            spark=MagicMock(),
            cfg={},
            child_job_modules={},
        )
        rt = _runtime(
            [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(name="Missing", entry_type="JOB"),
            ],
            [JobHop("Start", "Missing")],
            handlers={"SPECIAL": handle_special, "JOB": handler},
        )
        with self.assertRaises(JobExecutionError) as ctx:
            rt.run()
        self.assertIn("No child JOB module mapping", str(ctx.exception))


class TestVariableSubstitution(unittest.TestCase):
    def test_dollar_and_percent_and_env_fallback(self):
        os.environ["TEST_ENV_FALLBACK"] = "from-env"
        try:
            out = substitute_variables(
                "${A}-${B}-%%C%%-${TEST_ENV_FALLBACK}",
                {"A": "1", "B": "2", "C": "3"},
            )
            self.assertEqual(out, "1-2-3-from-env")
        finally:
            os.environ.pop("TEST_ENV_FALLBACK", None)

    def test_nested_substitution(self):
        out = substitute_variables("${OUTER}", {"OUTER": "${INNER}", "INNER": "z"})
        self.assertEqual(out, "z")


class TestParserGeneralEntries(unittest.TestCase):
    def test_parses_general_entries_and_variable_value(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <job>
          <name>GeneralSample</name>
          <entries>
            <entry><name>Start</name><type>SPECIAL</type><start>Y</start></entry>
            <entry><name>Gate</name><type>DUMMY</type></entry>
            <entry>
              <name>Init</name>
              <type>SET_VARIABLES</type>
              <replacevars>Y</replacevars>
              <file_variable_type>JVM</file_variable_type>
              <fields>
                <field>
                  <variable_name>PROJECT_HOME</variable_name>
                  <variable_value>${Internal.Job.Filename.Directory}</variable_value>
                  <variable_type>ROOT_JOB</variable_type>
                </field>
              </fields>
            </entry>
            <entry>
              <name>Child</name>
              <type>JOB</type>
              <jobname>ChildJob</jobname>
              <filename>${Internal.Job.Filename.Directory}/ChildJob.kjb</filename>
              <parameters>
                <pass_all_parameters>Y</pass_all_parameters>
                <parameter><name>P1</name><value>${PROJECT_HOME}</value></parameter>
              </parameters>
            </entry>
            <entry>
              <name>Load</name>
              <type>TRANS</type>
              <transname>T1</transname>
              <filename>${Internal.Job.Filename.Directory}/T1.ktr</filename>
            </entry>
            <entry><name>Success</name><type>SUCCESS</type></entry>
          </entries>
          <hops>
            <hop><from>Start</from><to>Gate</to><enabled>Y</enabled><unconditional>Y</unconditional></hop>
            <hop><from>Gate</from><to>Init</to><enabled>Y</enabled></hop>
            <hop><from>Init</from><to>Child</to><enabled>Y</enabled><evaluation>Y</evaluation></hop>
            <hop><from>Child</from><to>Load</to><enabled>Y</enabled><evaluation>Y</evaluation></hop>
            <hop><from>Load</from><to>Success</to><enabled>Y</enabled><evaluation>Y</evaluation></hop>
            <hop><from>Load</from><to>Success</to><enabled>Y</enabled><evaluation>N</evaluation><unconditional>N</unconditional></hop>
          </hops>
        </job>
        """
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "GeneralSample.kjb"
            path.write_text(xml, encoding="utf-8")
            job = parse_job(path)

        types = {e.name: e.entry_type for e in job.entries}
        self.assertEqual(types["Start"], "SPECIAL")
        self.assertTrue(next(e for e in job.entries if e.name == "Start").is_start)
        self.assertEqual(types["Gate"], "DUMMY")
        self.assertEqual(types["Init"], "SET_VARIABLES")
        self.assertEqual(types["Child"], "JOB")
        self.assertEqual(types["Load"], "TRANS")
        self.assertEqual(types["Success"], "SUCCESS")

        init = next(e for e in job.entries if e.name == "Init")
        self.assertEqual(init.attributes.get("replace"), "Y")
        field = init.attributes["fields"][0]
        self.assertEqual(field["variable_type"], "ROOT_JOB")
        self.assertEqual(field["variable_string"], "${Internal.Job.Filename.Directory}")
        self.assertEqual(field["variable_value"], "${Internal.Job.Filename.Directory}")

        child = next(e for e in job.entries if e.name == "Child")
        self.assertEqual(child.attributes.get("pass_all_parameters"), "Y")
        self.assertEqual(child.attributes["parameters"][0]["name"], "P1")

        fail_hop = [h for h in job.hops if h.evaluation == "N"][0]
        self.assertFalse(fail_hop.unconditional)


class TestEndToEndGeneralWorkflow(unittest.TestCase):
    def test_execution_order_and_exit_status(self):
        order: list[str] = []

        def trans_ok(_spark, cfg):
            order.append(f"trans:{cfg.get('BATCH')}")
            return "ok"

        handlers = build_handlers(
            spark=MagicMock(),
            cfg={},
            entry_types={"SPECIAL", "DUMMY", "SET_VARIABLES", "TRANS", "SUCCESS"},
            trans_runners={"Load": trans_ok},
            child_job_modules={},
        )
        vars_ = {"BATCH": "seed"}
        rt = JobRuntime(
            name="GeneralFlow",
            entries=[
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(name="Dummy", entry_type="DUMMY"),
                JobEntry(
                    name="Init",
                    entry_type="SET_VARIABLES",
                    attributes={
                        "replace": "Y",
                        "fields": [
                            {
                                "variable_name": "BATCH",
                                "variable_string": "override",
                                "variable_type": "CURRENT_JOB",
                            }
                        ],
                    },
                ),
                JobEntry(
                    name="Load",
                    entry_type="TRANS",
                    attributes={"pass_all_parameters": "Y"},
                ),
                JobEntry(name="Success", entry_type="SUCCESS"),
            ],
            hops=[
                JobHop("Start", "Dummy"),
                JobHop("Dummy", "Init"),
                JobHop("Init", "Load"),
                JobHop("Load", "Success"),
            ],
            variables=vars_,
            handlers=handlers,
            variable_scopes=[vars_],
        )
        result = rt.run()
        self.assertTrue(result.success)
        self.assertEqual(
            rt.executed, ["Start", "Dummy", "Init", "Load", "Success"]
        )
        self.assertEqual(order, ["trans:override"])
        self.assertEqual(result.name, "Success")


class TestSpecHelpers(unittest.TestCase):
    def test_entries_and_hops_from_defs(self):
        entries = entries_from_defs(
            [
                {
                    "name": "Start",
                    "entry_type": "SPECIAL",
                    "is_start": True,
                    "attributes": {},
                }
            ]
        )
        hops = hops_from_defs(
            [
                {
                    "from_name": "Start",
                    "to_name": "X",
                    "enabled": True,
                    "unconditional": True,
                    "evaluation": True,
                }
            ]
        )
        self.assertTrue(entries[0].is_start)
        self.assertTrue(hops[0].unconditional)


if __name__ == "__main__":
    unittest.main()
