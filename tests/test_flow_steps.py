"""Tests for Pentaho Flow step migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_abort_config,
    parse_append_streams_config,
    parse_block_until_steps_finish_config,
    parse_blocking_step_config,
    parse_identify_last_row_config,
    parse_java_filter_config,
    parse_job_executor_config,
    parse_meta_inject_config,
    parse_prioritize_streams_config,
    parse_single_threader_config,
    parse_step_metadata,
    parse_switch_case_config,
    parse_trans_executor_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.flow_handlers import FLOW_HANDLERS
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    extra_inputs: list[str] | None = None,
    successors: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="FlowTrans", file_path=Path("flow.ktr"))
    steps = []
    hops = []
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        steps.append(inp)
        hops.append(PentahoHop(from_name="Input", to_name=step_name))
    for name in extra_inputs or []:
        steps.append(PentahoStep(name=name, step_type="Dummy", attributes={}, raw_element=None))
        hops.append(PentahoHop(from_name=name, to_name=step_name))
    steps.append(step)
    for name in successors or []:
        steps.append(PentahoStep(name=name, step_type="Dummy", attributes={}, raw_element=None))
        hops.append(PentahoHop(from_name=step_name, to_name=name))
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


class TestFlowParsers(unittest.TestCase):
    def test_abort_parse(self):
        cfg = parse_abort_config(ET.fromstring("""
        <step>
          <row_threshold>10</row_threshold>
          <message>Too many errors</message>
          <always_log_rows>Y</always_log_rows>
        </step>
        """))
        self.assertEqual(cfg["row_threshold"], 10)
        self.assertEqual(cfg["message"], "Too many errors")
        self.assertTrue(cfg["always_log_rows"])

    def test_append_parse(self):
        cfg = parse_append_streams_config(ET.fromstring("""
        <step><head_name>A</head_name><tail_name>B</tail_name></step>
        """))
        self.assertEqual(cfg["stream_order"], ["A", "B"])

    def test_block_until_parse(self):
        cfg = parse_block_until_steps_finish_config(ET.fromstring("""
        <step>
          <steps>
            <step><name>Load</name><CopyNr>0</CopyNr></step>
            <step><name>Prep</name><CopyNr>0</CopyNr></step>
          </steps>
        </step>
        """))
        self.assertEqual(len(cfg["wait_steps"]), 2)
        self.assertEqual(cfg["wait_steps"][0]["name"], "Load")

    def test_blocking_parse(self):
        cfg = parse_blocking_step_config(ET.fromstring("""
        <step><pass_all_rows>N</pass_all_rows><directory>/tmp</directory></step>
        """))
        self.assertFalse(cfg["pass_all_rows"])
        self.assertEqual(cfg["directory"], "/tmp")

    def test_switch_case_parse(self):
        cfg = parse_switch_case_config(ET.fromstring("""
        <step>
          <fieldname>status</fieldname>
          <default_target_step>Other</default_target_step>
          <cases>
            <case><value>A</value><target_step>Active</target_step></case>
            <case><value>I</value><target_step>Inactive</target_step></case>
          </cases>
        </step>
        """))
        self.assertEqual(cfg["switch_field"], "status")
        self.assertEqual(len(cfg["cases"]), 2)
        self.assertEqual(cfg["default_target_step"], "Other")

    def test_java_filter_parse(self):
        cfg = parse_java_filter_config(ET.fromstring("""
        <step>
          <condition>age &gt; 18</condition>
          <send_true_to>Adult</send_true_to>
          <send_false_to>Minor</send_false_to>
        </step>
        """))
        self.assertIn("age", cfg["condition"])
        self.assertEqual(cfg["send_true_to"], "Adult")

    def test_identify_last_row_parse(self):
        cfg = parse_identify_last_row_config(ET.fromstring(
            "<step><resultfieldname>is_last</resultfieldname></step>"
        ))
        self.assertEqual(cfg["result_field"], "is_last")

    def test_meta_inject_parse(self):
        cfg = parse_meta_inject_config(ET.fromstring("""
        <step>
          <filename>/etl/child.ktr</filename>
          <no_execution>Y</no_execution>
          <mappings>
            <mapping>
              <source_field>path</source_field>
              <target_step>CSV Input</target_step>
              <target_attribute>filename</target_attribute>
            </mapping>
          </mappings>
        </step>
        """))
        self.assertEqual(cfg["filename"], "/etl/child.ktr")
        self.assertTrue(cfg["no_execution"])
        self.assertEqual(cfg["mappings"][0]["target_step"], "CSV Input")

    def test_job_and_trans_executor_parse(self):
        job = parse_job_executor_config(ET.fromstring("""
        <step>
          <job_name>child_job</job_name>
          <filename>/jobs/child.kjb</filename>
          <group_size>100</group_size>
          <parameters>
            <parameter><name>P1</name><field>col1</field></parameter>
          </parameters>
        </step>
        """))
        self.assertEqual(job["job_name"], "child_job")
        self.assertEqual(job["parameters"][0]["name"], "P1")

        trans = parse_trans_executor_config(ET.fromstring("""
        <step>
          <trans_name>child_trans</trans_name>
          <filename>/trans/child.ktr</filename>
        </step>
        """))
        self.assertEqual(trans["trans_name"], "child_trans")

    def test_prioritize_and_single_threader_parse(self):
        pri = parse_prioritize_streams_config(ET.fromstring("""
        <step>
          <steps>
            <step><name>High</name></step>
            <step><name>Low</name></step>
          </steps>
        </step>
        """))
        self.assertEqual(pri["stream_priority"], ["High", "Low"])

        st = parse_single_threader_config(ET.fromstring("""
        <step>
          <filename>/sub.ktr</filename>
          <inject_step>Injector</inject_step>
          <retrieve_step>Output</retrieve_step>
          <batch_size>50</batch_size>
        </step>
        """))
        self.assertEqual(st["inject_step"], "Injector")
        self.assertEqual(st["batch_size"], "50")

    def test_parse_step_metadata_dispatch(self):
        el = ET.fromstring("<step><message>x</message><row_threshold>1</row_threshold></step>")
        meta = parse_step_metadata(el, "Abort")
        self.assertEqual(meta["message"], "x")


class TestFlowHandlers(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()
        register_builtin_validators()

    def test_flow_handlers_registered(self):
        types = set()
        for h in FLOW_HANDLERS:
            types |= set(getattr(h, "_TYPES", set()))
        for required in (
            "abort", "append", "blockuntilstepsfinish", "blockingstep",
            "detectemptystream", "metainject", "identifylastrow", "javafilter",
            "jobexecutor", "prioritystream", "singlethreader", "switchcase",
            "transexecutor",
        ):
            self.assertIn(required, types)

    def test_abort_raises_with_threshold(self):
        xml = """
        <step>
          <row_threshold>5</row_threshold>
          <message>Stop now</message>
        </step>
        """
        lines, status = self.registry.generate_code("Abort", _ctx(xml, "Abort", "Abort step"))
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("raise RuntimeError", code)
        self.assertIn("Stop now", code)
        self.assertIn("_abort_count_", code)
        self.assertTrue(_syntax_ok(lines))

    def test_append_streams_union(self):
        xml = """
        <step>
          <head_name>Head</head_name>
          <tail_name>Tail</tail_name>
        </step>
        """
        ctx = _ctx(xml, "Append", "Append streams", extra_inputs=["Head", "Tail"], with_input=False)
        # Rebuild so Head/Tail are the only predecessors
        lines, status = self.registry.generate_code("Append", ctx)
        code = "\n".join(lines)
        self.assertIn("unionByName", code)
        self.assertIn("df_Head", code)
        self.assertIn("df_Tail", code)
        self.assertTrue(_syntax_ok(lines))

    def test_block_until_documents_limitation(self):
        xml = """
        <step>
          <steps>
            <step><name>Input</name><CopyNr>0</CopyNr></step>
          </steps>
        </step>
        """
        lines, status = self.registry.generate_code(
            "BlockUntilStepsFinish",
            _ctx(xml, "BlockUntilStepsFinish", "Wait"),
        )
        code = "\n".join(lines)
        self.assertIn("LIMITATION", code)
        self.assertIn("df_Input.count()", code)
        self.assertIn(status, ("partial", "partially_supported", "converted"))

    def test_blocking_step_cache(self):
        lines, status = self.registry.generate_code(
            "BlockingStep",
            _ctx("<step><pass_all_rows>Y</pass_all_rows></step>", "BlockingStep", "Block"),
        )
        code = "\n".join(lines)
        self.assertIn(".cache()", code)
        self.assertIn(".count()", code)
        self.assertIn(status, ("converted", "partial", "partially_supported"))

    def test_detect_empty_stream(self):
        lines, status = self.registry.generate_code(
            "DetectEmptyStream",
            _ctx("<step/>", "DetectEmptyStream", "Empty check"),
        )
        code = "\n".join(lines)
        self.assertIn("limit(1).count()", code)
        self.assertIn("if _empty_flag_", code)
        self.assertTrue(_syntax_ok(lines))

    def test_dummy_passthrough_unchanged(self):
        lines, status = self.registry.generate_code(
            "Dummy",
            _ctx("<step/>", "Dummy", "Noop sink"),
        )
        code = "\n".join(lines)
        self.assertIn("Pass-through", code)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertNotIn("raise ", code)

    def test_filter_rows_still_works(self):
        xml = """
        <step>
          <compare>
            <condition>
              <leftvalue>id</leftvalue>
              <function>=</function>
              <value><type>Integer</type><text>1</text></value>
            </condition>
          </compare>
        </step>
        """
        lines, status = self.registry.generate_code(
            "FilterRows",
            _ctx(xml, "FilterRows", "Filter"),
        )
        code = "\n".join(lines)
        self.assertIn(".filter(", code)
        self.assertIn(status, ("converted", "partial", "partially_supported"))

    def test_identify_last_row(self):
        lines, status = self.registry.generate_code(
            "IdentifyLastRow",
            _ctx("<step><resultfieldname>is_last</resultfieldname></step>", "IdentifyLastRow", "Last"),
        )
        code = "\n".join(lines)
        self.assertIn("row_number()", code)
        self.assertIn("is_last", code)
        self.assertTrue(_syntax_ok(lines))

    def test_java_filter_supported(self):
        xml = """
        <step>
          <condition>status == "A"</condition>
          <send_true_to>Ok</send_true_to>
          <send_false_to>Bad</send_false_to>
        </step>
        """
        ctx = _ctx(xml, "JavaFilter", "JFilter", successors=["Ok", "Bad"])
        lines, status = self.registry.generate_code("JavaFilter", ctx)
        code = "\n".join(lines)
        self.assertIn(".filter(", code)
        self.assertTrue(_syntax_ok(lines))

    def test_java_filter_unsupported_preserved(self):
        xml = """
        <step>
          <condition>name.matches("x.*")</condition>
        </step>
        """
        lines, status = self.registry.generate_code(
            "JavaFilter",
            _ctx(xml, "JavaFilter", "JFilter2"),
        )
        code = "\n".join(lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn(status, ("partial", "partially_supported"))

    def test_java_filter_equals_ignore_case(self):
        xml = """
        <step>
          <condition>status.equalsIgnoreCase("Active")</condition>
        </step>
        """
        lines, status = self.registry.generate_code(
            "JavaFilter",
            _ctx(xml, "JavaFilter", "JFilterIg"),
        )
        code = "\n".join(lines)
        self.assertIn("lower(col(", code)
        self.assertIn(".filter(", code)
        self.assertIn(status, ("converted", "partial", "partially_supported"))

    def test_abort_always_log_and_option(self):
        xml = """
        <step>
          <row_threshold>0</row_threshold>
          <message>boom</message>
          <always_log_rows>Y</always_log_rows>
          <abort_option>AbortAndLog</abort_option>
        </step>
        """
        lines, status = self.registry.generate_code(
            "Abort", _ctx(xml, "Abort", "Abort log")
        )
        code = "\n".join(lines)
        self.assertIn("print(", code)
        self.assertIn("abort_option", code)
        self.assertIn("raise RuntimeError", code)

    def test_blocking_pass_all_rows_false_last_row(self):
        lines, status = self.registry.generate_code(
            "BlockingStep",
            _ctx("<step><pass_all_rows>N</pass_all_rows></step>", "BlockingStep", "Block last"),
        )
        code = "\n".join(lines)
        self.assertIn("row_number()", code)
        self.assertIn("_max_block", code)
        self.assertIn(status, ("converted", "partial", "partially_supported"))

    def test_append_single_stream_partial(self):
        lines, status = self.registry.generate_code(
            "Append",
            _ctx("<step><head_name>Input</head_name></step>", "Append", "Append1"),
        )
        code = "\n".join(lines)
        self.assertIn(status, ("partial", "partially_supported"))
        self.assertIn("df_Input", code)

    def test_switch_case_contains_and_typed(self):
        xml = """
        <step>
          <fieldname>code</fieldname>
          <use_contains>Y</use_contains>
          <case_value_type>Integer</case_value_type>
          <default_target_step>Other</default_target_step>
          <cases>
            <case><value>1</value><target_step>One</target_step></case>
          </cases>
        </step>
        """
        ctx = _ctx(xml, "SwitchCase", "RouteC", successors=["One", "Other"])
        lines, status = self.registry.generate_code("SwitchCase", ctx)
        code = "\n".join(lines)
        self.assertIn("contains(", code)
        self.assertIn("lit(1)", code)
        self.assertIn("df_One", code)
        self.assertIn("df_Other", code)

    def test_filter_true_false_branches(self):
        xml = """
        <step>
          <send_true_to>Ok</send_true_to>
          <send_false_to>Bad</send_false_to>
          <compare>
            <condition>
              <leftvalue>id</leftvalue>
              <function>=</function>
              <value><type>Integer</type><text>1</text></value>
            </condition>
          </compare>
        </step>
        """
        ctx = _ctx(xml, "FilterRows", "F", successors=["Ok", "Bad"])
        lines, status = self.registry.generate_code("FilterRows", ctx)
        code = "\n".join(lines)
        self.assertIn("df_Ok", code)
        self.assertIn("df_Bad", code)
        self.assertIn(".filter(", code)

    def test_block_until_missing_wait_step_warns(self):
        xml = """
        <step>
          <steps>
            <step><name>MissingStep</name><CopyNr>0</CopyNr></step>
          </steps>
        </step>
        """
        lines, status = self.registry.generate_code(
            "BlockUntilStepsFinish",
            _ctx(xml, "BlockUntilStepsFinish", "Wait2"),
        )
        code = "\n".join(lines)
        self.assertIn("WARNING", code)
        self.assertIn("MissingStep", code)

    def test_executor_parameters_preserved(self):
        xml = """
        <step>
          <filename>/j.kjb</filename>
          <job_name>child</job_name>
          <result_rows_target_step>Results</result_rows_target_step>
          <parameters>
            <parameter><name>ENV</name><field>env_col</field></parameter>
          </parameters>
        </step>
        """
        lines, status = self.registry.generate_code(
            "JobExecutor", _ctx(xml, "JobExecutor", "Exec"),
        )
        code = "\n".join(lines)
        self.assertIn("ENV", code)
        self.assertIn("result_rows_target_step", code)
        self.assertIn("_exec_meta_", code)

    def test_switch_case_when_and_branches(self):
        xml = """
        <step>
          <fieldname>region</fieldname>
          <default_target_step>Other</default_target_step>
          <cases>
            <case><value>US</value><target_step>US Branch</target_step></case>
            <case><value>EU</value><target_step>EU Branch</target_step></case>
          </cases>
        </step>
        """
        ctx = _ctx(
            xml, "SwitchCase", "Route",
            successors=["US Branch", "EU Branch", "Other"],
        )
        lines, status = self.registry.generate_code("SwitchCase", ctx)
        code = "\n".join(lines)
        self.assertIn("when(", code)
        self.assertIn("df_US_Branch", code)
        self.assertIn("df_EU_Branch", code)
        self.assertIn("df_Other", code)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertTrue(_syntax_ok(lines))

    def test_prioritize_streams(self):
        xml = """
        <step>
          <steps>
            <step><name>High</name></step>
            <step><name>Low</name></step>
          </steps>
        </step>
        """
        ctx = _ctx(xml, "PriorityStream", "Prio", extra_inputs=["High", "Low"], with_input=False)
        lines, status = self.registry.generate_code("PriorityStream", ctx)
        code = "\n".join(lines)
        self.assertIn("unionByName", code)
        self.assertIn("_prio", code)
        self.assertTrue(_syntax_ok(lines))

    def test_meta_inject_stub(self):
        xml = """
        <step>
          <filename>/child.ktr</filename>
          <mappings>
            <mapping>
              <source_field>f</source_field>
              <target_step>S</target_step>
              <target_attribute>a</target_attribute>
            </mapping>
          </mappings>
        </step>
        """
        lines, status = self.registry.generate_code(
            "MetaInject",
            _ctx(xml, "MetaInject", "Inject"),
        )
        code = "\n".join(lines)
        self.assertIn("_meta_inject_", code)
        self.assertIn("LIMITATION", code)
        self.assertIn(status, ("partial", "partially_supported"))

    def test_job_executor_stub(self):
        lines, status = self.registry.generate_code(
            "JobExecutor",
            _ctx(
                "<step><job_name>j</job_name><filename>/j.kjb</filename></step>",
                "JobExecutor",
                "Run job",
            ),
        )
        code = "\n".join(lines)
        self.assertIn("_exec_meta_", code)
        self.assertIn("LIMITATION", code)

    def test_trans_executor_stub(self):
        lines, status = self.registry.generate_code(
            "TransExecutor",
            _ctx(
                "<step><trans_name>t</trans_name><filename>/t.ktr</filename></step>",
                "TransExecutor",
                "Run trans",
            ),
        )
        code = "\n".join(lines)
        self.assertIn("_exec_meta_", code)
        self.assertIn("Transformation Executor", code)

    def test_single_threader_stub(self):
        lines, status = self.registry.generate_code(
            "SingleThreader",
            _ctx(
                "<step><filename>/s.ktr</filename><inject_step>I</inject_step></step>",
                "SingleThreader",
                "ST",
            ),
        )
        code = "\n".join(lines)
        self.assertIn("_single_threader_", code)
        self.assertIn("LIMITATION", code)
        self.assertIn("distribut", code.lower())


if __name__ == "__main__":
    unittest.main()
