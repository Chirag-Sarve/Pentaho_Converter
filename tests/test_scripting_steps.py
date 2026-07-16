"""Tests for Pentaho Scripting transformation migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.scripting_converter import convert_java_expression
from pentaho_converter.step_xml import (
    parse_exec_sql_config,
    parse_exec_sql_row_config,
    parse_javascript_value_config,
    parse_regex_eval_config,
    parse_rules_accumulator_config,
    parse_rules_executor_config,
    parse_step_metadata,
    parse_user_defined_java_class_config,
    parse_user_defined_java_expression_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.scripting_handlers import SCRIPTING_HANDLERS


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
    trans = PentahoTransformation(name="ScriptingTrans", file_path=Path("scripting.ktr"))
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


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestScriptingParsers(unittest.TestCase):
    def test_exec_sql_parse(self):
        cfg = parse_exec_sql_config(ET.fromstring("""
        <step>
          <connection>dw</connection>
          <sql>UPDATE tgt SET x=1 WHERE id=?</sql>
          <execute_each_row>Y</execute_each_row>
          <single_statement>Y</single_statement>
          <set_params>Y</set_params>
          <arguments><argument><name>id</name></argument></arguments>
          <insert_field>ins</insert_field>
        </step>
        """))
        self.assertEqual(cfg["connection"], "dw")
        self.assertIn("UPDATE", cfg["sql"])
        self.assertTrue(cfg["execute_each_row"])
        self.assertEqual(cfg["arguments"], ["id"])
        self.assertEqual(cfg["insert_field"], "ins")

    def test_exec_sql_row_parse(self):
        cfg = parse_exec_sql_row_config(ET.fromstring("""
        <step>
          <connection>dw</connection>
          <sql_field>sql_text</sql_field>
          <sendOneStatement>N</sendOneStatement>
          <commit>10</commit>
        </step>
        """))
        self.assertEqual(cfg["sql_field"], "sql_text")
        self.assertFalse(cfg["send_one_statement"])
        self.assertEqual(cfg["commit"], "10")

    def test_javascript_parse(self):
        cfg = parse_javascript_value_config(ET.fromstring("""
        <step>
          <jsScripts>
            <jsScript>
              <jsScript_type>0</jsScript_type>
              <jsScript_name>Script 1</jsScript_name>
              <jsScript_script>var total = price * qty;</jsScript_script>
            </jsScript>
          </jsScripts>
          <fields>
            <field><name>total</name><rename>total</rename><type>Number</type></field>
          </fields>
          <optimizationLevel>9</optimizationLevel>
        </step>
        """))
        self.assertIn("price * qty", cfg["script"])
        self.assertEqual(cfg["fields"][0]["name"], "total")
        self.assertEqual(cfg["optimization_level"], "9")

    def test_regex_eval_parse(self):
        cfg = parse_regex_eval_config(ET.fromstring("""
        <step>
          <matcher>phone</matcher>
          <script>(\\d{3})-(\\d{4})</script>
          <resultfieldname>ok</resultfieldname>
          <caseinsensitive>Y</caseinsensitive>
          <allowcapturegroups>Y</allowcapturegroups>
          <fields>
            <field><name>area</name><type>String</type></field>
            <field><name>num</name><type>String</type></field>
          </fields>
        </step>
        """))
        self.assertEqual(cfg["matcher"], "phone")
        self.assertTrue(cfg["case_insensitive"])
        self.assertEqual(len(cfg["fields"]), 2)

    def test_rules_parse(self):
        cfg = parse_rules_accumulator_config(ET.fromstring("""
        <step>
          <rule-definition>rule "r1" when Fact(status == "A") then Result(flag = "Y") end</rule-definition>
          <fields><field><name>flag</name><type>String</type></field></fields>
        </step>
        """))
        self.assertIn("rule", cfg["rule_definition"])
        self.assertEqual(cfg["result_columns"][0]["name"], "flag")

        cfg2 = parse_rules_executor_config(ET.fromstring("""
        <step><rule-file>rules.drl</rule-file></step>
        """))
        self.assertEqual(cfg2["rule_file"], "rules.drl")

    def test_udjc_parse(self):
        cfg = parse_user_defined_java_class_config(ET.fromstring("""
        <step>
          <definitions>
            <definition>
              <class_type>TRANSFORM_CLASS</class_type>
              <class_name>MyProc</class_name>
              <class_source>import java.util.*; public boolean processRow(){ return true; }</class_source>
            </definition>
          </definitions>
          <fields><field><field_name>out1</field_name><type>String</type></field></fields>
        </step>
        """))
        self.assertEqual(cfg["class_name"], "MyProc")
        self.assertIn("processRow", cfg["class_source"])
        self.assertEqual(cfg["fields"][0]["name"], "out1")

    def test_udje_parse(self):
        cfg = parse_user_defined_java_expression_config(ET.fromstring("""
        <step>
          <fields>
            <field><name>total</name><java>a + b</java><type>Number</type></field>
          </fields>
        </step>
        """))
        self.assertEqual(cfg["fields"][0]["expression"], "a + b")


class TestScriptingConverters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertGreaterEqual(len(SCRIPTING_HANDLERS), 9)
        handled = set()
        for h in SCRIPTING_HANDLERS:
            handled |= set(getattr(h, "_TYPES", set()))
        for t in (
            "execsql", "execsqlrow", "formula", "scriptvaluemod",
            "regexeval", "ruleaccumulator", "ruleexecutor",
            "userdefinedjavaclass", "userdefinedjavaexpression",
        ):
            self.assertIn(t, handled)

    def test_exec_sql(self):
        xml = """
        <step>
          <connection>dw</connection>
          <sql>SELECT 1 AS x</sql>
          <execute_each_row>N</execute_each_row>
        </step>
        """
        outcome = self.registry.convert_step("ExecSQL", _ctx(xml, "ExecSQL", "RunSQL"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("spark.sql(", code)
        self.assertIn("preserved.connection", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_exec_sql_row(self):
        xml = """
        <step>
          <connection>dw</connection>
          <sql_field>sql_text</sql_field>
          <sendOneStatement>Y</sendOneStatement>
        </step>
        """
        outcome = self.registry.convert_step("ExecSQLRow", _ctx(xml, "ExecSQLRow", "RowSQL"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("toLocalIterator", code)
        self.assertIn("does not scale", code)
        self.assertIn("spark.sql(_sql)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "partial")

    def test_formula(self):
        xml = """
        <step>
          <formula>
            <field_name>band</field_name>
            <formula_string>IF([amount]&gt;100;"HI";"LO")</formula_string>
            <value_type>String</value_type>
          </formula>
        </step>
        """
        outcome = self.registry.convert_step("Formula", _ctx(xml, "Formula", "F"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("withColumn", code)
        self.assertIn("when(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_javascript_simple(self):
        xml = """
        <step>
          <jsScripts>
            <jsScript>
              <jsScript_type>0</jsScript_type>
              <jsScript_name>Script 1</jsScript_name>
              <jsScript_script>var total = price * qty;</jsScript_script>
            </jsScript>
          </jsScripts>
          <fields>
            <field><name>total</name><rename>total</rename><type>Number</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "ScriptValueMod", _ctx(xml, "ScriptValueMod", "JS")
        )
        code = "\n".join(outcome.code_lines)
        self.assertTrue(
            "original JavaScript" in code or "JS transform" in code or "price" in code
        )
        self.assertIn("withColumn", code)
        self.assertIn("total", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_regex_eval_captures(self):
        xml = """
        <step>
          <matcher>phone</matcher>
          <script>(\\d{3})-(\\d{4})</script>
          <resultfieldname>matched</resultfieldname>
          <caseinsensitive>Y</caseinsensitive>
          <allowcapturegroups>Y</allowcapturegroups>
          <fields>
            <field><name>area</name></field>
            <field><name>num</name></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step("RegexEval", _ctx(xml, "RegexEval", "RE"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("rlike(", code)
        self.assertIn("regexp_extract(", code)
        self.assertIn("(?i)", code)
        self.assertIn('lit("Y")', code)
        self.assertIn('lit("N")', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("converted", "partial"))

    def test_rules_accumulator(self):
        xml = """
        <step>
          <rule-definition>when Fact(status == "A") then Result(flag = "Y")</rule-definition>
          <fields><field><name>flag</name></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "RuleAccumulator", _ctx(xml, "RuleAccumulator", "RA")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("when(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_rules_executor(self):
        xml = """
        <step>
          <rule-definition>rule "x" when X(a == "1") then Y(b = "2") end</rule-definition>
        </step>
        """
        outcome = self.registry.convert_step(
            "RuleExecutor", _ctx(xml, "RuleExecutor", "RX")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("Drools", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_udjc(self):
        xml = """
        <step>
          <definitions>
            <definition>
              <class_name>Proc</class_name>
              <class_source>import java.util.*; public boolean processRow(){ return true; }</class_source>
            </definition>
          </definitions>
          <fields><field><field_name>out</field_name></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "UserDefinedJavaClass", _ctx(xml, "UserDefinedJavaClass", "UDJC")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("cannot be translated", code)
        self.assertIn("processRow", code)
        self.assertIn("withColumn", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_udje_simple(self):
        xml = """
        <step>
          <fields>
            <field><name>total</name><java>a + b</java></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "UserDefinedJavaExpression", _ctx(xml, "UserDefinedJavaExpression", "UDJE")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("withColumn", code)
        self.assertIn("col(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        expr, warn = convert_java_expression("a + b")
        self.assertIsNotNone(expr)
        self.assertIsNone(warn)

    def test_udje_unsupported(self):
        xml = """
        <step>
          <fields>
            <field><name>x</name><java>new HashMap()</java></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "UserDefinedJavaExpression", _ctx(xml, "UserDefinedJavaExpression", "UDJE2")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertEqual(outcome.status, "partial")


class TestScriptingEdgeCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_exec_sql_empty(self):
        outcome = self.registry.convert_step(
            "ExecSQL", _ctx("<step><sql></sql></step>", "ExecSQL", "EmptySQL")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_exec_sql_each_row_args_quote_and_stats(self):
        xml = """
        <step>
          <connection>dw</connection>
          <sql>UPDATE t SET a=? WHERE id=?</sql>
          <execute_each_row>Y</execute_each_row>
          <set_params>Y</set_params>
          <quoteString>Y</quoteString>
          <arguments>
            <argument><name>val</name></argument>
            <argument><name>id</name></argument>
          </arguments>
          <insert_field>ins_cnt</insert_field>
        </step>
        """
        outcome = self.registry.convert_step("ExecSQL", _ctx(xml, "ExecSQL", "BindSQL"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("toLocalIterator", code)
        self.assertTrue("quote_string" in code or "quoteString" in code or "SQL-quoted" in code)
        self.assertIn("ins_cnt", code)
        self.assertIn("replace(\"'\", \"''\")", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "partial")

    def test_formula_empty_and_unsupported(self):
        empty = self.registry.convert_step(
            "Formula",
            _ctx("<step><field_name>x</field_name><formula></formula></step>", "Formula", "EF"),
        )
        self.assertIn("WARNING", "\n".join(empty.code_lines))
        self.assertEqual(empty.status, "partial")

        bad = self.registry.convert_step(
            "Formula",
            _ctx(
                "<step><field_name>x</field_name>"
                "<formula>INDIRECT([a])</formula></step>",
                "Formula",
                "BadF",
            ),
        )
        code = "\n".join(bad.code_lines)
        self.assertTrue("WARNING" in code or "expr(" in code)
        self.assertTrue(_syntax_ok(bad.code_lines))

    def test_javascript_unsupported_and_start_script(self):
        xml = """
        <step>
          <jsScripts>
            <jsScript>
              <jsScript_type>1</jsScript_type>
              <jsScript_name>Start</jsScript_name>
              <jsScript_script>var init = 1;</jsScript_script>
            </jsScript>
            <jsScript>
              <jsScript_type>0</jsScript_type>
              <jsScript_name>Main</jsScript_name>
              <jsScript_script>
                while(true){ putRow(row); }
                var out = price;
              </jsScript_script>
            </jsScript>
          </jsScripts>
          <fields><field><name>out</name><type>Number</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "ScriptValueMod", _ctx(xml, "ScriptValueMod", "JSBad")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("start", code.lower())
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "partial")

    def test_regex_invalid_pattern(self):
        xml = """
        <step>
          <matcher>f</matcher>
          <script>([unclosed</script>
          <resultfieldname>ok</resultfieldname>
        </step>
        """
        outcome = self.registry.convert_step("RegexEval", _ctx(xml, "RegexEval", "BadRE"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("Invalid regex", code)
        self.assertIn('lit("Y")', code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "partial")

    def test_rules_accumulator_migration_hint(self):
        xml = """
        <step>
          <rule-definition>rule "complex" when eval(true) then end</rule-definition>
          <fields><field><name>total</name><type>Number</type></field></fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "RuleAccumulator", _ctx(xml, "RuleAccumulator", "RA2")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("groupBy", code)
        self.assertIn("total", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
