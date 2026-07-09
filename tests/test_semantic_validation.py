"""Tests for semantic validation and honest conversion reporting."""

from __future__ import annotations

import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.reporting import build_conversion_report
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.validation.code_checks import validate_python_fragment
from pentaho_converter.validation.step_validators import parse_step_config, register_builtin_validators


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(
        name=step_name,
        step_type=step_type,
        attributes={},
        raw_element=step_el,
    )
    trans = PentahoTransformation(name="EmployeeDemo", file_path=Path("EmployeeDemo.ktr"))
    if with_input:
        input_step = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [input_step, step]
        hops = [PentahoHop(from_name="Input", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


class TestSemanticValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_calculator_requires_output_columns(self):
        xml = """
        <step>
          <calculation>
            <field_name>total</field_name>
            <calc_type>ADD</calc_type>
            <field_a>a</field_a>
            <field_b>b</field_b>
          </calculation>
        </step>
        """
        outcome = self.registry.convert_step("Calculator", _ctx(xml, "Calculator", "Calc"))
        self.assertIn(outcome.status, ("converted", "partial"))
        self.assertGreater(outcome.semantic_score, 0.5)
        self.assertIn("total", "\n".join(outcome.code_lines))

    def test_passthrough_marked_partial_or_lower(self):
        outcome = self.registry.convert_step(
            "TotallyUnknownStepXYZ",
            _ctx("<step/>", "TotallyUnknownStepXYZ", "Mystery"),
        )
        self.assertIn(outcome.status, ("unsupported", "partial", "partially_supported", "failed"))
        self.assertLess(outcome.semantic_score, 0.95)

    def test_placeholder_code_fails_syntax_validation(self):
        ok, errors = validate_python_fragment(
            ["df_x = spark.createDataFrame([], '_placeholder STRING')"]
        )
        self.assertFalse(ok)
        self.assertTrue(errors)

    def test_filter_rows_generates_filter(self):
        xml = """
        <step>
          <compare>
            <condition>
              <negated>N</negated>
              <leftvalue>age</leftvalue>
              <function>></function>
              <value><type>Integer</type><text>18</text></value>
            </condition>
          </compare>
        </step>
        """
        outcome = self.registry.convert_step("FilterRows", _ctx(xml, "FilterRows", "Filter"))
        self.assertIn(".filter(", "\n".join(outcome.code_lines))
        self.assertGreaterEqual(outcome.semantic_score, 0.5)

    def test_simple_filter_condition_uses_equality(self):
        from pentaho_converter.filter_converter import convert_simple_condition

        expr = convert_simple_condition("status = 'ACTIVE'")
        self.assertIn("==", expr)
        self.assertNotIn("= 'ACTIVE'", expr.replace("==", ""))

    def test_report_has_two_metrics(self):
        from pentaho_converter.conversion_outcome import StepConversionOutcome
        from pentaho_converter.models import ConversionStats

        stats = ConversionStats()
        stats.step_outcomes = [
            StepConversionOutcome(
                status="converted", semantic_score=1.0, detail="RowGen", handler_name="RowGenerator"
            ),
            StepConversionOutcome(
                status="partial", semantic_score=0.6, detail="Formula", handler_name="Formula"
            ),
        ]
        stats.step_results = []
        report = build_conversion_report(stats, "EmployeeDemo")
        self.assertEqual(report.transformation_name, "EmployeeDemo")
        self.assertGreater(report.coverage_percent, 0)
        self.assertGreater(report.semantic_accuracy_percent, 0)
        self.assertLess(report.semantic_accuracy_percent, 100)


if __name__ == "__main__":
    unittest.main()
