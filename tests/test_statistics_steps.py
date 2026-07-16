"""Tests for Statistics transformation migration (Group By family + sampling + metrics)."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_analytic_query_config,
    parse_group_by_config,
    parse_reservoir_sampling_config,
    parse_sample_rows_config,
    parse_steps_metrics_config,
    parse_step_metadata,
    parse_univariate_stats_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    input_columns: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="Trans", file_path=Path("t.ktr"))
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Input", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    ctx = StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)
    if input_columns:
        ctx.extra["input_columns"] = list(input_columns)
    return ctx


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {l}" for l in lines))
        return True
    except SyntaxError:
        return False


class TestStatisticsParsers(unittest.TestCase):
    def test_analytic_query_parse(self):
        xml = """<step>
          <group><field><name>customer_id</name></field></group>
          <fields>
            <field>
              <aggregate>PREV_MONTH</aggregate>
              <subject>purchase_month</subject>
              <type>LAG</type>
              <valuefield>1</valuefield>
            </field>
            <field>
              <aggregate>NEXT_AMT</aggregate>
              <subject>amount</subject>
              <type>LEAD</type>
              <valuefield>2</valuefield>
            </field>
          </fields>
        </step>"""
        cfg = parse_analytic_query_config(ET.fromstring(xml))
        self.assertEqual(cfg["group_fields"], ["customer_id"])
        self.assertEqual(len(cfg["analytic_fields"]), 2)
        self.assertEqual(cfg["analytic_fields"][0]["function"], "LAG")
        self.assertEqual(cfg["analytic_fields"][1]["offset"], 2)

    def test_memory_group_by_parse(self):
        xml = """<step>
          <group><field><name>dept</name></field><field><name>dept</name></field></group>
          <fields>
            <field><name>total</name><subject>salary</subject><aggregate>SUM</aggregate></field>
          </fields>
          <give_back_row>N</give_back_row>
        </step>"""
        cfg = parse_group_by_config(ET.fromstring(xml))
        self.assertEqual(cfg["group_keys"], ["dept"])  # deduped
        self.assertEqual(cfg["aggregates"][0]["aggregate"], "SUM")

    def test_sample_rows_parse(self):
        xml = """<step>
          <linesrange>1..5,10,20..22</linesrange>
          <linenumfield>rn</linenumfield>
        </step>"""
        cfg = parse_sample_rows_config(ET.fromstring(xml))
        self.assertEqual(cfg["line_ranges"], [(1, 5), (10, 10), (20, 22)])
        self.assertEqual(cfg["line_num_field"], "rn")

    def test_reservoir_parse(self):
        xml = """<step>
          <reservoir_sampling>
            <sample_size>50</sample_size>
            <seed>42</seed>
          </reservoir_sampling>
        </step>"""
        cfg = parse_reservoir_sampling_config(ET.fromstring(xml))
        self.assertEqual(cfg["sample_size"], 50)
        self.assertEqual(cfg["seed"], 42)

    def test_univariate_parse(self):
        xml = """<step>
          <univariate_stats>
            <source_field_name>amount</source_field_name>
            <N>Y</N><mean>Y</mean><stdDev>Y</stdDev>
            <min>Y</min><max>Y</max><median>Y</median>
            <percentile>0.9</percentile><interpolate>Y</interpolate>
          </univariate_stats>
        </step>"""
        cfg = parse_univariate_stats_config(ET.fromstring(xml))
        self.assertEqual(cfg["stats"][0]["source_field"], "amount")
        self.assertAlmostEqual(cfg["stats"][0]["percentile"], 0.9)

    def test_steps_metrics_parse(self):
        xml = """<step>
          <steps>
            <step><name>Input</name><copyNr>0</copyNr><stepRequired>Y</stepRequired></step>
          </steps>
          <stepnamefield>Stepname</stepnamefield>
          <steplinesinputfield>Linesinput</steplinesinputfield>
        </step>"""
        cfg = parse_steps_metrics_config(ET.fromstring(xml))
        self.assertEqual(cfg["metric_steps"][0]["name"], "Input")
        self.assertTrue(cfg["metric_steps"][0]["required"])


class TestStatisticsCodegen(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_group_by_still_works(self):
        xml = """<step>
          <name>GB</name><type>GroupBy</type>
          <group><field><name>region</name></field></group>
          <fields>
            <field><name>total</name><subject>sales</subject><aggregate>SUM</aggregate></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step("GroupBy", _ctx(xml, "GroupBy", "GB"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("groupBy", code)
        self.assertIn("agg", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_memory_group_by(self):
        xml = """<step>
          <name>MGB</name><type>MemoryGroupBy</type>
          <group><field><name>region</name></field></group>
          <fields>
            <field><name>avg_sales</name><subject>sales</subject><aggregate>AVERAGE</aggregate></field>
            <field><name>cnt</name><subject>sales</subject><aggregate>COUNT_ALL</aggregate></field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "MemoryGroupBy", _ctx(xml, "MemoryGroupBy", "MGB")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("Memory Group By", code)
        self.assertIn("groupBy", code)
        self.assertTrue(any("distributed" in l.lower() or "JVM" in l for l in outcome.code_lines))
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_analytic_query_lead_lag(self):
        xml = """<step>
          <name>AQ</name><type>AnalyticQuery</type>
          <group><field><name>cust</name></field></group>
          <fields>
            <field>
              <aggregate>prev_amt</aggregate>
              <subject>amount</subject>
              <type>LAG</type>
              <valuefield>1</valuefield>
            </field>
            <field>
              <aggregate>next_amt</aggregate>
              <subject>amount</subject>
              <type>LEAD</type>
              <valuefield>1</valuefield>
            </field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "AnalyticQuery", _ctx(xml, "AnalyticQuery", "AQ")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("Window", code)
        self.assertIn("lag(", code)
        self.assertIn("lead(", code)
        self.assertIn("partitionBy", code)
        self.assertNotIn("spark.sql", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_analytic_query_cumulative_rank_extension(self):
        xml = """<step>
          <name>AQ2</name><type>AnalyticQuery</type>
          <group><field><name>g</name></field></group>
          <fields>
            <field>
              <aggregate>rnk</aggregate>
              <subject>v</subject>
              <type>RANK</type>
              <valuefield>0</valuefield>
            </field>
            <field>
              <aggregate>csum</aggregate>
              <subject>v</subject>
              <type>SUM</type>
              <valuefield>0</valuefield>
            </field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "AnalyticQuery", _ctx(xml, "AnalyticQuery", "AQ2")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("rank()", code)
        self.assertIn("_sum(", code)
        self.assertIn("rowsBetween", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_sample_rows_ranges(self):
        xml = """<step>
          <name>SR</name><type>SampleRows</type>
          <linesrange>1..3,10</linesrange>
          <linenumfield>line_nr</linenumfield>
        </step>"""
        outcome = self.registry.convert_step("SampleRows", _ctx(xml, "SampleRows", "SR"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("row_number", code)
        self.assertIn("filter", code)
        self.assertIn("line_nr", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_sample_rows_percentage(self):
        xml = """<step>
          <name>SR2</name><type>SampleRows</type>
          <linesrange>1</linesrange>
          <percentage>0.25</percentage>
          <seed>7</seed>
        </step>"""
        outcome = self.registry.convert_step("SampleRows", _ctx(xml, "SampleRows", "SR2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn(".sample(", code)
        self.assertIn("seed=7", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_reservoir_sampling(self):
        xml = """<step>
          <name>RS</name><type>ReservoirSampling</type>
          <reservoir_sampling>
            <sample_size>100</sample_size>
            <seed>3</seed>
          </reservoir_sampling>
        </step>"""
        outcome = self.registry.convert_step(
            "ReservoirSampling", _ctx(xml, "ReservoirSampling", "RS")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("rand(3)", code)
        self.assertIn("limit(100)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_univariate_stats(self):
        xml = """<step>
          <name>UV</name><type>UnivariateStats</type>
          <univariate_stats>
            <source_field_name>x</source_field_name>
            <N>Y</N><mean>Y</mean><stdDev>Y</stdDev>
            <min>Y</min><max>Y</max><median>Y</median>
            <percentile>0.95</percentile><interpolate>Y</interpolate>
          </univariate_stats>
        </step>"""
        outcome = self.registry.convert_step(
            "UnivariateStats", _ctx(xml, "UnivariateStats", "UV")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".agg(", code)
        self.assertIn("count(", code)
        self.assertIn("avg(", code)
        self.assertIn("stddev_samp", code)
        self.assertIn("variance_samp", code)
        self.assertIn("_min(", code)
        self.assertIn("_max(", code)
        self.assertIn("percentile_approx", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_sample_rows_fixed_count_with_seed(self):
        xml = """<step>
          <name>SR3</name><type>SampleRows</type>
          <linesrange>1</linesrange>
          <nr_lines>25</nr_lines>
          <seed>99</seed>
        </step>"""
        outcome = self.registry.convert_step("SampleRows", _ctx(xml, "SampleRows", "SR3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("rand(99)", code)
        self.assertIn("limit(25)", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_memory_group_by_native_xml_format(self):
        """MemoryGroupByMeta uses aggregate=output name, type=agg function."""
        xml = """<step>
          <name>MGB2</name><type>MemoryGroupBy</type>
          <give_back_row>Y</give_back_row>
          <group><field><name>dept</name></field></group>
          <fields>
            <field>
              <aggregate>total</aggregate>
              <subject>amt</subject>
              <type>SUM</type>
            </field>
            <field>
              <aggregate>sd</aggregate>
              <subject>amt</subject>
              <type>STANDARD_DEVIATION</type>
            </field>
            <field>
              <aggregate>med</aggregate>
              <subject>amt</subject>
              <type>MEDIAN</type>
            </field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "MemoryGroupBy", _ctx(xml, "MemoryGroupBy", "MGB2")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("groupBy", code)
        self.assertIn("stddev_samp", code)
        self.assertIn("percentile_approx", code)
        self.assertIn("JVM", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_analytic_query_with_order_fields(self):
        xml = """<step>
          <name>AQ3</name><type>AnalyticQuery</type>
          <group><field><name>cust</name></field></group>
          <orders>
            <field><name>order_dt</name><ascending>Y</ascending></field>
          </orders>
          <fields>
            <field>
              <aggregate>prev</aggregate>
              <subject>amt</subject>
              <type>LAG</type>
              <valuefield>1</valuefield>
            </field>
          </fields>
        </step>"""
        outcome = self.registry.convert_step(
            "AnalyticQuery", _ctx(xml, "AnalyticQuery", "AQ3")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn('col("order_dt").asc()', code)
        self.assertIn("lag(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_edge_empty_input_and_missing_config(self):
        xml = """<step><name>AQE</name><type>AnalyticQuery</type>
          <group/><fields/></step>"""
        outcome = self.registry.convert_step(
            "AnalyticQuery",
            _ctx(xml, "AnalyticQuery", "AQE", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

        xml2 = """<step><name>SRX</name><type>SampleRows</type>
          <linesrange>abc</linesrange></step>"""
        outcome2 = self.registry.convert_step(
            "SampleRows", _ctx(xml2, "SampleRows", "SRX")
        )
        self.assertIn("filter(lit(False))", "\n".join(outcome2.code_lines))
        self.assertTrue(_syntax_ok(outcome2.code_lines))

        xml3 = """<step><name>GBE</name><type>GroupBy</type>
          <group/><fields/></step>"""
        outcome3 = self.registry.convert_step("GroupBy", _ctx(xml3, "GroupBy", "GBE"))
        code3 = "\n".join(outcome3.code_lines)
        self.assertIn("groupBy()", code3)
        self.assertTrue(_syntax_ok(outcome3.code_lines))

    def test_steps_metrics(self):
        xml = """<step>
          <name>SM</name><type>StepsMetrics</type>
          <steps>
            <step><name>Input</name><copyNr>0</copyNr><stepRequired>N</stepRequired></step>
          </steps>
          <stepnamefield>Stepname</stepnamefield>
          <stepidfield>Stepid</stepidfield>
          <steplinesinputfield>Linesinput</steplinesinputfield>
          <steplinesoutputfield>Linesoutput</steplinesoutputfield>
          <steplinesreadfield>Linesread</steplinesreadfield>
          <steplinesupdatedfield>Linesupdated</steplinesupdatedfield>
          <steplineswrittentfield>Lineswritten</steplineswrittentfield>
          <steplineserrorsfield>Lineserrors</steplineserrorsfield>
          <stepsecondsfield>Seconds</stepsecondsfield>
        </step>"""
        outcome = self.registry.convert_step(
            "StepsMetrics", _ctx(xml, "StepsMetrics", "SM")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("count()", code)
        self.assertIn("createDataFrame", code)
        self.assertIn("unsupported runtime", code.lower())
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_parse_step_metadata_keys(self):
        for step_type, snippet in [
            ("AnalyticQuery", "<group><field><name>a</name></field></group><fields/>"),
            ("MemoryGroupBy", "<group><field><name>a</name></field></group><fields/>"),
            ("SampleRows", "<linesrange>1</linesrange>"),
            ("ReservoirSampling", "<reservoir_sampling><sample_size>1</sample_size><seed>1</seed></reservoir_sampling>"),
            ("UnivariateStats", "<univariate_stats><source_field_name>x</source_field_name></univariate_stats>"),
            ("StepsMetrics", "<steps/><stepnamefield>n</stepnamefield>"),
        ]:
            el = ET.fromstring(f"<step>{snippet}</step>")
            meta = parse_step_metadata(el, step_type)
            self.assertIsInstance(meta, dict)
            self.assertTrue(meta, f"expected metadata for {step_type}")


if __name__ == "__main__":
    unittest.main()
