"""Production-audit regressions for TextFile I/O, validation scoring, and related generators."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.calculator_converter import (
    convert_calculation_result,
)
from pentaho_converter.conversion_outcome import format_display_status
from pentaho_converter.graph import StepDAG
from pentaho_converter.group_by_converter import convert_group_by_step
from pentaho_converter.lineage import validate_column_lineage
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.path_utils import (
    normalize_text_file_basename,
    spark_text_file_path_expr,
)
from pentaho_converter.step_xml import CalculationSpec
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.validation.base import SemanticValidationResult
from pentaho_converter.validation.step_validators import TextFileOutputValidator, _score
from pentaho_converter.validation.validators_extended import TextFileInputValidator
from pentaho_converter.value_mapper_converter import convert_value_mapper_step


def _ctx(step_xml: str, step_type: str, step_name: str, *, with_input: bool = False) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    trans = PentahoTransformation(name="AuditTrans", file_path=Path("audit.ktr"))
    if with_input:
        inp = PentahoStep(name="Upstream", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Upstream", to_name=step_name)]
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


class TestTextFileInputLocaleMetadata(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_locale_and_field_format_are_info_not_partial(self):
        xml = """
        <step>
          <filename>C:\\data\\customers_staging</filename>
          <extention>csv</extention>
          <separator>,</separator>
          <enclosure>"</enclosure>
          <encoding>UTF-8</encoding>
          <header>Y</header>
          <date_format_locale>en_US</date_format_locale>
          <date_format_lenient>Y</date_format_lenient>
          <shortFileFieldName>short_name</shortFileFieldName>
          <fields>
            <field>
              <name>amount</name><type>Number</type>
              <currency>$</currency><decimal>.</decimal><group>,</group>
              <format>#,##0.00</format><repeat>N</repeat>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "TextFileInput", _ctx(xml, "TextFileInput", "TFI_LOCALE")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("spark.read", code)
        self.assertIn(".csv(", code)
        self.assertIn("customers_staging.csv", code)
        self.assertIn("date_format_locale", code)
        self.assertTrue(
            any(line.lstrip().startswith("# INFO:") for line in outcome.code_lines),
            msg="locale/formatting should be INFO comments",
        )
        self.assertEqual(outcome.status, "converted")
        self.assertGreaterEqual(outcome.semantic_score, 0.95)
        display = outcome.display_status or format_display_status(
            outcome.status, warnings=outcome.warnings, infos=outcome.infos, errors=outcome.errors
        )
        self.assertTrue(
            "Legacy metadata" in display or display == "CONVERTED",
            msg=f"unexpected display_status={display!r} infos={outcome.infos!r}",
        )


class TestTextFileOutputFormattingMetadata(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_split_every_and_masks_are_info_converted(self):
        xml = """
        <step>
          <file>
            <name>C:\\data\\customers_staging</name>
            <extention>csv</extention>
            <split>Y</split>
            <splitevery>5000</splitevery>
            <create_parent_folder>Y</create_parent_folder>
          </file>
          <separator>,</separator>
          <enclosure>"</enclosure>
          <encoding>UTF-8</encoding>
          <header>Y</header>
          <fields>
            <field>
              <name>amount</name>
              <format>0.00</format>
              <currency>$</currency>
              <precision>2</precision>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "TextFileOutput",
            _ctx(xml, "TextFileOutput", "TFO_FMT", with_input=True),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn(".csv(", code)
        self.assertIn(".mode(", code)
        self.assertIn("split_every", code)
        self.assertIn("preserved.field_format", code)
        self.assertTrue(any("# INFO:" in line for line in outcome.code_lines))
        self.assertEqual(outcome.status, "converted")
        self.assertGreaterEqual(outcome.semantic_score, 0.95)


class TestSortRowsHelperLineage(unittest.TestCase):
    def test_sort_helper_columns_ignored_in_lineage(self):
        code_lines = [
            '_sort_out = df_in.withColumn("_sort_ci_customer_id", lower(col("customer_id").cast("string")))',
            'df_out = _sort_out.orderBy(col("_sort_ci_customer_id")).drop("_sort_ci_customer_id")',
        ]
        errors, warnings = validate_column_lineage(
            code_lines, {"customer_id", "name"}, "SortRows"
        )
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])
        joined = " ".join(warnings)
        self.assertNotIn("_sort_ci_customer_id", joined)


class TestGroupByEmptyInput(unittest.TestCase):
    def test_global_agg_skips_unnecessary_union(self):
        lines = convert_group_by_step(
            {
                "give_back_row": True,
                "group_keys": [],
                "aggregates": [
                    {"name": "total", "subject": "amt", "aggregate": "SUM"},
                ],
            },
            "df_in",
            "df_out",
            "GB",
        )
        code = "\n".join(lines)
        self.assertIn("groupBy()", code)
        self.assertNotIn("_gb_default =", code)
        self.assertNotIn("unionByName", code)
        self.assertIn("give_back_row", code)

    def test_grouped_agg_keeps_empty_default_union(self):
        lines = convert_group_by_step(
            {
                "give_back_row": True,
                "group_keys": ["dept"],
                "aggregates": [
                    {"name": "total", "subject": "amt", "aggregate": "SUM"},
                ],
            },
            "df_in",
            "df_out",
            "GB",
        )
        code = "\n".join(lines)
        self.assertIn("_gb_default", code)
        self.assertIn("unionByName", code)


class TestDecimalCalculatorOutput(unittest.TestCase):
    def test_bignumber_output_uses_decimal(self):
        calc = CalculationSpec(
            field_name="line_total",
            calc_type="MULTIPLY",
            field_a="qty",
            field_b="price",
            value_type="BigNumber",
            value_length="18",
            value_precision="4",
        )
        result = convert_calculation_result(calc)
        self.assertIn("decimal(18,4)", result.expr)
        self.assertNotIn('cast("double")', result.expr.replace("'", '"'))
        self.assertNotIn("cast('double')", result.expr)

    def test_number_preserves_decimal_when_operands_are_decimal(self):
        calc = CalculationSpec(
            field_name="line_total",
            calc_type="MULTIPLY",
            field_a="qty",
            field_b="price",
            value_type="Number",
        )
        result = convert_calculation_result(
            calc,
            operand_types={"qty": "BigNumber", "price": "BigNumber"},
        )
        self.assertIn("decimal(", result.expr)
        self.assertNotIn("cast('double')", result.expr)


class TestValueMapperDefaultMapping(unittest.TestCase):
    def test_default_does_not_overwrite_mapped_values(self):
        lines, status = convert_value_mapper_step(
            {
                "field_to_use": "region",
                "target_field": "region_label",
                "non_match_default": "Unknown",
                "mappings": [
                    {"source": "N", "target": "North"},
                    {"source": "", "target": "Empty"},
                ],
            },
            "df_in",
            "df_out",
            "VM",
        )
        code = "\n".join(lines)
        self.assertEqual(status, "converted")
        self.assertIn("lit('North')", code)
        self.assertIn("otherwise(lit('Unknown'))", code)
        self.assertLess(code.index("lit('North')"), code.index("otherwise(lit('Unknown'))"))
        # Must not blank every non-null into Unknown before mapping.
        self.assertNotRegex(
            code,
            r"when\(col\(\"region\"\)\.isNotNull\(\),\s*lit\('Unknown'\)\)",
        )


class TestReadAfterWriteStagingPaths(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_windows_staging_path_matches_write_and_read(self):
        raw = r"C:\etl\customers_staging"
        write_path = normalize_text_file_basename(raw, "csv")
        write_expr = spark_text_file_path_expr(write_path)
        read_expr = spark_text_file_path_expr(
            normalize_text_file_basename(raw + ".csv", "csv")
        )
        self.assertEqual(write_expr, read_expr)
        self.assertIn("customers_staging.csv", write_expr)

        out_xml = """
        <step>
          <filename>C:\\etl\\customers_staging</filename>
          <extension>csv</extension>
          <separator>,</separator>
          <encoding>UTF-8</encoding>
          <header>Y</header>
        </step>
        """
        in_xml = """
        <step>
          <filename>C:\\etl\\customers_staging</filename>
          <extension>csv</extension>
          <separator>,</separator>
          <encoding>UTF-8</encoding>
          <header>Y</header>
          <fields><field><name>id</name><type>Integer</type></field></fields>
        </step>
        """
        out_outcome = self.registry.convert_step(
            "TextFileOutput",
            _ctx(out_xml, "TextFileOutput", "WriteStaging", with_input=True),
        )
        in_outcome = self.registry.convert_step(
            "TextFileInput",
            _ctx(in_xml, "TextFileInput", "ReadStaging"),
        )
        out_code = "\n".join(out_outcome.code_lines)
        in_code = "\n".join(in_outcome.code_lines)
        self.assertIn("customers_staging.csv", out_code)
        self.assertIn("customers_staging.csv", in_code)
        self.assertIn("Spark CSV", out_code)
        self.assertIn("Spark CSV outputs are directories", in_code)
        self.assertEqual(out_outcome.status, "converted")
        self.assertEqual(in_outcome.status, "converted")


class TestValidationScoringLevels(unittest.TestCase):
    def test_infos_do_not_reduce_score(self):
        result = SemanticValidationResult(
            infos=["preserved locale metadata", "field formatting comment"],
            warnings=[],
            errors=[],
            syntax_valid=True,
        )
        result.score = _score(result)
        self.assertEqual(result.score, 1.0)

    def test_warnings_reduce_score_errors_more(self):
        warned = SemanticValidationResult(warnings=["semantic gap"], syntax_valid=True)
        erred = SemanticValidationResult(errors=["missing write"], syntax_valid=True)
        self.assertEqual(_score(warned), 0.95)
        self.assertLess(_score(erred), 0.5)

    def test_textfile_input_validator_locale_is_info(self):
        validator = TextFileInputValidator()
        code_lines = [
            "# Pentaho step: TFI (type: TextFileInput)",
            "# INFO: preserved Legacy Text File Input option: date_format_locale='en_US'",
            "# INFO: preserved.field_format name='amount' options={'currency': '$'}",
            "# NOTE: Spark CSV outputs are directories — load the same path written by Text File Output",
            "df_out = (",
            "    spark.read",
            '    .option("header", True)',
            '    .option("sep", ",")',
            '    .option("encoding", "UTF-8")',
            '    .option("inferSchema", False)',
            "    .schema('amount DOUBLE')",
            '"    .csv(f\'{PENTAHO_DATA_DIR}/customers_staging.csv\')"',
            ")",
        ]
        # Fix the odd quoting above for a clean fragment
        code_lines[-2] = "    .csv(f'{PENTAHO_DATA_DIR}/customers_staging.csv')"
        ctx = _ctx(
            "<step><filename>x.csv</filename><separator>,</separator></step>",
            "TextFileInput",
            "TFI",
        )
        result = validator.validate(ctx, {"filename": "x.csv", "separator": ","}, code_lines)
        self.assertTrue(result.infos)
        self.assertGreaterEqual(result.score, 0.95)
        self.assertNotIn("legacy_unsupported_options", result.properties_missing)

    def test_textfile_output_validator_split_every_is_info(self):
        validator = TextFileOutputValidator()
        code_lines = [
            "# INFO: preserved split_every='5000'",
            "# INFO: preserved.field_format name='amount' options={'precision': '2'}",
            "df_out = df_in",
            'writer = df_out.write.format("csv")',
            "writer = writer.option(\"header\", True)",
            "writer = writer.option(\"sep\", ',')",
            "writer = writer.option(\"encoding\", 'UTF-8')",
            "writer.mode('overwrite').save(f'{PENTAHO_DATA_DIR}/customers_staging.csv')",
        ]
        ctx = _ctx(
            "<step><filename>x.csv</filename><separator>,</separator></step>",
            "TextFileOutput",
            "TFO",
            with_input=True,
        )
        result = validator.validate(
            ctx,
            {"filename": "x.csv", "separator": ",", "header": True, "encoding": "UTF-8"},
            code_lines,
        )
        self.assertTrue(_syntax_ok(code_lines))
        self.assertGreaterEqual(result.score, 0.95)
        self.assertTrue(any("split_every" in i.lower() or "formatting" in i.lower() for i in result.infos))

    def test_display_status_labels(self):
        self.assertEqual(
            format_display_status("converted", infos=["locale"]),
            "CONVERTED (Legacy metadata preserved)",
        )
        self.assertEqual(
            format_display_status("converted", warnings=["fixed-width"]),
            "CONVERTED WITH WARNINGS",
        )
        self.assertEqual(format_display_status("partial"), "PARTIAL")


class TestLoggingImportDoesNotShadowStepFunctions(unittest.TestCase):
    """Regression: nested ``import logging`` caused UnboundLocalError on step entry."""

    def test_strip_helper_and_inlined_tfo_keeps_logging_global(self):
        import symtable
        from pentaho_converter.code_generator import (
            PySparkCodeGenerator,
            _strip_redundant_logging_import,
        )
        from pentaho_converter.generation_config import GenerationConfig
        from pentaho_converter.models import ConversionStats
        from pentaho_converter.validation.code_checks import validate_python_fragment

        self.assertEqual(
            _strip_redundant_logging_import(
                ["import logging", "logging.info('x')", "  import logging"]
            ),
            ["logging.info('x')"],
        )

        ok, errors = validate_python_fragment(
            ["out_df = in_df", "logging.info('wrote %s', 'x')"]
        )
        self.assertTrue(ok, errors)

        out_xml = """
        <step>
          <name>Write Active</name>
          <type>TextFileOutput</type>
          <filename>customers_active_out</filename>
          <extension>csv</extension>
          <separator>,</separator>
          <header>Y</header>
          <encoding>UTF-8</encoding>
          <fields><field><name>customer_id</name></field></fields>
        </step>
        """
        step_el = ET.fromstring(textwrap.dedent(out_xml).strip())
        step = PentahoStep(
            name="Write Active",
            step_type="TextFileOutput",
            attributes={},
            raw_element=step_el,
        )
        upstream = PentahoStep(
            name="Upstream", step_type="RowGenerator", attributes={}, raw_element=None
        )
        trans = PentahoTransformation(
            name="tr_tfo", file_path=Path("tr_tfo.ktr")
        )
        trans.steps = [upstream, step]
        trans.hops = [PentahoHop(from_name="Upstream", to_name="Write Active")]

        gen = PySparkCodeGenerator(generation_config=GenerationConfig.defaults())
        original = gen.registry.convert_step

        def convert_step(step_type, ctx):
            result = original(step_type, ctx)
            if ctx.step.name == "Write Active":
                # Historical bug: handler emitted import logging inside the step body.
                result.code_lines = ["import logging", *list(result.code_lines)]
            return result

        gen.registry.convert_step = convert_step  # type: ignore[method-assign]
        lines, _, _ = gen.generate_inlined_transformation_block(
            trans, ConversionStats(), [], run_func_name="run_tr_tfo"
        )
        text = "\n".join(lines)
        self.assertNotRegex(text, r"(?m)^\s+import logging\s*$")

        st = symtable.symtable("import logging\n" + text, "<tfo>", "exec")
        found = False
        for child in st.get_children():
            if "Write_Active" in child.get_name():
                sym = child.lookup("logging")
                self.assertTrue(sym.is_global())
                self.assertFalse(sym.is_local())
                found = True
                break
        self.assertTrue(found)


if __name__ == "__main__":
    unittest.main()
