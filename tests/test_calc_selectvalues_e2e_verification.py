"""End-to-end verification of Calculator and Select Values fixes.

Converts the sample KTR variants under tests/samples/ and asserts:
- every Calculator step generates executable PySpark
- no Calculator produces "_calculator_unresolved"
- every Select Values variant is parsed correctly
- no Select Values step reports "[failed]"
- downstream DataFrames are never broken
- validation reports unsupported features instead of generating invalid code
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

from pentaho_converter.code_generator import PySparkCodeGenerator
from pentaho_converter.models import ConversionStats
from pentaho_converter.step_xml import parse_calculations, parse_select_values_config
from pentaho_converter.transformation_parser import parse_transformation

SAMPLES = Path(__file__).resolve().parent / "samples"

KTR_FILES = [
    SAMPLES / "Calculator_Variants.ktr",
    SAMPLES / "SelectValues_Variants.ktr",
    SAMPLES / "Calc_SelectValues_Chain.ktr",
]


def _compile_ok(code: str) -> tuple[bool, str]:
    try:
        ast.parse(code)
        return True, ""
    except SyntaxError as exc:
        return False, str(exc)


def _df_assignments(code: str) -> dict[str, list[str]]:
    """Map df_* names to RHS expressions that assign them."""
    assigned: dict[str, list[str]] = {}
    for line in code.splitlines():
        stripped = line.strip()
        m = re.match(r"(df_\w+)\s*=\s*(.+)$", stripped)
        if m:
            assigned.setdefault(m.group(1), []).append(m.group(2))
    return assigned


def _used_df_refs(code: str) -> set[str]:
    return set(re.findall(r"\b(df_\w+)\b", code))


class TestCalcSelectValuesE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.results: dict[str, dict] = {}
        generator = PySparkCodeGenerator()
        for path in KTR_FILES:
            self_assert = cls()
            self_assert.assertTrue(path.exists(), f"missing fixture {path}")
            trans = parse_transformation(path)
            stats = ConversionStats()
            logs: list[str] = []
            code = generator.generate_transformation(trans, stats, logs)
            cls.results[path.name] = {
                "path": path,
                "trans": trans,
                "stats": stats,
                "logs": logs,
                "code": code,
            }

    def test_fixtures_exist_and_parse(self):
        for name, bundle in self.results.items():
            steps = bundle["trans"].steps
            self.assertGreater(len(steps), 0, name)
            self.assertTrue(bundle["code"].strip(), name)

    def test_generated_pyspark_is_executable_syntax(self):
        for name, bundle in self.results.items():
            ok, err = _compile_ok(bundle["code"])
            self.assertTrue(ok, f"{name} syntax error: {err}\n{bundle['code'][:500]}")

    def test_no_calculator_unresolved_placeholder(self):
        for name, bundle in self.results.items():
            code = bundle["code"]
            self.assertNotIn("_calculator_unresolved", code, name)
            # Calculator must not invent an empty DF when upstream exists
            for result in bundle["stats"].step_results:
                if result.step_type.strip().lower() != "calculator":
                    continue
                self.assertNotIn(
                    "_calculator_unresolved",
                    "\n".join(
                        line
                        for line in code.splitlines()
                        if result.step_name.replace(" ", "_") in line
                        or f"Calculator: {result.step_name}" in line
                    ),
                    f"{name} / {result.step_name}",
                )

    def test_every_calculator_generates_df_assignment(self):
        for name, bundle in self.results.items():
            code = bundle["code"]
            assigned = _df_assignments(code)
            for step in bundle["trans"].steps:
                if step.step_type.strip().lower() != "calculator":
                    continue
                safe = step.name.replace(" ", "_").replace("-", "_")
                df_name = f"df_{safe}"
                self.assertIn(df_name, assigned, f"{name}: missing assignment for {df_name}")
                # Must not be empty unresolved createDataFrame
                rhs_joined = " | ".join(assigned[df_name])
                self.assertNotIn("_calculator_unresolved", rhs_joined, name)
                # First assignment should reference upstream or withColumn chain start
                self.assertTrue(
                    any(
                        "withColumn" in rhs or "df_" in rhs or "spark.range" in rhs
                        for rhs in assigned[df_name]
                    ),
                    f"{name}/{step.name}: unexpected RHS {assigned[df_name]}",
                )

    def test_no_select_values_failed_status(self):
        for name, bundle in self.results.items():
            code = bundle["code"]
            for result in bundle["stats"].step_results:
                if result.step_type.strip().lower().replace(" ", "") != "selectvalues":
                    continue
                self.assertNotEqual(
                    result.status,
                    "failed",
                    f"{name}/{result.step_name} failed: {result.errors}",
                )
                marker = f"# Step: {result.step_name} ({result.step_type}) [failed]"
                self.assertNotIn(marker, code, name)

    def test_select_values_variants_parsed_correctly(self):
        bundle = self.results["SelectValues_Variants.ktr"]
        by_name = {s.name: s for s in bundle["trans"].steps}

        cfg_rename = parse_select_values_config(by_name["SV select rename"].raw_element)
        self.assertEqual(
            [f["name"] for f in cfg_rename["select_fields"]],
            [
                "customer_id",
                "customer_name",
                "amount",
                "status",
                "tmp_col",
                "debug",
                "as_of",
            ],
        )
        self.assertEqual(cfg_rename["select_fields"][0]["rename"], "id")

        cfg_remove = parse_select_values_config(by_name["SV remove under fields"].raw_element)
        self.assertEqual(set(cfg_remove["remove_names"]), {"tmp_col", "debug"})

        cfg_meta = parse_select_values_config(by_name["SV meta only"].raw_element)
        self.assertEqual(len(cfg_meta["meta_changes"]), 2)
        self.assertEqual(cfg_meta["meta_changes"][0]["type_name"], "Integer")
        self.assertEqual(cfg_meta["meta_changes"][1]["conversion_mask"], "yyyy-MM-dd")

        cfg_step = parse_select_values_config(by_name["SV remove step level"].raw_element)
        self.assertIn("status", cfg_step["remove_names"])
        self.assertEqual(cfg_step["select_columns"], ["id", "name", "amount", "as_of"])

        cfg_unspec = parse_select_values_config(by_name["SV select unspecified"].raw_element)
        self.assertTrue(cfg_unspec["select_unspecified"])

        cfg_full = parse_select_values_config(by_name["SV full meta"].raw_element)
        meta = cfg_full["meta_changes"][0]
        self.assertEqual(meta["type_name"], "Number")
        self.assertEqual(meta["currency_symbol"], "$")
        self.assertEqual(meta["encoding"], "UTF-8")
        self.assertEqual(meta["storage_type"], "normal")

        code = bundle["code"]
        self.assertIn('.alias("id")', code)
        self.assertIn("to_date(", code)
        self.assertIn("preserved.meta", code)
        self.assertIn("select_unspecified", code)

    def test_calculator_variants_parsed_correctly(self):
        bundle = self.results["Calculator_Variants.ktr"]
        by_name = {s.name: s for s in bundle["trans"].steps}

        short = parse_calculations(by_name["Calc short names"].raw_element)
        self.assertEqual([c.calc_type for c in short], ["MULTIPLY", "UPPER_CASE"])

        numeric = parse_calculations(by_name["Calc numeric IDs"].raw_element)
        self.assertEqual(numeric[0].calc_type, "ADD")
        self.assertEqual(numeric[1].calc_type, "STRING_LEN")

        long_desc = parse_calculations(by_name["Calc long desc"].raw_element)
        self.assertEqual(long_desc[0].calc_type, "PERCENT_1")
        self.assertEqual(long_desc[1].calc_type, "SUBTRACT")

        multi = parse_calculations(by_name["Calc multi ops"].raw_element)
        self.assertEqual(len(multi), 3)
        self.assertTrue(multi[2].remove)

        empty = parse_calculations(by_name["Calc empty"].raw_element)
        self.assertEqual(empty, [])

        code = bundle["code"]
        self.assertIn("lit(100)", code)
        self.assertIn("date_add", code)
        self.assertIn("upper(", code)
        # Empty calculator preserves upstream DF
        self.assertIn("preserving upstream DataFrame", code)
        self.assertNotIn("_calculator_unresolved", code)

    def test_unsupported_reported_not_invalid_empty_df(self):
        bundle = self.results["Calculator_Variants.ktr"]
        unsup = next(
            r for r in bundle["stats"].step_results if r.step_name == "Calc unsupported"
        )
        # Must generate code (not abort) and report via warning/partial, not empty DF
        self.assertNotEqual(unsup.status, "failed", unsup.errors)
        self.assertTrue(
            unsup.status in ("converted", "partial", "partially_supported")
            or any("not supported" in w.lower() for w in unsup.warnings)
            or any("not supported" in e.lower() for e in unsup.errors)
            or "WARNING" in bundle["code"],
            f"unsupported calc not reported: status={unsup.status} warn={unsup.warnings}",
        )
        self.assertIn("JARO", "\n".join(unsup.warnings + unsup.errors + [bundle["code"]]))

        empty = next(r for r in bundle["stats"].step_results if r.step_name == "Calc empty")
        self.assertEqual(empty.status, "partial")
        self.assertTrue(
            any("metadata" in e.lower() for e in empty.errors)
            or any("metadata" in w.lower() for w in empty.warnings)
        )
        # Still assigned from upstream
        self.assertIn("df_Calc_empty = df_Calc_unsupported", bundle["code"])

    def test_downstream_dataframes_never_broken(self):
        for name, bundle in self.results.items():
            code = bundle["code"]
            assigned = _df_assignments(code)
            # Every df_* used on RHS (except spark helpers) should be assigned earlier
            lines = [ln.strip() for ln in code.splitlines() if ln.strip()]
            seen: set[str] = set()
            for line in lines:
                m = re.match(r"(df_\w+)\s*=\s*(.+)$", line)
                if not m:
                    continue
                lhs, rhs = m.group(1), m.group(2)
                for ref in re.findall(r"\b(df_\w+)\b", rhs):
                    if ref == lhs:
                        continue
                    self.assertIn(
                        ref,
                        seen,
                        f"{name}: {lhs} references {ref} before it is assigned\n{line}",
                    )
                seen.add(lhs)

            # No empty code_lines for Calculator / SelectValues
            for result in bundle["stats"].step_results:
                st = result.step_type.strip().lower().replace(" ", "")
                if st not in ("calculator", "selectvalues"):
                    continue
                self.assertTrue(
                    result.status != "failed" or result.errors,
                    f"{name}/{result.step_name}: failed without diagnostics",
                )
                # failed would have been aborted with empty lines — ensure status is not failed
                self.assertNotEqual(result.status, "failed", f"{name}/{result.step_name}: {result.errors}")

    def test_chain_continuity_calc_to_select_to_calc(self):
        bundle = self.results["Calc_SelectValues_Chain.ktr"]
        code = bundle["code"]
        # Calc → Select → Calc all present and linked
        self.assertIn("df_Calc_amounts = df_Generate_Rows", code)
        self.assertIn("df_Select_values = df_Calc_amounts", code)
        self.assertIn("df_Calc_final = df_Select_values", code)
        self.assertIn('withColumn("gross"', code)
        self.assertIn('withColumn("unit_net"', code)
        self.assertIn(".select(", code)
        self.assertNotIn("[failed]", code)
        self.assertNotIn("_calculator_unresolved", code)
        self.assertNotIn("createDataFrame([], '_placeholder", code)

        statuses = {
            r.step_name: r.status
            for r in bundle["stats"].step_results
            if r.step_type in ("Calculator", "SelectValues")
        }
        for step_name, status in statuses.items():
            self.assertIn(
                status,
                ("converted", "partial", "partially_supported"),
                f"{step_name}={status}",
            )

    def test_step_comment_markers_never_failed_for_targets(self):
        for name, bundle in self.results.items():
            for line in bundle["code"].splitlines():
                if "(Calculator)" in line or "(SelectValues)" in line:
                    self.assertNotIn(
                        "[failed]",
                        line,
                        f"{name}: failed marker in {line!r}",
                    )

    def test_rename_onto_dropped_upstream_column_is_allowed(self):
        """SelectValues may rename onto an upstream name that the select list drops."""
        bundle = self.results["Calc_SelectValues_Chain.ktr"]
        sv = next(r for r in bundle["stats"].step_results if r.step_name == "Select values")
        self.assertNotEqual(sv.status, "failed", sv.errors)
        self.assertIn('.alias("sku")', bundle["code"])
        # Collision should be a warning (projection), not a hard abort.
        self.assertTrue(
            any("reuses upstream column name via projection" in w for w in sv.warnings)
            or "sku_upper" in bundle["code"],
            sv.warnings,
        )


if __name__ == "__main__":
    unittest.main()
