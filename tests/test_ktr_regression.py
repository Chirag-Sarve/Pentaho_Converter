"""Regression tests: every .ktr in the repository must convert without semantic failures."""

from __future__ import annotations

import io
import unittest
import zipfile
from pathlib import Path

from pentaho_converter.code_generator import PySparkCodeGenerator
from pentaho_converter.models import ConversionStats
from pentaho_converter.pipeline import convert_pentaho_project
from pentaho_converter.steps.base import build_default_registry
from pentaho_converter.transformation_parser import parse_transformation

ROOT = Path(__file__).resolve().parents[1]


class TestKtrRegression(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()
        cls.generator = PySparkCodeGenerator(cls.registry)

    def _convert_ktr(self, path: Path) -> tuple[str, ConversionStats]:
        trans = parse_transformation(path)
        stats = ConversionStats()
        code = self.generator.generate_transformation(trans, stats, [])
        return code, stats

    def test_all_ktr_files_convert_cleanly(self):
        ktr_files = sorted(ROOT.rglob("*.ktr"))
        self.assertGreater(len(ktr_files), 0, "expected at least one .ktr sample")
        for path in ktr_files:
            with self.subTest(ktr=path.relative_to(ROOT).as_posix()):
                code, stats = self._convert_ktr(path)
                self.assertNotIn("_placeholder", code)
                for step in stats.step_results:
                    self.assertEqual(
                        step.status,
                        "converted",
                        f"{step.step_name} ({step.step_type}): {step.errors}",
                    )
                    self.assertGreaterEqual(step.semantic_score, 0.95)

    def test_customer_load_return_and_filter(self):
        code, _ = self._convert_ktr(ROOT / "samples/Transformations/Customer_Load.ktr")
        self.assertIn("return df_Table_output", code)
        self.assertIn("df_Table_output = df_Select_values", code)
        self.assertIn('col("status") == lit(\'ACTIVE\')', code)
        self.assertNotIn("'col(\"ACTIVE\")'", code)

    def test_master_zip_project(self):
        buf = io.BytesIO()
        samples = ROOT / "samples"
        with zipfile.ZipFile(buf, "w") as zf:
            for p in samples.rglob("*"):
                if p.is_file():
                    zf.write(p, p.relative_to(samples).as_posix())
        result = convert_pentaho_project(buf.getvalue(), "Master")
        self.assertGreater(result.stats.steps_converted, 0)
        for step in result.stats.step_results:
            self.assertEqual(step.status, "converted", step.step_name)


if __name__ == "__main__":
    unittest.main()
