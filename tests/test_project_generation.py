"""Tests for multi-file Databricks project generation."""

from __future__ import annotations

import ast
import io
import unittest
import zipfile
from pathlib import Path

from pentaho_converter.pipeline import convert_pentaho_project

SAMPLES = Path(__file__).resolve().parents[1] / "samples"


class TestProjectGeneration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not SAMPLES.exists():
            raise unittest.SkipTest("samples/ not present")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for p in SAMPLES.rglob("*"):
                if p.is_file():
                    zf.write(p, p.relative_to(SAMPLES).as_posix())
        cls.result = convert_pentaho_project(buf.getvalue(), "Retail_ETL")

    def test_emits_flat_package_layout(self):
        files = self.result.files
        self.assertIn("Retail_ETL/Master_ETL.py", files)
        self.assertIn("Retail_ETL/config.py", files)
        self.assertIn("Retail_ETL/requirements.txt", files)
        self.assertIn("Retail_ETL/jobs/__init__.py", files)
        self.assertIn("Retail_ETL/VALIDATION_REPORT.md", files)
        self.assertIn("Retail_ETL/engine/__init__.py", files)
        self.assertIn("Retail_ETL/engine/runtime.py", files)
        self.assertIn("Retail_ETL/engine/handlers.py", files)
        self.assertIn("Retail_ETL/engine/job_runtime.py", files)
        self.assertIn("Retail_ETL/engine/job_models.py", files)
        self.assertIn("Retail_ETL/engine/variables.py", files)
        self.assertIn("Retail_ETL/engine/job_specs.py", files)
        # Must NOT generate transformations/ or utils/
        for path in files:
            norm = path.replace("\\", "/")
            self.assertNotIn("/transformations/", norm)
            self.assertNotIn("/utils/", norm)

    def test_job_inlines_transformations(self):
        files = self.result.files
        self.assertIn("Retail_ETL/jobs/Master.py", files)
        master_job = files["Retail_ETL/jobs/Master.py"]
        self.assertIn("def run(", master_job)
        self.assertIn("from engine.runtime import execute_registered_job", master_job)
        self.assertIn("execute_registered_job(", master_job)
        self.assertNotIn("class JobRuntime", master_job)
        self.assertNotIn("def handle_special(", master_job)
        self.assertIn('"""Run Customer_Load."""', master_job)
        self.assertIn('"""Run Sales_Load."""', master_job)
        self.assertIn("def run_Customer_Load(", master_job)
        self.assertIn("def run_Sales_Load(", master_job)
        self.assertIn("def step_01_", master_job)
        for metadata_name in (
            "JOB_PARAMETERS",
            "ENTRY_DEFS",
            "HOP_DEFS",
            "TRANS_RUNNERS",
            "CHILD_JOB_MODULES",
            "CONVERSION_TODOS",
        ):
            self.assertNotIn(metadata_name, master_job)
        specs = files["Retail_ETL/engine/job_specs.py"]
        self.assertIn("JOB_SPECS", specs)
        self.assertIn("'entries'", specs)
        self.assertIn("'hops'", specs)
        # No separate transformation modules
        self.assertNotIn("Retail_ETL/transformations/Customer_Load.py", files)
        self.assertNotIn("Retail_ETL/transformations/Sales_Load.py", files)

    def test_job_modules_exclude_pentaho_runtime_metadata(self):
        forbidden = (
            "JOB_PARAMETERS",
            "ENTRY_DEFS",
            "HOP_DEFS",
            "TRANS_RUNNERS",
            "CHILD_JOB_MODULES",
            "CONVERSION_TODOS",
        )
        for path, content in self.result.files.items():
            if path.startswith("Retail_ETL/jobs/") and path.endswith(".py"):
                with self.subTest(path=path):
                    for metadata_name in forbidden:
                        self.assertNotIn(metadata_name, content)

    def test_engine_has_runtime_classes(self):
        runtime = self.result.files["Retail_ETL/engine/job_runtime.py"]
        self.assertIn("class JobRuntime", runtime)
        handlers = self.result.files["Retail_ETL/engine/handlers.py"]
        self.assertIn("def handle_special", handlers)
        self.assertIn("def build_handlers", handlers)
        models = self.result.files["Retail_ETL/engine/job_models.py"]
        self.assertIn("class JobEntry", models)
        self.assertIn("class JobHop", models)

    def test_master_entrypoint_calls_primary_job(self):
        master = self.result.files["Retail_ETL/Master_ETL.py"]
        self.assertIn("from jobs.Master import run", master)
        self.assertIn("def run(", master)
        self.assertIn("import logging", master)

    def test_all_python_files_parse(self):
        for path, content in self.result.files.items():
            if path.endswith(".py"):
                with self.subTest(path=path):
                    ast.parse(content)

    def test_main_workflow_points_at_master(self):
        self.assertEqual(self.result.main_workflow, "Retail_ETL/Master_ETL.py")

    def test_validation_accepts_engine_owned_job_metadata(self):
        report = self.result.files["Retail_ETL/VALIDATION_REPORT.md"]
        self.assertIn("**Status:** PASSED", report)

    def test_config_has_helpers_no_utils(self):
        config = self.result.files["Retail_ETL/config.py"]
        self.assertIn("def merge_config", config)
        self.assertIn("def resolve_data_path", config)
        self.assertIn("def apply_spark_runtime_hints", config)


if __name__ == "__main__":
    unittest.main()
