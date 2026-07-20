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

    def test_config_loaded_once_not_in_every_step(self):
        """Config vars are initialized in run_*; pure transform steps omit them."""
        import re

        master_job = self.result.files["Retail_ETL/jobs/Master.py"]
        # Orchestrator loads parameters once
        run_match = re.search(
            r"def run_Customer_Load\(spark, config=None\):(.*?)(?=\ndef |\Z)",
            master_job,
            flags=re.S,
        )
        self.assertIsNotNone(run_match)
        self.assertIn("BATCH_DATE = config.get(", run_match.group(1))
        # No per-step config.get duplication
        step_bodies = re.findall(
            r"^def (step_\d+\w+)\(([^)]*)\):\n(.*?)(?=^def |\Z)",
            master_job,
            flags=re.M | re.S,
        )
        self.assertGreater(len(step_bodies), 0)
        for name, signature, body in step_bodies:
            with self.subTest(step=name):
                self.assertNotIn(
                    "config.get(",
                    body,
                    msg=f"{name} must not re-initialize config",
                )
                # Pure transforms in the sample should not take config at all
                if any(
                    token in name.lower()
                    for token in (
                        "filter",
                        "sort",
                        "group_by",
                        "select_values",
                    )
                ):
                    params = [p.strip() for p in signature.split(",") if p.strip()]
                    self.assertNotIn("config", params)
                    self.assertNotIn("BATCH_DATE", params)

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
        self.assertIn("_require_modules", master)
        self.assertIn("importlib.util.find_spec", master)
        self.assertIn("except ModuleNotFoundError:", master)
        self.assertIn("IMPORT DIAGNOSTICS", master)

    def test_generated_modules_are_notebook_safe_for_file(self):
        """Databricks notebooks lack ``__file__``; bootstrap must catch NameError."""
        master = self.result.files["Retail_ETL/Master_ETL.py"]
        self.assertIn("def _project_root()", master)
        self.assertIn("except NameError:", master)
        self.assertIn("notebookPath()", master)
        self.assertIn("SparkFiles.getRootDirectory()", master)
        self.assertIn("_is_project_root", master)
        self.assertIn("/Workspace", master)
        self.assertNotIn(
            "_ROOT = Path(__file__).resolve().parent\n",
            master,
        )
        # Must not blindly return cwd without validating project markers
        self.assertNotIn("return Path.cwd()\n", master)

        job = self.result.files["Retail_ETL/jobs/Master.py"]
        self.assertIn("def _project_root()", job)
        self.assertIn("except NameError:", job)
        self.assertNotIn(
            "_ROOT = Path(__file__).resolve().parent.parent\n",
            job,
        )

        # Simulate notebook: evaluating Path(__file__) raises NameError → climb cwd
        ns: dict = {"Path": Path, "sys": __import__("sys")}
        bootstrap = master[
            master.index("def _workspace_path_variants") : master.index("_ROOT = _project_root()")
        ]
        # Provide minimal stubs so notebook/Spark lookups fail closed
        exec(bootstrap + "\n_root_fn = _project_root\n", ns)
        # Without a real project layout this raises ModuleNotFoundError (no cwd assumption)
        with self.assertRaises(ModuleNotFoundError):
            ns["_root_fn"]()

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
        self.assertIn("def ensure_data_dir", config)
        self.assertIn("CREATE VOLUME IF NOT EXISTS", config)
        # Free Edition / serverless may reject conf keys — must not hard-fail.
        self.assertIn("def _set(key: str, value: str)", config)
        self.assertIn("except Exception:", config)
        self.assertIn('spark.sql.adaptive.enabled', config)
        # Spark ETL data must use a UC Volume path, not /Workspace
        self.assertIn("/Volumes/", config)
        self.assertNotIn("PENTAHO_DATA_DIR = '/Workspace/", config)

    def test_apply_spark_runtime_hints_tolerates_config_not_available(self):
        """Hints apply on paid; rejected keys are skipped on Free Edition."""
        config_src = self.result.files["Retail_ETL/config.py"]
        start = config_src.index("def apply_spark_runtime_hints")
        helper = config_src[start:]
        ns: dict = {}
        stub = (
            "from typing import Any, Mapping\n"
            "TARGET_CATALOG = 'main'\n"
            "TARGET_SCHEMA = 'default'\n"
            "PENTAHO_DATA_DIR = '/tmp'\n"
            "DEFAULT_CONFIG = {}\n"
        )
        exec(compile(stub + helper, "<config>", "exec"), ns)

        class _Conf:
            def __init__(self):
                self.sets = []

            def set(self, key, value):
                self.sets.append((key, value))
                if key == "spark.sql.adaptive.enabled":
                    raise Exception("CONFIG_NOT_AVAILABLE")

        class _Spark:
            def __init__(self):
                self.conf = _Conf()

        spark = _Spark()
        ns["apply_spark_runtime_hints"](spark, {"spark_aqe": True, "spark_shuffle_partitions": 8})
        keys = [k for k, _ in spark.conf.sets]
        self.assertIn("spark.sql.adaptive.enabled", keys)
        self.assertIn("spark.sql.shuffle.partitions", keys)
        # Did not raise despite CONFIG_NOT_AVAILABLE on AQE
        self.assertGreaterEqual(len(spark.conf.sets), 2)

if __name__ == "__main__":
    unittest.main()
