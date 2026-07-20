"""Tests for JobExecutionError traceback preservation and DF guards."""

from __future__ import annotations

import io
import logging
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

from pentaho_converter.pipeline import convert_pentaho_project
from pentaho_converter.runtime_templates.engine.df_guards import (
    require_dataframe,
)
from pentaho_converter.runtime_templates.engine.handlers import make_trans_handler
from pentaho_converter.runtime_templates.engine.job_models import EntryResult, JobEntry, JobHop
from pentaho_converter.runtime_templates.engine.job_runtime import JobExecutionError, JobRuntime


class TestJobFailurePreservesOriginalException(unittest.TestCase):
    def test_no_failure_hop_raises_original_exception(self):
        original = ValueError("Column customer_id missing before Write Active step")

        def boom(_runtime, entry):
            return EntryResult(name=entry.name, success=False, error=original)

        runtime = JobRuntime(
            name="jb_customer_load",
            entries=[
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(name="Run Customer Filter", entry_type="TRANS"),
            ],
            hops=[
                JobHop(from_name="Start", to_name="Run Customer Filter", enabled=True),
                # success-only hop (default) — does not fire on failure
                JobHop(from_name="Run Customer Filter", to_name="Success", enabled=True),
            ],
            handlers={
                "SPECIAL": lambda r, e: EntryResult(name=e.name, success=True),
                "TRANS": boom,
            },
        )

        with self.assertRaises(ValueError) as ctx:
            runtime.run()
        self.assertIs(ctx.exception, original)
        self.assertIn("customer_id", str(ctx.exception))
        self.assertNotIn("no failure/unconditional hop", str(ctx.exception))

    def test_no_failure_hop_without_error_still_raises_job_error(self):
        def soft_fail(_runtime, entry):
            return EntryResult(name=entry.name, success=False, error=None)

        runtime = JobRuntime(
            name="jb_customer_load",
            entries=[
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(name="Run Customer Filter", entry_type="TRANS"),
            ],
            hops=[
                JobHop(from_name="Start", to_name="Run Customer Filter", enabled=True),
            ],
            handlers={
                "SPECIAL": lambda r, e: EntryResult(name=e.name, success=True),
                "TRANS": soft_fail,
            },
        )
        with self.assertRaises(JobExecutionError) as ctx:
            runtime.run()
        self.assertIn("no failure/unconditional hop", str(ctx.exception))


class TestTransHandlerDiagnostics(unittest.TestCase):
    def test_handler_logs_full_traceback_and_context(self):
        def runner(_spark, _cfg):
            def step_04_Write_Active(select_fields_df):
                raise ValueError("Column customer_id missing before Write Active step")

            return step_04_Write_Active(object())

        handler = make_trans_handler(
            spark=MagicMock(),
            cfg={},
            trans_runners={"Run Customer Filter": runner},
        )
        runtime = JobRuntime(
            name="jb_customer_load",
            entries=[],
            hops=[],
            handlers={},
        )
        entry = JobEntry(name="Run Customer Filter", entry_type="TRANS")

        with self.assertLogs(level=logging.ERROR) as captured:
            result = handler(runtime, entry)

        self.assertFalse(result.success)
        self.assertIsInstance(result.error, ValueError)
        joined = "\n".join(captured.output)
        self.assertIn("Full traceback", joined)
        self.assertIn("step_04_Write_Active", joined)
        self.assertIn("Column customer_id missing", joined)


class TestRequireDataframe(unittest.TestCase):
    def test_none_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            require_dataframe(
                None,
                transformation="tr_customer_filter",
                step_name="Write Active",
                func_name="step_04_Write_Active",
            )
        self.assertIn("DataFrame is None before Write Active step", str(ctx.exception))

    def test_missing_column_raises_value_error(self):
        df = MagicMock()
        df.columns = ["customer_name", "region"]
        with self.assertRaises(ValueError) as ctx:
            require_dataframe(
                df,
                transformation="tr_customer_filter",
                step_name="Write Active",
                func_name="step_04_Write_Active",
                required_columns=["customer_id", "customer_name", "region"],
            )
        self.assertIn("Column customer_id missing before Write Active step", str(ctx.exception))


class TestGeneratedCustomerLoadGuards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1] / "_tmp_test101_extract"
        if not root.exists():
            raise unittest.SkipTest("_tmp_test101_extract not present")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for path in root.rglob("*"):
                if path.is_file():
                    zf.write(path, path.relative_to(root).as_posix())
        cls.result = convert_pentaho_project(buf.getvalue(), "test101")

    def test_jb_customer_load_emits_guards_and_logging(self):
        job = self.result.files["test101/jobs/jb_customer_load.py"]
        self.assertIn("from engine.df_guards import log_step_dataframe, require_dataframe", job)
        self.assertIn("def step_04_Write_Active(", job)
        self.assertIn("def run_tr_customer_filter(", job)
        self.assertIn("require_dataframe(", job)
        self.assertIn("log_step_dataframe(", job)
        self.assertIn("_tfo_missing", job)
        self.assertIn("missing before Write Active step", job)
        self.assertIn("_filter_missing", job)
        self.assertIn("missing before Keep Active step", job)

    def test_engine_copies_df_guards_and_preserves_original(self):
        runtime = self.result.files["test101/engine/job_runtime.py"]
        self.assertIn("raise result.error", runtime)
        self.assertNotRegex(
            runtime,
            r"raise JobExecutionError\(\s*f\"Job '\{self\.name\}' failed at entry '\{name\}' \"\s*"
            r"f\"with no failure/unconditional hop\"\s*\) from result\.error",
        )
        handlers = self.result.files["test101/engine/handlers.py"]
        self.assertIn("log_exception_diagnostics", handlers)
        self.assertIn("test101/engine/df_guards.py", self.result.files)


if __name__ == "__main__":
    unittest.main()
