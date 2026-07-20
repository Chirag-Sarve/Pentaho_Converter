"""Tests for Databricks-portable path resolution and FILE_EXISTS checks."""

from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path

from pentaho_converter.generation_config import GenerationConfig
from pentaho_converter.project_generator import DatabricksProjectGenerator
from pentaho_converter.runtime_templates.engine.handlers import (
    _fs_exists,
    handle_file_exists,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime


class TestResolveDataPath(unittest.TestCase):
    def _resolve_fn(self, data_dir: str):
        gen = DatabricksProjectGenerator(
            generation_config=GenerationConfig(
                catalog="main",
                schema="default",
                data_dir=data_dir,
            )
        )
        src = gen._generate_config("demo")  # noqa: SLF001
        ns: dict = {}
        exec(compile(src, "<config>", "exec"), ns)
        return ns["resolve_data_path"]

    def test_local_output_maps_to_data_dir_basename(self):
        resolve = self._resolve_fn("/Volumes/main/default/pentaho_data")
        resolved = resolve("/output/high_value_customers.csv")
        self.assertEqual(
            resolved,
            "/Volumes/main/default/pentaho_data/high_value_customers.csv",
        )

    def test_volumes_path_unchanged(self):
        resolve = self._resolve_fn("/Volumes/main/default/pentaho_data")
        path = "/Volumes/main/default/pentaho_data/keep.csv"
        self.assertEqual(resolve(path), path)


class TestFileExistsHandler(unittest.TestCase):
    def test_detects_spark_success_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "high_value_customers.csv"
            out.mkdir()
            (out / "_SUCCESS").write_text("", encoding="utf-8")
            self.assertTrue(_fs_exists(str(out)))

    def test_file_exists_uses_resolved_data_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "high_value_customers.csv"
            target.write_text("a,b\n1,2\n", encoding="utf-8")

            cfg_mod = types.ModuleType("config")
            cfg_mod.PENTAHO_DATA_DIR = tmp

            def resolve_data_path(path, cfg=None):
                name = path.rstrip("/").rsplit("/", 1)[-1]
                return f"{tmp}/{name}"

            cfg_mod.resolve_data_path = resolve_data_path
            sys.modules["config"] = cfg_mod
            try:
                runtime = JobRuntime(
                    name="demo",
                    entries=[],
                    hops=[],
                    handlers={},
                )
                runtime.config = {"PENTAHO_DATA_DIR": tmp}
                entry = JobEntry(
                    name="Check Output File Exists",
                    entry_type="FILE_EXISTS",
                    filename="/output/high_value_customers.csv",
                )
                result = handle_file_exists(runtime, entry)
                self.assertTrue(result.success)
                self.assertTrue(result.result)
            finally:
                sys.modules.pop("config", None)


if __name__ == "__main__":
    unittest.main()
