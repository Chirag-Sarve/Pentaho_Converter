"""Tests for upload-only Databricks Free Edition deployment helpers."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from databricks.databricks_client import (
    DeployResult,
    deploy_project,
    mkdirs,
    test_connection,
    to_notebook_source,
    workspace_object_path,
)


class TestConnectionFreeEdition(unittest.TestCase):
    def test_success_message(self):
        with patch("databricks.databricks_client._request", return_value=(200, {"userName": "a@b.com"})):
            result = test_connection("https://adb.example.com", "dapi123")
        self.assertTrue(result.ok)
        self.assertEqual(result.message, "Connected successfully.")

    def test_requires_host_and_token(self):
        self.assertFalse(test_connection("", "tok").ok)
        self.assertFalse(test_connection("https://adb.example.com", "").ok)

    def test_falls_back_to_workspace_list(self):
        responses = [
            (404, {"error": "missing"}),
            (200, {"objects": []}),
        ]

        def fake_request(host, token, path, payload=None, method="GET"):
            return responses.pop(0)

        with patch("databricks.databricks_client._request", side_effect=fake_request):
            result = test_connection("https://adb.example.com", "dapi123")
        self.assertTrue(result.ok)
        self.assertEqual(result.message, "Connected successfully.")

    def test_auth_failure(self):
        with patch("databricks.databricks_client._request", return_value=(401, {"error": "bad"})):
            result = test_connection("https://adb.example.com", "bad")
        self.assertFalse(result.ok)
        self.assertIn("Authentication failed", result.message)


class TestWorkspacePaths(unittest.TestCase):
    def test_notebook_path_strips_py(self):
        path = workspace_object_path(
            "/Workspace/Pentaho_Migration/Retail_ETL",
            "Master_ETL.py",
            as_file=False,
        )
        self.assertEqual(path, "/Workspace/Pentaho_Migration/Retail_ETL/Master_ETL")

    def test_file_path_keeps_py(self):
        path = workspace_object_path(
            "/Workspace/Pentaho_Migration/Retail_ETL",
            "jobs/daily/load_sales.py",
            as_file=True,
        )
        self.assertEqual(
            path,
            "/Workspace/Pentaho_Migration/Retail_ETL/jobs/daily/load_sales.py",
        )

    def test_mkdirs_posts_path(self):
        with patch("databricks.databricks_client._request", return_value=(200, {})) as mock_req:
            result = mkdirs("https://adb.example.com", "tok", "/Workspace/Pentaho_Migration")
        self.assertTrue(result.ok)
        mock_req.assert_called_once()
        args = mock_req.call_args
        self.assertEqual(args[0][2], "/api/2.0/workspace/mkdirs")
        self.assertEqual(args[1]["payload"]["path"], "/Workspace/Pentaho_Migration")


class TestDeployProject(unittest.TestCase):
    def test_master_as_notebook_modules_as_files(self):
        calls: list[tuple] = []

        def fake_request(host, token, path, payload=None, method="GET"):
            calls.append((path, method, payload))
            return 200, {}

        files = {
            "Retail_ETL/Master_ETL.py": "def run_workflow(spark):\n    return None\n",
            "Retail_ETL/config.py": "CATALOG = 'main'\n",
            "Retail_ETL/jobs/load.py": "def run_load(spark):\n    return None\n",
            "Retail_ETL/README.md": "# Project\n",
        }
        with patch("databricks.databricks_client._request", side_effect=fake_request):
            result = deploy_project(
                "https://adb.example.com",
                "tok",
                files,
                base_dir="/Workspace/Pentaho_Migration",
                project_name="Retail_ETL",
            )

        self.assertTrue(result.ok)
        self.assertEqual(result.location, "/Workspace/Pentaho_Migration/Retail_ETL")
        self.assertEqual(result.uploaded_count, 4)
        self.assertEqual(result.failed_count, 0)
        # No nested Retail_ETL/Retail_ETL
        self.assertTrue(all("/Retail_ETL/Retail_ETL/" not in p for p in result.uploaded))

        import_calls = [c for c in calls if c[0] == "/api/2.0/workspace/import"]
        self.assertEqual(len(import_calls), 4)

        by_path = {c[2]["path"]: c[2] for c in import_calls}
        # Entry notebook: SOURCE + PYTHON, no .py extension
        master = by_path["/Workspace/Pentaho_Migration/Retail_ETL/Master_ETL"]
        self.assertEqual(master["format"], "SOURCE")
        self.assertEqual(master["language"], "PYTHON")

        # Modules: AUTO files with .py extension (importable)
        config = by_path["/Workspace/Pentaho_Migration/Retail_ETL/config.py"]
        self.assertEqual(config["format"], "AUTO")
        self.assertNotIn("language", config)

        job = by_path["/Workspace/Pentaho_Migration/Retail_ETL/jobs/load.py"]
        self.assertEqual(job["format"], "AUTO")

        readme = by_path["/Workspace/Pentaho_Migration/Retail_ETL/README.md"]
        self.assertEqual(readme["format"], "AUTO")

        for path, _method, _payload in calls:
            self.assertNotIn("/jobs/", path) if path.startswith("/api") else None
            self.assertNotIn("runs/submit", path)

    def test_continues_after_partial_failures(self):
        def fake_request(host, token, path, payload=None, method="GET"):
            if path == "/api/2.0/workspace/import":
                target = (payload or {}).get("path", "")
                if target.endswith("bad_file.py") or target.endswith("/bad_file"):
                    return 400, {"message": "import denied"}
            return 200, {}

        files = {
            "good.py": "x = 1\n",
            "bad_file.py": "y = 2\n",
            "also_good.py": "z = 3\n",
        }
        with patch("databricks.databricks_client._request", side_effect=fake_request):
            result = deploy_project(
                "https://adb.example.com",
                "tok",
                files,
                base_dir="/Workspace/Pentaho_Migration",
                project_name="Demo",
            )

        self.assertTrue(result.ok)
        self.assertEqual(result.uploaded_count, 2)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.failed[0]["file"], "bad_file.py")
        self.assertIn("Uploaded: 2 files", result.message)
        self.assertIn("Failed: 1 files", result.message)

    def test_all_failures_return_not_ok(self):
        with patch(
            "databricks.databricks_client._request",
            return_value=(500, {"message": "boom"}),
        ):
            result = deploy_project(
                "https://adb.example.com",
                "tok",
                {"a.py": "print(1)"},
                project_name="X",
            )
        self.assertFalse(result.ok)
        self.assertIsInstance(result, DeployResult)

    def test_notebook_helper_still_works(self):
        src = to_notebook_source(
            "def run_a(spark):\n    return 1\n\ndef main():\n    pass\n"
        )
        self.assertIn("# Databricks notebook source", src)
        self.assertNotIn("def main():", src)


if __name__ == "__main__":
    unittest.main()
