"""Unit tests for Repository-category Pentaho Job Entries.

Covers CONNECTED_TO_REPOSITORY and EXPORT_REPOSITORY —
parser, success/failure, variable substitution, stubs, local serialization.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import repository_ops as rops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_connected_to_repository,
    handle_export_repository,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime


def _runtime(*, variables: dict | None = None, repository: dict | None = None) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=None,
        cfg={},
        entry_types={"CONNECTED_TO_REPOSITORY", "EXPORT_REPOSITORY"},
        trans_runners={},
        child_job_modules={},
    )
    rt = JobRuntime(
        name="RepoTestJob",
        entries=[],
        hops=[],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )
    rt.result_filenames = []
    if repository is not None:
        rt.repository = repository
    return rt


_REPO_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>RepoSample</name>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>CheckRepo</name>
      <type>CONNECTED_TO_REPOSITORY</type>
      <isspecificrep>Y</isspecificrep>
      <repname>${REPO_NAME}</repname>
      <isspecificuser>Y</isspecificuser>
      <username>${REPO_USER}</username>
      <custom_meta>keep</custom_meta>
    </entry>
    <entry>
      <name>ExportRepo</name>
      <type>EXPORT_REPOSITORY</type>
      <repositoryname>${REPO_NAME}</repositoryname>
      <username>${REPO_USER}</username>
      <password>${REPO_PASS}</password>
      <targetfilename>${OUT}/repo_export.xml</targetfilename>
      <iffileexists>0</iffileexists>
      <export_type>Export_All</export_type>
      <directoryPath>${SRC}</directoryPath>
      <add_date>N</add_date>
      <add_time>N</add_time>
      <SpecifyFormat>N</SpecifyFormat>
      <date_time_format>yyyyMMdd_HHmmss</date_time_format>
      <createfolder>Y</createfolder>
      <newfolder>N</newfolder>
      <add_result_filesname>Y</add_result_filesname>
      <nr_errors_less_than>10</nr_errors_less_than>
      <success_condition>success_if_no_errors</success_condition>
    </entry>
  </entries>
</job>
"""


class TestRepoParser(unittest.TestCase):
    def test_parses_repository_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "repo.kjb"
            path.write_text(_REPO_KJB, encoding="utf-8")
            job = parse_job(path)

        by_type = {e.entry_type: e for e in job.entries if e.entry_type != "SPECIAL"}
        self.assertIn("CONNECTED_TO_REPOSITORY", by_type)
        self.assertIn("EXPORT_REPOSITORY", by_type)

        conn = by_type["CONNECTED_TO_REPOSITORY"].attributes
        self.assertEqual(conn["isspecificrep"], "Y")
        self.assertEqual(conn["repname"], "${REPO_NAME}")
        self.assertEqual(conn["username"], "${REPO_USER}")
        self.assertEqual(conn["custom_meta"], "keep")

        exp = by_type["EXPORT_REPOSITORY"].attributes
        self.assertEqual(exp["export_type"], "Export_All")
        self.assertEqual(exp["directoryPath"], "${SRC}")
        self.assertEqual(exp["add_result_filesname"], "Y")
        self.assertEqual(exp["targetfilename"], "${OUT}/repo_export.xml")


class TestConnectedToRepository(unittest.TestCase):
    def test_fails_without_repository(self) -> None:
        outcome = rops.check_connected_to_repository()
        self.assertFalse(outcome.success)
        self.assertTrue(any("Databricks" in w for w in outcome.warnings))

    def test_override_success(self) -> None:
        outcome = rops.check_connected_to_repository(
            isspecificrep=True,
            repname="MainRepo",
            connected_override="Y",
        )
        self.assertTrue(outcome.success)

    def test_override_wrong_user_with_meta(self) -> None:
        outcome = rops.check_connected_to_repository(
            isspecificuser=True,
            username="alice",
            repository_meta={"username": "bob"},
            connected_override="Y",
        )
        self.assertFalse(outcome.success)

    def test_synthetic_meta_match(self) -> None:
        outcome = rops.check_connected_to_repository(
            isspecificrep=True,
            repname="MainRepo",
            isspecificuser=True,
            username="etl",
            repository_meta={
                "connected": "Y",
                "name": "MainRepo",
                "username": "etl",
            },
        )
        self.assertTrue(outcome.success)

    def test_synthetic_meta_name_mismatch(self) -> None:
        outcome = rops.check_connected_to_repository(
            isspecificrep=True,
            repname="A",
            repository_meta={"connected": "Y", "name": "B"},
        )
        self.assertFalse(outcome.success)

    def test_handler_variables_override(self) -> None:
        rt = _runtime(
            variables={
                "REPO_NAME": "MainRepo",
                "REPO_USER": "etl",
                "REPOSITORY_CONNECTED": "Y",
            }
        )
        res = handle_connected_to_repository(
            rt,
            JobEntry(
                name="CheckRepo",
                entry_type="CONNECTED_TO_REPOSITORY",
                attributes={
                    "isspecificrep": "Y",
                    "repname": "${REPO_NAME}",
                    "isspecificuser": "Y",
                    "username": "${REPO_USER}",
                },
            ),
        )
        self.assertTrue(res.success)


class TestExportRepository(unittest.TestCase):
    def test_export_from_local_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "repo_objs"
            src.mkdir()
            (src / "job1.kjb").write_text("<job/>", encoding="utf-8")
            (src / "tr1.ktr").write_text("<transformation/>", encoding="utf-8")
            out = root / "out" / "export.xml"
            outcome = rops.export_repository_to_xml(
                repositoryname="Main",
                username="etl",
                targetfilename=str(out),
                directory_path=str(src),
                export_type="Export_All",
                createfolder=True,
                add_result_filesname=True,
            )
            self.assertTrue(outcome.success, outcome.message)
            self.assertTrue(out.exists())
            text = out.read_text(encoding="utf-8")
            self.assertIn("repository_export", text)
            self.assertIn("job1.kjb", text)
            self.assertIn("tr1.ktr", text)
            self.assertIn("TODO", text.upper() or "todo")
            self.assertEqual(outcome.extra.get("object_count"), 2)

    def test_export_jobs_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "objs"
            src.mkdir()
            (src / "a.kjb").write_text("<job/>", encoding="utf-8")
            (src / "b.ktr").write_text("<transformation/>", encoding="utf-8")
            out = root / "jobs.xml"
            outcome = rops.export_repository_to_xml(
                targetfilename=str(out),
                directory_path=str(src),
                export_type="Export_Jobs",
            )
            self.assertTrue(outcome.success)
            self.assertEqual(outcome.extra["object_count"], 1)

    def test_export_missing_source_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "x.xml"
            outcome = rops.export_repository_to_xml(
                targetfilename=str(out),
                directory_path=str(Path(tmp) / "missing"),
                allow_stub=False,
            )
            self.assertFalse(outcome.success)

    def test_export_stub_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "stub.xml"
            outcome = rops.export_repository_to_xml(
                targetfilename=str(out),
                repositoryname="R",
                allow_stub=True,
            )
            self.assertTrue(outcome.success)
            self.assertTrue(out.exists())
            self.assertIn("todo", out.read_text(encoding="utf-8").lower())

    def test_iffileexists_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "exists.xml"
            out.write_text("<old/>", encoding="utf-8")
            outcome = rops.export_repository_to_xml(
                targetfilename=str(out),
                iffileexists="2",
                allow_stub=True,
            )
            self.assertFalse(outcome.success)

    def test_handler_export_with_vars_and_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "src"
            src.mkdir()
            (src / "j.kjb").write_text("<job/>", encoding="utf-8")
            out_dir = root / "out"
            rt = _runtime(
                variables={
                    "REPO_NAME": "Main",
                    "REPO_USER": "etl",
                    "REPO_PASS": "x",
                    "OUT": str(out_dir),
                    "SRC": str(src),
                }
            )
            res = handle_export_repository(
                rt,
                JobEntry(
                    name="ExportRepo",
                    entry_type="EXPORT_REPOSITORY",
                    attributes={
                        "repositoryname": "${REPO_NAME}",
                        "username": "${REPO_USER}",
                        "password": "${REPO_PASS}",
                        "targetfilename": "${OUT}/repo_export.xml",
                        "directoryPath": "${SRC}",
                        "export_type": "Export_All",
                        "createfolder": "Y",
                        "add_result_filesname": "Y",
                        "iffileexists": "0",
                    },
                ),
            )
            self.assertTrue(res.success)
            self.assertTrue((out_dir / "repo_export.xml").exists())
            self.assertTrue(rt.result_filenames)


class TestRegistration(unittest.TestCase):
    def test_handlers_registered(self) -> None:
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types=set(),
            trans_runners={},
            child_job_modules={},
        )
        self.assertIn("CONNECTED_TO_REPOSITORY", handlers)
        self.assertIn("EXPORT_REPOSITORY", handlers)


if __name__ == "__main__":
    unittest.main()
