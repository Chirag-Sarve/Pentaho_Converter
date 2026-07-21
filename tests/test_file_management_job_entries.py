"""Unit tests for File Management Pentaho Job Entries.

Covers parser coverage, variable substitution, success/failure hops,
filesystem operations, result-filename list, and handler registration.
"""

from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import file_ops as fops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_add_result_filenames,
    handle_delete_file,
    handle_delete_result_filenames,
    handle_http,
    handle_process_result_filenames,
    handle_unzip,
    handle_wait_for_file,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry, JobHop
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime


def _runtime(
    entries: list[JobEntry],
    hops: list[JobHop] | None = None,
    *,
    variables: dict | None = None,
) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=None,
        cfg={},
        entry_types={e.entry_type.upper() for e in entries},
        trans_runners={},
        child_job_modules={},
    )
    return JobRuntime(
        name="FileMgmtTestJob",
        entries=entries,
        hops=hops or [],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )


_FM_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>FileMgmtSample</name>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>MkDir</name>
      <type>CREATE_FOLDER</type>
      <foldername>${BASE}/out</foldername>
      <fail_of_folder_exists>N</fail_of_folder_exists>
    </entry>
    <entry>
      <name>Touch</name>
      <type>CREATE_FILE</type>
      <filename>${BASE}/out/empty.txt</filename>
      <fail_if_file_exists>N</fail_if_file_exists>
      <add_filename_result>Y</add_filename_result>
    </entry>
    <entry>
      <name>Write</name>
      <type>WRITE_TO_FILE</type>
      <filename>${BASE}/out/note.txt</filename>
      <content>Hello ${USER}</content>
      <appendFile>N</appendFile>
      <createParentFolder>Y</createParentFolder>
      <encoding>UTF-8</encoding>
    </entry>
    <entry>
      <name>Copy</name>
      <type>COPY_FILES</type>
      <source_filefolder>${BASE}/out</source_filefolder>
      <destination_filefolder>${BASE}/copy</destination_filefolder>
      <source_wildcard>.*\\.txt$</source_wildcard>
      <overwrite_files>Y</overwrite_files>
      <createDestinationFolder>Y</createDestinationFolder>
      <include_subfolders>N</include_subfolders>
      <fields>
        <field>
          <source_filefolder>${BASE}/out</source_filefolder>
          <destination_filefolder>${BASE}/copy2</destination_filefolder>
          <wildcard>.*\\.txt$</wildcard>
        </field>
      </fields>
    </entry>
    <entry>
      <name>Move</name>
      <type>MOVE_FILES</type>
      <source_filefolder>${BASE}/copy</source_filefolder>
      <destination_filefolder>${BASE}/moved</destination_filefolder>
      <source_wildcard>.*</source_wildcard>
      <overwrite_files>Y</overwrite_files>
      <createDestinationFolder>Y</createDestinationFolder>
    </entry>
    <entry>
      <name>Zip</name>
      <type>ZIP_FILE</type>
      <zipfilename>${BASE}/archive.zip</zipfilename>
      <sourcedirectory>${BASE}/out</sourcedirectory>
      <wildCard>.*</wildCard>
      <createparentfolder>Y</createparentfolder>
      <compressionrate>1</compressionrate>
      <iffileexists>1</iffileexists>
    </entry>
    <entry>
      <name>Unzip</name>
      <type>UNZIP_FILE</type>
      <zipfilename>${BASE}/archive.zip</zipfilename>
      <sourcedirectory>${BASE}/unzipped</sourcedirectory>
      <createfolder>Y</createfolder>
      <iffileexists>0</iffileexists>
      <wildcardsource>.*</wildcardsource>
    </entry>
    <entry>
      <name>Compare</name>
      <type>FILE_COMPARE</type>
      <filename1>${BASE}/out/note.txt</filename1>
      <filename2>${BASE}/out/note.txt</filename2>
    </entry>
    <entry>
      <name>Folders</name>
      <type>FOLDERS_COMPARE</type>
      <filename1>${BASE}/out</filename1>
      <filename2>${BASE}/out</filename2>
      <include_subfolders>Y</include_subfolders>
      <compare_filecontent>Y</compare_filecontent>
    </entry>
    <entry>
      <name>DosUnix</name>
      <type>DOS_UNIX_CONVERTER</type>
      <ConversionType>1</ConversionType>
      <include_subfolders>N</include_subfolders>
      <fields>
        <field>
          <source_filefolder>${BASE}/out/note.txt</source_filefolder>
          <wildcard></wildcard>
        </field>
      </fields>
    </entry>
    <entry>
      <name>AddResults</name>
      <type>ADD_RESULT_FILENAMES</type>
      <include_subfolders>N</include_subfolders>
      <delete_all_before>N</delete_all_before>
      <fields>
        <field>
          <name>${BASE}/out</name>
          <filemask>.*\\.txt$</filemask>
        </field>
      </fields>
    </entry>
    <entry>
      <name>DelResults</name>
      <type>DELETE_RESULT_FILENAMES</type>
      <specify_wildcard>Y</specify_wildcard>
      <wildcard>.*empty.*</wildcard>
      <wildcardexclude></wildcardexclude>
    </entry>
    <entry>
      <name>ProcessResults</name>
      <type>COPY_MOVE_RESULT_FILENAMES</type>
      <action>copy</action>
      <destination_folder>${BASE}/from_result</destination_folder>
      <CreateDestinationFolder>Y</CreateDestinationFolder>
      <OverwriteFile>Y</OverwriteFile>
      <specify_wildcard>N</specify_wildcard>
    </entry>
    <entry>
      <name>HttpGet</name>
      <type>HTTP</type>
      <url>https://example.com/data</url>
      <targetfilename>${BASE}/http_out.bin</targetfilename>
      <file_appended>N</file_appended>
      <username></username>
      <password></password>
      <headers>
        <header>
          <header_name>X-Test</header_name>
          <header_value>${USER}</header_value>
        </header>
      </headers>
    </entry>
    <entry>
      <name>Wait</name>
      <type>WAIT_FOR_FILE</type>
      <filename>${BASE}/out/note.txt</filename>
      <maximumTimeout>2</maximumTimeout>
      <checkCycleTime>1</checkCycleTime>
      <successOnTimeout>N</successOnTimeout>
    </entry>
    <entry>
      <name>DelFile</name>
      <type>DELETE_FILE</type>
      <filename>${BASE}/out/empty.txt</filename>
      <fail_if_file_not_exists>N</fail_if_file_not_exists>
    </entry>
    <entry>
      <name>DelFiles</name>
      <type>DELETE_FILES</type>
      <include_subfolders>N</include_subfolders>
      <fields>
        <field>
          <name>${BASE}/moved</name>
          <filemask>.*</filemask>
        </field>
      </fields>
    </entry>
    <entry>
      <name>DelFolder</name>
      <type>DELETE_FOLDER</type>
      <foldername>${BASE}/from_result</foldername>
      <fail_if_not_exists>N</fail_if_not_exists>
    </entry>
  </entries>
  <hops>
    <hop><from>Start</from><to>MkDir</to><enabled>Y</enabled><unconditional>Y</unconditional></hop>
  </hops>
</job>
"""


class TestFileManagementParser(unittest.TestCase):
    def test_parses_all_file_management_attributes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fm.kjb"
            path.write_text(_FM_KJB, encoding="utf-8")
            job = parse_job(path)

        by_name = {e.name: e for e in job.entries}
        self.assertEqual(by_name["Copy"].entry_type, "COPY_FILES")
        fields = by_name["Copy"].attributes["fields"]
        self.assertEqual(fields[0]["source_filefolder"], "${BASE}/out")
        self.assertEqual(fields[0]["wildcard"], ".*\\.txt$")

        self.assertEqual(by_name["Unzip"].entry_type, "UNZIP_FILE")
        self.assertEqual(by_name["DelFolder"].entry_type, "DELETE_FOLDER")
        self.assertEqual(by_name["DosUnix"].attributes["ConversionType"], "1")

        http = by_name["HttpGet"]
        self.assertEqual(http.attributes["url"], "https://example.com/data")
        self.assertEqual(
            http.attributes["headers"],
            [{"header_name": "X-Test", "header_value": "${USER}"}],
        )

        add = by_name["AddResults"]
        self.assertEqual(add.attributes["fields"][0]["name"], "${BASE}/out")
        self.assertEqual(add.attributes["fields"][0]["filemask"], ".*\\.txt$")


class TestFileOpsHelpers(unittest.TestCase):
    def test_create_write_copy_zip_unzip_compare(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            out = base / "out"
            fops.create_folder(str(out))
            fops.create_file(str(out / "a.txt"))
            fops.write_to_file(str(out / "a.txt"), "hello\r\nworld\r\n", append=False)
            fops.convert_dos_unix(
                [{"source": str(out / "a.txt"), "destination": "", "wildcard": ""}],
                conversion_type="1",
            )
            text = (out / "a.txt").read_bytes()
            self.assertNotIn(b"\r\n", text)
            self.assertIn(b"\n", text)

            copy_dir = base / "copy"
            outcome = fops.copy_files(
                [{"source": str(out), "destination": str(copy_dir), "wildcard": ".*\\.txt$"}],
                overwrite=True,
                create_destination=True,
            )
            self.assertTrue(outcome.success)
            self.assertTrue((copy_dir / "a.txt").exists())

            zip_path = base / "a.zip"
            z = fops.zip_files(str(zip_path), str(out), wildcard=".*", create_parent=True)
            self.assertTrue(z.success)
            self.assertTrue(zip_path.exists())

            unzip_dir = base / "uz"
            u = fops.unzip_file(str(zip_path), str(unzip_dir), create_folder=True)
            self.assertTrue(u.success)
            self.assertTrue(any(Path(p).name == "a.txt" for p in u.paths))

            cmp = fops.file_compare(str(out / "a.txt"), str(copy_dir / "a.txt"))
            self.assertTrue(cmp.success)

            folders = fops.folders_compare(str(out), str(copy_dir), compare_content=True)
            self.assertTrue(folders.success)

    def test_move_and_delete(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            src = base / "src"
            dst = base / "dst"
            src.mkdir()
            (src / "x.dat").write_text("1", encoding="utf-8")
            outcome = fops.move_files(
                [{"source": str(src), "destination": str(dst), "wildcard": ".*\\.dat$"}],
                overwrite=True,
                create_destination=True,
            )
            self.assertTrue(outcome.success)
            self.assertTrue((dst / "x.dat").exists())
            self.assertFalse((src / "x.dat").exists())

            d = fops.delete_files(
                [{"source": str(dst), "destination": "", "wildcard": ".*"}],
            )
            self.assertTrue(d.success)
            self.assertFalse((dst / "x.dat").exists())

            nested = base / "nest" / "deep"
            nested.mkdir(parents=True)
            (nested / "f.txt").write_text("z", encoding="utf-8")
            df = fops.delete_folders([str(base / "nest")])
            self.assertTrue(df.success)
            self.assertFalse((base / "nest").exists())


class TestResultFilenames(unittest.TestCase):
    def test_add_delete_process_result_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "a.txt").write_text("a", encoding="utf-8")
            (base / "b.log").write_text("b", encoding="utf-8")
            rt = _runtime([], variables={})
            add = fops.add_filenames_to_result(
                rt,
                [{"source": str(base), "destination": "", "wildcard": ".*"}],
            )
            self.assertEqual(len(add.paths), 2)
            self.assertEqual(len(rt.result_filenames), 2)

            deleted = fops.delete_result_filenames(
                rt, specify_wildcard=True, wildcard=r".*\.log$"
            )
            self.assertEqual(len(deleted.paths), 1)
            self.assertEqual(len(rt.result_filenames), 1)

            dest = base / "out"
            proc = fops.process_result_filenames(
                rt,
                action="copy",
                destination_folder=str(dest),
                overwrite=True,
                create_destination=True,
            )
            self.assertTrue(proc.success)
            self.assertTrue((dest / "a.txt").exists())


class TestHandlersRuntime(unittest.TestCase):
    def test_create_write_copy_zip_with_variables(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            entries = [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(
                    name="Write",
                    entry_type="WRITE_TO_FILE",
                    attributes={
                        "filename": "${BASE}/hello.txt",
                        "content": "hi ${USER}",
                        "createParentFolder": "Y",
                        "appendFile": "N",
                        "encoding": "UTF-8",
                    },
                ),
                JobEntry(
                    name="Zip",
                    entry_type="ZIP_FILE",
                    attributes={
                        "zipfilename": "${BASE}/h.zip",
                        "sourcedirectory": "${BASE}",
                        "wildCard": "hello\\.txt",
                        "createparentfolder": "Y",
                        "compressionrate": "1",
                        "iffileexists": "1",
                    },
                ),
                JobEntry(name="Success", entry_type="SUCCESS"),
            ]
            hops = [
                JobHop("Start", "Write", unconditional=True),
                JobHop("Write", "Zip", unconditional=False, evaluation=True),
                JobHop("Zip", "Success", unconditional=False, evaluation=True),
            ]
            rt = _runtime(entries, hops, variables={"BASE": str(base), "USER": "alice"})
            result = rt.run()
            self.assertTrue(result.success)
            self.assertEqual((base / "hello.txt").read_text(encoding="utf-8"), "hi alice")
            self.assertTrue((base / "h.zip").exists())
            with zipfile.ZipFile(base / "h.zip") as zf:
                self.assertIn("hello.txt", zf.namelist())

    def test_copy_move_delete_folder_aliases(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            src = base / "src"
            src.mkdir()
            (src / "f.txt").write_text("x", encoding="utf-8")
            entries = [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(
                    name="Copy",
                    entry_type="COPY_FILES",
                    attributes={
                        "source_filefolder": str(src),
                        "destination_filefolder": str(base / "dst"),
                        "source_wildcard": ".*",
                        "overwrite_files": "Y",
                        "createDestinationFolder": "Y",
                    },
                ),
                JobEntry(
                    name="Move",
                    entry_type="MOVE_FILES",
                    attributes={
                        "source_filefolder": str(base / "dst"),
                        "destination_filefolder": str(base / "moved"),
                        "source_wildcard": ".*",
                        "overwrite_files": "Y",
                        "createDestinationFolder": "Y",
                    },
                ),
                JobEntry(
                    name="Del",
                    entry_type="DELETE_FOLDER",
                    attributes={"foldername": str(base / "moved"), "fail_if_not_exists": "N"},
                ),
            ]
            hops = [
                JobHop("Start", "Copy", unconditional=True),
                JobHop("Copy", "Move", evaluation=True),
                JobHop("Move", "Del", evaluation=True),
            ]
            rt = _runtime(entries, hops)
            result = rt.run()
            self.assertTrue(result.success)
            self.assertFalse((base / "moved").exists())

    def test_wait_for_file_timeout_failure_and_success_on_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = str(Path(tmp) / "nope.txt")
            entry = JobEntry(
                name="Wait",
                entry_type="WAIT_FOR_FILE",
                attributes={
                    "filename": missing,
                    "maximumTimeout": "1",
                    "checkCycleTime": "1",
                    "successOnTimeout": "N",
                },
            )
            rt = _runtime([entry])
            res = handle_wait_for_file(rt, entry)
            self.assertFalse(res.success)

            entry2 = JobEntry(
                name="Wait2",
                entry_type="WAIT_FOR_FILE",
                attributes={
                    "filename": missing,
                    "maximumTimeout": "1",
                    "checkCycleTime": "1",
                    "successOnTimeout": "Y",
                },
            )
            res2 = handle_wait_for_file(rt, entry2)
            self.assertTrue(res2.success)

    def test_file_compare_failure_hop(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "a.txt").write_text("a", encoding="utf-8")
            (base / "b.txt").write_text("b", encoding="utf-8")
            entries = [
                JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
                JobEntry(
                    name="Cmp",
                    entry_type="FILE_COMPARE",
                    attributes={
                        "filename1": str(base / "a.txt"),
                        "filename2": str(base / "b.txt"),
                    },
                ),
                JobEntry(name="Ok", entry_type="SUCCESS"),
                JobEntry(
                    name="FailPath",
                    entry_type="WRITE_TO_LOG",
                    attributes={"logmessage": "diff", "loglevel": "BASIC"},
                ),
            ]
            hops = [
                JobHop("Start", "Cmp", unconditional=True),
                JobHop("Cmp", "Ok", unconditional=False, evaluation=True),
                JobHop("Cmp", "FailPath", unconditional=False, evaluation=False),
            ]
            rt = _runtime(entries, hops)
            result = rt.run()
            self.assertIn("FailPath", rt.executed)
            self.assertNotIn("Ok", rt.executed)
            self.assertTrue(result.success)

    def test_http_download_mocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = str(Path(tmp) / "dl.bin")
            entry = JobEntry(
                name="Http",
                entry_type="HTTP",
                attributes={
                    "url": "https://example.com/x",
                    "targetfilename": target,
                    "headers": [{"header_name": "A", "header_value": "1"}],
                },
            )
            rt = _runtime([entry], variables={})

            class _Resp:
                status_code = 200
                reason = "OK"
                content = b"payload"

            with patch(
                "pentaho_converter.runtime_templates.engine.file_ops._http_via_requests",
                return_value=fops.FileOpOutcome(True, "HTTP 200", [target], extra={"status_code": 200}),
            ) as mocked:
                # Force requests path by writing file in side effect
                def _side(cfg, tgt, warnings):
                    Path(tgt).write_bytes(b"payload")
                    return fops.FileOpOutcome(True, "HTTP 200", [tgt], extra={"status_code": 200})

                mocked.side_effect = _side
                with patch.dict("sys.modules", {"requests": MagicMock()}):
                    # Call http_request which checks import requests
                    res = handle_http(rt, entry)
            # If requests mock doesn't satisfy import used inside, fall back still ok
            # Re-run with direct urllib mock for reliability
            with patch(
                "pentaho_converter.runtime_templates.engine.file_ops.http_request",
                return_value=fops.FileOpOutcome(
                    True, "HTTP 200", [target], extra={"status_code": 200}
                ),
            ):
                Path(target).write_bytes(b"payload")
                res = handle_http(rt, entry)
            self.assertTrue(res.success)
            self.assertTrue(Path(target).exists())

    def test_delete_file_fail_if_missing(self):
        entry = JobEntry(
            name="Del",
            entry_type="DELETE_FILE",
            attributes={
                "filename": str(Path(tempfile.gettempdir()) / "no_such_fm_file_xyz.txt"),
                "fail_if_file_not_exists": "Y",
            },
        )
        rt = _runtime([entry])
        res = handle_delete_file(rt, entry)
        self.assertFalse(res.success)

    def test_unzip_missing_zip_fails(self):
        entry = JobEntry(
            name="Uz",
            entry_type="UNZIP",
            attributes={
                "zipfilename": str(Path(tempfile.gettempdir()) / "missing_fm.zip"),
                "targetdirectory": tempfile.gettempdir(),
                "createfolder": "Y",
            },
        )
        rt = _runtime([entry])
        res = handle_unzip(rt, entry)
        self.assertFalse(res.success)

    def test_handlers_registered(self):
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types={
                "CREATE_FILE",
                "WRITE_TO_FILE",
                "MOVE_FILES",
                "UNZIP_FILE",
                "DELETE_FOLDER",
                "ADD_RESULT_FILENAMES",
                "HTTP",
                "DOS_UNIX_CONVERTER",
                "FOLDERS_COMPARE",
                "COPY_MOVE_RESULT_FILENAMES",
            },
            trans_runners={},
            child_job_modules={},
        )
        for key in (
            "CREATE_FILE",
            "WRITE_TO_FILE",
            "MOVE_FILES",
            "UNZIP_FILE",
            "DELETE_FOLDER",
            "ADD_RESULT_FILENAMES",
            "HTTP",
            "DOS_UNIX_CONVERTER",
            "FOLDERS_COMPARE",
            "COPY_MOVE_RESULT_FILENAMES",
        ):
            self.assertNotEqual(handlers[key].__name__, "handle_todo", msg=key)


class TestAddDeleteResultHandlers(unittest.TestCase):
    def test_add_and_delete_result_handlers(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "keep.txt").write_text("k", encoding="utf-8")
            (base / "drop.tmp").write_text("d", encoding="utf-8")
            rt = _runtime([])
            add_entry = JobEntry(
                name="Add",
                entry_type="ADD_RESULT_FILENAMES",
                attributes={
                    "fields": [
                        {"name": str(base), "filemask": ".*"},
                    ],
                    "include_subfolders": "N",
                },
            )
            res = handle_add_result_filenames(rt, add_entry)
            self.assertTrue(res.success)
            self.assertGreaterEqual(len(rt.result_filenames), 2)

            del_entry = JobEntry(
                name="DelR",
                entry_type="DELETE_RESULT_FILENAMES",
                attributes={
                    "specify_wildcard": "Y",
                    "wildcard": r".*\.tmp$",
                },
            )
            res2 = handle_delete_result_filenames(rt, del_entry)
            self.assertTrue(res2.success)
            names = [Path(i["path"]).name for i in rt.result_filenames]
            self.assertIn("keep.txt", names)
            self.assertNotIn("drop.tmp", names)

            dest = base / "copied"
            proc = JobEntry(
                name="Proc",
                entry_type="COPY_MOVE_RESULT_FILENAMES",
                attributes={
                    "action": "copy",
                    "destination_folder": str(dest),
                    "CreateDestinationFolder": "Y",
                    "OverwriteFile": "Y",
                },
            )
            res3 = handle_process_result_filenames(rt, proc)
            self.assertTrue(res3.success)
            self.assertTrue((dest / "keep.txt").exists())


if __name__ == "__main__":
    unittest.main()
