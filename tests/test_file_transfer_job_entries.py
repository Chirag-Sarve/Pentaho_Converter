"""Unit tests for File Transfer Pentaho Job Entries.

Covers FTP, FTP_PUT, FTP_DELETE, FTPS_GET, FTPS_PUT, SFTP, SFTPPUT —
parser, mocked success/failure, auth errors, variable substitution.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import transfer_ops as tops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_ftp_delete,
    handle_ftp_get,
    handle_ftp_put,
    handle_ftps_get,
    handle_ftps_put,
    handle_sftp_get,
    handle_sftp_put,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime


def _runtime(*, variables: dict | None = None) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=None,
        cfg={},
        entry_types={
            "FTP",
            "FTP_PUT",
            "FTP_DELETE",
            "FTPS_GET",
            "FTPS_PUT",
            "SFTP",
            "SFTPPUT",
        },
        trans_runners={},
        child_job_modules={},
    )
    rt = JobRuntime(
        name="TransferTestJob",
        entries=[],
        hops=[],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )
    rt.result_filenames = []
    return rt


_TRANSFER_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>TransferSample</name>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>GetFtp</name>
      <type>FTP</type>
      <servername>${FTP_HOST}</servername>
      <port>21</port>
      <username>${FTP_USER}</username>
      <password>${FTP_PASS}</password>
      <ftpdirectory>/inbound</ftpdirectory>
      <targetdirectory>${LOCAL}/in</targetdirectory>
      <wildcard>.*\\.csv$</wildcard>
      <binary>Y</binary>
      <timeout>30</timeout>
      <active>N</active>
      <only_new>N</only_new>
      <isaddresult>Y</isaddresult>
      <proxy_host></proxy_host>
      <custom_tag>keep</custom_tag>
    </entry>
    <entry>
      <name>PutFtp</name>
      <type>FTP_PUT</type>
      <servername>${FTP_HOST}</servername>
      <serverport>21</serverport>
      <username>${FTP_USER}</username>
      <password>${FTP_PASS}</password>
      <remoteDirectory>/outbound</remoteDirectory>
      <localDirectory>${LOCAL}/out</localDirectory>
      <wildcard>*.csv</wildcard>
      <binary>Y</binary>
      <only_new>Y</only_new>
      <active>N</active>
    </entry>
    <entry>
      <name>DelFtp</name>
      <type>FTP_DELETE</type>
      <protocol>FTP</protocol>
      <servername>${FTP_HOST}</servername>
      <port>21</port>
      <username>${FTP_USER}</username>
      <password>${FTP_PASS}</password>
      <ftpdirectory>/tmp</ftpdirectory>
      <wildcard>.*\\.tmp$</wildcard>
      <timeout>30</timeout>
      <active>N</active>
    </entry>
    <entry>
      <name>GetFtps</name>
      <type>FTPS_GET</type>
      <servername>${FTP_HOST}</servername>
      <port>21</port>
      <username>${FTP_USER}</username>
      <password>${FTP_PASS}</password>
      <FTPSdirectory>/secure</FTPSdirectory>
      <targetdirectory>${LOCAL}/ftps</targetdirectory>
      <wildcard>.*</wildcard>
      <binary>Y</binary>
      <connection_type>0</connection_type>
      <isaddresult>Y</isaddresult>
    </entry>
    <entry>
      <name>PutFtps</name>
      <type>FTPS_PUT</type>
      <servername>${FTP_HOST}</servername>
      <serverport>21</serverport>
      <username>${FTP_USER}</username>
      <password>${FTP_PASS}</password>
      <remoteDirectory>/upload</remoteDirectory>
      <localDirectory>${LOCAL}/up</localDirectory>
      <wildcard>*</wildcard>
      <binary>Y</binary>
      <connection_type>0</connection_type>
    </entry>
    <entry>
      <name>GetSftp</name>
      <type>SFTP</type>
      <servername>${SFTP_HOST}</servername>
      <serverport>22</serverport>
      <username>${SFTP_USER}</username>
      <password>${SFTP_PASS}</password>
      <sftpdirectory>/data</sftpdirectory>
      <targetdirectory>${LOCAL}/sftp</targetdirectory>
      <wildcard>.*\\.txt$</wildcard>
      <isaddresult>Y</isaddresult>
      <createtargetfolder>Y</createtargetfolder>
      <usekeyfilename>N</usekeyfilename>
    </entry>
    <entry>
      <name>PutSftp</name>
      <type>SFTPPUT</type>
      <servername>${SFTP_HOST}</servername>
      <serverport>22</serverport>
      <username>${SFTP_USER}</username>
      <password>${SFTP_PASS}</password>
      <sftpdirectory>/upload</sftpdirectory>
      <localdirectory>${LOCAL}/sftp_out</localdirectory>
      <wildcard>*</wildcard>
      <createRemoteFolder>Y</createRemoteFolder>
      <aftersftpput>nothing</aftersftpput>
      <successWhenNoFile>Y</successWhenNoFile>
      <addFilenameResut>Y</addFilenameResut>
    </entry>
  </entries>
</job>
"""


class TestTransferParser(unittest.TestCase):
    def test_parses_all_transfer_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "xfer.kjb"
            path.write_text(_TRANSFER_KJB, encoding="utf-8")
            job = parse_job(path)
        by_type = {e.entry_type: e for e in job.entries if e.entry_type != "SPECIAL"}
        for needed in (
            "FTP",
            "FTP_PUT",
            "FTP_DELETE",
            "FTPS_GET",
            "FTPS_PUT",
            "SFTP",
            "SFTPPUT",
        ):
            self.assertIn(needed, by_type)
        self.assertEqual(by_type["FTP"].attributes["servername"], "${FTP_HOST}")
        self.assertEqual(by_type["FTP"].attributes["custom_tag"], "keep")
        self.assertEqual(by_type["FTPS_GET"].attributes["FTPSdirectory"], "/secure")
        self.assertEqual(by_type["SFTPPUT"].attributes["createRemoteFolder"], "Y")
        self.assertEqual(by_type["FTP_DELETE"].attributes["protocol"], "FTP")


class TestFtpOps(unittest.TestCase):
    def _mock_ftp(self, names: list[str] | None = None) -> MagicMock:
        ftp = MagicMock()
        ftp.nlst.return_value = names or ["a.csv", "b.txt", "."]
        ftp.pwd.return_value = "/"
        # cwd succeeds for remote folders; fails for file names → treated as files

        def _cwd(name: str) -> None:
            base = name.split("/")[-1]
            if base in {".", ".."}:
                return
            if name.startswith("/") or name in {"/in", "/out", "/tmp", "/inbound"}:
                return
            # Any other name is a file for these tests
            raise tops.error_perm("550 not a directory")

        ftp.cwd.side_effect = _cwd
        return ftp

    def test_ftp_get_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "in"
            ftp = self._mock_ftp(["report.csv"])

            def _retr(cmd, callback):
                callback(b"id,name\n1,a\n")

            ftp.retrbinary.side_effect = _retr
            with patch.object(tops, "_connect_ftp", return_value=(ftp, [])):
                outcome = tops.ftp_get(
                    servername="ftp.example",
                    username="u",
                    password="p",
                    ftpdirectory="/in",
                    targetdirectory=str(local),
                    wildcard=r".*\.csv$",
                    binary=True,
                )
            self.assertTrue(outcome.success, outcome.message)
            self.assertTrue((local / "report.csv").exists())
            self.assertIn(str(local / "report.csv"), outcome.paths)

    def test_ftp_get_auth_failure(self) -> None:
        with patch.object(
            tops,
            "_connect_ftp",
            side_effect=tops.error_perm("530 Login incorrect"),
        ):
            outcome = tops.ftp_get(
                servername="ftp.example",
                username="bad",
                password="bad",
                targetdirectory="/tmp",
            )
        self.assertFalse(outcome.success)
        self.assertIn("530", str(outcome.error))

    def test_ftp_put_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp)
            (local / "up.csv").write_text("a,b\n", encoding="utf-8")
            ftp = self._mock_ftp([])
            ftp.nlst.return_value = []
            with patch.object(tops, "_connect_ftp", return_value=(ftp, [])):
                outcome = tops.ftp_put(
                    servername="ftp.example",
                    username="u",
                    password="p",
                    remote_directory="/out",
                    local_directory=str(local),
                    wildcard="*.csv",
                )
            self.assertTrue(outcome.success)
            ftp.storbinary.assert_called()
            self.assertEqual(outcome.extra["count"], 1)

    def test_ftp_put_missing_local(self) -> None:
        outcome = tops.ftp_put(
            servername="ftp.example",
            local_directory="/no/such/dir",
        )
        self.assertFalse(outcome.success)

    def test_ftp_delete_success(self) -> None:
        ftp = self._mock_ftp(["x.tmp", "keep.csv"])
        with patch.object(tops, "_connect_ftp", return_value=(ftp, [])):
            outcome = tops.ftp_delete(
                protocol="FTP",
                servername="ftp.example",
                username="u",
                password="p",
                wildcard=r".*\.tmp$",
            )
        self.assertTrue(outcome.success)
        ftp.delete.assert_called_with("x.tmp")

    def test_handler_ftp_get_variables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / "in"
            ftp = self._mock_ftp(["a.csv"])
            ftp.retrbinary.side_effect = lambda cmd, cb: cb(b"x")
            rt = _runtime(
                variables={
                    "FTP_HOST": "ftp.example",
                    "FTP_USER": "u",
                    "FTP_PASS": "p",
                    "LOCAL": str(tmp),
                }
            )
            with patch.object(tops, "_connect_ftp", return_value=(ftp, [])):
                res = handle_ftp_get(
                    rt,
                    JobEntry(
                        name="GetFtp",
                        entry_type="FTP",
                        attributes={
                            "servername": "${FTP_HOST}",
                            "username": "${FTP_USER}",
                            "password": "${FTP_PASS}",
                            "targetdirectory": "${LOCAL}/in",
                            "wildcard": r".*\.csv$",
                            "isaddresult": "Y",
                            "binary": "Y",
                        },
                    ),
                )
            self.assertTrue(res.success)
            self.assertTrue(rt.result_filenames)


class TestFtpsOps(unittest.TestCase):
    def test_ftps_get_uses_tls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ftp = MagicMock()
            ftp.nlst.return_value = ["sec.dat"]
            ftp.pwd.return_value = "/"
            ftp.cwd.side_effect = lambda name: (
                None
                if name.startswith("/") or name in {".", ".."}
                else (_ for _ in ()).throw(tops.error_perm("550"))
            )
            ftp.retrbinary.side_effect = lambda cmd, cb: cb(b"data")
            with patch.object(tops, "_connect_ftp", return_value=(ftp, [])) as conn:
                outcome = tops.ftps_get(
                    servername="ftps.example",
                    username="u",
                    password="p",
                    targetdirectory=str(Path(tmp)),
                    wildcard=".*",
                    connection_type="0",
                )
            self.assertTrue(outcome.success)
            self.assertTrue(conn.call_args.kwargs.get("tls") or conn.call_args[1].get("tls"))

    def test_handler_ftps_put(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "f.bin").write_bytes(b"abc")
            ftp = MagicMock()
            ftp.nlst.return_value = []
            with patch.object(tops, "_connect_ftp", return_value=(ftp, [])):
                res = handle_ftps_put(
                    _runtime(),
                    JobEntry(
                        name="PutFtps",
                        entry_type="FTPS_PUT",
                        attributes={
                            "servername": "ftps.example",
                            "username": "u",
                            "password": "p",
                            "localDirectory": str(tmp),
                            "remoteDirectory": "/up",
                            "wildcard": "*",
                        },
                    ),
                )
            self.assertTrue(res.success)


class TestSftpOps(unittest.TestCase):
    def test_sftp_missing_paramiko(self) -> None:
        with patch.object(
            tops, "_import_paramiko", side_effect=ImportError("no paramiko")
        ):
            outcome = tops.sftp_get(
                servername="sftp.example",
                username="u",
                password="p",
                targetdirectory="/tmp",
            )
        self.assertFalse(outcome.success)
        self.assertIn("paramiko", str(outcome.error))

    def test_sftp_get_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            transport = MagicMock()
            sftp = MagicMock()
            sftp.listdir.return_value = ["a.txt", "b.csv"]

            def _stat(name: str):
                st = MagicMock()
                st.st_mode = 0o100644
                return st

            sftp.stat.side_effect = _stat

            def _get(name, dest):
                Path(dest).write_text("hello", encoding="utf-8")

            sftp.get.side_effect = _get
            with patch.object(
                tops, "_connect_sftp", return_value=(transport, sftp, [])
            ):
                outcome = tops.sftp_get(
                    servername="sftp.example",
                    username="u",
                    password="p",
                    targetdirectory=str(tmp),
                    wildcard=r".*\.txt$",
                )
            self.assertTrue(outcome.success)
            self.assertTrue((Path(tmp) / "a.txt").exists())
            sftp.get.assert_called()

    def test_sftp_put_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "up.txt").write_text("x", encoding="utf-8")
            transport = MagicMock()
            sftp = MagicMock()
            with patch.object(
                tops, "_connect_sftp", return_value=(transport, sftp, [])
            ):
                outcome = tops.sftp_put(
                    servername="sftp.example",
                    username="u",
                    password="p",
                    sftpdirectory="/upload",
                    localdirectory=str(tmp),
                    wildcard="*",
                    create_remote_folder=True,
                    add_filename_result=True,
                )
            self.assertTrue(outcome.success)
            sftp.put.assert_called()
            self.assertTrue(outcome.paths)

    def test_sftp_put_success_when_no_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            empty = Path(tmp) / "empty"
            empty.mkdir()
            outcome = tops.sftp_put(
                servername="sftp.example",
                username="u",
                password="p",
                localdirectory=str(empty),
                success_when_no_file=True,
            )
            self.assertTrue(outcome.success)

    def test_handler_sftp_get_auth_fail(self) -> None:
        with patch.object(
            tops,
            "_connect_sftp",
            side_effect=Exception("Authentication failed"),
        ):
            res = handle_sftp_get(
                _runtime(),
                JobEntry(
                    name="GetSftp",
                    entry_type="SFTP",
                    attributes={
                        "servername": "sftp.example",
                        "username": "u",
                        "password": "bad",
                        "targetdirectory": "/tmp",
                    },
                ),
            )
        self.assertFalse(res.success)


class TestRegistration(unittest.TestCase):
    def test_handlers_registered(self) -> None:
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types=set(),
            trans_runners={},
            child_job_modules={},
        )
        for key in (
            "FTP",
            "FTP_PUT",
            "FTP_DELETE",
            "FTPS_GET",
            "FTPS_PUT",
            "SFTP",
            "SFTPPUT",
        ):
            self.assertIn(key, handlers)


if __name__ == "__main__":
    unittest.main()
