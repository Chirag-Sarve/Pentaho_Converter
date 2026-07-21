"""Unit tests for File Encryption (PGP) Pentaho Job Entries.

Covers PGP_ENCRYPT_FILES, PGP_DECRYPT_FILES, PGP_VERIFY_FILES —
parser, mocked GnuPG success/failure, missing files, invalid keys/signatures,
variable substitution.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import pgp_ops as pops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_pgp_decrypt_files,
    handle_pgp_encrypt_files,
    handle_pgp_verify_files,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime


def _runtime(*, variables: dict | None = None) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=None,
        cfg={},
        entry_types={
            "PGP_ENCRYPT_FILES",
            "PGP_DECRYPT_FILES",
            "PGP_VERIFY_FILES",
        },
        trans_runners={},
        child_job_modules={},
    )
    rt = JobRuntime(
        name="PgpTestJob",
        entries=[],
        hops=[],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )
    rt.result_filenames = []
    return rt


_PGP_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>PgpSample</name>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>Encrypt</name>
      <type>PGP_ENCRYPT_FILES</type>
      <gpglocation>${GPG_BIN}</gpglocation>
      <asciiMode>Y</asciiMode>
      <include_subfolders>N</include_subfolders>
      <add_result_filesname>Y</add_result_filesname>
      <create_destination_folder>Y</create_destination_folder>
      <iffileexists>0</iffileexists>
      <success_condition>success_if_no_errors</success_condition>
      <nr_errors_less_than>10</nr_errors_less_than>
      <publickeyfile>${PUB_KEY}</publickeyfile>
      <fields>
        <field>
          <action_type>encrypt</action_type>
          <source_filefolder>${SRC}</source_filefolder>
          <userid>${RECIPIENT}</userid>
          <destination_filefolder>${OUT}</destination_filefolder>
          <wildcard>.*\\.txt$</wildcard>
        </field>
      </fields>
      <custom_keep>Y</custom_keep>
    </entry>
    <entry>
      <name>Decrypt</name>
      <type>PGP_DECRYPT_FILES</type>
      <gpglocation>${GPG_BIN}</gpglocation>
      <add_result_filesname>Y</add_result_filesname>
      <create_destination_folder>Y</create_destination_folder>
      <privatekeyfile>${PRIV_KEY}</privatekeyfile>
      <fields>
        <field>
          <source_filefolder>${ENC}</source_filefolder>
          <passphrase>${PASSPHRASE}</passphrase>
          <destination_filefolder>${DEC}</destination_filefolder>
          <wildcard>.*\\.gpg$</wildcard>
        </field>
      </fields>
    </entry>
    <entry>
      <name>Verify</name>
      <type>PGP_VERIFY_FILES</type>
      <gpglocation>${GPG_BIN}</gpglocation>
      <filename>${SIGNED}</filename>
      <detachedfilename>${DETACHED}</detachedfilename>
      <useDetachedSignature>Y</useDetachedSignature>
      <publickeyfile>${PUB_KEY}</publickeyfile>
    </entry>
  </entries>
</job>
"""


class TestPgpParser(unittest.TestCase):
    def test_parses_all_pgp_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pgp.kjb"
            path.write_text(_PGP_KJB, encoding="utf-8")
            job = parse_job(path)
        by_type = {e.entry_type: e for e in job.entries if e.entry_type != "SPECIAL"}
        self.assertIn("PGP_ENCRYPT_FILES", by_type)
        self.assertIn("PGP_DECRYPT_FILES", by_type)
        self.assertIn("PGP_VERIFY_FILES", by_type)

        enc = by_type["PGP_ENCRYPT_FILES"].attributes
        self.assertEqual(enc["asciiMode"], "Y")
        self.assertEqual(enc["custom_keep"], "Y")
        self.assertEqual(enc["publickeyfile"], "${PUB_KEY}")
        self.assertEqual(enc["fields"][0]["userid"], "${RECIPIENT}")
        self.assertEqual(enc["fields"][0]["action_type"], "encrypt")

        dec = by_type["PGP_DECRYPT_FILES"].attributes
        self.assertEqual(dec["fields"][0]["passphrase"], "${PASSPHRASE}")
        self.assertEqual(dec["privatekeyfile"], "${PRIV_KEY}")

        ver = by_type["PGP_VERIFY_FILES"].attributes
        self.assertEqual(ver["useDetachedSignature"], "Y")
        self.assertEqual(ver["filename"], "${SIGNED}")


class TestPgpEncrypt(unittest.TestCase):
    def test_missing_gnupg(self) -> None:
        with patch.object(pops, "_import_gnupg", side_effect=ImportError("no gnupg")):
            outcome = pops.encrypt_files(
                [{"source_filefolder": "/x", "userid": "a@b", "destination_filefolder": "/y"}]
            )
        self.assertFalse(outcome.success)
        self.assertIn("gnupg", str(outcome.error).lower())

    def test_encrypt_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            out = Path(tmp) / "out"
            src.mkdir()
            out.mkdir()
            (src / "a.txt").write_text("hello", encoding="utf-8")

            gpg = MagicMock()
            result = MagicMock()
            result.ok = True

            def _encrypt(data, recipients, armor=True, always_trust=True, sign=None, output=None):
                Path(output).write_text("-----BEGIN PGP MESSAGE-----\n", encoding="utf-8")
                return result

            gpg.encrypt.side_effect = _encrypt
            gpg.import_keys.return_value = MagicMock(count=1)

            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.encrypt_files(
                    [
                        {
                            "source_filefolder": str(src),
                            "destination_filefolder": str(out),
                            "userid": "recv@example.com",
                            "wildcard": r".*\.txt$",
                            "action_type": "encrypt",
                        }
                    ],
                    ascii_mode=True,
                    public_key_file="",
                    add_result_filesname=True,
                )
            self.assertTrue(outcome.success, outcome.message)
            self.assertTrue(outcome.paths)
            self.assertTrue(Path(outcome.paths[0]).exists())

    def test_encrypt_no_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gpg = MagicMock()
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.encrypt_files(
                    [
                        {
                            "source_filefolder": str(tmp),
                            "destination_filefolder": str(tmp),
                            "userid": "a@b",
                            "wildcard": r".*\.missing$",
                        }
                    ]
                )
            self.assertFalse(outcome.success)

    def test_handler_encrypt_variables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src"
            out = Path(tmp) / "out"
            src.mkdir()
            out.mkdir()
            (src / "a.txt").write_text("x", encoding="utf-8")
            gpg = MagicMock()
            result = MagicMock(ok=True)

            def _encrypt(*args, **kwargs):
                Path(kwargs["output"]).write_bytes(b"enc")
                return result

            gpg.encrypt.side_effect = _encrypt
            rt = _runtime(
                variables={
                    "SRC": str(src),
                    "OUT": str(out),
                    "RECIPIENT": "recv@example.com",
                }
            )
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                res = handle_pgp_encrypt_files(
                    rt,
                    JobEntry(
                        name="Encrypt",
                        entry_type="PGP_ENCRYPT_FILES",
                        attributes={
                            "asciiMode": "Y",
                            "add_result_filesname": "Y",
                            "create_destination_folder": "Y",
                            "fields": [
                                {
                                    "source_filefolder": "${SRC}",
                                    "destination_filefolder": "${OUT}",
                                    "userid": "${RECIPIENT}",
                                    "wildcard": r".*\.txt$",
                                    "action_type": "encrypt",
                                }
                            ],
                        },
                    ),
                )
            self.assertTrue(res.success)
            self.assertTrue(rt.result_filenames)


class TestPgpDecrypt(unittest.TestCase):
    def test_decrypt_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            enc_dir = Path(tmp) / "enc"
            dec_dir = Path(tmp) / "dec"
            enc_dir.mkdir()
            dec_dir.mkdir()
            (enc_dir / "a.txt.gpg").write_bytes(b"cipher")

            gpg = MagicMock()
            result = MagicMock(ok=True)

            def _decrypt(blob, passphrase=None, output=None):
                Path(output).write_text("plain", encoding="utf-8")
                return result

            gpg.decrypt.side_effect = _decrypt
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.decrypt_files(
                    [
                        {
                            "source_filefolder": str(enc_dir),
                            "destination_filefolder": str(dec_dir),
                            "passphrase": "secret",
                            "wildcard": r".*\.gpg$",
                        }
                    ]
                )
            self.assertTrue(outcome.success, outcome.message)
            self.assertTrue((dec_dir / "a.txt").exists())

    def test_decrypt_bad_passphrase(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            enc_dir = Path(tmp) / "enc"
            enc_dir.mkdir()
            (enc_dir / "a.gpg").write_bytes(b"cipher")
            gpg = MagicMock()
            result = MagicMock(ok=False, status="bad passphrase")
            gpg.decrypt.return_value = result
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.decrypt_files(
                    [
                        {
                            "source_filefolder": str(enc_dir),
                            "destination_filefolder": str(tmp),
                            "passphrase": "wrong",
                            "wildcard": ".*",
                        }
                    ]
                )
            self.assertFalse(outcome.success)

    def test_decrypt_unresolved_passphrase_var(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gpg = MagicMock()
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.decrypt_files(
                    [
                        {
                            "source_filefolder": str(tmp),
                            "destination_filefolder": str(tmp),
                            "passphrase": "${MISSING}",
                            "wildcard": ".*",
                        }
                    ]
                )
            self.assertFalse(outcome.success)

    def test_handler_decrypt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            enc_dir = Path(tmp) / "enc"
            dec_dir = Path(tmp) / "dec"
            enc_dir.mkdir()
            dec_dir.mkdir()
            (enc_dir / "f.gpg").write_bytes(b"c")
            gpg = MagicMock()
            result = MagicMock(ok=True)

            def _decrypt(blob, passphrase=None, output=None):
                Path(output).write_text("ok", encoding="utf-8")
                return result

            gpg.decrypt.side_effect = _decrypt
            rt = _runtime(variables={"PASSPHRASE": "pw", "ENC": str(enc_dir), "DEC": str(dec_dir)})
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                res = handle_pgp_decrypt_files(
                    rt,
                    JobEntry(
                        name="Decrypt",
                        entry_type="PGP_DECRYPT_FILES",
                        attributes={
                            "add_result_filesname": "Y",
                            "fields": [
                                {
                                    "source_filefolder": "${ENC}",
                                    "destination_filefolder": "${DEC}",
                                    "passphrase": "${PASSPHRASE}",
                                    "wildcard": ".*",
                                }
                            ],
                        },
                    ),
                )
            self.assertTrue(res.success)


class TestPgpVerify(unittest.TestCase):
    def test_verify_missing_file(self) -> None:
        outcome = pops.verify_signature(filename="/no/such/file")
        self.assertFalse(outcome.success)

    def test_verify_embedded_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signed = Path(tmp) / "msg.asc"
            signed.write_text("signed", encoding="utf-8")
            gpg = MagicMock()
            result = MagicMock(
                valid=True,
                username="Alice",
                key_id="ABC",
                fingerprint="FP",
                status="signature valid",
            )
            gpg.verify_file.return_value = result
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.verify_signature(filename=str(signed))
            self.assertTrue(outcome.success)
            self.assertTrue(outcome.extra.get("valid"))

    def test_verify_invalid_signature(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signed = Path(tmp) / "msg.asc"
            signed.write_text("signed", encoding="utf-8")
            gpg = MagicMock()
            result = MagicMock(valid=False, status="BAD signature")
            gpg.verify_file.return_value = result
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.verify_signature(filename=str(signed))
            self.assertFalse(outcome.success)

    def test_verify_detached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "file.bin"
            sig = Path(tmp) / "file.bin.sig"
            data.write_bytes(b"data")
            sig.write_bytes(b"sig")
            gpg = MagicMock()
            result = MagicMock(valid=True, status="signature valid", key_id="1")
            gpg.verify_data.return_value = result
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.verify_signature(
                    filename=str(data),
                    detached_filename=str(sig),
                    use_detached_signature=True,
                )
            self.assertTrue(outcome.success)

    def test_verify_detached_missing_sig(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "file.bin"
            data.write_bytes(b"data")
            gpg = MagicMock()
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                outcome = pops.verify_signature(
                    filename=str(data),
                    detached_filename=str(Path(tmp) / "missing.sig"),
                    use_detached_signature=True,
                )
            self.assertFalse(outcome.success)

    def test_handler_verify(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            signed = Path(tmp) / "s.asc"
            signed.write_text("x", encoding="utf-8")
            gpg = MagicMock()
            gpg.verify_file.return_value = MagicMock(valid=True, status="ok")
            rt = _runtime(variables={"SIGNED": str(signed)})
            with patch.object(pops, "_make_gpg", return_value=(gpg, [])):
                res = handle_pgp_verify_files(
                    rt,
                    JobEntry(
                        name="Verify",
                        entry_type="PGP_VERIFY_FILES",
                        attributes={
                            "filename": "${SIGNED}",
                            "useDetachedSignature": "N",
                        },
                    ),
                )
            self.assertTrue(res.success)


class TestRegistration(unittest.TestCase):
    def test_handlers_registered(self) -> None:
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types=set(),
            trans_runners={},
            child_job_modules={},
        )
        for key in ("PGP_ENCRYPT_FILES", "PGP_DECRYPT_FILES", "PGP_VERIFY_FILES"):
            self.assertIn(key, handlers)


if __name__ == "__main__":
    unittest.main()
