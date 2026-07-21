"""Unit tests for Mail-category Pentaho Job Entries.

Covers: MAIL, GET_POP (Get Mails POP3/IMAP), MAIL_VALIDATOR —
parser coverage, variable substitution, success/failure hops,
SMTP/auth failures, and generated-config helpers.
"""

from __future__ import annotations

import tempfile
import unittest
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import MagicMock, patch

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_get_pop,
    handle_mail,
    handle_mail_validator,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry, JobHop
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime
from pentaho_converter.runtime_templates.engine.mail_ops import (
    get_mails,
    get_mails_config_from_attributes,
    mail_config_from_attributes,
    send_smtp_mail,
    validate_email_address,
    validate_email_addresses,
)


def _runtime(
    entries: list[JobEntry],
    hops: list[JobHop] | None = None,
    *,
    variables: dict | None = None,
    parameters: dict | None = None,
) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=None,
        cfg={},
        entry_types={e.entry_type.upper() for e in entries},
        trans_runners={},
        child_job_modules={},
    )
    rt = JobRuntime(
        name="MailTestJob",
        entries=entries,
        hops=hops or [],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )
    if parameters:
        rt.parameters = dict(parameters)
    return rt


_MAIL_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>MailSample</name>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>Send Notice</name>
      <type>MAIL</type>
      <server>${SMTP_HOST}</server>
      <port>587</port>
      <destination>${MAIL_TO}</destination>
      <destinationCc>cc@example.com</destinationCc>
      <destinationBCc>bcc@example.com</destinationBCc>
      <replyto>from@example.com</replyto>
      <replytoname>ETL Bot</replytoname>
      <replyToAddresses>reply@example.com</replyToAddresses>
      <subject>Hello ${USER}</subject>
      <comment>Body for ${USER}</comment>
      <use_HTML>Y</use_HTML>
      <encoding>UTF-8</encoding>
      <use_auth>Y</use_auth>
      <auth_user>smtp_user</auth_user>
      <auth_password>${SMTP_PASS}</auth_password>
      <use_secure_auth>Y</use_secure_auth>
      <secureconnectiontype>TLS</secureconnectiontype>
      <use_Priority>Y</use_Priority>
      <priority>high</priority>
      <importance>high</importance>
      <sensitivity>normal</sensitivity>
      <include_files>N</include_files>
      <zip_files>N</zip_files>
      <zip_name></zip_name>
      <only_comment>N</only_comment>
      <include_date>N</include_date>
      <contact_person>Ops</contact_person>
      <contact_phone>555</contact_phone>
      <filetypes>
        <filetype>GENERAL</filetype>
        <filetype>LOG</filetype>
      </filetypes>
      <embeddedimages>
        <embeddedimage>
          <image_name>logo.png</image_name>
          <content_id>cid_logo</content_id>
        </embeddedimage>
      </embeddedimages>
    </entry>
    <entry>
      <name>Get Inbox</name>
      <type>GET_POP</type>
      <servername>${MAIL_HOST}</servername>
      <username>${MAIL_USER}</username>
      <password>${MAIL_PASS}</password>
      <usessl>Y</usessl>
      <sslport>993</sslport>
      <protocol>IMAP</protocol>
      <outputdirectory>${OUT_DIR}</outputdirectory>
      <filenamepattern>${subject}_${nr}.eml</filenamepattern>
      <retrievemails>0</retrievemails>
      <firstmails>0</firstmails>
      <delete>N</delete>
      <savemessage>Y</savemessage>
      <saveattachment>Y</saveattachment>
      <usedifferentfolderforattachment>N</usedifferentfolderforattachment>
      <attachmentfolder></attachmentfolder>
      <attachmentwildcard>*.pdf</attachmentwildcard>
      <valueimaplist>unread</valueimaplist>
      <imapfirstmails>0</imapfirstmails>
      <imapfolder>INBOX</imapfolder>
      <sendersearch>alerts@</sendersearch>
      <nottermsendersearch>N</nottermsendersearch>
      <receipientsearch></receipientsearch>
      <nottermreceipientsearch>N</nottermreceipientsearch>
      <subjectsearch>FAIL</subjectsearch>
      <nottermsubjectsearch>N</nottermsubjectsearch>
      <bodysearch></bodysearch>
      <nottermbodysearch>N</nottermbodysearch>
      <conditionreceiveddate></conditionreceiveddate>
      <nottermreceiveddatesearch>N</nottermreceiveddatesearch>
      <receiveddate1></receiveddate1>
      <receiveddate2></receiveddate2>
      <actiontype>get</actiontype>
      <movetoimapfolder></movetoimapfolder>
      <createmovetofolder>N</createmovetofolder>
      <createlocalfolder>Y</createlocalfolder>
      <aftergetimap>nothing</aftergetimap>
      <includesubfolders>N</includesubfolders>
      <useproxy>N</useproxy>
      <proxyusername></proxyusername>
    </entry>
    <entry>
      <name>Check Address</name>
      <type>MAIL_VALIDATOR</type>
      <emailAddress>${CHECK_EMAIL}</emailAddress>
      <smtpCheck>N</smtpCheck>
      <timeout>0</timeout>
      <defaultSMTP></defaultSMTP>
      <emailSender>noreply@example.com</emailSender>
    </entry>
  </entries>
  <hops>
    <hop><from>Start</from><to>Send Notice</to><enabled>Y</enabled><unconditional>Y</unconditional></hop>
  </hops>
</job>
"""


class TestMailParserCoverage(unittest.TestCase):
    def test_parses_mail_get_pop_validator_attributes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "mail_sample.kjb"
            path.write_text(_MAIL_KJB, encoding="utf-8")
            job = parse_job(path)

        by_name = {e.name: e for e in job.entries}
        mail = by_name["Send Notice"]
        self.assertEqual(mail.entry_type, "MAIL")
        self.assertEqual(mail.attributes["server"], "${SMTP_HOST}")
        self.assertEqual(mail.attributes["destinationCc"], "cc@example.com")
        self.assertEqual(mail.attributes["destinationBCc"], "bcc@example.com")
        self.assertEqual(mail.attributes["replyToAddresses"], "reply@example.com")
        self.assertEqual(mail.attributes["use_secure_auth"], "Y")
        self.assertEqual(mail.attributes["filetypes"], ["GENERAL", "LOG"])
        self.assertEqual(
            mail.attributes["embeddedimages"],
            [{"image_name": "logo.png", "content_id": "cid_logo"}],
        )

        get_pop = by_name["Get Inbox"]
        self.assertEqual(get_pop.entry_type, "GET_POP")
        self.assertEqual(get_pop.attributes["protocol"], "IMAP")
        self.assertEqual(get_pop.attributes["sslport"], "993")
        self.assertEqual(get_pop.attributes["valueimaplist"], "unread")
        self.assertEqual(get_pop.attributes["sendersearch"], "alerts@")

        validator = by_name["Check Address"]
        self.assertEqual(validator.entry_type, "MAIL_VALIDATOR")
        self.assertEqual(validator.attributes["emailAddress"], "${CHECK_EMAIL}")
        self.assertEqual(validator.attributes["smtpCheck"], "N")


class TestMailConfigHelpers(unittest.TestCase):
    def test_mail_config_from_attributes(self):
        cfg = mail_config_from_attributes(
            {
                "server": "smtp.example.com",
                "port": "465",
                "destination": "a@x.com, b@x.com",
                "destinationCc": "c@x.com",
                "sender_address": "from@x.com",
                "sender_name": "Bot",
                "use_HTML": "Y",
                "use_auth": "Y",
                "use_secure_auth": "Y",
                "secureconnectiontype": "SSL",
            }
        )
        self.assertEqual(cfg.server, "smtp.example.com")
        self.assertEqual(cfg.port, 465)
        self.assertEqual(cfg.to, ["a@x.com", "b@x.com"])
        self.assertEqual(cfg.cc, ["c@x.com"])
        self.assertEqual(cfg.from_address, "from@x.com")
        self.assertTrue(cfg.html)
        self.assertEqual(cfg.secure_type, "SSL")

    def test_get_mails_config_from_attributes(self):
        cfg = get_mails_config_from_attributes(
            {
                "servername": "mail.example.com",
                "protocol": "POP3",
                "usessl": "Y",
                "sslport": "995",
                "retrievemails": "2",
                "firstmails": "5",
                "delete": "Y",
            }
        )
        self.assertEqual(cfg.protocol, "POP3")
        self.assertTrue(cfg.use_ssl)
        self.assertEqual(cfg.ssl_port, 995)
        self.assertEqual(cfg.retrieve_mails, 2)
        self.assertEqual(cfg.first_mails, 5)
        self.assertTrue(cfg.delete)


class TestMailValidator(unittest.TestCase):
    def test_syntax_success(self):
        outcome = validate_email_address("user@example.com")
        self.assertTrue(outcome.valid)

    def test_syntax_failure(self):
        outcome = validate_email_address("not-an-email")
        self.assertFalse(outcome.valid)

    def test_space_separated_fail_fast(self):
        outcome = validate_email_addresses("ok@example.com bad")
        self.assertFalse(outcome.valid)

    def test_smtp_check_warns(self):
        outcome = validate_email_address(
            "user@example.com", smtp_check=True, sender="a@b.com"
        )
        self.assertTrue(outcome.valid)
        self.assertTrue(any("smtpCheck" in w for w in outcome.warnings))

    def test_handler_success_and_variable_substitution(self):
        entry = JobEntry(
            name="Check",
            entry_type="MAIL_VALIDATOR",
            attributes={"emailAddress": "${CHECK_EMAIL}", "smtpCheck": "N"},
        )
        rt = _runtime([entry], variables={"CHECK_EMAIL": "ok@example.com"})
        result = handle_mail_validator(rt, entry)
        self.assertTrue(result.success)

    def test_handler_empty_address_fails(self):
        entry = JobEntry(
            name="Check",
            entry_type="MAIL_VALIDATOR",
            attributes={"emailAddress": ""},
        )
        result = handle_mail_validator(_runtime([entry]), entry)
        self.assertFalse(result.success)

    def test_handler_invalid_config_smtp_without_sender(self):
        entry = JobEntry(
            name="Check",
            entry_type="MAIL_VALIDATOR",
            attributes={
                "emailAddress": "ok@example.com",
                "smtpCheck": "Y",
                "emailSender": "",
            },
        )
        result = handle_mail_validator(_runtime([entry]), entry)
        self.assertFalse(result.success)

    def test_success_and_failure_hops(self):
        start = JobEntry(name="Start", entry_type="SPECIAL", is_start=True)
        check = JobEntry(
            name="Check",
            entry_type="MAIL_VALIDATOR",
            attributes={"emailAddress": "bad", "smtpCheck": "N"},
        )
        ok = JobEntry(name="OK", entry_type="SUCCESS")
        bad = JobEntry(name="Bad", entry_type="SUCCESS")
        rt = _runtime(
            [start, check, ok, bad],
            [
                JobHop("Start", "Check", unconditional=True),
                JobHop("Check", "OK", unconditional=False, evaluation=True),
                JobHop("Check", "Bad", unconditional=False, evaluation=False),
            ],
        )
        result = rt.run()
        self.assertTrue(result.success)
        self.assertIn("Bad", rt.executed)
        self.assertNotIn("OK", rt.executed)


class TestMailSend(unittest.TestCase):
    def test_skipped_when_mail_enabled_n(self):
        entry = JobEntry(
            name="Send",
            entry_type="MAIL",
            attributes={
                "server": "localhost",
                "destination": "a@b.com",
                "replyto": "from@b.com",
                "subject": "Hi",
            },
        )
        rt = _runtime([entry], variables={"MAIL_ENABLED": "N"})
        result = handle_mail(rt, entry)
        self.assertTrue(result.success)
        self.assertFalse(result.result["sent"])
        self.assertTrue(result.result["skipped"])

    def test_variable_substitution_and_send(self):
        entry = JobEntry(
            name="Send",
            entry_type="MAIL",
            attributes={
                "server": "${SMTP_HOST}",
                "port": "587",
                "destination": "${MAIL_TO}",
                "replyto": "from@example.com",
                "subject": "Hi ${USER}",
                "comment": "Body ${USER}",
                "use_auth": "Y",
                "auth_user": "u",
                "auth_password": "${SMTP_PASS}",
                "use_secure_auth": "Y",
                "secureconnectiontype": "TLS",
            },
        )
        client = MagicMock()
        client.send_message.return_value = {}
        factory = MagicMock(return_value=client)

        rt = _runtime(
            [entry],
            variables={
                "SMTP_HOST": "smtp.example.com",
                "MAIL_TO": "to@example.com",
                "USER": "Alice",
                "SMTP_PASS": "secret",
                "MAIL_ENABLED": "Y",
            },
        )
        from pentaho_converter.runtime_templates.engine.handlers import (
            _resolve_mail_attrs,
        )
        from pentaho_converter.runtime_templates.engine.mail_ops import (
            mail_config_from_attributes as _cfg,
        )

        attrs = _resolve_mail_attrs(entry.attributes, rt)
        cfg = _cfg(attrs)
        send_result = send_smtp_mail(cfg, smtp_factory=factory)
        self.assertTrue(send_result.sent)
        factory.assert_called()
        client.ehlo.assert_called()
        client.starttls.assert_called()
        client.login.assert_called_with("u", "secret")
        client.send_message.assert_called()

        # Handler path with mocked send_smtp_mail
        with patch(
            "pentaho_converter.runtime_templates.engine.handlers.send_smtp_mail"
        ) as mocked:
            mocked.return_value = MagicMock(
                sent=True, recipients=["to@example.com"], warnings=[], message_id=""
            )
            result = handle_mail(rt, entry)
            self.assertTrue(result.success)
            self.assertTrue(result.result["sent"])
            cfg_arg = mocked.call_args[0][0]
            self.assertEqual(cfg_arg.server, "smtp.example.com")
            self.assertEqual(cfg_arg.to, ["to@example.com"])
            self.assertEqual(cfg_arg.subject, "Hi Alice")

    def test_auth_failure(self):
        entry = JobEntry(
            name="Send",
            entry_type="MAIL",
            attributes={
                "server": "smtp.example.com",
                "destination": "to@example.com",
                "replyto": "from@example.com",
                "subject": "x",
                "use_auth": "Y",
                "auth_user": "u",
                "auth_password": "bad",
            },
        )
        client = MagicMock()
        client.login.side_effect = Exception("535 Authentication failed")
        factory = MagicMock(return_value=client)
        rt = _runtime([entry], variables={"MAIL_ENABLED": "Y"})
        with patch(
            "pentaho_converter.runtime_templates.engine.handlers.send_smtp_mail",
            side_effect=Exception("535 Authentication failed"),
        ):
            result = handle_mail(rt, entry)
        self.assertFalse(result.success)
        self.assertIn("Authentication", str(result.error))

    def test_invalid_configuration_missing_server(self):
        entry = JobEntry(
            name="Send",
            entry_type="MAIL",
            attributes={
                "destination": "to@example.com",
                "replyto": "from@example.com",
            },
        )
        rt = _runtime([entry], variables={"MAIL_ENABLED": "Y"})
        result = handle_mail(rt, entry)
        self.assertFalse(result.success)

    def test_encrypted_password_warning(self):
        from pentaho_converter.runtime_templates.engine.mail_ops import MailSendConfig

        cfg = MailSendConfig(
            server="smtp.example.com",
            port=25,
            to=["to@example.com"],
            from_address="from@example.com",
            use_auth=True,
            auth_user="u",
            auth_password="Encrypted abc",
        )
        client = MagicMock()
        client.send_message.return_value = {}
        factory = MagicMock(return_value=client)
        result = send_smtp_mail(cfg, smtp_factory=factory)
        self.assertTrue(any("PDI-encrypted" in w for w in result.warnings))


class TestGetPop(unittest.TestCase):
    def _eml_bytes(self, subject: str = "Hello", sender: str = "a@b.com") -> bytes:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = "me@example.com"
        msg.set_content("body text")
        return msg.as_bytes()

    def test_pop3_retrieve_and_save(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            raw = self._eml_bytes(subject="Alert FAIL", sender="alerts@x.com")
            conn = MagicMock()
            conn.list.return_value = (b"+OK", [b"1 100"], 0)
            conn.retr.return_value = (b"+OK", raw.split(b"\r\n"), len(raw))
            factory = MagicMock(return_value=conn)

            cfg = get_mails_config_from_attributes(
                {
                    "servername": "pop.example.com",
                    "username": "u",
                    "password": "p",
                    "protocol": "POP3",
                    "usessl": "N",
                    "outputdirectory": str(out),
                    "savemessage": "Y",
                    "saveattachment": "N",
                    "createlocalfolder": "Y",
                    "retrievemails": "0",
                    "sendersearch": "alerts@",
                    "subjectsearch": "FAIL",
                }
            )
            result = get_mails(cfg, pop_factory=factory)
            self.assertEqual(result.retrieved, 1)
            self.assertEqual(len(result.saved_messages), 1)
            self.assertTrue(Path(result.saved_messages[0]).is_file())

    def test_handler_auth_failure(self):
        entry = JobEntry(
            name="Get",
            entry_type="GET_POP",
            attributes={
                "servername": "pop.example.com",
                "username": "u",
                "password": "bad",
                "protocol": "POP3",
                "outputdirectory": "/tmp/out",
                "createlocalfolder": "Y",
            },
        )
        with patch(
            "pentaho_converter.runtime_templates.engine.handlers.get_mails",
            side_effect=Exception("Authentication failed"),
        ):
            result = handle_get_pop(_runtime([entry]), entry)
        self.assertFalse(result.success)

    def test_invalid_configuration_missing_server(self):
        entry = JobEntry(
            name="Get",
            entry_type="GET_POP",
            attributes={
                "username": "u",
                "password": "p",
                "protocol": "POP3",
                "outputdirectory": "/tmp/out",
            },
        )
        result = handle_get_pop(_runtime([entry]), entry)
        self.assertFalse(result.success)

    def test_variable_substitution(self):
        with tempfile.TemporaryDirectory() as tmp:
            entry = JobEntry(
                name="Get",
                entry_type="GET_POP",
                attributes={
                    "servername": "${MAIL_HOST}",
                    "username": "${MAIL_USER}",
                    "password": "p",
                    "protocol": "POP3",
                    "outputdirectory": "${OUT_DIR}",
                    "createlocalfolder": "Y",
                    "savemessage": "Y",
                },
            )
            rt = _runtime(
                [entry],
                variables={
                    "MAIL_HOST": "pop.example.com",
                    "MAIL_USER": "alice",
                    "OUT_DIR": str(Path(tmp) / "mail_out"),
                },
            )
            with patch(
                "pentaho_converter.runtime_templates.engine.handlers.get_mails"
            ) as mocked:
                mocked.return_value = MagicMock(
                    retrieved=0,
                    deleted=0,
                    saved_messages=[],
                    saved_attachments=[],
                    warnings=[],
                )
                result = handle_get_pop(rt, entry)
                self.assertTrue(result.success)
                cfg = mocked.call_args[0][0]
                self.assertEqual(cfg.server, "pop.example.com")
                self.assertEqual(cfg.username, "alice")
                self.assertIn("mail_out", cfg.output_directory)

    def test_proxy_emits_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            conn = MagicMock()
            conn.list.return_value = (b"+OK", [], 0)
            factory = MagicMock(return_value=conn)
            cfg = get_mails_config_from_attributes(
                {
                    "servername": "pop.example.com",
                    "username": "u",
                    "password": "p",
                    "protocol": "POP3",
                    "outputdirectory": str(out),
                    "useproxy": "Y",
                    "proxyusername": "proxy",
                    "createlocalfolder": "Y",
                    "savemessage": "N",
                    "saveattachment": "N",
                }
            )
            # action get with save off still needs out dir created — allow empty retrieve
            cfg.save_message = False
            cfg.save_attachment = False
            result = get_mails(cfg, pop_factory=factory)
            self.assertTrue(any("useproxy" in w for w in result.warnings))


class TestMailRegistration(unittest.TestCase):
    def test_build_handlers_registers_mail_types(self):
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types={"MAIL", "GET_POP", "MAIL_VALIDATOR"},
            trans_runners={},
            child_job_modules={},
        )
        self.assertIs(handlers["MAIL"], handle_mail)
        self.assertIs(handlers["GET_POP"], handle_get_pop)
        self.assertIs(handlers["MAIL_VALIDATOR"], handle_mail_validator)

    def test_end_to_end_mail_skip_flow(self):
        start = JobEntry(name="Start", entry_type="SPECIAL", is_start=True)
        mail = JobEntry(
            name="Notify",
            entry_type="MAIL",
            attributes={
                "server": "localhost",
                "destination": "a@b.com",
                "replyto": "from@b.com",
                "subject": "x",
            },
        )
        success = JobEntry(name="Success", entry_type="SUCCESS")
        rt = _runtime(
            [start, mail, success],
            [
                JobHop("Start", "Notify", unconditional=True),
                JobHop("Notify", "Success", unconditional=False, evaluation=True),
            ],
            variables={"MAIL_ENABLED": "N"},
        )
        result = rt.run()
        self.assertTrue(result.success)
        self.assertEqual(rt.executed, ["Start", "Notify", "Success"])


if __name__ == "__main__":
    unittest.main()
