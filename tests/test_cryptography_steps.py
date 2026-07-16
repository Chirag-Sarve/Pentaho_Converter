"""Tests for Cryptography transformation migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_pgp_decrypt_stream_config,
    parse_pgp_encrypt_stream_config,
    parse_secret_key_generator_config,
    parse_step_metadata,
    parse_symmetric_crypto_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="Trans", file_path=Path("t.ktr"))
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Input", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {l}" for l in lines))
        return True
    except SyntaxError:
        return False


class TestCryptographyParsers(unittest.TestCase):
    def test_pgp_encrypt_parse(self):
        xml = """<step>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <keyname>alice@example.com</keyname>
          <keynameInField>N</keynameInField>
          <streamfield>payload</streamfield>
          <resultfieldname>encrypted</resultfieldname>
          <asciiarmor>Y</asciiarmor>
          <compression>ZIP</compression>
          <integritycheck>Y</integritycheck>
          <keyring>/keys/pubring.gpg</keyring>
        </step>"""
        cfg = parse_pgp_encrypt_stream_config(ET.fromstring(xml))
        self.assertEqual(cfg["gpg_location"], "/usr/bin/gpg")
        self.assertEqual(cfg["key_name"], "alice@example.com")
        self.assertEqual(cfg["stream_field"], "payload")
        self.assertEqual(cfg["result_field"], "encrypted")
        self.assertTrue(cfg["ascii_armor"])
        self.assertEqual(cfg["compression"], "ZIP")
        self.assertTrue(cfg["integrity_check"])
        self.assertEqual(cfg["keyring_path"], "/keys/pubring.gpg")

    def test_pgp_decrypt_redacts_passphrase(self):
        xml = """<step>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <passhrase>super-secret-pass</passhrase>
          <streamfield>cipher_text</streamfield>
          <resultfieldname>plain</resultfieldname>
          <passphraseFromField>N</passphraseFromField>
        </step>"""
        cfg = parse_pgp_decrypt_stream_config(ET.fromstring(xml))
        self.assertTrue(cfg["passphrase_configured"])
        self.assertIn("dbutils.secrets.get", cfg["passphrase_secret_ref"])
        self.assertNotIn("super-secret-pass", str(cfg))
        self.assertEqual(cfg["extras"].get("passhrase"), "<redacted>")

    def test_secret_key_generator_parse(self):
        xml = """<step>
          <fields>
            <field>
              <algorithm>AES</algorithm>
              <scheme>AES</scheme>
              <secretKeyLen>256</secretKeyLen>
              <secretKeyCount>3</secretKeyCount>
            </field>
            <field>
              <algorithm>DESede</algorithm>
              <scheme>DESede</scheme>
              <secretKeyLen>168</secretKeyLen>
              <secretKeyCount>1</secretKeyCount>
            </field>
          </fields>
          <secretKeyFieldName>secretKey</secretKeyFieldName>
          <secretKeyLengthFieldName>keyLen</secretKeyLengthFieldName>
          <algorithmFieldName>algo</algorithmFieldName>
          <outputKeyInBinary>N</outputKeyInBinary>
        </step>"""
        cfg = parse_secret_key_generator_config(ET.fromstring(xml))
        self.assertEqual(len(cfg["keys"]), 2)
        self.assertEqual(cfg["keys"][0]["algorithm"], "AES")
        self.assertEqual(cfg["keys"][0]["key_length"], 256)
        self.assertEqual(cfg["keys"][0]["key_count"], 3)
        self.assertEqual(cfg["encoding"], "hex")
        self.assertEqual(cfg["secret_key_field"], "secretKey")

    def test_symmetric_crypto_parse_redacts_key(self):
        xml = """<step>
          <operation_type>encrypt</operation_type>
          <algorithm>AES</algorithm>
          <schema>AES/CBC/PKCS5Padding</schema>
          <messageField>msg</messageField>
          <resultfieldname>cipher</resultfieldname>
          <secretKey>0123456789abcdef0123456789abcdef</secretKey>
          <secretKeyInField>N</secretKeyInField>
          <readKeyAsBinary>N</readKeyAsBinary>
          <outputResultAsBinary>N</outputResultAsBinary>
          <iv>00112233445566778899aabbccddeeff</iv>
        </step>"""
        cfg = parse_symmetric_crypto_config(ET.fromstring(xml))
        self.assertEqual(cfg["operation_type"], "encrypt")
        self.assertEqual(cfg["algorithm"], "AES")
        self.assertEqual(cfg["cipher_mode"], "CBC")
        self.assertEqual(cfg["padding"], "PKCS5Padding")
        self.assertTrue(cfg["secret_key_configured"])
        self.assertNotIn("0123456789abcdef", str(cfg.get("secret_key_secret_ref")))
        self.assertNotIn("0123456789abcdef0123456789abcdef", str(cfg))
        self.assertEqual(cfg["extras"].get("secretKey"), "<redacted>")
        self.assertTrue(cfg["iv_configured"])
        self.assertEqual(cfg["extras"].get("iv"), "<redacted>")

    def test_parse_step_metadata_dispatcher(self):
        xml = """<step>
          <gpglocation>/gpg</gpglocation>
          <streamfield>s</streamfield>
          <resultfieldname>r</resultfieldname>
          <keyname>k</keyname>
        </step>"""
        cfg = parse_step_metadata(ET.fromstring(xml), "PGPEncryptStream")
        self.assertEqual(cfg["stream_field"], "s")


class TestCryptographyCodegen(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_pgp_encrypt_codegen(self):
        xml = """<step>
          <name>Enc</name><type>PGPEncryptStream</type>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <keyname>bob@example.com</keyname>
          <streamfield>payload</streamfield>
          <resultfieldname>encrypted</resultfieldname>
          <asciiarmor>Y</asciiarmor>
        </step>"""
        outcome = self.registry.convert_step(
            "PGPEncryptStream", _ctx(xml, "PGPEncryptStream", "Enc")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("gnupg", code)
        self.assertIn("encrypted", code)
        self.assertIn("payload", code)
        self.assertIn("preserved.key_name", code)
        self.assertIn("Databricks Secrets", code)
        self.assertNotIn("bob-secret", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertIn(outcome.status, ("partial", "converted", "manual_required"))

    def test_pgp_decrypt_secrets_never_emitted(self):
        xml = """<step>
          <name>Dec</name><type>PGPDecryptStream</type>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <passhrase>DO_NOT_EMIT_THIS_PASSPHRASE</passhrase>
          <streamfield>cipher_text</streamfield>
          <resultfieldname>plain</resultfieldname>
        </step>"""
        outcome = self.registry.convert_step(
            "PGPDecryptStream", _ctx(xml, "PGPDecryptStream", "Dec")
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("DO_NOT_EMIT_THIS_PASSPHRASE", code)
        self.assertIn("dbutils.secrets.get", code)
        self.assertIn("gnupg", code)
        self.assertIn("plain", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_secret_key_generator_codegen(self):
        xml = """<step>
          <name>SKG</name><type>SecretKeyGenerator</type>
          <fields>
            <field>
              <algorithm>AES</algorithm>
              <scheme>AES</scheme>
              <secretKeyLen>128</secretKeyLen>
              <secretKeyCount>2</secretKeyCount>
            </field>
          </fields>
          <secretKeyFieldName>secretKey</secretKeyFieldName>
          <algorithmFieldName>algo</algorithmFieldName>
          <secretKeyLengthFieldName>keyLen</secretKeyLengthFieldName>
          <outputKeyInBinary>N</outputKeyInBinary>
        </step>"""
        outcome = self.registry.convert_step(
            "SecretKeyGenerator",
            _ctx(xml, "SecretKeyGenerator", "SKG", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("token_bytes", code)
        self.assertIn("createDataFrame", code)
        self.assertIn("secretKey", code)
        self.assertIn("AES", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")

    def test_symmetric_crypto_codegen_redacts_key(self):
        xml = """<step>
          <name>SC</name><type>SymmetricCryptoTrans</type>
          <operation_type>encrypt</operation_type>
          <algorithm>AES</algorithm>
          <schema>AES/ECB/PKCS5Padding</schema>
          <messageField>msg</messageField>
          <resultfieldname>cipher</resultfieldname>
          <secretKey>aabbccddeeff00112233445566778899</secretKey>
          <secretKeyInField>N</secretKeyInField>
        </step>"""
        outcome = self.registry.convert_step(
            "SymmetricCryptoTrans", _ctx(xml, "SymmetricCryptoTrans", "SC")
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("aabbccddeeff00112233445566778899", code)
        self.assertIn("dbutils.secrets.get", code)
        self.assertIn("cryptography", code)
        self.assertIn("Cipher", code)
        self.assertIn("cipher", code)
        self.assertIn("preserved.operation_type", code)
        self.assertIn("preserved.algorithm", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_symmetric_decrypt_key_in_field(self):
        xml = """<step>
          <name>SCD</name><type>SymmetricCryptoTrans</type>
          <operation_type>decrypt</operation_type>
          <algorithm>AES</algorithm>
          <schema>AES</schema>
          <messageField>cipher</messageField>
          <resultfieldname>msg</resultfieldname>
          <secretKeyInField>Y</secretKeyInField>
          <secretKeyField>key_col</secretKeyField>
        </step>"""
        outcome = self.registry.convert_step(
            "SymmetricCryptoTrans", _ctx(xml, "SymmetricCryptoTrans", "SCD")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("key_col", code)
        self.assertIn("do_encrypt = False", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_missing_stream_field_partial(self):
        xml = """<step>
          <name>Enc2</name><type>PGPEncryptStream</type>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <keyname>x</keyname>
          <resultfieldname>encrypted</resultfieldname>
        </step>"""
        outcome = self.registry.convert_step(
            "PGPEncryptStream", _ctx(xml, "PGPEncryptStream", "Enc2")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_handlers_registered(self):
        for st in (
            "PGPEncryptStream",
            "PGPDecryptStream",
            "SecretKeyGenerator",
            "SymmetricCryptoTrans",
        ):
            conv = self.registry.get_converter(st)
            self.assertIsNotNone(conv, msg=f"missing converter for {st}")

    def test_pgp_encrypt_armor_compression_integrity(self):
        xml = """<step>
          <name>EncA</name><type>PGPEncryptStream</type>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <keyname>alice@example.com</keyname>
          <streamfield>payload</streamfield>
          <resultfieldname>encrypted</resultfieldname>
          <asciiarmor>Y</asciiarmor>
          <compression>ZIP</compression>
          <integritycheck>Y</integritycheck>
          <keyring>/mnt/keys</keyring>
          <publickey>/mnt/keys/alice.asc</publickey>
        </step>"""
        outcome = self.registry.convert_step(
            "PGPEncryptStream", _ctx(xml, "PGPEncryptStream", "EncA")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("armor=True", code)
        self.assertIn("--compress-algo=ZIP", code)
        self.assertIn("--force-mdc", code)
        self.assertIn("gpgbinary", code)
        self.assertIn("/usr/bin/gpg", code)
        self.assertIn("gnupghome", code)
        self.assertIn("/mnt/keys", code)
        self.assertIn("scope='pgp'", code)
        self.assertIn("preserved.ascii_armor", code)
        self.assertIn("preserved.compression", code)
        self.assertIn("preserved.integrity_check", code)
        self.assertIn("preserved.public_key", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_pgp_encrypt_keyname_in_field(self):
        xml = """<step>
          <name>EncF</name><type>PGPEncryptStream</type>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <keynameInField>Y</keynameInField>
          <keynameFieldName>recipient</keynameFieldName>
          <streamfield>payload</streamfield>
          <resultfieldname>encrypted</resultfieldname>
        </step>"""
        outcome = self.registry.convert_step(
            "PGPEncryptStream", _ctx(xml, "PGPEncryptStream", "EncF")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("recipient", code)
        self.assertIn("col('recipient')", code)
        self.assertIn("col('payload')", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_pgp_decrypt_private_key_and_integrity(self):
        xml = """<step>
          <name>Dec2</name><type>PGPDecryptStream</type>
          <gpglocation>/usr/bin/gpg</gpglocation>
          <passhrase>SECRET_PASS</passhrase>
          <streamfield>cipher_text</streamfield>
          <resultfieldname>plain</resultfieldname>
          <privatekey>/mnt/secring.gpg</privatekey>
          <keyring>/mnt/gnupg</keyring>
          <integritycheck>Y</integritycheck>
        </step>"""
        outcome = self.registry.convert_step(
            "PGPDecryptStream", _ctx(xml, "PGPDecryptStream", "Dec2")
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("SECRET_PASS", code)
        self.assertIn("private_key", code)
        self.assertIn("Integrity", code)
        self.assertIn("gpgbinary", code)
        self.assertIn("/mnt/gnupg", code)
        self.assertIn("preserved.private_key", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_secret_key_binary_encoding_and_invalid_size(self):
        xml = """<step>
          <name>SKG2</name><type>SecretKeyGenerator</type>
          <fields>
            <field>
              <algorithm>AES</algorithm>
              <scheme>AES</scheme>
              <secretKeyLen>99</secretKeyLen>
              <secretKeyCount>1</secretKeyCount>
            </field>
          </fields>
          <secretKeyFieldName>secretKey</secretKeyFieldName>
          <outputKeyInBinary>Y</outputKeyInBinary>
        </step>"""
        outcome = self.registry.convert_step(
            "SecretKeyGenerator",
            _ctx(xml, "SecretKeyGenerator", "SKG2", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("BinaryType()", code)
        self.assertIn("invalid AES key size", code)
        self.assertIn("cryptography", code)
        self.assertIn("token_bytes", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))
        self.assertEqual(outcome.status, "converted")

    def test_symmetric_cbc_iv_and_mode_preserved(self):
        xml = """<step>
          <name>SCB</name><type>SymmetricCryptoTrans</type>
          <operation_type>encrypt</operation_type>
          <algorithm>AES</algorithm>
          <schema>AES/CBC/PKCS5Padding</schema>
          <messageField>msg</messageField>
          <resultfieldname>cipher</resultfieldname>
          <secretKey>deadbeefdeadbeefdeadbeefdeadbeef</secretKey>
          <iv>00112233445566778899aabbccddeeff</iv>
          <ivField>iv_col</ivField>
        </step>"""
        outcome = self.registry.convert_step(
            "SymmetricCryptoTrans", _ctx(xml, "SymmetricCryptoTrans", "SCB")
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("deadbeefdeadbeefdeadbeefdeadbeef", code)
        self.assertNotIn("00112233445566778899aabbccddeeff", code)
        self.assertIn("preserved.cipher_mode='CBC'", code)
        self.assertIn("modes.CBC", code)
        self.assertIn("iv_col", code)
        self.assertIn("scope='crypto'", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_symmetric_invalid_algorithm_warning(self):
        xml = """<step>
          <name>SCI</name><type>SymmetricCryptoTrans</type>
          <operation_type>encrypt</operation_type>
          <algorithm>RC4</algorithm>
          <schema>RC4</schema>
          <messageField>msg</messageField>
          <resultfieldname>cipher</resultfieldname>
          <secretKeyInField>Y</secretKeyInField>
          <secretKeyField>k</secretKeyField>
        </step>"""
        outcome = self.registry.convert_step(
            "SymmetricCryptoTrans", _ctx(xml, "SymmetricCryptoTrans", "SCI")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("unsupported algorithm", code.lower() + code)  # tolerate wording
        self.assertIn("RC4", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_validators_registered(self):
        from pentaho_converter.validation.step_validators import register_builtin_validators
        from pentaho_converter.validation.registry import get_validator

        register_builtin_validators()
        for st in (
            "PGPEncryptStream",
            "PGPDecryptStream",
            "SecretKeyGenerator",
            "SymmetricCryptoTrans",
        ):
            self.assertIsNotNone(get_validator(st), msg=f"missing validator for {st}")

    def test_no_placeholder_on_empty_input(self):
        xml = """<step>
          <name>SKG3</name><type>SecretKeyGenerator</type>
          <fields></fields>
          <secretKeyFieldName>secretKey</secretKeyFieldName>
        </step>"""
        outcome = self.registry.convert_step(
            "SecretKeyGenerator",
            _ctx(xml, "SecretKeyGenerator", "SKG3", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotIn("_placeholder", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
