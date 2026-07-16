"""Tests for Pentaho Validation step migration."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_credit_card_validator_config,
    parse_data_validator_config,
    parse_mail_validator_config,
    parse_step_metadata,
    parse_xsd_validator_config,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.validation_handlers import VALIDATION_HANDLERS
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(
    step_xml: str,
    step_type: str,
    step_name: str,
    *,
    with_input: bool = True,
    successors: list[str] | None = None,
) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="ValidationTrans", file_path=Path("validation.ktr"))
    steps = []
    hops = []
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        steps.append(inp)
        hops.append(PentahoHop(from_name="Input", to_name=step_name))
    steps.append(step)
    for name in successors or []:
        steps.append(PentahoStep(name=name, step_type="Dummy", attributes={}, raw_element=None))
        hops.append(PentahoHop(from_name=step_name, to_name=name))
    trans.steps = steps
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestValidationParsers(unittest.TestCase):
    def test_credit_card_parse(self):
        cfg = parse_credit_card_validator_config(ET.fromstring("""
        <step>
          <fieldname>cc_num</fieldname>
          <resultfieldname>is_valid</resultfieldname>
          <cardtype>vendor</cardtype>
          <onlydigits>Y</onlydigits>
          <notvalidmsg>err_msg</notvalidmsg>
        </step>
        """))
        self.assertEqual(cfg["fieldname"], "cc_num")
        self.assertEqual(cfg["resultfieldname"], "is_valid")
        self.assertEqual(cfg["cardtype"], "vendor")
        self.assertTrue(cfg["onlydigits"])
        self.assertEqual(cfg["notvalidmsg"], "err_msg")

    def test_data_validator_parse(self):
        cfg = parse_data_validator_config(ET.fromstring("""
        <step>
          <validate_all>Y</validate_all>
          <concat_errors>Y</concat_errors>
          <concat_separator>;</concat_separator>
          <validator_field>
            <name>age</name>
            <validation_name>AgeRange</validation_name>
            <null_allowed>N</null_allowed>
            <data_type>Integer</data_type>
            <data_type_verified>Y</data_type_verified>
            <min_value>0</min_value>
            <max_value>120</max_value>
            <regular_expression>^[0-9]+$</regular_expression>
            <allowed_value>
              <value>18</value>
              <value>21</value>
            </allowed_value>
          </validator_field>
          <validator_field>
            <name>email</name>
            <null_allowed>Y</null_allowed>
            <max_length>100</max_length>
          </validator_field>
        </step>
        """))
        self.assertTrue(cfg["validate_all"])
        self.assertTrue(cfg["concat_errors"])
        self.assertEqual(cfg["concat_separator"], ";")
        self.assertEqual(len(cfg["validations"]), 2)
        age = cfg["validations"][0]
        self.assertEqual(age["field_name"], "age")
        self.assertFalse(age["null_allowed"])
        self.assertEqual(age["minimum_value"], "0")
        self.assertEqual(age["allowed_values"], ["18", "21"])
        self.assertEqual(age["regular_expression"], "^[0-9]+$")

    def test_mail_validator_parse(self):
        cfg = parse_mail_validator_config(ET.fromstring("""
        <step>
          <emailfield>mail</emailfield>
          <resultfieldname>ok</resultfieldname>
          <ResultAsString>Y</ResultAsString>
          <smtpCheck>N</smtpCheck>
          <emailValideMsg>good</emailValideMsg>
          <emailNotValideMsg>bad</emailNotValideMsg>
          <errorsFieldName>mail_err</errorsFieldName>
        </step>
        """))
        self.assertEqual(cfg["emailfield"], "mail")
        self.assertTrue(cfg["result_as_string"])
        self.assertFalse(cfg["smtp_check"])
        self.assertEqual(cfg["errors_field_name"], "mail_err")

    def test_xsd_validator_parse(self):
        cfg = parse_xsd_validator_config(ET.fromstring("""
        <step>
          <xmlstream>payload</xmlstream>
          <xmlsourcefile>N</xmlsourcefile>
          <xdsfilename>/schemas/order.xsd</xdsfilename>
          <xsdsource>filename</xsdsource>
          <resultfieldname>xsd_ok</resultfieldname>
          <outputstringfield>Y</outputstringfield>
          <ifxmlvalid>VALID</ifxmlvalid>
          <ifxmlunvalid>INVALID</ifxmlunvalid>
          <addvalidationmsg>Y</addvalidationmsg>
          <validationmsgfield>xsd_msg</validationmsgfield>
          <allowExternalEntities>N</allowExternalEntities>
        </step>
        """))
        self.assertEqual(cfg["xmlstream"], "payload")
        self.assertEqual(cfg["xsdfilename"], "/schemas/order.xsd")
        self.assertTrue(cfg["outputstringfield"])
        self.assertEqual(cfg["ifxmlinvalid"], "INVALID")
        self.assertFalse(cfg["allow_external_entities"])

    def test_parse_step_metadata_routes(self):
        for stype, tag in (
            ("CreditCardValidator", "fieldname"),
            ("Validator", "validate_all"),
            ("MailValidator", "emailfield"),
            ("XSDValidator", "xmlstream"),
        ):
            el = ET.fromstring(f"<step><{tag}>x</{tag}></step>")
            meta = parse_step_metadata(el, stype)
            self.assertTrue(meta, msg=stype)


class TestValidationHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertEqual(len(VALIDATION_HANDLERS), 4)

    def test_credit_card_luhn_code(self):
        xml = """
        <step>
          <fieldname>cc</fieldname>
          <resultfieldname>valid</resultfieldname>
          <cardtype>brand</cardtype>
          <onlydigits>Y</onlydigits>
          <notvalidmsg>reason</notvalidmsg>
        </step>
        """
        ctx = _ctx(xml, "CreditCardValidator", "CC")
        lines, status = self.registry.generate_code("CreditCardValidator", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial"))
        self.assertIn("Luhn", code)
        self.assertIn("_cc_udf_", code)
        self.assertIn("valid", code)
        self.assertIn("brand", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_credit_card_accept_reject(self):
        xml = """
        <step>
          <fieldname>cc</fieldname>
          <resultfieldname>valid</resultfieldname>
          <send_true_to>Ok</send_true_to>
          <send_false_to>Bad</send_false_to>
        </step>
        """
        ctx = _ctx(xml, "CreditCardValidator", "CC", successors=["Ok", "Bad"])
        lines, status = self.registry.generate_code("CreditCardValidator", ctx)
        code = "\n".join(lines)
        self.assertIn("df_Ok", code)
        self.assertIn("df_Bad", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_data_validator_rules(self):
        xml = """
        <step>
          <validate_all>Y</validate_all>
          <validator_field>
            <name>status</name>
            <null_allowed>N</null_allowed>
            <allowed_value><value>A</value><value>B</value></allowed_value>
          </validator_field>
          <validator_field>
            <name>amount</name>
            <min_value>0</min_value>
            <max_value>1000</max_value>
            <only_numeric_allowed>Y</only_numeric_allowed>
          </validator_field>
          <send_true_to>Good</send_true_to>
          <send_false_to>Reject</send_false_to>
        </step>
        """
        ctx = _ctx(xml, "Validator", "DV", successors=["Good", "Reject"])
        lines, status = self.registry.generate_code("Validator", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial"))
        self.assertIn("_row_valid", code)
        self.assertIn("df_Good", code)
        self.assertIn("df_Reject", code)
        self.assertIn("isin", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_data_validator_regex_and_date(self):
        xml = """
        <step>
          <validator_field>
            <name>code</name>
            <regular_expression>^[A-Z]{3}$</regular_expression>
          </validator_field>
          <validator_field>
            <name>dt</name>
            <data_type>Date</data_type>
            <data_type_verified>Y</data_type_verified>
            <conversion_mask>yyyy-MM-dd</conversion_mask>
          </validator_field>
        </step>
        """
        ctx = _ctx(xml, "DataValidator", "DV2")
        lines, status = self.registry.generate_code("DataValidator", ctx)
        code = "\n".join(lines)
        self.assertIn("rlike", code)
        self.assertIn("to_date", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_mail_validator_relaxed(self):
        xml = """
        <step>
          <emailfield>email</emailfield>
          <resultfieldname>ok</resultfieldname>
          <smtpCheck>N</smtpCheck>
          <errorsFieldName>err</errorsFieldName>
        </step>
        """
        ctx = _ctx(xml, "MailValidator", "MV")
        lines, status = self.registry.generate_code("MailValidator", ctx)
        code = "\n".join(lines)
        self.assertEqual(status, "converted")
        self.assertIn("_mail_udf_", code)
        self.assertIn("email.utils", code)
        self.assertIn("relaxed", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_mail_validator_smtp_partial(self):
        xml = """
        <step>
          <emailfield>email</emailfield>
          <resultfieldname>ok</resultfieldname>
          <smtpCheck>Y</smtpCheck>
          <timeout>5000</timeout>
          <defaultSMTP>smtp.example.com</defaultSMTP>
        </step>
        """
        ctx = _ctx(xml, "MailValidator", "MV2")
        lines, status = self.registry.generate_code("MailValidator", ctx)
        code = "\n".join(lines)
        self.assertEqual(status, "partial")
        self.assertIn("smtpCheck", code)
        self.assertIn("WARNING", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_xsd_validator_file(self):
        xml = """
        <step>
          <xmlstream>xml_payload</xmlstream>
          <xdsfilename>/tmp/schema.xsd</xdsfilename>
          <xsdsource>filename</xsdsource>
          <resultfieldname>ok</resultfieldname>
          <addvalidationmsg>Y</addvalidationmsg>
          <validationmsgfield>msg</validationmsgfield>
        </step>
        """
        ctx = _ctx(xml, "XSDValidator", "XV")
        lines, status = self.registry.generate_code("XSDValidator", ctx)
        code = "\n".join(lines)
        self.assertIn("_xsd_udf_", code)
        self.assertIn("XMLSchema", code)
        self.assertIn("/tmp/schema.xsd", code)
        self.assertIn("LIMITATION", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_xsd_missing_schema_partial(self):
        xml = """
        <step>
          <xmlstream>xml_payload</xmlstream>
          <xsdsource>filename</xsdsource>
          <resultfieldname>ok</resultfieldname>
        </step>
        """
        ctx = _ctx(xml, "XSDValidator", "XV2")
        lines, status = self.registry.generate_code("XSDValidator", ctx)
        self.assertEqual(status, "partial")
        self.assertIn("WARNING", "\n".join(lines))
        self.assertTrue(_syntax_ok(lines))

    def test_credit_card_not_stolen_by_generator(self):
        """CreditCardValidator must not be handled by Random Credit Card generator."""
        ctx = _ctx(
            "<step><fieldname>cc</fieldname><resultfieldname>r</resultfieldname></step>",
            "CreditCardValidator",
            "CC",
        )
        lines, _ = self.registry.generate_code("CreditCardValidator", ctx)
        code = "\n".join(lines)
        self.assertIn("Credit Card Validator", code)
        self.assertNotIn("Generate Random Credit Card", code)

    def test_semantic_validators_registered(self):
        for stype in (
            "creditcardvalidator",
            "validator",
            "datavalidator",
            "mailvalidator",
            "xsdvalidator",
        ):
            self.assertIsNotNone(get_validator(stype), msg=stype)

    def test_mail_accept_reject_branches(self):
        xml = """
        <step>
          <emailfield>email</emailfield>
          <resultfieldname>ok</resultfieldname>
          <smtpCheck>N</smtpCheck>
          <send_true_to>ValidMail</send_true_to>
          <send_false_to>BadMail</send_false_to>
        </step>
        """
        ctx = _ctx(xml, "MailValidator", "MV3", successors=["ValidMail", "BadMail"])
        lines, status = self.registry.generate_code("MailValidator", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial"))
        self.assertIn("df_ValidMail", code)
        self.assertIn("df_BadMail", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_mail_strict_mode_uses_tighter_pattern(self):
        """smtpCheck=Y maps to strict structural mode when SMTP is unsupported."""
        xml = """
        <step>
          <emailfield>email</emailfield>
          <resultfieldname>ok</resultfieldname>
          <smtpCheck>Y</smtpCheck>
        </step>
        """
        ctx = _ctx(xml, "MailValidator", "MV4")
        lines, status = self.registry.generate_code("MailValidator", ctx)
        code = "\n".join(lines)
        self.assertEqual(status, "partial")
        self.assertIn("strict=True", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_xsd_accept_reject_and_namespaces_documented(self):
        xml = """
        <step>
          <xmlstream>payload</xmlstream>
          <xdsfilename>/schemas/a.xsd</xdsfilename>
          <xsdsource>filename</xsdsource>
          <resultfieldname>ok</resultfieldname>
          <send_true_to>OkXml</send_true_to>
          <send_false_to>BadXml</send_false_to>
        </step>
        """
        ctx = _ctx(xml, "XSDValidator", "XV3", successors=["OkXml", "BadXml"])
        lines, status = self.registry.generate_code("XSDValidator", ctx)
        code = "\n".join(lines)
        self.assertIn("df_OkXml", code)
        self.assertIn("df_BadXml", code)
        self.assertIn("NAMESPACE", code)
        self.assertIn("keyref", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_data_validator_mandatory_and_minmax(self):
        xml = """
        <step>
          <validator_field>
            <name>qty</name>
            <null_allowed>N</null_allowed>
            <min_value>1</min_value>
            <max_value>99</max_value>
            <decimal_symbol>,</decimal_symbol>
            <grouping_symbol>.</grouping_symbol>
          </validator_field>
        </step>
        """
        ctx = _ctx(xml, "Validator", "DV3")
        lines, status = self.registry.generate_code("Validator", ctx)
        code = "\n".join(lines)
        self.assertIn("null_allowed", code)
        self.assertIn("regexp_replace", code)
        self.assertIn(">= lit(1)", code)
        self.assertIn("<= lit(99)", code)
        self.assertIn("preserved.validation[0]=", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_edge_cases_missing_fields(self):
        cases = [
            ("CreditCardValidator", "<step><resultfieldname>r</resultfieldname></step>", "WARNING"),
            ("MailValidator", "<step><resultfieldname>r</resultfieldname></step>", "WARNING"),
            ("XSDValidator", "<step><resultfieldname>r</resultfieldname></step>", "WARNING"),
            ("Validator", "<step><validate_all>Y</validate_all></step>", "WARNING"),
        ]
        for stype, xml, marker in cases:
            with self.subTest(stype=stype):
                ctx = _ctx(xml, stype, "E")
                lines, status = self.registry.generate_code(stype, ctx)
                code = "\n".join(lines)
                self.assertIn(marker, code)
                self.assertIn(status, ("partial", "converted", "failed"))
                self.assertTrue(_syntax_ok(lines), code)

    def test_credit_card_empty_and_null_handling_in_udf(self):
        xml = """
        <step>
          <fieldname>cc</fieldname>
          <resultfieldname>valid</resultfieldname>
          <cardtype>brand</cardtype>
          <onlydigits>Y</onlydigits>
        </step>
        """
        ctx = _ctx(xml, "CreditCardValidator", "CC2")
        lines, _ = self.registry.generate_code("CreditCardValidator", ctx)
        code = "\n".join(lines)
        self.assertIn("Credit card number is null", code)
        self.assertIn("Credit card number is empty", code)
        self.assertIn("Failed Luhn", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_xsd_invalid_xml_path_in_udf(self):
        xml = """
        <step>
          <xmlstream>payload</xmlstream>
          <xmlsourcefile>Y</xmlsourcefile>
          <xdsfilename>/missing.xsd</xdsfilename>
          <xsdsource>filename</xsdsource>
          <resultfieldname>ok</resultfieldname>
        </step>
        """
        ctx = _ctx(xml, "XSDValidator", "XV4")
        lines, _ = self.registry.generate_code("XSDValidator", ctx)
        code = "\n".join(lines)
        self.assertIn("XML file not found", code)
        self.assertIn("Invalid XML", code)
        self.assertIn("XSD file missing", code)
        self.assertTrue(_syntax_ok(lines), code)

    def test_display_name_step_types(self):
        """Space-separated display names must still resolve to handlers."""
        for stype, xml_snip, needle in (
            ("Credit Card Validator", "<fieldname>cc</fieldname><resultfieldname>r</resultfieldname>", "Luhn"),
            ("Mail Validator", "<emailfield>e</emailfield><resultfieldname>r</resultfieldname>", "_mail_udf_"),
            ("XSD Validator", "<xmlstream>x</xmlstream><xdsfilename>/a.xsd</xdsfilename>", "XMLSchema"),
            ("Data Validator", "<validator_field><name>a</name><null_allowed>N</null_allowed></validator_field>", "_row_valid"),
        ):
            with self.subTest(stype=stype):
                ctx = _ctx(f"<step>{xml_snip}</step>", stype, "D")
                lines, status = self.registry.generate_code(stype, ctx)
                code = "\n".join(lines)
                self.assertIn(needle, code)
                self.assertIn(status, ("converted", "partial"))
                self.assertTrue(_syntax_ok(lines), code)

    def test_metadata_propagation_keys(self):
        from pentaho_converter.metadata_propagation import get_converter_metadata

        xml = """
        <step>
          <emailfield>mail</emailfield>
          <resultfieldname>ok</resultfieldname>
          <smtpCheck>N</smtpCheck>
        </step>
        """
        ctx = _ctx(xml, "MailValidator", "MV5")
        meta = get_converter_metadata(ctx)
        self.assertEqual(meta.get("emailfield"), "mail")
        self.assertIn("emailfield", meta.get("_propagated_keys") or meta.keys())


if __name__ == "__main__":
    unittest.main()
