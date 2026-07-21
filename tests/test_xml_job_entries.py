"""Unit tests for XML-category Pentaho Job Entries.

Covers XML_WELL_FORMED, DTD_VALIDATOR, XSD_VALIDATOR, XSLT —
parser coverage, success/failure, variable substitution, edge cases.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pentaho_converter.job_parser import parse_job
from pentaho_converter.runtime_templates.engine import xml_ops as xops
from pentaho_converter.runtime_templates.engine.handlers import (
    build_handlers,
    handle_dtd_validator,
    handle_xml_well_formed,
    handle_xsd_validator,
    handle_xslt,
)
from pentaho_converter.runtime_templates.engine.job_models import JobEntry
from pentaho_converter.runtime_templates.engine.job_runtime import JobRuntime

try:
    from lxml import etree as _lxml_etree  # noqa: F401

    HAS_LXML = True
except ImportError:
    HAS_LXML = False


def _runtime(*, variables: dict | None = None) -> JobRuntime:
    vars_ = variables if variables is not None else {}
    handlers = build_handlers(
        spark=None,
        cfg={},
        entry_types={
            "XML_WELL_FORMED",
            "DTD_VALIDATOR",
            "XSD_VALIDATOR",
            "XSLT",
        },
        trans_runners={},
        child_job_modules={},
    )
    rt = JobRuntime(
        name="XmlTestJob",
        entries=[],
        hops=[],
        variables=vars_,
        handlers=handlers,
        root_variables=vars_,
        variable_scopes=[vars_],
    )
    rt.result_filenames = []
    return rt


_XML_KJB = """<?xml version="1.0" encoding="UTF-8"?>
<job>
  <name>XmlSample</name>
  <entries>
    <entry>
      <name>Start</name>
      <type>SPECIAL</type>
      <start>Y</start>
    </entry>
    <entry>
      <name>WellFormed</name>
      <type>XML_WELL_FORMED</type>
      <arg_from_previous>N</arg_from_previous>
      <include_subfolders>Y</include_subfolders>
      <nr_errors_less_than>${MAX_BAD}</nr_errors_less_than>
      <success_condition>success_if_no_errors</success_condition>
      <resultfilenames>all_filenames</resultfilenames>
      <fields>
        <field>
          <source_filefolder>${XML_DIR}</source_filefolder>
          <wildcard>.*\\.xml$</wildcard>
        </field>
      </fields>
      <custom_attr>keep_me</custom_attr>
    </entry>
    <entry>
      <name>DtdCheck</name>
      <type>DTD_VALIDATOR</type>
      <xmlfilename>${XML_DIR}/book.xml</xmlfilename>
      <dtdfilename>${XML_DIR}/book.dtd</dtdfilename>
      <dtdintern>N</dtdintern>
    </entry>
    <entry>
      <name>XsdCheck</name>
      <type>XSD_VALIDATOR</type>
      <xmlfilename>${XML_DIR}/person.xml</xmlfilename>
      <xsdfilename>${XML_DIR}/person.xsd</xsdfilename>
      <allowExternalEntities>N</allowExternalEntities>
    </entry>
    <entry>
      <name>XslRun</name>
      <type>XSLT</type>
      <xmlfilename>${XML_DIR}/in.xml</xmlfilename>
      <xslfilename>${XML_DIR}/style.xsl</xslfilename>
      <outputfilename>${OUT}/out.xml</outputfilename>
      <iffileexists>0</iffileexists>
      <addfiletoresult>Y</addfiletoresult>
      <filenamesfromprevious>N</filenamesfromprevious>
      <xsltfactory>SAXON</xsltfactory>
      <parameters>
        <parameter>
          <field>${TITLE}</field>
          <name>docTitle</name>
        </parameter>
      </parameters>
      <outputproperties>
        <outputproperty>
          <name>encoding</name>
          <value>UTF-8</value>
        </outputproperty>
        <outputproperty>
          <name>indent</name>
          <value>yes</value>
        </outputproperty>
      </outputproperties>
    </entry>
  </entries>
</job>
"""


class TestXmlParser(unittest.TestCase):
    def test_parse_all_xml_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "xml.kjb"
            path.write_text(_XML_KJB, encoding="utf-8")
            job = parse_job(path)

        by_type = {
            e.entry_type: e for e in job.entries if e.entry_type != "SPECIAL"
        }
        self.assertIn("XML_WELL_FORMED", by_type)
        self.assertIn("DTD_VALIDATOR", by_type)
        self.assertIn("XSD_VALIDATOR", by_type)
        self.assertIn("XSLT", by_type)

        wf = by_type["XML_WELL_FORMED"].attributes
        self.assertEqual(wf.get("include_subfolders"), "Y")
        self.assertEqual(wf.get("nr_errors_less_than"), "${MAX_BAD}")
        self.assertEqual(wf.get("custom_attr"), "keep_me")
        self.assertEqual(len(wf.get("fields") or []), 1)
        self.assertEqual(wf["fields"][0]["source_filefolder"], "${XML_DIR}")

        dtd = by_type["DTD_VALIDATOR"].attributes
        self.assertEqual(dtd.get("dtdintern"), "N")
        self.assertIn("${XML_DIR}", dtd.get("xmlfilename", ""))

        xsd = by_type["XSD_VALIDATOR"].attributes
        self.assertEqual(xsd.get("allowExternalEntities"), "N")

        xslt = by_type["XSLT"].attributes
        self.assertEqual(xslt.get("xsltfactory"), "SAXON")
        self.assertEqual(xslt.get("iffileexists"), "0")
        params = xslt.get("parameters") or []
        self.assertEqual(len(params), 1)
        self.assertEqual(params[0]["name"], "docTitle")
        self.assertEqual(params[0]["field"], "${TITLE}")
        self.assertEqual(params[0]["value"], "${TITLE}")
        props = xslt.get("outputproperties") or []
        self.assertEqual(len(props), 2)
        self.assertEqual(props[0]["name"], "encoding")


class TestXmlWellFormed(unittest.TestCase):
    def test_success_and_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            good = root / "good.xml"
            bad = root / "bad.xml"
            good.write_text("<root><a/></root>", encoding="utf-8")
            bad.write_text("<root><a></root>", encoding="utf-8")

            ok = xops.xml_well_formed(
                [{"source": str(root), "wildcard": r".*\.xml$"}],
                success_condition="success_if_no_errors",
            )
            self.assertFalse(ok.success)
            self.assertEqual(len(ok.extra.get("well") or []), 1)
            self.assertEqual(len(ok.extra.get("bad") or []), 1)

            ok2 = xops.xml_well_formed(
                [{"source": str(good), "wildcard": ""}],
                success_condition="success_if_no_errors",
            )
            self.assertTrue(ok2.success)

            ok3 = xops.xml_well_formed(
                [{"source": str(root), "wildcard": r".*\.xml$"}],
                success_condition="success_if_bad_formed_files_less",
                nr_errors_less_than="2",
            )
            self.assertTrue(ok3.success)

            only_well = xops.xml_well_formed(
                [{"source": str(root), "wildcard": r".*\.xml$"}],
                success_condition="success_if_at_least_x_files_well_formed",
                nr_errors_less_than="1",
                resultfilenames="add_well_formed_files_only",
            )
            self.assertTrue(only_well.success)
            self.assertEqual(only_well.paths, [str(good)])

    def test_handler_variable_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.xml").write_text("<ok/>", encoding="utf-8")
            rt = _runtime(variables={"XML_DIR": str(root)})
            entry = JobEntry(
                name="WellFormed",
                entry_type="XML_WELL_FORMED",
                attributes={
                    "fields": [
                        {"source_filefolder": "${XML_DIR}", "wildcard": r".*\.xml$"}
                    ],
                    "success_condition": "success_if_no_errors",
                    "resultfilenames": "all_filenames",
                },
            )
            result = handle_xml_well_formed(rt, entry)
            self.assertTrue(result.success)
            self.assertTrue(rt.result_filenames)

    def test_missing_files_fails(self) -> None:
        outcome = xops.xml_well_formed(
            [{"source": "/no/such/path", "wildcard": ""}],
        )
        self.assertFalse(outcome.success)


class TestDtdValidator(unittest.TestCase):
    def _sample(self, root: Path) -> tuple[Path, Path]:
        dtd = root / "note.dtd"
        dtd.write_text(
            '<!ELEMENT note (to,from)>\n<!ELEMENT to (#PCDATA)>\n<!ELEMENT from (#PCDATA)>\n',
            encoding="utf-8",
        )
        xml = root / "note.xml"
        xml.write_text(
            '<?xml version="1.0"?>\n'
            "<!DOCTYPE note SYSTEM \"note.dtd\">\n"
            "<note><to>A</to><from>B</from></note>\n",
            encoding="utf-8",
        )
        return xml, dtd

    @unittest.skipUnless(HAS_LXML, "lxml required")
    def test_dtd_pass_and_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            xml, dtd = self._sample(root)
            ok = xops.dtd_validate(str(xml), str(dtd), dtd_intern=False)
            self.assertTrue(ok.success, ok.message)

            bad = root / "bad.xml"
            bad.write_text(
                '<?xml version="1.0"?>\n'
                "<!DOCTYPE note SYSTEM \"note.dtd\">\n"
                "<note><to>A</to></note>\n",
                encoding="utf-8",
            )
            fail = xops.dtd_validate(str(bad), str(dtd), dtd_intern=False)
            self.assertFalse(fail.success)
            self.assertTrue(fail.errors)

    def test_dtd_missing_lxml(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            xml = Path(tmp) / "a.xml"
            xml.write_text("<note/>", encoding="utf-8")
            with patch.object(
                xops, "_import_lxml", side_effect=ImportError("no lxml")
            ):
                outcome = xops.dtd_validate(str(xml), str(Path(tmp) / "a.dtd"))
            self.assertFalse(outcome.success)
            self.assertIn("lxml", str(outcome.error))

    @unittest.skipUnless(HAS_LXML, "lxml required")
    def test_handler_dtd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            xml, dtd = self._sample(root)
            rt = _runtime(variables={"XML_DIR": str(root)})
            entry = JobEntry(
                name="DtdCheck",
                entry_type="DTD_VALIDATOR",
                attributes={
                    "xmlfilename": "${XML_DIR}/note.xml",
                    "dtdfilename": "${XML_DIR}/note.dtd",
                    "dtdintern": "N",
                },
            )
            result = handle_dtd_validator(rt, entry)
            self.assertTrue(result.success)


class TestXsdValidator(unittest.TestCase):
    def _sample(self, root: Path) -> tuple[Path, Path]:
        xsd = root / "person.xsd"
        xsd.write_text(
            """<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="person">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="name" type="xs:string"/>
        <xs:element name="age" type="xs:integer"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
""",
            encoding="utf-8",
        )
        xml = root / "person.xml"
        xml.write_text(
            '<?xml version="1.0"?><person><name>Ada</name><age>36</age></person>',
            encoding="utf-8",
        )
        return xml, xsd

    @unittest.skipUnless(HAS_LXML, "lxml required")
    def test_xsd_pass_and_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            xml, xsd = self._sample(root)
            ok = xops.xsd_validate(str(xml), str(xsd))
            self.assertTrue(ok.success, ok.message)

            bad = root / "bad.xml"
            bad.write_text(
                '<?xml version="1.0"?><person><name>Ada</name><age>x</age></person>',
                encoding="utf-8",
            )
            fail = xops.xsd_validate(str(bad), str(xsd))
            self.assertFalse(fail.success)
            self.assertTrue(fail.errors)

            warn = xops.xsd_validate(str(xml), str(xsd), allow_external_entities=True)
            self.assertTrue(warn.success)
            self.assertTrue(any("allowExternalEntities" in w for w in warn.warnings))

    def test_xsd_missing_file(self) -> None:
        outcome = xops.xsd_validate("/no/xml.xml", "/no/schema.xsd")
        self.assertFalse(outcome.success)

    @unittest.skipUnless(HAS_LXML, "lxml required")
    def test_handler_xsd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._sample(root)
            rt = _runtime(variables={"XML_DIR": str(root)})
            entry = JobEntry(
                name="XsdCheck",
                entry_type="XSD_VALIDATOR",
                attributes={
                    "xmlfilename": "${XML_DIR}/person.xml",
                    "xsdfilename": "${XML_DIR}/person.xsd",
                },
            )
            result = handle_xsd_validator(rt, entry)
            self.assertTrue(result.success)


class TestXslt(unittest.TestCase):
    def _sample(self, root: Path) -> tuple[Path, Path, Path]:
        xml = root / "in.xml"
        xml.write_text(
            '<?xml version="1.0"?><doc><item>hello</item></doc>',
            encoding="utf-8",
        )
        xsl = root / "style.xsl"
        xsl.write_text(
            """<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:param name="docTitle" select="'Untitled'"/>
  <xsl:output method="xml" encoding="UTF-8" indent="yes"/>
  <xsl:template match="/">
    <out>
      <title><xsl:value-of select="$docTitle"/></title>
      <body><xsl:value-of select="doc/item"/></body>
    </out>
  </xsl:template>
</xsl:stylesheet>
""",
            encoding="utf-8",
        )
        out = root / "out.xml"
        return xml, xsl, out

    @unittest.skipUnless(HAS_LXML, "lxml required")
    def test_xslt_success_parameters_and_iffileexists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            xml, xsl, out = self._sample(root)
            outcome = xops.xsl_transform(
                str(xml),
                str(xsl),
                str(out),
                parameters=[{"name": "docTitle", "value": "Report"}],
                output_properties=[
                    {"name": "encoding", "value": "UTF-8"},
                    {"name": "indent", "value": "yes"},
                ],
                iffileexists="1",
                xsltfactory="SAXON",
            )
            self.assertTrue(outcome.success, outcome.message)
            self.assertTrue(out.exists())
            text = out.read_text(encoding="utf-8")
            self.assertIn("Report", text)
            self.assertTrue(any("xsltfactory" in w for w in outcome.warnings))

            # iffileexists=1 → skip existing (success, no rewrite)
            mtime = out.stat().st_mtime
            skip = xops.xsl_transform(
                str(xml), str(xsl), str(out), iffileexists="1"
            )
            self.assertTrue(skip.success)
            self.assertEqual(skip.paths, [])
            self.assertEqual(out.stat().st_mtime, mtime)

            # iffileexists=2 → fail
            fail = xops.xsl_transform(
                str(xml), str(xsl), str(out), iffileexists="2"
            )
            self.assertFalse(fail.success)

            # iffileexists=0 → unique name
            unique = xops.xsl_transform(
                str(xml), str(xsl), str(out), iffileexists="0"
            )
            self.assertTrue(unique.success)
            self.assertTrue(unique.paths)
            self.assertNotEqual(unique.paths[0], str(out))

    @unittest.skipUnless(HAS_LXML, "lxml required")
    def test_handler_xslt_add_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            xml, xsl, out = self._sample(root)
            rt = _runtime(
                variables={
                    "XML_DIR": str(root),
                    "OUT": str(root),
                    "TITLE": "FromVar",
                }
            )
            entry = JobEntry(
                name="XslRun",
                entry_type="XSLT",
                attributes={
                    "xmlfilename": "${XML_DIR}/in.xml",
                    "xslfilename": "${XML_DIR}/style.xsl",
                    "outputfilename": "${OUT}/out.xml",
                    "iffileexists": "3",
                    "addfiletoresult": "Y",
                    "xsltfactory": "JAXP",
                    "parameters": [
                        {"name": "docTitle", "field": "${TITLE}", "value": "${TITLE}"}
                    ],
                    "outputproperties": [{"name": "encoding", "value": "UTF-8"}],
                },
            )
            result = handle_xslt(rt, entry)
            self.assertTrue(result.success)
            self.assertTrue(any(i["path"].endswith("out.xml") for i in rt.result_filenames))
            self.assertIn("FromVar", Path(out).read_text(encoding="utf-8"))

    def test_xslt_missing_input(self) -> None:
        outcome = xops.xsl_transform("/no/in.xml", "/no/style.xsl", "/tmp/out.xml")
        self.assertFalse(outcome.success)


class TestXmlRegistration(unittest.TestCase):
    def test_handlers_registered(self) -> None:
        handlers = build_handlers(
            spark=None,
            cfg={},
            entry_types=set(),
            trans_runners={},
            child_job_modules={},
        )
        for key in ("XML_WELL_FORMED", "DTD_VALIDATOR", "XSD_VALIDATOR", "XSLT"):
            self.assertIn(key, handlers)


if __name__ == "__main__":
    unittest.main()
