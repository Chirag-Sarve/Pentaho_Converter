"""Parse nested Pentaho step XML structures from step elements."""

from __future__ import annotations

import html
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any
from xml.etree import ElementTree as ET

# Pentaho CalculatorMetaFunction.calc_desc (index → name). Index 2 is COPY_FIELD.
_CALC_DESC_BY_ID: tuple[str, ...] = (
    "-",
    "CONSTANT",
    "COPY_FIELD",
    "ADD",
    "SUBTRACT",
    "MULTIPLY",
    "DIVIDE",
    "SQUARE",
    "SQUARE_ROOT",
    "PERCENT_1",
    "PERCENT_2",
    "PERCENT_3",
    "COMBINATION_1",
    "COMBINATION_2",
    "ROUND_1",
    "ROUND_2",
    "ROUND_STD_1",
    "ROUND_STD_2",
    "CEIL",
    "FLOOR",
    "NVL",
    "ADD_DAYS",
    "YEAR_OF_DATE",
    "MONTH_OF_DATE",
    "DAY_OF_YEAR",
    "DAY_OF_MONTH",
    "DAY_OF_WEEK",
    "WEEK_OF_YEAR",
    "WEEK_OF_YEAR_ISO8601",
    "YEAR_OF_DATE_ISO8601",
    "BYTE_TO_HEX_ENCODE",
    "HEX_TO_BYTE_DECODE",
    "CHAR_TO_HEX_ENCODE",
    "HEX_TO_CHAR_DECODE",
    "CRC32",
    "ADLER32",
    "MD5",
    "SHA1",
    "LEVENSHTEIN_DISTANCE",
    "METAPHONE",
    "DOUBLE_METAPHONE",
    "ABS",
    "REMOVE_TIME_FROM_DATE",
    "DATE_DIFF",
    "ADD3",
    "INIT_CAP",
    "UPPER_CASE",
    "LOWER_CASE",
    "MASK_XML",
    "USE_CDATA",
    "REMOVE_CR",
    "REMOVE_LF",
    "REMOVE_CRLF",
    "REMOVE_TAB",
    "GET_ONLY_DIGITS",
    "REMOVE_DIGITS",
    "STRING_LEN",
    "LOAD_FILE_CONTENT_BINARY",
    "ADD_TIME_TO_DATE",
    "QUARTER_OF_DATE",
    "SUBSTITUTE_VARIABLE",
    "UNESCAPE_XML",
    "ESCAPE_HTML",
    "UNESCAPE_HTML",
    "ESCAPE_SQL",
    "DATE_WORKING_DIFF",
    "ADD_MONTHS",
    "CHECK_XML_FILE_WELL_FORMED",
    "CHECK_XML_WELL_FORMED",
    "GET_FILE_ENCODING",
    "DAMERAU_LEVENSHTEIN",
    "NEEDLEMAN_WUNSH",
    "JARO",
    "JARO_WINKLER",
    "SOUNDEX",
    "REFINED_SOUNDEX",
    "ADD_HOURS",
    "ADD_MINUTES",
    "DATE_DIFF_MSEC",
    "DATE_DIFF_SEC",
    "DATE_DIFF_MN",
    "DATE_DIFF_HR",
    "HOUR_OF_DAY",
    "MINUTE_OF_HOUR",
    "SECOND_OF_MINUTE",
    "ROUND_CUSTOM_1",
    "ROUND_CUSTOM_2",
    "ADD_SECONDS",
    "REMAINDER",
)

# Non-localized long descriptions accepted by CalculatorMetaFunction.getCalcFunctionType.
_CALC_LONG_DESC_BY_TYPE: dict[str, str] = {
    "A + B": "ADD",
    "A - B": "SUBTRACT",
    "A * B": "MULTIPLY",
    "A / B": "DIVIDE",
    "A * A": "SQUARE",
    "100 * A / B": "PERCENT_1",
    "A - ( A * B / 100 )": "PERCENT_2",
    "A + ( A * B / 100 )": "PERCENT_3",
    "A + B * C": "COMBINATION_1",
    "A + B + C": "ADD3",
}

_CALC_TYPE_ALIASES: dict[str, str] = {
    "COPY_OF_FIELD": "COPY_FIELD",
    "PLUS": "ADD",
    "MINUS": "SUBTRACT",
    "MOD": "REMAINDER",
    "SQRT": "SQUARE_ROOT",
    "ROUND": "ROUND_1",
    "UPPER": "UPPER_CASE",
    "LOWER": "LOWER_CASE",
    "INITCAP": "INIT_CAP",
    "LENGTH": "STRING_LEN",
}

# Back-compat for callers that still look up by numeric string.
_CALC_TYPE_BY_ID: dict[str, str] = {
    str(i): name for i, name in enumerate(_CALC_DESC_BY_ID) if i > 0
}

# Step types whose nested XML is parsed into ``PentahoStep.parsed_config``.
_STRUCTURED_STEP_TYPES = frozenset({
    "calculator",
    "mergejoin",
    "groupby",
    # Statistics
    "memorygroupby",
    "analyticquery",
    "samplerows",
    "reservoirsampling",
    "univariatestats",
    "univariatestatistics",
    "stepsmetrics",
    "outputstepsmetrics",
    "valuemapper",
    "filterrows",
    "rowgenerator",
    "datagrid",
    "sequence",
    "addsequence",
    "checksum",
    "addachecksum",
    "numberrange",
    "fieldschangesequence",
    "addvaluefieldschangingsequence",
    "textfileoutput",
    "textfileoutputlegacy",
    "tableinput",
    "databaselookup",
    "dblookup",
    "streamlookup",
    # Lookup category
    "dbproc",
    "calldbproc",
    "calldbprocedure",
    "dbjoin",
    "databasejoin",
    "dynamicsqlrow",
    "fileexists",
    "tableexists",
    "columnexists",
    "checkfilelocked",
    "fileslocked",
    "lockedfiles",
    "webserviceavailable",
    "checkwebserviceavailable",
    "http",
    "httpclient",
    "httppost",
    "httpget",
    "rest",
    "restclient",
    "webservice",
    "webservicelookup",
    "fuzzymatch",
    "formula",
    "ifnull",
    "iffieldvaluenull",
    "iffieldvalueisnull",
    # Utility steps
    "clonerow",
    "clonerows",
    "nullif",
    "delay",
    "delayrow",
    "changefileencoding",
    "fileencoding",
    "metastructure",
    "stepmetastructure",
    "metadatastructureofstream",
    "writetolog",
    "tablecompare",
    "zipfile",
    "processfiles",
    "execprocess",
    "executeaprocess",
    "ssh",
    "runsshcommands",
    "mail",
    "sendmail",
    "syslogmessage",
    "writetosyslog",
    "sendmessagetosyslog",
    "edi2xml",
    "editoxml",
    # Field / string transforms
    "constant",
    "addconstants",
    "addconstant",
    "selectvalues",
    "setvalueconstant",
    "setfieldvaluetoaconstant",
    "setfieldvalueconstant",
    "setvaluefield",
    "setfieldvalue",
    "concatfields",
    "addxml",
    "replaceinstring",
    "replacestring",
    "stringoperations",
    "stringcut",
    "stringscut",
    # Reshape / sort / unique
    "rownormaliser",
    "rownormalizer",
    "normaliser",
    "rowdenormaliser",
    "rowdenormalizer",
    "denormaliser",
    "flattener",
    "rowflattener",
    "splitfieldtorows",
    "fieldsplitter",
    "splitfields",
    "sortrows",
    "unique",
    "uniquerows",
    "uniquerowsbyhashset",
    "uniquerowshashset",
    "uniquehashset",
    # Hierarchy / slave sequence / XSLT
    "closuregenerator",
    "closure",
    "getslavesequence",
    "getidfromslaveserver",
    "getidfromslave",
    "xslt",
    "xsltransformation",
    "xsltransform",
    # Input steps with structured file/connection metadata
    "csvinput",
    "textfileinput",
    "oldtextfileinput",
    "excelinput",
    "jsoninput",
    "getxmldata",
    "xmlinput",
    "xmlinputstream",
    "fixedinput",
    "fixedfileinput",
    "gzipcsvinput",
    "s3csvinput",
    "propertyinput",
    "yamlinput",
    "loadfileinput",
    "accessinput",
    "salesforceinput",
    "getfilenames",
    "gettablenames",
    "randomvalue",
    # Output steps with structured lookup / file metadata
    "insertupdate",
    "update",
    "delete",
    "synchronizeaftermerge",
    "synchronizemerge",
    "tableoutput",
    "jsonoutput",
    "xmloutput",
    "excelwriter",
    "typeexcelwriter",
    "exceloutput",
    "microsoftexceloutput",
    "sapinput",
    "saperpinput",
    "sqlfileoutput",
    "s3fileoutput",
    "propertyoutput",
    "accessoutput",
    "salesforceinsert",
    "salesforceupdate",
    "salesforceupsert",
    "salesforcedelete",
    # Streaming steps
    "recordsfromstream",
    "getrecordsfromstream",
    "kafkaconsumer",
    "kafkaconsumerinput",
    "kafkastreaminput",
    "kafka",
    "kafkaproducer",
    "kafkaproduceroutput",
    "jmsconsumer",
    "jmsconsumerinput",
    "activemqconsumer",
    "jmsproducer",
    "jmsproduceroutput",
    "activemqproducer",
    "mqttconsumer",
    "mqttconsumerinput",
    "mqttclient",
    "mqttproducer",
    "mqttproduceroutput",
    # Inline steps (Injector / Socket Reader / Socket Writer)
    "injector",
    "socketreader",
    "socketwriter",
    # Flow steps
    "abort",
    "append",
    "appendstreams",
    "blockuntilstepsfinish",
    "blockthisstepuntilstepsfinish",
    "blockingstep",
    "block",
    "detectemptystream",
    "detectempty",
    "dummy",
    "dummytrans",
    "dummydonothing",
    "metainject",
    "etlmetadatainjection",
    "filterrows",
    "identifylastrow",
    "identifylastrowinastream",
    "javafilter",
    "jobexecutor",
    "prioritystream",
    "prioritizestreams",
    "singlethreader",
    "switchcase",
    "transexecutor",
    "transformationexecutor",
    # Mapping (sub-transformation) category
    "mapping",
    "mappingsubtransformation",
    "simplemapping",
    "simplemappingsubtransformation",
    "mappinginput",
    "mappinginputspecification",
    "mappingoutput",
    "mappingoutputspecification",
    # Job category (result rows/files + variables)
    "rowstoresult",
    "copyrowstoresult",
    "rowsfromresult",
    "getrowsfromresult",
    "filesfromresult",
    "getfilesfromresult",
    "filestoresult",
    "setfilesinresult",
    "setfilestoresult",
    "setvariable",
    "setvariables",
    "getvariable",
    "getvariables",
    # Pentaho Server (BA Server) category
    "callendpoint",
    "callendpointstep",
    "getsessionvariable",
    "getsessionvariables",
    "getsessionvariablestep",
    "setsessionvariable",
    "setsessionvariables",
    "setsessionvariablestep",
    # Join steps
    "joinrows",
    "joiner",
    "mergerows",
    "mergerow",
    "multimergejoin",
    "multiwaymergejoin",
    "multimerge",
    "sortedmerge",
    "xmljoin",
    # Data Warehouse (SCD / junk dimension)
    "dimensionlookup",
    "dimensionlookupupdate",
    "combinationlookup",
    # Validation steps
    "creditcardvalidator",
    "creditcard",
    "validator",
    "datavalidator",
    "mailvalidator",
    "emailvalidator",
    "xsdvalidator",
    "xmlschemavalidator",
    # Scripting steps
    "execsql",
    "executesql",
    "sql",
    "execsqlrow",
    "executerowsqlscript",
    "executerowsql",
    "scriptvaluemod",
    "javascriptvalue",
    "modifiedjavascriptvalue",
    "regexeval",
    "regularexpression",
    "ruleaccumulator",
    "rulesaccumulator",
    "ruleexecutor",
    "rulesexecutor",
    "userdefinedjavaclass",
    "userdefinedjavaexpression",
    # Cryptography steps
    "pgpencryptstream",
    "pgpencrypt",
    "pgpdecryptstream",
    "pgpdecrypt",
    "secretkeygenerator",
    "secretkeygen",
    "symmetriccryptotrans",
    "symmetriccrypto",
    "symmetriccryptography",
    # Bulk Loading steps
    "gpbulkloader",
    "greenplumbulkloader",
    "greenplumload",
    "greenplumloader",
    "gpload",
    "infobrightloader",
    "infobrightbulkloader",
    "infobright",
    "vectorwisebulkloader",
    "ingresvectorwisebulkloader",
    "vwbulkloader",
    "ingresbulkloader",
    "vectorwiseloader",
    "monetdbbulkloader",
    "monetdbloader",
    "monetdbbulk",
    "mysqlbulkloader",
    "mysqlloader",
    "loaddatainfile",
    "orabulkloader",
    "oraclebulkloader",
    "oracleloader",
    "sqlldr",
    "pgbulkloader",
    "postgresqlbulkloader",
    "postgresbulkloader",
    "psqlbulkloader",
    "terafast",
    "teradatafastloadbulkloader",
    "terafastbulkloader",
    "teradatafastload",
    "fastload",
    "teradatabulkloader",
    "teradatatptbulkloader",
    "tptbulkloader",
    "teratpt",
    "teradatatpt",
    "verticabulkloader",
    "verticaloader",
    "verticacopy",
    # Experimental
    "sftpput",
    "sftpputfile",
    "putafilewithsftp",
    "putsftp",
    "script",
    "scriptvalues",
    "experimentalscript",
    # Pentaho Server (also listed near Job category above)
    "callendpoint",
    "callendpointstep",
    "getsessionvariable",
    "getsessionvariables",
    "getsessionvariablestep",
    "setsessionvariable",
    "setsessionvariables",
    "setsessionvariablestep",
    # Big Data / file format I/O (aligned with parse_step_metadata)
    "avroinput",
    "avrofileinput",
    "avrooutput",
    "avrofileoutput",
    "mongodbinput",
    "mongoinput",
    "mongodboutput",
    "mongooutput",
    "parquetinput",
    "parquetfileinput",
    "parquetoutput",
    "parquetfileoutput",
    "orcinput",
    "orcfileinput",
    "orcoutput",
    "orcfileoutput",
    "deltaoutput",
    "deltafileoutput",
    "writeoutdelta",
    "csvoutput",
    "csvfileoutput",
    "hadoopfileinput",
    "hadoopfileinputplugin",
    "hadoopfileoutputplugin",
    # Advanced / string aliases
    "systeminfo",
    "rank",
    "top",
    "rowsfilter",
    "limit",
    "regexreplace",
    "replacenull",
    "splunkinput",
    "splunkoutput",
    "splunk",
    "microsoftexcelinput",
    "microsoftexcelwriter",
    "fixedwidthinput",
    "gzipcsv",
    "gzipcsvfileinput",
    "s3csvfileinput",
    "s3fileinput",
    "s3output",
    "propertiesinput",
    "propertiesoutput",
    "microsoftaccessinput",
    "microsoftaccessoutput",
    # Extra I/O aliases + intentionally unsupported (metadata preservation)
    "loadfile",
    "loadfilecontentinmemory",
    "getsubfolders",
    "getsubfoldernames",
    "getfilesrowscount",
    "filesrowscount",
    "generaterandomvalue",
    "randomccnumbergenerator",
    "generaterandomcreditcardnumbers",
    "creditcardgenerator",
    "staxxmlinput",
    "xmlinputstreamstax",
    "xmlpad",
    "xmlwriter",
    "shapefilereader",
    "esrishapefile",
    "esrishapefilereader",
    "gisfileinput",
    "dbfinput",
    "xbaseinput",
    "sasinput",
    "ldapinput",
    "ldapoutput",
    "ldifinput",
    "rssinput",
    "rssoutput",
    "hl7input",
    "autodoc",
    "autodocoutput",
    "automaticdocumentationoutput",
    "getrepositorynames",
    "mailinput",
    "emailmessagesinput",
    "emailinput",
    "cubeinput",
    "cubeoutput",
    "deserializefromfile",
    "deserialisefromfile",
    "serializetofile",
    "serialisetofile",
    "mondrianinput",
    "olapinput",
    "xmla",
    "xmlainput",
    "pentahoreporting",
    "pentahoreportingoutput",
    "reportexport",
    "prptoutput",
})

# Pentaho ValueMeta type codes → type names (Select Values meta tab).
_PENTAHO_TYPE_BY_CODE: dict[str, str] = {
    "0": "",
    "1": "Number",
    "2": "String",
    "3": "Date",
    "4": "Boolean",
    "5": "Integer",
    "6": "BigNumber",
    "7": "Serializable",
    "8": "Binary",
    "9": "Timestamp",
    "10": "Internet Address",
}

_KNOWN_GROUP_AGG_TYPES = frozenset({
    "SUM",
    "AVG",
    "AVERAGE",
    "MIN",
    "MAX",
    "COUNT",
    "COUNT_ALL",
    "COUNT_ANY",
    "COUNT_DISTINCT",
    "FIRST",
    "LAST",
    "FIRST_INCL_NULL",
    "LAST_INCL_NULL",
    "MEDIAN",
    "PERCENTILE",
    "STDDEV",
    "STD_DEV",
    "STANDARD_DEVIATION",
    "VARIANCE",
    "CONCAT_COMMA",
    "CONCAT_STRING",
})


def _normalize_agg_token(raw: str) -> str:
    return (raw or "").strip().upper().replace(" ", "_")


def _is_known_aggregate_type(raw: str) -> bool:
    return _normalize_agg_token(raw) in _KNOWN_GROUP_AGG_TYPES


def _text(elem: ET.Element | None, default: str = "") -> str:
    if elem is None or elem.text is None:
        return default
    # Preserve whitespace-only values (e.g. tab delimiter "\t").
    if elem.text.strip() == "" and elem.text != "":
        return elem.text
    return elem.text.strip()


def _child_text(parent: ET.Element, tag: str, default: str = "") -> str:
    return _text(parent.find(tag), default)


def _bool_from_yn(value: str, default: bool = False) -> bool:
    if not value:
        return default
    return value.strip().upper() in ("Y", "YES", "TRUE", "1")


def _normalize_calc_type(raw: str) -> str:
    """Normalize Calculator calc_type from ID, short name, alias, or long desc."""
    if not raw:
        return ""
    text = raw.strip()
    if text.isdigit():
        idx = int(text)
        if 0 <= idx < len(_CALC_DESC_BY_ID):
            return _CALC_DESC_BY_ID[idx]
        return text
    upper = text.upper().replace(" ", "_")
    if upper in _CALC_TYPE_ALIASES:
        return _CALC_TYPE_ALIASES[upper]
    # Exact match against calc_desc short names
    for name in _CALC_DESC_BY_ID:
        if name.upper() == upper or name.upper() == text.upper():
            return name if name != "COPY_FIELD" else "COPY_FIELD"
    # Long description forms ("A + B", "100 * A / B", …)
    collapsed = " ".join(text.split())
    if collapsed in _CALC_LONG_DESC_BY_TYPE:
        return _CALC_LONG_DESC_BY_TYPE[collapsed]
    # Tolerate missing spaces around operators
    collapsed_ns = collapsed.replace(" ", "")
    for long_desc, short in _CALC_LONG_DESC_BY_TYPE.items():
        if long_desc.replace(" ", "") == collapsed_ns:
            return short
    return upper


def _element_to_dict(elem: ET.Element) -> dict[str, Any]:
    """Recursively convert an XML element to a JSON-serializable dict."""
    result: dict[str, Any] = {}
    if elem.text and elem.text.strip():
        result["_text"] = elem.text.strip()
    for child in elem:
        val: Any = _text(child) if len(child) == 0 else _element_to_dict(child)
        tag = child.tag
        if tag in result:
            existing = result[tag]
            if isinstance(existing, list):
                existing.append(val)
            else:
                result[tag] = [existing, val]
        else:
            result[tag] = val
    return result


def _metadata_value(obj: Any) -> Any:
    if is_dataclass(obj):
        return {k: _metadata_value(v) for k, v in asdict(obj).items()}
    if isinstance(obj, list):
        return [_metadata_value(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _metadata_value(v) for k, v in obj.items()}
    return obj


@dataclass
class ConstantField:
    name: str
    type_name: str = "String"
    value: str = ""
    set_empty_string: bool = False
    format: str = ""
    currency: str = ""
    decimal: str = ""
    group: str = ""
    length: str = ""
    precision: str = ""


@dataclass
class RowGeneratorField:
    name: str
    type_name: str = "String"
    value: str = ""
    length: str = ""
    precision: str = ""
    format: str = ""
    set_empty_string: bool = False


@dataclass
class CalculationSpec:
    field_name: str
    calc_type: str
    field_a: str = ""
    field_b: str = ""
    field_c: str = ""
    value_type: str = ""
    conversion_mask: str = ""
    value: str = ""
    remove: bool = False
    decimal_symbol: str = ""
    grouping_symbol: str = ""
    currency_symbol: str = ""
    value_length: str = ""
    value_precision: str = ""


@dataclass
class StringOpField:
    in_stream_name: str
    out_stream_name: str = ""
    trim_type: str = "none"
    lower_upper: str = "none"
    init_cap: bool = False
    # Optional extended operations (StringCut / ReplaceInString compatibility)
    cut_from: str = ""
    cut_to: str = ""
    replace_string: str = ""
    replace_by_string: str = ""
    use_regex: bool = False
    replace_field_by_string: str = ""
    set_empty_string: bool = False
    whole_word: bool = False
    case_sensitive: bool = True
    is_unicode: bool = False
    # String Operations–specific
    padding_type: str = "none"  # none | left | right
    pad_char: str = " "
    pad_len: str = ""
    digits_type: str = "none"  # none | only | remove
    mask_xml: str = "none"  # none | escapexml | cdata | unescapexml | escapehtml | ...
    remove_special_characters: str = "none"  # none | cr | lf | crlf | tab | espace


@dataclass
class SequenceConfig:
    field_name: str = "seq"
    start_at: int = 1
    increment_by: int = 1
    max_value: int | None = None
    use_counter: bool = True
    use_database: bool = False
    connection: str = ""
    schema_name: str = ""
    sequence_name: str = ""
    counter_name: str = ""


@dataclass
class SelectFieldSpec:
    name: str
    rename: str = ""
    length: str = ""
    precision: str = ""


@dataclass
class SelectMetaChange:
    name: str
    rename: str = ""
    type_name: str = ""
    length: str = ""
    precision: str = ""
    conversion_mask: str = ""
    decimal_symbol: str = ""
    grouping_symbol: str = ""
    currency_symbol: str = ""
    encoding: str = ""
    storage_type: str = ""
    date_format_lenient: bool = False
    date_format_locale: str = ""
    date_format_timezone: str = ""
    lenient_string_to_number: bool = False


@dataclass
class SetValueConstantField:
    name: str
    value: str = ""
    mask: str = ""
    set_empty_string: bool = False


@dataclass
class SetValueFieldSpec:
    name: str
    replace_by: str = ""


@dataclass
class ConcatFieldsConfig:
    target_field_name: str = ""
    target_field_length: int = 0
    remove_selected_fields: bool = False
    separator: str = ""
    enclosure: str = ""
    enclosure_forced: bool = False
    encoding: str = ""
    fields: list[dict[str, str]] = field(default_factory=list)


@dataclass
class AddXmlField:
    name: str
    element: str = ""
    type_name: str = "String"
    format: str = ""
    nullif: str = ""
    attribute: bool = False
    attribute_parent_name: str = ""
    length: str = ""
    precision: str = ""
    currency: str = ""
    decimal: str = ""
    grouping: str = ""


@dataclass
class AddXmlConfig:
    value_name: str = "xmlvaluename"
    root_node: str = "Row"
    encoding: str = "UTF-8"
    omit_xml_header: bool = True
    omit_null_values: bool = False
    fields: list[AddXmlField] = field(default_factory=list)


def get_step_element(step) -> ET.Element | None:
    """Return the raw XML element for a parsed step."""
    return step.raw_element


def parse_constant_fields(step_el: ET.Element) -> list[ConstantField]:
    """Parse Add Constants step field definitions.

    Pentaho stores the constant value in ``nullif``; some exports also use ``value``.
    """
    results: list[ConstantField] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        name = _child_text(field_el, "name")
        if not name:
            continue
        value = (
            _child_text(field_el, "nullif")
            or _child_text(field_el, "value")
            or _child_text(field_el, "default")
        )
        results.append(
            ConstantField(
                name=name,
                type_name=_child_text(field_el, "type", "String"),
                value=value,
                set_empty_string=_child_text(field_el, "set_empty_string", "N").upper() == "Y",
                format=_child_text(field_el, "format"),
                currency=_child_text(field_el, "currency"),
                decimal=_child_text(field_el, "decimal"),
                group=_child_text(field_el, "group"),
                length=_child_text(field_el, "length"),
                precision=_child_text(field_el, "precision"),
            )
        )
    return results


def parse_row_generator_fields(step_el: ET.Element) -> list[RowGeneratorField]:
    """Parse Generate Rows field definitions (value in default, value, or nullif)."""
    results: list[RowGeneratorField] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        name = _child_text(field_el, "name")
        if not name:
            continue
        value = (
            _child_text(field_el, "default")
            or _child_text(field_el, "value")
            or _child_text(field_el, "string")
            or _child_text(field_el, "nullif")
        )
        if _child_text(field_el, "set_empty_string", "N").upper() == "Y" and value == "":
            value = ""
        results.append(
            RowGeneratorField(
                name=name,
                type_name=_child_text(field_el, "type", "String"),
                value=value,
                length=_child_text(field_el, "length"),
                precision=_child_text(field_el, "precision"),
                format=_child_text(field_el, "format"),
                set_empty_string=_child_text(field_el, "set_empty_string", "N").upper() == "Y",
            )
        )
    return results


def parse_file_block(step_el: ET.Element) -> dict[str, Any]:
    """Parse nested ``<file>`` block (Text File Input/Output)."""
    file_el = step_el.find("file")
    if file_el is None:
        return {}
    block: dict[str, Any] = {}
    for child in file_el:
        if len(child) == 0:
            block[child.tag] = _text(child)
        else:
            block[child.tag] = _element_to_dict(child)
    if not block.get("name") and file_el.text and file_el.text.strip():
        block["name"] = file_el.text.strip()
    return block


def extract_step_property(step_el: ET.Element, key: str, default: str = "") -> str:
    """Read a scalar step property, including nested <file><name> paths."""
    if key in ("file", "filename"):
        file_block = parse_file_block(step_el)
        path = file_block.get("name", "")
        if path:
            return str(path)
        file_el = step_el.find("file")
        if file_el is not None and file_el.text and file_el.text.strip():
            return file_el.text.strip()
    return _child_text(step_el, key, default)


def parse_data_grid_rows(step_el: ET.Element) -> tuple[list[str], list[list[str]]]:
    """Parse Data Grid / embedded line data into column names and row values."""
    columns = [f.name for f in parse_row_generator_fields(step_el) if f.name]
    rows: list[list[str]] = []
    data_el = step_el.find("data")
    if data_el is None:
        return columns, rows
    for line_el in data_el.findall("line"):
        items = [_text(item) for item in line_el.findall("item")]
        if items:
            rows.append(items)
    return columns, rows


def parse_calculations(step_el: ET.Element) -> list[CalculationSpec]:
    """Parse all Calculator step calculation entries.

    Accepts official Spoon tags (``field_name`` / ``calc_type``) and camelCase
    variants sometimes seen in repository or hand-edited exports.
    """
    results: list[CalculationSpec] = []
    calc_nodes = list(step_el.findall("calculation"))
    if not calc_nodes:
        calc_nodes = list(step_el.findall(".//calculation"))
    for calc_el in calc_nodes:
        field_name = (
            _child_text(calc_el, "field_name")
            or _child_text(calc_el, "fieldName")
            or _child_text(calc_el, "name")
        )
        if not field_name:
            continue
        calc_type_raw = (
            _child_text(calc_el, "calc_type")
            or _child_text(calc_el, "calcType")
            or _child_text(calc_el, "calculation")
        )
        results.append(
            CalculationSpec(
                field_name=field_name,
                calc_type=_normalize_calc_type(calc_type_raw),
                field_a=_child_text(calc_el, "field_a") or _child_text(calc_el, "fieldA"),
                field_b=_child_text(calc_el, "field_b") or _child_text(calc_el, "fieldB"),
                field_c=_child_text(calc_el, "field_c") or _child_text(calc_el, "fieldC"),
                value_type=_child_text(calc_el, "value_type") or _child_text(calc_el, "valueType"),
                conversion_mask=_child_text(calc_el, "conversion_mask")
                or _child_text(calc_el, "conversionMask"),
                value=_child_text(calc_el, "value"),
                remove=_child_text(calc_el, "remove", "N").upper() == "Y",
                decimal_symbol=_child_text(calc_el, "decimal_symbol")
                or _child_text(calc_el, "decimalSymbol"),
                grouping_symbol=_child_text(calc_el, "grouping_symbol")
                or _child_text(calc_el, "groupingSymbol"),
                currency_symbol=_child_text(calc_el, "currency_symbol")
                or _child_text(calc_el, "currencySymbol"),
                value_length=_child_text(calc_el, "value_length")
                or _child_text(calc_el, "valueLength"),
                value_precision=_child_text(calc_el, "value_precision")
                or _child_text(calc_el, "valuePrecision"),
            )
        )
    return results


def parse_string_operation_fields(step_el: ET.Element) -> list[StringOpField]:
    """Parse String Operations / Replace in String / String Cut field definitions."""
    results: list[StringOpField] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        in_name = _child_text(field_el, "in_stream_name") or _child_text(field_el, "name")
        if not in_name:
            continue
        out_name = _child_text(field_el, "out_stream_name") or in_name
        results.append(
            StringOpField(
                in_stream_name=in_name,
                out_stream_name=out_name,
                trim_type=_child_text(field_el, "trim_type", "none").lower(),
                lower_upper=_child_text(field_el, "lower_upper", "none").lower(),
                init_cap=_child_text(field_el, "initcap", "N").upper() == "Y"
                or _child_text(field_el, "init_cap", "N").upper() == "Y",
                cut_from=_child_text(field_el, "cut_from") or _child_text(field_el, "start"),
                cut_to=_child_text(field_el, "cut_to") or _child_text(field_el, "end"),
                replace_string=_child_text(field_el, "replace_string") or _child_text(field_el, "search"),
                replace_by_string=_child_text(field_el, "replace_by_string")
                or _child_text(field_el, "replace"),
                use_regex=_bool_from_yn(_child_text(field_el, "use_regex", "N")),
                replace_field_by_string=_child_text(field_el, "replace_field_by_string"),
                set_empty_string=_bool_from_yn(_child_text(field_el, "set_empty_string", "N")),
                whole_word=_bool_from_yn(_child_text(field_el, "whole_word", "N")),
                case_sensitive=_bool_from_yn(_child_text(field_el, "case_sensitive", "Y"), default=True),
                is_unicode=_bool_from_yn(_child_text(field_el, "is_unicode", "N")),
                padding_type=(
                    _child_text(field_el, "padding_type")
                    or _child_text(field_el, "padding", "none")
                    or "none"
                ).lower(),
                pad_char=_child_text(field_el, "pad_char")
                or _child_text(field_el, "padChar", " ")
                or " ",
                pad_len=_child_text(field_el, "pad_len")
                or _child_text(field_el, "padLen")
                or _child_text(field_el, "pad_length"),
                digits_type=(
                    _child_text(field_el, "digits_type")
                    or _child_text(field_el, "digits", "none")
                    or "none"
                ).lower(),
                mask_xml=(
                    _child_text(field_el, "mask_xml")
                    or _child_text(field_el, "maskXML")
                    or _child_text(field_el, "escape", "none")
                    or "none"
                ).lower().replace(" ", ""),
                remove_special_characters=(
                    _child_text(field_el, "remove_special_characters")
                    or _child_text(field_el, "removeSpecialCharacters", "none")
                    or "none"
                ).lower().replace(" ", ""),
            )
        )
    return results


def parse_string_operations_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse String Operations fields with full option set."""
    return {"fields": _metadata_value(parse_string_operation_fields(step_el))}


def parse_string_cut_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse String Cut — fields with cut_from/cut_to (Java 0-based, end exclusive)."""
    fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            in_name = _child_text(field_el, "in_stream_name") or _child_text(field_el, "name")
            if not in_name:
                continue
            fields.append({
                "in_stream_name": in_name,
                "out_stream_name": _child_text(field_el, "out_stream_name") or "",
                "cut_from": _child_text(field_el, "cut_from") or "0",
                "cut_to": _child_text(field_el, "cut_to") or "0",
            })
    return {"fields": fields}


def parse_checksum_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Add a Checksum algorithm, result field, and input fields."""
    fields_el = step_el.find("fields")
    field_names = [
        _child_text(f, "name")
        for f in (fields_el if fields_el is not None else ET.Element("x")).findall("field")
        if _child_text(f, "name")
    ]
    return {
        "checksum_type": (
            _child_text(step_el, "checksumtype")
            or _child_text(step_el, "checksum_type")
            or _child_text(step_el, "type", "CRC32")
            or "CRC32"
        ).upper().replace(" ", "-"),
        "result_field": (
            _child_text(step_el, "resultfieldName")
            or _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "result_field")
            or "checksum"
        ),
        "result_type": (
            _child_text(step_el, "resultType")
            or _child_text(step_el, "result_type")
            or "string"
        ).lower(),
        "compatibility_mode": _bool_from_yn(
            _child_text(step_el, "compatibilityMode")
            or _child_text(step_el, "compatibility_mode", "N")
        ),
        "old_checksum_behaviour": _bool_from_yn(
            _child_text(step_el, "oldChecksumBehaviourMode")
            or _child_text(step_el, "old_checksum_behaviour", "N")
            or _child_text(step_el, "oldChecksumBehaviour", "N")
        ),
        "fields": field_names,
    }


def parse_number_range_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Number Range input/output/fallback and inclusive-lower exclusive-upper rules."""
    rules: list[dict[str, Any]] = []
    rules_el = step_el.find("rules")
    rule_nodes = (
        list(rules_el.findall("rule")) if rules_el is not None else list(step_el.findall("rule"))
    )
    for rule_el in rule_nodes:
        rules.append({
            "lower_bound": _child_text(rule_el, "lower_bound")
            or _child_text(rule_el, "lowerBound"),
            "upper_bound": _child_text(rule_el, "upper_bound")
            or _child_text(rule_el, "upperBound"),
            "value": _child_text(rule_el, "value"),
        })
    return {
        "input_field": _child_text(step_el, "inputField")
        or _child_text(step_el, "input_field"),
        "output_field": _child_text(step_el, "outputField")
        or _child_text(step_el, "output_field")
        or "range",
        "fallback_value": _child_text(step_el, "fallBackValue")
        or _child_text(step_el, "fallback_value")
        or _child_text(step_el, "default"),
        "rules": rules,
        # Documented Pentaho semantics: lowerBound <= x < upperBound
        "lower_inclusive": True,
        "upper_inclusive": False,
    }


def parse_fields_change_sequence_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Add value fields changing sequence grouping keys and counter."""
    fields_el = step_el.find("fields")
    field_names = [
        _child_text(f, "name")
        for f in (fields_el if fields_el is not None else ET.Element("x")).findall("field")
        if _child_text(f, "name")
    ]
    start_raw = _child_text(step_el, "start") or "1"
    incr_raw = _child_text(step_el, "increment") or "1"
    try:
        start_at = int(start_raw or "1")
    except ValueError:
        start_at = 1
    try:
        increment_by = int(incr_raw or "1")
    except ValueError:
        increment_by = 1
    return {
        "result_field": (
            _child_text(step_el, "resultfieldName")
            or _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "valuename")
            or "change_seq"
        ),
        "start_at": start_at,
        "increment_by": increment_by,
        "group_fields": field_names,
    }


def parse_closure_generator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Closure Generator parent/child/distance and root-is-zero settings."""
    return {
        "parent_id_field": (
            _child_text(step_el, "parent_id_field")
            or _child_text(step_el, "parentIdField")
            or _child_text(step_el, "parent_id")
            or ""
        ),
        "child_id_field": (
            _child_text(step_el, "child_id_field")
            or _child_text(step_el, "childIdField")
            or _child_text(step_el, "child_id")
            or ""
        ),
        "distance_field": (
            _child_text(step_el, "distance_field")
            or _child_text(step_el, "distanceField")
            or _child_text(step_el, "distance")
            or "distance"
        ),
        "is_root_zero": _bool_from_yn(
            _child_text(step_el, "is_root_zero")
            or _child_text(step_el, "isRootZero")
            or _child_text(step_el, "root_is_zero", "N")
        ),
        # Documented safety limit matching Pentaho recurseParents distance > 50.
        "max_depth": 50,
    }


def parse_get_slave_sequence_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get ID from Slave Server value/slave/sequence/increment options."""
    incr_raw = _child_text(step_el, "increment") or "10000"
    try:
        increment = int(incr_raw or "10000")
    except ValueError:
        increment = 10000
    return {
        "value_name": (
            _child_text(step_el, "valuename")
            or _child_text(step_el, "value_name")
            or _child_text(step_el, "field_name")
            or "id"
        ),
        "slave_server": (
            _child_text(step_el, "slave")
            or _child_text(step_el, "slave_server")
            or _child_text(step_el, "slaveServerName")
            or ""
        ),
        "sequence_name": (
            _child_text(step_el, "seqname")
            or _child_text(step_el, "sequence_name")
            or _child_text(step_el, "sequenceName")
            or ""
        ),
        "increment": increment,
        "increment_raw": incr_raw,
    }


def parse_xslt_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse XSL Transformation stylesheet, field mappings, parameters, and factory."""
    parameters: list[dict[str, str]] = []
    params_el = step_el.find("parameters")
    for param_el in (params_el if params_el is not None else ET.Element("x")).findall("parameter"):
        name = _child_text(param_el, "name") or ""
        field = _child_text(param_el, "field") or ""
        if name or field:
            parameters.append({"name": name, "field": field})

    output_properties: list[dict[str, str]] = []
    props_el = step_el.find("outputproperties")
    for prop_el in (props_el if props_el is not None else ET.Element("x")).findall("outputproperty"):
        name = _child_text(prop_el, "name") or ""
        value = _child_text(prop_el, "value") or ""
        if name or value:
            output_properties.append({"name": name, "value": value})

    xsl_file_field_use = _bool_from_yn(
        _child_text(step_el, "xslfilefielduse")
        or _child_text(step_el, "xslFileFieldUse", "N")
    )
    is_a_file_raw = (
        _child_text(step_el, "xslfieldisafile")
        or _child_text(step_el, "xslFieldIsAFile")
    )
    # Pentaho default: when using a field and is-a-file is unset, treat field as a file path.
    if xsl_file_field_use and not (is_a_file_raw or "").strip():
        xsl_field_is_a_file = True
    else:
        xsl_field_is_a_file = _bool_from_yn(is_a_file_raw or "Y")

    return {
        "xsl_filename": (
            _child_text(step_el, "xslfilename")
            or _child_text(step_el, "xslFilename")
            or _child_text(step_el, "xsl_filename")
            or ""
        ),
        "field_name": (
            _child_text(step_el, "fieldname")
            or _child_text(step_el, "field_name")
            or _child_text(step_el, "xml_field")
            or ""
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or _child_text(step_el, "result_field")
            or "result"
        ),
        "xsl_file_field": (
            _child_text(step_el, "xslfilefield")
            or _child_text(step_el, "xslFileField")
            or ""
        ),
        "xsl_file_field_use": xsl_file_field_use,
        "xsl_field_is_a_file": xsl_field_is_a_file,
        "xsl_factory": (
            _child_text(step_el, "xslfactory")
            or _child_text(step_el, "xslFactory")
            or "JAXP"
        ),
        "parameters": parameters,
        "output_properties": output_properties,
    }


def _normalize_pentaho_type(raw: str) -> str:
    """Map Pentaho type codes or names to a canonical type string."""
    text = (raw or "").strip()
    if not text:
        return ""
    if text in _PENTAHO_TYPE_BY_CODE:
        return _PENTAHO_TYPE_BY_CODE[text]
    return text


def _iter_step_or_fields(step_el: ET.Element, tag: str):
    """Yield named child elements directly under step or under <fields>."""
    for el in step_el.findall(tag):
        yield el
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for el in fields_el.findall(tag):
            yield el


def parse_select_values_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Select Values select / remove / meta configuration.

    Handles:
    - Official Spoon layout: ``<fields><field/>…<remove/>…<meta/>…</fields>``
    - Step-level ``<remove>`` / ``<meta>`` siblings
    - Select-tab length/precision and unofficial ``<type>`` on select fields
    """
    select_fields: list[SelectFieldSpec] = []
    select_field_types: dict[str, str] = {}
    fields_el = step_el.find("fields")
    container = fields_el if fields_el is not None else step_el
    # Also scan step-level <field> when nested under fields and at step (rare exports).
    field_nodes = list(container.findall("field"))
    if fields_el is not None:
        for field_el in step_el.findall("field"):
            if field_el not in field_nodes:
                field_nodes.append(field_el)
    for field_el in field_nodes:
        name = _child_text(field_el, "name") or _child_text(field_el, "field_name")
        if not name:
            continue
        select_fields.append(
            SelectFieldSpec(
                name=name,
                rename=_child_text(field_el, "rename"),
                length=_child_text(field_el, "length"),
                precision=_child_text(field_el, "precision"),
            )
        )
        type_raw = _child_text(field_el, "type") or _child_text(field_el, "type_name")
        if type_raw:
            select_field_types[name] = _normalize_pentaho_type(type_raw)

    remove_names: list[str] = []
    for rem_el in _iter_step_or_fields(step_el, "remove"):
        name = _child_text(rem_el, "name") or _child_text(rem_el, "field_name")
        if name:
            remove_names.append(name)

    meta_changes: list[SelectMetaChange] = []
    for meta_el in _iter_step_or_fields(step_el, "meta"):
        name = _child_text(meta_el, "name") or _child_text(meta_el, "field_name")
        if not name:
            continue
        type_raw = (
            _child_text(meta_el, "type")
            or _child_text(meta_el, "type_name")
            or _child_text(meta_el, "value_type")
        )
        storage_raw = (
            _child_text(meta_el, "storage_type")
            or _child_text(meta_el, "storageType")
            or _child_text(meta_el, "storagetype")
        )
        meta_changes.append(
            SelectMetaChange(
                name=name,
                rename=_child_text(meta_el, "rename"),
                type_name=_normalize_pentaho_type(type_raw),
                length=_child_text(meta_el, "length"),
                precision=_child_text(meta_el, "precision"),
                conversion_mask=_child_text(meta_el, "conversion_mask")
                or _child_text(meta_el, "conversionMask")
                or _child_text(meta_el, "format"),
                decimal_symbol=_child_text(meta_el, "decimal_symbol")
                or _child_text(meta_el, "decimalSymbol")
                or _child_text(meta_el, "decimal"),
                grouping_symbol=_child_text(meta_el, "grouping_symbol")
                or _child_text(meta_el, "groupingSymbol")
                or _child_text(meta_el, "grouping")
                or _child_text(meta_el, "group"),
                currency_symbol=_child_text(meta_el, "currency_symbol")
                or _child_text(meta_el, "currencySymbol")
                or _child_text(meta_el, "currency"),
                encoding=_child_text(meta_el, "encoding"),
                storage_type=storage_raw,
                date_format_lenient=_bool_from_yn(
                    _child_text(meta_el, "date_format_lenient")
                    or _child_text(meta_el, "dateFormatLenient")
                ),
                date_format_locale=_child_text(meta_el, "date_format_locale")
                or _child_text(meta_el, "dateFormatLocale"),
                date_format_timezone=_child_text(meta_el, "date_format_timezone")
                or _child_text(meta_el, "dateFormatTimeZone"),
                lenient_string_to_number=_bool_from_yn(
                    _child_text(meta_el, "lenient_string_to_number")
                    or _child_text(meta_el, "lenientStringToNumber")
                ),
            )
        )

    select_unspecified = _bool_from_yn(
        _child_text(container, "select_unspecified")
        or _child_text(step_el, "select_unspecified")
        or _child_text(container, "selectUnspecified")
    )
    return {
        "select_fields": _metadata_value(select_fields),
        "remove_names": remove_names,
        "meta_changes": _metadata_value(meta_changes),
        "select_unspecified": select_unspecified,
        "fields": [
            {
                "name": f.name,
                "rename": f.rename,
                "type": select_field_types.get(f.name, ""),
                "length": f.length,
                "precision": f.precision,
            }
            for f in select_fields
        ],
        "select_columns": [f.name for f in select_fields],
        "output_columns": [f.rename or f.name for f in select_fields],
        "parse_ok": True,
    }


def parse_constant_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Add Constants step into structured metadata."""
    constants = parse_constant_fields(step_el)
    return {
        "constants": _metadata_value(constants),
        "fields": _metadata_value(constants),
    }


def parse_set_value_constant_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Set Field Value to a Constant step."""
    fields: list[SetValueConstantField] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append(
                SetValueConstantField(
                    name=name,
                    value=_child_text(field_el, "value"),
                    mask=_child_text(field_el, "mask"),
                    set_empty_string=_bool_from_yn(
                        _child_text(field_el, "set_empty_string", "N")
                    ),
                )
            )
    return {
        "usevar": _bool_from_yn(_child_text(step_el, "usevar", "N")),
        "fields": _metadata_value(fields),
        "set_fields": _metadata_value(fields),
    }


def parse_set_value_field_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Set Field Value step (copy one field into another)."""
    fields: list[SetValueFieldSpec] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append(
                SetValueFieldSpec(
                    name=name,
                    replace_by=_child_text(field_el, "replaceby")
                    or _child_text(field_el, "replace_by"),
                )
            )
    return {"fields": _metadata_value(fields), "set_fields": _metadata_value(fields)}


def parse_concat_fields_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Concat Fields target, separator, and field list."""
    concat_el = step_el.find("ConcatFields")
    target = ""
    target_len = 0
    remove_selected = False
    if concat_el is not None:
        target = _child_text(concat_el, "targetFieldName")
        try:
            target_len = int(_child_text(concat_el, "targetFieldLength", "0") or "0")
        except ValueError:
            target_len = 0
        remove_selected = _bool_from_yn(
            _child_text(concat_el, "removeSelectedFields", "N")
        )
    else:
        target = (
            _child_text(step_el, "targetFieldName")
            or _child_text(step_el, "target_field_name")
            or _child_text(step_el, "target_field")
        )
        try:
            target_len = int(
                _child_text(step_el, "targetFieldLength", "0")
                or _child_text(step_el, "target_field_length", "0")
                or "0"
            )
        except ValueError:
            target_len = 0
        remove_selected = _bool_from_yn(
            _child_text(step_el, "removeSelectedFields")
            or _child_text(step_el, "remove_selected_fields", "N")
        )

    output_fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            output_fields.append({
                "name": name,
                "type": _child_text(field_el, "type"),
                "format": _child_text(field_el, "format"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
                "nullif": _child_text(field_el, "nullif"),
                "trim_type": _child_text(field_el, "trim_type"),
            })

    cfg = ConcatFieldsConfig(
        target_field_name=target,
        target_field_length=target_len,
        remove_selected_fields=remove_selected,
        separator=_child_text(step_el, "separator", ""),
        enclosure=_child_text(step_el, "enclosure", ""),
        enclosure_forced=_bool_from_yn(
            _child_text(step_el, "enclosure_forced")
            or _child_text(step_el, "enclosureForced", "N")
        ),
        encoding=_child_text(step_el, "encoding", ""),
        fields=output_fields,
    )
    return _metadata_value(cfg)


def parse_add_xml_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Add XML (encode fields as an XML string column) configuration."""
    file_el = step_el.find("file")
    omit_header = True
    omit_null = False
    if file_el is not None:
        omit_header = _bool_from_yn(_child_text(file_el, "omitXMLheader", "Y"), default=True)
        omit_null = _bool_from_yn(_child_text(file_el, "omitNullValues", "N"))
    else:
        omit_header = _bool_from_yn(_child_text(step_el, "omitXMLheader", "Y"), default=True)
        omit_null = _bool_from_yn(_child_text(step_el, "omitNullValues", "N"))

    fields: list[AddXmlField] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append(
                AddXmlField(
                    name=name,
                    element=_child_text(field_el, "element") or name,
                    type_name=_child_text(field_el, "type", "String"),
                    format=_child_text(field_el, "format"),
                    nullif=_child_text(field_el, "nullif"),
                    attribute=_bool_from_yn(_child_text(field_el, "attribute", "N")),
                    attribute_parent_name=_child_text(field_el, "attributeParentName"),
                    length=_child_text(field_el, "length"),
                    precision=_child_text(field_el, "precision"),
                    currency=_child_text(field_el, "currency"),
                    decimal=_child_text(field_el, "decimal"),
                    grouping=_child_text(field_el, "group"),
                )
            )

    cfg = AddXmlConfig(
        value_name=_child_text(step_el, "valueName", "xmlvaluename")
        or _child_text(step_el, "value_name", "xmlvaluename"),
        root_node=_child_text(step_el, "xml_repeat_element", "Row")
        or _child_text(step_el, "root_node", "Row"),
        encoding=_child_text(step_el, "encoding", "UTF-8"),
        omit_xml_header=omit_header,
        omit_null_values=omit_null,
        fields=fields,
    )
    return _metadata_value(cfg)


def parse_replace_in_string_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Replace in String field-level replacements."""
    operations = parse_string_operation_fields(step_el)
    return {
        "operations": [
            {
                "in": o.in_stream_name,
                "out": o.out_stream_name,
                "replace_string": o.replace_string,
                "replace_by_string": o.replace_by_string,
                "replace_field_by_string": o.replace_field_by_string,
                "use_regex": o.use_regex,
                "whole_word": o.whole_word,
                "case_sensitive": o.case_sensitive,
                "set_empty_string": o.set_empty_string,
                "is_unicode": o.is_unicode,
            }
            for o in operations
        ],
        "fields": _metadata_value(operations),
    }


def parse_filter_compare_element(step_el: ET.Element) -> ET.Element | None:
    """Return the root condition element from a Filter Rows step."""
    compare_el = step_el.find("compare")
    if compare_el is None:
        return None
    condition = compare_el.find("condition")
    if condition is not None:
        return condition
    # Some exports place conditions directly under compare
    if compare_el.find("conditions") is not None:
        return compare_el
    return compare_el


def parse_value_constant(value_el: ET.Element | None) -> tuple[str, str]:
    """Return (type_name, text_value) from a Pentaho condition value element."""
    if value_el is None:
        return "String", ""
    type_name = _child_text(value_el, "type", "String")
    text_val = _child_text(value_el, "text")
    if _child_text(value_el, "isnull", "N").upper() == "Y":
        return type_name, ""
    return type_name, text_val


def unescape_xml(text: str) -> str:
    """Decode XML entities in Pentaho function operators."""
    return html.unescape(text or "").strip()


@dataclass
class GroupByField:
    name: str
    aggregate: str = ""
    subject: str = ""
    type_name: str = "String"
    valuefield: str = ""


@dataclass
class JoinKeyPair:
    left: str
    right: str


@dataclass
class ValueMapping:
    source: str
    target: str


@dataclass
class TextFileOutputConfig:
    filename: str = ""
    extension: str = ""
    separator: str = ","
    header: bool = True
    footer: bool = False
    encoding: str = "utf-8"
    compression: str = "none"
    enclosure: str = ""
    enclosure_forced: bool = False
    append: bool = False
    create_parent_folder: bool = False
    split: bool = False
    fast_dump: bool = False
    padded: bool = False
    file_as_command: bool = False
    file_name_in_field: bool = False
    file_name_field: str = ""
    add_date: bool = False
    add_time: bool = False
    specify_format: bool = False
    date_time_format: str = ""
    split_every: str = ""
    ended_line: str = ""
    servlet_output: bool = False
    do_not_open_new_file_init: bool = False
    file: dict[str, Any] = field(default_factory=dict)
    output_fields: list[dict[str, str]] = field(default_factory=list)


@dataclass
class TableInputConfig:
    connection: str = ""
    sql: str = ""
    schema: str = ""
    table: str = ""
    limit: int = 0
    execute_each_row: bool = False
    variables_active: bool = False
    lazy_conversion: bool = False
    parameters: list[dict[str, str]] = field(default_factory=list)


@dataclass
class DatabaseLookupKey:
    stream_field: str = ""
    table_field: str = ""
    name2: str = ""


@dataclass
class DbOutputKey:
    """Lookup key for Insert/Update, Update, Delete, Synchronize After Merge."""

    stream_field: str = ""
    table_field: str = ""
    condition: str = "="
    stream_field2: str = ""


@dataclass
class DbOutputValue:
    """Mapped update/insert column for DB output steps."""

    stream_field: str = ""
    table_field: str = ""
    update: bool = True


@dataclass
class DatabaseLookupReturnField:
    name: str = ""
    rename: str = ""
    default: str = ""
    type_name: str = ""


@dataclass
class DatabaseLookupConfig:
    connection: str = ""
    schema: str = ""
    table: str = ""
    cached: bool = False
    cache_size: int = 0
    orderby: str = ""
    fail_on_multiple: bool = False
    eat_row_on_failure: bool = False
    keys: list[DatabaseLookupKey] = field(default_factory=list)
    return_fields: list[DatabaseLookupReturnField] = field(default_factory=list)


@dataclass
class DimensionLookupKey:
    """Natural / business key: stream field ↔ dimension table column."""

    stream_field: str = ""
    table_field: str = ""


@dataclass
class DimensionLookupField:
    """Dimension attribute with SCD update strategy."""

    stream_field: str = ""
    table_field: str = ""
    update_type: str = "Insert"  # Insert|Update|PunchThrough|Date*|LastVersion


@dataclass
class DimensionLookupConfig:
    """Pentaho Dimension Lookup/Update (SCD Type 1 / Type 2) metadata."""

    connection: str = ""
    schema: str = ""
    table: str = ""
    update: bool = True
    commit_size: int = 100
    cache_size: int = 5000
    preload_cache: bool = False
    keys: list[DimensionLookupKey] = field(default_factory=list)
    fields: list[DimensionLookupField] = field(default_factory=list)
    # Surrogate / technical key
    technical_key: str = ""
    technical_key_rename: str = ""
    tech_key_creation: str = "tablemax"  # autoinc | sequence | tablemax
    use_autoinc: bool = False
    version_field: str = ""
    sequence_name: str = ""
    # Effective dating
    stream_datefield: str = ""
    date_from: str = ""
    date_to: str = ""
    min_year: int = 1900
    max_year: int = 2199
    use_start_date_alternative: bool = False
    start_date_alternative: str = "none"
    start_date_field_name: str = ""
    use_batch: bool = True


@dataclass
class CombinationLookupConfig:
    """Pentaho Combination Lookup/Update (junk / combination dimension) metadata."""

    connection: str = ""
    schema: str = ""
    table: str = ""
    commit_size: int = 100
    cache_size: int = 9999
    preload_cache: bool = False
    replace_fields: bool = False
    use_hash: bool = False
    hash_field: str = ""
    keys: list[DimensionLookupKey] = field(default_factory=list)
    technical_key: str = ""
    tech_key_creation: str = "tablemax"
    use_autoinc: bool = False
    sequence_name: str = ""
    last_update_field: str = ""


@dataclass
class MergeJoinConfig:
    join_type: str = "INNER"
    step1: str = ""
    step2: str = ""
    keys: list[JoinKeyPair] = field(default_factory=list)
    keys_1: list[str] = field(default_factory=list)
    keys_2: list[str] = field(default_factory=list)


@dataclass
class JoinRowsConfig:
    directory: str = ""
    prefix: str = ""
    cache_size: int = 500
    main_step: str = ""
    condition: dict[str, Any] | None = None


@dataclass
class MergeRowsConfig:
    flag_field: str = "flagfield"
    reference: str = ""
    compare: str = ""
    key_fields: list[str] = field(default_factory=list)
    value_fields: list[str] = field(default_factory=list)
    keys: list[JoinKeyPair] = field(default_factory=list)


@dataclass
class MultiwayMergeJoinConfig:
    join_type: str = "INNER"
    number_input: int = 0
    input_steps: list[str] = field(default_factory=list)
    key_fields: list[str] = field(default_factory=list)


@dataclass
class SortedMergeConfig:
    sort_fields: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class XMLJoinConfig:
    value_xml_field: str = ""
    target_xml_step: str = ""
    target_xml_field: str = ""
    source_xml_step: str = ""
    source_xml_field: str = ""
    target_xpath: str = ""
    join_compare_field: str = ""
    encoding: str = ""
    complex_join: bool = False
    omit_xml_header: bool = False
    omit_null_values: bool = False


@dataclass
class SwitchCaseRule:
    value: str
    target_step: str = ""


@dataclass
class RankConfig:
    top_bottom: str = "top"
    rank: bool = True
    sort_size: int = 10
    field_name: str = ""
    rank_field: str = "rank"


def _parse_group_by_aggregate_field(field_el: ET.Element) -> GroupByField | None:
    """Parse one Group By aggregate field from Pentaho XML (both export formats)."""
    name = _child_text(field_el, "name")
    agg_raw = _child_text(field_el, "aggregate")
    type_raw = _child_text(field_el, "type")
    subject = _child_text(field_el, "subject")
    valuefield = _child_text(field_el, "valuefield")

    if not agg_raw:
        return None

    agg_is_type = _is_known_aggregate_type(agg_raw)
    type_is_agg = _is_known_aggregate_type(type_raw)

    if agg_is_type and name:
        output_name = name
        agg_type = agg_raw
        source = subject or valuefield or name
        value_type = type_raw if type_raw and not type_is_agg else "String"
    elif not agg_is_type and type_is_agg:
        output_name = agg_raw
        agg_type = type_raw
        source = subject or valuefield or output_name
        value_type = "String"
    elif name:
        output_name = name
        agg_type = agg_raw if agg_is_type else (type_raw or "SUM")
        source = subject or valuefield or name
        value_type = type_raw if type_raw and not type_is_agg else "String"
    else:
        return None

    if not output_name or not agg_type:
        return None

    return GroupByField(
        name=output_name,
        aggregate=_normalize_agg_token(agg_type),
        subject=source,
        type_name=value_type or "String",
        valuefield=valuefield,
    )


def parse_group_by_fields(step_el: ET.Element) -> tuple[list[str], list[GroupByField]]:
    """Return (group_key_names, aggregate_field_specs) from a Group By step."""
    group_keys: list[str] = []
    aggregates: list[GroupByField] = []

    group_el = step_el.find("group")
    if group_el is not None:
        for field_el in group_el.findall("field"):
            name = _child_text(field_el, "name")
            if name:
                group_keys.append(name)

    for container_tag in ("fields", "aggregates"):
        container = step_el.find(container_tag)
        if container is None:
            continue
        for field_el in container.findall("field"):
            name = _child_text(field_el, "name")
            parsed = _parse_group_by_aggregate_field(field_el)
            if parsed is not None:
                aggregates.append(parsed)
            elif container_tag == "fields" and name and not _child_text(field_el, "aggregate"):
                if name not in group_keys:
                    group_keys.append(name)

    if not group_keys and not aggregates:
        for field_el in (step_el.find("fields") or ET.Element("x")).findall("field"):
            name = _child_text(field_el, "name")
            parsed = _parse_group_by_aggregate_field(field_el)
            if parsed is not None:
                aggregates.append(parsed)
            elif name:
                group_keys.append(name)

    return group_keys, aggregates


def parse_sort_fields(step_el: ET.Element) -> list[tuple[str, bool]]:
    """Return list of (column_name, ascending) from Sort Rows step."""
    return [
        (item["name"], item["ascending"])
        for item in parse_sort_rows_config(step_el).get("sort_fields") or []
        if item.get("name")
    ]


def parse_sort_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Sort Rows fields plus directory / uniqueness options."""
    sort_fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            sort_fields.append({
                "name": name,
                "ascending": _child_text(field_el, "ascending", "Y").upper() != "N",
                "case_sensitive": _bool_from_yn(
                    _child_text(field_el, "case_sensitive", "Y"), default=True
                ),
                "collator_enabled": _bool_from_yn(
                    _child_text(field_el, "collator_enabled", "N")
                ),
                "collator_strength": _child_text(field_el, "collator_strength", "0"),
                "presorted": _bool_from_yn(_child_text(field_el, "presorted", "N")),
            })
    return {
        "sort_fields": sort_fields,
        "directory": _child_text(step_el, "directory"),
        "prefix": _child_text(step_el, "prefix"),
        "sort_size": _child_text(step_el, "sort_size"),
        "free_memory": _child_text(step_el, "free_memory"),
        "compress": _bool_from_yn(_child_text(step_el, "compress", "N")),
        "compress_variable": _child_text(step_el, "compress_variable"),
        "unique_rows": _bool_from_yn(
            _child_text(step_el, "unique_rows")
            or _child_text(step_el, "unique", "N")
        ),
    }


def parse_unique_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Unique Rows / Unique Rows (HashSet) compare fields and counters."""
    compare_fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            # UniqueRows: case_insensitive; HashSet often stores case_sensitive
            case_insensitive = _bool_from_yn(
                _child_text(field_el, "case_insensitive", "N")
            )
            if _child_text(field_el, "case_sensitive"):
                case_insensitive = not _bool_from_yn(
                    _child_text(field_el, "case_sensitive", "Y"), default=True
                )
            compare_fields.append({
                "name": name,
                "case_insensitive": case_insensitive,
            })
    return {
        "compare_fields": compare_fields,
        "count_rows": _bool_from_yn(_child_text(step_el, "count_rows", "N")),
        "count_field": _child_text(step_el, "count_field")
        or _child_text(step_el, "countfield", "count"),
        "reject_duplicate_row": _bool_from_yn(
            _child_text(step_el, "reject_duplicate_row", "N")
        ),
        "error_description": _child_text(step_el, "error_description"),
        "store_values": _bool_from_yn(_child_text(step_el, "store_values", "Y"), default=True),
    }


def parse_normaliser_type_fields(step_el: ET.Element) -> list[tuple[str, list[str]]]:
    """Legacy helper — prefer parse_normaliser_config for full metadata."""
    cfg = parse_normaliser_config(step_el)
    groups: dict[str, list[str]] = {}
    for field in cfg.get("fields") or []:
        value = field.get("value") or ""
        groups.setdefault(value, []).append(field.get("name") or "")
    type_field = cfg.get("type_field") or "typefield"
    return [(type_field, [n for n in names if n]) for names in groups.values()]


def parse_normaliser_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Row Normaliser typefield + name/value/norm field map."""
    fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append({
                "name": name,
                "value": _child_text(field_el, "value"),
                "norm": _child_text(field_el, "norm"),
            })
    # Legacy <type> blocks (older converter format)
    if not fields:
        for type_el in step_el.findall("type") + (
            step_el.find("types") or ET.Element("x")
        ).findall("type"):
            value = _child_text(type_el, "type_field") or _child_text(type_el, "name")
            for vf in type_el.findall("field"):
                fname = _child_text(vf, "name")
                if fname:
                    fields.append({
                        "name": fname,
                        "value": value,
                        "norm": _child_text(vf, "norm") or fname,
                    })
    return {
        "type_field": _child_text(step_el, "typefield", "typefield")
        or _child_text(step_el, "type_field", "typefield"),
        "fields": fields,
    }


def parse_denormaliser_group_fields(step_el: ET.Element) -> tuple[list[str], str, list[str]]:
    """Legacy helper — prefer parse_denormaliser_config."""
    cfg = parse_denormaliser_config(step_el)
    return (
        list(cfg.get("group_fields") or []),
        str(cfg.get("key_field") or ""),
        [t.get("target_name") for t in (cfg.get("target_fields") or []) if t.get("target_name")],
    )


def parse_denormaliser_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Row Denormaliser group keys, key field, and target field specs."""
    group_el = step_el.find("group")
    group_fields = [
        _child_text(g, "name")
        for g in (group_el if group_el is not None else ET.Element("x")).findall("field")
        if _child_text(g, "name")
    ]
    target_fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            target_name = (
                _child_text(field_el, "target_name")
                or _child_text(field_el, "name")
            )
            if not target_name:
                continue
            target_fields.append({
                "field_name": _child_text(field_el, "field_name")
                or _child_text(field_el, "name"),
                "key_value": _child_text(field_el, "key_value"),
                "target_name": target_name,
                "target_type": _child_text(field_el, "target_type", "String"),
                "target_format": _child_text(field_el, "target_format"),
                "target_length": _child_text(field_el, "target_length"),
                "target_precision": _child_text(field_el, "target_precision"),
                "target_decimal_symbol": _child_text(field_el, "target_decimal_symbol"),
                "target_grouping_symbol": _child_text(field_el, "target_grouping_symbol"),
                "target_currency_symbol": _child_text(field_el, "target_currency_symbol"),
                "target_null_string": _child_text(field_el, "target_null_string"),
                "target_aggregation_type": (
                    _child_text(field_el, "target_aggregation_type", "NONE") or "NONE"
                ).upper(),
            })
    return {
        "key_field": _child_text(step_el, "key_field")
        or _child_text(step_el, "target_field")
        or _child_text(step_el, "type_field"),
        "group_fields": group_fields,
        "target_fields": target_fields,
    }


def parse_flattener_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Row Flattener source field and target field names."""
    fields_el = step_el.find("fields")
    targets = [
        _child_text(f, "name")
        for f in (fields_el if fields_el is not None else ET.Element("x")).findall("field")
        if _child_text(f, "name")
    ]
    return {
        "field_name": _child_text(step_el, "field_name")
        or _child_text(step_el, "field"),
        "target_fields": targets,
    }


def parse_split_field_to_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Split Field to Rows delimiter / new-field options."""
    return {
        "split_field": _child_text(step_el, "splitfield")
        or _child_text(step_el, "split_field"),
        "delimiter": _child_text(step_el, "delimiter", ";") or ";",
        "new_field": _child_text(step_el, "newfield")
        or _child_text(step_el, "new_field"),
        "include_row_number": _bool_from_yn(_child_text(step_el, "rownum", "N")),
        "row_number_field": _child_text(step_el, "rownum_field")
        or _child_text(step_el, "row_number_field", "rownr"),
        "reset_row_number": _bool_from_yn(
            _child_text(step_el, "resetrownumber", "Y"), default=True
        ),
        "delimiter_is_regex": _bool_from_yn(
            _child_text(step_el, "delimiter_is_regex", "N")
        ),
    }


def parse_split_fields_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Split Fields (FieldSplitter) delimiter and output field list."""
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append({
                "name": name,
                "id": _child_text(field_el, "id"),
                "idrem": _bool_from_yn(_child_text(field_el, "idrem", "N")),
                "type": _child_text(field_el, "type", "String"),
                "format": _child_text(field_el, "format"),
                "group": _child_text(field_el, "group"),
                "decimal": _child_text(field_el, "decimal"),
                "currency": _child_text(field_el, "currency"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
                "nullif": _child_text(field_el, "nullif"),
                "ifnull": _child_text(field_el, "ifnull"),
                "trimtype": _child_text(field_el, "trimtype", "none"),
            })
    return {
        "split_field": _child_text(step_el, "splitfield")
        or _child_text(step_el, "split_field"),
        "delimiter": _child_text(step_el, "delimiter", ",") or ",",
        "enclosure": _child_text(step_el, "enclosure", ""),
        "fields": fields,
    }


def parse_join_keys(step_el: ET.Element) -> list[JoinKeyPair]:
    """Parse join keys from Merge Join, lookup, and legacy key_N/value_N XML."""
    keys: list[JoinKeyPair] = []

    keys1_el = step_el.find("keys_1")
    keys2_el = step_el.find("keys_2")
    if keys1_el is not None and keys2_el is not None:
        left_keys = [_text(k) for k in keys1_el.findall("key") if _text(k)]
        right_keys = [_text(k) for k in keys2_el.findall("key") if _text(k)]
        if left_keys and right_keys:
            pair_count = max(len(left_keys), len(right_keys))
            for i in range(pair_count):
                left = left_keys[min(i, len(left_keys) - 1)]
                right = right_keys[min(i, len(right_keys) - 1)]
                keys.append(JoinKeyPair(left=left, right=right))
            return keys

    idx = 1
    while idx <= 20:
        left = _child_text(step_el, f"key_{idx}")
        right = _child_text(step_el, f"value_{idx}")
        if not left and not right:
            break
        if left and right:
            keys.append(JoinKeyPair(left=left, right=right))
        idx += 1

    if not keys:
        k1 = _child_text(step_el, "key_1")
        k2 = _child_text(step_el, "key_2")
        if k1 and k2 and not _child_text(step_el, "value_1"):
            keys.append(JoinKeyPair(left=k1, right=k2))

    if not keys:
        for key_block in step_el.findall("key"):
            name = _child_text(key_block, "name")
            field_el = key_block.find("field")
            field_val = _text(field_el) if field_el is not None else ""
            left = name or field_val
            right = field_val or name or left
            if left:
                keys.append(JoinKeyPair(left=left, right=right))

    if not keys:
        lookup_el = step_el.find("lookup")
        if lookup_el is not None:
            for key_el in lookup_el.findall("key"):
                left = _child_text(key_el, "name") or _child_text(key_el, "field")
                right = _child_text(key_el, "lookup") or _child_text(key_el, "name2")
                if left:
                    keys.append(JoinKeyPair(left=left, right=right or left))

    return keys


def format_spark_join_on(
    left_var: str, right_var: str, keys: list[JoinKeyPair]
) -> tuple[str, bool]:
    """Return (join_on_expression, use_on_keyword).

    When both streams use the same column names, returns (\"[\\\"col\\\"]\", True).
    Otherwise returns a boolean column expression for df.join(df, expr, how).
    """
    if not keys:
        return "", True
    if all(k.left == k.right for k in keys):
        names = ", ".join(f'"{k.left}"' for k in keys)
        return f"[{names}]", True
    cond = " & ".join(
        f'({left_var}["{k.left}"] == {right_var}["{k.right or k.left}"])' for k in keys
    )
    return cond, False


def _parse_value_mapping_entry(el: ET.Element) -> ValueMapping | None:
    """Parse one ValueMapper mapping row (valuemap / field / value)."""
    # Empty <source_value/> is a valid Pentaho mapping (null/blank → target).
    has_src = (
        el.find("source_value") is not None
        or el.find("from") is not None
        or el.find("source") is not None
    )
    src = (
        _child_text(el, "source_value")
        or _child_text(el, "from")
        or _child_text(el, "source")
    )
    tgt = (
        _child_text(el, "target_value")
        or _child_text(el, "to")
        or _child_text(el, "target")
    )
    if has_src or src or tgt:
        return ValueMapping(source=src or "", target=tgt or "")
    return None


def parse_value_mappings(step_el: ET.Element) -> tuple[str, str, list[ValueMapping], str]:
    """Return (source_field, target_field, mappings, default_value).

    Supports both classic tags (``field_to_use``, ``non_match_default``,
    ``fields``/``valuemap``) and Spoon exports that use ``fieldname``,
    ``nonmatch_default``, and ``values``/``value``.
    """
    source = (
        _child_text(step_el, "field_to_use")
        or _child_text(step_el, "from_field")
        or _child_text(step_el, "fieldname")
        or _child_text(step_el, "field_name")
    )
    target = (
        _child_text(step_el, "target_field")
        or _child_text(step_el, "to_field")
        or source
    )
    default = (
        _child_text(step_el, "non_match_default")
        or _child_text(step_el, "nonmatch_default")
        or _child_text(step_el, "default")
    )
    mappings: list[ValueMapping] = []
    for vm_el in step_el.findall("valuemap") + step_el.findall("mapping"):
        entry = _parse_value_mapping_entry(vm_el)
        if entry is not None:
            mappings.append(entry)
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            entry = _parse_value_mapping_entry(field_el)
            if entry is not None:
                mappings.append(entry)
    # Spoon / newer exports: <values><value><source_value/>…
    values_el = step_el.find("values")
    if values_el is not None:
        for value_el in values_el.findall("value"):
            entry = _parse_value_mapping_entry(value_el)
            if entry is not None:
                mappings.append(entry)
    return source, target, mappings, default


def parse_sequence_config(step_el: ET.Element) -> SequenceConfig:
    field_name = (
        _child_text(step_el, "valuename")
        or _child_text(step_el, "fieldname")
        or _child_text(step_el, "field_name")
        or "seq"
    )
    start_raw = (
        _child_text(step_el, "start")
        or _child_text(step_el, "start_at")
        or "1"
    )
    incr_raw = (
        _child_text(step_el, "increment")
        or _child_text(step_el, "increment_by")
        or "1"
    )
    try:
        start_at = int(start_raw or "1")
    except ValueError:
        start_at = 1
    try:
        increment_by = int(incr_raw or "1")
    except ValueError:
        increment_by = 1
    max_raw = (
        _child_text(step_el, "max_value")
        or _child_text(step_el, "maxvalue")
        or _child_text(step_el, "maximum")
    )
    max_value = int(max_raw) if max_raw else None
    use_database = _bool_from_yn(
        _child_text(step_el, "use_database")
        or _child_text(step_el, "usedatabase", "N")
    )
    use_counter = _bool_from_yn(
        _child_text(step_el, "use_counter")
        or _child_text(step_el, "usecounter", "Y"),
        default=True,
    )
    if use_database:
        use_counter = False
    return SequenceConfig(
        field_name=field_name,
        start_at=start_at,
        increment_by=increment_by,
        max_value=max_value,
        use_counter=use_counter,
        use_database=use_database,
        connection=_child_text(step_el, "connection"),
        schema_name=_child_text(step_el, "schema"),
        sequence_name=_child_text(step_el, "seqname")
        or _child_text(step_el, "sequence"),
        counter_name=_child_text(step_el, "counter_name")
        or _child_text(step_el, "countername"),
    )


def parse_system_info_fields(step_el: ET.Element) -> list[tuple[str, str]]:
    """Return list of (output_field_name, system_type_code)."""
    results: list[tuple[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        name = _child_text(field_el, "name")
        sys_type = _child_text(field_el, "type") or _child_text(field_el, "system_type")
        if name and sys_type:
            results.append((name, sys_type))
    return results


def parse_system_info_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get System Info fields into structured metadata."""
    fields = [
        {"name": name, "system_type": sys_type}
        for name, sys_type in parse_system_info_fields(step_el)
    ]
    return {"fields": fields, "output_columns": [f["name"] for f in fields]}


def parse_rank_config_dict(step_el: ET.Element) -> dict[str, Any]:
    """Parse Rank step metadata as a dict."""
    return _metadata_value(parse_rank_config(step_el))


def parse_top_n_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Top N / Limit / Rows Filter metadata."""
    return {
        "nr_lines": _child_text(step_el, "nr_lines")
        or _child_text(step_el, "limit")
        or _child_text(step_el, "nrlines")
        or "10",
        "limit": _child_text(step_el, "limit") or _child_text(step_el, "nr_lines") or "10",
    }


def parse_replace_null_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Replace Nulls field replacements."""
    fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name") or _child_text(field_el, "field")
        if not name:
            continue
        fields.append({
            "name": name,
            "value": _child_text(field_el, "value") or _child_text(field_el, "replace_value"),
            "set_empty_string": _child_text(field_el, "set_empty_string", "N"),
        })
    return {
        "fields": fields,
        "replace_by_value": _child_text(step_el, "replaceByValue")
        or _child_text(step_el, "replace_by_value"),
    }


def parse_file_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse common file/JDBC Output step path + table metadata."""
    file_el = step_el.find("file")
    filename = (
        _child_text(step_el, "filename")
        or _child_text(step_el, "file")
        or _child_text(step_el, "name")
        or _child_text(step_el, "dbname")
        or (_child_text(file_el, "name") if file_el is not None else "")
        or (_child_text(file_el, "filename") if file_el is not None else "")
    )
    return {
        "filename": filename,
        "file": filename,
        "table": _child_text(step_el, "table") or _child_text(step_el, "tablename"),
        "bucket": _child_text(step_el, "bucket"),
        "file_format": _child_text(step_el, "file_format") or _child_text(step_el, "format"),
        "separator": _child_text(step_el, "separator", ","),
        "header": _child_text(step_el, "header", "Y"),
        "encoding": _child_text(step_el, "encoding"),
        "append": _child_text(step_el, "append", "N"),
        "create_file": _child_text(step_el, "create_file") or _child_text(step_el, "create"),
    }


def parse_splunk_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Splunk Input/Output connection metadata (preserved for migration)."""
    cfg: dict[str, Any] = {}
    for tag in (
        "host", "port", "username", "password", "query", "index",
        "sourcetype", "source", "connection", "filename", "app",
    ):
        val = _child_text(step_el, tag)
        if val:
            cfg[tag] = val
    return cfg


def parse_switch_case_rules(step_el: ET.Element) -> tuple[str, list[SwitchCaseRule], str]:
    """Return (switch_field, case_rules, default_target)."""
    switch_field = _child_text(step_el, "fieldname") or _child_text(step_el, "field_name")
    default_target = _child_text(step_el, "default_target_step")
    rules: list[SwitchCaseRule] = []
    cases_el = step_el.find("cases")
    if cases_el is not None:
        for case_el in cases_el.findall("case"):
            rules.append(
                SwitchCaseRule(
                    value=_child_text(case_el, "value"),
                    target_step=_child_text(case_el, "target_step"),
                )
            )
    return switch_field, rules, default_target


def parse_switch_case_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse all Switch / Case XML properties into structured metadata."""
    switch_field, rules, default_target = parse_switch_case_rules(step_el)
    return {
        "fieldname": switch_field,
        "switch_field": switch_field,
        "default_target_step": default_target,
        "use_contains": _bool_from_yn(
            _child_text(step_el, "use_contains") or _child_text(step_el, "is_contains")
        ),
        "case_value_type": _child_text(step_el, "case_value_type"),
        "case_value_format": _child_text(step_el, "case_value_format"),
        "case_value_decimal": _child_text(step_el, "case_value_decimal"),
        "case_value_group": _child_text(step_el, "case_value_group"),
        "cases": [
            {"value": r.value, "target_step": r.target_step}
            for r in rules
        ],
        "rules": _metadata_value(rules),
    }


def parse_abort_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Abort step threshold, message, and logging options."""
    threshold_raw = (
        _child_text(step_el, "row_threshold")
        or _child_text(step_el, "rowThreshold")
        or _child_text(step_el, "threshold")
        or "0"
    )
    try:
        row_threshold = int(float(threshold_raw)) if threshold_raw else 0
    except ValueError:
        row_threshold = 0
    return {
        "row_threshold": row_threshold,
        "row_threshold_raw": threshold_raw,
        "message": (
            _child_text(step_el, "message")
            or _child_text(step_el, "abort_message")
            or "Pentaho Abort step triggered"
        ),
        "always_log_rows": _bool_from_yn(
            _child_text(step_el, "always_log_rows")
            or _child_text(step_el, "alwaysLogRows")
        ),
        "abort_option": (
            _child_text(step_el, "abort_option")
            or _child_text(step_el, "abortOption")
        ),
    }


def parse_append_streams_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Append Streams head/tail ordering metadata."""
    head = (
        _child_text(step_el, "head_name")
        or _child_text(step_el, "headName")
        or _child_text(step_el, "head")
    )
    tail = (
        _child_text(step_el, "tail_name")
        or _child_text(step_el, "tailName")
        or _child_text(step_el, "tail")
    )
    # Info stream subjects sometimes nested under <info>
    info_el = step_el.find("info")
    if info_el is not None:
        streams = [
            _child_text(s, "name") or _text(s)
            for s in info_el.findall("stream")
            if (_child_text(s, "name") or _text(s))
        ]
        if not head and streams:
            head = streams[0]
        if not tail and len(streams) > 1:
            tail = streams[1]
    return {
        "head_name": head,
        "tail_name": tail,
        "stream_order": [s for s in (head, tail) if s],
    }


def parse_block_until_steps_finish_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Block This Step Until Steps Finish dependency list."""
    steps: list[dict[str, str]] = []
    container = step_el.find("steps")
    if container is None:
        container = step_el.find("steplist")
    if container is not None:
        for step_ref in container.findall("step"):
            name = (
                _child_text(step_ref, "name")
                or _child_text(step_ref, "step_name")
                or _text(step_ref)
            )
            if not name:
                continue
            steps.append({
                "name": name,
                "copy_nr": (
                    _child_text(step_ref, "CopyNr")
                    or _child_text(step_ref, "copy_nr")
                    or _child_text(step_ref, "copynr")
                    or "0"
                ),
            })
    return {"wait_steps": steps, "steps": steps}


def parse_blocking_step_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Blocking Step synchronization and temp-file options."""
    return {
        "pass_all_rows": _bool_from_yn(
            _child_text(step_el, "pass_all_rows")
            or _child_text(step_el, "passAllRows"),
            default=True,
        ),
        "directory": _child_text(step_el, "directory") or _child_text(step_el, "directory_path"),
        "prefix": _child_text(step_el, "prefix"),
        "cache_size": _child_text(step_el, "cache_size") or _child_text(step_el, "cacheSize"),
        "compress_files": _bool_from_yn(
            _child_text(step_el, "compress") or _child_text(step_el, "compressFiles")
        ),
    }


def parse_detect_empty_stream_config(step_el: ET.Element) -> dict[str, Any]:
    """Detect Empty Stream has no step-specific options; preserve any raw children."""
    extras = {
        child.tag: (_text(child) if list(child) == [] else ET.tostring(child, encoding="unicode"))
        for child in list(step_el)
        if child.tag not in {"name", "type", "description", "distribute", "custom_distribution",
                             "copies", "partitioning", "remotesteps", "GUI", "attributes"}
    }
    return {"extras": extras}


def parse_dummy_config(step_el: ET.Element) -> dict[str, Any]:
    """Dummy is a no-op; preserve residual non-standard properties if present."""
    return parse_detect_empty_stream_config(step_el)


def _parse_executor_parameters(step_el: ET.Element) -> list[dict[str, str]]:
    params: list[dict[str, str]] = []
    for container_tag in ("parameters", "variable_mappings", "param"):
        container = step_el.find(container_tag) if container_tag != "param" else step_el
        if container is None:
            continue
        for p in container.findall("parameter") + container.findall("variable") + container.findall("param"):
            name = (
                _child_text(p, "name")
                or _child_text(p, "variable")
                or _child_text(p, "parameter")
            )
            if not name and p.tag == "parameter":
                name = _text(p)
            if not name:
                continue
            params.append({
                "name": name,
                "field": _child_text(p, "field") or _child_text(p, "stream_name"),
                "input": _child_text(p, "input") or _child_text(p, "value") or _child_text(p, "static"),
            })
    return params


def parse_meta_inject_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse ETL Metadata Injection target transformation and field mappings."""
    mappings: list[dict[str, str]] = []
    map_el = step_el.find("mappings")
    if map_el is None:
        map_el = step_el.find("meta_inject")
    if map_el is not None:
        for entry in list(map_el):
            if entry.tag not in {"mapping", "meta", "field", "source"}:
                # Flat sibling style under mappings
                continue
            mappings.append({
                "source_field": (
                    _child_text(entry, "source_field")
                    or _child_text(entry, "sourcefield")
                    or _child_text(entry, "source")
                ),
                "target_step": (
                    _child_text(entry, "target_step")
                    or _child_text(entry, "targetstep")
                    or _child_text(entry, "step")
                ),
                "target_attribute": (
                    _child_text(entry, "target_attribute")
                    or _child_text(entry, "target_field")
                    or _child_text(entry, "attribute")
                    or _child_text(entry, "key")
                ),
                "target_detail": _child_text(entry, "detail") or _child_text(entry, "target_detail"),
            })
    # Alternate flat XML used by some exports
    for src, tgt_step, tgt_attr in zip(
        step_el.findall("source_field") or [],
        step_el.findall("target_step") or [],
        step_el.findall("target_attribute") or step_el.findall("target_field") or [],
    ):
        mappings.append({
            "source_field": _text(src),
            "target_step": _text(tgt_step),
            "target_attribute": _text(tgt_attr),
            "target_detail": "",
        })
    return {
        "specification_method": (
            _child_text(step_el, "specification_method")
            or _child_text(step_el, "specificationMethod")
        ),
        "trans_name": (
            _child_text(step_el, "trans_name")
            or _child_text(step_el, "transformation_name")
            or _child_text(step_el, "transName")
        ),
        "filename": (
            _child_text(step_el, "filename")
            or _child_text(step_el, "file_name")
            or _child_text(step_el, "fileName")
        ),
        "directory_path": (
            _child_text(step_el, "directory_path")
            or _child_text(step_el, "directory")
        ),
        "source_step": _child_text(step_el, "source_step") or _child_text(step_el, "sourceStep"),
        "target_file": _child_text(step_el, "target_file") or _child_text(step_el, "targetFile"),
        "no_execution": _bool_from_yn(
            _child_text(step_el, "no_execution") or _child_text(step_el, "noExecution")
        ),
        "stream_source_step": (
            _child_text(step_el, "stream_source_step")
            or _child_text(step_el, "streamSourceStep")
        ),
        "stream_target_step": (
            _child_text(step_el, "stream_target_step")
            or _child_text(step_el, "streamTargetStep")
        ),
        "mappings": mappings,
        "parameters": _parse_executor_parameters(step_el),
    }


def parse_identify_last_row_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Identify Last Row flag field name."""
    return {
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "result_field")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
    }


def parse_java_filter_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Java Filter condition and TRUE/FALSE hop targets."""
    return {
        "condition": _child_text(step_el, "condition") or "true",
        "send_true_to": _child_text(step_el, "send_true_to"),
        "send_false_to": _child_text(step_el, "send_false_to"),
    }


def parse_job_executor_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Job Executor child job path, grouping, and parameter mappings."""
    return {
        "specification_method": (
            _child_text(step_el, "specification_method")
            or _child_text(step_el, "specificationMethod")
        ),
        "job_name": (
            _child_text(step_el, "job_name")
            or _child_text(step_el, "jobName")
            or _child_text(step_el, "name_job")
        ),
        "filename": (
            _child_text(step_el, "filename")
            or _child_text(step_el, "file_name")
            or _child_text(step_el, "fileName")
        ),
        "directory_path": (
            _child_text(step_el, "directory_path")
            or _child_text(step_el, "directory")
        ),
        "group_size": _child_text(step_el, "group_size") or _child_text(step_el, "groupSize"),
        "group_field": _child_text(step_el, "group_field") or _child_text(step_el, "groupField"),
        "group_time": _child_text(step_el, "group_time") or _child_text(step_el, "groupTime"),
        "result_rows_target_step": (
            _child_text(step_el, "result_rows_target_step")
            or _child_text(step_el, "resultRowsTargetStep")
        ),
        "result_files_target_step": (
            _child_text(step_el, "result_files_target_step")
            or _child_text(step_el, "resultFilesTargetStep")
        ),
        "execution_result_target_step": (
            _child_text(step_el, "execution_result_target_step")
            or _child_text(step_el, "executionResultTargetStep")
        ),
        "parameters": _parse_executor_parameters(step_el),
        "inherit_all_variables": _bool_from_yn(
            _child_text(step_el, "inherit_all_vars")
            or _child_text(step_el, "parameters_inherit_all")
            or _child_text(step_el, "inheritingAllVariables")
        ),
    }


def parse_prioritize_streams_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Prioritize Streams ordered input stream list."""
    streams: list[str] = []
    for tag in ("step", "stream", "priority"):
        for el in step_el.findall(tag):
            name = _child_text(el, "name") or _child_text(el, "step") or _text(el)
            if name and name not in streams:
                streams.append(name)
    container = step_el.find("steps")
    if container is None:
        container = step_el.find("info")
    if container is not None:
        for el in list(container):
            name = _child_text(el, "name") or _text(el)
            if name and name not in streams:
                streams.append(name)
    return {"stream_priority": streams, "streams": streams}


def parse_single_threader_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Single Threader sub-transformation and inject/retrieve steps."""
    return {
        "specification_method": (
            _child_text(step_el, "specification_method")
            or _child_text(step_el, "specificationMethod")
        ),
        "trans_name": (
            _child_text(step_el, "trans_name")
            or _child_text(step_el, "transformation_name")
            or _child_text(step_el, "transName")
        ),
        "filename": (
            _child_text(step_el, "filename")
            or _child_text(step_el, "file_name")
            or _child_text(step_el, "fileName")
        ),
        "directory_path": (
            _child_text(step_el, "directory_path")
            or _child_text(step_el, "directory")
        ),
        "batch_size": _child_text(step_el, "batch_size") or _child_text(step_el, "batchSize"),
        "batch_time": _child_text(step_el, "batch_time") or _child_text(step_el, "batchTime"),
        "inject_step": (
            _child_text(step_el, "inject_step")
            or _child_text(step_el, "injectStep")
        ),
        "retrieve_step": (
            _child_text(step_el, "retrieve_step")
            or _child_text(step_el, "retrieveStep")
        ),
        "parameters": _parse_executor_parameters(step_el),
        "pass_parameters": _bool_from_yn(
            _child_text(step_el, "passing_parameter_values")
            or _child_text(step_el, "passParameters")
        ),
    }


def parse_trans_executor_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Transformation Executor child transformation and parameter mappings."""
    return {
        "specification_method": (
            _child_text(step_el, "specification_method")
            or _child_text(step_el, "specificationMethod")
        ),
        "trans_name": (
            _child_text(step_el, "trans_name")
            or _child_text(step_el, "transformation_name")
            or _child_text(step_el, "transName")
        ),
        "filename": (
            _child_text(step_el, "filename")
            or _child_text(step_el, "file_name")
            or _child_text(step_el, "fileName")
        ),
        "directory_path": (
            _child_text(step_el, "directory_path")
            or _child_text(step_el, "directory")
        ),
        "group_size": _child_text(step_el, "group_size") or _child_text(step_el, "groupSize"),
        "group_field": _child_text(step_el, "group_field") or _child_text(step_el, "groupField"),
        "group_time": _child_text(step_el, "group_time") or _child_text(step_el, "groupTime"),
        "result_rows_target_step": (
            _child_text(step_el, "result_rows_target_step")
            or _child_text(step_el, "resultRowsTargetStep")
            or _child_text(step_el, "output_rows_source_step")
        ),
        "result_files_target_step": (
            _child_text(step_el, "result_files_target_step")
            or _child_text(step_el, "resultFilesTargetStep")
        ),
        "execution_result_target_step": (
            _child_text(step_el, "execution_result_target_step")
            or _child_text(step_el, "executionResultTargetStep")
        ),
        "parameters": _parse_executor_parameters(step_el),
        "inherit_all_variables": _bool_from_yn(
            _child_text(step_el, "inherit_all_vars")
            or _child_text(step_el, "parameters_inherit_all")
            or _child_text(step_el, "inheritingAllVariables")
        ),
    }


def _parse_mapping_io_definition(mapping_el: ET.Element | None) -> dict[str, Any]:
    """Parse one MappingIODefinition ``<mapping>`` node (input or output path)."""
    if mapping_el is None:
        return {
            "input_step": "",
            "output_step": "",
            "main_path": False,
            "rename_on_output": False,
            "description": "",
            "connectors": [],
        }
    connectors: list[dict[str, str]] = []
    for conn in mapping_el.findall("connector"):
        parent = (
            _child_text(conn, "parent")
            or _child_text(conn, "field")
            or _child_text(conn, "source")
        )
        child = (
            _child_text(conn, "child")
            or _child_text(conn, "mapping")
            or _child_text(conn, "target")
        )
        if parent or child:
            connectors.append({"parent": parent, "child": child or parent})
    return {
        "input_step": _child_text(mapping_el, "input_step"),
        "output_step": _child_text(mapping_el, "output_step"),
        "main_path": _bool_from_yn(_child_text(mapping_el, "main_path")),
        "rename_on_output": _bool_from_yn(_child_text(mapping_el, "rename_on_output")),
        "description": _child_text(mapping_el, "description"),
        "connectors": connectors,
    }


def _parse_mapping_parameters_node(params_el: ET.Element | None) -> dict[str, Any]:
    """Parse Mapping ``<parameters>`` with ``<variablemapping>`` entries."""
    parameters: list[dict[str, str]] = []
    inherit_all = True
    if params_el is None:
        return {"parameters": parameters, "inherit_all_variables": inherit_all}
    inherit_all = _bool_from_yn(
        _child_text(params_el, "inherit_all_vars")
        or _child_text(params_el, "inherit_all_variables")
        or _child_text(params_el, "inheritingAllVariables"),
        default=True,
    )
    for vm in params_el.findall("variablemapping") + params_el.findall("variable_mapping"):
        parameters.append({
            "variable": (
                _child_text(vm, "variable")
                or _child_text(vm, "name")
                or _child_text(vm, "parameter")
            ),
            "input": (
                _child_text(vm, "input")
                or _child_text(vm, "value")
                or _child_text(vm, "field")
            ),
        })
    # Fallback: executor-style parameter children mixed into Mapping XML
    for p in params_el.findall("parameter") + params_el.findall("param"):
        name = (
            _child_text(p, "name")
            or _child_text(p, "variable")
            or _child_text(p, "parameter")
        )
        if not name:
            continue
        parameters.append({
            "variable": name,
            "input": _child_text(p, "input") or _child_text(p, "value") or _child_text(p, "field"),
        })
    return {"parameters": parameters, "inherit_all_variables": inherit_all}


def _parse_legacy_mapping_connectors(container: ET.Element | None) -> list[dict[str, str]]:
    """Legacy Mapping XML used ``<connector><field/><mapping/></connector>`` under input/output."""
    if container is None:
        return []
    connectors: list[dict[str, str]] = []
    for conn in container.findall("connector"):
        parent = (
            _child_text(conn, "field")
            or _child_text(conn, "parent")
            or _child_text(conn, "source")
        )
        child = (
            _child_text(conn, "mapping")
            or _child_text(conn, "child")
            or _child_text(conn, "target")
        )
        if parent or child:
            connectors.append({"parent": parent, "child": child or parent})
    return connectors


def _parse_mapping_io_list(container: ET.Element | None) -> list[dict[str, Any]]:
    """Parse all ``<mapping>`` IO definitions under an ``<input>`` or ``<output>`` node."""
    if container is None:
        return []
    definitions = [
        _parse_mapping_io_definition(m)
        for m in container.findall("mapping")
    ]
    if definitions:
        return definitions
    # Legacy flat connectors → synthesize one main-path definition
    legacy = _parse_legacy_mapping_connectors(container)
    if legacy:
        return [{
            "input_step": "",
            "output_step": "",
            "main_path": True,
            "rename_on_output": True,
            "description": "",
            "connectors": legacy,
        }]
    return []


def _parse_mapping_location(step_el: ET.Element) -> dict[str, Any]:
    """Shared child-transformation location fields for Mapping / Simple Mapping."""
    return {
        "specification_method": (
            _child_text(step_el, "specification_method")
            or _child_text(step_el, "specificationMethod")
        ),
        "trans_name": (
            _child_text(step_el, "trans_name")
            or _child_text(step_el, "transformation_name")
            or _child_text(step_el, "transName")
        ),
        "filename": (
            _child_text(step_el, "filename")
            or _child_text(step_el, "file_name")
            or _child_text(step_el, "fileName")
        ),
        "directory_path": (
            _child_text(step_el, "directory_path")
            or _child_text(step_el, "directory")
        ),
        "trans_object_id": (
            _child_text(step_el, "trans_object_id")
            or _child_text(step_el, "transObjectId")
        ),
    }


_MAPPING_KNOWN_TAGS = frozenset({
    "name", "type", "description", "distribute", "custom_distribution",
    "copies", "partitioning", "remotesteps", "GUI", "attributes",
    "specification_method", "specificationMethod",
    "trans_name", "transformation_name", "transName",
    "filename", "file_name", "fileName",
    "directory_path", "directory",
    "trans_object_id", "transObjectId",
    "mappings", "input", "output", "parameters",
    "allow_multiple_input", "allow_multiple_output",
    "fields", "select_unspecified",
})


def _mapping_residual_extras(step_el: ET.Element) -> dict[str, Any]:
    """Preserve unrecognized Mapping step children so no Pentaho property is dropped."""
    extras: dict[str, Any] = {}
    for child in list(step_el):
        if child.tag in _MAPPING_KNOWN_TAGS:
            continue
        if list(child) == []:
            extras[child.tag] = _text(child)
        else:
            extras[child.tag] = ET.tostring(child, encoding="unicode")
    return extras


def parse_mapping_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Mapping (Sub-transformation) including multi I/O paths and parameters."""
    mappings_el = step_el.find("mappings")
    input_mappings: list[dict[str, Any]] = []
    output_mappings: list[dict[str, Any]] = []
    param_info: dict[str, Any] = {"parameters": [], "inherit_all_variables": True}

    if mappings_el is not None:
        input_mappings = _parse_mapping_io_list(mappings_el.find("input"))
        output_mappings = _parse_mapping_io_list(mappings_el.find("output"))
        param_info = _parse_mapping_parameters_node(mappings_el.find("parameters"))
    else:
        # Very old exports kept input/output connectors at step root
        input_mappings = _parse_mapping_io_list(step_el.find("input"))
        output_mappings = _parse_mapping_io_list(step_el.find("output"))
        param_info = _parse_mapping_parameters_node(step_el.find("parameters"))

    allow_multi_input = _child_text(step_el, "allow_multiple_input")
    allow_multi_output = _child_text(step_el, "allow_multiple_output")

    cfg = {
        **_parse_mapping_location(step_el),
        "input_mappings": input_mappings,
        "output_mappings": output_mappings,
        "parameters": param_info["parameters"],
        "inherit_all_variables": param_info["inherit_all_variables"],
        "allow_multiple_input": (
            _bool_from_yn(allow_multi_input, default=len(input_mappings) > 1)
            if allow_multi_input
            else len(input_mappings) > 1
        ),
        "allow_multiple_output": (
            _bool_from_yn(allow_multi_output, default=len(output_mappings) > 1)
            if allow_multi_output
            else len(output_mappings) > 1
        ),
    }
    extras = _mapping_residual_extras(step_el)
    if extras:
        cfg["extras"] = extras
    return cfg


def parse_simple_mapping_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Simple Mapping (Sub-transformation) — single input and output path."""
    mappings_el = step_el.find("mappings")
    input_mapping: dict[str, Any] = _parse_mapping_io_definition(None)
    output_mapping: dict[str, Any] = _parse_mapping_io_definition(None)
    param_info: dict[str, Any] = {"parameters": [], "inherit_all_variables": True}

    if mappings_el is not None:
        input_node = mappings_el.find("input")
        output_node = mappings_el.find("output")
        if input_node is not None:
            mapping_el = input_node.find("mapping")
            if mapping_el is not None:
                input_mapping = _parse_mapping_io_definition(mapping_el)
            else:
                legacy = _parse_legacy_mapping_connectors(input_node)
                if legacy:
                    input_mapping = {
                        "input_step": "",
                        "output_step": "",
                        "main_path": True,
                        "rename_on_output": True,
                        "description": "",
                        "connectors": legacy,
                    }
        if output_node is not None:
            mapping_el = output_node.find("mapping")
            if mapping_el is not None:
                output_mapping = _parse_mapping_io_definition(mapping_el)
            else:
                legacy = _parse_legacy_mapping_connectors(output_node)
                if legacy:
                    output_mapping = {
                        "input_step": "",
                        "output_step": "",
                        "main_path": True,
                        "rename_on_output": False,
                        "description": "",
                        "connectors": legacy,
                    }
        param_info = _parse_mapping_parameters_node(mappings_el.find("parameters"))
    else:
        param_info = _parse_mapping_parameters_node(step_el.find("parameters"))

    # Normalize for consumers that iterate list-style mappings
    input_mappings = [input_mapping] if (
        input_mapping.get("connectors")
        or input_mapping.get("input_step")
        or input_mapping.get("output_step")
        or input_mapping.get("main_path")
    ) else []
    output_mappings = [output_mapping] if (
        output_mapping.get("connectors")
        or output_mapping.get("input_step")
        or output_mapping.get("output_step")
        or output_mapping.get("main_path")
    ) else []

    cfg = {
        **_parse_mapping_location(step_el),
        "input_mapping": input_mapping,
        "output_mapping": output_mapping,
        "input_mappings": input_mappings or [input_mapping],
        "output_mappings": output_mappings or [output_mapping],
        "parameters": param_info["parameters"],
        "inherit_all_variables": param_info["inherit_all_variables"],
        "allow_multiple_input": False,
        "allow_multiple_output": False,
    }
    extras = _mapping_residual_extras(step_el)
    if extras:
        cfg["extras"] = extras
    return cfg


def parse_mapping_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Mapping Input Specification field contract and unspecified-field flag."""
    fields_el = step_el.find("fields")
    fields: list[dict[str, Any]] = []
    select_unspecified = False
    if fields_el is not None:
        select_unspecified = _bool_from_yn(
            _child_text(fields_el, "select_unspecified")
            or _child_text(step_el, "select_unspecified")
        )
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            length_raw = _child_text(field_el, "length")
            precision_raw = _child_text(field_el, "precision")
            try:
                length = int(length_raw) if length_raw not in ("", None) else -1
            except ValueError:
                length = -1
            try:
                precision = int(precision_raw) if precision_raw not in ("", None) else -1
            except ValueError:
                precision = -1
            # Specified fields are the required contract; optional extras only when
            # select_unspecified is enabled. Preserve any explicit required/default tags.
            optional_tag = _child_text(field_el, "optional")
            required_tag = (
                _child_text(field_el, "required")
                or _child_text(field_el, "mandatory")
            )
            if optional_tag:
                required = not _bool_from_yn(optional_tag, default=False)
            elif required_tag:
                required = _bool_from_yn(required_tag, default=True)
            else:
                required = True
            fields.append({
                "name": name,
                "type": _child_text(field_el, "type") or "String",
                "length": length,
                "precision": precision,
                "default_value": (
                    _child_text(field_el, "default")
                    or _child_text(field_el, "default_value")
                    or _child_text(field_el, "nullif")
                ),
                "required": required,
                "optional": not required,
            })
    else:
        select_unspecified = _bool_from_yn(_child_text(step_el, "select_unspecified"))

    cfg = {
        "fields": fields,
        "field_names": [f["name"] for f in fields],
        "select_unspecified": select_unspecified,
        "include_unspecified_fields": select_unspecified,
    }
    extras = _mapping_residual_extras(step_el)
    if extras:
        cfg["extras"] = extras
    return cfg


def parse_mapping_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Mapping Output Specification — usually empty; preserve any declared fields."""
    fields_el = step_el.find("fields")
    fields: list[dict[str, Any]] = []
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append({
                "name": name,
                "type": _child_text(field_el, "type") or "",
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
                "rename": (
                    _child_text(field_el, "rename")
                    or _child_text(field_el, "target")
                    or _child_text(field_el, "child")
                ),
            })
    # Runtime renames are injected by the parent Mapping step; capture placeholders.
    renames: list[dict[str, str]] = []
    for conn in step_el.findall("connector") + (
        fields_el.findall("connector") if fields_el is not None else []
    ):
        parent = _child_text(conn, "parent") or _child_text(conn, "source")
        child = _child_text(conn, "child") or _child_text(conn, "target")
        if parent or child:
            renames.append({"parent": parent, "child": child or parent})

    cfg = {
        "fields": fields,
        "field_names": [f["name"] for f in fields],
        "output_columns": [f["name"] for f in fields],
        "renames": renames,
    }
    extras = _mapping_residual_extras(step_el)
    if extras:
        cfg["extras"] = extras
    return cfg


def parse_rank_config(step_el: ET.Element) -> RankConfig:
    return RankConfig(
        top_bottom=_child_text(step_el, "top_bottom", "top").lower(),
        rank=_child_text(step_el, "rank", "Y").upper() == "Y",
        sort_size=int(_child_text(step_el, "sort_size", "10") or "10"),
        field_name=_child_text(step_el, "field_name"),
        rank_field=_child_text(step_el, "rank_name", "rank") or "rank",
    )


def parse_javascript_script(step_el: ET.Element) -> str:
    for tag in ("jsScripts_script", "script", "javascript"):
        el = step_el.find(tag)
        if el is not None and (el.text or "").strip():
            return (el.text or "").strip()
    scripts_el = step_el.find("jsScripts")
    if scripts_el is not None:
        for script_el in scripts_el.findall("jsScript"):
            body = _child_text(script_el, "jsScript_script") or _text(script_el)
            if body:
                return body
    return _child_text(step_el, "script")


def parse_filter_condition_tree(node: ET.Element | None) -> dict[str, Any] | None:
    """Parse a Filter Rows condition node into a structured dict."""
    if node is None:
        return None
    result: dict[str, Any] = {}
    for tag in ("negated", "operator", "leftvalue", "rightvalue"):
        val = _child_text(node, tag)
        if val:
            result[tag] = val
    func = _child_text(node, "function")
    if func:
        result["function"] = unescape_xml(func)
    value_el = node.find("value")
    if value_el is not None:
        type_name, text_val = parse_value_constant(value_el)
        value_dict: dict[str, str] = {"type": type_name, "text": text_val}
        for extra in ("name", "isnull"):
            extra_val = _child_text(value_el, extra)
            if extra_val:
                value_dict[extra] = extra_val
        result["value"] = value_dict
    conditions_el = node.find("conditions")
    if conditions_el is not None:
        children = [
            parse_filter_condition_tree(child)
            for child in conditions_el.findall("condition")
        ]
        result["conditions"] = [c for c in children if c]
    return result


def parse_filter_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse all Filter Rows XML properties."""
    root = parse_filter_compare_element(step_el)
    return {
        "compare_value": _child_text(step_el, "compare_value"),
        "send_true_to": _child_text(step_el, "send_true_to"),
        "send_false_to": _child_text(step_el, "send_false_to"),
        "condition": parse_filter_condition_tree(root),
    }


def parse_row_generator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Row Generator / Data Grid step metadata."""
    columns, rows = parse_data_grid_rows(step_el)
    try:
        limit = int(_child_text(step_el, "limit", "1") or "1")
    except ValueError:
        limit = 1
    return {
        "limit": limit,
        "fields": _metadata_value(parse_row_generator_fields(step_el)),
        "columns": columns,
        "rows": rows,
        "never_ending": _bool_from_yn(_child_text(step_el, "never_ending")),
        "interval_in_ms": _child_text(step_el, "interval_in_ms"),
        "row_time_field": _child_text(step_el, "row_time_field"),
        "last_time_field": _child_text(step_el, "last_time_field"),
    }


def parse_merge_join_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Merge Join step metadata including stream names and key lists."""
    keys1_el = step_el.find("keys_1")
    keys2_el = step_el.find("keys_2")
    keys_1 = [_text(k) for k in (keys1_el.findall("key") if keys1_el is not None else []) if _text(k)]
    keys_2 = [_text(k) for k in (keys2_el.findall("key") if keys2_el is not None else []) if _text(k)]
    cfg = MergeJoinConfig(
        join_type=_child_text(step_el, "join_type", "INNER"),
        step1=_child_text(step_el, "step1"),
        step2=_child_text(step_el, "step2"),
        keys=parse_join_keys(step_el),
        keys_1=keys_1,
        keys_2=keys_2,
    )
    return _metadata_value(cfg)


def parse_join_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Join Rows (Cartesian Product) temp-cache / main-stream / condition XML."""
    cache_raw = _child_text(step_el, "cache_size", "500")
    try:
        cache_size = int(cache_raw) if cache_raw else 500
    except ValueError:
        cache_size = 500

    condition: dict[str, Any] | None = None
    compare_el = parse_filter_compare_element(step_el)
    if compare_el is not None:
        condition = parse_filter_condition_tree(compare_el)
    else:
        cond_el = step_el.find("condition")
        if cond_el is not None:
            condition = parse_filter_condition_tree(cond_el)

    cfg = JoinRowsConfig(
        directory=_child_text(step_el, "directory"),
        prefix=_child_text(step_el, "prefix"),
        cache_size=cache_size,
        main_step=_child_text(step_el, "main"),
        condition=condition,
    )
    return _metadata_value(cfg)


def parse_merge_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Merge Rows (diff) reference/compare streams, keys, and value fields."""
    key_fields: list[str] = []
    keys_el = step_el.find("keys")
    if keys_el is not None:
        for key_el in keys_el.findall("key"):
            name = _text(key_el) or _child_text(key_el, "name")
            if name:
                key_fields.append(name)
    if not key_fields:
        for key_el in step_el.findall("key"):
            name = _text(key_el) if list(key_el) == [] else (
                _child_text(key_el, "name") or _child_text(key_el, "field")
            )
            if name:
                key_fields.append(name)

    value_fields: list[str] = []
    values_el = step_el.find("values")
    if values_el is not None:
        for value_el in values_el.findall("value"):
            name = _text(value_el) or _child_text(value_el, "name")
            if name:
                value_fields.append(name)
    if not value_fields:
        for value_el in step_el.findall("value"):
            name = _text(value_el) if list(value_el) == [] else _child_text(value_el, "name")
            if name:
                value_fields.append(name)

    pairs = parse_join_keys(step_el)
    if not pairs and key_fields:
        pairs = [JoinKeyPair(left=k, right=k) for k in key_fields]

    cfg = MergeRowsConfig(
        flag_field=_child_text(step_el, "flag_field", "flagfield") or "flagfield",
        reference=_child_text(step_el, "reference"),
        compare=_child_text(step_el, "compare"),
        key_fields=key_fields,
        value_fields=value_fields,
        keys=pairs,
    )
    return _metadata_value(cfg)


def parse_multiway_merge_join_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Multiway Merge Join input steps and per-stream key field lists."""
    key_fields: list[str] = []
    keys_el = step_el.find("keys")
    if keys_el is not None:
        for key_el in keys_el.findall("key"):
            name = _text(key_el)
            if name:
                key_fields.append(name)

    number_raw = _child_text(step_el, "number_input", "0")
    try:
        number_input = int(number_raw) if number_raw else 0
    except ValueError:
        number_input = 0

    input_steps: list[str] = []
    if number_input > 0:
        for index in range(number_input):
            name = _child_text(step_el, f"step{index}")
            if name:
                input_steps.append(name)
    else:
        index = 0
        while True:
            name = _child_text(step_el, f"step{index}")
            if not name:
                break
            input_steps.append(name)
            index += 1
            if index > 32:
                break

    if not number_input:
        number_input = len(input_steps)

    cfg = MultiwayMergeJoinConfig(
        join_type=_child_text(step_el, "join_type", "INNER") or "INNER",
        number_input=number_input,
        input_steps=input_steps,
        key_fields=key_fields,
    )
    return _metadata_value(cfg)


def parse_sorted_merge_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Sorted Merge ordering fields (same field XML shape as Sort Rows)."""
    sort_cfg = parse_sort_rows_config(step_el)
    cfg = SortedMergeConfig(sort_fields=list(sort_cfg.get("sort_fields") or []))
    return _metadata_value(cfg)


def parse_xml_join_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse XML Join target/source streams, XPath, and fragment options."""
    cfg = XMLJoinConfig(
        value_xml_field=(
            _child_text(step_el, "valueXMLfield")
            or _child_text(step_el, "valueXMLField")
        ),
        target_xml_step=_child_text(step_el, "targetXMLstep"),
        target_xml_field=_child_text(step_el, "targetXMLfield"),
        source_xml_step=_child_text(step_el, "sourceXMLstep"),
        source_xml_field=_child_text(step_el, "sourceXMLfield"),
        target_xpath=_child_text(step_el, "targetXPath"),
        join_compare_field=_child_text(step_el, "joinCompareField"),
        encoding=_child_text(step_el, "encoding"),
        complex_join=_bool_from_yn(_child_text(step_el, "complexJoin")),
        omit_xml_header=_bool_from_yn(_child_text(step_el, "omitXMLHeader")),
        omit_null_values=_bool_from_yn(_child_text(step_el, "omitNullValues")),
    )
    return _metadata_value(cfg)


def parse_formula_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Formula step metadata including nested formula_string entries.

    Supports:
    - flat ``<field_name>`` + ``<formula>`` on the step
    - ``<formula><field_name/><formula_string/></formula>``
    - Spoon exports: ``<formula><field><field_name/>…</field></formula>``
    """
    formulas: list[dict[str, str]] = []

    def _append_formula_el(el: ET.Element) -> None:
        field_name = _child_text(el, "field_name")
        formula_string = (
            _child_text(el, "formula_string")
            or _child_text(el, "formula")
        )
        if not (field_name or formula_string):
            return
        formulas.append({
            "field_name": field_name or "formula_result",
            "formula": unescape_xml(formula_string),
            "value_type": _child_text(el, "value_type"),
            "replace_field": _child_text(el, "replace_field"),
            "length": (
                _child_text(el, "length")
                or _child_text(el, "value_length")
            ),
            "precision": (
                _child_text(el, "precision")
                or _child_text(el, "value_precision")
            ),
        })

    for formula_el in step_el.findall("formula"):
        nested_fields = formula_el.findall("field")
        if nested_fields:
            for field_el in nested_fields:
                _append_formula_el(field_el)
        else:
            _append_formula_el(formula_el)

    if not formulas:
        flat_formula = unescape_xml(_child_text(step_el, "formula"))
        flat_field = _child_text(step_el, "field_name")
        if flat_formula:
            formulas.append({
                "field_name": flat_field or "formula_result",
                "formula": flat_formula,
                "value_type": _child_text(step_el, "value_type"),
                "replace_field": _child_text(step_el, "replace_field"),
                "length": (
                    _child_text(step_el, "length")
                    or _child_text(step_el, "value_length")
                ),
                "precision": (
                    _child_text(step_el, "precision")
                    or _child_text(step_el, "value_precision")
                ),
            })

    primary = formulas[0] if formulas else {}
    return {
        "formulas": formulas,
        "field_name": primary.get("field_name", ""),
        "formula": primary.get("formula", ""),
        "value_type": primary.get("value_type", ""),
    }


def parse_exec_sql_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Execute SQL Script connection, SQL, args, and transaction settings."""
    arguments: list[str] = []
    args_el = step_el.find("arguments")
    if args_el is not None:
        for arg_el in args_el.findall("argument"):
            name = (
                _child_text(arg_el, "name")
                or _child_text(arg_el, "argument")
                or _text(arg_el)
            )
            if name:
                arguments.append(name)
    # Flat argument_name nodes (older exports)
    for arg_el in step_el.findall("argument"):
        name = _child_text(arg_el, "name") or _text(arg_el)
        if name and name not in arguments:
            arguments.append(name)

    sql = unescape_xml(
        _child_text(step_el, "sql")
        or _child_text(step_el, "SQL")
        or _child_text(step_el, "sql_script")
    )
    return {
        "connection": _child_text(step_el, "connection"),
        "sql": sql,
        "execute_each_row": _bool_from_yn(
            _child_text(step_el, "execute_each_row")
            or _child_text(step_el, "executedEachInputRow")
        ),
        "single_statement": _bool_from_yn(
            _child_text(step_el, "single_statement")
            or _child_text(step_el, "singleStatement")
        ),
        "replace_variables": _bool_from_yn(
            _child_text(step_el, "replace_variables")
            or _child_text(step_el, "replaceVariables")
            or _child_text(step_el, "variables_active")
        ),
        "set_params": _bool_from_yn(
            _child_text(step_el, "set_params") or _child_text(step_el, "setParams")
        ),
        "quote_string": _bool_from_yn(
            _child_text(step_el, "quoteString") or _child_text(step_el, "quote_string")
        ),
        "arguments": arguments,
        "insert_field": _child_text(step_el, "insert_field") or _child_text(step_el, "insertField"),
        "update_field": _child_text(step_el, "update_field") or _child_text(step_el, "updateField"),
        "delete_field": _child_text(step_el, "delete_field") or _child_text(step_el, "deleteField"),
        "read_field": _child_text(step_el, "read_field") or _child_text(step_el, "readField"),
    }


def parse_exec_sql_row_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Execute Row SQL Script template / per-row SQL field configuration."""
    return {
        "connection": _child_text(step_el, "connection"),
        "sql_field": (
            _child_text(step_el, "sql_field")
            or _child_text(step_el, "sqlfield")
            or _child_text(step_el, "sqlFieldName")
            or _child_text(step_el, "sqlfieldname")
        ),
        "sql_from_file": _bool_from_yn(
            _child_text(step_el, "sqlFromfile")
            or _child_text(step_el, "sql_from_file")
            or _child_text(step_el, "sqlFromFile")
        ),
        "sql_filename_field": (
            _child_text(step_el, "sqlfilenamefield")
            or _child_text(step_el, "sqlFileNameField")
            or _child_text(step_el, "sql_filename_field")
        ),
        "send_one_statement": _bool_from_yn(
            _child_text(step_el, "sendOneStatement")
            or _child_text(step_el, "send_one_statement")
            or _child_text(step_el, "single_statement"),
            default=True,
        ),
        "commit": _child_text(step_el, "commit") or "1",
        "insert_field": _child_text(step_el, "insert_field") or _child_text(step_el, "insertField"),
        "update_field": _child_text(step_el, "update_field") or _child_text(step_el, "updateField"),
        "delete_field": _child_text(step_el, "delete_field") or _child_text(step_el, "deleteField"),
        "read_field": _child_text(step_el, "read_field") or _child_text(step_el, "readField"),
        "sql": unescape_xml(_child_text(step_el, "sql")),
    }


def parse_javascript_value_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Modified Java Script Value scripts and output field definitions."""
    scripts: list[dict[str, Any]] = []
    scripts_el = step_el.find("jsScripts")
    if scripts_el is not None:
        for script_el in scripts_el.findall("jsScript"):
            body = (
                _child_text(script_el, "jsScript_script")
                or _child_text(script_el, "script")
                or _text(script_el)
            )
            scripts.append({
                "name": _child_text(script_el, "jsScript_name") or _child_text(script_el, "name"),
                "type": (
                    _child_text(script_el, "jsScript_type")
                    or _child_text(script_el, "type")
                    or "0"
                ),
                "script": unescape_xml(body),
            })
    if not scripts:
        body = parse_javascript_script(step_el)
        if body:
            scripts.append({"name": "Script 1", "type": "0", "script": body})

    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append({
                "name": name,
                "rename": _child_text(field_el, "rename") or name,
                "type": _child_text(field_el, "type"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
                "replace": _bool_from_yn(_child_text(field_el, "replace")),
            })

    transform = next(
        (s["script"] for s in scripts if str(s.get("type", "0")) in ("0", "transform", "")),
        scripts[0]["script"] if scripts else "",
    )
    return {
        "scripts": scripts,
        "script": transform,
        "fields": fields,
        "optimization_level": (
            _child_text(step_el, "optimizationLevel")
            or _child_text(step_el, "optimization_level")
        ),
        "compatible": _bool_from_yn(_child_text(step_el, "compatible")),
    }


def parse_regex_eval_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Regex Evaluation pattern, flags, matcher, and capture-group fields."""
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append({
                "name": name,
                "type": _child_text(field_el, "type"),
                "format": _child_text(field_el, "format"),
                "group": _child_text(field_el, "group"),
                "decimal": _child_text(field_el, "decimal"),
                "currency": _child_text(field_el, "currency"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
                "nullif": _child_text(field_el, "nullif"),
                "ifnull": _child_text(field_el, "ifnull"),
                "trim_type": _child_text(field_el, "trimtype") or _child_text(field_el, "trim_type"),
            })

    pattern = unescape_xml(
        _child_text(step_el, "script")
        or _child_text(step_el, "regex")
        or _child_text(step_el, "pattern")
    )
    return {
        "matcher": (
            _child_text(step_el, "matcher")
            or _child_text(step_el, "field")
            or _child_text(step_el, "fieldToMatch")
        ),
        "pattern": pattern,
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
        "use_variable_interpolation": _bool_from_yn(
            _child_text(step_el, "usevar") or _child_text(step_el, "useVar")
        ),
        "allow_capture_groups": _bool_from_yn(
            _child_text(step_el, "allowcapturegroups")
            or _child_text(step_el, "allowCaptureGroups")
            or _child_text(step_el, "allowvar"),
            default=bool(fields),
        ),
        "replace_fields": _bool_from_yn(
            _child_text(step_el, "replacefields") or _child_text(step_el, "replaceFields")
        ),
        "case_insensitive": _bool_from_yn(
            _child_text(step_el, "caseinsensitive") or _child_text(step_el, "caseInsensitive")
        ),
        "canon_eq": _bool_from_yn(
            _child_text(step_el, "canoneq") or _child_text(step_el, "canonEq")
        ),
        "comment": _bool_from_yn(_child_text(step_el, "comment")),
        "dotall": _bool_from_yn(_child_text(step_el, "dotall") or _child_text(step_el, "dotAll")),
        "multiline": _bool_from_yn(
            _child_text(step_el, "multiline") or _child_text(step_el, "multiLine")
        ),
        "unicode": _bool_from_yn(_child_text(step_el, "unicode")),
        "unix_lines": _bool_from_yn(
            _child_text(step_el, "unix") or _child_text(step_el, "unixLines")
        ),
        "fields": fields,
    }


def parse_rules_accumulator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Rules Accumulator Drools rule source and output column metadata."""
    return _parse_rules_common(step_el, kind="accumulator")


def parse_rules_executor_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Rules Executor Drools rule source, order, and result columns."""
    return _parse_rules_common(step_el, kind="executor")


def _parse_rules_common(step_el: ET.Element, *, kind: str) -> dict[str, Any]:
    result_columns: list[dict[str, Any]] = []
    for container_tag in ("fields", "result-columns", "columns"):
        container = step_el.find(container_tag)
        if container is None:
            continue
        for field_el in list(container):
            name = (
                _child_text(field_el, "name")
                or _child_text(field_el, "column-name")
                or _child_text(field_el, "field_name")
            )
            if not name:
                continue
            result_columns.append({
                "name": name,
                "type": _child_text(field_el, "type") or _child_text(field_el, "column-type"),
            })

    rule_definition = unescape_xml(
        _child_text(step_el, "rule-definition")
        or _child_text(step_el, "ruleDefinition")
        or _child_text(step_el, "rules")
        or _child_text(step_el, "script")
    )
    return {
        "kind": kind,
        "rule_file": (
            _child_text(step_el, "rule-file")
            or _child_text(step_el, "ruleFile")
            or _child_text(step_el, "filename")
        ),
        "rule_definition": rule_definition,
        "keep_input_fields": _bool_from_yn(
            _child_text(step_el, "keep-input-fields")
            or _child_text(step_el, "keepInputFields"),
            default=True,
        ),
        "rule_source": (
            _child_text(step_el, "ruleSource")
            or _child_text(step_el, "rule-source")
            or ("file" if _child_text(step_el, "rule-file") else "definition")
        ),
        "result_columns": result_columns,
    }


def parse_user_defined_java_class_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse User Defined Java Class definitions, imports, and output fields."""
    definitions: list[dict[str, Any]] = []
    defs_el = step_el.find("definitions")
    if defs_el is not None:
        for def_el in defs_el.findall("definition"):
            definitions.append({
                "class_type": (
                    _child_text(def_el, "class_type")
                    or _child_text(def_el, "classType")
                    or "TRANSFORM_CLASS"
                ),
                "class_name": (
                    _child_text(def_el, "class_name")
                    or _child_text(def_el, "className")
                    or "Processor"
                ),
                "class_source": unescape_xml(
                    _child_text(def_el, "class_source")
                    or _child_text(def_el, "classSource")
                    or _text(def_el)
                ),
            })

    if not definitions:
        source = unescape_xml(
            _child_text(step_el, "class_source")
            or _child_text(step_el, "source")
            or _child_text(step_el, "code")
        )
        if source:
            definitions.append({
                "class_type": "TRANSFORM_CLASS",
                "class_name": _child_text(step_el, "class_name") or "Processor",
                "class_source": source,
            })

    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = (
                _child_text(field_el, "field_name")
                or _child_text(field_el, "name")
            )
            if not name:
                continue
            fields.append({
                "name": name,
                "type": _child_text(field_el, "field_type") or _child_text(field_el, "type"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
            })

    info_steps: list[str] = []
    info_el = step_el.find("info_steps") or step_el.find("info")
    if info_el is not None:
        for child in list(info_el):
            name = _child_text(child, "name") or _text(child)
            if name:
                info_steps.append(name)

    primary = definitions[0] if definitions else {}
    return {
        "definitions": definitions,
        "class_name": primary.get("class_name", "Processor"),
        "class_source": primary.get("class_source", ""),
        "fields": fields,
        "info_steps": info_steps,
        "clear_result_fields": _bool_from_yn(
            _child_text(step_el, "clear_result_fields")
            or _child_text(step_el, "clearResultFields")
        ),
    }


def parse_user_defined_java_expression_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse User Defined Java Expression field expressions."""
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            expression = unescape_xml(
                _child_text(field_el, "java")
                or _child_text(field_el, "expression")
                or _child_text(field_el, "formula")
            )
            if not name and not expression:
                continue
            fields.append({
                "name": name or "java_result",
                "expression": expression,
                "type": _child_text(field_el, "type"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
                "replace": _bool_from_yn(_child_text(field_el, "replace")),
            })

    if not fields:
        expression = unescape_xml(
            _child_text(step_el, "java")
            or _child_text(step_el, "expression")
        )
        name = _child_text(step_el, "name") or _child_text(step_el, "field_name")
        if expression:
            fields.append({
                "name": name or "java_result",
                "expression": expression,
                "type": _child_text(step_el, "type"),
                "length": "",
                "precision": "",
                "replace": False,
            })

    return {"fields": fields}


def parse_ifnull_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse IfNull / If Field Value Is Null replacement field metadata."""
    replacements: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            value = _child_text(field_el, "value") or _child_text(field_el, "replacement")
            replacements.append({
                "name": name,
                "value": value,
                "mask": _child_text(field_el, "mask"),
                "set_empty_string": _bool_from_yn(_child_text(field_el, "set_empty_string")),
            })

    value_types: list[dict[str, Any]] = []
    types_el = step_el.find("valuetypes")
    if types_el is not None:
        for type_el in types_el.findall("valuetype"):
            type_name = _child_text(type_el, "name") or _child_text(type_el, "type")
            if not type_name:
                continue
            value_types.append({
                "type": type_name,
                "value": _child_text(type_el, "value") or _child_text(type_el, "replacement"),
                "mask": _child_text(type_el, "mask"),
                "set_empty_string": _bool_from_yn(_child_text(type_el, "set_empty_string")),
            })

    return {
        "replacements": replacements,
        "value_types": value_types,
        "replace_all": _child_text(step_el, "replaceAllByValue"),
        "replace_all_mask": _child_text(step_el, "replaceAllMask"),
        "set_empty_string_all": _bool_from_yn(_child_text(step_el, "setEmptyStringAll")),
        "select_fields": _bool_from_yn(_child_text(step_el, "selectFields")),
        "select_values_type": _bool_from_yn(_child_text(step_el, "selectValuesType")),
    }


def parse_clone_row_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Clone Row duplicate-count and clone-flag metadata."""
    nr_raw = (
        _child_text(step_el, "nrclones")
        or _child_text(step_el, "nrClones")
        or _child_text(step_el, "nr_clones")
        or "0"
    )
    try:
        nr_clones = int(float(nr_raw)) if nr_raw else 0
    except ValueError:
        nr_clones = 0
    return {
        "nr_clones": max(0, nr_clones),
        "nr_clones_raw": nr_raw,
        "add_clone_flag": _bool_from_yn(
            _child_text(step_el, "addcloneflag") or _child_text(step_el, "addCloneFlag")
        ),
        "clone_flag_field": (
            _child_text(step_el, "cloneflagfield")
            or _child_text(step_el, "cloneFlagField")
            or "cloneflag"
        ),
        "nr_clone_in_field": _bool_from_yn(
            _child_text(step_el, "nrcloneinfield") or _child_text(step_el, "nrCloneInField")
        ),
        "nr_clone_field": (
            _child_text(step_el, "nrclonefield") or _child_text(step_el, "nrCloneField")
        ),
        "add_clone_num": _bool_from_yn(
            _child_text(step_el, "addclonenum") or _child_text(step_el, "addCloneNum")
        ),
        "clone_num_field": (
            _child_text(step_el, "clonenumfield")
            or _child_text(step_el, "cloneNumField")
            or "clonenum"
        ),
    }


def parse_null_if_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Null If comparison values that should become null."""
    fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name") or _child_text(field_el, "field")
            if not name:
                continue
            fields.append({
                "name": name,
                "value": (
                    _child_text(field_el, "value")
                    or _child_text(field_el, "nullif")
                    or _child_text(field_el, "null_if")
                ),
                "type": _child_text(field_el, "type"),
            })
    if not fields:
        flat_name = _child_text(step_el, "fieldname") or _child_text(step_el, "field")
        if flat_name:
            fields.append({
                "name": flat_name,
                "value": _child_text(step_el, "value") or _child_text(step_el, "nullif"),
                "type": _child_text(step_el, "type"),
            })
    return {"fields": fields}


def parse_delay_row_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Delay Row interval and time-scale metadata."""
    timeout = (
        _child_text(step_el, "timeout")
        or _child_text(step_el, "delay")
        or "0"
    )
    scale = (
        _child_text(step_el, "scaletime")
        or _child_text(step_el, "scaleTimeCode")
        or _child_text(step_el, "timeUnit")
        or "milliseconds"
    )
    # Pentaho sometimes encodes scale as an integer code: 0=ms, 1=s, 2=min, 3=hr
    scale_map = {"0": "milliseconds", "1": "seconds", "2": "minutes", "3": "hours"}
    if scale.strip() in scale_map:
        scale = scale_map[scale.strip()]
    try:
        timeout_val: float | int = float(timeout) if timeout else 0
        if timeout_val == int(timeout_val):
            timeout_val = int(timeout_val)
    except ValueError:
        timeout_val = 0
    return {
        "timeout": timeout_val,
        "timeout_raw": timeout,
        "scale_time": scale.lower() if isinstance(scale, str) else "milliseconds",
    }


def parse_change_file_encoding_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Change File Encoding source/target paths and encodings."""
    return {
        "source_file": (
            _child_text(step_el, "sourcefilename")
            or _child_text(step_el, "sourceFilename")
            or _child_text(step_el, "source_file")
            or _child_text(step_el, "file")
        ),
        "target_file": (
            _child_text(step_el, "targetfilename")
            or _child_text(step_el, "targetFilename")
            or _child_text(step_el, "target_file")
        ),
        "source_encoding": (
            _child_text(step_el, "sourceencoding")
            or _child_text(step_el, "sourceEncoding")
            or _child_text(step_el, "source_encoding")
            or "UTF-8"
        ),
        "target_encoding": (
            _child_text(step_el, "targetencoding")
            or _child_text(step_el, "targetEncoding")
            or _child_text(step_el, "target_encoding")
            or "UTF-8"
        ),
        "source_file_field": (
            _child_text(step_el, "sourcefilenamefield")
            or _child_text(step_el, "sourceFilenameField")
        ),
        "target_file_field": (
            _child_text(step_el, "targetfilenamefield")
            or _child_text(step_el, "targetFilenameField")
        ),
        "create_parent_folder": _bool_from_yn(
            _child_text(step_el, "createparentfolder")
            or _child_text(step_el, "create_parent_folder")
        ),
        "add_source_result": _bool_from_yn(_child_text(step_el, "addsourceresultfilenames")),
        "add_target_result": _bool_from_yn(_child_text(step_el, "addtargetresultfilenames")),
    }


def parse_meta_structure_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Metadata Structure of Stream output field names."""
    return {
        "output_rowcount": _bool_from_yn(
            _child_text(step_el, "outputRowcount") or _child_text(step_el, "output_rowcount")
        ),
        "rowcount_field": (
            _child_text(step_el, "rowcountField") or _child_text(step_el, "rowcount_field") or "rowcount"
        ),
        "position_field": (
            _child_text(step_el, "positionField") or _child_text(step_el, "position_field") or "Position"
        ),
        "fieldname_field": (
            _child_text(step_el, "fieldnameField")
            or _child_text(step_el, "fieldname_field")
            or "Fieldname"
        ),
        "comments_field": (
            _child_text(step_el, "commentsField") or _child_text(step_el, "comments_field") or "Comments"
        ),
        "type_field": (
            _child_text(step_el, "typeField") or _child_text(step_el, "type_field") or "Type"
        ),
        "length_field": (
            _child_text(step_el, "lengthField") or _child_text(step_el, "length_field") or "Length"
        ),
        "precision_field": (
            _child_text(step_el, "precisionField")
            or _child_text(step_el, "precision_field")
            or "Precision"
        ),
        "origin_field": (
            _child_text(step_el, "originField") or _child_text(step_el, "origin_field") or "Origin"
        ),
    }


# ---------------------------------------------------------------------------
# Job category: result rows/files + variables
# ---------------------------------------------------------------------------


def parse_rows_to_result_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Copy Rows to Result (no step-specific XML; preserve residual attrs)."""
    extras: dict[str, str] = {}
    skip = frozenset({
        "name", "type", "description", "distribute", "custom_distribution",
        "copies", "partitioning", "remotesteps", "GUI", "draw", "attributes",
        "fields", "cluster_schema",
    })
    for child in list(step_el):
        tag = (child.tag or "").strip()
        if not tag or tag in skip:
            continue
        text = (child.text or "").strip()
        if text:
            extras[tag] = text
    return {
        "result_buffer": "rows",
        "preserve_order": True,
        "extras": extras,
    }


def parse_rows_from_result_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get Rows from Result field schema metadata."""
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name") or _child_text(field_el, "field_name")
        if not name:
            continue
        length_raw = _child_text(field_el, "length")
        precision_raw = _child_text(field_el, "precision")
        try:
            length = int(float(length_raw)) if length_raw not in ("", None) else -2
        except ValueError:
            length = -2
        try:
            precision = int(float(precision_raw)) if precision_raw not in ("", None) else -2
        except ValueError:
            precision = -2
        fields.append({
            "name": name,
            "type": _child_text(field_el, "type") or "String",
            "type_name": _child_text(field_el, "type") or "String",
            "length": length,
            "precision": precision,
        })
    return {
        "fields": fields,
        "output_columns": [f["name"] for f in fields],
        "result_buffer": "rows",
    }


def parse_files_from_result_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get Files from Result (fixed ResultFile schema; preserve residual attrs)."""
    extras: dict[str, str] = {}
    skip = frozenset({
        "name", "type", "description", "distribute", "custom_distribution",
        "copies", "partitioning", "remotesteps", "GUI", "draw", "attributes",
        "fields", "cluster_schema",
    })
    for child in list(step_el):
        tag = (child.tag or "").strip()
        if not tag or tag in skip:
            continue
        text = (child.text or "").strip()
        if text:
            extras[tag] = text
    # Standard columns emitted by PDI FilesFromResult
    fields = [
        {"name": "type", "type": "String", "type_name": "String"},
        {"name": "filename", "type": "String", "type_name": "String"},
        {"name": "path", "type": "String", "type_name": "String"},
        {"name": "parentorigin", "type": "String", "type_name": "String"},
        {"name": "origin", "type": "String", "type_name": "String"},
        {"name": "comment", "type": "String", "type_name": "String"},
        {"name": "timestamp", "type": "Date", "type_name": "Date"},
    ]
    return {
        "fields": fields,
        "output_columns": [f["name"] for f in fields],
        "result_buffer": "files",
        "preserve_order": True,
        "extras": extras,
    }


def parse_files_to_result_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Set Files in Result filename field and ResultFile type."""
    extras: dict[str, str] = {}
    skip = frozenset({
        "name", "type", "description", "distribute", "custom_distribution",
        "copies", "partitioning", "remotesteps", "GUI", "draw", "attributes",
        "fields", "cluster_schema", "filename_field", "file_type",
    })
    for child in list(step_el):
        tag = (child.tag or "").strip()
        if not tag or tag in skip:
            continue
        text = (child.text or "").strip()
        if text:
            extras[tag] = text
    return {
        "filename_field": (
            _child_text(step_el, "filename_field")
            or _child_text(step_el, "filenameField")
            or _child_text(step_el, "filename")
        ),
        "file_type": (
            _child_text(step_el, "file_type")
            or _child_text(step_el, "fileType")
            or "GENERAL"
        ),
        "result_buffer": "files",
        "preserve_order": True,
        "extras": extras,
    }


def parse_set_variable_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Set Variables field→variable mappings and formatting flag."""
    fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        field_name = (
            _child_text(field_el, "field_name")
            or _child_text(field_el, "fieldName")
            or _child_text(field_el, "name")
        )
        variable_name = (
            _child_text(field_el, "variable_name")
            or _child_text(field_el, "variableName")
            or _child_text(field_el, "variable")
        )
        if not field_name and not variable_name:
            continue
        fields.append({
            "field_name": field_name,
            "variable_name": variable_name or field_name,
            "variable_type": (
                _child_text(field_el, "variable_type")
                or _child_text(field_el, "variableType")
                or "JVM"
            ),
            "default_value": (
                _child_text(field_el, "default_value")
                or _child_text(field_el, "defaultValue")
            ),
        })
    # Legacy flat single-variable attributes
    if not fields:
        var_name = (
            _child_text(step_el, "variable_name")
            or _child_text(step_el, "variableName")
            or _child_text(step_el, "name")
        )
        if var_name:
            fields.append({
                "field_name": _child_text(step_el, "field_name") or var_name,
                "variable_name": var_name,
                "variable_type": _child_text(step_el, "variable_type") or "JVM",
                "default_value": (
                    _child_text(step_el, "default_value")
                    or _child_text(step_el, "variable_value")
                    or _child_text(step_el, "value")
                ),
            })
    return {
        "fields": fields,
        "use_formatting": _bool_from_yn(
            _child_text(step_el, "use_formatting")
            or _child_text(step_el, "useFormatting"),
            default=False,
        ),
        "variable_names": [f["variable_name"] for f in fields if f.get("variable_name")],
    }


def parse_get_variable_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get Variables field definitions (name, variable string, type, trim)."""
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name") or _child_text(field_el, "field_name")
        variable = (
            _child_text(field_el, "variable")
            or _child_text(field_el, "variable_string")
            or _child_text(field_el, "variableString")
        )
        if not name and not variable:
            continue
        length_raw = _child_text(field_el, "length")
        precision_raw = _child_text(field_el, "precision")
        try:
            length = int(float(length_raw)) if length_raw not in ("", None) else -1
        except ValueError:
            length = -1
        try:
            precision = int(float(precision_raw)) if precision_raw not in ("", None) else -1
        except ValueError:
            precision = -1
        fields.append({
            "name": name or variable,
            # Keep empty variable strings as empty (do not invent ${name});
            # Get Variables requires an explicit variable specification in PDI.
            "variable": variable,
            "type": _child_text(field_el, "type") or "String",
            "type_name": _child_text(field_el, "type") or "String",
            "format": _child_text(field_el, "format"),
            "currency": _child_text(field_el, "currency"),
            "decimal": _child_text(field_el, "decimal"),
            "group": _child_text(field_el, "group"),
            "length": length,
            "precision": precision,
            "trim_type": (
                _child_text(field_el, "trim_type")
                or _child_text(field_el, "trimType")
                or "none"
            ),
        })
    # Legacy flat attributes
    if not fields:
        var_name = _child_text(step_el, "variable_name") or _child_text(step_el, "variable")
        field_name = _child_text(step_el, "field_name") or var_name
        if field_name or var_name:
            fields.append({
                "name": field_name or var_name,
                "variable": var_name or "",
                "type": _child_text(step_el, "type") or "String",
                "type_name": _child_text(step_el, "type") or "String",
                "format": _child_text(step_el, "format"),
                "currency": "",
                "decimal": "",
                "group": "",
                "length": -1,
                "precision": -1,
                "trim_type": "none",
            })
    return {
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
    }


def parse_write_to_log_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Write to Log level, subject, row limit, and field list."""
    fields: list[str] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name") or _child_text(field_el, "field")
            if name:
                fields.append(name)
    limit_rows = _bool_from_yn(
        _child_text(step_el, "limitRows") or _child_text(step_el, "limit_rows")
    )
    limit_raw = (
        _child_text(step_el, "limitRowsNumber")
        or _child_text(step_el, "limit_rows_number")
        or "10"
    )
    try:
        limit_n = int(float(limit_raw)) if limit_raw else 10
    except ValueError:
        limit_n = 10
    return {
        "log_level": (
            _child_text(step_el, "loglevel")
            or _child_text(step_el, "logLevel")
            or _child_text(step_el, "log_level")
            or "Basic"
        ),
        "display_header": _bool_from_yn(
            _child_text(step_el, "displayHeader") or _child_text(step_el, "display_header")
        ),
        "limit_rows": limit_rows,
        "limit_rows_number": max(0, limit_n),
        "log_message": (
            _child_text(step_el, "logmessage")
            or _child_text(step_el, "logMessage")
            or _child_text(step_el, "message")
        ),
        "log_subject": (
            _child_text(step_el, "logsubject")
            or _child_text(step_el, "logSubject")
            or _child_text(step_el, "subject")
        ),
        "fields": fields,
    }


def parse_table_compare_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Table Compare connections, tables, keys, and result fields."""
    key_fields: list[str] = []
    keys_el = (
        step_el.find("key_fields")
        if step_el.find("key_fields") is not None
        else step_el.find("keys") if step_el.find("keys") is not None
        else step_el.find("keyFields")
    )
    if keys_el is not None:
        for key_el in list(keys_el):
            name = (
                _child_text(key_el, "key_field")
                or _child_text(key_el, "name")
                or _child_text(key_el, "field")
                or (key_el.text or "").strip()
            )
            if name:
                key_fields.append(name)
    if not key_fields:
        flat = _child_text(step_el, "key_fields") or _child_text(step_el, "keyFields")
        if flat:
            key_fields = [p.strip() for p in flat.replace(";", ",").split(",") if p.strip()]

    exclude_fields: list[str] = []
    excl_el = (
        step_el.find("exclude_fields")
        if step_el.find("exclude_fields") is not None
        else step_el.find("excludeFields")
    )
    if excl_el is not None:
        for item in list(excl_el):
            name = _child_text(item, "name") or _child_text(item, "field") or (item.text or "").strip()
            if name:
                exclude_fields.append(name)

    return {
        "reference_connection": (
            _child_text(step_el, "reference_connection")
            or _child_text(step_el, "referenceConnection")
        ),
        "compare_connection": (
            _child_text(step_el, "compare_connection")
            or _child_text(step_el, "compareConnection")
        ),
        "reference_schema": (
            _child_text(step_el, "reference_schema") or _child_text(step_el, "referenceSchema")
        ),
        "compare_schema": (
            _child_text(step_el, "compare_schema") or _child_text(step_el, "compareSchema")
        ),
        "reference_table": (
            _child_text(step_el, "reference_table") or _child_text(step_el, "referenceTable")
        ),
        "compare_table": (
            _child_text(step_el, "compare_table") or _child_text(step_el, "compareTable")
        ),
        "key_fields": key_fields,
        "exclude_fields": exclude_fields,
        "nr_errors_field": (
            _child_text(step_el, "nr_errors") or _child_text(step_el, "nrErrorsField") or "nrErrors"
        ),
        "nr_records_reference_field": (
            _child_text(step_el, "nr_records_reference")
            or _child_text(step_el, "nrRecordsReferenceField")
            or "nrRecordsReference"
        ),
        "nr_records_compare_field": (
            _child_text(step_el, "nr_records_compare")
            or _child_text(step_el, "nrRecordsCompareField")
            or "nrRecordsCompare"
        ),
        "nr_errors_left_join_field": (
            _child_text(step_el, "nr_errors_left_join")
            or _child_text(step_el, "nrErrorsLeftJoinField")
            or "nrErrorsLeftJoin"
        ),
        "nr_errors_inner_join_field": (
            _child_text(step_el, "nr_errors_inner_join")
            or _child_text(step_el, "nrErrorsInnerJoinField")
            or "nrErrorsInnerJoin"
        ),
        "nr_errors_right_join_field": (
            _child_text(step_el, "nr_errors_right_join")
            or _child_text(step_el, "nrErrorsRightJoinField")
            or "nrErrorsRightJoin"
        ),
        "key_desc_field": (
            _child_text(step_el, "key_description")
            or _child_text(step_el, "keyDescriptionField")
            or "keyDescription"
        ),
        "value_reference_field": (
            _child_text(step_el, "value_reference")
            or _child_text(step_el, "valueReferenceField")
            or "valueReference"
        ),
        "value_compare_field": (
            _child_text(step_el, "value_compare")
            or _child_text(step_el, "valueCompareField")
            or "valueCompare"
        ),
    }


def parse_zip_file_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Zip File archive, source fields, and compression options."""
    return {
        "zip_filename": (
            _child_text(step_el, "zipfilename")
            or _child_text(step_el, "zipFilename")
            or _child_text(step_el, "zip_filename")
        ),
        "zip_filename_field": (
            _child_text(step_el, "zipfilenamefield")
            or _child_text(step_el, "zipFilenameField")
        ),
        "source_filename_field": (
            _child_text(step_el, "sourcefilenamefield")
            or _child_text(step_el, "sourceFilenameField")
            or _child_text(step_el, "source_filename_field")
        ),
        "target_filename_field": (
            _child_text(step_el, "targetfilenamefield")
            or _child_text(step_el, "targetFilenameField")
        ),
        "move_to_folder": (
            _child_text(step_el, "movetofolder")
            or _child_text(step_el, "moveToFolder")
            or _child_text(step_el, "movetodirectory")
        ),
        "overwrite_zip_entry": _bool_from_yn(
            _child_text(step_el, "overwritezipentry") or _child_text(step_el, "overwriteZipEntry")
        ),
        "create_parent_folder": _bool_from_yn(
            _child_text(step_el, "createparentfolder") or _child_text(step_el, "createParentFolder")
        ),
        "keep_source_folder": _bool_from_yn(
            _child_text(step_el, "keepsourcefolder") or _child_text(step_el, "keepSourceFolder")
        ),
        "add_filename_result": _bool_from_yn(
            _child_text(step_el, "addfilenameresult") or _child_text(step_el, "addFilenameResult")
        ),
        "include_subfolders": _bool_from_yn(
            _child_text(step_el, "includesubfolders") or _child_text(step_el, "includeSubFolders")
        ),
        "compression": (
            _child_text(step_el, "compressionrate")
            or _child_text(step_el, "compression")
            or "DEFLATED"
        ),
        "after_zip": (
            _child_text(step_el, "afterzip")
            or _child_text(step_el, "afterZip")
            or "do_nothing"
        ),
    }


def parse_process_files_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Process Files copy/move/delete configuration."""
    operation = (
        _child_text(step_el, "operation_type")
        or _child_text(step_el, "operationtype")
        or _child_text(step_el, "operation")
        or "copy"
    )
    # Pentaho may encode as numeric: 0=copy, 1=move, 2=delete
    op_map = {"0": "copy", "1": "move", "2": "delete"}
    if operation.strip() in op_map:
        operation = op_map[operation.strip()]
    return {
        "operation": operation.lower(),
        "source_filename_field": (
            _child_text(step_el, "sourcefilenamefield")
            or _child_text(step_el, "sourceFilenameField")
        ),
        "target_filename_field": (
            _child_text(step_el, "targetfilenamefield")
            or _child_text(step_el, "targetFilenameField")
        ),
        "overwrite_target": _bool_from_yn(
            _child_text(step_el, "overwritetargetfile")
            or _child_text(step_el, "overwriteTargetFile")
        ),
        "create_parent_folder": _bool_from_yn(
            _child_text(step_el, "createparentfolder") or _child_text(step_el, "createParentFolder")
        ),
        "add_result_filenames": _bool_from_yn(
            _child_text(step_el, "addresultfilenames") or _child_text(step_el, "addResultFilenames")
        ),
        "simulate": _bool_from_yn(_child_text(step_el, "simulate")),
    }


def parse_exec_process_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Execute a Process executable, args, and result fields."""
    arguments: list[str] = []
    args_el = step_el.find("arguments") or step_el.find("argument")
    if args_el is not None:
        if args_el.tag.lower() == "argument" and (args_el.text or "").strip():
            arguments.append(args_el.text.strip())
        for arg_el in args_el.findall("argument") if args_el.tag.lower() != "argument" else []:
            val = (arg_el.text or "").strip() or _child_text(arg_el, "name")
            if val:
                arguments.append(val)
    arg_fields: list[str] = []
    af_el = step_el.find("argumentfields") or step_el.find("argumentFields")
    if af_el is not None:
        for field_el in af_el.findall("field") + af_el.findall("argumentfield"):
            name = _child_text(field_el, "name") or (field_el.text or "").strip()
            if name:
                arg_fields.append(name)
    return {
        "process_field": (
            _child_text(step_el, "processfield")
            or _child_text(step_el, "ProcessField")
            or _child_text(step_el, "process")
        ),
        "executable": (
            _child_text(step_el, "executable")
            or _child_text(step_el, "command")
            or _child_text(step_el, "process")
        ),
        "arguments": arguments,
        "argument_fields": arg_fields,
        "output_field": (
            _child_text(step_el, "outputfield")
            or _child_text(step_el, "OutputField")
            or "outputLine"
        ),
        "error_field": (
            _child_text(step_el, "errorfield") or _child_text(step_el, "ErrorField") or "errorLine"
        ),
        "exit_value_field": (
            _child_text(step_el, "exitvaluefield")
            or _child_text(step_el, "ExitValueField")
            or "exitValue"
        ),
        "fail_when_nonzero": _bool_from_yn(
            _child_text(step_el, "failwhennonzero") or _child_text(step_el, "failWhenNonZero")
        ),
        "output_delimited": _bool_from_yn(
            _child_text(step_el, "outputdelimitedfields")
            or _child_text(step_el, "OutputDelimitedFields")
        ),
        "output_delimiter": (
            _child_text(step_el, "outputdelimiter") or _child_text(step_el, "OutputDelimiter")
        ),
    }


def parse_ssh_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Run SSH Commands host, auth, and command metadata."""
    return {
        "server": (
            _child_text(step_el, "serverName")
            or _child_text(step_el, "servername")
            or _child_text(step_el, "server")
            or _child_text(step_el, "host")
        ),
        "port": _child_text(step_el, "port") or "22",
        "username": (
            _child_text(step_el, "userName")
            or _child_text(step_el, "username")
            or _child_text(step_el, "user")
        ),
        "password": _child_text(step_el, "password"),
        "use_private_key": _bool_from_yn(
            _child_text(step_el, "usePrivateKey") or _child_text(step_el, "useprivatekey")
        ),
        "key_file": (
            _child_text(step_el, "keyFileName")
            or _child_text(step_el, "keyfilename")
            or _child_text(step_el, "privateKey")
        ),
        "passphrase": (
            _child_text(step_el, "passPhrase") or _child_text(step_el, "passphrase")
        ),
        "command": (
            _child_text(step_el, "command")
            or _child_text(step_el, "commands")
            or _child_text(step_el, "script")
        ),
        "command_field": (
            _child_text(step_el, "commandfield")
            or _child_text(step_el, "commandField")
        ),
        "use_command_field": _bool_from_yn(
            _child_text(step_el, "dynamiccommand") or _child_text(step_el, "dynamicCommand")
        ),
        "stdout_field": (
            _child_text(step_el, "stdOutFieldName")
            or _child_text(step_el, "stdoutfield")
            or "stdOut"
        ),
        "stderr_field": (
            _child_text(step_el, "stdErrFieldName")
            or _child_text(step_el, "stderrfield")
            or "stdErr"
        ),
        "timeout": _child_text(step_el, "timeOut") or _child_text(step_el, "timeout") or "0",
        "proxy_host": _child_text(step_el, "proxyHost") or _child_text(step_el, "proxyhost"),
        "proxy_port": _child_text(step_el, "proxyPort") or _child_text(step_el, "proxyport"),
        "proxy_username": (
            _child_text(step_el, "proxyUsername") or _child_text(step_el, "proxyusername")
        ),
    }


def parse_mail_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Mail/SMTP configuration including recipients and attachments."""
    attached_filenames: list[str] = []
    attach_el = step_el.find("embeddedimages") or step_el.find("attachments")
    if attach_el is not None:
        for item in list(attach_el):
            name = (
                _child_text(item, "image_name")
                or _child_text(item, "filename")
                or _child_text(item, "name")
                or (item.text or "").strip()
            )
            if name:
                attached_filenames.append(name)
    return {
        "server": _child_text(step_el, "server") or _child_text(step_el, "smtpServer"),
        "port": _child_text(step_el, "port") or "25",
        "destination": (
            _child_text(step_el, "destination")
            or _child_text(step_el, "destinationAddress")
            or _child_text(step_el, "to")
        ),
        "destination_cc": (
            _child_text(step_el, "destinationCc") or _child_text(step_el, "destinationcc") or _child_text(step_el, "cc")
        ),
        "destination_bcc": (
            _child_text(step_el, "destinationBCc")
            or _child_text(step_el, "destinationbcc")
            or _child_text(step_el, "bcc")
        ),
        "reply_to": _child_text(step_el, "replyTo") or _child_text(step_el, "replyto"),
        "reply_address": (
            _child_text(step_el, "replyAddress") or _child_text(step_el, "replyaddress")
        ),
        "subject": _child_text(step_el, "subject"),
        "comment": (
            _child_text(step_el, "comment")
            or _child_text(step_el, "body")
            or _child_text(step_el, "message")
        ),
        "include_date": _bool_from_yn(_child_text(step_el, "include_date") or _child_text(step_el, "includeDate")),
        "only_comment": _bool_from_yn(_child_text(step_el, "only_comment") or _child_text(step_el, "onlyComment")),
        "use_html": _bool_from_yn(_child_text(step_el, "use_HTML") or _child_text(step_el, "useHTML")),
        "encoding": _child_text(step_el, "encoding") or "UTF-8",
        "priority": _child_text(step_el, "priority"),
        "include_files": _bool_from_yn(
            _child_text(step_el, "include_files") or _child_text(step_el, "includeFiles")
        ),
        "zip_files": _bool_from_yn(_child_text(step_el, "zip_files") or _child_text(step_el, "zipFiles")),
        "zip_filename": _child_text(step_el, "zip_filename") or _child_text(step_el, "zipFilename"),
        "use_authentication": _bool_from_yn(
            _child_text(step_el, "use_auth") or _child_text(step_el, "useauthentication")
        ),
        "auth_user": _child_text(step_el, "auth_user") or _child_text(step_el, "authUser"),
        "auth_password": (
            _child_text(step_el, "auth_password") or _child_text(step_el, "authPassword")
        ),
        "use_secure_auth": _bool_from_yn(
            _child_text(step_el, "use_secureAuth") or _child_text(step_el, "secureauth")
        ),
        "secure_connection_type": (
            _child_text(step_el, "secureconnectiontype")
            or _child_text(step_el, "secureConnectionType")
            or "TLS"
        ),
        "contact_person": _child_text(step_el, "contact_person") or _child_text(step_el, "contactPerson"),
        "contact_phone": _child_text(step_el, "contact_phone") or _child_text(step_el, "contactPhone"),
        "attached_filenames": attached_filenames,
        "attach_content_field": (
            _child_text(step_el, "attachContentField") or _child_text(step_el, "attachcontentfield")
        ),
        "attach_filename_field": (
            _child_text(step_el, "attachFilenameField") or _child_text(step_el, "attachfilenamefield")
        ),
    }


def parse_syslog_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Send Message to Syslog server, facility, and severity."""
    return {
        "server": (
            _child_text(step_el, "serverName")
            or _child_text(step_el, "servername")
            or _child_text(step_el, "server")
        ),
        "port": _child_text(step_el, "port") or "514",
        "facility": _child_text(step_el, "facility") or "USER",
        "priority": (
            _child_text(step_el, "priority")
            or _child_text(step_el, "severity")
            or "INFO"
        ),
        "message_field": (
            _child_text(step_el, "messageFieldName")
            or _child_text(step_el, "messagefield")
            or _child_text(step_el, "message_field")
        ),
        "add_timestamp": _bool_from_yn(
            _child_text(step_el, "addTimestamp") or _child_text(step_el, "addtimestamp")
        ),
        "date_pattern": (
            _child_text(step_el, "datePattern") or _child_text(step_el, "datepattern")
        ),
        "add_hostname": _bool_from_yn(
            _child_text(step_el, "addHostName") or _child_text(step_el, "addhostname")
        ),
    }


def parse_edi_to_xml_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse EDI to XML input/output field mapping."""
    return {
        "input_field": (
            _child_text(step_el, "inputfield")
            or _child_text(step_el, "inputField")
            or _child_text(step_el, "edi_field")
        ),
        "output_field": (
            _child_text(step_el, "outputfield")
            or _child_text(step_el, "outputField")
            or _child_text(step_el, "xml_field")
            or "xml"
        ),
    }


def parse_group_by_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Group By / Memory Group By step metadata."""
    group_keys, aggregates = parse_group_by_fields(step_el)
    all_fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            all_fields.append({
                "name": _child_text(field_el, "name"),
                "type": _child_text(field_el, "type"),
                "aggregate": _child_text(field_el, "aggregate"),
                "subject": _child_text(field_el, "subject"),
                "valuefield": _child_text(field_el, "valuefield"),
            })
    # Deduplicate group keys while preserving order
    seen: set[str] = set()
    deduped_keys: list[str] = []
    for key in group_keys:
        if key and key not in seen:
            seen.add(key)
            deduped_keys.append(key)
    return {
        "group_keys": deduped_keys,
        "aggregates": _metadata_value(aggregates),
        "fields": all_fields,
        "give_back_row": _bool_from_yn(_child_text(step_el, "give_back_row")),
        "all_rows": _bool_from_yn(_child_text(step_el, "all_rows")),
        "always_giving_back_one_row": _bool_from_yn(
            _child_text(step_el, "always_giving_back_one_row")
            or _child_text(step_el, "give_back_row")
        ),
        "directory": _child_text(step_el, "directory"),
        "prefix": _child_text(step_el, "prefix"),
        "add_line_nr": _bool_from_yn(_child_text(step_el, "add_linenr")),
        "line_nr_in_group_field": _child_text(step_el, "linenr_fieldname"),
        "ignore_aggregate": _bool_from_yn(_child_text(step_el, "ignore_aggregate")),
        "field_ignore": _child_text(step_el, "field_ignore"),
    }


def parse_analytic_query_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Analytic Query (LEAD/LAG window) group + function fields."""
    group_fields: list[str] = []
    group_el = step_el.find("group")
    if group_el is not None:
        for field_el in group_el.findall("field"):
            name = _child_text(field_el, "name")
            if name and name not in group_fields:
                group_fields.append(name)

    analytic_fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            # Output name may be under <aggregate> (AnalyticQueryMeta) or <name>
            out_name = (
                _child_text(field_el, "aggregate")
                or _child_text(field_el, "name")
            )
            subject = _child_text(field_el, "subject")
            fun_type = (
                _child_text(field_el, "type")
                or _child_text(field_el, "aggregate_type")
                or "LAG"
            ).strip().upper()
            offset_raw = (
                _child_text(field_el, "valuefield")
                or _child_text(field_el, "value_field")
                or "1"
            )
            try:
                offset = int(float(offset_raw)) if offset_raw else 1
            except ValueError:
                offset = 1
            # Order fields if present (extension; Pentaho relies on pre-sorted input)
            order_field = _child_text(field_el, "order_field") or _child_text(
                field_el, "orderfield"
            )
            ascending = _bool_from_yn(
                _child_text(field_el, "ascending", "Y"), default=True
            )
            if out_name:
                analytic_fields.append({
                    "name": out_name,
                    "subject": subject or out_name,
                    "function": fun_type,
                    "offset": offset,
                    "order_field": order_field,
                    "ascending": ascending,
                    "valuefield": offset_raw,
                    "type": fun_type,
                })

    order_fields: list[dict[str, Any]] = []
    orders_el = step_el.find("orders")
    if orders_el is None:
        orders_el = step_el.find("order")
    if orders_el is not None:
        for field_el in orders_el.findall("field"):
            name = _child_text(field_el, "name")
            if name:
                order_fields.append({
                    "name": name,
                    "ascending": _bool_from_yn(
                        _child_text(field_el, "ascending", "Y"), default=True
                    ),
                })

    return {
        "group_fields": group_fields,
        "partition_fields": list(group_fields),
        "order_fields": order_fields,
        "analytic_fields": analytic_fields,
        "fields": analytic_fields,
    }


def _parse_sample_rows_ranges(lines_range: str) -> list[tuple[int, int]]:
    """Parse Sample Rows linesrange (e.g. '1', '1..10', '1,5,10..20')."""
    ranges: list[tuple[int, int]] = []
    for part in (lines_range or "").split(","):
        token = part.strip()
        if not token:
            continue
        if ".." in token:
            left, _, right = token.partition("..")
            try:
                start = int(left.strip())
                end = int(right.strip())
            except ValueError:
                continue
            if end < start:
                start, end = end, start
            ranges.append((start, end))
        else:
            try:
                n = int(token)
            except ValueError:
                continue
            ranges.append((n, n))
    return ranges


def parse_sample_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Sample Rows line-range and optional line-number field."""
    lines_range = (
        _child_text(step_el, "linesrange")
        or _child_text(step_el, "lines_range")
        or "1"
    )
    line_num_field = (
        _child_text(step_el, "linenumfield")
        or _child_text(step_el, "line_num_field")
    )
    # Optional / non-standard attributes sometimes present in custom KTRs
    percentage_raw = (
        _child_text(step_el, "percentage")
        or _child_text(step_el, "sample_percentage")
    )
    row_count_raw = (
        _child_text(step_el, "nr_lines")
        or _child_text(step_el, "row_count")
        or _child_text(step_el, "limit")
    )
    seed_raw = _child_text(step_el, "seed") or _child_text(step_el, "random_seed")
    percentage: float | None = None
    if percentage_raw:
        try:
            percentage = float(percentage_raw)
            if percentage > 1.0:
                percentage = percentage / 100.0
        except ValueError:
            percentage = None
    row_count: int | None = None
    if row_count_raw:
        try:
            row_count = int(float(row_count_raw))
        except ValueError:
            row_count = None
    seed: int | None = None
    if seed_raw:
        try:
            seed = int(float(seed_raw))
        except ValueError:
            seed = None
    return {
        "lines_range": lines_range,
        "line_ranges": _parse_sample_rows_ranges(lines_range),
        "line_num_field": line_num_field,
        "percentage": percentage,
        "row_count": row_count,
        "seed": seed,
    }


def parse_reservoir_sampling_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Reservoir Sampling sample size and random seed."""
    node = step_el.find("reservoir_sampling")
    root = node if node is not None else step_el
    sample_size_raw = (
        _child_text(root, "sample_size")
        or _child_text(step_el, "sample_size")
        or "100"
    )
    seed_raw = (
        _child_text(root, "seed")
        or _child_text(step_el, "seed")
        or "1"
    )
    try:
        sample_size = int(float(sample_size_raw))
    except ValueError:
        sample_size = 100
    try:
        seed = int(float(seed_raw))
    except ValueError:
        seed = 1
    replacement = _bool_from_yn(
        _child_text(root, "replacement")
        or _child_text(step_el, "replacement")
        or _child_text(step_el, "with_replacement")
    )
    return {
        "sample_size": sample_size,
        "sample_size_raw": sample_size_raw,
        "seed": seed,
        "seed_raw": seed_raw,
        "replacement": replacement,
    }


def parse_univariate_stats_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Univariate Statistics field metric selections."""
    stats: list[dict[str, Any]] = []
    for uni_el in step_el.findall("univariate_stats"):
        source = _child_text(uni_el, "source_field_name")
        percentile_raw = _child_text(uni_el, "percentile", "-1")
        try:
            percentile = float(percentile_raw)
        except ValueError:
            percentile = -1.0
        # XML stores booleans as Y/N (addTagValue), default True when absent
        def _flag(tag: str, default: bool = True) -> bool:
            raw = _child_text(uni_el, tag)
            if not raw:
                return default
            return _bool_from_yn(raw, default=default)

        if source:
            stats.append({
                "source_field": source,
                "calc_n": _flag("N", True),
                "calc_mean": _flag("mean", True),
                "calc_std_dev": _flag("stdDev", True),
                "calc_variance": _flag("variance", True) if _child_text(uni_el, "variance") else None,
                "calc_min": _flag("min", True),
                "calc_max": _flag("max", True),
                "calc_median": _flag("median", True),
                "percentile": percentile,
                "interpolate": _flag("interpolate", True),
            })
    return {"stats": stats, "fields": stats}


def parse_steps_metrics_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Output Steps Metrics watched-step and output field names."""
    metric_steps: list[dict[str, Any]] = []
    steps_el = step_el.find("steps")
    if steps_el is not None:
        for step_node in steps_el.findall("step"):
            name = _child_text(step_node, "name")
            if name:
                metric_steps.append({
                    "name": name,
                    "copy_nr": _child_text(step_node, "copyNr"),
                    "required": _bool_from_yn(_child_text(step_node, "stepRequired")),
                })
    return {
        "metric_steps": metric_steps,
        "step_name_field": _child_text(step_el, "stepnamefield") or "Stepname",
        "step_id_field": _child_text(step_el, "stepidfield") or "Stepid",
        "lines_input_field": _child_text(step_el, "steplinesinputfield") or "Linesinput",
        "lines_output_field": _child_text(step_el, "steplinesoutputfield") or "Linesoutput",
        "lines_read_field": _child_text(step_el, "steplinesreadfield") or "Linesread",
        "lines_updated_field": _child_text(step_el, "steplinesupdatedfield") or "Linesupdated",
        "lines_written_field": _child_text(step_el, "steplineswrittentfield") or "Lineswritten",
        "lines_errors_field": _child_text(step_el, "steplineserrorsfield") or "Lineserrors",
        "seconds_field": _child_text(step_el, "stepsecondsfield") or "Seconds",
    }


def parse_value_mapper_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Value Mapper step metadata."""
    source, target, mappings, default = parse_value_mappings(step_el)
    return {
        "field_to_use": source,
        "target_field": target,
        "non_match_default": default,
        "mappings": _metadata_value(mappings),
        # Pentaho ValueMapper defaults to case-sensitive matching.
        "case_sensitive": _bool_from_yn(
            _child_text(step_el, "case_sensitive"), default=True
        ),
        "non_empty": _bool_from_yn(_child_text(step_el, "non_empty")),
    }


def parse_sequence_config_dict(step_el: ET.Element) -> dict[str, Any]:
    """Parse Sequence step metadata as a dict."""
    return _metadata_value(parse_sequence_config(step_el))


def parse_text_file_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Text File Output / TextFileOutputLegacy nested file and format properties."""
    file_block = parse_file_block(step_el)
    filename = (
        str(file_block.get("name", ""))
        or extract_step_property(step_el, "filename")
        or _child_text(step_el, "filename")
    )
    extension = (
        str(file_block.get("extention", ""))
        or str(file_block.get("extension", ""))
        or _child_text(step_el, "extension")
    )
    output_fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            output_fields.append({
                "name": _child_text(field_el, "name"),
                "type": _child_text(field_el, "type"),
                "format": _child_text(field_el, "format"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
                "currency": _child_text(field_el, "currency"),
                "decimal": _child_text(field_el, "decimal"),
                "group": _child_text(field_el, "group"),
                "nullif": _child_text(field_el, "nullif"),
                "trim_type": _child_text(field_el, "trim_type"),
            })
    cfg = TextFileOutputConfig(
        filename=filename,
        extension=extension,
        separator=_child_text(step_el, "separator", ""),
        header=_bool_from_yn(_child_text(step_el, "header", "Y"), default=True),
        footer=_bool_from_yn(_child_text(step_el, "footer")),
        encoding=_child_text(step_el, "encoding", ""),
        compression=(
            _child_text(step_el, "compression", "")
            or str(file_block.get("compression", "") or "")
        ),
        enclosure=_child_text(step_el, "enclosure") or _child_text(step_el, "quote"),
        enclosure_forced=_bool_from_yn(_child_text(step_el, "enclosure_forced")),
        append=_bool_from_yn(file_block.get("append", _child_text(step_el, "file_appended"))),
        create_parent_folder=_bool_from_yn(
            file_block.get("create_parent_folder", _child_text(step_el, "create_parent_folder"))
        ),
        split=_bool_from_yn(file_block.get("split", "")),
        fast_dump=_bool_from_yn(
            file_block.get("fast_dump", _child_text(step_el, "fast_dump"))
        ),
        padded=_bool_from_yn(file_block.get("pad", _child_text(step_el, "padded"))),
        file_as_command=_bool_from_yn(
            file_block.get("is_command", _child_text(step_el, "is_command"))
        ),
        file_name_in_field=_bool_from_yn(_child_text(step_el, "fileNameInField")),
        file_name_field=_child_text(step_el, "fileNameField"),
        add_date=_bool_from_yn(file_block.get("add_date", "")),
        add_time=_bool_from_yn(file_block.get("add_time", "")),
        specify_format=_bool_from_yn(file_block.get("SpecifyFormat", file_block.get("specify_format", ""))),
        date_time_format=str(
            file_block.get("date_time_format", "")
            or file_block.get("date_format", "")
            or ""
        ),
        split_every=str(file_block.get("splitevery", "") or file_block.get("split_every", "") or ""),
        ended_line=_child_text(step_el, "endedLine"),
        servlet_output=_bool_from_yn(file_block.get("servlet_output", "")),
        do_not_open_new_file_init=_bool_from_yn(file_block.get("do_not_open_new_file_init", "")),
        file=file_block,
        output_fields=output_fields,
    )
    result = _metadata_value(cfg)
    # Preserve additional scalars used by Spark writer / migration warnings.
    result["escape"] = _child_text(step_el, "escapechar") or _child_text(step_el, "escape")
    result["file_type"] = (
        _child_text(step_el, "file_type")
        or _child_text(step_el, "filetype")
        or str(file_block.get("type", "") or "")
    )
    result["add_to_result_filenames"] = _bool_from_yn(
        file_block.get("add_to_result_filenames", _child_text(step_el, "add_to_result_filenames"))
    )
    result["haspartno"] = _bool_from_yn(file_block.get("haspartno", ""))
    return result


def parse_table_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Table Input SQL, connection, and parameter metadata."""
    parameters: list[dict[str, str]] = []
    params_el = step_el.find("parameters")
    if params_el is not None:
        for param_el in params_el.findall("parameter"):
            parameters.append({
                "name": _child_text(param_el, "name"),
                "default": _child_text(param_el, "default"),
                "description": _child_text(param_el, "description"),
            })
    try:
        limit = int(_child_text(step_el, "limit", "0") or "0")
    except ValueError:
        limit = 0
    cfg = TableInputConfig(
        connection=_child_text(step_el, "connection"),
        sql=_child_text(step_el, "sql"),
        schema=_child_text(step_el, "schema"),
        table=_child_text(step_el, "table"),
        limit=limit,
        execute_each_row=_bool_from_yn(_child_text(step_el, "execute_each_row")),
        variables_active=_bool_from_yn(
            _child_text(step_el, "variables_active")
            or _child_text(step_el, "variableReplacementActive")
        ),
        lazy_conversion=_bool_from_yn(_child_text(step_el, "lazy_conversion_active")),
        parameters=parameters,
    )
    return _metadata_value(cfg)


def _parse_text_file_input_file_entries(step_el: ET.Element) -> list[dict[str, str]]:
    """Collect interleaved ``<file><name/><filemask/>…`` entries from Legacy/modern XML."""
    file_el = step_el.find("file")
    if file_el is None:
        return []
    entries: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for child in file_el:
        if len(child) > 0:
            continue
        tag = child.tag
        text = _text(child)
        if tag == "name":
            if any(current.get(k) for k in ("name", "filemask", "exclude_filemask")):
                entries.append(current)
                current = {}
            current["name"] = text
        else:
            current[tag] = text
    if current:
        entries.append(current)
    return entries


def parse_text_file_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Text File Input / OldTextFileInput complete Legacy-compatible metadata."""
    file_entries = _parse_text_file_input_file_entries(step_el)
    file_block = parse_file_block(step_el)
    primary = file_entries[0] if file_entries else {}
    filename = (
        primary.get("name", "")
        or _child_text(step_el, "filename")
        or _child_text(step_el, "file")
        or str(file_block.get("name", "") or "")
        or str(file_block.get("filename", "") or "")
    )
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name")
        if not name:
            continue
        try:
            length = int(_child_text(field_el, "length", "0") or "0")
        except ValueError:
            length = 0
        try:
            precision = int(_child_text(field_el, "precision", "0") or "0")
        except ValueError:
            precision = 0
        fields.append({
            "name": name,
            "type": _child_text(field_el, "type", "String"),
            "length": length,
            "precision": precision,
            "format": _child_text(field_el, "format"),
            "currency": _child_text(field_el, "currency"),
            "decimal": _child_text(field_el, "decimal"),
            "group": _child_text(field_el, "group"),
            "null_if": _child_text(field_el, "nullif") or _child_text(field_el, "null_if"),
            "nullif": _child_text(field_el, "nullif") or _child_text(field_el, "null_if"),
            "default": _child_text(field_el, "ifnull") or _child_text(field_el, "default"),
            "ifnull": _child_text(field_el, "ifnull") or _child_text(field_el, "default"),
            "position": _child_text(field_el, "position"),
            "trim_type": _child_text(field_el, "trim_type"),
            "repeat": _child_text(field_el, "repeat"),
        })

    filters: list[dict[str, str]] = []
    filters_el = step_el.find("filters")
    if filters_el is not None:
        for filter_el in filters_el.findall("filter"):
            filters.append({
                "filter_string": _child_text(filter_el, "filter_string"),
                "filter_position": _child_text(filter_el, "filter_position"),
                "filter_is_last_line": _child_text(filter_el, "filter_is_last_line"),
                "filter_is_positive": _child_text(filter_el, "filter_is_positive"),
            })

    extension = (
        str(file_block.get("extention", "") or "")
        or str(file_block.get("extension", "") or "")
        or primary.get("extention", "")
        or primary.get("extension", "")
        or _child_text(step_el, "extention")
        or _child_text(step_el, "extension")
    )
    file_type = (
        primary.get("type", "")
        or str(file_block.get("type", "") or "")
        or _child_text(step_el, "file_type")
        or _child_text(step_el, "filetype")
    )
    # Align write→read basenames: when Text File Output appends .csv for
    # extension-less paths, Text File Input must do the same.
    if not extension and str(file_type).strip().upper() in ("CSV", "CSV FILE", ""):
        # Infer csv when separator is present (CSV reader mode) and basename has no suffix.
        sep = _child_text(step_el, "separator", "")
        base = (filename or "").replace("\\", "/").rsplit("/", 1)[-1]
        if sep and base and "." not in base:
            extension = "csv"

    return {
        "filename": filename,
        "file": filename,
        "file_entries": file_entries,
        "file_block": file_block,
        "extension": extension,
        "extention": extension,
        "filemask": primary.get("filemask", "") or str(file_block.get("filemask", "") or ""),
        "exclude_filemask": (
            primary.get("exclude_filemask", "")
            or str(file_block.get("exclude_filemask", "") or "")
        ),
        "file_required": primary.get("file_required", "") or str(file_block.get("file_required", "") or ""),
        "include_subfolders": (
            primary.get("include_subfolders", "")
            or str(file_block.get("include_subfolders", "") or "")
            or _child_text(step_el, "include_subfolders")
        ),
        "separator": _child_text(step_el, "separator", ""),
        "enclosure": _child_text(step_el, "enclosure") or _child_text(step_el, "quote"),
        "enclosure_breaks": _child_text(step_el, "enclosure_breaks"),
        "escapechar": _child_text(step_el, "escapechar") or _child_text(step_el, "escape"),
        "header": _child_text(step_el, "header", ""),
        "nr_headerlines": _child_text(step_el, "nr_headerlines"),
        "footer": _child_text(step_el, "footer"),
        "nr_footerlines": _child_text(step_el, "nr_footerlines"),
        "line_wrapped": _child_text(step_el, "line_wrapped"),
        "nr_wraps": _child_text(step_el, "nr_wraps"),
        "layout_paged": _child_text(step_el, "layout_paged"),
        "nr_lines_per_page": _child_text(step_el, "nr_lines_per_page"),
        "nr_lines_doc_header": _child_text(step_el, "nr_lines_doc_header"),
        "noempty": _child_text(step_el, "noempty"),
        "include": _child_text(step_el, "include"),
        "include_field": _child_text(step_el, "include_field"),
        "rownum": _child_text(step_el, "rownum"),
        "rownumByFile": _child_text(step_el, "rownumByFile"),
        "rownum_field": _child_text(step_el, "rownum_field"),
        "format": _child_text(step_el, "format"),
        "encoding": _child_text(step_el, "encoding"),
        "length": _child_text(step_el, "length"),
        "buffer_size": _child_text(step_el, "buffer_size") or _child_text(step_el, "buffersize"),
        "lazy_conversion": (
            _child_text(step_el, "lazy_conversion")
            or _child_text(step_el, "lazyConversionActive")
            or _child_text(step_el, "lazy_conversion_active")
        ),
        "compression": (
            str(file_block.get("compression", "") or "")
            or primary.get("compression", "")
            or _child_text(step_el, "compression")
            or _child_text(step_el, "file_compression")
        ),
        "file_compression": (
            str(file_block.get("compression", "") or "")
            or primary.get("compression", "")
            or _child_text(step_el, "compression")
            or _child_text(step_el, "file_compression")
        ),
        "file_type": file_type,
        "accept_filenames": _child_text(step_el, "accept_filenames"),
        "passing_through_fields": _child_text(step_el, "passing_through_fields"),
        "accept_field": _child_text(step_el, "accept_field"),
        "accept_stepname": _child_text(step_el, "accept_stepname"),
        "add_to_result_filenames": _child_text(step_el, "add_to_result_filenames"),
        "limit": _child_text(step_el, "limit"),
        "error_ignored": _child_text(step_el, "error_ignored"),
        "skip_bad_files": _child_text(step_el, "skip_bad_files"),
        "file_error_field": _child_text(step_el, "file_error_field"),
        "file_error_message_field": _child_text(step_el, "file_error_message_field"),
        "error_line_skipped": _child_text(step_el, "error_line_skipped"),
        "error_count_field": _child_text(step_el, "error_count_field"),
        "error_fields_field": _child_text(step_el, "error_fields_field"),
        "error_text_field": _child_text(step_el, "error_text_field"),
        "bad_line_files_destination_directory": _child_text(
            step_el, "bad_line_files_destination_directory"
        ),
        "bad_line_files_extension": _child_text(step_el, "bad_line_files_extension"),
        "error_line_files_destination_directory": _child_text(
            step_el, "error_line_files_destination_directory"
        ),
        "error_line_files_extension": _child_text(step_el, "error_line_files_extension"),
        "line_number_files_destination_directory": _child_text(
            step_el, "line_number_files_destination_directory"
        ),
        "line_number_files_extension": _child_text(step_el, "line_number_files_extension"),
        "date_format_lenient": _child_text(step_el, "date_format_lenient"),
        "date_format_locale": _child_text(step_el, "date_format_locale"),
        "shortFileFieldName": _child_text(step_el, "shortFileFieldName"),
        "pathFieldName": _child_text(step_el, "pathFieldName"),
        "hiddenFieldName": _child_text(step_el, "hiddenFieldName"),
        "lastModificationTimeFieldName": _child_text(step_el, "lastModificationTimeFieldName"),
        "uriNameFieldName": _child_text(step_el, "uriNameFieldName"),
        "rootUriNameFieldName": _child_text(step_el, "rootUriNameFieldName"),
        "extensionFieldName": _child_text(step_el, "extensionFieldName"),
        "sizeFieldName": _child_text(step_el, "sizeFieldName"),
        "filters": filters,
        "fields": fields,
        "output_columns": [f["name"] for f in fields],
    }


def parse_file_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse common file-based Input step metadata (path, separator, fields)."""
    file_el = step_el.find("file")
    filename = (
        _child_text(step_el, "filename")
        or _child_text(step_el, "file")
        or (_child_text(file_el, "name") if file_el is not None else "")
        or (_child_text(file_el, "filename") if file_el is not None else "")
    )
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name")
        if not name:
            continue
        try:
            length = int(_child_text(field_el, "length", "0") or "0")
        except ValueError:
            length = 0
        fields.append({
            "name": name,
            "type": _child_text(field_el, "type", "String"),
            "length": length,
            "format": _child_text(field_el, "format"),
            "null_if": _child_text(field_el, "nullif") or _child_text(field_el, "null_if"),
            "default": _child_text(field_el, "ifnull") or _child_text(field_el, "default"),
        })
    return {
        "filename": filename,
        "file": filename,
        "separator": _child_text(step_el, "separator", ","),
        "enclosure": _child_text(step_el, "enclosure") or _child_text(step_el, "quote"),
        "header": _child_text(step_el, "header", "Y"),
        "encoding": _child_text(step_el, "encoding"),
        "compression": _child_text(step_el, "compression") or _child_text(step_el, "file_compression"),
        "file_type": _child_text(step_el, "file_type") or _child_text(step_el, "filetype"),
        "sheetname": _child_text(step_el, "sheetname", "Sheet1"),
        "row_tag": _child_text(step_el, "row_tag") or _child_text(step_el, "xml_source_element", "row"),
        "fields": fields,
        "output_columns": [f["name"] for f in fields],
    }


def parse_salesforce_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Salesforce Input connection and SOQL metadata."""
    return {
        "url": _child_text(step_el, "targeturl") or _child_text(step_el, "url"),
        "username": _child_text(step_el, "username"),
        "module": _child_text(step_el, "module") or _child_text(step_el, "object"),
        "query": _child_text(step_el, "query") or _child_text(step_el, "soql") or _child_text(step_el, "sql"),
        "condition": _child_text(step_el, "condition"),
        "fields": [
            {"name": _child_text(f, "name"), "type": _child_text(f, "type", "String")}
            for f in (step_el.find("fields").findall("field") if step_el.find("fields") is not None else [])
            if _child_text(f, "name")
        ],
    }


def parse_get_file_names_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get File Names / folder listing metadata."""
    file_el = step_el.find("file")
    return {
        "filename": (
            _child_text(step_el, "filename")
            or _child_text(step_el, "directory")
            or (_child_text(file_el, "name") if file_el is not None else "")
        ),
        "include_subfolders": _child_text(step_el, "include_subfolders")
        or _child_text(step_el, "include_subdir"),
        "wildcard": _child_text(step_el, "wildcard") or _child_text(step_el, "filemask"),
    }


def parse_get_table_names_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get Table Names schema/database metadata."""
    return {
        "schema": _child_text(step_el, "schemaname") or _child_text(step_el, "schema"),
        "database": _child_text(step_el, "database"),
        "connection": _child_text(step_el, "connection"),
        "include_system": _child_text(step_el, "includeSystemTables"),
    }


def parse_random_value_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Generate Random Value field definitions."""
    fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name")
        if name:
            fields.append({
                "name": name,
                "type": _child_text(field_el, "type") or _child_text(field_el, "randomtype", "number"),
            })
    return {
        "fields": fields,
        "output_columns": [f["name"] for f in fields],
        "limit": _child_text(step_el, "limit", "1"),
    }


def parse_db_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Insert/Update, Update, Delete, and Synchronize After Merge lookup metadata."""
    lookup_el = step_el.find("lookup")
    root = lookup_el if lookup_el is not None else step_el

    keys: list[dict[str, str]] = []
    for key_el in root.findall("key"):
        stream = (
            _child_text(key_el, "name")
            or _child_text(key_el, "keystream")
            or _child_text(key_el, "stream_field")
        )
        table_field = (
            _child_text(key_el, "field")
            or _child_text(key_el, "keylookup")
            or _child_text(key_el, "lookup")
            or stream
        )
        if stream or table_field:
            keys.append({
                "stream_field": stream or table_field,
                "table_field": table_field or stream,
                "condition": _child_text(key_el, "condition", "=") or "=",
                "stream_field2": _child_text(key_el, "name2") or _child_text(key_el, "keystream2"),
            })

    # Fallback: <fields><field><name>… used as key names by older exports
    if not keys:
        fields_el = step_el.find("fields")
        if fields_el is not None:
            for field_el in fields_el.findall("field"):
                name = _child_text(field_el, "name")
                if name:
                    keys.append({
                        "stream_field": name,
                        "table_field": name,
                        "condition": "=",
                        "stream_field2": "",
                    })

    values: list[dict[str, Any]] = []
    for value_el in root.findall("value"):
        stream = _child_text(value_el, "name") or _child_text(value_el, "updatestream")
        table_field = (
            _child_text(value_el, "rename")
            or _child_text(value_el, "updatelookup")
            or _child_text(value_el, "field")
            or stream
        )
        if stream or table_field:
            values.append({
                "stream_field": stream or table_field,
                "table_field": table_field or stream,
                "update": _bool_from_yn(_child_text(value_el, "update", "Y")),
            })

    schema = (
        _child_text(root, "schema")
        or _child_text(step_el, "schema")
        or _child_text(step_el, "schemaname")
    )
    table = (
        _child_text(root, "table")
        or _child_text(step_el, "table")
        or _child_text(step_el, "tablename")
    )
    return {
        "connection": _child_text(step_el, "connection"),
        "schema": schema,
        "table": table,
        "keys": keys,
        "values": values,
        "commit": _child_text(step_el, "commit") or _child_text(root, "commit"),
        "truncate": _child_text(step_el, "truncate"),
        "operation_order_field": (
            _child_text(step_el, "operation_order_field")
            or _child_text(root, "operation_order_field")
            or _child_text(step_el, "order_field")
        ),
        "order_insert": _child_text(step_el, "order_insert") or _child_text(root, "order_insert"),
        "order_update": _child_text(step_el, "order_update") or _child_text(root, "order_update"),
        "order_delete": _child_text(step_el, "order_delete") or _child_text(root, "order_delete"),
        "perform_lookup": _child_text(step_el, "perform_lookup") or _child_text(root, "perform_lookup"),
    }


def parse_table_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Table Output truncate / partitioning / batch options."""
    return {
        "connection": _child_text(step_el, "connection"),
        "schema": _child_text(step_el, "schema") or _child_text(step_el, "schemaname"),
        "table": _child_text(step_el, "table") or _child_text(step_el, "tablename"),
        "truncate": _child_text(step_el, "truncate", "N"),
        "ignore_errors": _child_text(step_el, "ignore_errors", "N"),
        "use_batch": _child_text(step_el, "use_batch", "Y"),
        "commit": _child_text(step_el, "commit"),
        "partitioning_enabled": _child_text(step_el, "partitioning_enabled", "N"),
        "partitioning_field": _child_text(step_el, "partitioning_field"),
        "specify_fields": _child_text(step_el, "specify_fields", "N"),
        "fields": [
            {
                "stream_field": _child_text(f, "stream_name") or _child_text(f, "name"),
                "table_field": _child_text(f, "column_name") or _child_text(f, "name"),
            }
            for f in (step_el.find("fields").findall("field") if step_el.find("fields") is not None else [])
            if _child_text(f, "stream_name") or _child_text(f, "name") or _child_text(f, "column_name")
        ],
    }


def parse_json_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse JSON Output file and bloc options."""
    file_el = step_el.find("file")
    return {
        "filename": (
            _child_text(step_el, "fileName")
            or _child_text(step_el, "filename")
            or (_child_text(file_el, "name") if file_el is not None else "")
        ),
        "extension": _child_text(step_el, "extension")
        or (_child_text(file_el, "extention") if file_el is not None else "")
        or (_child_text(file_el, "extension") if file_el is not None else ""),
        "operation": _child_text(step_el, "operation") or _child_text(step_el, "json_operation"),
        "json_bloc": _child_text(step_el, "json_bloc") or _child_text(step_el, "jsonBloc"),
        "nr_rows_in_bloc": _child_text(step_el, "nr_rows_in_bloc"),
        "output_value": _child_text(step_el, "outputValue") or _child_text(step_el, "output_value"),
        "compatibility_mode": _child_text(step_el, "compatibility_mode"),
        "encoding": _child_text(step_el, "encoding", "UTF-8"),
        "append": _child_text(step_el, "append", "N")
        or (_child_text(file_el, "append") if file_el is not None else "N"),
    }


def parse_xml_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse XML Output root/row tag and file options."""
    file_el = step_el.find("file")
    return {
        "filename": (
            _child_text(step_el, "fileName")
            or _child_text(step_el, "filename")
            or (_child_text(file_el, "name") if file_el is not None else "")
        ),
        "extension": _child_text(step_el, "extension")
        or (_child_text(file_el, "extention") if file_el is not None else "")
        or (_child_text(file_el, "extension") if file_el is not None else ""),
        "root_tag": _child_text(step_el, "mainElement") or _child_text(step_el, "root_tag", "rows"),
        "row_tag": _child_text(step_el, "repeatElement") or _child_text(step_el, "row_tag", "row"),
        "encoding": _child_text(step_el, "encoding", "UTF-8"),
        "namespace": _child_text(step_el, "namespace"),
        "omit_null": _child_text(step_el, "omitnullvalues") or _child_text(step_el, "omit_null"),
    }


def parse_excel_writer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Microsoft Excel Writer sheet / streaming options."""
    file_el = step_el.find("file")
    return {
        "filename": (
            _child_text(step_el, "file")
            or _child_text(step_el, "filename")
            or (_child_text(file_el, "name") if file_el is not None else "")
        ),
        "extension": _child_text(step_el, "extension")
        or (_child_text(file_el, "extension") if file_el is not None else "xlsx"),
        "sheetname": _child_text(step_el, "sheetname") or _child_text(step_el, "sheet_name", "Sheet1"),
        "header": _child_text(step_el, "header", "Y"),
        "footer": _child_text(step_el, "footer", "N"),
        "append": _child_text(step_el, "append", "N"),
        "streaming": _child_text(step_el, "sstream") or _child_text(step_el, "streaming", "N"),
        "template": _child_text(step_el, "template") or _child_text(step_el, "templatefile"),
        "if_file_exists": _child_text(step_el, "iffileexists") or _child_text(step_el, "if_file_exists"),
        "starting_cell": _child_text(step_el, "startingCell") or _child_text(step_el, "starting_cell", "A1"),
    }


def parse_excel_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Microsoft Excel Output (Deprecated) — full ExcelOutputMeta XML."""
    file_block = parse_file_block(step_el)
    template_el = step_el.find("template")
    custom_el = step_el.find("custom")

    filename = (
        str(file_block.get("name", ""))
        or _child_text(step_el, "filename")
        or _child_text(step_el, "file")
    )
    extension = (
        str(file_block.get("extention", ""))
        or str(file_block.get("extension", ""))
        or _child_text(step_el, "extension")
        or "xls"
    )
    sheetname = (
        str(file_block.get("sheetname", ""))
        or _child_text(step_el, "sheetname")
        or _child_text(step_el, "sheet_name")
        or "Sheet1"
    )

    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            fields.append({
                "name": name,
                "type": _child_text(field_el, "type", "String"),
                "format": _child_text(field_el, "format"),
                "nullif": _child_text(field_el, "nullif") or _child_text(field_el, "null_if"),
                "currency": _child_text(field_el, "currency"),
                "decimal": _child_text(field_el, "decimal"),
                "group": _child_text(field_el, "group"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
            })

    custom: dict[str, Any] = {}
    if custom_el is not None:
        for tag in (
            "header_font_name",
            "header_font_size",
            "header_font_bold",
            "header_font_italic",
            "header_font_underline",
            "header_font_orientation",
            "header_font_color",
            "header_background_color",
            "header_row_height",
            "header_alignment",
            "header_image",
            "row_font_name",
            "row_font_size",
            "row_font_color",
            "row_background_color",
        ):
            val = _child_text(custom_el, tag)
            if val:
                custom[tag] = val

    return {
        "filename": filename,
        "extension": extension,
        "sheetname": sheetname,
        "header": _child_text(step_el, "header", "Y"),
        "footer": _child_text(step_el, "footer", "N"),
        "encoding": _child_text(step_el, "encoding", ""),
        "append": _child_text(step_el, "append", "N"),
        "add_to_result_filenames": _child_text(step_el, "add_to_result_filenames", "N"),
        "do_not_open_newfile_init": str(file_block.get("do_not_open_newfile_init", "N")),
        "create_parent_folder": str(file_block.get("create_parent_folder", "N")),
        "split": str(file_block.get("split", "N")),
        "add_date": str(file_block.get("add_date", "N")),
        "add_time": str(file_block.get("add_time", "N")),
        "SpecifyFormat": str(file_block.get("SpecifyFormat", "N")),
        "date_time_format": str(file_block.get("date_time_format", "")),
        "autosizecolums": str(file_block.get("autosizecolums", "N")),
        "autosizecolumns": str(file_block.get("autosizecolums", file_block.get("autosizecolumns", "N"))),
        "nullisblank": str(file_block.get("nullisblank", "N")),
        "protect_sheet": str(file_block.get("protect_sheet", "N")),
        "password": str(file_block.get("password", "")),
        "password_set": bool(str(file_block.get("password", "") or "").strip()),
        "splitevery": str(file_block.get("splitevery", "") or "0"),
        "usetempfiles": str(file_block.get("usetempfiles", "N")),
        "tempdirectory": str(file_block.get("tempdirectory", "")),
        "template_enabled": (
            _child_text(template_el, "enabled", "N") if template_el is not None else "N"
        ),
        "template_append": (
            _child_text(template_el, "append", "N") if template_el is not None else "N"
        ),
        "template_filename": (
            _child_text(template_el, "filename") if template_el is not None else ""
        ),
        "fields": fields,
        "custom": custom,
        "file": file_block,
        # Excel Output (.xls) has no archive compression flag; preserve if present elsewhere.
        "compression": (
            _child_text(step_el, "compression")
            or str(file_block.get("compression", ""))
        ),
        "starting_cell": _child_text(step_el, "startingCell")
        or _child_text(step_el, "starting_cell", "A1"),
    }


def parse_sap_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse SAP Input (Deprecated) — SapInputMeta connection/function/fields XML."""
    function_el = step_el.find("function")
    function: dict[str, Any] = {}
    if function_el is not None:
        function = {
            "name": _child_text(function_el, "name") or _child_text(function_el, "function"),
            "description": _child_text(function_el, "description"),
            "group": _child_text(function_el, "group") or _child_text(function_el, "groupname"),
            "application": _child_text(function_el, "application") or _child_text(function_el, "appl"),
            "host": _child_text(function_el, "host"),
        }
    else:
        function = {
            "name": (
                _child_text(step_el, "function")
                or _child_text(step_el, "functionname")
                or _child_text(step_el, "module")
            ),
            "description": "",
            "group": "",
            "application": "",
            "host": "",
        }

    def _parse_sap_entries(parent_tag: str, child_tag: str) -> list[dict[str, Any]]:
        parent = step_el.find(parent_tag)
        if parent is None:
            return []
        rows: list[dict[str, Any]] = []
        for entry in parent.findall(child_tag):
            field_name = (
                _child_text(entry, "field_name")
                or _child_text(entry, "name")
                or _child_text(entry, "parameter_name")
            )
            if not field_name and not _child_text(entry, "parameter_name"):
                continue
            rows.append({
                "field_name": field_name,
                "sap_type": _child_text(entry, "sap_type") or _child_text(entry, "type"),
                "table_name": _child_text(entry, "table_name") or _child_text(entry, "tablename"),
                "parameter_name": _child_text(entry, "parameter_name") or field_name,
                "new_name": _child_text(entry, "new_name") or _child_text(entry, "rename"),
                "target_type": _child_text(entry, "target_type") or _child_text(entry, "type", "String"),
            })
        return rows

    parameters = _parse_sap_entries("parameters", "parameter")
    if not parameters:
        parameters = _parse_sap_entries("parameters", "field")
    fields = _parse_sap_entries("fields", "field")
    if not fields:
        fields = _parse_sap_entries("output", "field")

    # Batch / pagination often expressed as RFC_READ_TABLE parameters.
    batch_size = _child_text(step_el, "batch_size") or _child_text(step_el, "batchSize")
    page_size = _child_text(step_el, "page_size") or _child_text(step_el, "pagesize")
    row_skips = _child_text(step_el, "rowskips") or _child_text(step_el, "ROWSKIPS")
    filters: list[dict[str, Any]] = []
    for param in parameters:
        pname = (param.get("parameter_name") or param.get("field_name") or "").upper()
        if pname in ("ROWCOUNT", "BATCH_SIZE", "BATCHSIZE") and not batch_size:
            batch_size = param.get("field_name") or pname
        if pname in ("ROWSKIPS", "OFFSET", "PAGE") and not row_skips:
            row_skips = param.get("field_name") or pname
        if pname in ("OPTIONS", "FILTER", "TEXT") or (param.get("table_name") or "").upper() == "OPTIONS":
            filters.append(param)

    # Connection scalars may live on the named DatabaseMeta (SAP ERP) or on the step.
    connection_name = _child_text(step_el, "connection")
    sap_connection = {
        "name": connection_name,
        "host": (
            _child_text(step_el, "host")
            or _child_text(step_el, "server")
            or _child_text(step_el, "hostname")
            or function.get("host", "")
        ),
        "client": (
            _child_text(step_el, "client")
            or _child_text(step_el, "sap_client")
            or _child_text(step_el, "SAP_CLIENT")
        ),
        "system": (
            _child_text(step_el, "system")
            or _child_text(step_el, "sysnr")
            or _child_text(step_el, "system_number")
            or _child_text(step_el, "SAP_SYSTEM_NUMBER")
        ),
        "language": (
            _child_text(step_el, "language")
            or _child_text(step_el, "sap_language")
            or _child_text(step_el, "SAP_LANGUAGE")
        ),
        "username": (
            _child_text(step_el, "username")
            or _child_text(step_el, "user")
            or _child_text(step_el, "login")
        ),
        "password": _child_text(step_el, "password"),
        "password_set": bool(_child_text(step_el, "password").strip()),
        "port": _child_text(step_el, "port"),
    }
    # Never keep plaintext passwords in migration metadata consumed by codegen.
    if sap_connection.get("password"):
        sap_connection["password"] = "***REDACTED***"

    table_or_module = (
        function.get("name")
        or _child_text(step_el, "table")
        or _child_text(step_el, "module")
    )

    return {
        "connection": connection_name,
        "sap_connection": sap_connection,
        "client": sap_connection.get("client", ""),
        "system": sap_connection.get("system", ""),
        "language": sap_connection.get("language", ""),
        "username": sap_connection.get("username", ""),
        "password_set": bool(sap_connection.get("password_set")),
        "host": sap_connection.get("host", ""),
        "function": function,
        "function_name": function.get("name", ""),
        "module": table_or_module,
        "table": _child_text(step_el, "table") or table_or_module,
        "parameters": parameters,
        "fields": fields,
        "filters": filters,
        "batch_size": batch_size,
        "page_size": page_size,
        "row_skips": row_skips,
        "pagination": {
            "batch_size": batch_size,
            "page_size": page_size,
            "row_skips": row_skips,
        },
    }


def parse_salesforce_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Salesforce Insert/Update/Upsert/Delete connection metadata."""
    return {
        "url": _child_text(step_el, "targeturl") or _child_text(step_el, "url"),
        "username": _child_text(step_el, "username"),
        "module": _child_text(step_el, "module") or _child_text(step_el, "sfObject") or _child_text(step_el, "object"),
        "batch_size": _child_text(step_el, "batchSize") or _child_text(step_el, "batch_size"),
        "upsert_field": (
            _child_text(step_el, "upsertfield")
            or _child_text(step_el, "externalIdFieldName")
            or _child_text(step_el, "upsert_field")
            or _child_text(step_el, "idfield")
        ),
        "timeout": _child_text(step_el, "timeout"),
        "use_compression": _child_text(step_el, "useCompression"),
        "fields": [
            {
                "name": _child_text(f, "name"),
                "lookup": _child_text(f, "lookup"),
                "update": _child_text(f, "update", "Y"),
            }
            for f in (step_el.find("fields").findall("field") if step_el.find("fields") is not None else [])
            if _child_text(f, "name")
        ],
    }


def _parse_avro_fields(step_el: ET.Element) -> list[dict[str, Any]]:
    """Extract Avro field path/name/type definitions from step XML."""
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name") or _child_text(field_el, "path")
        if not name:
            continue
        fields.append({
            "name": name,
            "path": _child_text(field_el, "path") or _child_text(field_el, "source_path") or name,
            "type": _child_text(field_el, "type", "String"),
            "nullable": _child_text(field_el, "nullable") or _child_text(field_el, "nullif"),
            "default": _child_text(field_el, "default") or _child_text(field_el, "ifnull"),
            "format": _child_text(field_el, "format"),
        })
    return fields


def parse_avro_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Avro Input path, schema, compression, encoding, and recursive options."""
    file_el = step_el.find("file")
    filename = (
        _child_text(step_el, "filename")
        or _child_text(step_el, "fileName")
        or _child_text(step_el, "dataLocation")
        or _child_text(step_el, "data_location")
        or _child_text(step_el, "file")
        or (_child_text(file_el, "name") if file_el is not None else "")
        or (_child_text(file_el, "filename") if file_el is not None else "")
    )
    schema_filename = (
        _child_text(step_el, "schemaFilename")
        or _child_text(step_el, "schema_filename")
        or _child_text(step_el, "schemaLocation")
        or _child_text(step_el, "schema_location")
        or _child_text(step_el, "avroSchema")
    )
    schema_json = (
        _child_text(step_el, "schema")
        or _child_text(step_el, "jsonSchema")
        or _child_text(step_el, "json_schema")
        or _child_text(step_el, "defaultSchema")
        or _child_text(step_el, "embeddedSchema")
    )
    fields = _parse_avro_fields(step_el)
    return {
        "filename": filename,
        "file": filename,
        "schema_filename": schema_filename,
        "schema_json": schema_json,
        "compression": (
            _child_text(step_el, "compression")
            or _child_text(step_el, "file_compression")
            or _child_text(step_el, "codec")
            or (_child_text(file_el, "compression") if file_el is not None else "")
        ),
        "encoding": _child_text(step_el, "encoding") or _child_text(step_el, "charset"),
        "is_binary_encoded": (
            _child_text(step_el, "isDataBinaryEncoded")
            or _child_text(step_el, "dataBinaryEncoded")
            or _child_text(step_el, "binary_encoded")
        ),
        "recursive": (
            _child_text(step_el, "include_subfolders")
            or _child_text(step_el, "includeSubFolders")
            or _child_text(step_el, "recursive")
            or _child_text(step_el, "IsInFields")
            or (_child_text(file_el, "include_subfolders") if file_el is not None else "")
        ),
        "filemask": (
            _child_text(step_el, "filemask")
            or _child_text(step_el, "wildcard")
            or (_child_text(file_el, "filemask") if file_el is not None else "")
        ),
        "schema_in_field": (
            _child_text(step_el, "schemaInField")
            or _child_text(step_el, "schema_in_field")
            or _child_text(step_el, "schemaField")
        ),
        "data_in_field": (
            _child_text(step_el, "dataInField")
            or _child_text(step_el, "data_in_field")
            or _child_text(step_el, "passingThruFields")
        ),
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
    }


def parse_avro_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Avro Output path, mode, compression, and schema evolution options."""
    file_el = step_el.find("file")
    filename = (
        _child_text(step_el, "filename")
        or _child_text(step_el, "fileName")
        or _child_text(step_el, "file")
        or (_child_text(file_el, "name") if file_el is not None else "")
        or (_child_text(file_el, "filename") if file_el is not None else "")
    )
    fields = _parse_avro_fields(step_el)
    append_raw = (
        _child_text(step_el, "append")
        or (_child_text(file_el, "append") if file_el is not None else "")
        or "N"
    )
    overwrite_raw = (
        _child_text(step_el, "overwrite")
        or _child_text(step_el, "create_parent_folder")
        or "Y"
    )
    return {
        "filename": filename,
        "file": filename,
        "append": append_raw,
        "overwrite": overwrite_raw,
        "compression": (
            _child_text(step_el, "compression")
            or _child_text(step_el, "codec")
            or _child_text(step_el, "compressionCodec")
            or (_child_text(file_el, "compression") if file_el is not None else "")
        ),
        "schema_filename": (
            _child_text(step_el, "schemaFilename")
            or _child_text(step_el, "schema_filename")
            or _child_text(step_el, "schemaLocation")
        ),
        "schema_json": (
            _child_text(step_el, "schema")
            or _child_text(step_el, "jsonSchema")
            or _child_text(step_el, "json_schema")
        ),
        "schema_evolution": (
            _child_text(step_el, "schemaEvolution")
            or _child_text(step_el, "schema_evolution")
            or _child_text(step_el, "nullable")
            or _child_text(step_el, "writeSchema")
        ),
        "encoding": _child_text(step_el, "encoding"),
        "namespace": _child_text(step_el, "namespace") or _child_text(step_el, "recordNamespace"),
        "record_name": _child_text(step_el, "recordName") or _child_text(step_el, "record_name"),
        "doc": _child_text(step_el, "doc") or _child_text(step_el, "documentation"),
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
    }


def _parse_mongodb_fields(step_el: ET.Element) -> list[dict[str, Any]]:
    """Extract MongoDB field path mappings from step XML."""
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        fields_el = step_el.find("mongo_fields")
    targets = fields_el.findall("field") if fields_el is not None else []
    for field_el in targets:
        name = (
            _child_text(field_el, "field_name")
            or _child_text(field_el, "name")
            or _child_text(field_el, "incoming_field_name")
        )
        path = (
            _child_text(field_el, "field_path")
            or _child_text(field_el, "mongo_doc_path")
            or _child_text(field_el, "path")
            or name
        )
        if not name and not path:
            continue
        fields.append({
            "name": name or path,
            "path": path or name,
            "type": _child_text(field_el, "type") or _child_text(field_el, "kettle_type", "String"),
            "indexed": _child_text(field_el, "indexed_vals") or _child_text(field_el, "indexed"),
            "update_match": _child_text(field_el, "update_match_field") or _child_text(field_el, "matcher"),
            "modifier_operation": (
                _child_text(field_el, "modifier_op")
                or _child_text(field_el, "modifier_operation")
            ),
            "json": _child_text(field_el, "json"),
        })
    return fields


def _mongodb_connection_uri(cfg: dict[str, Any]) -> str:
    """Build a MongoDB connection URI from parsed connection pieces when URI is absent."""
    existing = (cfg.get("connection_uri") or "").strip()
    if existing:
        return existing
    host = (cfg.get("hostname") or "").strip()
    if not host:
        return ""
    port = (cfg.get("port") or "27017").strip() or "27017"
    user = (cfg.get("auth_user") or "").strip()
    password = (cfg.get("auth_password") or "").strip()
    auth_db = (cfg.get("auth_database") or cfg.get("database") or "").strip()
    use_ssl = str(cfg.get("use_ssl") or "").strip().upper() in ("Y", "YES", "TRUE", "1")
    scheme = "mongodb+srv" if str(cfg.get("use_srv") or "").upper() in ("Y", "YES", "TRUE", "1") else "mongodb"
    auth = ""
    if user:
        # Password left as placeholder secret reference when present
        secret = "${mongodb_password}" if password else ""
        auth = f"{user}:{secret}@" if secret else f"{user}@"
    uri = f"{scheme}://{auth}{host}"
    if scheme == "mongodb" and port and ":" not in host.split(",")[0]:
        uri = f"{uri}:{port}"
    params: list[str] = []
    if auth_db:
        params.append(f"authSource={auth_db}")
    if use_ssl:
        params.append("ssl=true")
    read_pref = (cfg.get("read_preference") or "").strip()
    if read_pref:
        params.append(f"readPreference={read_pref.replace(' ', '')}")
    if params:
        uri = f"{uri}/?{'&'.join(params)}"
    return uri


def parse_mongodb_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse MongoDB Input connection, query, projection, auth, and batch options."""
    fields = _parse_mongodb_fields(step_el)
    connection_uri = (
        _child_text(step_el, "connection_string")
        or _child_text(step_el, "connection_uri")
        or _child_text(step_el, "uri")
        or _child_text(step_el, "mongoURI")
        or _child_text(step_el, "host_port")
    )
    cfg: dict[str, Any] = {
        "connection_uri": connection_uri,
        "hostname": (
            _child_text(step_el, "hostname")
            or _child_text(step_el, "host")
            or _child_text(step_el, "hostnames")
        ),
        "port": (
            _child_text(step_el, "port")
            or (
                "27017"
                if (
                    _child_text(step_el, "hostname")
                    or _child_text(step_el, "host")
                    or _child_text(step_el, "hostnames")
                )
                else ""
            )
        ),
        "database": (
            _child_text(step_el, "db_name")
            or _child_text(step_el, "database")
            or _child_text(step_el, "dbname")
        ),
        "collection": _child_text(step_el, "collection") or _child_text(step_el, "collection_name"),
        "query": (
            _child_text(step_el, "query")
            or _child_text(step_el, "json_query")
            or _child_text(step_el, "query_expression")
        ),
        "projection": (
            _child_text(step_el, "fields_name")
            or _child_text(step_el, "fields_expression")
            or _child_text(step_el, "projection")
            or _child_text(step_el, "fields_query")
        ),
        "aggregation_pipeline": (
            _child_text(step_el, "agg_pipeline")
            or _child_text(step_el, "aggregation_pipeline")
            or _child_text(step_el, "pipeline")
        ),
        "query_is_pipeline": (
            _child_text(step_el, "query_is_pipeline")
            or _child_text(step_el, "execute_aggr")
            or _child_text(step_el, "use_agg_pipeline")
        ),
        "auth_user": (
            _child_text(step_el, "auth_user")
            or _child_text(step_el, "username")
            or _child_text(step_el, "authentication_user")
        ),
        "auth_password": (
            _child_text(step_el, "auth_password")
            or _child_text(step_el, "password")
            or _child_text(step_el, "authentication_password")
        ),
        "auth_database": (
            _child_text(step_el, "auth_db")
            or _child_text(step_el, "authentication_database")
            or _child_text(step_el, "auth_database")
        ),
        "auth_mechanism": (
            _child_text(step_el, "auth_mech")
            or _child_text(step_el, "authentication_mechanism")
            or _child_text(step_el, "auth_mechanism")
        ),
        "kerberos": _child_text(step_el, "use_kerberos") or _child_text(step_el, "kerberos"),
        "use_ssl": (
            _child_text(step_el, "ssl")
            or _child_text(step_el, "use_ssl")
            or _child_text(step_el, "ssl_enabled")
        ),
        "use_srv": _child_text(step_el, "use_srv") or _child_text(step_el, "use_mongodb_srv"),
        "read_preference": (
            _child_text(step_el, "read_preference")
            or _child_text(step_el, "read_pref")
            or _child_text(step_el, "readPreference")
        ),
        "tag_sets": _child_text(step_el, "tag_sets") or _child_text(step_el, "tag_set"),
        "connect_timeout": (
            _child_text(step_el, "connect_timeout")
            or _child_text(step_el, "connectTimeout")
            or _child_text(step_el, "connectTimeoutMS")
        ),
        "socket_timeout": (
            _child_text(step_el, "socket_timeout")
            or _child_text(step_el, "socketTimeout")
            or _child_text(step_el, "socketTimeoutMS")
        ),
        "batch_size": (
            _child_text(step_el, "batch_size")
            or _child_text(step_el, "batchSize")
            or _child_text(step_el, "size")
        ),
        "output_json": (
            _child_text(step_el, "output_json")
            or _child_text(step_el, "json_output")
            or _child_text(step_el, "output_as_json")
        ),
        "json_field": (
            _child_text(step_el, "json_field")
            or _child_text(step_el, "json_field_name")
            or _child_text(step_el, "output_field")
        ),
        "use_all_replica_members": (
            _child_text(step_el, "use_all_replica_members")
            or _child_text(step_el, "read_from_all")
        ),
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
    }
    cfg["connection_uri"] = _mongodb_connection_uri(cfg)
    return cfg


def parse_mongodb_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse MongoDB Output connection, write mode, upsert, batch, and auth options."""
    fields = _parse_mongodb_fields(step_el)
    connection_uri = (
        _child_text(step_el, "connection_string")
        or _child_text(step_el, "connection_uri")
        or _child_text(step_el, "uri")
        or _child_text(step_el, "mongoURI")
    )
    cfg: dict[str, Any] = {
        "connection_uri": connection_uri,
        "hostname": (
            _child_text(step_el, "hostname")
            or _child_text(step_el, "host")
            or _child_text(step_el, "hostnames")
        ),
        "port": (
            _child_text(step_el, "port")
            or (
                "27017"
                if (
                    _child_text(step_el, "hostname")
                    or _child_text(step_el, "host")
                    or _child_text(step_el, "hostnames")
                )
                else ""
            )
        ),
        "database": (
            _child_text(step_el, "db_name")
            or _child_text(step_el, "database")
            or _child_text(step_el, "dbname")
        ),
        "collection": _child_text(step_el, "collection") or _child_text(step_el, "collection_name"),
        "truncate": _child_text(step_el, "truncate") or _child_text(step_el, "drop"),
        "update": _child_text(step_el, "update") or _child_text(step_el, "do_updates"),
        "upsert": (
            _child_text(step_el, "upsert")
            or _child_text(step_el, "multi")
            or _child_text(step_el, "modifier_update")
        ),
        "multi": _child_text(step_el, "multi") or _child_text(step_el, "update_all"),
        "modifier_update": (
            _child_text(step_el, "modifier_update")
            or _child_text(step_el, "modifier")
        ),
        "write_concern": (
            _child_text(step_el, "write_concern")
            or _child_text(step_el, "writeConcern")
            or _child_text(step_el, "w_timeout")
        ),
        "batch_size": (
            _child_text(step_el, "batch_insert_size")
            or _child_text(step_el, "batch_size")
            or _child_text(step_el, "batchSize")
        ),
        "auth_user": (
            _child_text(step_el, "auth_user")
            or _child_text(step_el, "username")
            or _child_text(step_el, "authentication_user")
        ),
        "auth_password": (
            _child_text(step_el, "auth_password")
            or _child_text(step_el, "password")
            or _child_text(step_el, "authentication_password")
        ),
        "auth_database": (
            _child_text(step_el, "auth_db")
            or _child_text(step_el, "authentication_database")
            or _child_text(step_el, "auth_database")
        ),
        "auth_mechanism": (
            _child_text(step_el, "auth_mech")
            or _child_text(step_el, "authentication_mechanism")
            or _child_text(step_el, "auth_mechanism")
        ),
        "kerberos": _child_text(step_el, "use_kerberos") or _child_text(step_el, "kerberos"),
        "use_ssl": (
            _child_text(step_el, "ssl")
            or _child_text(step_el, "use_ssl")
            or _child_text(step_el, "ssl_enabled")
        ),
        "use_srv": _child_text(step_el, "use_srv") or _child_text(step_el, "use_mongodb_srv"),
        "retry_writes": _child_text(step_el, "retryWrites") or _child_text(step_el, "retry_writes"),
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
        "match_fields": [
            f["name"] for f in fields
            if str(f.get("update_match") or "").strip().upper() in ("Y", "YES", "TRUE", "1")
        ],
    }
    cfg["connection_uri"] = _mongodb_connection_uri(cfg)
    return cfg


def parse_database_lookup_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Database Lookup / Stream Lookup keys, return fields, and cache metadata."""
    keys: list[DatabaseLookupKey] = []
    lookup_el = step_el.find("lookup")
    if lookup_el is not None:
        for key_el in lookup_el.findall("key"):
            stream = _child_text(key_el, "name") or _child_text(key_el, "field")
            table_field = (
                _child_text(key_el, "field")
                or _child_text(key_el, "lookup")
                or _child_text(key_el, "name2")
            )
            if stream or table_field:
                keys.append(
                    DatabaseLookupKey(
                        stream_field=stream,
                        table_field=table_field or stream,
                        name2=_child_text(key_el, "name2"),
                    )
                )
    if not keys:
        keys = [
            DatabaseLookupKey(stream_field=pair.left, table_field=pair.right)
            for pair in parse_join_keys(step_el)
        ]

    return_fields: list[DatabaseLookupReturnField] = []
    value_parents = [step_el]
    if lookup_el is not None:
        value_parents.append(lookup_el)
    seen_return: set[str] = set()
    for parent in value_parents:
        for value_el in parent.findall("value"):
            name = _child_text(value_el, "name")
            if not name or name in seen_return:
                continue
            seen_return.add(name)
            return_fields.append(
                DatabaseLookupReturnField(
                    name=name,
                    rename=_child_text(value_el, "rename"),
                    default=_child_text(value_el, "default"),
                    type_name=_child_text(value_el, "type"),
                )
            )

    cache_size_raw = (
        _child_text(step_el, "cache_size")
        or _child_text(step_el, "cacheSize")
        or "0"
    )
    try:
        cache_size = int(cache_size_raw or "0")
    except ValueError:
        cache_size = 0

    cfg = DatabaseLookupConfig(
        connection=_child_text(step_el, "connection"),
        schema=_child_text(step_el, "schema"),
        table=_child_text(step_el, "table"),
        cached=_bool_from_yn(
            _child_text(step_el, "cached") or _child_text(step_el, "cache")
        ),
        cache_size=cache_size,
        orderby=_child_text(step_el, "orderby") or _child_text(step_el, "orderBy"),
        fail_on_multiple=_bool_from_yn(_child_text(step_el, "fail_on_multiple")),
        eat_row_on_failure=_bool_from_yn(_child_text(step_el, "eat_row_on_failure")),
        keys=keys,
        return_fields=return_fields,
    )
    return _metadata_value(cfg)


_DIM_UPDATE_TYPE_MAP: dict[str, str] = {
    "0": "Insert",
    "insert": "Insert",
    "typeii": "Insert",
    "type ii": "Insert",
    "type2": "Insert",
    "scd2": "Insert",
    "1": "Update",
    "update": "Update",
    "typei": "Update",
    "type i": "Update",
    "type1": "Update",
    "scd1": "Update",
    "2": "PunchThrough",
    "punchthrough": "PunchThrough",
    "punch through": "PunchThrough",
    "3": "DateInsertedOrUpdated",
    "dateinsertedorupdated": "DateInsertedOrUpdated",
    "4": "DateInserted",
    "dateinserted": "DateInserted",
    "5": "DateUpdated",
    "dateupdated": "DateUpdated",
    "6": "LastVersion",
    "lastversion": "LastVersion",
}


def _normalize_dim_update_type(raw: str) -> str:
    key = (raw or "").strip().lower().replace("_", " ")
    compact = key.replace(" ", "")
    return (
        _DIM_UPDATE_TYPE_MAP.get(key)
        or _DIM_UPDATE_TYPE_MAP.get(compact)
        or ((raw or "").strip() or "Insert")
    )


def _parse_dw_business_keys(fields_el: ET.Element | None) -> list[DimensionLookupKey]:
    keys: list[DimensionLookupKey] = []
    if fields_el is None:
        return keys
    for key_el in fields_el.findall("key"):
        stream = _child_text(key_el, "name") or _child_text(key_el, "field")
        table_field = (
            _child_text(key_el, "lookup")
            or _child_text(key_el, "field")
            or stream
        )
        if stream or table_field:
            keys.append(
                DimensionLookupKey(
                    stream_field=stream or table_field,
                    table_field=table_field or stream,
                )
            )
    return keys


def parse_dimension_lookup_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Dimension Lookup/Update SCD keys, attributes, and technical fields."""
    fields_el = step_el.find("fields")
    keys = _parse_dw_business_keys(fields_el)

    dim_fields: list[DimensionLookupField] = []
    stream_datefield = ""
    date_from = ""
    date_to = ""
    technical_key = ""
    technical_key_rename = ""
    tech_key_creation = "tablemax"
    use_autoinc = False
    version_field = ""

    if fields_el is not None:
        date_el = fields_el.find("date")
        if date_el is not None:
            stream_datefield = _child_text(date_el, "name")
            date_from = _child_text(date_el, "from")
            date_to = _child_text(date_el, "to")

        for field_el in fields_el.findall("field"):
            stream = _child_text(field_el, "name")
            table_field = _child_text(field_el, "lookup") or stream
            update_raw = _child_text(field_el, "update") or "Insert"
            if stream or table_field:
                dim_fields.append(
                    DimensionLookupField(
                        stream_field=stream or table_field,
                        table_field=table_field or stream,
                        update_type=_normalize_dim_update_type(update_raw),
                    )
                )

        return_el = fields_el.find("return")
        if return_el is not None:
            technical_key = _child_text(return_el, "name")
            technical_key_rename = _child_text(return_el, "rename")
            tech_key_creation = (
                _child_text(return_el, "creation_method") or "tablemax"
            ).strip().lower() or "tablemax"
            use_autoinc = _bool_from_yn(_child_text(return_el, "use_autoinc"))
            version_field = _child_text(return_el, "version")

    def _int_field(*tags: str, default: int) -> int:
        for tag in tags:
            raw = _child_text(step_el, tag)
            if raw:
                try:
                    return int(raw)
                except ValueError:
                    pass
        return default

    cfg = DimensionLookupConfig(
        connection=_child_text(step_el, "connection"),
        schema=_child_text(step_el, "schema"),
        table=_child_text(step_el, "table") or _child_text(step_el, "tablename"),
        update=_bool_from_yn(_child_text(step_el, "update", "Y"), default=True),
        commit_size=_int_field("commit", default=100),
        cache_size=_int_field("cache_size", "cacheSize", default=5000),
        preload_cache=_bool_from_yn(
            _child_text(step_el, "preload_cache")
            or _child_text(step_el, "preloadCache")
        ),
        keys=keys,
        fields=dim_fields,
        technical_key=technical_key,
        technical_key_rename=technical_key_rename,
        tech_key_creation=tech_key_creation,
        use_autoinc=use_autoinc,
        version_field=version_field,
        sequence_name=_child_text(step_el, "sequence"),
        stream_datefield=stream_datefield,
        date_from=date_from,
        date_to=date_to,
        min_year=_int_field("min_year", default=1900),
        max_year=_int_field("max_year", default=2199),
        use_start_date_alternative=_bool_from_yn(
            _child_text(step_el, "use_start_date_alternative")
        ),
        start_date_alternative=(
            _child_text(step_el, "start_date_alternative") or "none"
        ).strip().lower() or "none",
        start_date_field_name=_child_text(step_el, "start_date_field_name"),
        use_batch=_bool_from_yn(_child_text(step_el, "useBatch", "Y"), default=True),
    )
    meta = _metadata_value(cfg)
    # Aliases expected by join/lookup lineage helpers
    meta["keys"] = [
        {"stream_field": k.stream_field, "table_field": k.table_field, "left": k.stream_field, "right": k.table_field}
        for k in keys
    ]
    meta["return_fields"] = [
        {
            "name": technical_key,
            "rename": technical_key_rename or technical_key,
            "default": "",
            "type_name": "Integer",
        }
    ] if technical_key else []
    meta["cached"] = cfg.cache_size > 0 or cfg.preload_cache
    return meta


def parse_combination_lookup_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Combination Lookup/Update business keys and surrogate-key settings."""
    fields_el = step_el.find("fields")
    keys = _parse_dw_business_keys(fields_el)

    technical_key = ""
    tech_key_creation = "tablemax"
    use_autoinc = False
    if fields_el is not None:
        return_el = fields_el.find("return")
        if return_el is not None:
            technical_key = _child_text(return_el, "name")
            tech_key_creation = (
                _child_text(return_el, "creation_method") or "tablemax"
            ).strip().lower() or "tablemax"
            use_autoinc = _bool_from_yn(_child_text(return_el, "use_autoinc"))

    def _int_field(*tags: str, default: int) -> int:
        for tag in tags:
            raw = _child_text(step_el, tag)
            if raw:
                try:
                    return int(raw)
                except ValueError:
                    pass
        return default

    cfg = CombinationLookupConfig(
        connection=_child_text(step_el, "connection"),
        schema=_child_text(step_el, "schema"),
        table=_child_text(step_el, "table") or _child_text(step_el, "tablename"),
        commit_size=_int_field("commit", default=100),
        cache_size=_int_field("cache_size", "cacheSize", default=9999),
        preload_cache=_bool_from_yn(
            _child_text(step_el, "preloadCache")
            or _child_text(step_el, "preload_cache")
        ),
        replace_fields=_bool_from_yn(_child_text(step_el, "replace")),
        use_hash=_bool_from_yn(_child_text(step_el, "crc")),
        hash_field=_child_text(step_el, "crcfield"),
        keys=keys,
        technical_key=technical_key,
        tech_key_creation=tech_key_creation,
        use_autoinc=use_autoinc,
        sequence_name=_child_text(step_el, "sequence"),
        last_update_field=_child_text(step_el, "last_update_field"),
    )
    meta = _metadata_value(cfg)
    meta["keys"] = [
        {"stream_field": k.stream_field, "table_field": k.table_field, "left": k.stream_field, "right": k.table_field}
        for k in keys
    ]
    meta["return_fields"] = [
        {
            "name": technical_key,
            "rename": technical_key,
            "default": "",
            "type_name": "Integer",
        }
    ] if technical_key else []
    meta["cached"] = cfg.cache_size > 0 or cfg.preload_cache
    return meta


def _parse_lookup_argument_list(step_el: ET.Element, *parent_tags: str) -> list[dict[str, str]]:
    """Parse generic argument/parameter/header field lists used by Lookup steps."""
    items: list[dict[str, str]] = []
    parents: list[ET.Element] = [step_el]
    for tag in parent_tags:
        found = step_el.find(tag)
        if found is not None:
            parents.append(found)
    for parent in parents:
        for child in list(parent):
            if child.tag not in (
                "argument", "parameter", "field", "lookupArgument",
                "header", "lookupValue", "lookup", "value",
            ):
                continue
            name = (
                _child_text(child, "name")
                or _child_text(child, "field")
                or _child_text(child, "argument")
                or child.get("name", "")
            )
            if not name and len(child) == 0 and _text(child):
                name = _text(child)
            if not name:
                continue
            items.append({
                "name": name,
                "field": (
                    _child_text(child, "field")
                    or _child_text(child, "argumentField")
                    or _child_text(child, "parameterField")
                    or name
                ),
                "direction": (
                    _child_text(child, "direction")
                    or _child_text(child, "dir")
                    or _child_text(child, "inout")
                ),
                "type": _child_text(child, "type") or _child_text(child, "type_name"),
                "rename": _child_text(child, "rename") or _child_text(child, "rename_to"),
                "default": _child_text(child, "default") or _child_text(child, "default_value"),
                "value": _child_text(child, "value") or _child_text(child, "parameter"),
                "header": _child_text(child, "header") or _child_text(child, "parameter"),
            })
    return items


def parse_db_proc_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Call DB Procedure connection, procedure name, and I/O parameters."""
    parameters: list[dict[str, str]] = []
    arg_parent = step_el.find("lookup")
    if arg_parent is None:
        arg_parent = step_el.find("arguments")
    if arg_parent is None:
        arg_parent = step_el
    for arg_el in arg_parent.findall("argument"):
        name = _child_text(arg_el, "name") or _child_text(arg_el, "field")
        if not name:
            continue
        parameters.append({
            "name": name,
            "direction": _child_text(arg_el, "direction") or _child_text(arg_el, "dir") or "IN",
            "type": _child_text(arg_el, "type"),
            "rename": _child_text(arg_el, "rename"),
        })
    if not parameters:
        parameters = _parse_lookup_argument_list(step_el, "argument", "arguments", "lookup")

    results: list[dict[str, str]] = []
    result_els = list(step_el.findall("result"))
    results_parent = step_el.find("results")
    if results_parent is not None:
        result_els.extend(results_parent.findall("result"))
    for res_el in result_els:
        name = _child_text(res_el, "name") or _child_text(res_el, "field") or _text(res_el)
        if not name:
            continue
        results.append({
            "name": name,
            "type": _child_text(res_el, "type"),
            "rename": _child_text(res_el, "rename"),
        })
    result_name = _child_text(step_el, "result_name") or _child_text(step_el, "resultName")
    if result_name and not any(r["name"] == result_name for r in results):
        results.append({
            "name": result_name,
            "type": _child_text(step_el, "result_type") or _child_text(step_el, "resultType"),
            "rename": "",
        })

    return {
        "connection": _child_text(step_el, "connection"),
        "procedure": (
            _child_text(step_el, "procedure")
            or _child_text(step_el, "procedureName")
            or _child_text(step_el, "proc")
        ),
        "auto_commit": _bool_from_yn(
            _child_text(step_el, "auto_commit") or _child_text(step_el, "autoCommit"), True
        ),
        "parameters": parameters,
        "results": results,
        "result_name": result_name,
    }


def parse_db_join_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Database Join SQL, outer-join flag, and parameter fields."""
    params: list[dict[str, str]] = []
    for parent_tag in ("parameter", "parameters", "lookup"):
        parent = step_el.find(parent_tag)
        if parent is None:
            continue
        for field_el in parent.findall("field") + parent.findall("parameter"):
            name = _child_text(field_el, "name") or _child_text(field_el, "field")
            if not name:
                continue
            params.append({
                "name": name,
                "type": _child_text(field_el, "type"),
            })
    # Flat parameter fields
    for field_el in step_el.findall("parameter"):
        name = _child_text(field_el, "name") or _child_text(field_el, "field") or _text(field_el)
        if name and not any(p["name"] == name for p in params):
            params.append({"name": name, "type": _child_text(field_el, "type")})

    rowlimit_raw = _child_text(step_el, "rowlimit") or _child_text(step_el, "rowLimit") or "0"
    try:
        row_limit = int(rowlimit_raw or "0")
    except ValueError:
        row_limit = 0

    return {
        "connection": _child_text(step_el, "connection"),
        "sql": _child_text(step_el, "sql") or _child_text(step_el, "lookup"),
        "outer_join": _bool_from_yn(
            _child_text(step_el, "outer_join")
            or _child_text(step_el, "outerjoin")
            or _child_text(step_el, "outerJoin")
        ),
        "row_limit": row_limit,
        "replace_vars": _bool_from_yn(
            _child_text(step_el, "replace_vars") or _child_text(step_el, "replaceVariables")
        ),
        "parameters": params,
    }


def parse_dynamic_sql_row_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Dynamic SQL Row template / SQL-field configuration."""
    return {
        "connection": _child_text(step_el, "connection"),
        "sql": _child_text(step_el, "sql"),
        "sql_field": (
            _child_text(step_el, "sql_fieldname")
            or _child_text(step_el, "sqlFieldName")
            or _child_text(step_el, "sqlfieldname")
            or _child_text(step_el, "sql_field")
        ),
        "outer_join": _bool_from_yn(
            _child_text(step_el, "outer_join") or _child_text(step_el, "outerJoin")
        ),
        "replace_vars": _bool_from_yn(
            _child_text(step_el, "replace_vars") or _child_text(step_el, "replaceVars")
        ),
        "query_only_on_change": _bool_from_yn(
            _child_text(step_el, "query_only_on_change")
            or _child_text(step_el, "queryOnlyOnChange")
        ),
        "row_limit": _child_text(step_el, "rowlimit") or _child_text(step_el, "rowLimit") or "0",
    }


def parse_file_exists_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse File Exists path and result field configuration."""
    return {
        "filename": (
            _child_text(step_el, "filename")
            or _child_text(step_el, "fileName")
            or _child_text(step_el, "file")
        ),
        "filename_field": (
            _child_text(step_el, "filenamefield")
            or _child_text(step_el, "filenameField")
            or _child_text(step_el, "fileNameField")
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or _child_text(step_el, "result")
            or "file_exists"
        ),
        "include_file_type": _bool_from_yn(
            _child_text(step_el, "includefiletype") or _child_text(step_el, "includeFileType")
        ),
        "file_type_field": (
            _child_text(step_el, "filetypefieldname") or _child_text(step_el, "fileTypeFieldName")
        ),
        "add_filename_result": _bool_from_yn(
            _child_text(step_el, "addresultfilenames") or _child_text(step_el, "addResultFilenames")
        ),
    }


def parse_table_exists_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Table Exists connection/schema/table and result field."""
    return {
        "connection": _child_text(step_el, "connection"),
        "schema": (
            _child_text(step_el, "schemaname")
            or _child_text(step_el, "schemaName")
            or _child_text(step_el, "schema")
        ),
        "schema_field": (
            _child_text(step_el, "schemanamefield")
            or _child_text(step_el, "schemaNameField")
        ),
        "table": (
            _child_text(step_el, "tablename")
            or _child_text(step_el, "tableName")
            or _child_text(step_el, "table")
        ),
        "table_field": (
            _child_text(step_el, "tablenamefield")
            or _child_text(step_el, "tableNameField")
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "table_exists"
        ),
    }


def parse_column_exists_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Column Exists table/column metadata and result field."""
    return {
        "connection": _child_text(step_el, "connection"),
        "schema": (
            _child_text(step_el, "schemaname")
            or _child_text(step_el, "schemaName")
            or _child_text(step_el, "schema")
        ),
        "schema_field": (
            _child_text(step_el, "schemanamefield")
            or _child_text(step_el, "schemaNameField")
        ),
        "table": (
            _child_text(step_el, "tablename")
            or _child_text(step_el, "tableName")
            or _child_text(step_el, "table")
        ),
        "table_field": (
            _child_text(step_el, "tablenamefield")
            or _child_text(step_el, "tableNameField")
        ),
        "column": (
            _child_text(step_el, "columnname")
            or _child_text(step_el, "columnName")
            or _child_text(step_el, "column")
        ),
        "column_field": (
            _child_text(step_el, "columnnamefield")
            or _child_text(step_el, "columnNameField")
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "column_exists"
        ),
    }


def parse_check_file_locked_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Check if File is Locked field configuration."""
    return {
        "filename": _child_text(step_el, "filename") or _child_text(step_el, "file"),
        "filename_field": (
            _child_text(step_el, "filenamefield")
            or _child_text(step_el, "filenameField")
            or _child_text(step_el, "fileNameField")
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "file_locked"
        ),
        "add_filename_result": _bool_from_yn(
            _child_text(step_el, "addresultfilenames") or _child_text(step_el, "addResultFilenames")
        ),
    }


def parse_webservice_available_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Check if Webservice is Available URL / timeout / auth metadata."""
    connect_timeout = (
        _child_text(step_el, "connectTimeOut")
        or _child_text(step_el, "connectTimeout")
        or _child_text(step_el, "connectionTimeout")
        or "0"
    )
    read_timeout = (
        _child_text(step_el, "readTimeOut")
        or _child_text(step_el, "readTimeout")
        or "0"
    )
    return {
        "url": _child_text(step_el, "url") or _child_text(step_el, "URL"),
        "url_in_field": _bool_from_yn(
            _child_text(step_el, "urlInField") or _child_text(step_el, "urlInfield")
        ),
        "url_field": (
            _child_text(step_el, "urlField")
            or _child_text(step_el, "urlfield")
            or _child_text(step_el, "URLField")
        ),
        "connect_timeout": connect_timeout,
        "read_timeout": read_timeout,
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "webservice_available"
        ),
        "http_login": _child_text(step_el, "httpLogin") or _child_text(step_el, "username"),
        "http_password": _child_text(step_el, "httpPassword") or _child_text(step_el, "password"),
        "proxy_host": _child_text(step_el, "proxyHost"),
        "proxy_port": _child_text(step_el, "proxyPort"),
    }


def parse_http_client_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse HTTP Client URL, headers, auth, timeouts, and query parameters."""
    args = []
    for parent_tag in ("lookup", "argument", "parameters"):
        parent = step_el.find(parent_tag) if parent_tag != "argument" else None
        search_root = parent if parent is not None else step_el
        for arg_el in search_root.findall("arg") + search_root.findall("argument"):
            field = _child_text(arg_el, "argumentField") or _child_text(arg_el, "field")
            param = _child_text(arg_el, "argumentParameter") or _child_text(arg_el, "parameter") or _child_text(arg_el, "name")
            if field or param:
                args.append({"field": field, "parameter": param})
        if parent is not None:
            break
    # Also scan top-level argument pairs
    if not args:
        for arg_el in step_el.findall("argument"):
            field = _child_text(arg_el, "argumentField") or _child_text(arg_el, "field")
            param = _child_text(arg_el, "argumentParameter") or _child_text(arg_el, "parameter")
            if field or param:
                args.append({"field": field, "parameter": param})

    headers = []
    header_parent = step_el.find("header") or step_el.find("headers") or step_el
    for hdr in header_parent.findall("header") + step_el.findall("header"):
        field = _child_text(hdr, "headerField") or _child_text(hdr, "field")
        param = _child_text(hdr, "headerParameter") or _child_text(hdr, "parameter") or _child_text(hdr, "name")
        if field or param:
            headers.append({"field": field, "parameter": param, "value": _child_text(hdr, "value")})

    return {
        "url": _child_text(step_el, "url") or _child_text(step_el, "URL"),
        "url_in_field": _bool_from_yn(_child_text(step_el, "urlInField")),
        "url_field": _child_text(step_el, "urlField") or _child_text(step_el, "urlfield"),
        "encoding": _child_text(step_el, "encoding") or "UTF-8",
        "http_login": _child_text(step_el, "httpLogin") or _child_text(step_el, "username"),
        "http_password": _child_text(step_el, "httpPassword") or _child_text(step_el, "password"),
        "proxy_host": _child_text(step_el, "proxyHost"),
        "proxy_port": _child_text(step_el, "proxyPort"),
        "proxy_username": _child_text(step_el, "proxyUsername"),
        "proxy_password": _child_text(step_el, "proxyPassword"),
        "connection_timeout": (
            _child_text(step_el, "connectionTimeout")
            or _child_text(step_el, "socketTimeout")
            or "10000"
        ),
        "result_field": (
            _child_text(step_el, "fieldName")
            or _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "result")
            or "result"
        ),
        "response_code_field": (
            _child_text(step_el, "resultcodefieldname")
            or _child_text(step_el, "responseStatusCode")
        ),
        "response_time_field": (
            _child_text(step_el, "responsetimefieldname")
            or _child_text(step_el, "responseTimeField")
        ),
        "arguments": args,
        "headers": headers,
    }


def parse_http_post_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse HTTP Post payload, headers, auth, and result fields."""
    base = parse_http_client_config(step_el)
    base.update({
        "request_entity": (
            _child_text(step_el, "requestEntity")
            or _child_text(step_el, "requestentity")
            or _child_text(step_el, "entityField")
        ),
        "content_type": (
            _child_text(step_el, "contentType")
            or _child_text(step_el, "content_type")
            or "application/x-www-form-urlencoded"
        ),
        "post_a_file": _bool_from_yn(
            _child_text(step_el, "postafile") or _child_text(step_el, "postAFile")
        ),
        "query_parameters": base.get("arguments") or [],
    })
    # Query parameter list often uses <query> children for HTTPPOST
    query_params: list[dict[str, str]] = []
    for q_el in step_el.findall("query") + list((step_el.find("querys") or step_el).findall("query")):
        field = _child_text(q_el, "parameterField") or _child_text(q_el, "field")
        param = _child_text(q_el, "parameterName") or _child_text(q_el, "name")
        if field or param:
            query_params.append({"field": field, "parameter": param})
    if query_params:
        base["query_parameters"] = query_params
    return base


def parse_rest_client_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse REST Client endpoint, method, headers, body, and response mapping."""
    headers = []
    header_root = step_el.find("headers") or step_el.find("header") or step_el
    for hdr in list(header_root):
        if hdr.tag not in ("header", "field"):
            continue
        name = _child_text(hdr, "name") or _child_text(hdr, "header") or hdr.get("name", "")
        field = _child_text(hdr, "field") or _child_text(hdr, "value")
        if name or field:
            headers.append({"name": name, "field": field, "value": _child_text(hdr, "value")})

    params = []
    param_root = step_el.find("parameters") or step_el.find("parameter") or step_el
    for p in list(param_root):
        if p.tag not in ("parameter", "field", "query"):
            continue
        name = _child_text(p, "name") or _child_text(p, "parameter")
        field = _child_text(p, "field") or _child_text(p, "value")
        if name or field:
            params.append({"name": name, "field": field})

    matrix = []
    matrix_root = step_el.find("matrixParameters") or step_el.find("matrixparameters")
    if matrix_root is not None:
        for m in list(matrix_root):
            name = _child_text(m, "name") or _child_text(m, "parameter")
            field = _child_text(m, "field")
            if name or field:
                matrix.append({"name": name, "field": field})

    return {
        "url": _child_text(step_el, "url") or _child_text(step_el, "restUrl"),
        "url_in_field": _bool_from_yn(_child_text(step_el, "urlInField")),
        "url_field": _child_text(step_el, "urlField"),
        "method": (
            _child_text(step_el, "method")
            or _child_text(step_el, "httpMethod")
            or "GET"
        ).upper(),
        "method_in_field": _bool_from_yn(_child_text(step_el, "dynamicMethod")),
        "method_field": _child_text(step_el, "methodFieldName") or _child_text(step_el, "methodField"),
        "body_field": (
            _child_text(step_el, "bodyField")
            or _child_text(step_el, "bodyfield")
            or _child_text(step_el, "httpEntity")
        ),
        "application_type": (
            _child_text(step_el, "applicationType")
            or _child_text(step_el, "contentType")
            or "TEXT PLAIN"
        ),
        "http_login": _child_text(step_el, "httpLogin") or _child_text(step_el, "username"),
        "http_password": _child_text(step_el, "httpPassword") or _child_text(step_el, "password"),
        "preemptive": _bool_from_yn(_child_text(step_el, "preemptive")),
        "proxy_host": _child_text(step_el, "proxyHost"),
        "proxy_port": _child_text(step_el, "proxyPort"),
        "connection_timeout": _child_text(step_el, "connectionTimeout") or "10000",
        "read_timeout": _child_text(step_el, "readTimeout") or "10000",
        "result_field": (
            _child_text(step_el, "resultfield")
            or _child_text(step_el, "fieldName")
            or _child_text(step_el, "result")
            or "result"
        ),
        "response_code_field": (
            _child_text(step_el, "resultcodefield")
            or _child_text(step_el, "responseStatusCode")
        ),
        "response_header_field": (
            _child_text(step_el, "responseHeaderField")
            or _child_text(step_el, "headersResponseName")
        ),
        "headers": headers,
        "parameters": params,
        "matrix_parameters": matrix,
    }


def parse_web_services_lookup_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Web Services Lookup WSDL / SOAP action / parameter mappings."""
    params = _parse_lookup_argument_list(step_el, "fields", "parameters", "lookup")
    # Response mappings
    outputs: list[dict[str, str]] = []
    for parent_tag in ("fields", "outFields", "output"):
        parent = step_el.find(parent_tag)
        if parent is None:
            continue
        for field_el in parent.findall("field"):
            name = _child_text(field_el, "name") or _child_text(field_el, "wsName")
            rename = _child_text(field_el, "rename") or _child_text(field_el, "outName")
            if name or rename:
                outputs.append({
                    "name": name,
                    "rename": rename or name,
                    "type": _child_text(field_el, "type"),
                    "ws_name": _child_text(field_el, "wsName") or name,
                })

    return {
        "url": _child_text(step_el, "url") or _child_text(step_el, "wsURL"),
        "wsdl": (
            _child_text(step_el, "wsdlUrl")
            or _child_text(step_el, "wsdl")
            or _child_text(step_el, "wsdlURL")
        ),
        "operation": (
            _child_text(step_el, "operationName")
            or _child_text(step_el, "operation")
            or _child_text(step_el, "callOperation")
        ),
        "soap_action": (
            _child_text(step_el, "soapAction")
            or _child_text(step_el, "soap_action")
            or _child_text(step_el, "action")
        ),
        "http_login": _child_text(step_el, "httpLogin") or _child_text(step_el, "username"),
        "http_password": _child_text(step_el, "httpPassword") or _child_text(step_el, "password"),
        "proxy_host": _child_text(step_el, "proxyHost"),
        "proxy_port": _child_text(step_el, "proxyPort"),
        "call_step": _child_text(step_el, "callStep"),
        "passing_input_data": _bool_from_yn(
            _child_text(step_el, "passingInputData") or _child_text(step_el, "passInputData")
        ),
        "compatible": _bool_from_yn(_child_text(step_el, "compatible"), True),
        "repeating_element": _child_text(step_el, "repeatingElement"),
        "reply_as_string": _bool_from_yn(_child_text(step_el, "replyAsString")),
        "parameters": params,
        "output_fields": outputs,
    }


def parse_fuzzy_match_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Fuzzy Match algorithm, fields, and similarity thresholds."""
    try:
        minimal = float(
            _child_text(step_el, "minimalValue")
            or _child_text(step_el, "mininimalValue")
            or _child_text(step_el, "minimal")
            or "0"
        )
    except ValueError:
        minimal = 0.0
    try:
        maximal = float(
            _child_text(step_el, "maximalValue")
            or _child_text(step_el, "maximal")
            or "1"
        )
    except ValueError:
        maximal = 1.0

    return {
        "algorithm": (
            _child_text(step_el, "algorithm")
            or _child_text(step_el, "matcher")
            or "levenshtein"
        ),
        "lookup_field": (
            _child_text(step_el, "lookupfield")
            or _child_text(step_el, "lookupField")
            or _child_text(step_el, "lookupvalue")
        ),
        "main_stream_field": (
            _child_text(step_el, "mainstreamfield")
            or _child_text(step_el, "mainStreamField")
            or _child_text(step_el, "matchfield")
        ),
        "output_match_field": (
            _child_text(step_el, "outputmatchvalue")
            or _child_text(step_el, "outputMatchField")
            or _child_text(step_el, "matchfield")
            or "match"
        ),
        "output_value_field": (
            _child_text(step_el, "outputvalue")
            or _child_text(step_el, "outputValueField")
            or _child_text(step_el, "valuename")
        ),
        "minimal_value": minimal,
        "maximal_value": maximal,
        "case_sensitive": _bool_from_yn(
            _child_text(step_el, "caseSensitive") or _child_text(step_el, "casesensitive")
        ),
        "get_closer_value": _bool_from_yn(
            _child_text(step_el, "closervalue") or _child_text(step_el, "getCloserValue"),
            True,
        ),
        "separator": _child_text(step_el, "separator") or ",",
    }


def _parse_streaming_option_pairs(step_el: ET.Element) -> dict[str, str]:
    """Parse Kafka/MQTT/JMS Options tab key-value pairs from common Pentaho shapes."""
    options: dict[str, str] = {}
    for parent_tag in ("Options", "options", "CONFIG", "config", "properties"):
        parent = step_el.find(parent_tag)
        if parent is None:
            continue
        for opt in list(parent):
            key = (
                _child_text(opt, "property")
                or _child_text(opt, "name")
                or _child_text(opt, "key")
                or opt.get("name", "")
            )
            val = (
                _child_text(opt, "value")
                or _child_text(opt, "VALUE")
                or (_text(opt) if len(opt) == 0 else "")
            )
            if key:
                options[str(key).strip()] = str(val).strip()
    return options


def _parse_streaming_topics(step_el: ET.Element) -> list[str]:
    """Collect topic names from Topic/Topics/topic/TOPIC XML variants."""
    topics: list[str] = []
    for tag in ("topic", "TOPIC", "Topic"):
        direct = _child_text(step_el, tag)
        if direct:
            topics.extend(t.strip() for t in direct.replace(";", ",").split(",") if t.strip())
    for parent_tag in ("Topic", "Topics", "topics", "TOPICS"):
        parent = step_el.find(parent_tag)
        if parent is None:
            continue
        nested = _child_text(parent, "topic") or _child_text(parent, "name") or _text(parent)
        if nested and len(parent) == 0:
            topics.extend(t.strip() for t in nested.replace(";", ",").split(",") if t.strip())
        for child in parent:
            name = (
                _child_text(child, "topic")
                or _child_text(child, "name")
                or _text(child)
                or child.get("name", "")
            )
            if name and name.strip():
                topics.append(name.strip())
    # De-dupe, preserve order
    seen: set[str] = set()
    ordered: list[str] = []
    for t in topics:
        if t not in seen:
            seen.add(t)
            ordered.append(t)
    return ordered


def _parse_streaming_fields(step_el: ET.Element) -> list[dict[str, str]]:
    """Parse stream field mappings (input name → output name / type)."""
    fields: list[dict[str, str]] = []
    for fields_el in step_el.findall("fields") + step_el.findall("FIELDS") + step_el.findall("Fields"):
        for field_el in list(fields_el):
            name = (
                _child_text(field_el, "name")
                or _child_text(field_el, "OutputName")
                or _child_text(field_el, "outputName")
                or _child_text(field_el, "output_name")
            )
            input_name = (
                _child_text(field_el, "InputName")
                or _child_text(field_el, "inputName")
                or _child_text(field_el, "input_name")
                or _child_text(field_el, "path")
            )
            type_name = (
                _child_text(field_el, "type")
                or _child_text(field_el, "Type")
                or "String"
            )
            if name or input_name:
                fields.append({
                    "name": name or input_name,
                    "input_name": input_name or name,
                    "type": type_name or "String",
                })
    if fields:
        return fields
    # Flat FIELD / field children under step
    for field_el in step_el.findall("field") + step_el.findall("FIELD"):
        name = _child_text(field_el, "name") or _child_text(field_el, "OutputName")
        if name:
            fields.append({
                "name": name,
                "input_name": _child_text(field_el, "InputName") or name,
                "type": _child_text(field_el, "type") or _child_text(field_el, "Type") or "String",
            })
    return fields


def parse_records_from_stream_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get Records from Stream field metadata (child streaming entry)."""
    fields = _parse_streaming_fields(step_el)
    source_step = (
        _child_text(step_el, "source_step")
        or _child_text(step_el, "sourceStep")
        or _child_text(step_el, "step_from")
        or _child_text(step_el, "from_step")
    )
    return {
        "source_step": source_step,
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
        "message_field": _child_text(step_el, "message_field") or _child_text(step_el, "messageField"),
        "key_field": _child_text(step_el, "key_field") or _child_text(step_el, "keyField"),
    }


def parse_kafka_consumer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Kafka Consumer bootstrap, topics, group, offsets, and security options."""
    topics = _parse_streaming_topics(step_el)
    options = _parse_streaming_option_pairs(step_el)
    bootstrap = (
        _child_text(step_el, "DIRECT_BOOTSTRAP_SERVERS")
        or _child_text(step_el, "bootstrap_servers")
        or _child_text(step_el, "bootstrapServers")
        or _child_text(step_el, "BOOTSTRAP_SERVERS")
        or options.get("bootstrap.servers", "")
    )
    group = (
        _child_text(step_el, "CONSUMER_GROUP")
        or _child_text(step_el, "consumer_group")
        or _child_text(step_el, "consumerGroup")
        or _child_text(step_el, "groupId")
        or options.get("group.id", "")
    )
    starting = (
        _child_text(step_el, "startingOffsets")
        or _child_text(step_el, "startingOffset")
        or _child_text(step_el, "auto.offset.reset")
        or options.get("auto.offset.reset", "")
        or options.get("startingOffsets", "")
    )
    offset_el = step_el.find("Offset") or step_el.find("offset") or step_el.find("OFFSET")
    if offset_el is not None and not starting:
        starting = (
            _child_text(offset_el, "startingOffset")
            or _child_text(offset_el, "startingOffsets")
            or _text(offset_el)
        )
    # Map Pentaho earliest/latest → Spark startingOffsets
    starting_norm = (starting or "").strip().lower()
    if starting_norm in ("earliest", "latest"):
        spark_starting = starting_norm
    elif starting_norm in ("beginning", "from_beginning"):
        spark_starting = "earliest"
    elif starting_norm in ("end", "from_end", "latest_offset"):
        spark_starting = "latest"
    elif starting.strip():
        spark_starting = starting.strip()
    else:
        spark_starting = "latest"

    security: dict[str, str] = {}
    for key, val in options.items():
        low = key.lower()
        if any(
            tok in low
            for tok in (
                "security", "sasl", "ssl", "truststore", "keystore",
                "jaas", "protocol", "mechanism",
            )
        ):
            security[key] = val

    fields = _parse_streaming_fields(step_el)
    return {
        "bootstrap_servers": bootstrap,
        "topics": topics,
        "topic": ",".join(topics),
        "consumer_group": group,
        "starting_offsets": spark_starting,
        "starting_offsets_raw": starting,
        "connection_type": (
            _child_text(step_el, "CONNECTION_TYPE")
            or _child_text(step_el, "connection_type")
            or _child_text(step_el, "DIRECT")
        ),
        "cluster_name": _child_text(step_el, "CLUSTER_NAME") or _child_text(step_el, "cluster_name"),
        "batch_size": _child_text(step_el, "BATCH_SIZE") or _child_text(step_el, "batchSize"),
        "batch_duration": _child_text(step_el, "BATCH_DURATION") or _child_text(step_el, "batchDuration"),
        "auto_commit": _child_text(step_el, "AUTO_COMMIT") or options.get("enable.auto.commit", ""),
        "sub_transformation": (
            _child_text(step_el, "TRANSFORMATION")
            or _child_text(step_el, "transformation")
            or _child_text(step_el, "trans_path")
        ),
        "checkpoint_location": (
            _child_text(step_el, "checkpointLocation")
            or _child_text(step_el, "checkpoint_location")
            or options.get("checkpointLocation", "")
        ),
        "options": options,
        "security": security,
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
        "key_field": _child_text(step_el, "keyField") or _child_text(step_el, "key_field"),
        "message_field": _child_text(step_el, "messageField") or _child_text(step_el, "message_field"),
    }


def parse_kafka_producer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Kafka Producer topic, key/value fields, and serialization options."""
    topics = _parse_streaming_topics(step_el)
    options = _parse_streaming_option_pairs(step_el)
    bootstrap = (
        _child_text(step_el, "DIRECT_BOOTSTRAP_SERVERS")
        or _child_text(step_el, "bootstrap_servers")
        or _child_text(step_el, "bootstrapServers")
        or _child_text(step_el, "BOOTSTRAP_SERVERS")
        or options.get("bootstrap.servers", "")
    )
    return {
        "bootstrap_servers": bootstrap,
        "topics": topics,
        "topic": topics[0] if topics else (_child_text(step_el, "topic") or ""),
        "key_field": (
            _child_text(step_el, "keyField")
            or _child_text(step_el, "key_field")
            or _child_text(step_el, "KEY_FIELD")
            or options.get("key.field", "")
        ),
        "message_field": (
            _child_text(step_el, "messageField")
            or _child_text(step_el, "message_field")
            or _child_text(step_el, "valueField")
            or _child_text(step_el, "value_field")
            or _child_text(step_el, "MESSAGE_FIELD")
            or options.get("message.field", "")
        ),
        "connection_type": (
            _child_text(step_el, "CONNECTION_TYPE")
            or _child_text(step_el, "connection_type")
        ),
        "cluster_name": _child_text(step_el, "CLUSTER_NAME") or _child_text(step_el, "cluster_name"),
        "checkpoint_location": (
            _child_text(step_el, "checkpointLocation")
            or _child_text(step_el, "checkpoint_location")
            or options.get("checkpointLocation", "")
        ),
        "options": options,
        "security": {
            k: v
            for k, v in options.items()
            if any(
                tok in k.lower()
                for tok in ("security", "sasl", "ssl", "truststore", "keystore", "jaas", "protocol")
            )
        },
        "fields": _parse_streaming_fields(step_el),
        "client_id": _child_text(step_el, "clientId") or options.get("client.id", ""),
        "acks": options.get("acks", ""),
        "compression": options.get("compression.type", "") or _child_text(step_el, "compression"),
    }


def parse_jms_consumer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse JMS Consumer destination, connection factory, and authentication."""
    options = _parse_streaming_option_pairs(step_el)
    destination = (
        _child_text(step_el, "destination")
        or _child_text(step_el, "Destination")
        or _child_text(step_el, "queue")
        or _child_text(step_el, "topic")
        or _child_text(step_el, "destinationName")
    )
    dest_type = (
        _child_text(step_el, "destination_type")
        or _child_text(step_el, "destinationType")
        or _child_text(step_el, "DESTINATION_TYPE")
        or ("topic" if "topic" in (_child_text(step_el, "topic") or "").lower() else "queue")
    )
    return {
        "destination": destination,
        "destination_type": dest_type,
        "connection_factory": (
            _child_text(step_el, "connectionFactory")
            or _child_text(step_el, "connection_factory")
            or _child_text(step_el, "CONNECTION_FACTORY")
            or _child_text(step_el, "jndiName")
        ),
        "url": (
            _child_text(step_el, "url")
            or _child_text(step_el, "brokerUrl")
            or _child_text(step_el, "broker_url")
            or _child_text(step_el, "connection_url")
        ),
        "username": _child_text(step_el, "username") or _child_text(step_el, "user"),
        "password": _child_text(step_el, "password"),
        "receive_timeout": _child_text(step_el, "receiveTimeout") or _child_text(step_el, "timeout"),
        "message_selector": _child_text(step_el, "messageSelector") or _child_text(step_el, "selector"),
        "client_id": _child_text(step_el, "clientId") or _child_text(step_el, "client_id"),
        "durable": _child_text(step_el, "durable") or _child_text(step_el, "durableSubscription"),
        "transacted": _child_text(step_el, "transacted"),
        "acknowledge_mode": _child_text(step_el, "acknowledgeMode") or _child_text(step_el, "ackMode"),
        "ssl": _child_text(step_el, "ssl") or _child_text(step_el, "useSsl") or _child_text(step_el, "useSSL"),
        "options": options,
        "fields": _parse_streaming_fields(step_el),
        "message_field": _child_text(step_el, "messageField") or _child_text(step_el, "message_field"),
        "sub_transformation": (
            _child_text(step_el, "TRANSFORMATION")
            or _child_text(step_el, "transformation")
        ),
    }


def parse_jms_producer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse JMS Producer destination, message fields, and delivery options."""
    options = _parse_streaming_option_pairs(step_el)
    return {
        "destination": (
            _child_text(step_el, "destination")
            or _child_text(step_el, "queue")
            or _child_text(step_el, "topic")
            or _child_text(step_el, "destinationName")
        ),
        "destination_type": (
            _child_text(step_el, "destination_type")
            or _child_text(step_el, "destinationType")
            or "queue"
        ),
        "connection_factory": (
            _child_text(step_el, "connectionFactory")
            or _child_text(step_el, "connection_factory")
            or _child_text(step_el, "jndiName")
        ),
        "url": (
            _child_text(step_el, "url")
            or _child_text(step_el, "brokerUrl")
            or _child_text(step_el, "broker_url")
        ),
        "username": _child_text(step_el, "username") or _child_text(step_el, "user"),
        "password": _child_text(step_el, "password"),
        "message_field": (
            _child_text(step_el, "messageField")
            or _child_text(step_el, "message_field")
            or _child_text(step_el, "field")
        ),
        "delivery_mode": _child_text(step_el, "deliveryMode") or _child_text(step_el, "delivery_mode"),
        "priority": _child_text(step_el, "priority"),
        "time_to_live": _child_text(step_el, "timeToLive") or _child_text(step_el, "ttl"),
        "persistent": _child_text(step_el, "persistent"),
        "ssl": _child_text(step_el, "ssl") or _child_text(step_el, "useSsl"),
        "options": options,
        "fields": _parse_streaming_fields(step_el),
    }


def parse_mqtt_consumer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse MQTT Consumer broker, topics, QoS, and authentication."""
    topics = _parse_streaming_topics(step_el)
    options = _parse_streaming_option_pairs(step_el)
    qos_raw = (
        _child_text(step_el, "qos")
        or _child_text(step_el, "QoS")
        or _child_text(step_el, "qualityOfService")
        or options.get("qos", "0")
    )
    return {
        "broker_url": (
            _child_text(step_el, "brokerUrl")
            or _child_text(step_el, "broker_url")
            or _child_text(step_el, "url")
            or _child_text(step_el, "mqttServer")
            or options.get("brokerUrl", "")
        ),
        "topics": topics,
        "topic": ",".join(topics) if topics else _child_text(step_el, "topic"),
        "qos": qos_raw,
        "username": _child_text(step_el, "username") or _child_text(step_el, "user"),
        "password": _child_text(step_el, "password"),
        "client_id": _child_text(step_el, "clientId") or _child_text(step_el, "client_id"),
        "clean_session": _child_text(step_el, "cleanSession") or _child_text(step_el, "clean_session"),
        "keep_alive": _child_text(step_el, "keepAliveInterval") or _child_text(step_el, "keep_alive"),
        "ssl": _child_text(step_el, "ssl") or _child_text(step_el, "useSsl"),
        "checkpoint_location": (
            _child_text(step_el, "checkpointLocation")
            or _child_text(step_el, "checkpoint_location")
        ),
        "options": options,
        "fields": _parse_streaming_fields(step_el),
        "message_field": _child_text(step_el, "messageField") or _child_text(step_el, "message_field"),
        "sub_transformation": (
            _child_text(step_el, "TRANSFORMATION")
            or _child_text(step_el, "transformation")
        ),
    }


def parse_mqtt_producer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse MQTT Producer broker, topic, QoS, and payload field mapping."""
    topics = _parse_streaming_topics(step_el)
    options = _parse_streaming_option_pairs(step_el)
    return {
        "broker_url": (
            _child_text(step_el, "brokerUrl")
            or _child_text(step_el, "broker_url")
            or _child_text(step_el, "url")
            or _child_text(step_el, "mqttServer")
        ),
        "topics": topics,
        "topic": topics[0] if topics else _child_text(step_el, "topic"),
        "qos": (
            _child_text(step_el, "qos")
            or _child_text(step_el, "QoS")
            or options.get("qos", "0")
        ),
        "username": _child_text(step_el, "username") or _child_text(step_el, "user"),
        "password": _child_text(step_el, "password"),
        "client_id": _child_text(step_el, "clientId") or _child_text(step_el, "client_id"),
        "message_field": (
            _child_text(step_el, "messageField")
            or _child_text(step_el, "message_field")
            or _child_text(step_el, "payloadField")
            or _child_text(step_el, "payload_field")
        ),
        "retained": _child_text(step_el, "retained") or _child_text(step_el, "retain"),
        "ssl": _child_text(step_el, "ssl") or _child_text(step_el, "useSsl"),
        "options": options,
        "fields": _parse_streaming_fields(step_el),
    }


# ---------------------------------------------------------------------------
# Inline steps (Injector / Socket Reader / Socket Writer)
# ---------------------------------------------------------------------------


_SOCKET_SCALAR_TAGS = frozenset({
    "hostname", "host", "host_name", "server",
    "port",
    "buffer_size", "buffersize", "bufferSize",
    "flush_interval", "flushinterval", "flushInterval",
    "compressed", "compress",
    "protocol", "encoding", "charset",
    "delimiter", "separator", "enclosure",
    "timeout", "socket_timeout", "socketTimeout", "connect_timeout",
    "output_format", "outputFormat", "format",
})


def _parse_socket_residual_options(step_el: ET.Element) -> dict[str, str]:
    """Capture non-standard socket tags so no Pentaho property is dropped."""
    skip = frozenset({
        "name", "type", "description", "distribute", "custom_distribution",
        "copies", "partitioning", "remotesteps", "GUI", "draw", "attributes",
        "fields", "field",
    })
    options: dict[str, str] = {}
    for child in step_el:
        if child.tag in skip or len(child) > 0:
            continue
        text = _text(child)
        if text == "":
            continue
        if child.tag not in _SOCKET_SCALAR_TAGS:
            options[child.tag] = text
    return options


_INJECTOR_FIELD_KNOWN_TAGS = frozenset({
    "name", "type", "Type", "length", "precision",
    "value", "default", "string", "nullif",
    "set_empty_string", "format", "null", "isnull",
})


def parse_injector_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Injector field metadata (name/type/length/precision) and optional values.

    Native Injector XML only defines schema; row values are injected at runtime via
    the Kettle API. Optional ``value``/``default`` tags are preserved when present.
    Residual field/step tags are retained under ``field_options`` / ``options``.
    """
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            type_name = (
                _child_text(field_el, "type")
                or _child_text(field_el, "Type")
                or "String"
            )
            # Resolve numeric type codes when Spoon stores ValueMeta ids
            if type_name.isdigit():
                type_name = _PENTAHO_TYPE_BY_CODE.get(type_name, "String") or "String"
            value = (
                _child_text(field_el, "value")
                or _child_text(field_el, "default")
                or _child_text(field_el, "string")
                or _child_text(field_el, "nullif")
            )
            length = _child_text(field_el, "length")
            precision = _child_text(field_el, "precision")
            set_empty = _bool_from_yn(_child_text(field_el, "set_empty_string", "N"))
            # Invalid / sentinel lengths from Const.toInt(..., -2)
            if length in ("-2", "-1"):
                length = ""
            if precision in ("-2", "-1"):
                precision = ""
            field_options: dict[str, str] = {}
            for child in field_el:
                if child.tag in _INJECTOR_FIELD_KNOWN_TAGS or len(child) > 0:
                    continue
                text = _text(child)
                if text != "":
                    field_options[child.tag] = text
            fields.append({
                "name": name,
                "type": type_name,
                "type_name": type_name,
                "length": length,
                "precision": precision,
                "value": value,
                "set_empty_string": set_empty,
                "format": _child_text(field_el, "format"),
                "null": _child_text(field_el, "null") or _child_text(field_el, "isnull"),
                "field_options": field_options,
            })

    # Multi-row data grids occasionally appear in custom Injector variants
    columns, grid_rows = parse_data_grid_rows(step_el)
    has_values = any(
        (f.get("value") not in (None, "") or f.get("set_empty_string"))
        for f in fields
    ) or bool(grid_rows)

    # Step-level residual scalars (reuse socket residual collector pattern)
    step_options: dict[str, str] = {}
    skip_step = frozenset({
        "name", "type", "description", "distribute", "custom_distribution",
        "copies", "partitioning", "remotesteps", "GUI", "draw", "attributes",
        "fields", "field", "data",
    })
    for child in step_el:
        if child.tag in skip_step or len(child) > 0:
            continue
        text = _text(child)
        if text != "":
            step_options[child.tag] = text

    return {
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
        "columns": columns or [f["name"] for f in fields if f.get("name")],
        "rows": grid_rows,
        "has_row_values": has_values,
        "inject_at_runtime": not has_values,
        "options": step_options,
    }


def parse_socket_reader_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Socket Reader host/port/buffer/compression plus optional text-socket hints."""
    hostname = (
        _child_text(step_el, "hostname")
        or _child_text(step_el, "host")
        or _child_text(step_el, "host_name")
        or _child_text(step_el, "server")
    )
    port = _child_text(step_el, "port")
    buffer_size = (
        _child_text(step_el, "buffer_size")
        or _child_text(step_el, "buffersize")
        or _child_text(step_el, "bufferSize")
        or "3000"
    )
    compressed_raw = (
        _child_text(step_el, "compressed")
        or _child_text(step_el, "compress")
    )
    protocol = (
        _child_text(step_el, "protocol")
        or _child_text(step_el, "Protocol")
        or "kettle"  # native PDI uses proprietary Kettle row serialization
    )
    encoding = (
        _child_text(step_el, "encoding")
        or _child_text(step_el, "charset")
        or _child_text(step_el, "Encoding")
    )
    delimiter = (
        _child_text(step_el, "delimiter")
        or _child_text(step_el, "separator")
    )
    enclosure = _child_text(step_el, "enclosure")
    timeout = (
        _child_text(step_el, "timeout")
        or _child_text(step_el, "socket_timeout")
        or _child_text(step_el, "socketTimeout")
        or _child_text(step_el, "connect_timeout")
    )
    fields = _parse_streaming_fields(step_el)
    if not fields:
        # Fall back to Injector-style <fields><field> schema if present
        inj = parse_injector_config(step_el)
        fields = inj.get("fields") or []

    return {
        "hostname": hostname,
        "host": hostname,
        "port": port,
        "buffer_size": buffer_size,
        "compressed": _bool_from_yn(compressed_raw, default=True) if compressed_raw else True,
        "compressed_raw": compressed_raw,
        "protocol": protocol,
        "encoding": encoding,
        "delimiter": delimiter,
        "enclosure": enclosure,
        "timeout": timeout,
        "fields": fields,
        "output_columns": [f["name"] for f in fields if f.get("name")],
        "options": _parse_socket_residual_options(step_el),
    }


def parse_socket_writer_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Socket Writer listen port, buffering, flush, and optional format hints."""
    hostname = (
        _child_text(step_el, "hostname")
        or _child_text(step_el, "host")
        or _child_text(step_el, "host_name")
        or _child_text(step_el, "server")
    )
    port = _child_text(step_el, "port")
    buffer_size = (
        _child_text(step_el, "buffer_size")
        or _child_text(step_el, "buffersize")
        or _child_text(step_el, "bufferSize")
        or "2000"
    )
    flush_interval = (
        _child_text(step_el, "flush_interval")
        or _child_text(step_el, "flushInterval")
        or _child_text(step_el, "flushinterval")
        or "5000"
    )
    compressed_raw = (
        _child_text(step_el, "compressed")
        or _child_text(step_el, "compress")
    )
    encoding = (
        _child_text(step_el, "encoding")
        or _child_text(step_el, "charset")
        or _child_text(step_el, "Encoding")
    )
    output_format = (
        _child_text(step_el, "output_format")
        or _child_text(step_el, "outputFormat")
        or _child_text(step_el, "format")
    )
    delimiter = (
        _child_text(step_el, "delimiter")
        or _child_text(step_el, "separator")
    )
    fields = _parse_streaming_fields(step_el)

    return {
        "hostname": hostname,
        "host": hostname,
        "port": port,
        "buffer_size": buffer_size,
        "flush_interval": flush_interval,
        "compressed": _bool_from_yn(compressed_raw, default=True) if compressed_raw else True,
        "compressed_raw": compressed_raw,
        "encoding": encoding,
        "output_format": output_format,
        "delimiter": delimiter,
        "fields": fields,
        "options": _parse_socket_residual_options(step_el),
    }


# ---------------------------------------------------------------------------
# Experimental steps — SFTP Put (org.pentaho.di.trans.steps.sftpput.SFTPPutMeta)
# ---------------------------------------------------------------------------


def parse_sftp_put_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Experimental SFTP Put (and JobEntrySFTPPUT-compatible) XML.

    Primary tags from ``SFTPPutMeta``:
    servername, serverport, username, password, sourceFileFieldName,
    remoteDirectoryFieldName, remoteFilenameFieldName, inputIsStream,
    addFilenameResut, usekeyfilename, keyfilename, keyfilepass, compression,
    proxy*, createRemoteFolder, aftersftpput, destinationfolderFieldName,
    createdestinationfolder.

    Also accepts JobEntrySFTPPUT aliases (localdirectory, sftpdirectory, wildcard,
    successWhenNoFile) and optional transfer extensions (timeout, overwrite,
    append, binary/ascii) preserved when present. Secret values are never
    returned as plaintext — only presence flags and Databricks Secrets refs.
    """
    raw_password = _child_text(step_el, "password")
    raw_key_pass = (
        _child_text(step_el, "keyfilepass")
        or _child_text(step_el, "keyFilePass")
        or _child_text(step_el, "passphrase")
        or _child_text(step_el, "passPhrase")
    )
    raw_proxy_password = (
        _child_text(step_el, "proxyPassword") or _child_text(step_el, "proxypassword")
    )
    has_password = bool(raw_password and raw_password.strip())
    has_key_passphrase = bool(raw_key_pass and raw_key_pass.strip())
    has_proxy_password = bool(raw_proxy_password and raw_proxy_password.strip())

    use_private_key = _bool_from_yn(
        _child_text(step_el, "usekeyfilename")
        or _child_text(step_el, "usePrivateKey")
        or _child_text(step_el, "useprivatekey")
        or _child_text(step_el, "usekeyfile")
    )
    auth_method = "private_key" if use_private_key else ("password" if has_password else "unspecified")

    transfer_mode_raw = (
        _child_text(step_el, "transfermode")
        or _child_text(step_el, "transferMode")
        or _child_text(step_el, "binary")
        or _child_text(step_el, "ftpMode")
        or ""
    )
    transfer_mode = ""
    if transfer_mode_raw:
        low = transfer_mode_raw.strip().lower()
        if low in ("y", "yes", "true", "1", "binary"):
            transfer_mode = "binary"
        elif low in ("n", "no", "false", "0", "ascii"):
            transfer_mode = "ascii"
        else:
            transfer_mode = transfer_mode_raw.strip()

    overwrite_raw = (
        _child_text(step_el, "overwrite")
        or _child_text(step_el, "overwritefiles")
        or _child_text(step_el, "dontOverwrite")
        or _child_text(step_el, "dontoverwrite")
    )
    # Stock SFTP Put always overwrites; honour explicit tags when present.
    if overwrite_raw:
        if overwrite_raw.strip().lower() in ("dont", "false", "n", "no", "0"):
            overwrite = False
        else:
            overwrite = _bool_from_yn(overwrite_raw, default=True)
    else:
        overwrite = True

    append = _bool_from_yn(
        _child_text(step_el, "append") or _child_text(step_el, "appendfile")
    )

    after_action = (
        _child_text(step_el, "aftersftpput")
        or _child_text(step_el, "afterSFTPPut")
        or ("delete" if _bool_from_yn(_child_text(step_el, "remove")) else "")
        or "nothing"
    )

    extras = _element_to_dict(step_el) if step_el is not None else {}
    for secret_tag in (
        "password", "keyfilepass", "keyFilePass", "passphrase", "passPhrase",
        "proxyPassword", "proxypassword",
    ):
        if secret_tag in extras:
            extras[secret_tag] = "<redacted>"

    key_file = (
        _child_text(step_el, "keyfilename")
        or _child_text(step_el, "keyFileName")
        or _child_text(step_el, "privateKey")
        or _child_text(step_el, "private_key")
    )
    # Path reference only — never inline key material from XML
    private_key_ref = key_file
    if key_file and (
        "BEGIN" in key_file.upper()
        or "PRIVATE KEY" in key_file.upper()
        or "\n" in key_file
    ):
        private_key_ref = "<inline-key-redacted; use Databricks Secrets scope='sftp' key='private_key'>"

    return {
        "host": (
            _child_text(step_el, "servername")
            or _child_text(step_el, "serverName")
            or _child_text(step_el, "server")
            or _child_text(step_el, "host")
        ),
        "port": (
            _child_text(step_el, "serverport")
            or _child_text(step_el, "serverPort")
            or _child_text(step_el, "port")
            or "22"
        ),
        "username": (
            _child_text(step_el, "username")
            or _child_text(step_el, "userName")
            or _child_text(step_el, "user")
        ),
        "authentication_method": auth_method,
        "password_configured": has_password,
        "password_secret_ref": (
            "dbutils.secrets.get(scope='sftp', key='password')" if has_password else ""
        ),
        "use_private_key": use_private_key,
        "private_key_ref": private_key_ref,
        "key_file": key_file if private_key_ref == key_file else "",
        "passphrase_configured": has_key_passphrase,
        "passphrase_secret_ref": (
            "dbutils.secrets.get(scope='sftp', key='passphrase')"
            if has_key_passphrase
            else ""
        ),
        "private_key_secret_ref": (
            "dbutils.secrets.get(scope='sftp', key='private_key')"
            if use_private_key
            else ""
        ),
        "local_filename": (
            _child_text(step_el, "localfilename")
            or _child_text(step_el, "localFilename")
            or _child_text(step_el, "filename")
        ),
        "local_directory": (
            _child_text(step_el, "localdirectory")
            or _child_text(step_el, "localDirectory")
        ),
        "local_filename_field": (
            _child_text(step_el, "sourceFileFieldName")
            or _child_text(step_el, "sourcefilenamefield")
            or _child_text(step_el, "source_filename_field")
            or _child_text(step_el, "localFilenameField")
        ),
        "remote_filename": (
            _child_text(step_el, "remotefilename")
            or _child_text(step_el, "remoteFilename")
        ),
        "remote_filename_field": (
            _child_text(step_el, "remoteFilenameFieldName")
            or _child_text(step_el, "remotefilenamefield")
            or _child_text(step_el, "remote_filename_field")
        ),
        "remote_directory": (
            _child_text(step_el, "sftpdirectory")
            or _child_text(step_el, "sftpDirectory")
            or _child_text(step_el, "remotedirectory")
            or _child_text(step_el, "remoteDirectory")
        ),
        "remote_directory_field": (
            _child_text(step_el, "remoteDirectoryFieldName")
            or _child_text(step_el, "remotedirectoryfield")
            or _child_text(step_el, "remote_directory_field")
        ),
        "create_remote_directory": _bool_from_yn(
            _child_text(step_el, "createRemoteFolder")
            or _child_text(step_el, "createremotefolder")
            or _child_text(step_el, "createfolder")
        ),
        "overwrite": overwrite,
        "append": append,
        "transfer_mode": transfer_mode or "binary",
        "timeout": (
            _child_text(step_el, "timeout")
            or _child_text(step_el, "timeOut")
            or _child_text(step_el, "connectiontimeout")
            or "0"
        ),
        "proxy_type": (
            _child_text(step_el, "proxyType") or _child_text(step_el, "proxytype")
        ),
        "proxy_host": (
            _child_text(step_el, "proxyHost") or _child_text(step_el, "proxyhost")
        ),
        "proxy_port": (
            _child_text(step_el, "proxyPort") or _child_text(step_el, "proxyport")
        ),
        "proxy_username": (
            _child_text(step_el, "proxyUsername")
            or _child_text(step_el, "proxyusername")
        ),
        "proxy_password_configured": has_proxy_password,
        "proxy_password_secret_ref": (
            "dbutils.secrets.get(scope='sftp', key='proxy_password')"
            if has_proxy_password
            else ""
        ),
        "compression": (
            _child_text(step_el, "compression") or "none"
        ),
        "variable_substitution": True,
        "input_is_stream": _bool_from_yn(
            _child_text(step_el, "inputIsStream")
            or _child_text(step_el, "inputisstream")
        ),
        "add_filename_to_result": _bool_from_yn(
            _child_text(step_el, "addFilenameResut")
            or _child_text(step_el, "addfilenameresult")
            or _child_text(step_el, "add_filename_result")
        ),
        "after_sftp_put": after_action,
        "destination_folder": (
            _child_text(step_el, "destinationfolder")
            or _child_text(step_el, "destinationFolder")
        ),
        "destination_folder_field": (
            _child_text(step_el, "destinationfolderFieldName")
            or _child_text(step_el, "destinationfolderfield")
        ),
        "create_destination_folder": _bool_from_yn(
            _child_text(step_el, "createdestinationfolder")
            or _child_text(step_el, "createDestinationFolder")
        ),
        "wildcard": _child_text(step_el, "wildcard"),
        "copy_previous": _bool_from_yn(_child_text(step_el, "copyprevious")),
        "copy_previous_files": _bool_from_yn(
            _child_text(step_el, "copypreviousfiles")
        ),
        "success_when_no_file": _bool_from_yn(
            _child_text(step_el, "successWhenNoFile")
            or _child_text(step_el, "successwhennofile")
        ),
        "use_old_ssh_algorithms": _bool_from_yn(
            _child_text(step_el, "useoldalgorithmtossh")
            or _child_text(step_el, "useOldAlgorithm")
            or _child_text(step_el, "legacySSH")
        ),
        "error_target_step": _parse_error_target_step(step_el),
        "logging_level": (
            _child_text(step_el, "loglevel")
            or _child_text(step_el, "logLevel")
            or _child_text(step_el, "logging")
        ),
        "extras": extras,
    }


# ---------------------------------------------------------------------------
# Pentaho Server (BA Server) — Call Endpoint / Session Variables
# ---------------------------------------------------------------------------


_CALL_ENDPOINT_KNOWN_TAGS = frozenset({
    "name", "type", "description", "distribute", "custom_distribution",
    "copies", "partitioning", "remotesteps", "GUI", "attributes",
    "serverUrl", "serverurl", "url", "userName", "username", "password",
    "isBypassingAuthentication", "useSessionAuthentication",
    "moduleName", "module", "isModuleFromField", "endpointPath", "endpoint",
    "path", "httpMethod", "method", "isEndpointFromField", "resultField",
    "statusCodeField", "responseTimeField", "fields", "field",
    "headers", "header", "timeout", "connectionTimeout", "connection_timeout",
    "readTimeout", "read_timeout", "retries", "retry", "retryCount",
    "retryDelay", "ssl", "verifySSL", "trustAllSSL", "requestBody", "body",
    "contentType", "content_type", "encoding", "error", "loglevel", "logLevel",
})


def parse_call_endpoint_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Call Endpoint (``CallEndpointStep`` / CallEndpointMeta) XML.

    Primary tags: serverUrl, userName, password, isBypassingAuthentication,
    moduleName, isModuleFromField, endpointPath, httpMethod, isEndpointFromField,
    resultField, statusCodeField, responseTimeField, fields/field
    (fieldName, parameter, defaultValue).

    Optional extension tags (headers, timeout, retry, SSL, body) are preserved
    when present even though stock CallEndpointMeta does not define them.
    Secrets are never returned as plaintext.
    """
    raw_password = _child_text(step_el, "password")
    has_password = bool(raw_password and raw_password.strip())

    parameters: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        field_name = (
            _child_text(field_el, "fieldName")
            or _child_text(field_el, "field_name")
            or _child_text(field_el, "name")
        )
        parameter = (
            _child_text(field_el, "parameter")
            or _child_text(field_el, "param")
        )
        default_value = (
            _child_text(field_el, "defaultValue")
            or _child_text(field_el, "default_value")
            or _child_text(field_el, "default")
        )
        if not field_name and not parameter and not default_value:
            continue
        parameters.append({
            "field_name": field_name,
            "parameter": parameter,
            "default_value": default_value,
        })

    headers: list[dict[str, str]] = []
    headers_el = step_el.find("headers")
    header_nodes = (
        headers_el.findall("header") if headers_el is not None else step_el.findall("header")
    )
    for hdr_el in header_nodes:
        name = (
            _child_text(hdr_el, "name")
            or _child_text(hdr_el, "parameter")
            or _child_text(hdr_el, "key")
        )
        value = _child_text(hdr_el, "value")
        field = _child_text(hdr_el, "field") or _child_text(hdr_el, "fieldName")
        if name or value or field:
            headers.append({"name": name, "value": value, "field": field})

    extras = _element_to_dict(step_el) if step_el is not None else {}
    for tag in list(extras.keys()):
        if tag in _CALL_ENDPOINT_KNOWN_TAGS or tag == "password":
            if tag == "password":
                extras[tag] = "<redacted>"
            elif tag in ("fields", "headers", "error", "GUI", "attributes", "remotesteps"):
                extras.pop(tag, None)
            else:
                # Keep scalar known tags out of extras — promoted below.
                if tag in (
                    "serverUrl", "serverurl", "url", "userName", "username",
                    "moduleName", "module", "endpointPath", "endpoint", "path",
                    "httpMethod", "method", "resultField", "statusCodeField",
                    "responseTimeField", "isBypassingAuthentication",
                    "useSessionAuthentication", "isModuleFromField",
                    "isEndpointFromField", "timeout", "connectionTimeout",
                    "connection_timeout", "readTimeout", "read_timeout",
                    "retries", "retry", "retryCount", "retryDelay", "ssl",
                    "verifySSL", "trustAllSSL", "requestBody", "body",
                    "contentType", "content_type", "encoding", "loglevel",
                    "logLevel",
                ):
                    extras.pop(tag, None)

    use_session_auth = _bool_from_yn(
        _child_text(step_el, "isBypassingAuthentication")
        or _child_text(step_el, "useSessionAuthentication")
    )
    server_url = (
        _child_text(step_el, "serverUrl")
        or _child_text(step_el, "serverurl")
        or _child_text(step_el, "url")
    )
    endpoint_path = (
        _child_text(step_el, "endpointPath")
        or _child_text(step_el, "endpoint")
        or _child_text(step_el, "path")
    )
    endpoint_from_field = _bool_from_yn(
        _child_text(step_el, "isEndpointFromField")
    )
    module_from_field = _bool_from_yn(_child_text(step_el, "isModuleFromField"))
    # When isEndpointFromField, moduleName/endpointPath/httpMethod are input field names.
    http_method_raw = (
        _child_text(step_el, "httpMethod")
        or _child_text(step_el, "method")
        or "GET"
    ).strip() or "GET"
    http_method = http_method_raw if endpoint_from_field else http_method_raw.upper()
    module_name = (
        _child_text(step_el, "moduleName")
        or _child_text(step_el, "module")
    )
    timeout = (
        _child_text(step_el, "timeout")
        or _child_text(step_el, "connectionTimeout")
        or _child_text(step_el, "connection_timeout")
    )
    read_timeout = (
        _child_text(step_el, "readTimeout")
        or _child_text(step_el, "read_timeout")
    )
    retries = (
        _child_text(step_el, "retries")
        or _child_text(step_el, "retry")
        or _child_text(step_el, "retryCount")
    )
    retry_delay = _child_text(step_el, "retryDelay")
    verify_ssl_raw = (
        _child_text(step_el, "verifySSL")
        or _child_text(step_el, "ssl")
    )
    trust_all = _bool_from_yn(_child_text(step_el, "trustAllSSL"))
    if trust_all:
        verify_ssl = False
    elif verify_ssl_raw:
        verify_ssl = _bool_from_yn(verify_ssl_raw, default=True)
    else:
        verify_ssl = True

    request_body = (
        _child_text(step_el, "requestBody")
        or _child_text(step_el, "body")
    )
    content_type = (
        _child_text(step_el, "contentType")
        or _child_text(step_el, "content_type")
    )
    variable_substitution = bool(
        ("${" in (server_url + module_name + endpoint_path + http_method_raw))
        or ("%%" in (server_url + module_name + endpoint_path + http_method_raw))
        or any(
            ("${" in (p.get("default_value") or "") or "%%" in (p.get("default_value") or ""))
            for p in parameters
        )
    )

    return {
        "server_url": server_url,
        "url": server_url,
        "username": (
            _child_text(step_el, "userName")
            or _child_text(step_el, "username")
        ),
        "password_configured": has_password,
        "password_secret_ref": (
            "dbutils.secrets.get(scope='pentaho_server', key='password')"
            if has_password
            else ""
        ),
        "use_session_authentication": use_session_auth,
        "is_bypassing_authentication": use_session_auth,
        "module_name": module_name,
        "module_from_field": module_from_field,
        "endpoint_path": endpoint_path,
        "http_method": http_method,
        "endpoint_from_field": endpoint_from_field,
        "result_field": _child_text(step_el, "resultField") or "result",
        "status_code_field": _child_text(step_el, "statusCodeField"),
        "response_time_field": _child_text(step_el, "responseTimeField"),
        "parameters": parameters,
        "request_parameters": parameters,
        "headers": headers,
        "timeout": timeout,
        "connection_timeout": timeout,
        "read_timeout": read_timeout,
        "retries": retries,
        "retry_delay": retry_delay,
        "verify_ssl": verify_ssl,
        "ssl_settings": {
            "verify_ssl": verify_ssl,
            "trust_all_ssl": trust_all,
        },
        "request_body": request_body,
        "content_type": content_type,
        "encoding": _child_text(step_el, "encoding") or "UTF-8",
        "variable_substitution": variable_substitution,
        "error_target_step": _parse_error_target_step(step_el),
        "logging_level": (
            _child_text(step_el, "loglevel")
            or _child_text(step_el, "logLevel")
        ),
        "extras": extras,
    }


def parse_get_session_variable_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Get Session Variables (``GetSessionVariableStep``) XML.

    Primary tags under fields/field: name, variable, type, format, currency,
    decimal, group, length, precision, trim_type, default_value.
    """
    fields: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        name = _child_text(field_el, "name") or _child_text(field_el, "field_name")
        variable = (
            _child_text(field_el, "variable")
            or _child_text(field_el, "variable_name")
            or _child_text(field_el, "variableName")
        )
        if not name and not variable:
            continue
        length_raw = _child_text(field_el, "length")
        precision_raw = _child_text(field_el, "precision")
        try:
            length = int(float(length_raw)) if length_raw not in ("", None) else -1
        except ValueError:
            length = -1
        try:
            precision = int(float(precision_raw)) if precision_raw not in ("", None) else -1
        except ValueError:
            precision = -1
        fields.append({
            "name": name or variable,
            "variable": variable,
            "variable_name": variable,
            "default_value": (
                _child_text(field_el, "default_value")
                or _child_text(field_el, "defaultValue")
            ),
            "type": _child_text(field_el, "type") or "String",
            "type_name": _child_text(field_el, "type") or "String",
            "format": _child_text(field_el, "format"),
            "currency": _child_text(field_el, "currency"),
            "decimal": _child_text(field_el, "decimal"),
            "group": _child_text(field_el, "group"),
            "length": length,
            "precision": precision,
            "trim_type": (
                _child_text(field_el, "trim_type")
                or _child_text(field_el, "trimType")
                or "none"
            ),
            "scope": "BA_SESSION",
        })

    # Legacy flat attributes
    if not fields:
        var_name = (
            _child_text(step_el, "variable")
            or _child_text(step_el, "variable_name")
            or _child_text(step_el, "variableName")
        )
        field_name = _child_text(step_el, "name") or _child_text(step_el, "field_name") or var_name
        if field_name or var_name:
            fields.append({
                "name": field_name or var_name,
                "variable": var_name or "",
                "variable_name": var_name or "",
                "default_value": (
                    _child_text(step_el, "default_value")
                    or _child_text(step_el, "defaultValue")
                ),
                "type": _child_text(step_el, "type") or "String",
                "type_name": _child_text(step_el, "type") or "String",
                "format": _child_text(step_el, "format"),
                "currency": "",
                "decimal": "",
                "group": "",
                "length": -1,
                "precision": -1,
                "trim_type": "none",
                "scope": "BA_SESSION",
            })

    extras: dict[str, Any] = {}
    for child in step_el:
        if child.tag in ("fields", "field", "name", "type", "description", "distribute",
                         "copies", "partitioning", "GUI", "attributes", "error"):
            continue
        text = _text(child)
        if text:
            extras[child.tag] = text

    return {
        "fields": fields,
        "session_variables": [f.get("variable") for f in fields if f.get("variable")],
        "variable_names": [
            f.get("variable") or f.get("name") for f in fields
            if f.get("variable") or f.get("name")
        ],
        "output_columns": [f.get("name") for f in fields if f.get("name")],
        "scope": "BA_SESSION",
        "variable_inheritance": False,
        "extras": extras,
    }


def parse_set_session_variable_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Set Session Variables (``SetSessionVariableStep``) XML.

    Primary tags: use_formatting; fields/field with name, variable, default_value.
    Stock step always overwrites session attributes for the first input row only.
    """
    fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
    for field_el in targets:
        field_name = (
            _child_text(field_el, "name")
            or _child_text(field_el, "field_name")
            or _child_text(field_el, "fieldName")
        )
        variable_name = (
            _child_text(field_el, "variable")
            or _child_text(field_el, "variable_name")
            or _child_text(field_el, "variableName")
        )
        if not field_name and not variable_name:
            continue
        fields.append({
            "field_name": field_name,
            "variable_name": variable_name or field_name,
            "variable": variable_name or field_name,
            "default_value": (
                _child_text(field_el, "default_value")
                or _child_text(field_el, "defaultValue")
            ),
            "scope": "BA_SESSION",
        })

    if not fields:
        var_name = (
            _child_text(step_el, "variable")
            or _child_text(step_el, "variable_name")
            or _child_text(step_el, "variableName")
        )
        if var_name:
            fields.append({
                "field_name": _child_text(step_el, "name") or var_name,
                "variable_name": var_name,
                "variable": var_name,
                "default_value": (
                    _child_text(step_el, "default_value")
                    or _child_text(step_el, "defaultValue")
                    or _child_text(step_el, "value")
                ),
                "scope": "BA_SESSION",
            })

    overwrite_raw = (
        _child_text(step_el, "overwrite")
        or _child_text(step_el, "overwriteExisting")
    )
    # Stock SetSessionVariable always overwrites; honour explicit tag when present.
    overwrite = _bool_from_yn(overwrite_raw, default=True) if overwrite_raw else True

    extras: dict[str, Any] = {}
    for child in step_el:
        if child.tag in (
            "fields", "field", "name", "type", "description", "distribute",
            "copies", "partitioning", "GUI", "attributes", "error",
            "use_formatting", "useFormatting", "overwrite", "overwriteExisting",
        ):
            continue
        text = _text(child)
        if text:
            extras[child.tag] = text

    return {
        "fields": fields,
        "use_formatting": _bool_from_yn(
            _child_text(step_el, "use_formatting")
            or _child_text(step_el, "useFormatting"),
            default=False,
        ),
        "overwrite": overwrite,
        "scope": "BA_SESSION",
        "variable_names": [f["variable_name"] for f in fields if f.get("variable_name")],
        "session_variables": [f["variable_name"] for f in fields if f.get("variable_name")],
        "extras": extras,
    }


def parse_experimental_script_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Experimental Script (javax.script multi-language) XML.

    Shares the same ``jsScripts`` / ``fields`` shape as Modified Java Script Value.
    Language is inferred from script-name / step-name extensions (PDI behavior):
    ``.py``, ``.rb``, ``.groovy``, ``.js`` (default Javascript).
    """
    base = parse_javascript_value_config(step_el)
    scripts = list(base.get("scripts") or [])
    language = "javascript"
    candidates: list[str] = []
    for script in scripts:
        name = str(script.get("name") or "")
        if "." in name:
            candidates.append(name.rsplit(".", 1)[-1].lower())
    step_name = _child_text(step_el, "name")
    if step_name and "." in step_name:
        candidates.append(step_name.rsplit(".", 1)[-1].lower())
    lang_tag = (
        _child_text(step_el, "scriptlanguage")
        or _child_text(step_el, "scriptLanguage")
        or _child_text(step_el, "language")
        or _child_text(step_el, "engine")
    )
    if lang_tag:
        candidates.insert(0, lang_tag.strip().lower())

    for ext in candidates:
        if ext in ("py", "python", "jython"):
            language = "python"
            break
        if ext in ("rb", "ruby", "jruby"):
            language = "ruby"
            break
        if ext in ("groovy", "gy"):
            language = "groovy"
            break
        if ext in ("js", "javascript", "ecmascript", "nashorn", "rhino"):
            language = "javascript"
            break

    extras = _element_to_dict(step_el) if step_el is not None else {}
    return {
        **base,
        "script_language": language,
        "script_engine": language,
        "error_target_step": _parse_error_target_step(step_el),
        "extras": extras,
    }


# ---------------------------------------------------------------------------
# Cryptography / Validation steps
# ---------------------------------------------------------------------------


def parse_pgp_encrypt_stream_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse PGP Encrypt Stream (GPG location, key, stream/result fields, optional armor).

    Native PDI XML tags: gpglocation, keyname, keynameInField, keynameFieldName,
    streamfield, resultfieldname.

    ``gpglocation`` is the path to the **gpg executable** (not the keyring home).
    Optional extension tags (ascii armor, compression, integrity, public key /
    keyring home) are preserved when present in the KTR.
    """
    extras = _element_to_dict(step_el) if step_el is not None else {}
    return {
        "gpg_location": (
            _child_text(step_el, "gpglocation")
            or _child_text(step_el, "gpgLocation")
            or _child_text(step_el, "gpg_location")
        ),
        "key_name": (
            _child_text(step_el, "keyname")
            or _child_text(step_el, "keyName")
            or _child_text(step_el, "key_name")
        ),
        "keyname_in_field": _bool_from_yn(
            _child_text(step_el, "keynameInField")
            or _child_text(step_el, "keyname_in_field")
        ),
        "keyname_field": (
            _child_text(step_el, "keynameFieldName")
            or _child_text(step_el, "keyname_field")
            or _child_text(step_el, "keyNameField")
        ),
        "stream_field": (
            _child_text(step_el, "streamfield")
            or _child_text(step_el, "streamField")
            or _child_text(step_el, "messageField")
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
        # Optional / extension properties (not always present in stock PDI)
        "public_key": (
            _child_text(step_el, "publickey")
            or _child_text(step_el, "publicKey")
            or _child_text(step_el, "public_key")
        ),
        "keyring_path": (
            _child_text(step_el, "keyring")
            or _child_text(step_el, "keyRing")
            or _child_text(step_el, "keyring_path")
            or _child_text(step_el, "keyRingPath")
        ),
        "ascii_armor": _bool_from_yn(
            _child_text(step_el, "asciiarmor")
            or _child_text(step_el, "asciiArmor")
            or _child_text(step_el, "ascii_armor")
            or _child_text(step_el, "armored"),
            default=True,
        ),
        "compression": (
            _child_text(step_el, "compression")
            or _child_text(step_el, "compress")
            or _child_text(step_el, "compression_algorithm")
        ),
        "integrity_check": _bool_from_yn(
            _child_text(step_el, "integritycheck")
            or _child_text(step_el, "integrityCheck")
            or _child_text(step_el, "integrity_check")
            or _child_text(step_el, "withIntegrity"),
            default=True,
        ),
        "error_target_step": _parse_error_target_step(step_el),
        "extras": extras,
    }


def parse_pgp_decrypt_stream_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse PGP Decrypt Stream (GPG location, passphrase, stream/result fields).

    Note: PDI serializes the passphrase tag as ``passhrase`` (historical typo).
    Passphrase values are never returned as plaintext — only a presence flag /
    secret-reference placeholder is stored.

    ``gpglocation`` is the path to the **gpg executable**. Optional ``keyring`` /
    ``private_key`` extension tags identify key material locations (never inlined).
    """
    # Tag is misspelled "passhrase" in PDI source; also accept "passphrase"
    raw_passphrase = (
        _child_text(step_el, "passhrase")
        or _child_text(step_el, "passphrase")
        or _child_text(step_el, "passPhrase")
    )
    has_passphrase = bool(raw_passphrase and raw_passphrase.strip())
    extras = _element_to_dict(step_el) if step_el is not None else {}
    # Redact secrets from extras copy
    for secret_tag in ("passhrase", "passphrase", "passPhrase", "password"):
        if secret_tag in extras:
            extras[secret_tag] = "<redacted>"
    return {
        "gpg_location": (
            _child_text(step_el, "gpglocation")
            or _child_text(step_el, "gpgLocation")
            or _child_text(step_el, "gpg_location")
        ),
        "passphrase_configured": has_passphrase,
        "passphrase_secret_ref": (
            "dbutils.secrets.get(scope='pgp', key='passphrase')"
            if has_passphrase
            else ""
        ),
        "passphrase_from_field": _bool_from_yn(
            _child_text(step_el, "passphraseFromField")
            or _child_text(step_el, "passphrase_from_field")
        ),
        "passphrase_field": (
            _child_text(step_el, "passphraseFieldName")
            or _child_text(step_el, "passphrase_field")
            or _child_text(step_el, "passPhraseField")
        ),
        "stream_field": (
            _child_text(step_el, "streamfield")
            or _child_text(step_el, "streamField")
            or _child_text(step_el, "messageField")
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
        "keyring_path": (
            _child_text(step_el, "keyring")
            or _child_text(step_el, "keyRing")
            or _child_text(step_el, "keyring_path")
        ),
        "private_key": (
            _child_text(step_el, "privatekey")
            or _child_text(step_el, "privateKey")
            or _child_text(step_el, "private_key")
        ),
        "integrity_check": _bool_from_yn(
            _child_text(step_el, "integritycheck")
            or _child_text(step_el, "integrityCheck")
            or _child_text(step_el, "integrity_check"),
            default=True,
        ),
        "error_target_step": _parse_error_target_step(step_el),
        "extras": extras,
    }


def parse_secret_key_generator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Secret Key Generator algorithm / length / count / encoding fields."""
    keys: list[dict[str, Any]] = []
    fields_el = step_el.find("fields")
    field_nodes = (
        list(fields_el.findall("field")) if fields_el is not None else []
    )
    # Fallback: some KTRs emit field children without a <fields> wrapper
    if not field_nodes:
        field_nodes = list(step_el.findall("field"))

    for field_el in field_nodes:
        algo = (
            _child_text(field_el, "algorithm")
            or _child_text(field_el, "Algorithm")
        )
        scheme = (
            _child_text(field_el, "scheme")
            or _child_text(field_el, "schema")
            or algo
        )
        length_raw = (
            _child_text(field_el, "secretKeyLen")
            or _child_text(field_el, "secretKeyLength")
            or _child_text(field_el, "keyLength")
            or ""
        )
        count_raw = (
            _child_text(field_el, "secretKeyCount")
            or _child_text(field_el, "howMany")
            or _child_text(field_el, "count")
            or "1"
        )
        try:
            key_length = int(float(length_raw)) if length_raw else 128
        except ValueError:
            key_length = 128
        try:
            key_count = int(float(count_raw)) if count_raw else 1
        except ValueError:
            key_count = 1
        if algo or scheme or length_raw:
            keys.append({
                "algorithm": algo or "AES",
                "scheme": scheme or algo or "AES",
                "key_length": key_length,
                "key_length_raw": length_raw,
                "key_count": max(key_count, 0),
                "key_count_raw": count_raw,
            })

    encoding = "binary" if _bool_from_yn(
        _child_text(step_el, "outputKeyInBinary")
        or _child_text(step_el, "output_key_in_binary")
    ) else "hex"

    return {
        "keys": keys,
        "fields": keys,
        "secret_key_field": (
            _child_text(step_el, "secretKeyFieldName")
            or _child_text(step_el, "secret_key_field")
            or "secretKey"
        ),
        "secret_key_length_field": (
            _child_text(step_el, "secretKeyLengthFieldName")
            or _child_text(step_el, "secret_key_length_field")
        ),
        "algorithm_field": (
            _child_text(step_el, "algorithmFieldName")
            or _child_text(step_el, "algorithm_field")
        ),
        "output_key_in_binary": encoding == "binary",
        "encoding": encoding,
        "error_target_step": _parse_error_target_step(step_el),
    }


def parse_symmetric_crypto_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Symmetric Cryptography encrypt/decrypt settings and field mappings.

    Secret key material is never returned as plaintext — only a presence flag and
    a Databricks Secrets placeholder reference are stored.
    """
    raw_secret = (
        _child_text(step_el, "secretKey")
        or _child_text(step_el, "secret_key")
        or _child_text(step_el, "password")
    )
    has_secret = bool(raw_secret and raw_secret.strip())
    operation = (
        _child_text(step_el, "operation_type")
        or _child_text(step_el, "operationType")
        or _child_text(step_el, "operation")
        or "encrypt"
    ).strip().lower()
    if operation not in ("encrypt", "decrypt"):
        # Accept localized / descriptive labels
        op_l = operation.lower()
        if "decrypt" in op_l:
            operation = "decrypt"
        else:
            operation = "encrypt"

    algorithm = (
        _child_text(step_el, "algorithm")
        or _child_text(step_el, "Algorithm")
        or "AES"
    )
    schema = (
        _child_text(step_el, "schema")
        or _child_text(step_el, "scheme")
        or algorithm
    )
    # Derive cipher mode from scheme when present (e.g. AES/CBC/PKCS5Padding)
    cipher_mode = ""
    padding = ""
    parts = [p.strip() for p in schema.replace("_", "/").split("/") if p.strip()]
    if len(parts) >= 2:
        cipher_mode = parts[1].upper()
    if len(parts) >= 3:
        padding = parts[2]

    iv = (
        _child_text(step_el, "iv")
        or _child_text(step_el, "IV")
        or _child_text(step_el, "initializationVector")
        or _child_text(step_el, "initialization_vector")
    )
    iv_field = (
        _child_text(step_el, "ivField")
        or _child_text(step_el, "iv_field")
        or _child_text(step_el, "initializationVectorField")
    )
    # Redact IV material from presence-only flag when it looks like a key blob
    iv_configured = bool(iv and iv.strip())
    iv_secret_ref = (
        "dbutils.secrets.get(scope='crypto', key='iv')" if iv_configured else ""
    )

    extras = _element_to_dict(step_el) if step_el is not None else {}
    for secret_tag in ("secretKey", "secret_key", "password", "iv", "IV"):
        if secret_tag in extras:
            extras[secret_tag] = "<redacted>"

    return {
        "operation_type": operation,
        "algorithm": algorithm,
        "schema": schema,
        "scheme": schema,
        "cipher_mode": cipher_mode,
        "padding": padding,
        "message_field": (
            _child_text(step_el, "messageField")
            or _child_text(step_el, "message_field")
            or _child_text(step_el, "streamfield")
        ),
        "result_field": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
        "secret_key_configured": has_secret,
        "secret_key_secret_ref": (
            "dbutils.secrets.get(scope='crypto', key='secret_key')"
            if has_secret
            else ""
        ),
        "secret_key_in_field": _bool_from_yn(
            _child_text(step_el, "secretKeyInField")
            or _child_text(step_el, "secret_key_in_field")
        ),
        "secret_key_field": (
            _child_text(step_el, "secretKeyField")
            or _child_text(step_el, "secret_key_field")
        ),
        "read_key_as_binary": _bool_from_yn(
            _child_text(step_el, "readKeyAsBinary")
            or _child_text(step_el, "read_key_as_binary")
        ),
        "output_result_as_binary": _bool_from_yn(
            _child_text(step_el, "outputResultAsBinary")
            or _child_text(step_el, "output_result_as_binary")
        ),
        "iv_configured": iv_configured,
        "iv_secret_ref": iv_secret_ref,
        "iv_field": iv_field,
        "error_target_step": _parse_error_target_step(step_el),
        "extras": extras,
    }


def parse_credit_card_validator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Credit Card Validator field, Luhn options, and result columns."""
    return {
        "fieldname": (
            _child_text(step_el, "fieldname")
            or _child_text(step_el, "fieldName")
            or _child_text(step_el, "dynamicField")
        ),
        "resultfieldname": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
        "cardtype": (
            _child_text(step_el, "cardtype")
            or _child_text(step_el, "cardType")
        ),
        "onlydigits": _bool_from_yn(
            _child_text(step_el, "onlydigits") or _child_text(step_el, "onlyDigits")
        ),
        "notvalidmsg": (
            _child_text(step_el, "notvalidmsg")
            or _child_text(step_el, "notValidMsg")
        ),
        "send_true_to": _child_text(step_el, "send_true_to"),
        "send_false_to": _child_text(step_el, "send_false_to"),
        "error_target_step": _parse_error_target_step(step_el),
    }


def _parse_error_target_step(step_el: ET.Element) -> str:
    """Extract optional step-error-handling destination (accepted/rejected hop)."""
    err = step_el.find("error")
    if err is None:
        err = step_el.find("step_error_handling")
    if err is None:
        return ""
    return (
        _child_text(err, "target_step")
        or _child_text(err, "targetStep")
        or _child_text(err, "error_dest_step")
        or _text(err)
    )


def parse_data_validator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Data Validator rules (validator_field) and error-handling options."""
    validations: list[dict[str, Any]] = []
    for field_el in step_el.findall("validator_field"):
        allowed: list[str] = []
        allowed_el = field_el.find("allowed_value")
        if allowed_el is not None:
            for val_el in allowed_el.findall("value"):
                text = _text(val_el)
                if text != "" or (val_el.text is not None):
                    allowed.append(text)
        validations.append({
            "field_name": (
                _child_text(field_el, "name")
                or _child_text(field_el, "field_name")
                or _child_text(field_el, "fieldName")
            ),
            "validation_name": (
                _child_text(field_el, "validation_name")
                or _child_text(field_el, "validationName")
            ),
            "maximum_length": _child_text(field_el, "max_length"),
            "minimum_length": _child_text(field_el, "min_length"),
            "null_allowed": _bool_from_yn(
                _child_text(field_el, "null_allowed"), default=True
            ),
            "only_null_allowed": _bool_from_yn(
                _child_text(field_el, "only_null_allowed")
            ),
            "only_numeric_allowed": _bool_from_yn(
                _child_text(field_el, "only_numeric_allowed")
            ),
            "data_type": _child_text(field_el, "data_type"),
            "data_type_verified": _bool_from_yn(
                _child_text(field_el, "data_type_verified")
            ),
            "conversion_mask": _child_text(field_el, "conversion_mask"),
            "decimal_symbol": _child_text(field_el, "decimal_symbol"),
            "grouping_symbol": _child_text(field_el, "grouping_symbol"),
            "maximum_value": _child_text(field_el, "max_value"),
            "minimum_value": _child_text(field_el, "min_value"),
            "start_string": _child_text(field_el, "start_string"),
            "end_string": _child_text(field_el, "end_string"),
            "start_string_not_allowed": _child_text(
                field_el, "start_string_not_allowed"
            ),
            "end_string_not_allowed": _child_text(field_el, "end_string_not_allowed"),
            "regular_expression": _child_text(field_el, "regular_expression"),
            "regular_expression_not_allowed": _child_text(
                field_el, "regular_expression_not_allowed"
            ),
            "error_code": _child_text(field_el, "error_code"),
            "error_description": _child_text(field_el, "error_description"),
            "is_sourcing_values": _bool_from_yn(
                _child_text(field_el, "is_sourcing_values")
            ),
            "sourcing_step": _child_text(field_el, "sourcing_step"),
            "sourcing_field": _child_text(field_el, "sourcing_field"),
            "allowed_values": allowed,
        })

    return {
        "validate_all": _bool_from_yn(_child_text(step_el, "validate_all")),
        "concat_errors": _bool_from_yn(_child_text(step_el, "concat_errors")),
        "concat_separator": (
            _child_text(step_el, "concat_separator")
            or _child_text(step_el, "concatenation_separator")
            or "|"
        ),
        "validations": validations,
        "send_true_to": _child_text(step_el, "send_true_to"),
        "send_false_to": _child_text(step_el, "send_false_to"),
        "error_target_step": _parse_error_target_step(step_el),
    }


def parse_mail_validator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Mail Validator email field, SMTP options, and result columns."""
    return {
        "emailfield": (
            _child_text(step_el, "emailfield")
            or _child_text(step_el, "emailField")
            or _child_text(step_el, "email_field")
        ),
        "resultfieldname": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
        "result_as_string": _bool_from_yn(
            _child_text(step_el, "ResultAsString")
            or _child_text(step_el, "resultAsString")
        ),
        "smtp_check": _bool_from_yn(
            _child_text(step_el, "smtpCheck") or _child_text(step_el, "smtp_check")
        ),
        "email_valid_msg": (
            _child_text(step_el, "emailValideMsg")
            or _child_text(step_el, "emailValidMsg")
            or "Email is valid"
        ),
        "email_not_valid_msg": (
            _child_text(step_el, "emailNotValideMsg")
            or _child_text(step_el, "emailNotValidMsg")
            or "Email is not valid"
        ),
        "errors_field_name": (
            _child_text(step_el, "errorsFieldName")
            or _child_text(step_el, "errors_field_name")
        ),
        "timeout": _child_text(step_el, "timeout") or "0",
        "default_smtp": (
            _child_text(step_el, "defaultSMTP") or _child_text(step_el, "default_smtp")
        ),
        "email_sender": (
            _child_text(step_el, "emailSender") or _child_text(step_el, "email_sender")
        ),
        "default_smtp_field": (
            _child_text(step_el, "defaultSMTPField")
            or _child_text(step_el, "default_smtp_field")
        ),
        "dynamic_default_smtp": _bool_from_yn(
            _child_text(step_el, "isdynamicDefaultSMTP")
            or _child_text(step_el, "isDynamicDefaultSMTP")
        ),
        "send_true_to": _child_text(step_el, "send_true_to"),
        "send_false_to": _child_text(step_el, "send_false_to"),
        "error_target_step": _parse_error_target_step(step_el),
    }


def parse_xsd_validator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse XSD Validator XML/XSD sources, result fields, and namespace options."""
    xsd_source = (
        _child_text(step_el, "xsdsource")
        or _child_text(step_el, "xsdSource")
        or "filename"
    )
    return {
        "xmlstream": (
            _child_text(step_el, "xmlstream")
            or _child_text(step_el, "xmlStream")
            or _child_text(step_el, "xml_field")
        ),
        "xmlsourcefile": _bool_from_yn(
            _child_text(step_el, "xmlsourcefile")
            or _child_text(step_el, "xmlSourceFile")
        ),
        "xsdfilename": (
            _child_text(step_el, "xdsfilename")  # Pentaho historical typo
            or _child_text(step_el, "xsdfilename")
            or _child_text(step_el, "xsdFilename")
        ),
        "xsdsource": xsd_source,
        "xsddefinedfield": (
            _child_text(step_el, "xsddefinedfield")
            or _child_text(step_el, "xsdDefinedField")
        ),
        "resultfieldname": (
            _child_text(step_el, "resultfieldname")
            or _child_text(step_el, "resultFieldName")
            or "result"
        ),
        "outputstringfield": _bool_from_yn(
            _child_text(step_el, "outputstringfield")
            or _child_text(step_el, "outputStringField")
        ),
        "ifxmlvalid": (
            _child_text(step_el, "ifxmlvalid") or _child_text(step_el, "ifXmlValid")
        ),
        "ifxmlinvalid": (
            _child_text(step_el, "ifxmlunvalid")  # Pentaho historical typo
            or _child_text(step_el, "ifxmlinvalid")
            or _child_text(step_el, "ifXmlInvalid")
        ),
        "addvalidationmsg": _bool_from_yn(
            _child_text(step_el, "addvalidationmsg")
            or _child_text(step_el, "addValidationMsg")
        ),
        "validationmsgfield": (
            _child_text(step_el, "validationmsgfield")
            or _child_text(step_el, "validationMsgField")
            or "ValidationMsgField"
        ),
        "allow_external_entities": _bool_from_yn(
            _child_text(step_el, "allowExternalEntities")
            or _child_text(step_el, "allow_external_entities"),
            default=True,
        ),
        "send_true_to": _child_text(step_el, "send_true_to"),
        "send_false_to": _child_text(step_el, "send_false_to"),
        "error_target_step": _parse_error_target_step(step_el),
    }


_BULK_LOADER_SKIP_TAGS = frozenset({
    "name", "type", "description", "distribute", "custom_distribution",
    "copies", "partitioning", "remotesteps", "GUI", "draw", "attributes",
    "mapping", "mappings", "fields", "field",
})

# Tags already promoted to first-class keys — omit from residual extras.
_BULK_LOADER_PROMOTED_TAGS = frozenset({
    "connection", "schema", "schemaname", "table", "tablename", "database",
    "dbname", "dbNameOverride", "dbnameoverride", "truncate",
    "loadMethod", "loadmethod", "load_method", "loadAction", "loadaction",
    "load_action", "load_mode", "bulkLoadMode", "commit", "commitSize",
    "commitsize", "batchSize", "batchsize", "bulkSize", "bulksize",
    "bufferSize", "buffersize", "bindSize", "bindsize", "readSize", "readsize",
    "delimiter", "enclosure", "escape", "escapeChar", "escapechar",
    "nullif", "nullString", "nullstring", "encoding", "charset", "characterSet",
    "compression", "fifoFileName", "fifofilename", "fifo_file", "dataFile",
    "datafile", "controlFile", "controlfile", "logFile", "logfile",
    "badFile", "badfile", "discardFile", "discardfile", "errorFile", "errorfile",
    "loadFile", "loadfile", "filename", "file", "psqlPath", "psqlpath",
    "sqlldr", "sqlldrPath", "sqlldrpath", "gploadPath", "gploadpath",
    "mclientPath", "mclientpath", "vwloadPath", "vwloadpath",
    "fastloadPath", "fastloadpath", "tptPath", "tptpath",
    "stopOnError", "stoponerror", "stop_on_error", "maxErrors", "maxerrors",
    "max_errors", "errorsAllowed", "errorsallowed", "rejectErrors",
    "reject_errors", "rejectLimit", "rejectlimit", "ignoreErrors", "ignoreerrors",
    "eraseFiles", "erasefiles", "direct", "parallel", "parallelDegree",
    "paralleldegree", "localInfile", "localinfile", "replace", "replaceData",
    "ignore", "ignoreDuplicates", "continueOnError", "continueonerror",
    "transactionSize", "transactionsize", "streamName", "streamname",
    "copyStatement", "copystatement", "tablespace", "tableSpace",
    "errorTable", "errortable", "workTable", "worktable", "logTable", "logtable",
    "sessions", "maxSessions", "maxsessions", "minSessions", "minsessions",
    "packFactor", "packfactor", "fillRecord", "fillrecord",
    "explicit_dates", "dateMask", "datemask", "dateFormat", "dateformat",
    "timeFormat", "timeformat", "timestampFormat", "timestampformat",
    "tptOperator", "tptoperator", "operator", "dataOperator", "dataoperator",
    "tbuildPath", "tbuildpath", "tbuild", "jobName", "jobname", "tptJobName",
    "agentHost", "agenthost", "agentPort", "agentport", "agent",
    "key", "keys", "keyFields", "keyfields",
})


def _parse_bulk_loader_fields(step_el: ET.Element) -> list[dict[str, str]]:
    """Parse stream→table field mappings from mapping/fields containers."""
    fields: list[dict[str, str]] = []
    containers: list[ET.Element] = []
    for tag in ("mapping", "mappings", "fields"):
        el = step_el.find(tag)
        if el is not None:
            containers.append(el)
    # Some KTRs place bare <mapping> or <field> children on the step root.
    for child in list(step_el):
        if child.tag in ("mapping", "field") and list(child):
            containers.append(child)

    seen: set[tuple[str, str]] = set()
    for container in containers:
        candidates = list(container.findall("field")) + list(container.findall("mapping"))
        if not candidates and container.tag in ("mapping", "field"):
            candidates = [container]
        for field_el in candidates:
            stream = (
                _child_text(field_el, "stream_name")
                or _child_text(field_el, "streamName")
                or _child_text(field_el, "name")
                or _child_text(field_el, "field")
            )
            table_field = (
                _child_text(field_el, "field_name")
                or _child_text(field_el, "column_name")
                or _child_text(field_el, "table_field")
                or _child_text(field_el, "tableField")
                or stream
            )
            if not stream and not table_field:
                continue
            key = (stream, table_field)
            if key in seen:
                continue
            seen.add(key)
            is_key_raw = (
                _child_text(field_el, "key")
                or _child_text(field_el, "isKey")
                or _child_text(field_el, "iskey")
            )
            item = {
                "stream_field": stream,
                "table_field": table_field,
                "date_mask": (
                    _child_text(field_el, "date_mask")
                    or _child_text(field_el, "dateMask")
                    or _child_text(field_el, "format")
                ),
                "is_key": "Y" if _bool_from_yn(is_key_raw) else "N",
                "update": _child_text(field_el, "update") or "Y",
            }
            fields.append(item)
    return fields


def _parse_bulk_loader_key_fields(step_el: ET.Element) -> list[str]:
    """Parse TPT/upsert key field names from key containers or field flags."""
    keys: list[str] = []
    for tag in ("key", "keys", "keyFields", "keyfields"):
        parent = step_el.find(tag)
        if parent is None:
            continue
        if list(parent) == []:
            text = _text(parent)
            if text:
                keys.extend(p.strip() for p in text.replace(";", ",").split(",") if p.strip())
            continue
        for child in list(parent):
            name = (
                _child_text(child, "name")
                or _child_text(child, "field")
                or _child_text(child, "key")
                or _text(child)
            )
            if name:
                keys.append(name)
    # Also promote fields marked as keys in mapping
    for item in _parse_bulk_loader_fields(step_el):
        if item.get("is_key", "N").upper() == "Y" and item.get("table_field"):
            keys.append(item["table_field"])
    # Deduplicate preserving order
    seen: set[str] = set()
    ordered: list[str] = []
    for name in keys:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def parse_bulk_loader_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse shared + vendor-specific Bulk Loader XML into structured metadata.

    Covers Greenplum, Infobright, VectorWise, MonetDB, MySQL, Oracle, PostgreSQL,
    Teradata FastLoad/TPT, and Vertica loaders. Residual child tags are kept in
    ``extras`` so no Pentaho property is dropped.
    """
    schema = (
        _child_text(step_el, "schema")
        or _child_text(step_el, "schemaname")
        or _child_text(step_el, "schemaName")
    )
    table = (
        _child_text(step_el, "table")
        or _child_text(step_el, "tablename")
        or _child_text(step_el, "tableName")
    )
    truncate_raw = (
        _child_text(step_el, "truncate")
        or _child_text(step_el, "Truncate")
    )
    load_action = (
        _child_text(step_el, "loadAction")
        or _child_text(step_el, "loadaction")
        or _child_text(step_el, "load_action")
        or _child_text(step_el, "load_mode")
        or _child_text(step_el, "bulkLoadMode")
    )
    # Truncate action without explicit truncate flag
    if not truncate_raw and load_action.strip().upper() in ("TRUNCATE", "REPLACE"):
        truncate_raw = "Y"

    extras: dict[str, Any] = {}
    for child in list(step_el):
        if child.tag in _BULK_LOADER_SKIP_TAGS or child.tag in _BULK_LOADER_PROMOTED_TAGS:
            continue
        if list(child) == []:
            text = _text(child)
            if text != "":
                extras[child.tag] = text
        else:
            extras[child.tag] = ET.tostring(child, encoding="unicode")

    cfg: dict[str, Any] = {
        "connection": _child_text(step_el, "connection"),
        "schema": schema,
        "table": table,
        "database": (
            _child_text(step_el, "database")
            or _child_text(step_el, "dbname")
            or _child_text(step_el, "dbName")
        ),
        "db_name_override": (
            _child_text(step_el, "dbNameOverride")
            or _child_text(step_el, "dbnameoverride")
        ),
        "truncate": truncate_raw or "N",
        "load_method": (
            _child_text(step_el, "loadMethod")
            or _child_text(step_el, "loadmethod")
            or _child_text(step_el, "load_method")
        ),
        "load_action": load_action,
        "bulk_load_mode": load_action,
        "commit_size": (
            _child_text(step_el, "commit")
            or _child_text(step_el, "commitSize")
            or _child_text(step_el, "commitsize")
        ),
        "batch_size": (
            _child_text(step_el, "batchSize")
            or _child_text(step_el, "batchsize")
            or _child_text(step_el, "bulkSize")
            or _child_text(step_el, "bulksize")
        ),
        "buffer_size": (
            _child_text(step_el, "bufferSize")
            or _child_text(step_el, "buffersize")
            or _child_text(step_el, "bindSize")
            or _child_text(step_el, "bindsize")
        ),
        "bind_size": _child_text(step_el, "bindSize") or _child_text(step_el, "bindsize"),
        "read_size": _child_text(step_el, "readSize") or _child_text(step_el, "readsize"),
        "delimiter": (
            _child_text(step_el, "delimiter")
            or _child_text(step_el, "separator")
        ),
        "enclosure": _child_text(step_el, "enclosure"),
        "escape_char": (
            _child_text(step_el, "escape")
            or _child_text(step_el, "escapeChar")
            or _child_text(step_el, "escapechar")
        ),
        "null_string": (
            _child_text(step_el, "nullif")
            or _child_text(step_el, "nullString")
            or _child_text(step_el, "nullstring")
        ),
        "encoding": (
            _child_text(step_el, "encoding")
            or _child_text(step_el, "charset")
            or _child_text(step_el, "characterSet")
        ),
        "charset": _child_text(step_el, "charset") or _child_text(step_el, "characterSet"),
        "compression": _child_text(step_el, "compression"),
        "file_format": _child_text(step_el, "fileFormat") or _child_text(step_el, "format"),
        "fifo_file": (
            _child_text(step_el, "fifoFileName")
            or _child_text(step_el, "fifofilename")
            or _child_text(step_el, "fifo_file")
        ),
        "data_file": (
            _child_text(step_el, "dataFile")
            or _child_text(step_el, "datafile")
            or _child_text(step_el, "filename")
            or _child_text(step_el, "file")
        ),
        "control_file": (
            _child_text(step_el, "controlFile")
            or _child_text(step_el, "controlfile")
        ),
        "log_file": _child_text(step_el, "logFile") or _child_text(step_el, "logfile"),
        "bad_file": _child_text(step_el, "badFile") or _child_text(step_el, "badfile"),
        "discard_file": (
            _child_text(step_el, "discardFile")
            or _child_text(step_el, "discardfile")
        ),
        "error_file": (
            _child_text(step_el, "errorFile")
            or _child_text(step_el, "errorfile")
        ),
        "load_file": _child_text(step_el, "loadFile") or _child_text(step_el, "loadfile"),
        "psql_path": _child_text(step_el, "psqlPath") or _child_text(step_el, "psqlpath"),
        "sqlldr_path": (
            _child_text(step_el, "sqlldr")
            or _child_text(step_el, "sqlldrPath")
            or _child_text(step_el, "sqlldrpath")
        ),
        "gpload_path": _child_text(step_el, "gploadPath") or _child_text(step_el, "gploadpath"),
        "mclient_path": (
            _child_text(step_el, "mclientPath")
            or _child_text(step_el, "mclientpath")
        ),
        "vwload_path": _child_text(step_el, "vwloadPath") or _child_text(step_el, "vwloadpath"),
        "fastload_path": (
            _child_text(step_el, "fastloadPath")
            or _child_text(step_el, "fastloadpath")
        ),
        "tpt_path": _child_text(step_el, "tptPath") or _child_text(step_el, "tptpath"),
        "stop_on_error": (
            _child_text(step_el, "stopOnError")
            or _child_text(step_el, "stoponerror")
            or _child_text(step_el, "stop_on_error")
            or "Y"
        ),
        "max_errors": (
            _child_text(step_el, "maxErrors")
            or _child_text(step_el, "maxerrors")
            or _child_text(step_el, "max_errors")
            or _child_text(step_el, "errorsAllowed")
            or _child_text(step_el, "errorsallowed")
        ),
        "reject_errors": (
            _child_text(step_el, "rejectErrors")
            or _child_text(step_el, "reject_errors")
        ),
        "reject_limit": (
            _child_text(step_el, "rejectLimit")
            or _child_text(step_el, "rejectlimit")
        ),
        "ignore_errors": (
            _child_text(step_el, "ignoreErrors")
            or _child_text(step_el, "ignoreerrors")
            or "N"
        ),
        "erase_files": (
            _child_text(step_el, "eraseFiles")
            or _child_text(step_el, "erasefiles")
            or "Y"
        ),
        "direct": _child_text(step_el, "direct") or "N",
        "parallel": (
            _child_text(step_el, "parallel")
            or _child_text(step_el, "Parallel")
            or "N"
        ),
        "parallel_degree": (
            _child_text(step_el, "parallelDegree")
            or _child_text(step_el, "paralleldegree")
        ),
        "local_infile": (
            _child_text(step_el, "localInfile")
            or _child_text(step_el, "localinfile")
            or "Y"
        ),
        "replace_data": (
            _child_text(step_el, "replace")
            or _child_text(step_el, "replaceData")
            or "N"
        ),
        "ignore_duplicates": (
            _child_text(step_el, "ignore")
            or _child_text(step_el, "ignoreDuplicates")
            or "N"
        ),
        "continue_on_error": (
            _child_text(step_el, "continueOnError")
            or _child_text(step_el, "continueonerror")
            or "N"
        ),
        "transaction_size": (
            _child_text(step_el, "transactionSize")
            or _child_text(step_el, "transactionsize")
        ),
        "stream_name": (
            _child_text(step_el, "streamName")
            or _child_text(step_el, "streamname")
        ),
        "copy_statement": (
            _child_text(step_el, "copyStatement")
            or _child_text(step_el, "copystatement")
        ),
        "table_space": (
            _child_text(step_el, "tablespace")
            or _child_text(step_el, "tableSpace")
        ),
        "error_table": (
            _child_text(step_el, "errorTable")
            or _child_text(step_el, "errortable")
        ),
        "work_table": (
            _child_text(step_el, "workTable")
            or _child_text(step_el, "worktable")
        ),
        "log_table": (
            _child_text(step_el, "logTable")
            or _child_text(step_el, "logtable")
        ),
        "sessions": _child_text(step_el, "sessions"),
        "max_sessions": (
            _child_text(step_el, "maxSessions")
            or _child_text(step_el, "maxsessions")
            or _child_text(step_el, "sessions")
        ),
        "min_sessions": (
            _child_text(step_el, "minSessions")
            or _child_text(step_el, "minsessions")
        ),
        "pack_factor": (
            _child_text(step_el, "packFactor")
            or _child_text(step_el, "packfactor")
        ),
        "fill_record": (
            _child_text(step_el, "fillRecord")
            or _child_text(step_el, "fillrecord")
        ),
        "explicit_dates": _child_text(step_el, "explicit_dates"),
        "date_mask": _child_text(step_el, "dateMask") or _child_text(step_el, "datemask"),
        "date_format": (
            _child_text(step_el, "dateFormat")
            or _child_text(step_el, "dateformat")
        ),
        "time_format": (
            _child_text(step_el, "timeFormat")
            or _child_text(step_el, "timeformat")
        ),
        "timestamp_format": (
            _child_text(step_el, "timestampFormat")
            or _child_text(step_el, "timestampformat")
        ),
        "tpt_operator": (
            _child_text(step_el, "tptOperator")
            or _child_text(step_el, "tptoperator")
            or _child_text(step_el, "operator")
            or _child_text(step_el, "dataOperator")
            or _child_text(step_el, "dataoperator")
        ),
        "tbuild_path": (
            _child_text(step_el, "tbuildPath")
            or _child_text(step_el, "tbuildpath")
            or _child_text(step_el, "tbuild")
        ),
        "tpt_job_name": (
            _child_text(step_el, "jobName")
            or _child_text(step_el, "jobname")
            or _child_text(step_el, "tptJobName")
        ),
        "agent_host": (
            _child_text(step_el, "agentHost")
            or _child_text(step_el, "agenthost")
            or _child_text(step_el, "agent")
        ),
        "agent_port": (
            _child_text(step_el, "agentPort")
            or _child_text(step_el, "agentport")
        ),
        "key_fields": _parse_bulk_loader_key_fields(step_el),
        "fields": _parse_bulk_loader_fields(step_el),
    }
    if extras:
        cfg["extras"] = extras
    return cfg


def parse_step_metadata(step_el: ET.Element, step_type: str) -> dict[str, Any]:
    """Parse all nested XML for a step type into structured metadata."""
    st = (step_type or "").strip().lower().replace(" ", "")
    parsers: dict[str, Any] = {
        "calculator": lambda el: {"calculations": _metadata_value(parse_calculations(el))},
        "mergejoin": parse_merge_join_config,
        "joinrows": parse_join_rows_config,
        "joiner": parse_join_rows_config,
        "mergerows": parse_merge_rows_config,
        "mergerow": parse_merge_rows_config,
        "multimergejoin": parse_multiway_merge_join_config,
        "multiwaymergejoin": parse_multiway_merge_join_config,
        "multimerge": parse_multiway_merge_join_config,
        "sortedmerge": parse_sorted_merge_config,
        "xmljoin": parse_xml_join_config,
        "groupby": parse_group_by_config,
        "memorygroupby": parse_group_by_config,
        "analyticquery": parse_analytic_query_config,
        "samplerows": parse_sample_rows_config,
        "reservoirsampling": parse_reservoir_sampling_config,
        "univariatestats": parse_univariate_stats_config,
        "univariatestatistics": parse_univariate_stats_config,
        "stepsmetrics": parse_steps_metrics_config,
        "outputstepsmetrics": parse_steps_metrics_config,
        "valuemapper": parse_value_mapper_config,
        "filterrows": parse_filter_rows_config,
        "rowgenerator": parse_row_generator_config,
        "datagrid": parse_row_generator_config,
        "sequence": parse_sequence_config_dict,
        "addsequence": parse_sequence_config_dict,
        "checksum": parse_checksum_config,
        "addachecksum": parse_checksum_config,
        "numberrange": parse_number_range_config,
        "fieldschangesequence": parse_fields_change_sequence_config,
        "addvaluefieldschangingsequence": parse_fields_change_sequence_config,
        "closuregenerator": parse_closure_generator_config,
        "closure": parse_closure_generator_config,
        "getslavesequence": parse_get_slave_sequence_config,
        "getidfromslaveserver": parse_get_slave_sequence_config,
        "getidfromslave": parse_get_slave_sequence_config,
        "xslt": parse_xslt_config,
        "xsltransformation": parse_xslt_config,
        "xsltransform": parse_xslt_config,
        "textfileoutput": parse_text_file_output_config,
        "textfileoutputlegacy": parse_text_file_output_config,
        "tableinput": parse_table_input_config,
        "databaselookup": parse_database_lookup_config,
        "dblookup": parse_database_lookup_config,
        "streamlookup": parse_database_lookup_config,
        "dimensionlookup": parse_dimension_lookup_config,
        "dimensionlookupupdate": parse_dimension_lookup_config,
        "combinationlookup": parse_combination_lookup_config,
        "dbproc": parse_db_proc_config,
        "calldbproc": parse_db_proc_config,
        "calldbprocedure": parse_db_proc_config,
        "dbjoin": parse_db_join_config,
        "databasejoin": parse_db_join_config,
        "dynamicsqlrow": parse_dynamic_sql_row_config,
        "fileexists": parse_file_exists_config,
        "tableexists": parse_table_exists_config,
        "columnexists": parse_column_exists_config,
        "checkfilelocked": parse_check_file_locked_config,
        "fileslocked": parse_check_file_locked_config,
        "lockedfiles": parse_check_file_locked_config,
        "webserviceavailable": parse_webservice_available_config,
        "checkwebserviceavailable": parse_webservice_available_config,
        "http": parse_http_client_config,
        "httpclient": parse_http_client_config,
        "httpget": parse_http_client_config,
        "httppost": parse_http_post_config,
        "rest": parse_rest_client_config,
        "restclient": parse_rest_client_config,
        "webservice": parse_web_services_lookup_config,
        "webservicelookup": parse_web_services_lookup_config,
        "fuzzymatch": parse_fuzzy_match_config,
        "formula": parse_formula_config,
        "ifnull": parse_ifnull_config,
        "iffieldvaluenull": parse_ifnull_config,
        "iffieldvalueisnull": parse_ifnull_config,
        "clonerow": parse_clone_row_config,
        "clonerows": parse_clone_row_config,
        "nullif": parse_null_if_config,
        "delay": parse_delay_row_config,
        "delayrow": parse_delay_row_config,
        "changefileencoding": parse_change_file_encoding_config,
        "fileencoding": parse_change_file_encoding_config,
        "metastructure": parse_meta_structure_config,
        "stepmetastructure": parse_meta_structure_config,
        "metadatastructureofstream": parse_meta_structure_config,
        "writetolog": parse_write_to_log_config,
        "tablecompare": parse_table_compare_config,
        "zipfile": parse_zip_file_config,
        "processfiles": parse_process_files_config,
        "execprocess": parse_exec_process_config,
        "executeaprocess": parse_exec_process_config,
        "ssh": parse_ssh_config,
        "runsshcommands": parse_ssh_config,
        "mail": parse_mail_config,
        "sendmail": parse_mail_config,
        "syslogmessage": parse_syslog_config,
        "writetosyslog": parse_syslog_config,
        "sendmessagetosyslog": parse_syslog_config,
        "edi2xml": parse_edi_to_xml_config,
        "editoxml": parse_edi_to_xml_config,
        "constant": parse_constant_config,
        "addconstants": parse_constant_config,
        "addconstant": parse_constant_config,
        "selectvalues": parse_select_values_config,
        "setvalueconstant": parse_set_value_constant_config,
        "setfieldvaluetoaconstant": parse_set_value_constant_config,
        "setfieldvalueconstant": parse_set_value_constant_config,
        "setvaluefield": parse_set_value_field_config,
        "setfieldvalue": parse_set_value_field_config,
        "concatfields": parse_concat_fields_config,
        "addxml": parse_add_xml_config,
        "replaceinstring": parse_replace_in_string_config,
        "replacestring": parse_replace_in_string_config,
        "stringoperations": parse_string_operations_config,
        "stringcut": parse_string_cut_config,
        "stringscut": parse_string_cut_config,
        "sortrows": parse_sort_rows_config,
        "unique": parse_unique_rows_config,
        "uniquerows": parse_unique_rows_config,
        "uniquerowsbyhashset": parse_unique_rows_config,
        "uniquerowshashset": parse_unique_rows_config,
        "uniquehashset": parse_unique_rows_config,
        "rownormaliser": parse_normaliser_config,
        "rownormalizer": parse_normaliser_config,
        "normaliser": parse_normaliser_config,
        "rowdenormaliser": parse_denormaliser_config,
        "rowdenormalizer": parse_denormaliser_config,
        "denormaliser": parse_denormaliser_config,
        "flattener": parse_flattener_config,
        "rowflattener": parse_flattener_config,
        "splitfieldtorows": parse_split_field_to_rows_config,
        "fieldsplitter": parse_split_fields_config,
        "splitfields": parse_split_fields_config,
        "csvinput": parse_file_input_config,
        "textfileinput": parse_text_file_input_config,
        "oldtextfileinput": parse_text_file_input_config,
        "excelinput": parse_file_input_config,
        "jsoninput": parse_file_input_config,
        "getxmldata": parse_file_input_config,
        "xmlinput": parse_file_input_config,
        "xmlinputstream": parse_file_input_config,
        "fixedinput": parse_file_input_config,
        "fixedfileinput": parse_file_input_config,
        "gzipcsvinput": parse_file_input_config,
        "s3csvinput": parse_file_input_config,
        "propertyinput": parse_file_input_config,
        "yamlinput": parse_file_input_config,
        "loadfileinput": parse_file_input_config,
        "accessinput": parse_file_input_config,
        "salesforceinput": parse_salesforce_input_config,
        "getfilenames": parse_get_file_names_config,
        "gettablenames": parse_get_table_names_config,
        "randomvalue": parse_random_value_config,
        "insertupdate": parse_db_output_config,
        "update": parse_db_output_config,
        "delete": parse_db_output_config,
        "synchronizeaftermerge": parse_db_output_config,
        "synchronizemerge": parse_db_output_config,
        "tableoutput": parse_table_output_config,
        "jsonoutput": parse_json_output_config,
        "xmloutput": parse_xml_output_config,
        "excelwriter": parse_excel_writer_config,
        "typeexcelwriter": parse_excel_writer_config,
        "exceloutput": parse_excel_output_config,
        "microsoftexceloutput": parse_excel_output_config,
        "sapinput": parse_sap_input_config,
        "saperpinput": parse_sap_input_config,
        "salesforceinsert": parse_salesforce_output_config,
        "salesforceupdate": parse_salesforce_output_config,
        "salesforceupsert": parse_salesforce_output_config,
        "salesforcedelete": parse_salesforce_output_config,
        # Big Data file formats
        "avroinput": parse_avro_input_config,
        "avrofileinput": parse_avro_input_config,
        "avrooutput": parse_avro_output_config,
        "avrofileoutput": parse_avro_output_config,
        "parquetinput": parse_file_input_config,
        "parquetfileinput": parse_file_input_config,
        "parquetoutput": parse_file_output_config,
        "parquetfileoutput": parse_file_output_config,
        "orcinput": parse_file_input_config,
        "orcfileinput": parse_file_input_config,
        "orcoutput": parse_file_output_config,
        "orcfileoutput": parse_file_output_config,
        "deltaoutput": parse_file_output_config,
        "deltafileoutput": parse_file_output_config,
        "writeoutdelta": parse_file_output_config,
        "csvoutput": parse_file_output_config,
        "csvfileoutput": parse_file_output_config,
        "hadoopfileinput": parse_file_input_config,
        "hadoopfileinputplugin": parse_file_input_config,
        "hadoopfileoutputplugin": parse_file_output_config,
        "accessoutput": parse_file_output_config,
        "microsoftaccessoutput": parse_file_output_config,
        "propertyoutput": parse_file_output_config,
        "propertiesoutput": parse_file_output_config,
        "s3fileoutput": parse_file_output_config,
        "s3output": parse_file_output_config,
        "sqlfileoutput": parse_file_output_config,
        "microsoftexcelinput": parse_file_input_config,
        "microsoftexcelwriter": parse_excel_writer_config,
        "fixedwidthinput": parse_file_input_config,
        "gzipcsv": parse_file_input_config,
        "gzipcsvfileinput": parse_file_input_config,
        "s3csvfileinput": parse_file_input_config,
        "s3fileinput": parse_file_input_config,
        "propertiesinput": parse_file_input_config,
        "microsoftaccessinput": parse_file_input_config,
        # Advanced / transform helpers
        "systeminfo": parse_system_info_config,
        "rank": parse_rank_config_dict,
        "top": parse_top_n_config,
        "rowsfilter": parse_top_n_config,
        "limit": parse_top_n_config,
        "regexreplace": parse_replace_in_string_config,
        "replacenull": parse_replace_null_config,
        "splunkinput": parse_splunk_config,
        "splunkoutput": parse_splunk_config,
        "splunk": parse_splunk_config,
        # Extra I/O aliases + unsupported (shared file / stub metadata)
        "loadfile": parse_file_input_config,
        "loadfilecontentinmemory": parse_file_input_config,
        "getsubfolders": parse_get_file_names_config,
        "getsubfoldernames": parse_get_file_names_config,
        "getfilesrowscount": parse_get_file_names_config,
        "filesrowscount": parse_get_file_names_config,
        "generaterandomvalue": parse_random_value_config,
        "randomccnumbergenerator": parse_random_value_config,
        "generaterandomcreditcardnumbers": parse_random_value_config,
        "creditcardgenerator": parse_random_value_config,
        "staxxmlinput": parse_file_input_config,
        "xmlinputstreamstax": parse_file_input_config,
        "xmlpad": parse_xml_output_config,
        "xmlwriter": parse_xml_output_config,
        "shapefilereader": parse_file_input_config,
        "esrishapefile": parse_file_input_config,
        "esrishapefilereader": parse_file_input_config,
        "gisfileinput": parse_file_input_config,
        "dbfinput": parse_file_input_config,
        "xbaseinput": parse_file_input_config,
        "sasinput": parse_file_input_config,
        "ldapinput": parse_file_input_config,
        "ldapoutput": parse_file_output_config,
        "ldifinput": parse_file_input_config,
        "rssinput": parse_file_input_config,
        "rssoutput": parse_file_output_config,
        "hl7input": parse_file_input_config,
        "autodoc": parse_file_output_config,
        "autodocoutput": parse_file_output_config,
        "automaticdocumentationoutput": parse_file_output_config,
        "getrepositorynames": parse_file_input_config,
        "mailinput": parse_file_input_config,
        "emailmessagesinput": parse_file_input_config,
        "emailinput": parse_file_input_config,
        "cubeinput": parse_file_input_config,
        "cubeoutput": parse_file_output_config,
        "deserializefromfile": parse_file_input_config,
        "deserialisefromfile": parse_file_input_config,
        "serializetofile": parse_file_output_config,
        "serialisetofile": parse_file_output_config,
        "mondrianinput": parse_file_input_config,
        "olapinput": parse_file_input_config,
        "xmla": parse_file_input_config,
        "xmlainput": parse_file_input_config,
        "pentahoreporting": parse_file_output_config,
        "pentahoreportingoutput": parse_file_output_config,
        "reportexport": parse_file_output_config,
        "prptoutput": parse_file_output_config,
        # Big Data NoSQL
        "mongodbinput": parse_mongodb_input_config,
        "mongoinput": parse_mongodb_input_config,
        "mongodboutput": parse_mongodb_output_config,
        "mongooutput": parse_mongodb_output_config,
        # Streaming
        "recordsfromstream": parse_records_from_stream_config,
        "getrecordsfromstream": parse_records_from_stream_config,
        "kafkaconsumer": parse_kafka_consumer_config,
        "kafkaconsumerinput": parse_kafka_consumer_config,
        "kafkastreaminput": parse_kafka_consumer_config,
        "kafka": parse_kafka_consumer_config,
        "kafkaproducer": parse_kafka_producer_config,
        "kafkaproduceroutput": parse_kafka_producer_config,
        "jmsconsumer": parse_jms_consumer_config,
        "jmsconsumerinput": parse_jms_consumer_config,
        "activemqconsumer": parse_jms_consumer_config,
        "jmsproducer": parse_jms_producer_config,
        "jmsproduceroutput": parse_jms_producer_config,
        "activemqproducer": parse_jms_producer_config,
        "mqttconsumer": parse_mqtt_consumer_config,
        "mqttconsumerinput": parse_mqtt_consumer_config,
        "mqttclient": parse_mqtt_consumer_config,
        "mqttproducer": parse_mqtt_producer_config,
        "mqttproduceroutput": parse_mqtt_producer_config,
        # Inline
        "injector": parse_injector_config,
        "socketreader": parse_socket_reader_config,
        "socketwriter": parse_socket_writer_config,
        # Flow
        "abort": parse_abort_config,
        "append": parse_append_streams_config,
        "appendstreams": parse_append_streams_config,
        "blockuntilstepsfinish": parse_block_until_steps_finish_config,
        "blockthisstepuntilstepsfinish": parse_block_until_steps_finish_config,
        "blockingstep": parse_blocking_step_config,
        "block": parse_blocking_step_config,
        "detectemptystream": parse_detect_empty_stream_config,
        "detectempty": parse_detect_empty_stream_config,
        "dummy": parse_dummy_config,
        "dummytrans": parse_dummy_config,
        "dummydonothing": parse_dummy_config,
        "metainject": parse_meta_inject_config,
        "etlmetadatainjection": parse_meta_inject_config,
        "identifylastrow": parse_identify_last_row_config,
        "identifylastrowinastream": parse_identify_last_row_config,
        "javafilter": parse_java_filter_config,
        "jobexecutor": parse_job_executor_config,
        "prioritystream": parse_prioritize_streams_config,
        "prioritizestreams": parse_prioritize_streams_config,
        "singlethreader": parse_single_threader_config,
        "switchcase": parse_switch_case_config,
        "transexecutor": parse_trans_executor_config,
        "transformationexecutor": parse_trans_executor_config,
        # Mapping (sub-transformation)
        "mapping": parse_mapping_config,
        "mappingsubtransformation": parse_mapping_config,
        "simplemapping": parse_simple_mapping_config,
        "simplemappingsubtransformation": parse_simple_mapping_config,
        "mappinginput": parse_mapping_input_config,
        "mappinginputspecification": parse_mapping_input_config,
        "mappingoutput": parse_mapping_output_config,
        "mappingoutputspecification": parse_mapping_output_config,
        # Job category
        "rowstoresult": parse_rows_to_result_config,
        "copyrowstoresult": parse_rows_to_result_config,
        "rowsfromresult": parse_rows_from_result_config,
        "getrowsfromresult": parse_rows_from_result_config,
        "filesfromresult": parse_files_from_result_config,
        "getfilesfromresult": parse_files_from_result_config,
        "filestoresult": parse_files_to_result_config,
        "setfilesinresult": parse_files_to_result_config,
        "setfilestoresult": parse_files_to_result_config,
        "setvariable": parse_set_variable_config,
        "setvariables": parse_set_variable_config,
        "getvariable": parse_get_variable_config,
        "getvariables": parse_get_variable_config,
        # Validation
        "creditcardvalidator": parse_credit_card_validator_config,
        "creditcard": parse_credit_card_validator_config,
        "validator": parse_data_validator_config,
        "datavalidator": parse_data_validator_config,
        "mailvalidator": parse_mail_validator_config,
        "emailvalidator": parse_mail_validator_config,
        "xsdvalidator": parse_xsd_validator_config,
        "xmlschemavalidator": parse_xsd_validator_config,
        # Scripting
        "execsql": parse_exec_sql_config,
        "executesql": parse_exec_sql_config,
        "sql": parse_exec_sql_config,
        "execsqlrow": parse_exec_sql_row_config,
        "executerowsqlscript": parse_exec_sql_row_config,
        "executerowsql": parse_exec_sql_row_config,
        "scriptvaluemod": parse_javascript_value_config,
        "javascriptvalue": parse_javascript_value_config,
        "modifiedjavascriptvalue": parse_javascript_value_config,
        "regexeval": parse_regex_eval_config,
        "regularexpression": parse_regex_eval_config,
        "ruleaccumulator": parse_rules_accumulator_config,
        "rulesaccumulator": parse_rules_accumulator_config,
        "ruleexecutor": parse_rules_executor_config,
        "rulesexecutor": parse_rules_executor_config,
        "userdefinedjavaclass": parse_user_defined_java_class_config,
        "userdefinedjavaexpression": parse_user_defined_java_expression_config,
        # Cryptography
        "pgpencryptstream": parse_pgp_encrypt_stream_config,
        "pgpencrypt": parse_pgp_encrypt_stream_config,
        "pgpdecryptstream": parse_pgp_decrypt_stream_config,
        "pgpdecrypt": parse_pgp_decrypt_stream_config,
        "secretkeygenerator": parse_secret_key_generator_config,
        "secretkeygen": parse_secret_key_generator_config,
        "symmetriccryptotrans": parse_symmetric_crypto_config,
        "symmetriccrypto": parse_symmetric_crypto_config,
        "symmetriccryptography": parse_symmetric_crypto_config,
        # Experimental
        "sftpput": parse_sftp_put_config,
        "sftpputfile": parse_sftp_put_config,
        "putafilewithsftp": parse_sftp_put_config,
        "putsftp": parse_sftp_put_config,
        # Pentaho Server
        "callendpoint": parse_call_endpoint_config,
        "callendpointstep": parse_call_endpoint_config,
        "getsessionvariable": parse_get_session_variable_config,
        "getsessionvariables": parse_get_session_variable_config,
        "getsessionvariablestep": parse_get_session_variable_config,
        "setsessionvariable": parse_set_session_variable_config,
        "setsessionvariables": parse_set_session_variable_config,
        "setsessionvariablestep": parse_set_session_variable_config,
        "script": parse_experimental_script_config,
        "scriptvalues": parse_experimental_script_config,
        "experimentalscript": parse_experimental_script_config,
        # Bulk Loading
        "gpbulkloader": parse_bulk_loader_config,
        "greenplumbulkloader": parse_bulk_loader_config,
        "greenplumload": parse_bulk_loader_config,
        "greenplumloader": parse_bulk_loader_config,
        "gpload": parse_bulk_loader_config,
        "infobrightloader": parse_bulk_loader_config,
        "infobrightbulkloader": parse_bulk_loader_config,
        "infobright": parse_bulk_loader_config,
        "vectorwisebulkloader": parse_bulk_loader_config,
        "ingresvectorwisebulkloader": parse_bulk_loader_config,
        "vwbulkloader": parse_bulk_loader_config,
        "ingresbulkloader": parse_bulk_loader_config,
        "vectorwiseloader": parse_bulk_loader_config,
        "monetdbbulkloader": parse_bulk_loader_config,
        "monetdbloader": parse_bulk_loader_config,
        "monetdbbulk": parse_bulk_loader_config,
        "mysqlbulkloader": parse_bulk_loader_config,
        "mysqlloader": parse_bulk_loader_config,
        "loaddatainfile": parse_bulk_loader_config,
        "orabulkloader": parse_bulk_loader_config,
        "oraclebulkloader": parse_bulk_loader_config,
        "oracleloader": parse_bulk_loader_config,
        "sqlldr": parse_bulk_loader_config,
        "pgbulkloader": parse_bulk_loader_config,
        "postgresqlbulkloader": parse_bulk_loader_config,
        "postgresbulkloader": parse_bulk_loader_config,
        "psqlbulkloader": parse_bulk_loader_config,
        "terafast": parse_bulk_loader_config,
        "teradatafastloadbulkloader": parse_bulk_loader_config,
        "terafastbulkloader": parse_bulk_loader_config,
        "teradatafastload": parse_bulk_loader_config,
        "fastload": parse_bulk_loader_config,
        "teradatabulkloader": parse_bulk_loader_config,
        "teradatatptbulkloader": parse_bulk_loader_config,
        "tptbulkloader": parse_bulk_loader_config,
        "teratpt": parse_bulk_loader_config,
        "teradatatpt": parse_bulk_loader_config,
        "verticabulkloader": parse_bulk_loader_config,
        "verticaloader": parse_bulk_loader_config,
        "verticacopy": parse_bulk_loader_config,
    }
    parser = parsers.get(st)
    if parser is None:
        return {}
    return parser(step_el)


def is_structured_step_type(step_type: str) -> bool:
    """Return True when the step type has a dedicated nested XML parser."""
    return (step_type or "").strip().lower().replace(" ", "") in _STRUCTURED_STEP_TYPES


def aggregate_to_spark(agg: str, col_name: str) -> str:
    """Map Pentaho aggregate name to PySpark agg expression."""
    a = (agg or "SUM").upper()
    c = f'col("{col_name}")'
    mapping = {
        "SUM": f"_sum({c})",
        "AVERAGE": f"avg({c})",
        "AVG": f"avg({c})",
        "MIN": f"_min({c})",
        "MAX": f"_max({c})",
        "COUNT": f"count({c})",
        "COUNT_ALL": "count(lit(1))",
        "COUNT ANY": "count(lit(1))",
        "COUNT_ANY": "count(lit(1))",
        "COUNT DISTINCT": f"countDistinct({c})",
        "COUNT_DISTINCT": f"countDistinct({c})",
        "FIRST": f"first({c})",
        "LAST": f"last({c})",
        "FIRST_INCL_NULL": f"first({c}, ignorenulls=True)",
        "LAST_INCL_NULL": f"last({c}, ignorenulls=True)",
        "MEDIAN": f"expr('percentile_approx(`{col_name}`, 0.5)')",
        "PERCENTILE": f"expr('percentile_approx(`{col_name}`, 0.5)')",
    }
    return mapping.get(a, f"sum({c})")


def system_info_expr(sys_type: str) -> str:
    """Map Pentaho SystemInfo type codes to PySpark expressions."""
    t = (sys_type or "").lower().replace(" ", "_")
    mapping = {
        "system_date_(fixed)": "current_date()",
        "system_date_(variable)": "current_date()",
        "system_datetime_(fixed)": "current_timestamp()",
        "system_datetime_(variable)": "current_timestamp()",
        "system_date": "current_date()",
        "system_datetime": "current_timestamp()",
        "job_name": "lit(spark.sparkContext.appName)",
        "transformation_name": "lit('transformation')",
        "step_name": "lit('step')",
        "hostname": "lit(spark.sparkContext.getConf().get('spark.driver.host', 'localhost'))",
        "hostname_real": "lit(spark.sparkContext.getConf().get('spark.driver.host', 'localhost'))",
        "ip_address": "lit('0.0.0.0')",
        "job_run_id": "expr('uuid()')",
        "batch_id": "expr('uuid()')",
        "parent_job_id": "lit('')",
        "parent_transformation_id": "lit('')",
        "system_info_user": "lit(spark.sparkContext.sparkUser())",
        "user_name": "lit(spark.sparkContext.sparkUser())",
        "username": "lit(spark.sparkContext.sparkUser())",
        "kettle_version": "lit('pyspark')",
        "kettle_build_version": "lit('pyspark')",
        "kettle_build_date": "current_date()",
        "internal_job_run_id": "expr('uuid()')",
        "internal_transformation_run_id": "expr('uuid()')",
        "current_pid": "lit(0)",
        "jvm_max_memory": "lit(0)",
        "jvm_total_memory": "lit(0)",
        "jvm_free_memory": "lit(0)",
    }
    for key, expr in mapping.items():
        if key in t or t in key:
            return expr
    if "date" in t and "time" not in t:
        return "current_date()"
    if "time" in t or "datetime" in t:
        return "current_timestamp()"
    if "uuid" in t or "id" in t:
        return "expr('uuid()')"
    return "lit('')"

