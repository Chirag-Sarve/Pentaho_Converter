"""Column lineage tracking and Pentaho variable substitution."""

from __future__ import annotations

import re
from typing import Any

from .metadata_models import ColumnLineage, ColumnSchema, LineageValidationResult
from .metadata_propagation import (
    get_converter_metadata,
    infer_lineage_from_metadata,
    merge_input_lineage,
    propagate_step_metadata,
    update_lineage_map,
    validate_lineage_before_convert,
)
from .validation.code_checks import columns_referenced, columns_written

__all__ = [
    "substitute_pentaho_variables",
    "infer_output_columns",
    "validate_column_lineage",
    "ColumnLineage",
    "ColumnSchema",
    "propagate_step_metadata",
    "get_converter_metadata",
    "infer_lineage_from_metadata",
    "validate_lineage_before_convert",
]


def substitute_pentaho_variables(text: str, parameters: dict[str, str]) -> str:
    """Replace ${VARIABLE} placeholders with transformation parameter values."""
    if not text or "${" not in text:
        return text

    def _repl(match: re.Match) -> str:
        key = match.group(1).strip()
        return parameters.get(key, match.group(0))

    return re.sub(r"\$\{([^}]+)\}", _repl, text)


def _columns_from_sql(sql: str) -> set[str]:
    """Extract output column names from a simple SQL SELECT list."""
    if not sql or not sql.strip():
        return set()

    text = re.sub(r"--[^\n]*", "", sql)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    match = re.search(r"\bSELECT\b\s+(.*?)\s+\bFROM\b", text, re.IGNORECASE | re.DOTALL)
    if not match:
        return set()

    clause = match.group(1).strip()
    if not clause or clause == "*" or clause.startswith("*"):
        return set()

    columns: set[str] = set()
    for raw in clause.split(","):
        item = raw.strip()
        if not item:
            continue

        alias_match = re.search(r"\bAS\s+([\"'`]?)(\w+)\1\s*$", item, re.IGNORECASE)
        if alias_match:
            columns.add(alias_match.group(2))
            continue

        token = item.split()[-1].strip().strip("\"'`")
        if "." in token:
            token = token.rsplit(".", 1)[-1]
        if re.fullmatch(r"\w+", token):
            columns.add(token)

    return columns


def _sql_from_generated_code(code: str) -> str:
    match = re.search(r"""spark\.sql\(\s*(['"])(.*?)\1\s*\)""", code, re.DOTALL)
    return match.group(2) if match else ""


def infer_output_columns(
    step_type: str,
    parsed: dict[str, Any],
    input_columns: set[str],
    code_lines: list[str] | None = None,
) -> set[str]:
    """Infer output column set after a step (for downstream lineage)."""
    st = step_type.strip().lower().replace(" ", "")
    code = "\n".join(code_lines or [])
    cols = set(input_columns)

    if st in ("rowgenerator", "datagrid"):
        names = {f.get("name") for f in parsed.get("fields", []) if f.get("name")}
        if parsed.get("columns"):
            names.update(c for c in parsed["columns"] if c)
        return names or cols

    if st == "tableinput":
        written = columns_written(code)
        if written:
            return written
        sql = (
            parsed.get("sql_resolved")
            or parsed.get("sql")
            or _sql_from_generated_code(code)
        )
        sql_cols = _columns_from_sql(str(sql))
        return sql_cols or cols

    if st in (
        "csvinput", "excelinput", "textfileinput", "oldtextfileinput", "jsoninput", "xmlinput", "getxmldata",
        "fixedinput", "fixedfileinput", "gzipcsvinput", "s3csvinput", "yamlinput",
        "propertyinput", "xmlinputstream", "loadfileinput", "accessinput", "sasinput",
        "xbaseinput", "shapefilereader", "getfilenames", "getsubfolders",
        "getfilesrowscount", "gettablenames", "randomvalue", "randomccnumbergenerator",
        "salesforceinput", "ldapinput", "ldifinput", "rssinput", "hl7input",
        "kafkaconsumer", "kafkaconsumerinput", "kafkastreaminput", "kafka",
        "mqttconsumer", "mqttconsumerinput", "mqttclient",
        "jmsconsumer", "jmsconsumerinput", "activemqconsumer",
        "recordsfromstream", "getrecordsfromstream",
        "sapinput", "saperpinput",
    ):
        names = {f.get("name") for f in parsed.get("fields", []) if isinstance(f, dict) and f.get("name")}
        # SAP fields often use new_name / field_name rather than name.
        for f in parsed.get("fields", []) if isinstance(parsed.get("fields"), list) else []:
            if isinstance(f, dict):
                if f.get("new_name"):
                    names.add(f["new_name"])
                if f.get("field_name"):
                    names.add(f["field_name"])
        names.update(c for c in (parsed.get("output_columns") or []) if c)
        written = columns_written(code) if code else set()
        return names or written or cols

    if st == "constant":
        for c in parsed.get("constants", []):
            if c.get("name"):
                cols.add(c["name"])
        return cols

    if st == "calculator":
        for calc in parsed.get("calculations", []):
            if calc.get("field_name"):
                cols.add(calc["field_name"])
            if calc.get("remove"):
                for key in ("field_a", "field_b", "field_c"):
                    if calc.get(key):
                        cols.discard(calc[key])
        return cols

    if st == "selectvalues":
        out = parsed.get("output_columns") or parsed.get("select_columns") or []
        return {c for c in out if c} or cols

    if st in ("setvalueconstant", "setvaluefield"):
        return cols | columns_written(code)

    if st == "concatfields":
        target = parsed.get("target_field_name")
        if target:
            cols.add(target)
        if parsed.get("remove_selected_fields"):
            for f in parsed.get("fields") or []:
                if f.get("name"):
                    cols.discard(f["name"])
        return cols

    if st == "addxml":
        value_name = parsed.get("value_name")
        if value_name:
            cols.add(value_name)
        return cols

    if st in ("replaceinstring", "replacestring", "stringoperations", "stringcut"):
        cols.update(columns_written(code))
        return cols

    if st == "groupby":
        keys = set(parsed.get("group_keys", []))
        aggs = {a.get("name") for a in parsed.get("aggregates", []) if a.get("name")}
        return keys | aggs

    if st == "memorygroupby":
        keys = set(parsed.get("group_keys", []))
        aggs = {a.get("name") for a in parsed.get("aggregates", []) if a.get("name")}
        return keys | aggs

    if st == "analyticquery":
        cols.update(columns_written(code))
        for field in parsed.get("analytic_fields") or parsed.get("fields") or []:
            name = field.get("name") if isinstance(field, dict) else None
            if name:
                cols.add(name)
        return cols

    if st in ("univariatestats", "univariatestatistics", "stepsmetrics", "outputstepsmetrics"):
        written = columns_written(code)
        return written if written else cols

    if st in ("filterrows", "sortrows", "replacenull", "valuemapper", "formula",
              "javafilter", "switchcase", "dummy", "dummytrans", "dummydonothing",
              "append", "appendstreams", "blockingstep", "block",
              "detectemptystream", "detectempty",
              "samplerows", "reservoirsampling",
              "execsql", "executesql", "sql", "execsqlrow", "executerowsqlscript",
              "executerowsql", "scriptvaluemod", "javascriptvalue",
              "modifiedjavascriptvalue", "regexeval", "regularexpression",
              "ruleaccumulator", "rulesaccumulator", "ruleexecutor", "rulesexecutor",
              "userdefinedjavaclass", "userdefinedjavaexpression",
              "mapping", "mappingsubtransformation",
              "simplemapping", "simplemappingsubtransformation",
              "mappingoutput", "mappingoutputspecification"):
        cols.update(columns_written(code))
        return cols

    if st in ("mappinginput", "mappinginputspecification"):
        names = set(parsed.get("field_names") or [])
        for field in parsed.get("fields") or []:
            if isinstance(field, dict) and field.get("name"):
                names.add(field["name"])
        if names:
            if parsed.get("select_unspecified") or parsed.get("include_unspecified_fields"):
                return cols | names
            return names
        cols.update(columns_written(code))
        return cols

    if st in ("identifylastrow", "identifylastrowinastream"):
        result_field = parsed.get("result_field") or "result"
        cols.add(result_field)
        cols.update(columns_written(code))
        return cols

    if st in (
        "unique", "uniquerows", "uniquerowsbyhashset",
        "uniquerowshashset", "uniquehashset",
    ):
        cols.update(columns_written(code))
        return cols

    if st in ("rownormaliser", "rownormalizer", "normaliser"):
        written = columns_written(code)
        return written if written else cols

    if st in ("rowdenormaliser", "rowdenormalizer", "denormaliser"):
        keys = set(parsed.get("group_fields") or [])
        targets = {
            (t.get("target_name") or t.get("field_name"))
            for t in (parsed.get("target_fields") or [])
            if isinstance(t, dict) and (t.get("target_name") or t.get("field_name"))
        }
        result = keys | targets
        return result if result else columns_written(code)

    if st in ("flattener", "rowflattener"):
        field_name = parsed.get("field_name") or ""
        targets = set(parsed.get("target_fields") or [])
        if field_name:
            cols.discard(field_name)
        cols.update(targets)
        cols.update(columns_written(code))
        return cols

    if st == "splitfieldtorows":
        new_field = parsed.get("new_field") or parsed.get("split_field")
        if new_field:
            cols.add(new_field)
        if parsed.get("include_row_number"):
            cols.add(parsed.get("row_number_field") or "rownr")
        cols.update(columns_written(code))
        return cols

    if st in ("fieldsplitter", "splitfields"):
        split_field = parsed.get("split_field") or ""
        if split_field:
            cols.discard(split_field)
        for f in parsed.get("fields") or []:
            if isinstance(f, dict) and f.get("name"):
                cols.add(f["name"])
        cols.update(columns_written(code))
        return cols

    if st in (
        "mergejoin", "joinrows", "joiner", "mergerows", "mergerow",
        "multimergejoin", "multiwaymergejoin", "multimerge", "sortedmerge",
        "xmljoin", "streamlookup", "databaselookup",
        "dblookup", "dbjoin", "databasejoin", "fuzzymatch",
    ):
        cols = set(input_columns) | columns_written(code)
        # Honour renames/drops so downstream required schemas stay accurate.
        for old, new in re.findall(
            r'withColumnRenamed\(["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']',
            code,
        ):
            cols.discard(old)
            cols.add(new)
        for dropped in re.findall(r'\.drop\(["\']([^"\']+)["\']', code):
            cols.discard(dropped)
        return cols

    if st in ("closuregenerator", "closure"):
        # Output schema is only parent, child, distance (Pentaho getFields).
        out = {
            parsed.get("parent_id_field") or "",
            parsed.get("child_id_field") or "",
            parsed.get("distance_field") or "distance",
        }
        out.discard("")
        return out or columns_written(code)

    if st in ("getslavesequence", "getidfromslaveserver", "getidfromslave"):
        value_name = parsed.get("value_name") or "id"
        cols.add(value_name)
        cols.update(columns_written(code))
        return cols

    if st in ("xslt", "xsltransformation", "xsltransform"):
        result_field = parsed.get("result_field") or "result"
        cols.add(result_field)
        cols.update(columns_written(code))
        return cols

    # ---- Utility steps ----
    if st in ("clonerow", "clonerows"):
        if parsed.get("add_clone_flag"):
            cols.add(parsed.get("clone_flag_field") or "cloneflag")
        if parsed.get("add_clone_num"):
            cols.add(parsed.get("clone_num_field") or "clonenum")
        cols.update(columns_written(code))
        return cols

    if st in ("nullif", "ifnull", "iffieldvaluenull", "iffieldvalueisnull", "delay", "delayrow", "writetolog"):
        cols.update(columns_written(code))
        return cols

    if st in ("metastructure", "stepmetastructure", "metadatastructureofstream"):
        out = {
            parsed.get("position_field") or "Position",
            parsed.get("fieldname_field") or "Fieldname",
            parsed.get("comments_field") or "Comments",
            parsed.get("type_field") or "Type",
            parsed.get("length_field") or "Length",
            parsed.get("precision_field") or "Precision",
            parsed.get("origin_field") or "Origin",
        }
        if parsed.get("output_rowcount"):
            out.add(parsed.get("rowcount_field") or "rowcount")
        return out

    if st == "tablecompare":
        cols.update(columns_written(code))
        for key in (
            "nr_errors_field", "nr_records_reference_field", "nr_records_compare_field",
            "key_desc_field", "value_reference_field", "value_compare_field",
        ):
            if parsed.get(key):
                cols.add(parsed[key])
        cols.add("_tc_diff")
        return cols

    if st in ("execprocess", "executeaprocess"):
        cols.add(parsed.get("output_field") or "outputLine")
        cols.add(parsed.get("error_field") or "errorLine")
        cols.add(parsed.get("exit_value_field") or "exitValue")
        cols.update(columns_written(code))
        return cols

    if st in ("ssh", "runsshcommands"):
        cols.add(parsed.get("stdout_field") or "stdOut")
        cols.add(parsed.get("stderr_field") or "stdErr")
        cols.update(columns_written(code))
        return cols

    if st in ("edi2xml", "editoxml"):
        cols.add(parsed.get("output_field") or "xml")
        cols.update(columns_written(code))
        return cols

    if st in (
        "changefileencoding", "fileencoding", "zipfile", "processfiles",
        "mail", "sendmail", "syslogmessage", "writetosyslog", "sendmessagetosyslog",
    ):
        # Side-effect utilities: stream passthrough
        return cols

    cols.update(columns_written(code))
    return cols


# Generator-created helper columns — never report as lineage gaps.
_GENERATED_HELPER_PREFIXES = (
    "_sort_",
    "_calc_",
    "_tmp_",
    "_vm_",
    "_sys_",
    "_join_",
    "_gb_",
    "_tfi_",
    "_out_",
    "_mapped_",
    "_cc_",
    "_mail_",
    "_xsd_",
)


def _is_generated_helper_column(name: str) -> bool:
    text = (name or "").strip()
    if not text:
        return False
    return text.startswith(_GENERATED_HELPER_PREFIXES)


def validate_column_lineage(
    code_lines: list[str],
    input_columns: set[str],
    step_type: str,
) -> tuple[list[str], list[str]]:
    """Return (errors, warnings) for column references vs upstream lineage."""
    if not input_columns:
        return [], []

    st = step_type.strip().lower().replace(" ", "")
    if st in (
        "tableinput", "csvinput", "rowgenerator", "datagrid",
        "jsoninput", "textfileinput", "oldtextfileinput", "excelinput", "xmlinput", "getxmldata",
        "parquetinput", "orcinput", "avroinput", "avrofileinput",
        "mongodbinput", "mongoinput",
        "fixedinput", "fixedfileinput", "gzipcsvinput", "s3csvinput", "yamlinput",
        "propertyinput", "xmlinputstream", "loadfileinput", "accessinput", "sasinput",
        "xbaseinput", "shapefilereader", "getfilenames", "getsubfolders",
        "getfilesrowscount", "gettablenames", "randomvalue", "randomccnumbergenerator",
        "salesforceinput", "ldapinput", "ldifinput", "rssinput", "hl7input",
        "systeminfo", "cubeinput", "mondrianinput", "olapinput", "mailinput",
        "getrepositorynames",
        "sapinput", "saperpinput",
    ):
        return [], []

    refs = columns_referenced("\n".join(code_lines))
    missing = sorted(
        name for name in (refs - input_columns) if not _is_generated_helper_column(name)
    )
    if not missing:
        return [], []

    if st == "selectvalues":
        return [], [
            f"SelectValues references columns not present upstream: {', '.join(missing)}"
        ]

    if st in ("filterrows", "calculator", "formula", "replacenull", "stringoperations", "ifnull",
              "iffieldvaluenull", "iffieldvalueisnull", "nullif",
              "javafilter", "switchcase", "identifylastrow", "identifylastrowinastream",
              "regexeval", "regularexpression", "scriptvaluemod",
              "userdefinedjavaexpression", "execsql", "execsqlrow"):
        return [], [
            f"Column lineage: upstream schema may not include: {', '.join(missing)}"
        ]

    return [], [f"Referenced columns not in upstream lineage: {', '.join(missing)}"]
