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

    if st in ("csvinput", "excelinput", "textfileinput", "jsoninput", "xmlinput", "getxmldata"):
        return cols

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

    if st == "groupby":
        keys = set(parsed.get("group_keys", []))
        aggs = {a.get("name") for a in parsed.get("aggregates", []) if a.get("name")}
        return keys | aggs

    if st in ("filterrows", "sortrows", "replacenull", "valuemapper", "formula"):
        cols.update(columns_written(code))
        return cols

    if st in ("mergejoin", "joinrows", "joiner", "streamlookup", "databaselookup"):
        return cols | columns_written(code)

    cols.update(columns_written(code))
    return cols


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
        "jsoninput", "textfileinput", "excelinput", "xmlinput", "getxmldata",
        "parquetinput", "orcinput", "avroinput",
    ):
        return [], []

    refs = columns_referenced("\n".join(code_lines))
    missing = sorted(refs - input_columns)
    if not missing:
        return [], []

    if st == "selectvalues":
        return [
            f"SelectValues references columns not present upstream: {', '.join(missing)}"
        ], []

    if st in ("filterrows", "calculator", "formula", "replacenull", "stringoperations", "ifnull"):
        return [], [
            f"Column lineage: upstream schema may not include: {', '.join(missing)}"
        ]

    return [], [f"Referenced columns not in upstream lineage: {', '.join(missing)}"]
