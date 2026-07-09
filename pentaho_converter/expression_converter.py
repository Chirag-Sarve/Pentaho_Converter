"""Pentaho formula/expression helpers for PySpark conversion."""

from __future__ import annotations

import re


def convert_formula(expr: str) -> str:
    """Best-effort conversion of Pentaho formula syntax to PySpark column expressions."""
    if not expr or not expr.strip():
        return "lit(None)"

    result = expr.strip()
    # Pentaho field references: [FieldName]
    result = re.sub(r"\[([^\]]+)\]", lambda m: f'col("{m.group(1).strip()}")', result)
    replacements = [
        (r"\bIF\s*\(", "when("),
        (r"\bAND\b", " & "),
        (r"\bOR\b", " | "),
        (r"\bNOT\b", "~"),
        (r"\bNULL\b", "None"),
        (r"\bTRUE\b", "True"),
        (r"\bFALSE\b", "False"),
        (r"\bCONCAT\s*\(", "concat("),
        (r"\bUPPER\s*\(", "upper("),
        (r"\bLOWER\s*\(", "lower("),
        (r"\bTRIM\s*\(", "trim("),
        (r"\bLENGTH\s*\(", "length("),
        (r"\bSUBSTR\s*\(", "substring("),
        (r"\bROUND\s*\(", "round("),
        (r"\bABS\s*\(", "abs("),
        (r"\bISNULL\s*\(", "isnull("),
        (r"\bNVL\s*\(", "coalesce("),
    ]
    for pattern, repl in replacements:
        result = re.sub(pattern, repl, result, flags=re.IGNORECASE)

    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", result):
        result = f'col("{result}")'

    if "col(" in result or "when(" in result or "concat(" in result:
        return result
    return f"expr({result!r})"


def convert_condition(condition: str) -> str:
    """Convert a filter condition to a PySpark filter expression."""
    if not condition.strip():
        return "lit(True)"
    converted = convert_formula(condition)
    if converted.startswith("expr("):
        return converted
    return f"expr({condition!r})"
