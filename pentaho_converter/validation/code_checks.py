"""Static checks on generated PySpark code fragments."""

from __future__ import annotations

import ast
import builtins as _builtins
import re

_PLACEHOLDER_PATTERNS = (
    r"_placeholder",
    r"placeholder STRING",
    r"lit\(None\)\s*#\s*",
    r"pass\s*#\s*",
)

_INCOMPLETE_PATTERNS = (
    r"jdbc:\.\.\.",
    r"lit\(''\)\s*#\s*UDJC",
)


def validate_python_fragment(code_lines: list[str]) -> tuple[bool, list[str]]:
    """Return (syntax_ok, errors) for a list of code lines."""
    errors: list[str] = []
    body = "\n".join(code_lines)
    if not body.strip():
        errors.append("No code generated for step.")
        return False, errors

    try:
        wrapped = "def _validate_step():\n" + "\n".join(
            f"    {line}" if line.strip() else "" for line in code_lines
        )
        ast.parse(wrapped)
    except SyntaxError as exc:
        errors.append(f"Generated code has syntax error: {exc.msg} (line {exc.lineno})")
        return False, errors

    for pattern in _PLACEHOLDER_PATTERNS:
        if re.search(pattern, body, re.IGNORECASE):
            errors.append(f"Generated code contains placeholder pattern: {pattern}")

    for pattern in _INCOMPLETE_PATTERNS:
        if re.search(pattern, body):
            errors.append(f"Generated code appears incomplete: {pattern}")

    undefined = _find_undefined_names(wrapped)
    if undefined:
        allowed = {
            "spark", "col", "lit", "when", "expr", "count", "coalesce", "broadcast",
            "upper", "lower", "trim", "ltrim", "rtrim", "initcap", "length",
            "substring", "round", "abs", "sqrt", "ceil", "floor", "pow",
            "concat", "isnull", "regexp_replace", "explode", "array",
            "to_date", "to_timestamp", "datediff", "date_add", "add_months",
            "year", "month", "dayofmonth", "dayofweek", "weekofyear",
            "current_date", "current_timestamp",
            "row_number", "rank", "dense_rank", "monotonically_increasing_id",
            "countDistinct", "first", "last", "levenshtein", "sum", "_sum", "avg", "max", "_max", "min", "_min",
            "Window", "True", "False", "None", "int", "float", "str",
            "TARGET_CATALOG", "TARGET_SCHEMA",
        }
        allowed.update(name for name in dir(_builtins) if not name.startswith("_"))
        bad = [n for n in undefined if n not in allowed and not n.startswith("df_")
               and not n.startswith("_w_") and not n.startswith("_lkp_")
               and not n.startswith("_empty_flag_") and not n.startswith("_sync_")
               and not n.startswith("_target_")]
        if bad:
            errors.append(f"Possibly undefined names in generated code: {', '.join(sorted(bad)[:5])}")

    return len(errors) == 0, errors


def _find_undefined_names(source: str) -> set[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()

    assigned: set[str] = set()
    used: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            if isinstance(node.ctx, ast.Store):
                assigned.add(node.id)
            elif isinstance(node.ctx, ast.Load):
                used.add(node.id)

    builtins_set = set(dir(_builtins))
    return used - assigned - builtins_set


def columns_referenced(code: str) -> set[str]:
    """Extract col('name') references from generated code."""
    return set(re.findall(r'col\(["\']([^"\']+)["\']\)', code))


def columns_written(code: str) -> set[str]:
    """Extract withColumn('name', ...) output columns."""
    written = set(re.findall(r'withColumn\(["\']([^"\']+)["\']', code))
    written.update(re.findall(r'\.alias\(["\']([^"\']+)["\']', code))
    return written
