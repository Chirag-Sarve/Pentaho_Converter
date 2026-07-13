"""Pentaho formula/expression helpers for PySpark conversion."""

from __future__ import annotations

import html
import re


def _split_pentaho_args(content: str, sep: str = ";") -> list[str]:
    """Split Pentaho function arguments by sep, respecting parentheses and quotes."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_string = False
    string_char = ""

    for ch in content:
        if in_string:
            current.append(ch)
            if ch == string_char:
                in_string = False
            continue
        if ch in ('"', "'"):
            in_string = True
            string_char = ch
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == sep and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

    parts.append("".join(current).strip())
    return parts


def _preprocess_fragment(fragment: str) -> str:
    """Apply non-IF Pentaho → PySpark rewrites to one expression fragment."""
    result = fragment.strip()
    if not result:
        return result

    result = re.sub(r"\[([^\]]+)\]", lambda m: f'col("{m.group(1).strip()}")', result)
    replacements = [
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

    return result


def _convert_pentaho_ifs(expr: str) -> str:
    """Convert Pentaho IF(cond; true; false) to PySpark when().otherwise() chains."""
    pattern = re.compile(r"\bIF\s*\(", re.IGNORECASE)
    match = pattern.search(expr)
    if not match:
        return _preprocess_fragment(expr)

    start = match.end()
    depth = 1
    index = start
    while index < len(expr) and depth > 0:
        if expr[index] == "(":
            depth += 1
        elif expr[index] == ")":
            depth -= 1
        index += 1

    if depth != 0:
        return _preprocess_fragment(expr)

    args = _split_pentaho_args(expr[start : index - 1], ";")
    if len(args) != 3:
        return _preprocess_fragment(expr)

    prefix = expr[: match.start()]
    suffix = expr[index:]
    cond = _convert_pentaho_ifs(args[0])
    true_val = _convert_pentaho_ifs(args[1])
    false_val = _convert_pentaho_ifs(args[2])
    core = f"when({cond}, {true_val}).otherwise({false_val})"

    if prefix.strip():
        return f"{_preprocess_fragment(prefix)}{core}{_convert_pentaho_ifs(suffix)}"
    if suffix.strip():
        return f"{core}{_convert_pentaho_ifs(suffix)}"
    return core


def convert_formula(expr: str) -> str:
    """Best-effort conversion of Pentaho formula syntax to PySpark column expressions."""
    if not expr or not expr.strip():
        return "lit(None)"

    result = _convert_pentaho_ifs(html.unescape(expr.strip()))

    if "col(" in result or "when(" in result or "concat(" in result or ".otherwise(" in result:
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
