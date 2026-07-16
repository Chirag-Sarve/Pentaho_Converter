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
            "concat", "concat_ws", "isnull", "regexp_replace", "regexp_extract", "explode", "explode_outer", "array",
            "split", "element_at", "collect_list", "from_csv",
            "md5", "sha1", "sha2", "crc32", "hex", "unhex", "soundex", "lag",
            "lpad", "rpad", "greatest", "conv", "dayofyear", "quarter", "hour", "minute", "second",
            "to_date", "to_timestamp", "datediff", "date_add", "add_months",
            "year", "month", "dayofmonth", "dayofweek", "weekofyear",
            "current_date", "current_timestamp",
            "row_number", "rank", "dense_rank", "monotonically_increasing_id",
            "countDistinct", "first", "last", "levenshtein", "sum", "_sum", "avg", "max", "_max", "min", "_min",
            "Window", "True", "False", "None", "int", "float", "str",
            "TARGET_CATALOG", "TARGET_SCHEMA", "PENTAHO_DATA_DIR",
            "udf", "StringType", "IntegerType", "LongType", "DoubleType",
            "BooleanType", "ArrayType", "StructType", "StructField", "MapType",
            "BinaryType",
            "DeltaTable",
            # Databricks / Spark SQL helpers used by Big Data & integration steps
            "dbutils", "to_json", "struct", "from_json", "schema_of_json",
        }
        allowed.update(name for name in dir(_builtins) if not name.startswith("_"))
        bad = [n for n in undefined if n not in allowed and not n.startswith("df_")
               and not n.endswith("_df") and "_df_" not in n
               and not n.startswith("_w_") and not n.startswith("_lkp_")
               and not n.startswith("_empty_flag_") and not n.startswith("_sync_")
               and not n.startswith("_target_") and not n.startswith("_norm_")
               and not n.startswith("_flat_") and not n.startswith("_split_")
               and not n.startswith("_parts_") and not n.startswith("_uniq_")
               and not n.startswith("_sort_") and not n.startswith("_ci_")
               and not n.startswith("_csv_") and not n.startswith("_chg_")
               and not n.startswith("_w_seq") and not n.startswith("_w_chg")
               and not n.startswith("_cg_") and not n.startswith("_xslt_")
               and not n.startswith("_w_slave")
               and not n.startswith("_dim_") and not n.startswith("_combo_")
               and not n.startswith("_scd_") and not n.startswith("_dw_")
               and not n.startswith("_max_")
               and not n.startswith("_pgp_") and not n.startswith("_symmetric_")
               and not n.startswith("_secret_key_")]
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
        if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            assigned.add(node.name)
        elif isinstance(node, ast.ClassDef):
            assigned.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                assigned.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.Name):
            if isinstance(node.ctx, ast.Store):
                assigned.add(node.id)
            elif isinstance(node.ctx, ast.Load):
                used.add(node.id)
        elif isinstance(node, ast.arg):
            assigned.add(node.arg)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            assigned.add(node.name)

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
