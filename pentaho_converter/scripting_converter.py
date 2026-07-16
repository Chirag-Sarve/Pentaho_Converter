"""Convert Pentaho Scripting steps to Databricks-compatible PySpark code."""

from __future__ import annotations

import logging
import re
from typing import Any

from .expression_converter import convert_formula

logger = logging.getLogger(__name__)

# Patterns that usually imply vendor-specific JDBC SQL (Spark SQL may differ).
_DB_SPECIFIC_SQL = re.compile(
    r"\b(WITH\s+\(NOLOCK\)|OPTION\s*\(|GOTO\s+|@@ROWCOUNT|SYSIBM\.|"
    r"DUAL\b|NVL2\s*\(|DECODE\s*\(|CONNECT\s+BY|START\s+WITH|"
    r"IDENTITY_INSERT|TOP\s+\d+\s+PERCENT|FETCH\s+FIRST\s+\d+\s+ROWS|"
    r"INFORMATION_SCHEMA\.|pg_catalog\.|mysqld\.|"
    r"BEGIN\s+TRAN|COMMIT\s+TRAN|ROLLBACK\s+TRAN|"
    r"EXECUTE\s+IMMEDIATE|DBMS_|UTL_)\b",
    re.IGNORECASE,
)

_JS_UNSUPPORTED = re.compile(
    r"\b(getInputRowMeta|getVariable|setVariable|createRowMeta|"
    r"putRow|getRow|addResultFile|fireTransEvent|"
    r"Packages\.|java\.|importPackage|load\(|eval\(|"
    r"while\s*\(|for\s*\(|function\s+\w+|try\s*\{|"
    r"Alert\(|print\(|println\()\b",
    re.IGNORECASE,
)

_JAVA_COMPLEX = re.compile(
    r"\b(new\s+\w+|throws\s+|catch\s*\(|class\s+|import\s+|"
    r"System\.|Map\b|List\b|HashMap|ArrayList|StringBuilder|"
    r"instanceof|synchronized|volatile)\b"
)

_PENTAHO_SPARK_TYPES = {
    "string": "string",
    "number": "double",
    "integer": "long",
    "bignumber": "decimal(38,18)",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "binary": "binary",
}


def _pentaho_to_spark_type(data_type: str) -> str | None:
    return _PENTAHO_SPARK_TYPES.get((data_type or "").strip().lower())


def _apply_trim(expr: str, trim_type: str) -> str:
    t = (trim_type or "").strip().lower().replace(" ", "")
    if t in ("both", "1", "trim"):
        return f"trim({expr})"
    if t in ("left", "ltrim", "2"):
        return f"ltrim({expr})"
    if t in ("right", "rtrim", "3"):
        return f"rtrim({expr})"
    return expr


def _append_stats_fields(
    lines: list[str],
    metadata: dict[str, Any],
    out_var: str,
) -> bool:
    """Append lit(0) placeholders for ExecSQL insert/update/delete/read counters."""
    added = False
    for key in ("insert_field", "update_field", "delete_field", "read_field"):
        fname = metadata.get(key)
        if fname:
            lines.append(
                f"{out_var} = {out_var}.withColumn({fname!r}, lit(0))  "
                "# JDBC rowcount unavailable from spark.sql()"
            )
            added = True
    return added


def _comment_block(title: str, text: str, *, max_lines: int = 40) -> list[str]:
    """Emit original source as PySpark comments (truncated for huge scripts)."""
    if not text:
        return []
    lines = [f"# --- {title} ---"]
    raw_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    for i, line in enumerate(raw_lines):
        if i >= max_lines:
            lines.append(f"# ... ({len(raw_lines) - max_lines} more line(s) omitted)")
            break
        lines.append(f"# {line}")
    lines.append(f"# --- end {title} ---")
    return lines


def _preserve(meta: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for key in keys:
        val = meta.get(key)
        if val in (None, "", [], {}):
            continue
        lines.append(f"# preserved.{key}={val!r}")
    return lines


def _sql_dialect_warnings(sql: str) -> list[str]:
    lines: list[str] = []
    if not sql or not sql.strip():
        lines.append("# WARNING: Empty or missing SQL script")
        return lines
    if _DB_SPECIFIC_SQL.search(sql):
        lines.append(
            "# WARNING: SQL contains database-specific constructs that may not run "
            "as Spark SQL — retain for JDBC execution or rewrite for Databricks."
        )
    if "?" in sql:
        lines.append(
            "# WARNING: JDBC '?' parameter placeholders are not native Spark SQL; "
            "bind via string formatting or use parameterized JDBC."
        )
    return lines


def convert_exec_sql_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Execute SQL Script → spark.sql (with JDBC / arg / txn warnings)."""
    lines = [f"# Execute SQL Script: {step_name}"]
    sql = str(metadata.get("sql") or "")
    connection = str(metadata.get("connection") or "")
    arguments = list(metadata.get("arguments") or [])
    execute_each = bool(metadata.get("execute_each_row"))
    single_stmt = bool(metadata.get("single_statement"))
    set_params = bool(metadata.get("set_params"))
    replace_vars = bool(metadata.get("replace_variables"))
    quote_string = bool(metadata.get("quote_string"))

    lines.extend(
        _preserve(
            metadata,
            (
                "connection",
                "sql",
                "execute_each_row",
                "single_statement",
                "replace_variables",
                "set_params",
                "quote_string",
                "arguments",
                "insert_field",
                "update_field",
                "delete_field",
                "read_field",
            ),
        )
    )
    lines.extend(_sql_dialect_warnings(sql))

    if connection:
        lines.append(
            f"# WARNING: Pentaho JDBC connection {connection!r} is not auto-migrated; "
            "prefer Unity Catalog / spark.sql against Databricks tables."
        )
    if execute_each:
        lines.append(
            "# WARNING: execute_each_row=Y implies per-input-row SQL — "
            "does not scale in Spark; consider batch rewrite."
        )
    if set_params and arguments:
        lines.append(
            f"# WARNING: set_params with arguments={arguments!r} — "
            "Spark SQL lacks JDBC PreparedStatement binding; "
            "values are string-substituted for '?' placeholders."
        )
    if quote_string:
        lines.append(
            "# NOTE: quote_string=Y — argument values are SQL-quoted with escaped quotes"
        )
    if single_stmt:
        lines.append("# preserved.transaction: single_statement=Y (Spark sessions have no JDBC TX)")
    if replace_vars:
        lines.append("# NOTE: replace_variables=Y — resolve ${var} in SQL before spark.sql()")

    status = "converted"
    if not sql.strip():
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    if connection or arguments or execute_each or set_params or _DB_SPECIFIC_SQL.search(sql):
        status = "partial"

    try:
        if in_df:
            lines.append(f"{in_df}.createOrReplaceTempView('_exec_sql_input')")
            if execute_each:
                lines.append(f"for _row in {in_df}.toLocalIterator():")
                lines.append("    try:")
                if arguments:
                    lines.append("        _args = _row.asDict(recursive=True)")
                    lines.append(f"        _sql = {sql!r}")
                    lines.append(
                        "        # Bind '?' left-to-right from argument field names"
                    )
                    for arg in arguments:
                        if quote_string:
                            lines.append(f"        _v = _args.get({arg!r})")
                            lines.append(
                                "        _rep = ('NULL' if _v is None else "
                                "\"'\" + str(_v).replace(\"'\", \"''\") + \"'\")"
                            )
                            lines.append("        _sql = _sql.replace('?', _rep, 1)")
                        else:
                            lines.append(
                                f"        _sql = _sql.replace('?', "
                                f"('' if _args.get({arg!r}) is None else "
                                f"str(_args.get({arg!r}))), 1)"
                            )
                    lines.append("        spark.sql(_sql)")
                else:
                    lines.append(f"        spark.sql({sql!r})")
                lines.append("    except Exception as _exec_exc:")
                lines.append(
                    "        # Invalid SQL / missing objects / dialect mismatch"
                )
                lines.append("        print(f'ExecSQL row failed: {_exec_exc}')")
                lines.append(f"{out_var} = {in_df}")
                if _append_stats_fields(lines, metadata, out_var):
                    status = "partial"
            else:
                if arguments and "?" in sql:
                    lines.append(
                        "# WARNING: Static SQL with '?' args but execute_each_row=N — "
                        "placeholders left as-is; migrate manually."
                    )
                    status = "partial"
                lines.append("try:")
                lines.append(f"    spark.sql({sql!r})")
                lines.append("except Exception as _exec_exc:")
                lines.append("    # Invalid SQL / unsupported dialect — review manually")
                lines.append("    print(f'ExecSQL failed: {_exec_exc}')")
                lines.append(f"{out_var} = {in_df}")
                if _append_stats_fields(lines, metadata, out_var):
                    status = "partial"
        else:
            lines.append("try:")
            lines.append(f"    {out_var} = spark.sql({sql!r})")
            lines.append("except Exception as _exec_exc:")
            lines.append("    print(f'ExecSQL failed: {_exec_exc}')")
            lines.append(
                f"    {out_var} = spark.createDataFrame([], '_placeholder STRING')"
            )
            status = "partial"
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("ExecSQL convert failed for %s: %s", step_name, exc)
        lines.append(f"# ERROR: {exc}")
        lines.append(
            f"{out_var} = {in_df}" if in_df else
            f"{out_var} = spark.createDataFrame([], '_placeholder STRING')"
        )
        return lines, "partial"

    return lines, status


def convert_exec_sql_row_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Execute Row SQL Script → per-row spark.sql with scale warnings."""
    lines = [f"# Execute Row SQL Script: {step_name}"]
    sql_field = str(metadata.get("sql_field") or "")
    sql_template = str(metadata.get("sql") or "")
    sql_filename_field = str(metadata.get("sql_filename_field") or "")
    connection = str(metadata.get("connection") or "")
    send_one = bool(metadata.get("send_one_statement", True))

    lines.extend(
        _preserve(
            metadata,
            (
                "connection",
                "sql_field",
                "sql",
                "sql_from_file",
                "sql_filename_field",
                "send_one_statement",
                "commit",
                "insert_field",
                "update_field",
                "delete_field",
                "read_field",
            ),
        )
    )
    lines.append(
        "# WARNING: Row-wise SQL execution does not scale in Spark — "
        "prefer set-based Spark SQL / Delta MERGE."
    )
    if connection:
        lines.append(
            f"# WARNING: JDBC connection {connection!r} preserved only; "
            "code uses spark.sql (rewrite for external DB via JDBC if required)."
        )
    if metadata.get("sql_from_file") and sql_filename_field:
        lines.append(
            f"# WARNING: sql_from_file=Y with filename field {sql_filename_field!r} — "
            "file contents are not auto-loaded; supply SQL via sql_field."
        )
    if not send_one:
        lines.append(
            "# WARNING: send_one_statement=N (multi-statement scripts) — "
            "Spark SQL executes one statement per call."
        )

    if not in_df:
        if sql_template:
            lines.extend(_sql_dialect_warnings(sql_template))
            lines.append(f"{out_var} = spark.sql({sql_template!r})")
            return lines, "partial"
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    if not sql_field and not sql_template:
        lines.append("# WARNING: missing sql_field and SQL template")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"

    lines.append("from pyspark.sql import Row")
    lines.append(f"_exec_row_parts_{out_var} = []")
    lines.append(f"for _row in {in_df}.toLocalIterator():")
    lines.append("    _row_d = _row.asDict(recursive=True)")
    if sql_field:
        lines.append(f"    _sql = str(_row_d.get({sql_field!r}) or '')")
    else:
        lines.append(f"    _sql = {sql_template!r}")
        lines.append("    try:")
        lines.append(
            "        _sql = _sql.format(**{k: '' if v is None else v for k, v in _row_d.items()})"
        )
        lines.append("    except Exception:")
        lines.append("        pass")
    lines.append("    if not _sql.strip():")
    lines.append("        continue")
    lines.append("    try:")
    lines.append("        spark.sql(_sql)")
    lines.append("    except Exception as _row_sql_exc:")
    lines.append("        # Invalid / dialect-specific SQL for this row")
    lines.append("        print(f'ExecSQLRow failed: {_row_sql_exc}')")
    # Preserve input row (ExecSQLRow typically passes stream through)
    lines.append(f"    _exec_row_parts_{out_var}.append(Row(**_row_d))")
    lines.append(f"if _exec_row_parts_{out_var}:")
    lines.append(f"    {out_var} = spark.createDataFrame(_exec_row_parts_{out_var})")
    for key in ("insert_field", "update_field", "delete_field", "read_field"):
        fname = metadata.get(key)
        if fname:
            lines.append(
                f"    {out_var} = {out_var}.withColumn({fname!r}, lit(0))  "
                "# JDBC rowcount unavailable"
            )
    lines.append("else:")
    lines.append(f"    {out_var} = {in_df}.limit(0)")
    return lines, "partial"


def convert_formula_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Formula → withColumn(convert_formula(...)); preserves unsupported exprs."""
    lines = [f"# Formula: {step_name}"]
    entries = list(metadata.get("formulas") or [])
    if not entries:
        formula = metadata.get("formula") or ""
        field_name = metadata.get("field_name") or "formula_result"
        if formula:
            entries = [{
                "field_name": field_name,
                "formula": formula,
                "value_type": metadata.get("value_type") or "",
            }]

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial" if not entries else "converted"

    if not entries:
        lines.append("# WARNING: Formula step has no formula expressions")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"

    lines.append(f"{out_var} = {in_df}")
    status = "converted"
    for entry in entries:
        field_name = entry.get("field_name") or "formula_result"
        formula = entry.get("formula") or ""
        value_type = entry.get("value_type") or ""
        for key in ("value_type", "replace_field", "length", "precision"):
            val = entry.get(key) or ""
            if val:
                lines.append(f"# preserved.{key}[{field_name}]={val!r}")
        if entry.get("replace_field"):
            lines.append(
                f"# WARNING: replace_field={entry.get('replace_field')!r} — "
                "Pentaho may overwrite an existing field; Spark withColumn overwrites by name"
            )
        if not formula.strip():
            lines.append(
                f"{out_var} = {out_var}.withColumn({field_name!r}, lit(None))  "
                "# empty formula"
            )
            status = "partial"
            continue
        converted = convert_formula(formula)
        # Heuristic: leftover LibreOffice tokens or bare [brackets]
        unsupported_toks = (
            "DATEVALUE", "LOOKUP(", "INDIRECT(", "CELL(", "HYPERLINK(",
            "INFO(", "NA(", "AREAS(", "GETPIVOTDATA(",
        )
        if "[" in formula and "col(" not in converted:
            lines.append(
                f"# WARNING: Formula {formula!r} may be unsupported — review expression"
            )
            status = "partial"
        elif any(tok in formula.upper() for tok in unsupported_toks) or any(
            tok.rstrip("(") in converted.upper()
            for tok in ("INDIRECT", "DATEVALUE", "LOOKUP", "GETPIVOTDATA")
        ):
            lines.append(
                f"# WARNING: Unsupported or approximate formula {formula!r}"
            )
            status = "partial"
        elif converted.startswith("expr(") and any(
            tok in formula.upper() for tok in ("VALUE(",)
        ):
            lines.append(
                f"# WARNING: Unsupported or approximate formula {formula!r}"
            )
            status = "partial"
        spark_t = _pentaho_to_spark_type(value_type)
        expr = f"({converted}).cast({spark_t!r})" if spark_t else converted
        lines.append(
            f"{out_var} = {out_var}.withColumn({field_name!r}, {expr})"
        )
    return lines, status


def _try_convert_js_assignment(script: str, field_name: str) -> str | None:
    """Best-effort: extract `var field = <expr>;` / `field = <expr>;` from JS."""
    patterns = [
        rf"(?:var|let|const)\s+{re.escape(field_name)}\s*=\s*([^;]+);",
        rf"(?:^|[;\n])\s*{re.escape(field_name)}\s*=\s*([^;]+);",
    ]
    for pat in patterns:
        m = re.search(pat, script, re.MULTILINE)
        if not m:
            continue
        rhs = m.group(1).strip()
        return _js_expr_to_pyspark(rhs)
    return None


def _js_expr_to_pyspark(expr: str) -> str | None:
    """Convert a simple JS RHS to a PySpark column expression, or None."""
    e = expr.strip().rstrip(";")
    if not e:
        return None
    if _JS_UNSUPPORTED.search(e) or "=>" in e or "{" in e:
        return None

    # String / number / bool / null literals
    if re.fullmatch(r'("([^"\\]|\\.)*"|\'([^\'\\]|\\.)*\')', e):
        return f"lit({e if e[0] == '"' else repr(e[1:-1])})"
    if re.fullmatch(r"-?\d+(\.\d+)?", e):
        return f"lit({e})"
    if e in ("true", "false"):
        return f"lit({e == 'true'})"
    if e in ("null", "undefined", "NaN"):
        return "lit(None)"

    # Field.method() patterns
    m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.toUpperCase\s*\(\s*\)", e)
    if m:
        return f'upper(col("{m.group(1)}"))'
    m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.toLowerCase\s*\(\s*\)", e)
    if m:
        return f'lower(col("{m.group(1)}"))'
    m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.trim\s*\(\s*\)", e)
    if m:
        return f'trim(col("{m.group(1)}"))'
    m = re.fullmatch(
        r"([A-Za-z_][A-Za-z0-9_]*)\.substring\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", e
    )
    if m:
        start = int(m.group(2)) + 1  # JS 0-based → Spark 1-based
        end = int(m.group(3))
        length = max(end - int(m.group(2)), 0)
        return f'substring(col("{m.group(1)}"), {start}, {length})'

    # Ternary: cond ? a : b
    tern = re.match(r"^(.+?)\s*\?\s*(.+?)\s*:\s*(.+)$", e)
    if tern:
        c = _js_expr_to_pyspark(tern.group(1).strip())
        a = _js_expr_to_pyspark(tern.group(2).strip())
        b = _js_expr_to_pyspark(tern.group(3).strip())
        if c and a and b:
            return f"when({c}, {a}).otherwise({b})"
        return None

    # Comparison / arithmetic with identifiers → formula-style conversion
    # Replace bare identifiers with [id] so convert_formula can map them.
    if re.search(r"[A-Za-z_]", e) and not re.search(r"[\"']", e):
        # Replace && || ! and JS equality
        e2 = e
        e2 = e2.replace("===", "==").replace("!==", "!=")
        e2 = re.sub(r"&&", " AND ", e2)
        e2 = re.sub(r"\|\|", " OR ", e2)
        e2 = re.sub(r"(?<![A-Za-z0-9_])!(?!=)", " NOT ", e2)

        def _bracket_ident(m: re.Match[str]) -> str:
            tok = m.group(0)
            if tok.upper() in {"AND", "OR", "NOT", "TRUE", "FALSE", "NULL"}:
                return tok
            if tok in {"true", "false", "null"}:
                return tok
            return f"[{tok}]"

        e2 = re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\b", _bracket_ident, e2)
        try:
            return convert_formula(e2)
        except Exception:
            return None

    # String concat with + is ambiguous — leave for manual
    if "+" in e and ("'" in e or '"' in e):
        return None

    return None


def convert_experimental_script_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Experimental Script (javax.script) → language-aware Databricks migration.

    - Javascript: reuse Modified Java Script Value approximate conversion.
    - Python: preserve script + emit a driver-side exec sketch with LIMITATIONs.
    - Ruby / Groovy / other: preserve metadata and document unsupported engines.
    """
    language = (metadata.get("script_language") or metadata.get("script_engine") or "javascript").strip().lower()
    scripts = list(metadata.get("scripts") or [])
    fields = list(metadata.get("fields") or [])
    transform = str(metadata.get("script") or "")

    if language in ("javascript", "js", "ecmascript", "nashorn", "rhino", ""):
        lines, status = convert_javascript_value_step(metadata, in_df, out_var, step_name)
        # Relabel header for Experimental category clarity
        if lines and lines[0].startswith("# Modified Java Script Value:"):
            lines[0] = f"# Experimental Script (javascript): {step_name}"
        lines.insert(1, "# preserved.script_language='javascript'")
        if metadata.get("error_target_step"):
            lines.insert(
                2,
                f"# preserved.error_target_step={metadata.get('error_target_step')!r}",
            )
        return lines, status

    lines = [f"# Experimental Script ({language}): {step_name}"]
    lines.extend(_preserve(metadata, ("script_language", "script_engine", "scripts", "fields")))
    if metadata.get("error_target_step"):
        lines.append(f"# preserved.error_target_step={metadata.get('error_target_step')!r}")

    for script in scripts:
        stype = str(script.get("type") or "0")
        label = {"0": "transform", "1": "start", "2": "end"}.get(stype, stype)
        lines.extend(
            _comment_block(
                f"{language} {label}: {script.get('name') or ''}",
                script.get("script") or "",
            )
        )

    if language in ("python", "py", "jython"):
        lines.append(
            "# LIMITATION: Experimental Script Python runs via javax.script/Jython in PDI; "
            "Databricks has no in-pipeline Jython engine - migrate logic into notebook cells "
            "or a Python UDF manually"
        )
        lines.append("# WARNING: Automated Python Script execution is not emitted as a Spark UDF")
        if transform.strip():
            lines.append("# --- optional driver-side sketch (enable manually; review security) ---")
            lines.append("# _script_locals = {'spark': spark, 'df': " + (in_df or "None") + "}")
            lines.append("# exec('''")
            for raw in transform.splitlines()[:40]:
                lines.append(f"# {raw}")
            lines.append("# ''', _script_locals)")
            lines.append(f"# {out_var} = _script_locals.get('df', {in_df or 'spark.createDataFrame([], StructType([]))'})")
    elif language in ("ruby", "rb", "jruby", "groovy", "gy"):
        lines.append(
            f"# UNSUPPORTED: Experimental Script language {language!r} has no Databricks "
            "Spark equivalent - preserve script and rewrite in PySpark/SQL"
        )
        lines.append(
            f"# WARNING: {language} Script cannot execute inside Databricks jobs as-is"
        )
    else:
        lines.append(
            f"# UNSUPPORTED: unknown Experimental Script language {language!r}"
        )

    if not in_df:
        lines.append("from pyspark.sql.types import StructType")
        lines.append(f"{out_var} = spark.createDataFrame([], StructType([]))")
        return lines, "partial"

    lines.append(f"{out_var} = {in_df}")
    for field in fields:
        name = field.get("rename") or field.get("name")
        if not name:
            continue
        spark_t = _pentaho_to_spark_type(field.get("type") or "") or "string"
        lines.append(
            f"{out_var} = {out_var}.withColumn({name!r}, lit(None).cast({spark_t!r}))  "
            f"# {language} field {'replace' if field.get('replace') else 'add'} placeholder"
        )
    logger.info(
        "ExperimentalScript '%s': language=%s fields=%s",
        step_name, language, len(fields),
    )
    return lines, "partial"


def convert_javascript_value_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Modified Java Script Value → approximate PySpark columns + preserve JS."""
    lines = [f"# Modified Java Script Value: {step_name}"]
    script = str(metadata.get("script") or "")
    scripts = list(metadata.get("scripts") or [])
    fields = list(metadata.get("fields") or [])

    lines.extend(
        _preserve(
            metadata,
            ("optimization_level", "compatible", "scripts"),
        )
    )
    if scripts:
        for s in scripts:
            stype = str(s.get("type") or "0")
            label = {"0": "transform", "1": "start", "2": "end"}.get(stype, stype)
            if label in ("start", "2", "end", "1") or stype in ("1", "2"):
                lines.append(
                    f"# WARNING: JS {label} script is not executed in Spark — preserved only"
                )
            lines.extend(_comment_block(f"JS {label}: {s.get('name') or ''}", s.get("script") or ""))
    elif script:
        lines.extend(_comment_block("original JavaScript", script))

    if script and _JS_UNSUPPORTED.search(script):
        lines.append(
            "# WARNING: Unsupported JavaScript features (Rhino APIs / control flow) — "
            "manual migration required for full fidelity."
        )

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    lines.append(f"{out_var} = {in_df}")
    if not fields and not script:
        lines.append("# WARNING: no JavaScript and no output fields configured")
        return lines, "partial"

    converted_any = False
    unsupported_any = False
    for field in fields:
        name = field.get("rename") or field.get("name")
        if not name:
            continue
        src_name = field.get("name") or name
        ftype = field.get("type") or ""
        if ftype:
            lines.append(f"# preserved.field_type[{name}]={ftype!r}")
        for key in ("length", "precision", "replace"):
            if field.get(key) not in (None, "", False):
                lines.append(f"# preserved.{key}[{name}]={field.get(key)!r}")
        expr = _try_convert_js_assignment(script, src_name) if script else None
        if expr is None and script:
            expr = _try_convert_js_assignment(script, name)
        spark_t = _pentaho_to_spark_type(ftype)
        if expr:
            if spark_t:
                expr = f"({expr}).cast({spark_t!r})"
            lines.append(f"{out_var} = {out_var}.withColumn({name!r}, {expr})")
            converted_any = True
        else:
            cast = spark_t or "string"
            lines.append(
                f"{out_var} = {out_var}.withColumn({name!r}, lit(None).cast({cast!r}))  "
                f"# JS field {src_name!r} not auto-translated"
            )
            unsupported_any = True

    if not fields and script:
        lines.append(
            "# WARNING: Script present but no declared output fields — "
            "side-effect-only JS is not emulated"
        )
        unsupported_any = True

    if converted_any and not unsupported_any and not (
        script and _JS_UNSUPPORTED.search(script)
    ):
        return lines, "converted"
    return lines, "partial"


def _regex_flags_prefix(metadata: dict[str, Any]) -> str:
    """Build inline Java regex flags supported by Spark regexp (subset)."""
    flags: list[str] = []
    if metadata.get("case_insensitive"):
        flags.append("i")
    if metadata.get("multiline"):
        flags.append("m")
    if metadata.get("dotall"):
        flags.append("s")
    # comment / unicode / unix / canon_eq are poorly supported in Spark Java regex
    return ("(?" + "".join(flags) + ")") if flags else ""


def convert_regex_eval_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Regex Evaluation → rlike + regexp_extract capture groups."""
    lines = [f"# Regex Evaluation: {step_name}"]
    matcher = str(metadata.get("matcher") or "field")
    pattern = str(metadata.get("pattern") or "")
    result_field = str(metadata.get("result_field") or "result")
    fields = list(metadata.get("fields") or [])

    lines.extend(
        _preserve(
            metadata,
            (
                "matcher",
                "pattern",
                "result_field",
                "use_variable_interpolation",
                "allow_capture_groups",
                "replace_fields",
                "case_insensitive",
                "canon_eq",
                "comment",
                "dotall",
                "multiline",
                "unicode",
                "unix_lines",
                "fields",
            ),
        )
    )

    status = "converted"
    if not pattern:
        lines.append("# WARNING: Missing regex pattern")
        status = "partial"
        pattern = ".*"

    # Validate pattern early
    try:
        re.compile(pattern)
    except re.error as exc:
        lines.append(f"# WARNING: Invalid regex pattern {pattern!r}: {exc}")
        status = "partial"

    unsupported_flags = []
    for flag_key, label in (
        ("canon_eq", "CANON_EQ"),
        ("comment", "COMMENTS"),
        ("unicode", "UNICODE_CASE"),
        ("unix_lines", "UNIX_LINES"),
    ):
        if metadata.get(flag_key):
            unsupported_flags.append(label)
    if unsupported_flags:
        lines.append(
            f"# WARNING: Regex flags {unsupported_flags} have limited/no Spark support"
        )
        status = "partial"
    if metadata.get("use_variable_interpolation"):
        lines.append(
            "# WARNING: usevar=Y — resolve ${variables} in pattern before runtime"
        )
        status = "partial"
    if metadata.get("replace_fields"):
        lines.append(
            "# NOTE: replacefields=Y — capture groups overwrite same-named inbound columns "
            "(Spark withColumn always overwrites by name)"
        )
    elif fields:
        colliding = [f.get("name") for f in fields if f.get("name")]
        lines.append(
            "# WARNING: replacefields=N — if capture names collide with inbound columns, "
            f"Spark still overwrites by name ({colliding!r}); rename captures if needed"
        )
        status = "partial"

    flag_prefix = _regex_flags_prefix(metadata)
    spark_pattern = f"{flag_prefix}{pattern}" if flag_prefix else pattern

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    lines.append(f"{out_var} = {in_df}")
    # Pentaho result field is Y/N string, not boolean
    lines.append(
        f"{out_var} = {out_var}.withColumn({result_field!r}, "
        f'when(col({matcher!r}).rlike({spark_pattern!r}), lit("Y")).otherwise(lit("N")))'
    )

    if fields and (
        metadata.get("allow_capture_groups")
        or metadata.get("allow_capture_groups") is None
    ):
        for idx, field in enumerate(fields, start=1):
            name = field.get("name")
            if not name:
                continue
            extract = (
                f"regexp_extract(col({matcher!r}), {spark_pattern!r}, {idx})"
            )
            extract = _apply_trim(extract, field.get("trim_type") or "")
            spark_t = _pentaho_to_spark_type(field.get("type") or "")
            if field.get("format"):
                lines.append(
                    f"# WARNING: capture format={field.get('format')!r} for {name!r} "
                    "not applied (Spark has no direct Pentaho format mask)"
                )
                status = "partial"
            if spark_t and spark_t != "string":
                extract = f"({extract}).cast({spark_t!r})"
            lines.append(
                f"{out_var} = {out_var}.withColumn({name!r}, {extract})"
            )
            nullif = field.get("nullif")
            ifnull = field.get("ifnull")
            if nullif:
                lines.append(
                    f"{out_var} = {out_var}.withColumn({name!r}, "
                    f"when(col({name!r}) == {nullif!r}, lit(None)).otherwise(col({name!r})))"
                )
            if ifnull:
                lines.append(
                    f"{out_var} = {out_var}.withColumn({name!r}, "
                    f"coalesce(col({name!r}), lit({ifnull!r})))"
                )
    elif fields:
        lines.append("# WARNING: Capture-group fields present but allowcapturegroups=N")
        status = "partial"

    return lines, status


def _simple_rules_to_when(rule_text: str) -> list[tuple[str, str]]:
    """Extract trivial when-then mappings from rule text if present.

    Looks for lines like: when Fact(field == "X") then Result(out = "Y")
    Returns list of (condition_hint, action_hint) for comment-level guidance only
    when parsing fails, or (spark_cond, spark_value) when trivial.
    """
    pairs: list[tuple[str, str]] = []
    if not rule_text:
        return pairs
    # Very simple: field == "literal" → set out = "literal"
    for m in re.finditer(
        r'when\s+\w+\s*\(\s*([A-Za-z_][\w]*)\s*==\s*"([^"]*)"\s*\)'
        r'.*?then\s+\w+\s*\(\s*([A-Za-z_][\w]*)\s*=\s*"([^"]*)"\s*\)',
        rule_text,
        re.IGNORECASE | re.DOTALL,
    ):
        field, val, out_field, out_val = m.group(1), m.group(2), m.group(3), m.group(4)
        pairs.append(
            (
                out_field,
                f'when(col("{field}") == lit({val!r}), lit({out_val!r}))',
            )
        )
    return pairs


def convert_rules_accumulator_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Rules Accumulator → preserve Drools; document that accumulation is manual.

    True Drools accumulate/collect semantics have no automatic Spark translation.
    When trivial when→then patterns exist they are emitted as row-level when().
    Otherwise emit passthrough + result-column stubs and a groupBy/agg migration hint.
    """
    lines = [f"# Rules Accumulator: {step_name}"]
    rule_def = str(metadata.get("rule_definition") or "")
    result_columns = list(metadata.get("result_columns") or [])

    lines.extend(
        _preserve(
            metadata,
            (
                "kind",
                "rule_file",
                "rule_definition",
                "keep_input_fields",
                "rule_source",
                "result_columns",
            ),
        )
    )
    lines.append(
        "# WARNING: Drools Rules Accumulator cannot auto-aggregate in Spark — "
        "DRL accumulate/collect must be rewritten as groupBy().agg() / window manually."
    )
    lines.extend(_comment_block("original rules (DRL)", rule_def))

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    pairs = _simple_rules_to_when(rule_def)
    if pairs:
        lines.append(f"{out_var} = {in_df}")
        for out_field, when_expr in pairs:
            lines.append(
                f"{out_var} = {out_var}.withColumn({out_field!r}, "
                f"{when_expr}.otherwise(lit(None)))"
            )
        lines.append(
            "# NOTE: Trivial when/then approximated as row-level when(); "
            "Drools accumulation / agenda groups are NOT emulated."
        )
        return lines, "partial"

    col_names = [c.get("name") for c in result_columns if c.get("name")]
    lines.append(
        f"# MIGRATION HINT: rewrite DRL accumulate into something like "
        f"{in_df}.groupBy(<keys>).agg(...) producing {col_names!r}"
    )
    if metadata.get("keep_input_fields", True):
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(
            f"{out_var} = {in_df}.limit(0)  "
            "# keep-input-fields=N — replace with rule-produced schema"
        )
    for col_meta in result_columns:
        name = col_meta.get("name")
        if not name:
            continue
        spark_t = _pentaho_to_spark_type(col_meta.get("type") or "") or "string"
        lines.append(
            f"{out_var} = {out_var}.withColumn({name!r}, lit(None).cast({spark_t!r}))  "
            "# rules accumulator output — implement aggregation manually"
        )
    return lines, "partial"


def convert_rules_executor_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Rules Executor → preserve Drools; emit conditional columns when feasible."""
    lines = [f"# Rules Executor: {step_name}"]
    rule_def = str(metadata.get("rule_definition") or "")
    result_columns = list(metadata.get("result_columns") or [])

    lines.extend(
        _preserve(
            metadata,
            (
                "kind",
                "rule_file",
                "rule_definition",
                "keep_input_fields",
                "rule_source",
                "result_columns",
            ),
        )
    )
    lines.append(
        "# WARNING: Drools Rules Executor evaluation order / agenda groups "
        "are not translated — preserve rules and reimplement as when()/SQL CASE."
    )
    lines.extend(_comment_block("original rules (DRL)", rule_def))

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    pairs = _simple_rules_to_when(rule_def)
    lines.append(f"{out_var} = {in_df}")
    if pairs:
        lines.append("# evaluation order preserved as successive withColumn(when())")
        for out_field, when_expr in pairs:
            lines.append(
                f"{out_var} = {out_var}.withColumn({out_field!r}, "
                f"{when_expr}.otherwise(lit(None)))"
            )
        return lines, "partial"

    for col_meta in result_columns:
        name = col_meta.get("name")
        if name:
            lines.append(
                f"{out_var} = {out_var}.withColumn({name!r}, lit(None).cast('string'))  "
                "# rules executor action — implement manually"
            )
    return lines, "partial"


def convert_user_defined_java_class_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """User Defined Java Class → metadata / comment preserve + manual warning."""
    lines = [f"# User Defined Java Class: {step_name}"]
    class_name = str(metadata.get("class_name") or "Processor")
    class_source = str(metadata.get("class_source") or "")
    definitions = list(metadata.get("definitions") or [])
    fields = list(metadata.get("fields") or [])

    lines.extend(
        _preserve(
            metadata,
            (
                "class_name",
                "definitions",
                "fields",
                "info_steps",
                "clear_result_fields",
            ),
        )
    )
    lines.append(
        "# WARNING: Arbitrary User Defined Java Class cannot be translated "
        "automatically to PySpark — manual rewrite required."
    )
    if definitions:
        for d in definitions:
            lines.extend(
                _comment_block(
                    f"Java class {d.get('class_name') or class_name} "
                    f"({d.get('class_type') or 'TRANSFORM_CLASS'})",
                    d.get("class_source") or "",
                )
            )
    else:
        lines.extend(_comment_block(f"Java class {class_name}", class_source))

    # Extract import-looking lines for metadata echo
    imports = [
        ln.strip()
        for ln in class_source.splitlines()
        if ln.strip().startswith("import ")
    ]
    if imports:
        lines.append(f"# preserved.imports={imports!r}")

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    lines.append(f"{out_var} = {in_df}")
    for field in fields:
        name = field.get("name")
        if name:
            lines.append(
                f"{out_var} = {out_var}.withColumn({name!r}, lit(None).cast('string'))  "
                f"# UDJC:{class_name}"
            )
    return lines, "partial"


def convert_java_expression(expression: str) -> tuple[str | None, str | None]:
    """Convert a simple Janino Java expression to PySpark.

    Returns (pyspark_expr, warning_or_none).
    """
    expr = (expression or "").strip()
    if not expr:
        return "lit(None)", "empty Java expression"

    if _JAVA_COMPLEX.search(expr):
        return None, f"unsupported Java syntax: {expr!r}"

    if re.search(r"\.\w+\s*\(", expr) and ".equals(" not in expr:
        return None, f"unsupported Java method call: {expr!r}"

    e = expr
    e = e.replace("&&", " AND ").replace("||", " OR ")
    e = re.sub(r"(?<![A-Za-z0-9_])!(?!=)", " NOT ", e)
    e = re.sub(
        r'([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*equals\s*\(\s*("([^"]*)"|\'([^\']*)\')\s*\)',
        lambda m: f'[{m.group(1)}] == {m.group(2)}',
        e,
    )

    reserved = {"AND", "OR", "NOT", "TRUE", "FALSE", "NULL"}

    def _bracket_ident(m: re.Match[str]) -> str:
        tok = m.group(0)
        if tok.upper() in reserved:
            return tok.upper() if tok.lower() in {"true", "false", "null"} else tok
        if tok in {"true", "false", "null"}:
            return {"true": "TRUE", "false": "FALSE", "null": "NULL"}[tok]
        return f"[{tok}]"

    # Bracket identifiers outside of string literals
    parts: list[str] = []
    in_str = False
    quote = ""
    buf: list[str] = []
    i = 0
    while i < len(e):
        ch = e[i]
        if in_str:
            buf.append(ch)
            if ch == quote and (i == 0 or e[i - 1] != "\\"):
                in_str = False
                parts.append("".join(buf))
                buf = []
            i += 1
            continue
        if ch in ('"', "'"):
            if buf:
                parts.append(re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\b", _bracket_ident, "".join(buf)))
                buf = []
            in_str = True
            quote = ch
            buf = [ch]
            i += 1
            continue
        buf.append(ch)
        i += 1
    if buf:
        piece = "".join(buf)
        parts.append(
            re.sub(r"\b[A-Za-z_][A-Za-z0-9_]*\b", _bracket_ident, piece)
            if not in_str else piece
        )
    e2 = "".join(parts)

    try:
        return convert_formula(e2), None
    except Exception as exc:
        return None, f"invalid Java expression: {exc}"


def convert_user_defined_java_expression_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """User Defined Java Expression → PySpark where feasible."""
    lines = [f"# User Defined Java Expression: {step_name}"]
    fields = list(metadata.get("fields") or [])
    lines.extend(_preserve(metadata, ("fields",)))

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    if not fields:
        lines.append("# WARNING: no Java expressions configured")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"

    lines.append(f"{out_var} = {in_df}")
    status = "converted"
    for field in fields:
        name = field.get("name") or "java_result"
        expression = field.get("expression") or ""
        lines.append(f"# preserved.java_expression[{name}]={expression!r}")
        pyspark_expr, warn = convert_java_expression(expression)
        if warn:
            lines.append(f"# WARNING: {warn}")
            status = "partial"
        if pyspark_expr:
            lines.append(
                f"{out_var} = {out_var}.withColumn({name!r}, {pyspark_expr})"
            )
        else:
            lines.append(
                f"{out_var} = {out_var}.withColumn({name!r}, lit(None).cast('string'))  "
                "# unsupported Java expression"
            )
            status = "partial"
    return lines, status
