"""Scripting-category job entry helpers (Shell, SQL, JavaScript/EVAL).

Driver-side / Databricks-compatible implementations used by
``handlers.handle_shell``, ``handle_sql``, and ``handle_eval``.
"""

from __future__ import annotations

import logging
import os
import re
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)


def yn_true(raw: Any, default: bool = False) -> bool:
    if raw is None or raw == "":
        return default
    return str(raw).strip().upper() in {"Y", "YES", "TRUE", "1", "T"}


def attr(attrs: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return str(attrs[key])
    return default


def attr_yn(attrs: Mapping[str, Any], *keys: str, default: bool = False) -> bool:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return yn_true(attrs[key], default)
    return default


def iter_warning_logs(prefix: str, warnings: Iterable[str]) -> None:
    for warning in warnings:
        logger.warning("%s | %s", prefix, warning)


@dataclass
class ScriptOutcome:
    success: bool
    message: str = ""
    stdout: str = ""
    stderr: str = ""
    exit_code: int | None = None
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Shell
# ---------------------------------------------------------------------------

# Commands that are usually fine on Databricks driver (Linux)
_SAFE_SHELL_PREFIXES = (
    "echo ",
    "printf ",
    "ls ",
    "cat ",
    "head ",
    "tail ",
    "wc ",
    "pwd",
    "date",
    "mkdir ",
    "cp ",
    "mv ",
    "rm ",
    "touch ",
    "test ",
    "true",
    "false",
    "[",
)

# Windows / OS-specific patterns that need a warning on Databricks
_OS_SPECIFIC = re.compile(
    r"\b(cmd\.exe|powershell|dir\b|copy\b|del\b|move\b|type\b|cls\b|"
    r"start\b|call\b|setlocal|endlocal|%\w+%|"
    r"C:\\|D:\\|\\\\)",
    re.IGNORECASE,
)


def expand_shell_percent_vars(text: str, variables: Mapping[str, Any]) -> str:
    """Expand both ``${VAR}`` (already done by caller) leftovers and ``%VAR%``."""
    out = text or ""
    for key, val in variables.items():
        out = out.replace("%" + str(key) + "%", str(val))
        out = out.replace("%" + str(key).upper() + "%", str(val))
        out = out.replace("%" + str(key).lower() + "%", str(val))
    return out


def _echo_compatible(script: str) -> tuple[str | None, list[str]]:
    """Translate simple ``echo …`` to a Python-printable payload (no shell)."""
    warnings: list[str] = []
    text = (script or "").strip()
    m = re.match(r"^(?:echo|printf)\s+(.*)$", text, re.IGNORECASE | re.DOTALL)
    if not m:
        return None, warnings
    payload = m.group(1)
    # Strip surrounding quotes commonly used in scripts
    if (payload.startswith('"') and payload.endswith('"')) or (
        payload.startswith("'") and payload.endswith("'")
    ):
        payload = payload[1:-1]
    # Windows ``echo X & dir …`` — only keep echo part with warning
    if " & " in payload or "&&" in text:
        warnings.append(
            "Compound shell command after echo — only echo portion executed in Python"
        )
        payload = re.split(r"\s&\s|&&", payload, maxsplit=1)[0].strip()
    return payload, warnings


def run_shell(
    *,
    script: str = "",
    filename: str = "",
    insert_script: bool = True,
    arguments: Sequence[str] | None = None,
    work_directory: str = "",
    variables: Mapping[str, Any] | None = None,
    timeout: float | None = None,
    env_extra: Mapping[str, str] | None = None,
) -> ScriptOutcome:
    """Execute a PDI Shell job entry.

    Prefer Python-side ``echo`` handling for Databricks portability. Otherwise
    use ``subprocess`` with warnings for OS-specific commands.
    """
    warnings: list[str] = []
    variables = variables or {}
    args = [str(a) for a in (arguments or []) if str(a)]

    if insert_script or script:
        command = expand_shell_percent_vars(script, variables)
    else:
        command = expand_shell_percent_vars(filename, variables)
        if args:
            # filename is the executable; append arguments
            try:
                command = " ".join([shlex.quote(command), *(shlex.quote(a) for a in args)])
            except Exception:
                command = " ".join([command, *args])

    if not command.strip():
        err = ValueError("SHELL script/filename is empty")
        return ScriptOutcome(False, str(err), error=err)

    if _OS_SPECIFIC.search(command):
        warnings.append(
            "Shell command looks OS-specific (Windows cmd / paths) — "
            "may fail on Databricks Linux drivers"
        )

    # Fast path: echo / printf → Python print (always succeeds like typical echo)
    echo_payload, echo_warns = _echo_compatible(command)
    warnings.extend(echo_warns)
    if echo_payload is not None:
        logger.info("SHELL echo (Python): %s", echo_payload)
        print(echo_payload)
        return ScriptOutcome(
            True,
            "echo via Python",
            stdout=echo_payload + "\n",
            exit_code=0,
            warnings=warnings,
            extra={"mode": "python_echo", "command": command},
        )

    cwd = work_directory or None
    if cwd and not Path(cwd).exists():
        warnings.append(f"work_directory does not exist: {cwd}")
        cwd = None

    env = dict(os.environ)
    for key, val in variables.items():
        env[str(key)] = str(val)
    if env_extra:
        env.update({str(k): str(v) for k, v in env_extra.items()})

    # Use shell=True for script strings (PDI insertScript behaviour)
    try:
        completed = subprocess.run(  # noqa: S603
            command,
            shell=True,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return ScriptOutcome(
            False,
            f"SHELL timed out after {timeout}s",
            stdout=str(exc.stdout or ""),
            stderr=str(exc.stderr or ""),
            warnings=warnings,
            error=exc,
            extra={"command": command},
        )
    except OSError as exc:
        return ScriptOutcome(
            False,
            str(exc),
            warnings=warnings,
            error=exc,
            extra={"command": command},
        )

    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if stdout:
        print(stdout, end="" if stdout.endswith("\n") else "\n")
    ok = completed.returncode == 0
    return ScriptOutcome(
        ok,
        f"exit={completed.returncode}",
        stdout=stdout,
        stderr=stderr,
        exit_code=completed.returncode,
        warnings=warnings,
        error=None if ok else RuntimeError(f"SHELL exit code {completed.returncode}"),
        extra={"command": command, "cwd": cwd},
    )


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_SQL_SPLIT = re.compile(r";\s*")
_DB_SPECIFIC_SQL = re.compile(
    r"\b(WITH\s+\(NOLOCK\)|OPTION\s*\(|GOTO\s+|@@ROWCOUNT|SYSIBM\.|"
    r"DUAL\b|NVL2\s*\(|DECODE\s*\(|CONNECT\s+BY|START\s+WITH|"
    r"IDENTITY_INSERT|BEGIN\s+TRAN|COMMIT\s+TRAN|ROLLBACK\s+TRAN|"
    r"EXECUTE\s+IMMEDIATE|DBMS_|UTL_|CALL\s+\w+\()",
    re.IGNORECASE,
)


def split_sql_statements(sql: str, *, send_one_statement: bool = False) -> list[str]:
    text = (sql or "").strip()
    if not text:
        return []
    if send_one_statement:
        return [text.rstrip(";").strip()] if text.rstrip(";").strip() else []
    parts: list[str] = []
    for chunk in _SQL_SPLIT.split(text):
        stmt = chunk.strip().rstrip(";").strip()
        if not stmt:
            continue
        # Skip comment-only blocks
        non_comment = [
            ln
            for ln in stmt.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        if not non_comment:
            continue
        parts.append(stmt)
    return parts


def load_sql_text(
    *,
    sql: str,
    sql_from_file: bool,
    sql_filename: str,
    resolve: Callable[[str], str] | None = None,
) -> tuple[str, list[str]]:
    resolve = resolve or (lambda s: s)
    warnings: list[str] = []
    if sql_from_file:
        path = Path(resolve(sql_filename))
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {path}")
        return path.read_text(encoding="utf-8"), warnings
    return sql or "", warnings


def execute_sql_statements(
    statements: Sequence[str],
    *,
    spark: Any = None,
    connection_meta: Mapping[str, Any] | None = None,
    connection_name: str = "",
) -> ScriptOutcome:
    """Execute SQL via Spark ``spark.sql`` (Databricks-native).

    JDBC connection metadata is preserved in warnings when present; Spark SQL
    is preferred for Databricks.
    """
    warnings: list[str] = []
    if connection_name or connection_meta:
        warnings.append(
            f"SQL connection={connection_name!r} — executing via Spark SQL; "
            "JDBC dialect differences may apply"
        )
    if not statements:
        return ScriptOutcome(
            True, "No executable SQL statements (comments only)", warnings=warnings
        )

    for stmt in statements:
        if _DB_SPECIFIC_SQL.search(stmt):
            warnings.append(
                "SQL contains vendor-specific constructs that may not run as Spark SQL"
            )
            break

    spark_sess = spark
    if spark_sess is None:
        try:
            from pyspark.sql import SparkSession

            spark_sess = SparkSession.getActiveSession()
        except Exception:
            spark_sess = None

    if spark_sess is None:
        # Dry-run / parse-only success for comment-heavy audit placeholders when
        # no Spark is available and every statement is a no-op comment block.
        err = RuntimeError(
            "SQL job entry requires an active Spark session on Databricks"
        )
        return ScriptOutcome(False, str(err), warnings=warnings, error=err)

    executed: list[str] = []
    try:
        for stmt in statements:
            logger.info("SQL exec: %s", stmt[:200].replace("\n", " "))
            spark_sess.sql(stmt)
            executed.append(stmt)
    except Exception as exc:  # noqa: BLE001
        return ScriptOutcome(
            False,
            str(exc),
            warnings=warnings,
            error=exc,
            extra={"executed": executed, "failed_statement": stmt if statements else ""},
        )

    return ScriptOutcome(
        True,
        f"Executed {len(executed)} statement(s)",
        warnings=warnings,
        extra={"executed": executed},
    )


# ---------------------------------------------------------------------------
# JavaScript (EVAL)
# ---------------------------------------------------------------------------

_JS_COMPLEX = re.compile(
    r"\b(function\s*\(|=>|Packages\.|java\.|importPackage|load\s*\(|"
    r"while\s*\(|for\s*\(|try\s*\{|catch\s*\(|new\s+\w+|"
    r"getInputRowMeta|putRow|getRow|fireTransEvent|"
    r"Alert\s*\(|println\s*\()\b",
    re.IGNORECASE,
)

_JS_TRUE = re.compile(r"^\s*(true|1)\s*;?\s*$", re.IGNORECASE)
_JS_FALSE = re.compile(r"^\s*(false|0)\s*;?\s*$", re.IGNORECASE)


def _js_get_variable_calls(script: str) -> list[tuple[str, str]]:
    """Return (full_match, var_name) for parent_job.getVariable / getVariable."""
    found: list[tuple[str, str]] = []
    for m in re.finditer(
        r"(?:parent_job\.)?getVariable\s*\(\s*[\"']([^\"']+)[\"']\s*\)",
        script,
        re.IGNORECASE,
    ):
        found.append((m.group(0), m.group(1)))
    return found


def translate_js_eval_to_python(
    script: str,
    *,
    variables: Mapping[str, Any],
) -> tuple[str | None, list[str], str]:
    """Best-effort JS → Python boolean expression.

    Returns ``(python_expr_or_None, warnings, mode)`` where mode is
    ``translated`` | ``literal`` | ``todo``.
    """
    warnings: list[str] = []
    text = (script or "").strip()
    if not text:
        return "False", warnings, "literal"

    if _JS_TRUE.match(text):
        return "True", warnings, "literal"
    if _JS_FALSE.match(text):
        return "False", warnings, "literal"

    if _JS_COMPLEX.search(text):
        warnings.append(
            "JavaScript contains unsupported constructs — "
            "emitting TODO for manual conversion"
        )
        return None, warnings, "todo"

    # Replace getVariable("X") with repr of resolved value
    expr = text
    for full, name in _js_get_variable_calls(text):
        val = variables.get(name, "")
        expr = expr.replace(full, repr(str(val)))

    # Common JS → Python tokens
    expr = expr.replace("===", "==").replace("!==", "!=")
    expr = re.sub(r"&&", " and ", expr)
    expr = re.sub(r"\|\|", " or ", expr)
    # careful with != already handled; replace standalone !
    expr = re.sub(r"(?<![A-Za-z0-9_])!(?!=)", " not ", expr)
    expr = re.sub(r"\btrue\b", "True", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bfalse\b", "False", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bnull\b", "None", expr, flags=re.IGNORECASE)
    expr = expr.rstrip(";").strip()

    # Reject leftover JS identifiers that look like APIs
    if re.search(r"\b(parent_job|previous_result|packages)\b", expr, re.IGNORECASE):
        warnings.append("Unresolved JavaScript host objects — manual conversion required")
        return None, warnings, "todo"

    # Safety: only allow a restricted expression character set
    if not re.fullmatch(
        r"[0-9A-Za-z_ \t\n\r\"'\\.<>=!+\-*/%&|()[\],]+",
        expr,
    ):
        warnings.append("JavaScript expression failed safety check — TODO")
        return None, warnings, "todo"

    return expr, warnings, "translated"


def evaluate_javascript(
    script: str,
    *,
    variables: Mapping[str, Any],
) -> ScriptOutcome:
    """Evaluate a PDI EVAL (JavaScript) job entry as a boolean Python result."""
    py_expr, warnings, mode = translate_js_eval_to_python(script, variables=variables)

    if mode == "todo" or py_expr is None:
        warnings.append("ORIGINAL_JAVASCRIPT_PRESERVED")
        return ScriptOutcome(
            False,
            "JavaScript not auto-translated",
            warnings=warnings,
            error=NotImplementedError(
                "EVAL/JavaScript requires manual conversion — see warnings / extra.script"
            ),
            extra={"mode": "todo", "script": script, "python_attempt": py_expr},
        )

    try:
        # Restricted eval — no builtins
        result = eval(py_expr, {"__builtins__": {}}, {})  # noqa: S307
        ok = bool(result)
    except Exception as exc:  # noqa: BLE001
        return ScriptOutcome(
            False,
            f"JS→Python eval failed: {exc}",
            warnings=warnings,
            error=exc,
            extra={"mode": mode, "python_expr": py_expr, "script": script},
        )

    return ScriptOutcome(
        ok,
        f"EVAL → {ok}",
        warnings=warnings,
        extra={"mode": mode, "python_expr": py_expr, "script": script, "value": result},
        error=None if ok else None,  # failure without exception is normal for false
    )
