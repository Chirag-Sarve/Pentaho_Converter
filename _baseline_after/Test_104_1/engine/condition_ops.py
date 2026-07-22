"""Conditions-category job entry helpers.

Pure functions used by Conditions handlers (Simple Eval, File Exists,
Table Exists, Wait for SQL, …). Databricks-compatible (stdlib + Spark/JDBC).
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


def yn_true(raw: Any, default: bool = False) -> bool:
    if raw is None or raw == "":
        return default
    return str(raw).strip().upper() in {"Y", "YES", "TRUE", "1"}


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
class CondOutcome:
    success: bool
    message: str = ""
    value: Any = None
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Simple Evaluation
# ---------------------------------------------------------------------------

_STRING_OPS = {
    "0": "equal",
    "equal": "equal",
    "equals": "equal",
    "1": "different",
    "different": "different",
    "notequal": "different",
    "not_equals": "different",
    "2": "contains",
    "contains": "contains",
    "3": "notcontains",
    "notcontains": "notcontains",
    "4": "startswith",
    "startswith": "startswith",
    "starts with": "startswith",
    "5": "notstatwith",
    "notstatwith": "notstatwith",
    "notstartswith": "notstatwith",
    "6": "endswith",
    "endswith": "endswith",
    "ends with": "endswith",
    "7": "notendwith",
    "notendwith": "notendwith",
    "notendswith": "notendwith",
    "8": "regexp",
    "regexp": "regexp",
    "regex": "regexp",
    "9": "inlist",
    "inlist": "inlist",
    "10": "notinlist",
    "notinlist": "notinlist",
}

_NUMBER_OPS = {
    "0": "equal",
    "equal": "equal",
    "1": "different",
    "different": "different",
    "2": "smaller",
    "smaller": "smaller",
    "3": "smallequal",
    "smallequal": "smallequal",
    "smallerequal": "smallequal",
    "4": "greater",
    "greater": "greater",
    "5": "greaterequal",
    "greaterequal": "greaterequal",
    "6": "between",
    "between": "between",
    "7": "inlist",
    "inlist": "inlist",
    "8": "notinlist",
    "notinlist": "notinlist",
}


def _norm_op(raw: str, table: Mapping[str, str]) -> str:
    key = (raw or "equal").strip().lower().replace(" ", "").replace("_", "")
    # Keep known aliases that include underscores removed above
    aliases = {
        "smallerequal": "smallequal",
        "greaterorequal": "greaterequal",
        "notequals": "different",
        "notequal": "different",
    }
    key = aliases.get(key, key)
    return table.get(key, table.get((raw or "").strip().lower(), "equal"))


def _split_list(raw: str) -> list[str]:
    return [p.strip() for p in re.split(r"[,;]", raw or "") if p.strip()]


def _parse_bool(raw: str) -> bool | None:
    text = (raw or "").strip().lower()
    if text in {"y", "yes", "true", "1", "t"}:
        return True
    if text in {"n", "no", "false", "0", "f"}:
        return False
    return None


def simple_eval(
    *,
    left: str,
    compare: str = "",
    minvalue: str = "",
    maxvalue: str = "",
    fieldtype: str = "string",
    successcondition: str = "equal",
    successnumbercondition: str = "equal",
    successbooleancondition: str = "false",
    successwhenvarset: bool = False,
    mask: str = "",
    var_is_set: bool | None = None,
) -> CondOutcome:
    """Evaluate a PDI Simple Evaluation condition."""
    ftype = (fieldtype or "string").strip().lower()
    if ftype in {"0", "string"}:
        ftype = "string"
    elif ftype in {"1", "number"}:
        ftype = "number"
    elif ftype in {"2", "datetime", "date_time", "date"}:
        ftype = "datetime"
    elif ftype in {"3", "boolean"}:
        ftype = "boolean"

    if successwhenvarset:
        set_ok = bool(var_is_set) if var_is_set is not None else bool(str(left))
        return CondOutcome(set_ok, f"successwhenvarset → {set_ok}", set_ok)

    warnings: list[str] = []

    if ftype == "boolean":
        bv = _parse_bool(left)
        if bv is None:
            return CondOutcome(
                False,
                f"Not a boolean: {left!r}",
                error=ValueError(f"SIMPLE_EVAL non-boolean: {left!r}"),
            )
        want = (successbooleancondition or "false").strip().lower()
        # PDI: successbooleancondition "true" → success when value is true
        if want in {"true", "0"}:
            ok = bv is True
        else:
            ok = bv is False
        return CondOutcome(ok, f"boolean {left!r} want={want} → {ok}", ok)

    if ftype == "number":
        op = _norm_op(successnumbercondition, _NUMBER_OPS)
        try:
            lv = float(left)
        except ValueError:
            return CondOutcome(
                False,
                f"Non-numeric left: {left!r}",
                error=ValueError(f"SIMPLE_EVAL non-numeric: {left!r}"),
            )
        if op == "between":
            try:
                lo, hi = float(minvalue), float(maxvalue)
            except ValueError:
                return CondOutcome(
                    False,
                    "Invalid between bounds",
                    error=ValueError(f"SIMPLE_EVAL between bounds: {minvalue!r}/{maxvalue!r}"),
                )
            ok = lo <= lv <= hi
        elif op in {"inlist", "notinlist"}:
            try:
                nums = [float(x) for x in _split_list(compare)]
            except ValueError:
                return CondOutcome(
                    False,
                    "Invalid number list",
                    error=ValueError(f"SIMPLE_EVAL number list: {compare!r}"),
                )
            ok = (lv in nums) if op == "inlist" else (lv not in nums)
        else:
            try:
                rv = float(compare)
            except ValueError:
                return CondOutcome(
                    False,
                    f"Non-numeric compare: {compare!r}",
                    error=ValueError(f"SIMPLE_EVAL non-numeric: {left!r} vs {compare!r}"),
                )
            ok = {
                "equal": lv == rv,
                "different": lv != rv,
                "smaller": lv < rv,
                "smallequal": lv <= rv,
                "greater": lv > rv,
                "greaterequal": lv >= rv,
            }.get(op, lv == rv)
        return CondOutcome(ok, f"number {lv} {op} {compare or (minvalue, maxvalue)} → {ok}", ok)

    if ftype == "datetime":
        fmt = mask or "%Y/%m/%d %H:%M:%S"
        try:
            lv = datetime.strptime(left, fmt)
        except ValueError:
            # Try ISO fallback
            try:
                lv = datetime.fromisoformat(left.replace("Z", "+00:00"))
                warnings.append(f"Parsed datetime with ISO fallback (mask={fmt!r})")
            except ValueError:
                return CondOutcome(
                    False,
                    f"Bad datetime: {left!r}",
                    error=ValueError(f"SIMPLE_EVAL datetime: {left!r}"),
                    warnings=warnings,
                )
        op = _norm_op(successnumbercondition, _NUMBER_OPS)
        if op == "between":
            try:
                lo = datetime.strptime(minvalue, fmt)
                hi = datetime.strptime(maxvalue, fmt)
            except ValueError as exc:
                return CondOutcome(False, str(exc), error=exc, warnings=warnings)
            ok = lo <= lv <= hi
        else:
            try:
                rv = datetime.strptime(compare, fmt)
            except ValueError as exc:
                return CondOutcome(False, str(exc), error=exc, warnings=warnings)
            ok = {
                "equal": lv == rv,
                "different": lv != rv,
                "smaller": lv < rv,
                "smallequal": lv <= rv,
                "greater": lv > rv,
                "greaterequal": lv >= rv,
            }.get(op, lv == rv)
        return CondOutcome(ok, f"datetime {op} → {ok}", ok, warnings)

    # string
    op = _norm_op(successcondition, _STRING_OPS)
    # Treat empty / null-ish
    if op in {"equal", "different"} and compare.lower() in {"null", "(null)"}:
        is_null = left is None or str(left) == ""
        ok = is_null if op == "equal" else (not is_null)
        return CondOutcome(ok, f"string null-check → {ok}", ok)
    if compare.lower() in {"empty", "(empty)"} and op in {"equal", "different"}:
        is_empty = str(left) == ""
        ok = is_empty if op == "equal" else (not is_empty)
        return CondOutcome(ok, f"string empty-check → {ok}", ok)

    ls, rs = str(left), str(compare)
    if op == "equal":
        ok = ls == rs
    elif op == "different":
        ok = ls != rs
    elif op == "contains":
        ok = rs in ls
    elif op == "notcontains":
        ok = rs not in ls
    elif op == "startswith":
        ok = ls.startswith(rs)
    elif op == "notstatwith":
        ok = not ls.startswith(rs)
    elif op == "endswith":
        ok = ls.endswith(rs)
    elif op == "notendwith":
        ok = not ls.endswith(rs)
    elif op == "regexp":
        try:
            ok = bool(re.search(rs, ls))
        except re.error as exc:
            return CondOutcome(False, str(exc), error=exc)
    elif op == "inlist":
        ok = ls in _split_list(rs)
    elif op == "notinlist":
        ok = ls not in _split_list(rs)
    else:
        ok = ls == rs
    return CondOutcome(ok, f"string {op!r} {ls!r} vs {rs!r} → {ok}", ok)


# ---------------------------------------------------------------------------
# File / folder conditions
# ---------------------------------------------------------------------------


def folder_is_empty(
    folder: str,
    *,
    include_subfolders: bool = False,
    wildcard: str = "",
    specify_wildcard: bool = False,
) -> CondOutcome:
    path = Path(folder)
    if not path.exists():
        err = FileNotFoundError(f"Folder does not exist: {folder}")
        return CondOutcome(False, str(err), error=err)
    if not path.is_dir():
        err = NotADirectoryError(f"Not a folder: {folder}")
        return CondOutcome(False, str(err), error=err)

    pattern = None
    if specify_wildcard and wildcard:
        try:
            pattern = re.compile(wildcard)
        except re.error:
            pattern = re.compile(re.escape(wildcard))

    iterator = path.rglob("*") if include_subfolders else path.iterdir()
    for fp in iterator:
        try:
            if not fp.is_file() and not (fp.is_dir() and not include_subfolders):
                # Count files; empty dirs alone don't make parent non-empty in PDI
                # when include_subfolders=N we only look at direct children files+dirs
                pass
            if include_subfolders:
                if fp.is_file():
                    if pattern and not pattern.search(fp.name):
                        continue
                    return CondOutcome(False, f"Non-empty (found {fp})", False)
            else:
                if pattern:
                    if fp.is_file() and pattern.search(fp.name):
                        return CondOutcome(False, f"Non-empty (found {fp})", False)
                else:
                    # Any file or subdirectory means non-empty
                    return CondOutcome(False, f"Non-empty (found {fp.name})", False)
        except OSError:
            continue
    return CondOutcome(True, f"Folder empty: {folder}", True)


def files_exist(
    paths: Sequence[str],
    *,
    exists_fn: Callable[[str], bool] | None = None,
) -> CondOutcome:
    exists_fn = exists_fn or (lambda p: Path(p).exists())
    missing: list[str] = []
    found: list[str] = []
    for raw in paths:
        text = str(raw or "").strip()
        if not text:
            continue
        if exists_fn(text):
            found.append(text)
        else:
            missing.append(text)
    ok = bool(found) and not missing
    # PDI FILES_EXIST: all listed files must exist
    ok = not missing and bool(paths)
    if not paths:
        return CondOutcome(False, "No files specified", False)
    return CondOutcome(
        ok,
        f"exists={len(found)} missing={len(missing)}",
        ok,
        extra={"found": found, "missing": missing},
        error=None if ok else FileNotFoundError(f"Missing: {', '.join(missing)}"),
    )


def check_files_locked(
    pairs: Sequence[Mapping[str, str]],
    *,
    include_subfolders: bool = False,
) -> CondOutcome:
    """Best-effort lock detection: try opening exclusively / appending.

    On Databricks/Linux exclusive locks are limited — we detect Windows
    sharing violations and treat unreadable-in-use files as locked.
    """
    locked: list[str] = []
    unchecked: list[str] = []
    warnings: list[str] = []
    files: list[Path] = []

    for pair in pairs:
        root = Path(pair.get("source") or pair.get("name") or "")
        mask = pair.get("wildcard") or pair.get("filemask") or ""
        if not root.exists():
            unchecked.append(str(root))
            warnings.append(f"Path not found: {root}")
            continue
        pattern = None
        if mask:
            try:
                pattern = re.compile(mask)
            except re.error:
                pattern = re.compile(re.escape(mask))
        if root.is_file():
            files.append(root)
        else:
            it = root.rglob("*") if include_subfolders else root.iterdir()
            for fp in it:
                if fp.is_file() and (pattern is None or pattern.search(fp.name)):
                    files.append(fp)

    for fp in files:
        try:
            # Exclusive append open — fails on Windows when locked
            with open(fp, "a+b") as fh:
                try:
                    import msvcrt  # type: ignore

                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                except ImportError:
                    try:
                        import fcntl  # type: ignore

                        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                    except ImportError:
                        warnings.append("No platform lock API — treated as unlocked")
                    except OSError:
                        locked.append(str(fp))
                except OSError:
                    locked.append(str(fp))
        except OSError:
            locked.append(str(fp))

    # Success when NO files are locked (PDI Check Files Locked)
    ok = not locked
    return CondOutcome(
        ok,
        f"locked={len(locked)} checked={len(files)}",
        ok,
        warnings,
        error=None if ok else PermissionError(f"Locked files: {locked}"),
        extra={"locked": locked, "missing": unchecked},
    )


def webservice_available(
    url: str,
    *,
    connect_timeout: float = 0,
    read_timeout: float = 0,
) -> CondOutcome:
    if not url:
        err = ValueError("WEBSERVICE_AVAILABLE url is empty")
        return CondOutcome(False, str(err), error=err)
    timeout = max(float(connect_timeout or 0), float(read_timeout or 0), 1.0)
    try:
        import requests  # type: ignore

        resp = requests.get(url, timeout=timeout)
        ok = 200 <= resp.status_code < 400
        return CondOutcome(
            ok,
            f"HTTP {resp.status_code}",
            ok,
            extra={"status_code": resp.status_code},
            error=None if ok else HTTPError(url, resp.status_code, resp.reason, hdrs=None, fp=None),
        )
    except ImportError:
        pass
    except Exception as exc:  # noqa: BLE001
        return CondOutcome(False, str(exc), error=exc)

    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=timeout) as resp:  # noqa: S310
            status = getattr(resp, "status", 200)
        ok = 200 <= int(status) < 400
        return CondOutcome(
            ok,
            f"HTTP {status}",
            ok,
            extra={"status_code": status},
            error=None if ok else ValueError(f"HTTP {status}"),
        )
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        return CondOutcome(False, str(exc), error=exc)


# ---------------------------------------------------------------------------
# File metrics
# ---------------------------------------------------------------------------

_SCALE = {"bytes": 1, "0": 1, "kbytes": 1024, "1": 1024, "mbytes": 1024**2, "2": 1024**2, "gbytes": 1024**3, "3": 1024**3}


def eval_files_metrics(
    paths: Sequence[str],
    *,
    evaluation_type: str = "size",
    comparevalue: str = "0",
    minvalue: str = "0",
    maxvalue: str = "0",
    successnumbercondition: str = "equal",
    scale: str = "bytes",
    recursive: bool = False,
    wildcard: str = "",
) -> CondOutcome:
    files: list[Path] = []
    pattern = None
    if wildcard:
        try:
            pattern = re.compile(wildcard)
        except re.error:
            pattern = re.compile(re.escape(wildcard))

    for raw in paths:
        root = Path(raw)
        if not root.exists():
            continue
        if root.is_file():
            files.append(root)
            continue
        it = root.rglob("*") if recursive else root.iterdir()
        for fp in it:
            if fp.is_file() and (pattern is None or pattern.search(fp.name)):
                files.append(fp)

    etype = (evaluation_type or "size").strip().lower()
    if etype in {"1", "count"}:
        metric = float(len(files))
    else:
        factor = _SCALE.get(str(scale).strip().lower(), 1)
        total = sum(fp.stat().st_size for fp in files)
        metric = total / factor

    # Reuse number comparison from simple_eval
    outcome = simple_eval(
        left=str(metric),
        compare=comparevalue,
        minvalue=minvalue,
        maxvalue=maxvalue,
        fieldtype="number",
        successnumbercondition=successnumbercondition,
    )
    outcome.extra = {"metric": metric, "file_count": len(files), "evaluation_type": etype}
    outcome.message = f"files_metrics {etype}={metric} → {outcome.success}"
    return outcome


# ---------------------------------------------------------------------------
# Database helpers (Spark catalog / JDBC)
# ---------------------------------------------------------------------------


def resolve_table_name(
    table: str,
    schema: str = "",
    *,
    catalog: str = "",
    default_schema: str = "",
) -> str:
    parts = [p for p in (catalog, schema or default_schema, table) if p]
    if table and "." in table:
        return table
    return ".".join(parts) if parts else table


def _spark_session(spark: Any = None) -> Any:
    if spark is not None:
        return spark
    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except Exception:
        return None


def jdbc_url_from_connection(conn: Mapping[str, Any]) -> tuple[str, dict[str, str], list[str]]:
    """Build a JDBC URL from a Pentaho DatabaseMeta-like attribute dict."""
    warnings: list[str] = []
    ctype = str(conn.get("type") or conn.get("database_type") or "").upper()
    host = str(conn.get("server") or conn.get("hostname") or "localhost")
    port = str(conn.get("port") or "")
    database = str(conn.get("database") or conn.get("dbname") or "")
    user = str(conn.get("username") or conn.get("user") or "")
    password = str(conn.get("password") or "")
    if password.startswith("Encrypted "):
        warnings.append("PDI-encrypted password cannot be decrypted — use plain/${VAR}")

    custom = str(conn.get("url") or conn.get("jdbc_url") or "")
    if custom:
        return custom, {"user": user, "password": password}, warnings

    templates = {
        "POSTGRESQL": f"jdbc:postgresql://{host}:{port or '5432'}/{database}",
        "MYSQL": f"jdbc:mysql://{host}:{port or '3306'}/{database}",
        "MSSQLNATIVE": f"jdbc:sqlserver://{host}:{port or '1433'};databaseName={database}",
        "MSSQL": f"jdbc:sqlserver://{host}:{port or '1433'};databaseName={database}",
        "ORACLE": f"jdbc:oracle:thin:@{host}:{port or '1521'}:{database}",
        "H2": f"jdbc:h2:{database}",
        "SQLITE": f"jdbc:sqlite:{database}",
        "GENERIC": str(conn.get("database") or ""),
    }
    url = templates.get(ctype, custom)
    if not url:
        warnings.append(f"Unknown connection type {ctype!r} — set jdbc_url on the connection")
    return url, {"user": user, "password": password}, warnings


def check_db_connections(
    names: Sequence[Mapping[str, Any]],
    connection_catalog: Mapping[str, Mapping[str, Any]],
    *,
    spark: Any = None,
    timeout_seconds: float = 30,
) -> CondOutcome:
    warnings: list[str] = []
    failed: list[str] = []
    ok_names: list[str] = []
    spark = _spark_session(spark)

    for item in names:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        waitfor = float(item.get("waitfor") or 0)
        waittime = str(item.get("waittime") or "second").lower()
        scale = 1
        if waittime in {"1", "minute", "minutes"}:
            scale = 60
        elif waittime in {"2", "hour", "hours"}:
            scale = 3600
        deadline = time.time() + (waitfor * scale if waitfor else 0)
        # Always attempt at least once
        conn_meta = connection_catalog.get(name) or {}
        last_err: BaseException | None = None
        while True:
            try:
                if spark is not None and not conn_meta:
                    # No JDBC meta — probe Spark with a trivial query
                    spark.sql("SELECT 1").collect()
                    ok_names.append(name)
                    last_err = None
                    break
                url, props, w = jdbc_url_from_connection(conn_meta)
                warnings.extend(w)
                if spark is not None and url:
                    (
                        spark.read.format("jdbc")
                        .option("url", url)
                        .option("dbtable", "(SELECT 1) AS t")
                        .option("user", props.get("user", ""))
                        .option("password", props.get("password", ""))
                        .option("loginTimeout", str(int(timeout_seconds)))
                        .load()
                        .limit(1)
                        .collect()
                    )
                    ok_names.append(name)
                    last_err = None
                    break
                # Stdlib fallback: try opening a TCP socket to host:port
                host = str(conn_meta.get("server") or conn_meta.get("hostname") or "")
                port = int(conn_meta.get("port") or 0)
                if host and port:
                    import socket

                    with socket.create_connection((host, port), timeout=timeout_seconds):
                        pass
                    ok_names.append(name)
                    last_err = None
                    warnings.append(
                        f"Connection {name!r}: TCP reachability only (no JDBC driver probe)"
                    )
                    break
                raise ConnectionError(
                    f"No Spark session / JDBC URL / host:port for connection {name!r}"
                )
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                if time.time() >= deadline:
                    break
                time.sleep(1)
        if last_err is not None:
            failed.append(name)
            warnings.append(f"Connection {name!r} failed: {last_err}")

    ok = not failed and bool(ok_names)
    return CondOutcome(
        ok,
        f"ok={ok_names} failed={failed}",
        ok,
        warnings,
        error=None if ok else ConnectionError(f"DB connections failed: {failed}"),
        extra={"ok": ok_names, "failed": failed},
    )


def table_exists(
    table: str,
    schema: str = "",
    *,
    spark: Any = None,
    catalog: str = "",
    connection_meta: Mapping[str, Any] | None = None,
) -> CondOutcome:
    spark = _spark_session(spark)
    full = resolve_table_name(table, schema, catalog=catalog)
    warnings: list[str] = []
    if spark is not None:
        try:
            # Spark 3 / UC
            if hasattr(spark.catalog, "tableExists"):
                if schema and hasattr(spark.catalog, "tableExists"):
                    try:
                        exists = spark.catalog.tableExists(full)
                    except TypeError:
                        exists = spark.catalog.tableExists(schema, table)
                else:
                    exists = spark.catalog.tableExists(full)
            else:
                tables = [t.name.lower() for t in spark.catalog.listTables(schema or None)]
                exists = table.lower() in tables
            return CondOutcome(bool(exists), f"tableExists({full})={exists}", bool(exists))
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Spark catalog check failed: {exc}")
    if connection_meta:
        warnings.append("JDBC table-exists fallback not fully implemented — returning failure")
    err = FileNotFoundError(f"Table not found / unverifiable: {full}")
    return CondOutcome(False, str(err), False, warnings, error=err)


def columns_exist(
    table: str,
    columns: Sequence[str],
    schema: str = "",
    *,
    spark: Any = None,
    catalog: str = "",
    case_sensitive: bool = False,
) -> CondOutcome:
    spark = _spark_session(spark)
    full = resolve_table_name(table, schema, catalog=catalog)
    if spark is None:
        err = RuntimeError("COLUMNS_EXIST requires an active Spark session")
        return CondOutcome(False, str(err), error=err)
    try:
        df = spark.table(full)
        names = list(df.columns)
    except Exception as exc:  # noqa: BLE001
        return CondOutcome(False, str(exc), error=exc)

    if case_sensitive:
        have = set(names)
        missing = [c for c in columns if c not in have]
    else:
        have = {n.lower() for n in names}
        missing = [c for c in columns if c.lower() not in have]
    ok = not missing and bool(columns)
    return CondOutcome(
        ok,
        f"missing={missing}",
        ok,
        extra={"missing": missing, "columns": names},
        error=None if ok else KeyError(f"Missing columns: {missing}"),
    )


def _compare_count(count: int, condition: str, limit: str) -> bool:
    cond = (condition or "rows_count_equal").strip().lower()
    try:
        threshold = int(float(limit or "0"))
    except ValueError:
        threshold = 0
    mapping = {
        "rows_count_equal": count == threshold,
        "equal": count == threshold,
        "rows_count_different": count != threshold,
        "different": count != threshold,
        "rows_count_smaller": count < threshold,
        "smaller": count < threshold,
        "rows_count_smaller_equal": count <= threshold,
        "smaller_equal": count <= threshold,
        "rows_count_greater": count > threshold,
        "greater": count > threshold,
        "rows_count_greater_equal": count >= threshold,
        "greater_equal": count >= threshold,
    }
    return mapping.get(cond, count == threshold)


def eval_table_row_count(
    *,
    table: str = "",
    schema: str = "",
    custom_sql: str = "",
    use_custom_sql: bool = False,
    success_condition: str = "rows_count_equal",
    limit: str = "0",
    spark: Any = None,
    catalog: str = "",
) -> CondOutcome:
    spark = _spark_session(spark)
    if spark is None:
        err = RuntimeError("EVAL_TABLE_CONTENT requires an active Spark session")
        return CondOutcome(False, str(err), error=err)
    try:
        if use_custom_sql and custom_sql.strip():
            sql = custom_sql.strip().rstrip(";")
            # If custom SQL is not already a COUNT, wrap row count via temp view
            lower = sql.lower()
            if "count(" in lower and lower.startswith("select"):
                rows = spark.sql(sql).collect()
                count = int(rows[0][0]) if rows else 0
            else:
                count = spark.sql(sql).count()
        else:
            full = resolve_table_name(table, schema, catalog=catalog)
            count = spark.table(full).count()
    except Exception as exc:  # noqa: BLE001
        return CondOutcome(False, str(exc), error=exc)

    ok = _compare_count(count, success_condition, limit)
    return CondOutcome(
        ok,
        f"count={count} condition={success_condition} limit={limit} → {ok}",
        count,
        extra={"count": count},
        error=None if ok else ValueError(f"Row count condition failed (count={count})"),
    )


def wait_for_sql(
    *,
    table: str = "",
    schema: str = "",
    custom_sql: str = "",
    use_custom_sql: bool = False,
    success_condition: str = "rows_count_equal",
    rows_count_value: str = "0",
    maximum_timeout: float = 0,
    check_cycle_time: float = 1,
    success_on_timeout: bool = False,
    spark: Any = None,
    catalog: str = "",
) -> CondOutcome:
    deadline = time.time() + max(float(maximum_timeout), 0)
    cycle = max(float(check_cycle_time), 0.1)
    last: CondOutcome | None = None
    while True:
        last = eval_table_row_count(
            table=table,
            schema=schema,
            custom_sql=custom_sql,
            use_custom_sql=use_custom_sql,
            success_condition=success_condition,
            limit=rows_count_value,
            spark=spark,
            catalog=catalog,
        )
        if last.success:
            return last
        if time.time() >= deadline:
            if success_on_timeout:
                return CondOutcome(
                    True,
                    "Timed out (success_on_timeout)",
                    last.value,
                    warnings=["WAIT_FOR_SQL timed out but success_on_timeout=Y"],
                    extra=last.extra,
                )
            err = TimeoutError(last.message or "WAIT_FOR_SQL timed out")
            return CondOutcome(False, str(err), last.value, error=err, extra=last.extra)
        time.sleep(cycle)


def delay_seconds(maximum_timeout: float, scaletime: str | int = 0) -> float:
    scale = str(scaletime).strip().lower()
    mult = 1.0
    if scale in {"1", "minute", "minutes"}:
        mult = 60.0
    elif scale in {"2", "hour", "hours"}:
        mult = 3600.0
    return float(maximum_timeout or 0) * mult
