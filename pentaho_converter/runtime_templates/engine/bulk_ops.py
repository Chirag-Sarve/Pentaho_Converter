"""Bulk-Loading job entry helpers (MySQL export/load, MSSQL load).

Spark/JDBC-first implementations for Databricks — avoids ``mysqldump``,
``mysql``, and ``bcp`` CLI tools unless no Spark path is available.
"""

from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from .condition_ops import jdbc_url_from_connection

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
class BulkOutcome:
    success: bool
    message: str = ""
    paths: list[str] = field(default_factory=list)
    row_count: int | None = None
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _spark_session(spark: Any = None) -> Any:
    if spark is not None:
        return spark
    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except Exception:
        return None


def _qualified_table(schema: str, table: str) -> str:
    schema = (schema or "").strip()
    table = (table or "").strip()
    if not table:
        return ""
    if "." in table:
        return table
    return f"{schema}.{table}" if schema else table


def _split_column_list(raw: str) -> list[str]:
    if not raw or not str(raw).strip():
        return []
    return [c.strip() for c in re.split(r"[,;]", str(raw)) if c.strip()]


def _resolve_output_path(path: Path, iffileexists: str | int) -> tuple[Path | None, list[str]]:
    """Return destination path or None to skip. ``iffileexists``: 0=unique, 1=do nothing, 2=fail, 3=overwrite."""
    warnings: list[str] = []
    mode = str(iffileexists).strip()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        return path, warnings
    if mode in {"1", "do_nothing"}:
        warnings.append(f"Output exists — skipped (iffileexists={mode}): {path}")
        return None, warnings
    if mode in {"2", "fail", ""}:
        raise FileExistsError(f"Output file already exists: {path}")
    if mode in {"0", "unique", "create_new"}:
        stem, suffix = path.stem, path.suffix
        n = 1
        while True:
            candidate = path.with_name(f"{stem}_{n}{suffix}")
            if not candidate.exists():
                return candidate, warnings
            n += 1
    # 3 / overwrite
    return path, warnings


def _jdbc_reader(spark: Any, url: str, props: Mapping[str, str], dbtable: str) -> Any:
    reader = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", props.get("user", ""))
        .option("password", props.get("password", ""))
    )
    return reader.load()


def _jdbc_writer(
    df: Any,
    url: str,
    props: Mapping[str, str],
    table: str,
    *,
    mode: str = "append",
    batchsize: int | None = None,
) -> None:
    writer = (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", props.get("user", ""))
        .option("password", props.get("password", ""))
        .mode(mode)
    )
    if batchsize and batchsize > 0:
        writer = writer.option("batchsize", str(batchsize))
    writer.save()


# ---------------------------------------------------------------------------
# MySQL Bulk File (export table → file)
# ---------------------------------------------------------------------------


def mysql_bulk_file(
    *,
    connection_meta: Mapping[str, Any] | None,
    connection_name: str = "",
    schema: str = "",
    table: str = "",
    filename: str = "",
    separator: str = "\t",
    enclosed: str = "",
    optionenclosed: bool = False,
    lineterminated: str = "\n",
    limitlines: str = "0",
    listcolumn: str = "",
    highpriority: bool = False,
    outdumpvalue: str | int = 0,
    iffileexists: str | int = 2,
    spark: Any = None,
) -> BulkOutcome:
    warnings: list[str] = []
    if highpriority:
        warnings.append("highpriority=Y is ignored (Spark JDBC path)")
    if optionenclosed:
        warnings.append("optionenclosed=Y approximated with standard CSV quoting")
    if str(outdumpvalue).strip() in {"1"}:
        warnings.append(
            "outdumpvalue=DUMPFILE (binary) is unsupported — writing text CSV instead"
        )

    if not table:
        err = ValueError("MYSQL_BULK_FILE tablename is empty")
        return BulkOutcome(False, str(err), error=err)
    if not filename:
        err = ValueError("MYSQL_BULK_FILE filename is empty")
        return BulkOutcome(False, str(err), error=err)

    try:
        out_path, w = _resolve_output_path(Path(filename), iffileexists)
        warnings.extend(w)
    except FileExistsError as exc:
        return BulkOutcome(False, str(exc), error=exc)
    if out_path is None:
        return BulkOutcome(True, "Skipped existing file", [], warnings=warnings)

    columns = _split_column_list(listcolumn)
    qtable = _qualified_table(schema, table)
    select_list = ", ".join(f"`{c}`" if c else c for c in columns) if columns else "*"
    limit = 0
    try:
        limit = int(float(limitlines or "0"))
    except ValueError:
        warnings.append(f"Invalid limitlines={limitlines!r} — ignored")
    query = f"(SELECT {select_list} FROM {qtable}"
    if limit > 0:
        query += f" LIMIT {limit}"
    query += ") AS bulk_export"

    spark_sess = _spark_session(spark)
    conn_meta = connection_meta or {}
    url, props, jw = jdbc_url_from_connection(conn_meta)
    warnings.extend(jw)
    if connection_name and not conn_meta:
        warnings.append(
            f"Connection {connection_name!r} metadata missing — "
            "set job <connection> or config.connections"
        )

    sep = separator if separator is not None and separator != "" else "\t"
    enc = enclosed or ""

    if spark_sess is not None and url:
        try:
            df = _jdbc_reader(spark_sess, url, props, query)
            # Write via Spark CSV then optionally rename single part file
            tmp_dir = out_path.with_suffix(out_path.suffix + ".spark_out")
            (
                df.write.mode("overwrite")
                .option("header", "false")
                .option("delimiter", sep)
                .option("quote", enc if enc else "\u0000")
                .option("escape", "\\")
                .option("nullValue", "\\N")
                .option("emptyValue", "")
                .csv(str(tmp_dir))
            )
            # Coalesce part files into the target path for Pentaho-like single file
            parts = sorted(tmp_dir.glob("part-*"))
            if not parts:
                # Databricks may use different part naming
                parts = sorted(p for p in tmp_dir.rglob("*") if p.is_file() and not p.name.startswith("_"))
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("wb") as dest:
                for part in parts:
                    dest.write(part.read_bytes())
            # cleanup best-effort
            shutil.rmtree(tmp_dir, ignore_errors=True)
            row_count = df.count()
            return BulkOutcome(
                True,
                f"Exported {row_count} row(s) → {out_path}",
                [str(out_path)],
                row_count,
                warnings,
                extra={"table": qtable, "mode": "spark_jdbc"},
            )
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Spark JDBC export failed: {exc}")
            return BulkOutcome(False, str(exc), error=exc, warnings=warnings)

    # Local fallback without Spark: not connected to MySQL — fail clearly
    err = RuntimeError(
        "MYSQL_BULK_FILE requires Spark + JDBC connection metadata on Databricks"
    )
    return BulkOutcome(False, str(err), error=err, warnings=warnings)


# ---------------------------------------------------------------------------
# MySQL Bulk Load (file → table)
# ---------------------------------------------------------------------------


def mysql_bulk_load(
    *,
    connection_meta: Mapping[str, Any] | None,
    connection_name: str = "",
    schema: str = "",
    table: str = "",
    filename: str = "",
    separator: str = "\t",
    enclosed: str = "",
    escaped: str = "\\",
    linestarted: str = "",
    lineterminated: str = "\n",
    replacedata: bool = True,
    ignorelines: str = "0",
    listattribut: str = "",
    localinfile: bool = True,
    prorityvalue: str | int = 0,
    spark: Any = None,
) -> BulkOutcome:
    warnings: list[str] = []
    if localinfile:
        warnings.append(
            "localinfile=Y (LOAD DATA LOCAL INFILE) is unavailable on Databricks — "
            "using Spark JDBC write instead"
        )
    if linestarted:
        warnings.append("linestarted is not applied on the Spark CSV reader")
    if str(prorityvalue).strip() not in {"", "0", "-1"}:
        warnings.append(f"prorityvalue={prorityvalue!r} ignored (Spark path)")

    if not table:
        err = ValueError("MYSQL_BULK_LOAD tablename is empty")
        return BulkOutcome(False, str(err), error=err)
    path = Path(filename)
    if not filename or not path.exists():
        err = FileNotFoundError(f"Input file not found: {filename}")
        return BulkOutcome(False, str(err), error=err)

    qtable = _qualified_table(schema, table)
    columns = _split_column_list(listattribut)
    try:
        skip = int(float(ignorelines or "0"))
    except ValueError:
        skip = 0
        warnings.append(f"Invalid ignorelines={ignorelines!r} — using 0")

    sep = separator if separator not in (None, "") else "\t"
    spark_sess = _spark_session(spark)
    conn_meta = connection_meta or {}
    url, props, jw = jdbc_url_from_connection(conn_meta)
    warnings.extend(jw)
    if connection_name and not conn_meta:
        warnings.append(f"Connection {connection_name!r} metadata missing")

    mode = "overwrite" if replacedata else "append"

    if spark_sess is not None and url:
        try:
            reader = (
                spark_sess.read.format("csv")
                .option("header", "false")
                .option("delimiter", sep)
                .option("quote", enclosed if enclosed else "\u0000")
                .option("escape", escaped or "\\")
                .option("nullValue", "\\N")
                .option("mode", "PERMISSIVE")
            )
            df = reader.load(str(path))
            if skip > 0:
                # Approximate IGNORE n LINES by dropping first n rows (driver-side)
                warnings.append(
                    f"ignorelines={skip} approximated by dropping first {skip} row(s)"
                )
                df = (
                    df.rdd.zipWithIndex()
                    .filter(lambda x: x[1] >= skip)
                    .map(lambda x: x[0])
                    .toDF(df.schema)
                )
            if columns:
                # Rename _c0,_c1,… to attribute list
                existing = df.columns
                rename_map = {}
                for idx, col in enumerate(columns):
                    if idx < len(existing):
                        rename_map[existing[idx]] = col
                for old, new in rename_map.items():
                    df = df.withColumnRenamed(old, new)
                df = df.select(*columns)
            _jdbc_writer(df, url, props, qtable, mode=mode)
            count = df.count()
            return BulkOutcome(
                True,
                f"Loaded {count} row(s) → {qtable}",
                [str(path)],
                count,
                warnings,
                extra={"table": qtable, "mode": mode, "path": "spark_jdbc"},
            )
        except Exception as exc:  # noqa: BLE001
            return BulkOutcome(False, str(exc), error=exc, warnings=warnings)

    err = RuntimeError(
        "MYSQL_BULK_LOAD requires Spark + JDBC connection metadata on Databricks"
    )
    return BulkOutcome(False, str(err), error=err, warnings=warnings)


# ---------------------------------------------------------------------------
# MSSQL Bulk Load (file → table)
# ---------------------------------------------------------------------------


def mssql_bulk_load(
    *,
    connection_meta: Mapping[str, Any] | None,
    connection_name: str = "",
    schema: str = "",
    table: str = "",
    filename: str = "",
    datafiletype: str = "char",
    fieldterminator: str = ",",
    lineterminated: str = "\n",
    codepage: str = "OEM",
    specificcodepage: str = "",
    formatfilename: str = "",
    firetriggers: bool = False,
    checkconstraints: bool = False,
    keepnulls: bool = False,
    keepidentity: bool = False,
    tablock: bool = False,
    startfile: str = "0",
    endfile: str = "0",
    orderby: str = "",
    orderdirection: str = "",
    maxerrors: str = "0",
    batchsize: str = "0",
    rowsperbatch: str = "0",
    errorfilename: str = "",
    adddatetime: bool = False,
    truncate: bool = False,
    spark: Any = None,
) -> BulkOutcome:
    warnings: list[str] = []
    # bcp-specific options → warnings
    for flag, label in (
        (firetriggers, "firetriggers"),
        (checkconstraints, "checkconstraints"),
        (keepnulls, "keepnulls"),
        (keepidentity, "keepidentity"),
        (tablock, "tablock"),
        (bool(formatfilename), "formatfilename"),
        (bool(errorfilename), "errorfilename"),
        (bool(orderby), "orderby"),
        (adddatetime, "adddatetime"),
        (str(datafiletype).lower() not in {"char", "native", ""}, f"datafiletype={datafiletype}"),
    ):
        if flag:
            warnings.append(
                f"{label} is a bcp-specific option — not applied on Spark JDBC path"
            )
    if codepage and codepage.upper() not in {"OEM", "ACP", "RAW", ""}:
        warnings.append(f"codepage={codepage!r} ignored (UTF-8 CSV reader)")
    if specificcodepage:
        warnings.append(f"specificcodepage={specificcodepage!r} ignored")

    if not table:
        err = ValueError("MSSQL_BULK_LOAD tablename is empty")
        return BulkOutcome(False, str(err), error=err)
    path = Path(filename)
    if not filename or not path.exists():
        err = FileNotFoundError(f"Input file not found: {filename}")
        return BulkOutcome(False, str(err), error=err)

    qtable = _qualified_table(schema, table)
    sep = fieldterminator if fieldterminator not in (None, "") else ","
    try:
        batch = int(float(batchsize or "0"))
    except ValueError:
        batch = 0
    try:
        start = int(float(startfile or "0"))
    except ValueError:
        start = 0
    try:
        end = int(float(endfile or "0"))
    except ValueError:
        end = 0

    spark_sess = _spark_session(spark)
    conn_meta = connection_meta or {}
    # Prefer MSSQL JDBC type if connection type missing
    if conn_meta and not conn_meta.get("type"):
        conn_meta = dict(conn_meta)
        conn_meta["type"] = "MSSQLNATIVE"
    url, props, jw = jdbc_url_from_connection(conn_meta)
    warnings.extend(jw)
    if connection_name and not conn_meta:
        warnings.append(f"Connection {connection_name!r} metadata missing")

    mode = "overwrite" if truncate else "append"

    if spark_sess is not None and url:
        try:
            if truncate:
                # Prefer truncate via SQL when possible
                try:
                    spark_sess.sql(f"TRUNCATE TABLE {qtable}")
                    warnings.append(
                        "truncate applied via spark.sql — may require Unity Catalog / JDBC SQL"
                    )
                    mode = "append"
                except Exception as exc:  # noqa: BLE001
                    warnings.append(
                        f"TRUNCATE via spark.sql failed ({exc}); using write mode=overwrite"
                    )
                    mode = "overwrite"

            df = (
                spark_sess.read.format("csv")
                .option("header", "false")
                .option("delimiter", sep)
                .option("nullValue", "")
                .option("mode", "PERMISSIVE")
                .load(str(path))
            )
            if start > 0 or end > 0:
                warnings.append(
                    f"startfile/endfile ({start}/{end}) approximated via row index filter"
                )
                rdd = df.rdd.zipWithIndex()
                if end > 0:
                    rdd = rdd.filter(lambda x: start <= x[1] < end)
                elif start > 0:
                    rdd = rdd.filter(lambda x: x[1] >= start)
                df = rdd.map(lambda x: x[0]).toDF(df.schema)

            _jdbc_writer(
                df,
                url,
                props,
                qtable,
                mode=mode,
                batchsize=batch if batch > 0 else None,
            )
            count = df.count()
            if rowsperbatch and str(rowsperbatch) not in {"0", ""}:
                warnings.append(f"rowsperbatch={rowsperbatch} ignored (Spark batchsize used)")
            if maxerrors and str(maxerrors) not in {"0", ""}:
                warnings.append(f"maxerrors={maxerrors} ignored on Spark path")
            return BulkOutcome(
                True,
                f"Loaded {count} row(s) → {qtable}",
                [str(path)],
                count,
                warnings,
                extra={"table": qtable, "mode": mode, "path": "spark_jdbc"},
            )
        except Exception as exc:  # noqa: BLE001
            return BulkOutcome(False, str(exc), error=exc, warnings=warnings)

    err = RuntimeError(
        "MSSQL_BULK_LOAD requires Spark + JDBC connection metadata on Databricks"
    )
    return BulkOutcome(False, str(err), error=err, warnings=warnings)
