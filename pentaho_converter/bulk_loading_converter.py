"""Convert Pentaho Bulk Loading steps to Databricks-compatible PySpark writes.

Vendor-native loaders (psql COPY, sqlldr, LOAD DATA, FastLoad, TPT, gpload,
Vertica COPY STREAM, MonetDB COPY, Infobright Loader, VectorWise) have no
Databricks equivalent. Every configuration property is preserved; migration
continues with:

1. Primary: Delta Lake ``saveAsTable`` (Unity Catalog)
2. Fallback: Spark JDBC writer (``format('jdbc')``) for RDBMS-target jobs
3. Optional: ``COPY INTO`` hints when Pentaho staging files are present
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .generation_config import GenerationConfig
from .table_names import table_write_lines

logger = logging.getLogger(__name__)

# JDBC drivers for Spark JDBC fallback (connection URL comes from secrets).
_JDBC_DRIVER_BY_VENDOR: dict[str, str] = {
    "Greenplum": "org.postgresql.Driver",
    "Infobright": "com.mysql.cj.jdbc.Driver",
    "Ingres VectorWise": "com.ingres.jdbc.IngresDriver",
    "MonetDB": "nl.cwi.monetdb.jdbc.MonetDriver",
    "MySQL": "com.mysql.cj.jdbc.Driver",
    "Oracle": "oracle.jdbc.OracleDriver",
    "PostgreSQL": "org.postgresql.Driver",
    "Teradata FastLoad": "com.teradata.jdbc.TeraDriver",
    "Teradata TPT": "com.teradata.jdbc.TeraDriver",
    "Vertica": "com.vertica.jdbc.Driver",
}

_PRESERVE_KEYS = (
    "connection",
    "schema",
    "table",
    "database",
    "db_name_override",
    "truncate",
    "load_method",
    "load_action",
    "bulk_load_mode",
    "commit_size",
    "batch_size",
    "buffer_size",
    "bind_size",
    "read_size",
    "delimiter",
    "enclosure",
    "escape_char",
    "null_string",
    "encoding",
    "charset",
    "compression",
    "file_format",
    "fifo_file",
    "data_file",
    "control_file",
    "log_file",
    "bad_file",
    "discard_file",
    "error_file",
    "load_file",
    "psql_path",
    "sqlldr_path",
    "gpload_path",
    "mclient_path",
    "vwload_path",
    "fastload_path",
    "tpt_path",
    "tbuild_path",
    "tpt_operator",
    "tpt_job_name",
    "agent_host",
    "agent_port",
    "stop_on_error",
    "max_errors",
    "reject_errors",
    "reject_limit",
    "ignore_errors",
    "erase_files",
    "direct",
    "parallel",
    "parallel_degree",
    "local_infile",
    "replace_data",
    "ignore_duplicates",
    "continue_on_error",
    "transaction_size",
    "stream_name",
    "copy_statement",
    "table_space",
    "error_table",
    "work_table",
    "log_table",
    "sessions",
    "max_sessions",
    "min_sessions",
    "pack_factor",
    "fill_record",
    "explicit_dates",
    "date_mask",
    "date_format",
    "time_format",
    "timestamp_format",
    "key_fields",
    "fields",
    "extras",
    "vendor",
    "native_loader",
)


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _yn(raw: Any, default: bool = False) -> bool:
    if raw is None or raw == "":
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().upper() in ("Y", "YES", "TRUE", "1")


def _str(meta: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        val = meta.get(key)
        if val not in (None, "", [], {}):
            return str(val)
    return default


def _resolve_mode(meta: dict[str, Any]) -> str:
    """Map Pentaho truncate / loadAction to Spark save mode."""
    truncate = _yn(meta.get("truncate"))
    action = _str(meta, "load_action", "bulk_load_mode").strip().upper()
    if truncate or action in ("TRUNCATE", "REPLACE", "OVERWRITE"):
        return "overwrite"
    return "append"


def _qualified_jdbc_table(schema: str, table: str) -> str:
    table_name = (table or "target_table").strip() or "target_table"
    schema_name = (schema or "").strip()
    if schema_name:
        return f"{schema_name}.{table_name}"
    return table_name


def _field_mapping_lines(
    fields: list[Any],
    *,
    in_df: str,
    mapped_var: str,
) -> tuple[list[str], str]:
    """Apply stream→table field renames when mappings differ."""
    select_cols: list[str] = []
    for item in fields:
        if not isinstance(item, dict):
            continue
        stream = (item.get("stream_field") or item.get("name") or "").strip()
        target = (item.get("table_field") or item.get("field_name") or stream).strip()
        if not stream:
            continue
        if target and stream != target:
            select_cols.append(f"col({stream!r}).alias({target!r})")
        else:
            select_cols.append(f"col({stream!r})")
    if not select_cols:
        return [], in_df
    return [f"{mapped_var} = {in_df}.select({', '.join(select_cols)})"], mapped_var


def _null_string_lines(write_df: str, null_string: str, out_ident: str) -> tuple[list[str], str]:
    """Replace Pentaho null-string sentinels with Spark nulls before write."""
    if not null_string:
        return [], write_df
    var = f"_bulk_nulls_{out_ident}"
    lines = [
        f"# Null handling: replace Pentaho null-string {null_string!r} with Spark null",
        f"{var} = {write_df}.replace({null_string!r}, None)",
    ]
    return lines, var


def _preserve_comments(meta: dict[str, Any], *, skip_keys: frozenset[str] | None = None) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set(skip_keys or ())
    skip = frozenset({
        "step_type", "step_name", "attributes", "transformation_parameters",
        "_propagated_keys", "_propagation_trace", "output_columns",
    }) | seen

    def _emit(key: str, val: object) -> None:
        low = key.lower()
        if any(tok in low for tok in ("password", "passwd", "secret")):
            lines.append(f"# preserved.{key}=<redacted>")
        else:
            lines.append(f"# preserved.{key}={val!r}")

    for key in _PRESERVE_KEYS:
        if key in skip:
            continue
        val = meta.get(key)
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)

    for key, val in meta.items():
        if key in seen or key in skip:
            continue
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    return lines


def _staging_file_hint(meta: dict[str, Any]) -> list[str]:
    """Document optional Databricks COPY INTO when Pentaho used staging files."""
    path = _str(meta, "fifo_file", "data_file", "load_file", "control_file")
    if not path:
        return []
    return [
        "# Optional Databricks path (manual): stage files then COPY INTO Unity Catalog table.",
        f"# COPY INTO hint source (Pentaho staging path): {path!r}",
        "# Prefer Delta saveAsTable below unless external files must be retained.",
    ]


def _vendor_notes(vendor: str, meta: dict[str, Any]) -> list[str]:
    """Emit transformation-specific unsupported / option notes."""
    lines: list[str] = []
    v = vendor.lower()
    if "greenplum" in v:
        lines.append(
            "# Greenplum: gpload YAML/control-file bulk options are unsupported on Databricks."
        )
        if _str(meta, "gpload_path", "control_file", "max_errors"):
            lines.append(
                f"# Greenplum bulk options gpload={_str(meta, 'gpload_path')!r} "
                f"control={_str(meta, 'control_file')!r} max_errors={_str(meta, 'max_errors')!r}"
            )
    elif "infobright" in v:
        lines.append(
            "# Infobright: bhloader / Infobright agent protocol unsupported; "
            "batch/agent settings preserved only."
        )
        if _str(meta, "agent_host", "agent_port", "batch_size", "data_file"):
            lines.append(
                f"# Infobright loader options agent={_str(meta, 'agent_host')!r}:"
                f"{_str(meta, 'agent_port')!r} batch={_str(meta, 'batch_size')!r} "
                f"data_file={_str(meta, 'data_file')!r}"
            )
    elif "vectorwise" in v or "ingres" in v:
        lines.append(
            "# Ingres VectorWise: vwload binary unsupported; connection/batch preserved."
        )
    elif "monetdb" in v:
        lines.append(
            "# MonetDB: mclient COPY INTO unsupported; Spark JDBC fallback emitted below."
        )
        if _str(meta, "mclient_path", "copy_statement", "buffer_size"):
            lines.append(
                f"# MonetDB COPY settings mclient={_str(meta, 'mclient_path')!r} "
                f"copy={_str(meta, 'copy_statement')!r} buffer={_str(meta, 'buffer_size')!r}"
            )
    elif vendor == "MySQL":
        lines.append(
            "# MySQL: LOAD DATA [LOCAL] INFILE / named-pipe FIFO unsupported on Databricks."
        )
        lines.append(
            f"# LOAD DATA props local_infile={_str(meta, 'local_infile')!r} "
            f"fifo={_str(meta, 'fifo_file')!r} replace={_str(meta, 'replace_data')!r} "
            f"ignore_dupes={_str(meta, 'ignore_duplicates')!r} "
            f"charset={_str(meta, 'encoding', 'charset')!r}"
        )
    elif vendor == "Oracle":
        lines.append(
            "# Oracle: SQL*Loader (sqlldr) control/data/bad/discard files unsupported."
        )
        lines.append(
            f"# SQL*Loader props sqlldr={_str(meta, 'sqlldr_path')!r} "
            f"control={_str(meta, 'control_file')!r} data={_str(meta, 'data_file')!r} "
            f"direct={_str(meta, 'direct')!r} bind={_str(meta, 'bind_size')!r} "
            f"read={_str(meta, 'read_size')!r} commit={_str(meta, 'commit_size')!r}"
        )
    elif "postgresql" in v:
        lines.append(
            "# PostgreSQL: psql COPY FROM STDIN unsupported; JDBC batch fallback emitted."
        )
        lines.append(
            f"# COPY props psql={_str(meta, 'psql_path')!r} delimiter={_str(meta, 'delimiter')!r} "
            f"enclosure={_str(meta, 'enclosure')!r} escape={_str(meta, 'escape_char')!r} "
            f"nullif={_str(meta, 'null_string')!r} encoding={_str(meta, 'encoding')!r}"
        )
    elif "fastload" in v:
        lines.append(
            "# Teradata FastLoad: FastLoad client/sessions/error tables unsupported on Databricks."
        )
        lines.append(
            f"# FastLoad options path={_str(meta, 'fastload_path')!r} "
            f"sessions={_str(meta, 'sessions', 'max_sessions')!r} "
            f"error_table={_str(meta, 'error_table')!r} log_table={_str(meta, 'log_table')!r}"
        )
    elif "tpt" in v:
        lines.append(
            "# Teradata TPT: tbuild / TPT operators (Load/Update/Stream) unsupported."
        )
        lines.append(
            f"# TPT operator={_str(meta, 'tpt_operator')!r} "
            f"tbuild={_str(meta, 'tbuild_path', 'tpt_path')!r} "
            f"job={_str(meta, 'tpt_job_name')!r} pack={_str(meta, 'pack_factor')!r} "
            f"sessions={_str(meta, 'sessions', 'max_sessions')!r} "
            f"load_action={_str(meta, 'load_action')!r} keys={meta.get('key_fields')!r}"
        )
    elif "vertica" in v:
        lines.append(
            "# Vertica: VerticaCopyStream / COPY FROM STDIN unsupported; JDBC fallback emitted."
        )
        lines.append(
            f"# Vertica COPY options stream={_str(meta, 'stream_name')!r} "
            f"copy={_str(meta, 'copy_statement')!r} "
            f"batch={_str(meta, 'batch_size', 'buffer_size')!r}"
        )
    return lines


def _jdbc_fallback_lines(
    *,
    vendor: str,
    write_df: str,
    schema: str,
    table: str,
    connection: str,
    mode: str,
    batch_size: str,
    commit_size: str,
) -> list[str]:
    """Emit Spark JDBC writer fallback for RDBMS targets (disabled by default)."""
    driver = _JDBC_DRIVER_BY_VENDOR.get(vendor, "org.postgresql.Driver")
    dbtable = _qualified_jdbc_table(schema, table)
    secret_scope = re.sub(r"[^0-9A-Za-z_]", "_", (connection or vendor).lower()) or "jdbc"
    batch = batch_size or commit_size or "10000"
    conn_label = connection or vendor

    lines = [
        "# --- JDBC FALLBACK (disabled) ---",
        f"# Use when the migration must write back to the source {vendor} RDBMS",
        "# instead of (or in addition to) Unity Catalog Delta. Requires the vendor",
        "# JDBC driver on the cluster classpath and a secret-backed JDBC URL.",
        f"_USE_{_safe_ident(vendor).upper()}_JDBC_FALLBACK = False  # set True to enable",
        f"if _USE_{_safe_ident(vendor).upper()}_JDBC_FALLBACK:",
        f"    # preserved.connection={conn_label!r}",
        f"    _jdbc_url = dbutils.secrets.get(scope={secret_scope!r}, key='jdbc_url')",
        f"    _jdbc_user = dbutils.secrets.get(scope={secret_scope!r}, key='username')",
        f"    _jdbc_password = dbutils.secrets.get(scope={secret_scope!r}, key='password')",
        "    (",
        f"        {write_df}.write.format('jdbc')",
        "        .option('url', _jdbc_url)",
        f"        .option('driver', {driver!r})",
        f"        .option('dbtable', {dbtable!r})",
        "        .option('user', _jdbc_user)",
        "        .option('password', _jdbc_password)",
        f"        .option('batchsize', {batch!r})",
        f"        .mode({mode!r})",
        "        .save()",
        "    )",
    ]
    return lines


def convert_bulk_loader_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    *,
    vendor: str,
    native_loader: str,
    generation_config: GenerationConfig | None = None,
) -> tuple[list[str], str]:
    """Generate Delta ``saveAsTable`` + JDBC fallback for a Pentaho bulk loader.

    Always returns ``partial`` because vendor-native bulk utilities are not
    available on Databricks; properties are preserved and migration continues.
    """
    meta = dict(metadata or {})
    meta.setdefault("vendor", vendor)
    meta.setdefault("native_loader", native_loader)

    schema = _str(meta, "schema", "schemaname")
    table = _str(meta, "table", "tablename")
    connection = _str(meta, "connection")
    mode = _resolve_mode(meta)
    commit_size = _str(meta, "commit_size", "commit")
    batch_size = _str(meta, "batch_size", "bulk_size")
    buffer_size = _str(meta, "buffer_size", "bind_size")
    max_errors = _str(meta, "max_errors", "reject_limit", "reject_errors")
    stop_on_error = meta.get("stop_on_error")
    null_string = _str(meta, "null_string")
    encoding = _str(meta, "encoding", "charset")
    delimiter = _str(meta, "delimiter")
    enclosure = _str(meta, "enclosure")
    escape = _str(meta, "escape_char", "escape")
    parallel = meta.get("parallel") or _str(meta, "parallel_degree", "sessions", "max_sessions")
    fields = meta.get("fields") if isinstance(meta.get("fields"), list) else []
    out_ident = _safe_ident(out_var)

    lines: list[str] = [
        f"# {vendor} Bulk Loader: {step_name}",
        f"# UNSUPPORTED: Native '{native_loader}' has no Databricks equivalent.",
        "# WARNING: Vendor bulk-load utilities (client binaries, named pipes, "
        "FastLoad/TPT, gpload, sqlldr, LOAD DATA LOCAL) are not available. "
        "Primary path: Delta Lake saveAsTable. Fallback: Spark JDBC writer.",
        "# Edge cases: empty DF writes 0 rows; null-string sentinels replaced when set; "
        "duplicate/constraint failures abort the Spark job (no vendor reject/bad file); "
        "no partial-commit/rollback of sqlldr/FastLoad/TPT.",
    ]
    lines.extend(_vendor_notes(vendor, meta))
    lines.extend(_staging_file_hint(meta))

    if not in_df:
        lines.append("# WARNING: No upstream DataFrame — empty dataset; write skipped.")
        lines.append("from pyspark.sql.types import StructType")
        lines.append(f"{out_var} = spark.createDataFrame([], StructType([]))")
        lines.extend(_preserve_comments(meta))
        logger.warning("%s '%s': missing input DataFrame", vendor, step_name)
        return lines, "partial"

    mapped_var = f"_bulk_mapped_{out_ident}"
    prep, write_df = _field_mapping_lines(fields, in_df=in_df, mapped_var=mapped_var)
    lines.extend(prep)

    null_lines, write_df = _null_string_lines(write_df, null_string, out_ident)
    lines.extend(null_lines)

    if encoding:
        lines.append(
            f"# preserved.encoding={encoding!r}  "
            "# relevant for staged-file / COPY INTO / JDBC charset"
        )
    if delimiter or enclosure or escape:
        lines.append(
            f"# preserved.file_format delimiter={delimiter!r} enclosure={enclosure!r} "
            f"escape={escape!r}"
        )
    if commit_size or batch_size or buffer_size:
        lines.append(
            f"# preserved.performance commit_size={commit_size!r} "
            f"batch_size={batch_size!r} buffer_size={buffer_size!r}  "
            "# mapped to JDBC batchsize in fallback; Delta autoscales"
        )
    if max_errors or stop_on_error not in (None, ""):
        lines.append(
            f"# WARNING: Reject limits / stop_on_error (max_errors={max_errors!r}, "
            f"stop_on_error={stop_on_error!r}) are not enforced by saveAsTable/JDBC; "
            "use quarantine tables or Expectations for bad rows."
        )
    if parallel not in (None, "", False):
        lines.append(
            f"# preserved.parallel={parallel!r}  "
            "# Spark parallelism via partitions; not vendor loader sessions"
        )
    if _str(meta, "load_action", "load_method", "bulk_load_mode"):
        lines.append(
            f"# preserved.bulk_load_mode load_action={_str(meta, 'load_action')!r} "
            f"load_method={_str(meta, 'load_method')!r} → spark mode={mode!r}"
        )
    if not table:
        lines.append(
            "# WARNING: Target table name missing from Pentaho metadata — "
            "placeholder UC table reference emitted."
        )
        logger.warning("%s '%s': missing target table", vendor, step_name)

    # Primary Databricks path
    write_lines = table_write_lines(
        out_var=out_var,
        in_df=write_df,
        table=table,
        source_schema=schema,
        step_name=step_name,
        config=generation_config or GenerationConfig.defaults(),
        mode=mode,
        step_type=f"{vendor}BulkLoader",
    )
    lines.extend(write_lines)

    # JDBC fallback for RDBMS-targeted migrations
    lines.extend(
        _jdbc_fallback_lines(
            vendor=vendor,
            write_df=write_df,
            schema=schema,
            table=table,
            connection=connection,
            mode=mode,
            batch_size=batch_size,
            commit_size=commit_size,
        )
    )

    # Avoid duplicating keys already documented inline
    lines.extend(
        _preserve_comments(
            meta,
            skip_keys=frozenset({"encoding"} if encoding else ()),
        )
    )

    logger.info(
        "%s BulkLoader '%s' → table=%s mode=%s jdbc_driver=%s status=partial",
        vendor,
        step_name,
        table or "<missing>",
        mode,
        _JDBC_DRIVER_BY_VENDOR.get(vendor, "?"),
    )
    return lines, "partial"
