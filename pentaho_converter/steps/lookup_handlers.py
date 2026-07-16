"""Handlers for Pentaho Lookup transformation steps.

Supports:
- Call DB Procedure, Database Join, Dynamic SQL Row
- File Exists, Table Exists, Column Exists
- Check if File is Locked, Check if Webservice is Available
- HTTP Client, HTTP Post, REST Client, Web Services Lookup
- Fuzzy Match (fuller algorithm / threshold support)

Stream Lookup and Database Lookup remain in transform_handlers /
database_lookup_converter (already fully implemented).
"""

from __future__ import annotations

import logging
import re

from ..lineage import substitute_pentaho_variables
from ..metadata_propagation import get_converter_metadata
from ..path_utils import spark_load_path_expr
from ..step_xml import (
    get_step_element,
    parse_check_file_locked_config,
    parse_column_exists_config,
    parse_db_join_config,
    parse_db_proc_config,
    parse_dynamic_sql_row_config,
    parse_file_exists_config,
    parse_fuzzy_match_config,
    parse_http_client_config,
    parse_http_post_config,
    parse_rest_client_config,
    parse_table_exists_config,
    parse_web_services_lookup_config,
    parse_webservice_available_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)

# Algorithms Spark/Python can approximate; others are preserved with warnings.
_FUZZY_SUPPORTED = frozenset({
    "levenshtein", "levenshteindistance", "dameraulevenshtein",
    "jaro", "jarowinkler", "metaphone", "doublemetaphone",
    "soundex", "refinedsoundex", "pairwise",
})
_FUZZY_LEVENSHTEIN = frozenset({
    "levenshtein", "levenshteindistance", "dameraulevenshtein",
})


def _norm(step_type: str) -> str:
    return step_type.strip().lower().replace(" ", "").replace("(", "").replace(")", "")


def _meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


def _params(context: StepContext) -> dict:
    return context.transformation.parameters or {}


def _passthrough(context: StepContext, label: str) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [f"# {label}: {context.step.name}"]
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_empty STRING')")
    return lines, "converted"


_SKIP_PRESERVE = frozenset({
    "step_type", "step_name", "attributes", "fields", "transformation_parameters",
    "_propagated_keys", "_propagation_trace",
})
_REDACT_KEYS = frozenset({
    "password", "auth_password", "http_password", "proxy_password", "passphrase",
})


def _preserve(meta: dict, keys: tuple[str, ...] = ()) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()

    def _emit(key: str, val: object) -> None:
        if key in _REDACT_KEYS or "password" in key.lower():
            lines.append(f"# preserved.{key}=<redacted>")
        else:
            lines.append(f"# preserved.{key}={val!r}")

    for key in keys:
        if key in seen:
            continue
        val = meta.get(key)
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    for key, val in meta.items():
        if key in seen or key in _SKIP_PRESERVE:
            continue
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    return lines


def _warn(step_name: str, message: str) -> None:
    logger.warning("Lookup step '%s': %s", step_name, message)


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _resolve_str(context: StepContext, value: str) -> str:
    return substitute_pentaho_variables(value or "", _params(context))


def _merge_cfg(meta: dict, cfg: dict) -> dict:
    for key, val in cfg.items():
        meta.setdefault(key, val)
    return meta


def _timeout_seconds(raw: object, default: float = 10.0) -> float:
    try:
        val = float(raw if raw not in (None, "") else default)
    except (TypeError, ValueError):
        return default
    # Pentaho often stores milliseconds
    if val > 1000:
        return val / 1000.0
    return max(val, 0.1)


def _qualified_table(schema: str, table: str) -> str:
    schema = (schema or "").strip()
    table = (table or "").strip()
    if schema and table:
        return f"{schema}.{table}"
    return table or schema


# ---------------------------------------------------------------------------
# Call DB Procedure
# ---------------------------------------------------------------------------


class CallDBProcedureHandler(BaseStepHandler):
    """Call DB Procedure → JDBC callable via spark/jaydebeapi stub + metadata."""

    _TYPES = {"dbproc", "calldbproc", "calldbprocedure"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_db_proc_config(step_el))

        procedure = _resolve_str(context, str(meta.get("procedure") or ""))
        connection = meta.get("connection") or ""
        parameters = meta.get("parameters") or []
        results = meta.get("results") or []

        lines = [f"# Call DB Procedure: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "connection", "procedure", "auto_commit", "parameters", "results", "result_name",
        )))
        lines.append(
            "# WARNING: Stored-procedure dialects (Oracle packages, SQL Server OUTPUT, "
            "Cursor results) are database-specific and not fully portable to Spark SQL."
        )

        if not procedure:
            _warn(context.step.name, "missing procedure name — passthrough")
            lines.append("# WARNING: missing procedure name")
            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = spark.createDataFrame([], '_empty STRING')")
            return lines, "partial"

        if not connection:
            _warn(context.step.name, "missing database connection — configure JDBC URL at runtime")
            lines.append("# WARNING: missing database connection — configure JDBC URL at runtime")

        in_params = [
            p for p in parameters
            if str(p.get("direction") or "IN").upper() in ("IN", "INOUT", "")
        ]
        out_params = [
            p for p in parameters
            if str(p.get("direction") or "").upper() in ("OUT", "INOUT", "RETURN")
        ]
        placeholders = ", ".join("?" for _ in (parameters or ([None] if not parameters else [])))
        if parameters:
            call_sql = f"{{CALL {procedure}({placeholders})}}"
        else:
            call_sql = f"{{CALL {procedure}}}"
        lines.append(f"_proc_sql_{out_var} = {call_sql!r}")
        lines.append(f"# preserved.call_sql={call_sql!r}")
        if parameters:
            lines.append(f"# preserved.parameter_bindings={parameters!r}")
        if results:
            lines.append(f"# preserved.return_bindings={results!r}")

        # Bind IN args from the first input row (driver-side JDBC callable skeleton)
        lines.append("import os")
        lines.append(
            f"_jdbc_url_{out_var} = os.environ.get("
            f"'PENTAHO_JDBC_URL_{str(connection).upper().replace(' ', '_')}' "
            f"if {bool(connection)!r} else 'PENTAHO_JDBC_URL', '')"
        )
        lines.append(
            f"# WARNING: Call DB Procedure requires a JDBC driver and callable support; "
            f"dialect-specific OUT/REF CURSOR behavior is UNSUPPORTED in Spark SQL"
        )
        if in_df:
            lines.append(f"{out_var} = {in_df}")
            if in_params:
                lines.append(
                    f"# IN parameter stream bindings: "
                    f"{[p.get('name') for p in in_params]!r} — pass as callproc args from row values"
                )
                lines.append(
                    f"_in_bindings_{out_var} = {[p.get('name') for p in in_params]!r}"
                )
            for res in list(results) + [
                {"name": p.get("rename") or p.get("name")} for p in out_params
            ]:
                rname = (res.get("rename") or res.get("name") or "") if isinstance(res, dict) else ""
                if not rname:
                    continue
                lines.append(
                    f"{out_var} = {out_var}.withColumn({rname!r}, lit(None).cast('string'))"
                )
                lines.append(
                    f"# WARNING: OUT/return column {rname!r} requires JDBC callable "
                    "binding — null stub emitted"
                )
            _warn(context.step.name, "stored procedure migrated as JDBC callable stub (partial)")
            return lines, "partial"

        lines.append(f"{out_var} = spark.createDataFrame([], '_proc_stub STRING')")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Database Join
# ---------------------------------------------------------------------------


class DatabaseJoinHandler(BaseStepHandler):
    """Database Join → parameterized lookup SQL joined back to the stream."""

    _TYPES = {"dbjoin", "databasejoin"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_db_join_config(step_el))

        sql = _resolve_str(context, str(meta.get("sql") or ""))
        outer = bool(meta.get("outer_join"))
        join_how = "left" if outer else "inner"
        params = meta.get("parameters") or []
        row_limit = int(meta.get("row_limit") or 0)

        lines = [f"# Database Join: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "connection", "sql", "outer_join", "row_limit", "replace_vars", "parameters",
        )))

        if not in_df:
            if sql:
                lines.append(f"{out_var} = spark.sql({sql!r})")
                if row_limit > 0:
                    lines.append(f"{out_var} = {out_var}.limit({row_limit})")
                return lines, "converted"
            return _passthrough(context, "Database Join")

        if not sql:
            _warn(context.step.name, "missing lookup SQL — passthrough")
            lines.append("# WARNING: missing lookup SQL")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        if not meta.get("connection"):
            _warn(context.step.name, "missing database connection — using spark.sql against catalog")
            lines.append("# WARNING: missing database connection — using spark.sql against catalog")

        lines.append(f"_sql_{out_var} = {sql!r}")
        join_keys = [p.get("name") for p in params if p.get("name")]

        if "?" in sql and params:
            # Never execute SQL containing JDBC '?' via spark.sql — rewrite or JDBC path
            _warn(
                context.step.name,
                "parameterized Database Join uses JDBC '?' — emitting prepared-statement skeleton",
            )
            lines.append(
                "# WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; "
                "emitting JDBC prepared-statement skeleton (foreachPartition)."
            )
            rewritten = sql
            for idx, p in enumerate(params):
                pname = p.get("name") or f"p{idx}"
                rewritten = rewritten.replace("?", f":{pname}", 1)
            lines.append(f"# preserved.sql_template={rewritten!r}")
            lines.append(f"_param_fields_{out_var} = {join_keys!r}")
            lines.append("import os")
            lines.append(
                "# foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):"
            )
            lines.append(
                f"# def _dbjoin_partition(rows):\n"
                f"#     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>\n"
                f"#     cur = conn.prepareStatement({sql!r})\n"
                f"#     for row in rows:\n"
                f"#         for i, f in enumerate(_param_fields_{out_var}, 1):\n"
                f"#             cur.setObject(i, row[f])\n"
                f"#         rs = cur.executeQuery(); ... yield joined rows"
            )
            # Still attempt a best-effort catalog join when SQL has no '?' after stripping comments —
            # for parameterized SQL, fall back to left-preserving stream with empty lookup cols.
            lines.append(
                f"# Fallback: preserve input stream; attach empty lookup side for schema continuity"
            )
            lines.append(f"{out_var} = {in_df}")
            lines.append(
                f"# Join type preserved as {join_how!r}; join keys={join_keys!r}"
            )
            return lines, "partial"

        lines.append(f"_lkp_{out_var} = spark.sql(_sql_{out_var})")
        if row_limit > 0:
            lines.append(f"_lkp_{out_var} = _lkp_{out_var}.limit({row_limit})")

        if join_keys:
            on_cols = ", ".join(repr(c) for c in join_keys)
            lines.append(
                f"{out_var} = {in_df}.join(_lkp_{out_var}, on=[{on_cols}], how={join_how!r})"
            )
            lines.append(
                "# NOTE: assumes lookup SQL result columns share names with stream "
                "parameter fields; adjust join expressions if aliases differ"
            )
        else:
            _warn(context.step.name, "no join keys — cross-style join")
            lines.append(
                f"# WARNING: no join keys / parameters — using {join_how!r} "
                "cross-style join via temporary key"
            )
            lines.append(f"_lkp_{out_var} = _lkp_{out_var}.withColumn('_dbjoin_k', lit(1))")
            lines.append(f"_main_{out_var} = {in_df}.withColumn('_dbjoin_k', lit(1))")
            lines.append(
                f"{out_var} = _main_{out_var}.join(_lkp_{out_var}, on='_dbjoin_k', how={join_how!r})"
                f".drop('_dbjoin_k')"
            )
        return lines, "converted"


# ---------------------------------------------------------------------------
# Dynamic SQL Row
# ---------------------------------------------------------------------------


class DynamicSQLRowHandler(BaseStepHandler):
    """Dynamic SQL Row → per-row SQL from field / template with injection warnings."""

    _TYPES = {"dynamicsqlrow"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_dynamic_sql_row_config(step_el))

        sql_template = _resolve_str(context, str(meta.get("sql") or ""))
        sql_field = meta.get("sql_field") or ""
        outer = bool(meta.get("outer_join"))
        try:
            row_limit = int(meta.get("row_limit") or 0)
        except (TypeError, ValueError):
            row_limit = 0

        lines = [f"# Dynamic SQL Row: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "connection", "sql", "sql_field", "outer_join", "replace_vars",
            "query_only_on_change", "row_limit",
        )))
        lines.append(
            "# WARNING: Dynamic SQL built from stream fields risks SQL injection — "
            "sanitize inputs or use parameterized JDBC queries."
        )
        _warn(context.step.name, "Dynamic SQL Row uses driver-side execution (SQL injection risk)")

        if not in_df:
            if sql_template:
                lines.append(f"{out_var} = spark.sql({sql_template!r})")
                if row_limit > 0:
                    lines.append(f"{out_var} = {out_var}.limit({row_limit})")
                return lines, "converted"
            return _passthrough(context, "Dynamic SQL Row")

        if not sql_field and not sql_template:
            _warn(context.step.name, "missing SQL template and sql_field")
            lines.append("# WARNING: missing SQL template and sql_field")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        if not meta.get("connection"):
            _warn(context.step.name, "missing database connection")
            lines.append("# WARNING: missing database connection")

        lines.append("from pyspark.sql import Row")
        lines.append(f"_dyn_parts_{out_var} = []")
        lines.append(f"_seen_sql_{out_var} = set()")
        lines.append(f"for _row in {in_df}.toLocalIterator():")
        lines.append(f"    _row_d = _row.asDict(recursive=True)")
        if sql_field:
            lines.append(f"    _sql = str(_row_d.get({sql_field!r}) or '')")
        else:
            lines.append(f"    _sql = {sql_template!r}")
            lines.append("    # Optional: format template with row values if placeholders exist")
            lines.append("    try:")
            lines.append("        _sql = _sql.format(**{k: '' if v is None else v for k, v in _row_d.items()})")
            lines.append("    except Exception:")
            lines.append("        _sql = _sql  # keep unresolved template on format errors")
        collect_expr = "spark.sql(_sql)"
        if row_limit > 0:
            collect_expr = f"spark.sql(_sql).limit({row_limit})"
        if meta.get("query_only_on_change"):
            lines.append(f"    if _sql in _seen_sql_{out_var}:")
            lines.append("        _lkp_rows = []")
            lines.append("    else:")
            lines.append(f"        _seen_sql_{out_var}.add(_sql)")
            lines.append("        try:")
            lines.append(f"            _lkp_rows = [r.asDict(recursive=True) for r in {collect_expr}.collect()]")
            lines.append("        except Exception as _dyn_exc:")
            lines.append("            # Timeout / invalid SQL / missing objects")
            lines.append("            _lkp_rows = []")
        else:
            lines.append("    try:")
            lines.append(f"        _lkp_rows = [r.asDict(recursive=True) for r in {collect_expr}.collect()]")
            lines.append("    except Exception as _dyn_exc:")
            lines.append("        _lkp_rows = []")
        if outer:
            lines.append("    if not _lkp_rows:")
            lines.append("        _lkp_rows = [{}]")
        lines.append("    for _lkp in _lkp_rows:")
        lines.append("        _merged = dict(_row_d)")
        lines.append("        _merged.update(_lkp)")
        lines.append(f"        _dyn_parts_{out_var}.append(Row(**_merged))")
        lines.append(f"if _dyn_parts_{out_var}:")
        lines.append(f"    {out_var} = spark.createDataFrame(_dyn_parts_{out_var})")
        lines.append("else:")
        if outer:
            lines.append(f"    {out_var} = {in_df}")
        else:
            lines.append(f"    {out_var} = {in_df}.limit(0)")
        lines.append(
            "# NOTE: toLocalIterator + collect is driver-side; for large streams "
            "replace with mapPartitions + JDBC."
        )
        return lines, "partial"


# ---------------------------------------------------------------------------
# File / Table / Column Exists
# ---------------------------------------------------------------------------


class FileExistsHandler(BaseStepHandler):
    """File Exists → DBFS / local / cloud filesystem existence check."""

    _TYPES = {"fileexists"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_file_exists_config(step_el))

        filename = _resolve_str(context, str(meta.get("filename") or ""))
        filename_field = meta.get("filename_field") or ""
        result_field = meta.get("result_field") or "file_exists"
        path_expr = spark_load_path_expr(filename) if filename else "''"

        lines = [f"# File Exists: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "filename", "filename_field", "result_field", "include_file_type",
            "file_type_field", "add_filename_result",
        )))

        lines.append("import os")
        lines.append("from pyspark.sql.functions import udf")
        lines.append("from pyspark.sql.types import BooleanType")
        lines.append("def _file_exists_path(p):")
        lines.append("    if p is None or str(p).strip() == '':")
        lines.append("        return False")
        lines.append("    path = str(p)")
        lines.append("    try:")
        lines.append("        # Databricks DBFS / Volumes via dbutils when available")
        lines.append("        _dbu = globals().get('dbutils')")
        lines.append("        if _dbu is not None:")
        lines.append("            try:")
        lines.append("                _dbu.fs.ls(path)")
        lines.append("                return True")
        lines.append("            except Exception:")
        lines.append("                return False")
        lines.append("        # Hadoop FS + local fallback")
        lines.append("        try:")
        lines.append("            jvm = spark._jvm")
        lines.append("            conf = spark._jsc.hadoopConfiguration()")
        lines.append("            uri = jvm.java.net.URI(path)")
        lines.append("            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)")
        lines.append("            return bool(fs.exists(jvm.org.apache.hadoop.fs.Path(path)))")
        lines.append("        except Exception:")
        lines.append("            return bool(os.path.exists(path))")
        lines.append("    except Exception:")
        lines.append("        return False")
        lines.append("_file_exists_udf = udf(_file_exists_path, BooleanType())")

        include_type = bool(meta.get("include_file_type"))
        type_field = meta.get("file_type_field") or "filetype"

        def _maybe_filetype(target: str) -> None:
            if not include_type:
                return
            lines.append("def _file_type_of(p):")
            lines.append("    if not p or not _file_exists_path(p):")
            lines.append("        return None")
            lines.append("    import os as _os")
            lines.append("    return 'Directory' if _os.path.isdir(str(p)) else 'File'")
            if filename_field and in_df:
                lines.append("from pyspark.sql.types import StringType")
                lines.append("_ft_udf = udf(_file_type_of, StringType())")
                lines.append(
                    f"{target} = {target}.withColumn({type_field!r}, _ft_udf(col({filename_field!r})))"
                )
            else:
                lines.append(
                    f"{target} = {target}.withColumn({type_field!r}, lit(_file_type_of({path_expr})))"
                )

        if filename_field and in_df:
            lines.append(
                f"{out_var} = {in_df}.withColumn({result_field!r}, _file_exists_udf(col({filename_field!r})))"
            )
            _maybe_filetype(out_var)
            return lines, "converted"

        if not in_df:
            if not filename:
                _warn(context.step.name, "missing filename")
                lines.append("# WARNING: missing filename")
            lines.append(f"_exists_{out_var} = _file_exists_path({path_expr})")
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"[{{'path': {path_expr}, {result_field!r}: _exists_{out_var}}}])"
            )
            if include_type:
                lines.append("def _file_type_of(p):")
                lines.append("    if not p or not _file_exists_path(p):")
                lines.append("        return None")
                lines.append("    import os as _os")
                lines.append("    return 'Directory' if _os.path.isdir(str(p)) else 'File'")
                lines.append(
                    f"{out_var} = {out_var}.withColumn({type_field!r}, lit(_file_type_of({path_expr})))"
                )
            return lines, "converted" if filename else "partial"

        if filename:
            lines.append(
                f"{out_var} = {in_df}.withColumn({result_field!r}, lit(_file_exists_path({path_expr})))"
            )
            if include_type:
                lines.append("def _file_type_of(p):")
                lines.append("    if not p or not _file_exists_path(p):")
                lines.append("        return None")
                lines.append("    import os as _os")
                lines.append("    return 'Directory' if _os.path.isdir(str(p)) else 'File'")
                lines.append(
                    f"{out_var} = {out_var}.withColumn({type_field!r}, lit(_file_type_of({path_expr})))"
                )
            return lines, "converted"

        _warn(context.step.name, "missing filename / filename_field")
        lines.append("# WARNING: missing filename / filename_field")
        lines.append(f"{out_var} = {in_df}.withColumn({result_field!r}, lit(False))")
        return lines, "partial"


class TableExistsHandler(BaseStepHandler):
    """Table Exists → Spark catalog / JDBC table inspection."""

    _TYPES = {"tableexists"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_table_exists_config(step_el))

        schema = meta.get("schema") or ""
        table = meta.get("table") or ""
        table_field = meta.get("table_field") or ""
        schema_field = meta.get("schema_field") or ""
        result_field = meta.get("result_field") or "table_exists"
        qualified = _qualified_table(str(schema), str(table))

        lines = [f"# Table Exists: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "connection", "schema", "schema_field", "table", "table_field", "result_field",
        )))
        if not meta.get("connection"):
            lines.append(
                "# WARNING: missing database connection — using spark.catalog "
                "(Unity Catalog / Hive); JDBC metadata requires a configured warehouse"
            )

        lines.append("def _table_exists(name, db=None):")
        lines.append("    if not name:")
        lines.append("        return False")
        lines.append("    try:")
        lines.append("        if db:")
        lines.append("            return spark.catalog.tableExists(f'{db}.{name}')")
        lines.append("        if '.' in str(name):")
        lines.append("            return spark.catalog.tableExists(str(name))")
        lines.append("        return spark.catalog.tableExists(str(name))")
        lines.append("    except Exception:")
        lines.append("        try:")
        lines.append("            spark.table(str(name) if not db else f'{db}.{name}')")
        lines.append("            return True")
        lines.append("        except Exception:")
        lines.append("            return False")

        if table_field and in_df:
            lines.append("from pyspark.sql.functions import udf")
            lines.append("from pyspark.sql.types import BooleanType")
            if schema_field:
                lines.append(
                    "def _row_table_exists(t, s):"
                )
                lines.append("    return _table_exists(t, s)")
                lines.append("_te_udf = udf(_row_table_exists, BooleanType())")
                lines.append(
                    f"{out_var} = {in_df}.withColumn("
                    f"{result_field!r}, _te_udf(col({table_field!r}), col({schema_field!r})))"
                )
            else:
                lines.append("_te_udf = udf(lambda t: _table_exists(t, None), BooleanType())")
                lines.append(
                    f"{out_var} = {in_df}.withColumn({result_field!r}, _te_udf(col({table_field!r})))"
                )
            return lines, "converted"

        if not in_df:
            lines.append(f"_exists_{out_var} = _table_exists({table!r}, {schema!r} or None)")
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"[{{'table': {qualified!r}, {result_field!r}: _exists_{out_var}}}])"
            )
            return lines, "converted" if table else "partial"

        if table:
            lines.append(
                f"{out_var} = {in_df}.withColumn("
                f"{result_field!r}, lit(_table_exists({table!r}, {schema!r} or None)))"
            )
            return lines, "converted"

        lines.append("# WARNING: missing table / table_field")
        lines.append(f"{out_var} = {in_df}.withColumn({result_field!r}, lit(False))")
        return lines, "partial"


class ColumnExistsHandler(BaseStepHandler):
    """Column Exists → schema inspection via spark.table / DataFrame schema."""

    _TYPES = {"columnexists"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_column_exists_config(step_el))

        schema = meta.get("schema") or ""
        schema_field = meta.get("schema_field") or ""
        table = meta.get("table") or ""
        column = meta.get("column") or ""
        table_field = meta.get("table_field") or ""
        column_field = meta.get("column_field") or ""
        result_field = meta.get("result_field") or "column_exists"
        qualified = _qualified_table(str(schema), str(table))

        lines = [f"# Column Exists: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "connection", "schema", "schema_field", "table", "table_field",
            "column", "column_field", "result_field",
        )))
        if not meta.get("connection"):
            lines.append("# WARNING: missing database connection — inspecting Spark table schema")

        lines.append("def _column_exists(table_name, column_name, db=None):")
        lines.append("    if not table_name or not column_name:")
        lines.append("        return False")
        lines.append("    full = f'{db}.{table_name}' if db else str(table_name)")
        lines.append("    try:")
        lines.append("        cols = {c.name.lower() for c in spark.table(full).schema.fields}")
        lines.append("        return str(column_name).lower() in cols")
        lines.append("    except Exception:")
        lines.append("        # Schema evolution / missing table")
        lines.append("        return False")

        if (table_field or column_field or schema_field) and in_df:
            lines.append("from pyspark.sql.functions import udf")
            lines.append("from pyspark.sql.types import BooleanType")
            t_expr = f"col({table_field!r})" if table_field else f"lit({table!r})"
            c_expr = f"col({column_field!r})" if column_field else f"lit({column!r})"
            if schema_field:
                lines.append(
                    "def _row_column_exists(t, c, s):"
                )
                lines.append("    return _column_exists(t, c, s)")
                lines.append("_ce_udf = udf(_row_column_exists, BooleanType())")
                lines.append(
                    f"{out_var} = {in_df}.withColumn("
                    f"{result_field!r}, _ce_udf({t_expr}, {c_expr}, col({schema_field!r})))"
                )
            else:
                lines.append(
                    f"_ce_udf = udf(lambda t, c: _column_exists(t, c, {schema!r} or None), BooleanType())"
                )
                lines.append(
                    f"{out_var} = {in_df}.withColumn({result_field!r}, _ce_udf({t_expr}, {c_expr}))"
                )
            return lines, "converted"

        if not in_df:
            lines.append(
                f"_exists_{out_var} = _column_exists({qualified!r}, {column!r})"
            )
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"[{{'table': {qualified!r}, 'column': {column!r}, "
                f"{result_field!r}: _exists_{out_var}}}])"
            )
            return lines, "converted" if table and column else "partial"

        if table and column:
            lines.append(
                f"{out_var} = {in_df}.withColumn("
                f"{result_field!r}, lit(_column_exists({qualified!r}, {column!r})))"
            )
            return lines, "converted"

        lines.append("# WARNING: missing table/column metadata")
        lines.append(f"{out_var} = {in_df}.withColumn({result_field!r}, lit(False))")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Check if File is Locked
# ---------------------------------------------------------------------------


class CheckFileLockedHandler(BaseStepHandler):
    """Best-effort file lock detection (OS-dependent; documented limitations)."""

    _TYPES = {"checkfilelocked", "fileslocked", "lockedfiles"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_check_file_locked_config(step_el))

        filename = _resolve_str(context, str(meta.get("filename") or ""))
        filename_field = meta.get("filename_field") or ""
        result_field = meta.get("result_field") or "file_locked"
        path_expr = spark_load_path_expr(filename) if filename else "''"

        lines = [f"# Check if File is Locked: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "filename", "filename_field", "result_field", "add_filename_result",
        )))
        lines.append(
            "# WARNING: OS file locks are not uniformly visible on DBFS/S3/ABFS; "
            "this is a best-effort exclusive-open probe and may return false negatives."
        )
        _warn(
            context.step.name,
            "Check if File is Locked is best-effort only on object storage / DBFS",
        )
        lines.append("import os")
        lines.append("def _path_exists_for_lock(p):")
        lines.append("    if p is None or str(p).strip() == '':")
        lines.append("        return False")
        lines.append("    path = str(p)")
        lines.append("    _dbu = globals().get('dbutils')")
        lines.append("    if _dbu is not None:")
        lines.append("        try:")
        lines.append("            _dbu.fs.ls(path)")
        lines.append("            return True")
        lines.append("        except Exception:")
        lines.append("            return False")
        lines.append("    try:")
        lines.append("        jvm = spark._jvm")
        lines.append("        conf = spark._jsc.hadoopConfiguration()")
        lines.append("        uri = jvm.java.net.URI(path)")
        lines.append("        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)")
        lines.append("        return bool(fs.exists(jvm.org.apache.hadoop.fs.Path(path)))")
        lines.append("    except Exception:")
        lines.append("        return bool(os.path.exists(path))")
        lines.append("def _file_is_locked(p):")
        lines.append("    if p is None or str(p).strip() == '':")
        lines.append("        return False")
        lines.append("    path = str(p)")
        lines.append("    if not _path_exists_for_lock(path):")
        lines.append("        return False  # missing file — treat as not locked")
        lines.append("    # Object-store / DBFS paths cannot be flock'd — report unlocked")
        lines.append(
            "    if path.startswith(('dbfs:', 's3://', 's3a://', 'abfss://', 'wasbs://', '/Volumes/')):"
        )
        lines.append("        return False")
        lines.append("    try:")
        lines.append("        # Windows: msvcrt; POSIX: flock — both best-effort")
        lines.append("        import sys")
        lines.append("        fh = open(path, 'a+b')")
        lines.append("        try:")
        lines.append("            if sys.platform.startswith('win'):")
        lines.append("                import msvcrt")
        lines.append("                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)")
        lines.append("                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)")
        lines.append("            else:")
        lines.append("                import fcntl")
        lines.append("                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)")
        lines.append("                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)")
        lines.append("            return False")
        lines.append("        except Exception:")
        lines.append("            return True")
        lines.append("        finally:")
        lines.append("            fh.close()")
        lines.append("    except Exception:")
        lines.append("        return True")

        if filename_field and in_df:
            lines.append("from pyspark.sql.functions import udf")
            lines.append("from pyspark.sql.types import BooleanType")
            lines.append("_lock_udf = udf(_file_is_locked, BooleanType())")
            lines.append(
                f"{out_var} = {in_df}.withColumn({result_field!r}, _lock_udf(col({filename_field!r})))"
            )
            return lines, "partial"

        if not in_df:
            lines.append(f"_locked_{out_var} = _file_is_locked({path_expr})")
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"[{{'path': {path_expr}, {result_field!r}: _locked_{out_var}}}])"
            )
            return lines, "partial"

        if filename:
            lines.append(
                f"{out_var} = {in_df}.withColumn({result_field!r}, lit(_file_is_locked({path_expr})))"
            )
            return lines, "partial"

        lines.append("# WARNING: missing filename / filename_field")
        lines.append(f"{out_var} = {in_df}.withColumn({result_field!r}, lit(False))")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Check if Webservice is Available
# ---------------------------------------------------------------------------


class WebServiceAvailableHandler(BaseStepHandler):
    """HTTP availability probe with timeout / auth metadata preserved."""

    _TYPES = {"webserviceavailable", "checkwebserviceavailable"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_webservice_available_config(step_el))

        url = _resolve_str(context, str(meta.get("url") or ""))
        url_in_field = bool(meta.get("url_in_field"))
        url_field = meta.get("url_field") or ""
        result_field = meta.get("result_field") or "webservice_available"
        connect_t = _timeout_seconds(meta.get("connect_timeout"), 5.0)
        read_t = _timeout_seconds(meta.get("read_timeout"), 5.0)
        timeout = max(connect_t, read_t)
        login = meta.get("http_login") or ""
        proxy_host = meta.get("proxy_host") or ""
        proxy_port = meta.get("proxy_port") or ""

        lines = [f"# Check if Webservice is Available: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "url", "url_in_field", "url_field", "connect_timeout", "read_timeout", "result_field",
            "http_login", "proxy_host", "proxy_port",
        )))

        if url and not (url.startswith("http://") or url.startswith("https://")) and not url_in_field:
            _warn(context.step.name, f"URL may be invalid (missing scheme): {url!r}")
            lines.append(f"# WARNING: URL may be invalid (missing scheme): {url!r}")

        proxies = {}
        if proxy_host:
            port = proxy_port or "80"
            proxy_url = f"http://{proxy_host}:{port}"
            proxies = {"http": proxy_url, "https": proxy_url}

        lines.append("import requests")
        lines.append(f"def _ws_available(u, timeout={timeout}, auth=None, proxies=None):")
        lines.append("    if not u:")
        lines.append("        return False")
        lines.append("    try:")
        lines.append(
            "        r = requests.get(str(u), timeout=timeout, auth=auth, proxies=proxies or {})"
        )
        lines.append("        return r.status_code < 500")
        lines.append("    except requests.exceptions.Timeout:")
        lines.append("        return False")
        lines.append("    except requests.exceptions.RequestException:")
        lines.append("        return False")

        auth_expr = f"({login!r}, os.environ.get('PENTAHO_HTTP_PASSWORD', ''))" if login else "None"
        if login:
            lines.append("import os")
            lines.append("# WARNING: password not embedded — set PENTAHO_HTTP_PASSWORD at runtime")
        lines.append(f"_ws_proxies_{out_var} = {proxies!r}")

        use_field = url_in_field and url_field
        if use_field and in_df:
            lines.append("from pyspark.sql.functions import udf")
            lines.append("from pyspark.sql.types import BooleanType")
            lines.append(
                f"_ws_udf = udf(lambda u: _ws_available(u, {timeout}, None, _ws_proxies_{out_var}), BooleanType())"
            )
            lines.append(
                f"{out_var} = {in_df}.withColumn({result_field!r}, _ws_udf(col({url_field!r})))"
            )
            return lines, "converted"

        if not in_df:
            if not url:
                _warn(context.step.name, "missing URL")
                lines.append("# WARNING: missing URL")
            lines.append(
                f"_ok_{out_var} = _ws_available({url!r}, {timeout}, {auth_expr}, _ws_proxies_{out_var})"
            )
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"[{{'url': {url!r}, {result_field!r}: _ok_{out_var}}}])"
            )
            return lines, "converted" if url else "partial"

        if url:
            lines.append(
                f"{out_var} = {in_df}.withColumn("
                f"{result_field!r}, lit(_ws_available({url!r}, {timeout}, {auth_expr}, _ws_proxies_{out_var})))"
            )
            return lines, "converted"

        _warn(context.step.name, "missing URL")
        lines.append("# WARNING: missing URL")
        lines.append(f"{out_var} = {in_df}.withColumn({result_field!r}, lit(False))")
        return lines, "partial"


# ---------------------------------------------------------------------------
# HTTP Client / HTTP Post / REST Client
# ---------------------------------------------------------------------------


def _http_auth_setup(lines: list[str], meta: dict, prefix: str) -> str:
    """Append auth/header construction; return Python expr for auth=."""
    login = meta.get("http_login") or ""
    if login:
        lines.append(f"{prefix}_auth = ({login!r}, os.environ.get('PENTAHO_HTTP_PASSWORD', ''))")
        lines.append(
            "# WARNING: password not embedded — set PENTAHO_HTTP_PASSWORD or wire secret scope"
        )
        return f"{prefix}_auth"
    return "None"


def _http_proxies_setup(lines: list[str], meta: dict, prefix: str) -> str:
    """Append proxies dict for requests; return variable name or '{}'."""
    host = meta.get("proxy_host") or ""
    port = meta.get("proxy_port") or "80"
    if not host:
        lines.append(f"{prefix}_proxies = {{}}")
        return f"{prefix}_proxies"
    proxy_url = f"http://{host}:{port}"
    lines.append(f"{prefix}_proxies = {{'http': {proxy_url!r}, 'https': {proxy_url!r}}}")
    return f"{prefix}_proxies"


class HttpClientHandler(BaseStepHandler):
    """HTTP Client → requests GET with headers / query params / timeouts."""

    _TYPES = {"http", "httpclient", "httpget"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_http_client_config(step_el))
        return self._emit(context, in_df, out_var, meta, method="GET")

    def _emit(
        self,
        context: StepContext,
        in_df: str | None,
        out_var: str,
        meta: dict,
        *,
        method: str,
        body_field: str = "",
        content_type: str = "",
    ) -> tuple[list[str], str]:
        url = _resolve_str(context, str(meta.get("url") or ""))
        url_field = meta.get("url_field") or ""
        url_in_field = bool(meta.get("url_in_field"))
        result_field = meta.get("result_field") or "result"
        code_field = meta.get("response_code_field") or ""
        time_field = meta.get("response_time_field") or ""
        timeout = _timeout_seconds(meta.get("connection_timeout"), 10.0)
        headers = meta.get("headers") or []
        arguments = meta.get("arguments") or meta.get("query_parameters") or []

        label = "HTTP Post" if method == "POST" else "HTTP Client"
        lines = [f"# {label}: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "url", "url_in_field", "url_field", "encoding", "connection_timeout",
            "result_field", "response_code_field", "response_time_field",
            "arguments", "query_parameters", "headers", "proxy_host", "proxy_port",
            "request_entity", "content_type", "post_a_file",
        )))

        if url and not (url.startswith("http://") or url.startswith("https://") or url_in_field):
            lines.append(f"# WARNING: URL may be invalid (missing scheme): {url!r}")
        if not url and not (url_in_field and url_field):
            lines.append("# WARNING: missing URL")

        lines.append("import os, time, requests")
        auth_expr = _http_auth_setup(lines, meta, f"_{out_var}")
        proxies_expr = _http_proxies_setup(lines, meta, f"_{out_var}")

        # Static header map from constant header values
        header_literals = {
            (h.get("parameter") or h.get("name") or ""): (h.get("value") or "")
            for h in headers
            if (h.get("parameter") or h.get("name")) and h.get("value")
        }
        lines.append(f"_{out_var}_headers = {header_literals!r}")
        param_names = [a.get("parameter") or a.get("name") for a in arguments if a.get("parameter") or a.get("name")]
        lines.append(f"_{out_var}_param_keys = {param_names!r}")
        if meta.get("post_a_file"):
            lines.append(
                "# WARNING: post_a_file is preserved but multipart file upload is not "
                "auto-implemented — send file bytes via request entity / multipart manually"
            )

        lines.append(
            f"def _{out_var}_request(u, params=None, data=None, headers=None):"
        )
        lines.append("    t0 = time.time()")
        lines.append("    try:")
        lines.append(
            f"        resp = requests.request({method!r}, str(u), params=params or {{}}, "
            f"data=data, headers=headers or {{}}, timeout={timeout}, auth={auth_expr}, "
            f"proxies={proxies_expr})"
        )
        lines.append("        return resp.status_code, resp.text, int((time.time() - t0) * 1000)")
        lines.append("    except requests.exceptions.Timeout:")
        lines.append("        return None, 'TIMEOUT', int((time.time() - t0) * 1000)")
        lines.append("    except requests.exceptions.RequestException as exc:")
        lines.append("        return None, str(exc), int((time.time() - t0) * 1000)")

        if not url and not (url_in_field and url_field):
            _warn(context.step.name, "missing URL")
            lines.append("# WARNING: missing URL")

        if not in_df:
            lines.append(
                f"_code, _body, _ms = _{out_var}_request({url!r}, headers=_{out_var}_headers)"
            )
            cols = [f"'url': {url!r}", f"{result_field!r}: _body"]
            if code_field:
                cols.append(f"{code_field!r}: _code")
            if time_field:
                cols.append(f"{time_field!r}: _ms")
            lines.append(f"{out_var} = spark.createDataFrame([{{{', '.join(cols)}}}])")
            return lines, "converted" if url else "partial"

        # Row-wise HTTP via driver collect (documented limitation for large volumes)
        lines.append(
            "# NOTE: row-wise HTTP on the driver; use mapPartitions/requests Session "
            "for high-volume APIs. Authentication failures surface as body/status columns."
        )
        lines.append("from pyspark.sql import Row")
        lines.append(f"_http_rows_{out_var} = []")
        lines.append(f"for _row in {in_df}.toLocalIterator():")
        lines.append("    _d = _row.asDict(recursive=True)")
        if url_in_field and url_field:
            lines.append(f"    _u = _d.get({url_field!r}) or {url!r}")
        else:
            lines.append(f"    _u = {url!r}")
        lines.append(f"    _params = {{}}")
        for arg in arguments:
            field = arg.get("field") or ""
            param = arg.get("parameter") or arg.get("name") or field
            if field and param:
                lines.append(f"    _params[{param!r}] = _d.get({field!r})")
        lines.append(f"    _hdrs = dict(_{out_var}_headers)")
        for hdr in headers:
            field = hdr.get("field") or ""
            param = hdr.get("parameter") or hdr.get("name") or ""
            if field and param and not hdr.get("value"):
                lines.append(f"    _hdrs[{param!r}] = _d.get({field!r})")
        if content_type:
            lines.append(f"    _hdrs.setdefault('Content-Type', {content_type!r})")
        if body_field:
            lines.append(f"    _data = _d.get({body_field!r})")
        elif meta.get("request_entity"):
            lines.append(f"    _data = _d.get({str(meta.get('request_entity'))!r})")
        else:
            lines.append("    _data = None")
        lines.append(f"    _code, _body, _ms = _{out_var}_request(_u, _params, _data, _hdrs)")
        lines.append("    _d = dict(_d)")
        lines.append(f"    _d[{result_field!r}] = _body")
        if code_field:
            lines.append(f"    _d[{code_field!r}] = _code")
        if time_field:
            lines.append(f"    _d[{time_field!r}] = _ms")
        lines.append(f"    _http_rows_{out_var}.append(Row(**_d))")
        lines.append(f"if _http_rows_{out_var}:")
        lines.append(f"    {out_var} = spark.createDataFrame(_http_rows_{out_var})")
        lines.append("else:")
        lines.append(f"    {out_var} = {in_df}")
        status = "converted" if (url or (url_in_field and url_field)) else "partial"
        return lines, status


class HttpPostHandler(HttpClientHandler):
    """HTTP Post → requests POST with entity / form fields."""

    _TYPES = {"httppost"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_http_post_config(step_el))
        body_field = str(meta.get("request_entity") or "")
        content_type = str(meta.get("content_type") or "")
        return self._emit(
            context, in_df, out_var, meta,
            method="POST", body_field=body_field, content_type=content_type,
        )


class RestClientHandler(BaseStepHandler):
    """REST Client → Python requests with method / headers / body / response mapping."""

    _TYPES = {"rest", "restclient"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_rest_client_config(step_el))

        url = _resolve_str(context, str(meta.get("url") or ""))
        url_field = meta.get("url_field") or ""
        url_in_field = bool(meta.get("url_in_field"))
        method = str(meta.get("method") or "GET").upper()
        method_field = meta.get("method_field") or ""
        body_field = meta.get("body_field") or ""
        result_field = meta.get("result_field") or "result"
        code_field = meta.get("response_code_field") or ""
        header_out = meta.get("response_header_field") or ""
        timeout = _timeout_seconds(meta.get("connection_timeout"), 10.0)
        read_timeout = _timeout_seconds(meta.get("read_timeout"), timeout)
        app_type = str(meta.get("application_type") or "")

        lines = [f"# REST Client: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "url", "url_in_field", "url_field", "method", "method_in_field", "method_field",
            "body_field", "application_type", "connection_timeout", "read_timeout",
            "result_field", "response_code_field", "response_header_field",
            "headers", "parameters", "matrix_parameters", "proxy_host", "proxy_port", "preemptive",
        )))

        if url and not (url.startswith("http://") or url.startswith("https://") or url_in_field):
            lines.append(f"# WARNING: URL may be invalid (missing scheme): {url!r}")

        content_type_map = {
            "JSON": "application/json",
            "XML": "application/xml",
            "TEXT PLAIN": "text/plain",
            "TEXT XML": "application/xml",
            "APPLICATION/JSON": "application/json",
        }
        ct = content_type_map.get(app_type.upper(), app_type if "/" in app_type else "application/json")

        lines.append("import os, time, json, requests")
        auth_expr = _http_auth_setup(lines, meta, f"_{out_var}")
        proxies_expr = _http_proxies_setup(lines, meta, f"_{out_var}")
        static_headers = {
            (h.get("name") or ""): (h.get("value") or "")
            for h in (meta.get("headers") or [])
            if h.get("name") and h.get("value")
        }
        static_headers.setdefault("Content-Type", ct)
        lines.append(f"_{out_var}_headers = {static_headers!r}")
        if meta.get("preemptive"):
            lines.append("# preserved.preemptive=True — requests basic-auth is sent eagerly")
        matrix = meta.get("matrix_parameters") or []
        if matrix:
            lines.append(f"# preserved.matrix_parameters={matrix!r}")
            lines.append(
                "# NOTE: matrix parameters are applied as path segments (;key=value) when building URL"
            )

        lines.append(
            f"def _{out_var}_rest(u, method='GET', params=None, data=None, headers=None):"
        )
        lines.append("    t0 = time.time()")
        lines.append("    try:")
        lines.append(
            f"        resp = requests.request(str(method).upper(), str(u), "
            f"params=params or {{}}, data=data, headers=headers or {{}}, "
            f"timeout=({timeout}, {read_timeout}), auth={auth_expr}, proxies={proxies_expr})"
        )
        lines.append(
            "        return resp.status_code, resp.text, dict(resp.headers), "
            "int((time.time() - t0) * 1000)"
        )
        lines.append("    except requests.exceptions.Timeout:")
        lines.append("        return None, 'TIMEOUT', {}, int((time.time() - t0) * 1000)")
        lines.append("    except requests.exceptions.RequestException as exc:")
        lines.append("        return None, str(exc), {}, int((time.time() - t0) * 1000)")

        if not in_df:
            lines.append(
                f"_code, _body, _hdr, _ms = _{out_var}_rest("
                f"{url!r}, {method!r}, headers=_{out_var}_headers)"
            )
            cols = [f"'url': {url!r}", f"'method': {method!r}", f"{result_field!r}: _body"]
            if code_field:
                cols.append(f"{code_field!r}: _code")
            if header_out:
                cols.append(f"{header_out!r}: json.dumps(_hdr)")
            lines.append(f"{out_var} = spark.createDataFrame([{{{', '.join(cols)}}}])")
            return lines, "converted" if url else "partial"

        lines.append("from pyspark.sql import Row")
        lines.append(f"_rest_rows_{out_var} = []")
        lines.append(f"for _row in {in_df}.toLocalIterator():")
        lines.append("    _d = _row.asDict(recursive=True)")
        if url_in_field and url_field:
            lines.append(f"    _u = _d.get({url_field!r}) or {url!r}")
        else:
            lines.append(f"    _u = {url!r}")
        if meta.get("method_in_field") and method_field:
            lines.append(f"    _m = str(_d.get({method_field!r}) or {method!r}).upper()")
        else:
            lines.append(f"    _m = {method!r}")
        lines.append("    _params = {}")
        for p in meta.get("parameters") or []:
            name, field = p.get("name") or "", p.get("field") or ""
            if name and field:
                lines.append(f"    _params[{name!r}] = _d.get({field!r})")
        lines.append(f"    _hdrs = dict(_{out_var}_headers)")
        for h in meta.get("headers") or []:
            name, field = h.get("name") or "", h.get("field") or ""
            if name and field and not h.get("value"):
                lines.append(f"    _hdrs[{name!r}] = _d.get({field!r})")
        if body_field:
            lines.append(f"    _data = _d.get({body_field!r})")
        else:
            lines.append("    _data = None")
        lines.append(f"    _code, _body, _hdr, _ms = _{out_var}_rest(_u, _m, _params, _data, _hdrs)")
        lines.append("    _d = dict(_d)")
        lines.append(f"    _d[{result_field!r}] = _body")
        if code_field:
            lines.append(f"    _d[{code_field!r}] = _code")
        if header_out:
            lines.append(f"    _d[{header_out!r}] = json.dumps(_hdr)")
        lines.append(f"    _rest_rows_{out_var}.append(Row(**_d))")
        lines.append(f"if _rest_rows_{out_var}:")
        lines.append(f"    {out_var} = spark.createDataFrame(_rest_rows_{out_var})")
        lines.append("else:")
        lines.append(f"    {out_var} = {in_df}")
        return lines, "converted" if (url or (url_in_field and url_field)) else "partial"


# ---------------------------------------------------------------------------
# Web Services Lookup (SOAP)
# ---------------------------------------------------------------------------


class WebServicesLookupHandler(BaseStepHandler):
    """SOAP / WSDL lookup — metadata preserved; stub with documented gaps."""

    _TYPES = {"webservice", "webservicelookup"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_web_services_lookup_config(step_el))

        url = _resolve_str(context, str(meta.get("url") or ""))
        wsdl = _resolve_str(context, str(meta.get("wsdl") or ""))
        operation = meta.get("operation") or ""
        soap_action = meta.get("soap_action") or ""
        outputs = meta.get("output_fields") or []

        lines = [f"# Web Services Lookup: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "url", "wsdl", "operation", "soap_action", "call_step",
            "passing_input_data", "compatible", "repeating_element", "reply_as_string",
            "parameters", "output_fields", "proxy_host", "proxy_port",
        )))
        lines.append(
            "# UNSUPPORTED: Full SOAP/WSDL stack (JAX-WS style) is not available in "
            "Databricks PySpark by default. Prefer REST migration or install zeep."
        )
        lines.append(
            "# Optional zeep stub (uncomment if zeep is installed on the cluster):"
        )
        lines.append(f"# from zeep import Client")
        if wsdl:
            lines.append(f"# _client = Client({wsdl!r})")
        else:
            lines.append(f"# _client = Client({url!r})")
        if operation:
            lines.append(f"# _result = _client.service.{operation}(...)")
        lines.append(f"# preserved.soap_action={soap_action!r}")

        if in_df:
            lines.append(f"{out_var} = {in_df}")
            for out in outputs:
                rename = out.get("rename") or out.get("name")
                if rename:
                    lines.append(
                        f"{out_var} = {out_var}.withColumn({rename!r}, lit(None).cast('string'))"
                    )
            lines.append(
                "# WARNING: SOAP response mappings emitted as null stubs"
            )
            return lines, "partial"

        lines.append(
            f"{out_var} = spark.createDataFrame("
            f"[{{'wsdl': {wsdl!r}, 'operation': {operation!r}, 'status': 'UNSUPPORTED'}}])"
        )
        return lines, "partial"


# ---------------------------------------------------------------------------
# Fuzzy Match
# ---------------------------------------------------------------------------


class FuzzyMatchHandler(BaseStepHandler):
    """Fuzzy Match → crossJoin + Levenshtein / Soundex with threshold filtering."""

    _TYPES = {"fuzzymatch"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            _merge_cfg(meta, parse_fuzzy_match_config(step_el))

        algorithm = str(meta.get("algorithm") or "levenshtein").strip()
        alg_key = algorithm.lower().replace(" ", "").replace("_", "")
        main_field = meta.get("main_stream_field") or meta.get("match_field") or ""
        lookup_field = meta.get("lookup_field") or main_field
        out_match = meta.get("output_match_field") or "match"
        out_value = meta.get("output_value_field") or ""
        minimal = float(meta.get("minimal_value") or 0)
        maximal = float(meta.get("maximal_value") or 1)
        case_sensitive = bool(meta.get("case_sensitive"))
        closer = bool(meta.get("get_closer_value", True))

        lines = [f"# Fuzzy Match: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "algorithm", "lookup_field", "main_stream_field", "output_match_field",
            "output_value_field", "minimal_value", "maximal_value", "case_sensitive",
            "get_closer_value", "separator",
        )))

        if len(inputs) < 2:
            _warn(context.step.name, "needs main + lookup streams")
            return _passthrough(context, "Fuzzy Match")

        main_df, lookup_df = inputs[0], inputs[1]
        if not main_field:
            main_field = "match"
            _warn(context.step.name, "main stream match field missing — defaulting to 'match'")
            lines.append("# WARNING: main stream match field missing — defaulting to 'match'")
        if not lookup_field:
            lookup_field = main_field

        unsupported_exact = alg_key not in _FUZZY_SUPPORTED
        if unsupported_exact:
            _warn(context.step.name, f"algorithm {algorithm!r} unsupported — falling back to levenshtein")
            lines.append(
                f"# WARNING: algorithm {algorithm!r} has no Spark equivalent — "
                "falling back to levenshtein; review results"
            )
            alg_key = "levenshtein"
        elif alg_key in ("jaro", "jarowinkler", "metaphone", "doublemetaphone"):
            _warn(context.step.name, f"algorithm {algorithm!r} approximated")
            lines.append(
                f"# WARNING: algorithm {algorithm!r} approximated "
                "(no native Spark Jaro/Metaphone)"
            )

        # Rename join fields to avoid ambiguous columns after crossJoin
        lines.append(
            f"_fm_main_{out_var} = {main_df}.withColumnRenamed("
            f"{main_field!r}, '_fm_main_key')"
        )
        lines.append(
            f"_fm_lkp_{out_var} = {lookup_df}.withColumnRenamed("
            f"{lookup_field!r}, '_fm_lkp_key')"
        )
        if not case_sensitive:
            main_expr = "lower(col('_fm_main_key').cast('string'))"
            lkp_expr = "lower(col('_fm_lkp_key').cast('string'))"
        else:
            main_expr = "col('_fm_main_key').cast('string')"
            lkp_expr = "col('_fm_lkp_key').cast('string')"

        lines.append(f"{out_var} = _fm_main_{out_var}.crossJoin(_fm_lkp_{out_var})")

        if alg_key in _FUZZY_LEVENSHTEIN or alg_key in ("pairwise", "jaro", "jarowinkler"):
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"'_fm_dist', levenshtein({main_expr}, {lkp_expr}))"
            )
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{out_match!r}, "
                f"(lit(1.0) - (col('_fm_dist') / "
                f"greatest(length({main_expr}), length({lkp_expr}), lit(1)))))"
            )
        elif alg_key in ("soundex", "refinedsoundex", "metaphone", "doublemetaphone"):
            lines.append(
                f"# WARNING: {algorithm} approximated via soundex equality (0/1 score)"
            )
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{out_match!r}, "
                f"when(soundex({main_expr}) == soundex({lkp_expr}), lit(1.0)).otherwise(lit(0.0)))"
            )
            lines.append(f"{out_var} = {out_var}.withColumn('_fm_dist', lit(0))")

        lines.append(
            f"{out_var} = {out_var}.filter("
            f"(col({out_match!r}) >= lit({minimal})) & (col({out_match!r}) <= lit({maximal})))"
        )

        if closer:
            lines.append(
                f"_w_{out_var} = Window.partitionBy(col('_fm_main_key'))"
                f".orderBy(col({out_match!r}).desc())"
            )
            lines.append(
                f"{out_var} = {out_var}.withColumn('_fm_rn', row_number().over(_w_{out_var}))"
            )
            lines.append(f"{out_var} = {out_var}.filter(col('_fm_rn') == 1).drop('_fm_rn')")

        lines.append(f"{out_var} = {out_var}.drop('_fm_dist')")

        if out_value and out_value != out_match:
            lines.append(
                f"{out_var} = {out_var}.withColumn({out_value!r}, col('_fm_lkp_key'))"
            )

        lines.append(
            "# NOTE: crossJoin can explode row counts on large lookup tables; "
            "consider blocking / approximate joins for production."
        )
        if unsupported_exact or alg_key in ("jaro", "jarowinkler", "metaphone", "doublemetaphone"):
            return lines, "partial"
        return lines, "converted"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

LOOKUP_HANDLERS: list[BaseStepHandler] = [
    CallDBProcedureHandler(),
    DatabaseJoinHandler(),
    DynamicSQLRowHandler(),
    FileExistsHandler(),
    TableExistsHandler(),
    ColumnExistsHandler(),
    CheckFileLockedHandler(),
    WebServiceAvailableHandler(),
    HttpClientHandler(),
    HttpPostHandler(),
    RestClientHandler(),
    WebServicesLookupHandler(),
    FuzzyMatchHandler(),
]
