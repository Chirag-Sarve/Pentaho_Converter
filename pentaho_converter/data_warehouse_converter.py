"""Convert Pentaho Data Warehouse steps to Databricks Delta MERGE PySpark.

Supports:
- Dimension Lookup/Update (SCD Type 1 / Type 2 / PunchThrough)
- Combination Lookup/Update (junk / combination dimensions)
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .generation_config import GenerationConfig
from .step_context import StepContext
from .table_names import qualify_table_name

logger = logging.getLogger(__name__)

_TYPE1 = frozenset({"Update"})
_TYPE2 = frozenset({"Insert"})
_PUNCH = frozenset({"PunchThrough"})
_TECHNICAL = frozenset({
    "DateInsertedOrUpdated",
    "DateInserted",
    "DateUpdated",
    "LastVersion",
})

_START_DATE_ALTS = {
    "none": "none",
    "0": "none",
    "sysdate": "sysdate",
    "systemdate": "sysdate",
    "1": "sysdate",
    "start_of_trans": "start_of_trans",
    "startoftrans": "start_of_trans",
    "2": "start_of_trans",
    "null": "null",
    "3": "null",
    "column_value": "column_value",
    "columnvalue": "column_value",
    "4": "column_value",
}

_BROADCAST_CACHE_LIMIT = 10000


def _safe_ident(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z_]", "_", name or "step")


def _normalize_start_date_alt(raw: Any) -> str:
    key = str(raw or "none").strip().lower().replace("-", "_").replace(" ", "_")
    return _START_DATE_ALTS.get(key) or _START_DATE_ALTS.get(key.replace("_", "")) or "none"


def _effective_date_expr(
    *,
    stream_date: str,
    alt: str,
    start_date_field: str,
    min_year: int,
    use_alternative: bool,
) -> tuple[str, list[str]]:
    comments: list[str] = []
    if stream_date:
        base = f'col("{stream_date}")'
    else:
        base = "current_timestamp()"
        comments.append(
            "# Stream datefield empty — using current_timestamp as effective date"
        )
    if not use_alternative or alt == "none":
        return base, comments
    if alt == "sysdate":
        comments.append("# start_date_alternative=sysdate → current_timestamp()")
        return "current_timestamp()", comments
    if alt == "start_of_trans":
        comments.append(
            "# start_date_alternative=start_of_trans → current_timestamp() "
            "(pipeline start time not available in generated fragment)"
        )
        return "current_timestamp()", comments
    if alt == "null":
        comments.append(
            f"# start_date_alternative=null → min_year bound {min_year}-01-01 (_scd_min_ts)"
        )
        return "_scd_min_ts", comments
    if alt == "column_value":
        if start_date_field:
            comments.append(
                f"# start_date_alternative=column_value → col({start_date_field!r})"
            )
            return f'col("{start_date_field}")', comments
        comments.append(
            "# WARNING: start_date_alternative=column_value but "
            "start_date_field_name is empty — falling back to stream/sysdate"
        )
        return base, comments
    comments.append(
        f"# WARNING: unknown start_date_alternative={alt!r} — using stream/sysdate"
    )
    return base, comments


def _null_bk_predicate(keys: list[tuple[str, str]]) -> str:
    return " | ".join(f'col("{s}").isNull()' for s, _ in keys)


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return default
    return str(value).strip().upper() in ("Y", "YES", "TRUE", "1", "T")


def _generation_config(context: StepContext | None) -> GenerationConfig:
    if context is None:
        return GenerationConfig.defaults()
    cfg = context.extra.get("generation_config")
    if isinstance(cfg, GenerationConfig):
        return cfg
    return GenerationConfig.defaults()


def _qualified_table(metadata: dict[str, Any], context: StepContext | None) -> str:
    schema = (metadata.get("schema") or "").strip()
    table = (metadata.get("table") or "").strip()
    if not table:
        attrs = metadata.get("attributes") or {}
        table = (attrs.get("table") or attrs.get("tablename") or "").strip()
        schema = schema or (attrs.get("schema") or "").strip()
    if not table:
        return ""
    return qualify_table_name(table, schema, config=_generation_config(context))


def _business_keys(metadata: dict[str, Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for source in (metadata.get("keys"), metadata.get("join_keys")):
        if not source:
            continue
        for item in source:
            if not isinstance(item, dict):
                continue
            stream = (
                item.get("stream_field")
                or item.get("left")
                or item.get("name")
                or ""
            ).strip()
            table = (
                item.get("table_field")
                or item.get("right")
                or item.get("lookup")
                or stream
            ).strip()
            if stream or table:
                pairs.append((stream or table, table or stream))
        if pairs:
            return pairs
    return pairs


def _dim_fields(metadata: dict[str, Any]) -> list[dict[str, str]]:
    fields: list[dict[str, str]] = []
    for item in metadata.get("fields") or []:
        if not isinstance(item, dict):
            continue
        stream = (item.get("stream_field") or item.get("name") or "").strip()
        table = (item.get("table_field") or item.get("lookup") or stream).strip()
        update_type = (item.get("update_type") or item.get("update") or "Insert").strip()
        if stream or table:
            fields.append({
                "stream_field": stream or table,
                "table_field": table or stream,
                "update_type": update_type,
            })
    return fields


def _preserve_lines(lines: list[str], metadata: dict[str, Any], *keys: str) -> None:
    for key in keys:
        if key in metadata and metadata[key] not in (None, "", [], {}):
            lines.append(f"# preserved.{key}={metadata[key]!r}")


def _tech_key_warnings(
    lines: list[str],
    step_label: str,
    step_name: str,
    creation: str,
    sequence_name: str,
    use_autoinc: bool,
) -> None:
    creation = (creation or "tablemax").strip().lower()
    if creation == "sequence":
        lines.append(
            f"# WARNING: {step_label} '{step_name}': database sequence "
            f"{sequence_name!r} is not native on Databricks — using "
            "table-max + row_number for surrogate keys. Consider Delta "
            "IDENTITY columns."
        )
    elif creation == "autoinc" or use_autoinc:
        lines.append(
            f"# WARNING: {step_label} '{step_name}': JDBC auto-increment mapped "
            "to table-max + row_number. Prefer Delta GENERATED ALWAYS AS IDENTITY."
        )
    else:
        lines.append(
            f"# Surrogate key strategy: tablemax (MAX(tk)+row_number) for '{step_name}'"
        )
    lines.append(
        "# Optional: ALTER TABLE ... CHANGE COLUMN tk "
        "GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values"
    )


def _assign_surrogate_key_lines(
    *,
    df_var: str,
    out_var: str,
    full_table: str,
    tech_key: str,
) -> list[str]:
    """Generate table-max + row_number surrogate key assignment."""
    return [
        "# tablemax + row_number (IDENTITY would omit tk from INSERT below)",
        f"_max_tk = spark.sql("
        f"\"SELECT COALESCE(MAX(`{tech_key}`), 0) AS m FROM {full_table}\""
        f").collect()[0][0]",
        "from pyspark.sql.window import Window as _DWWindow",
        f"{out_var} = {df_var}.withColumn("
        "\"_dw_rn\", row_number().over(_DWWindow.orderBy(lit(1))))",
        f"{out_var} = {out_var}.withColumn("
        f"\"{tech_key}\", (lit(_max_tk) + col(\"_dw_rn\")).cast(\"long\")).drop(\"_dw_rn\")",
    ]


def _should_broadcast(metadata: dict[str, Any]) -> tuple[bool, list[str]]:
    """Broadcast only for small Pentaho caches (preload/cached, size <= 10000)."""
    comments: list[str] = []
    preload = _as_bool(metadata.get("preload_cache"))
    cached = _as_bool(metadata.get("cached"))
    try:
        cache_size = int(metadata.get("cache_size") or 0)
    except (TypeError, ValueError):
        cache_size = 0

    if not (preload or cached):
        return False, comments

    if cache_size > _BROADCAST_CACHE_LIMIT:
        comments.append(
            f"# Large cache_size={cache_size} skips broadcast to avoid "
            "driver/executor memory pressure"
        )
        return False, comments

    # cache_size <= 10000, or 0 with preload (full preload of small dim)
    comments.append("# Cache: broadcast join approximates Pentaho preload/cache")
    return True, comments


def _bk_join_expr(left: str, right: str, keys: list[tuple[str, str]]) -> str:
    parts = [f'({left}["{s}"] == {right}["{t}"])' for s, t in keys]
    return " & ".join(parts)


def _select_expr(cols: list[str]) -> str:
    return ", ".join(f'"{c}"' for c in cols)


def _dim_read_columns(
    *,
    tech_key: str,
    keys: list[tuple[str, str]],
    date_from: str,
    date_to: str,
    version_field: str,
    current_flag_field: str,
    extra_table_fields: list[str] | None = None,
) -> list[str]:
    """Column prune list for dim lookup / compare / rejoin reads."""
    ordered: list[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            ordered.append(name)

    _add(tech_key)
    for _, table_f in keys:
        _add(table_f)
    _add(date_from)
    _add(date_to)
    _add(version_field)
    _add(current_flag_field)
    for col_name in extra_table_fields or []:
        _add(col_name)
    return ordered


def _emit_delta_tips(
    lines: list[str],
    *,
    date_from: str,
    date_to: str,
    keys: list[tuple[str, str]],
) -> None:
    lines.append(
        '# Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", '
        '"true")  # additive columns only'
    )
    if date_from or date_to:
        bk_hint = ", ".join(t for _, t in keys) or "business_keys"
        lines.append(
            f"# Post-load tip: partition pruning on {date_from or 'date_from'}/"
            f"{date_to or 'date_to'}; OPTIMIZE ... ZORDER BY ({bk_hint})"
        )


def _ensure_delta_import(lines: list[str]) -> None:
    if not any("from delta.tables import DeltaTable" in ln for ln in lines):
        lines.append("from delta.tables import DeltaTable")


def _fmt_sql_map(mapping: dict[str, str]) -> str:
    return "{" + ", ".join(f"{k!r}: {v!r}" for k, v in mapping.items()) + "}"


def _emit_delta_matched_update(
    lines: list[str],
    *,
    full: str,
    source_view: str,
    on_sql: str,
    set_map: dict[str, str],
    match_condition: str | None = None,
) -> None:
    """Emit DeltaTable merge WHEN MATCHED THEN UPDATE (semantically = SQL MERGE)."""
    _ensure_delta_import(lines)
    set_literal = _fmt_sql_map(set_map)
    lines.append("(")
    lines.append(f"    DeltaTable.forName(spark, {full!r}).alias(\"t\")")
    lines.append(
        f"    .merge(spark.table({source_view!r}).alias(\"s\"), {on_sql!r})"
    )
    if match_condition:
        lines.append(
            f"    .whenMatchedUpdate(condition={match_condition!r}, set={set_literal})"
        )
    else:
        lines.append(f"    .whenMatchedUpdate(set={set_literal})")
    lines.append("    .execute()")
    lines.append(")")
    lines.append(
        "# Delta transaction: each DeltaTable.merge().execute() is one atomic transaction"
    )


def _emit_delta_not_matched_insert(
    lines: list[str],
    *,
    full: str,
    source_view: str,
    on_sql: str,
    values_map: dict[str, str],
) -> None:
    """Emit DeltaTable merge WHEN NOT MATCHED THEN INSERT."""
    _ensure_delta_import(lines)
    values_literal = _fmt_sql_map(values_map)
    lines.append("(")
    lines.append(f"    DeltaTable.forName(spark, {full!r}).alias(\"t\")")
    lines.append(
        f"    .merge(spark.table({source_view!r}).alias(\"s\"), {on_sql!r})"
    )
    lines.append(f"    .whenNotMatchedInsert(values={values_literal})")
    lines.append("    .execute()")
    lines.append(")")
    lines.append(
        "# Delta transaction: each DeltaTable.merge().execute() is one atomic transaction"
    )


def _emit_active_dim(
    lines: list[str],
    *,
    dim_var: str,
    active_var: str,
    keys: list[tuple[str, str]],
    date_from: str,
    date_to: str,
    stream_date: str,
    current_flag_field: str,
    version_field: str,
    for_join: bool,
) -> None:
    """Filter/prepare active dimension rows for join or comparison."""
    if date_from and date_to and stream_date and for_join:
        lines.append(
            f"# Effective dating: {stream_date} between {date_from} and {date_to}"
        )
        lines.append(f"{active_var} = {dim_var}")
        return
    if current_flag_field:
        lines.append(
            f"{active_var} = {dim_var}.filter(col({current_flag_field!r}) == lit(True))"
        )
        lines.append("# Multiple active records: prefer LastVersion/current flag")
        return
    lines.append(f"{active_var} = {dim_var}")
    if version_field:
        lines.append(f"# Prefer highest {version_field} when multiple versions match")
        lines.append("from pyspark.sql.window import Window as _DWWindow")
        part = ", ".join(f'"{t}"' for _, t in keys)
        lines.append(
            f"{active_var} = {active_var}.withColumn("
            "\"_dw_ver_rn\", row_number().over("
            f"_DWWindow.partitionBy({part}).orderBy(col({version_field!r}).desc()))"
            ").filter(col(\"_dw_ver_rn\") == 1).drop(\"_dw_ver_rn\")"
        )


def _rejoin_dimension(
    lines: list[str],
    *,
    in_df: str,
    out_var: str,
    dim_var: str,
    full: str,
    keys: list[tuple[str, str]],
    date_from: str,
    date_to: str,
    stream_date: str,
    current_flag_field: str,
    version_field: str,
    tech_key: str,
    tech_rename: str,
    broadcast: bool,
    select_cols: list[str],
) -> None:
    """Always re-join dimension after updates; never reuse pre-update _dim_joined."""
    cols_expr = _select_expr(select_cols)
    lines.append(f"{dim_var} = spark.table({full!r}).select({cols_expr})")
    # Predicate pushdown: active rows before join when not using stream-date between
    if not (date_from and date_to and stream_date):
        if current_flag_field:
            lines.append(
                f"{dim_var} = {dim_var}.filter("
                f"col({current_flag_field!r}) == lit(True))"
            )
        elif date_to:
            lines.append(
                f"{dim_var} = {dim_var}.filter(col({date_to!r}) >= _scd_max_ts)"
            )
    if broadcast:
        lines.append(f"{dim_var} = broadcast({dim_var})")

    if date_from and date_to and stream_date:
        rejoin = [f'({in_df}["{s}"] == {dim_var}["{t}"])' for s, t in keys]
        rejoin.append(f'({in_df}["{stream_date}"] >= {dim_var}["{date_from}"])')
        rejoin.append(f'({in_df}["{stream_date}"] < {dim_var}["{date_to}"])')
        lines.append(
            f"{out_var} = {in_df}.join({dim_var}, on=({' & '.join(rejoin)}), how='left')"
        )
    elif current_flag_field:
        rejoin = _bk_join_expr(in_df, dim_var, keys)
        lines.append(
            f"{out_var} = {in_df}.join({dim_var}, on=({rejoin}), how='left')"
        )
    elif version_field:
        lines.append("from pyspark.sql.window import Window as _DWWindow")
        part = ", ".join(f'"{t}"' for _, t in keys)
        lines.append(
            f"_dim_cur = {dim_var}.withColumn("
            "\"_dw_ver_rn\", row_number().over("
            f"_DWWindow.partitionBy({part}).orderBy(col({version_field!r}).desc()))"
            ").filter(col(\"_dw_ver_rn\") == 1).drop(\"_dw_ver_rn\")"
        )
        rejoin = _bk_join_expr(in_df, "_dim_cur", keys)
        lines.append(
            f"{out_var} = {in_df}.join(_dim_cur, on=({rejoin}), how='left')"
        )
    else:
        bk_table = ", ".join(f'"{t}"' for _, t in keys)
        lines.append(
            f"_dim_cur = {dim_var}.dropDuplicates([{bk_table}])"
        )
        rejoin = _bk_join_expr(in_df, "_dim_cur", keys)
        lines.append(
            f"{out_var} = {in_df}.join(_dim_cur, on=({rejoin}), how='left')"
        )

    if tech_rename != tech_key:
        lines.append(
            f"{out_var} = {out_var}.withColumnRenamed({tech_key!r}, {tech_rename!r})"
        )


def convert_combination_lookup_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate Delta MERGE-based Combination Lookup/Update PySpark."""
    lines = [f"# Combination Lookup/Update: {step_name}"]
    safe = _safe_ident(out_var)

    if not in_df:
        msg = f"CombinationLookup '{step_name}': requires one input stream"
        lines.append(f"# WARNING: {msg}")
        lines.append(
            f"{out_var} = spark.createDataFrame([], '_combination_lookup_unresolved STRING')"
        )
        logger.warning(msg)
        return lines, "partial"

    full = _qualified_table(metadata, context)
    if not full:
        msg = f"CombinationLookup '{step_name}': target table missing"
        lines.append(f"# WARNING: {msg}")
        lines.append(f"{out_var} = {in_df}")
        logger.warning(msg)
        return lines, "partial"

    keys = _business_keys(metadata)
    tech_key = (metadata.get("technical_key") or "technical_key").strip() or "technical_key"
    creation = (metadata.get("tech_key_creation") or "tablemax").strip().lower()
    use_autoinc = _as_bool(metadata.get("use_autoinc"))
    replace_fields = _as_bool(metadata.get("replace_fields"))
    last_update = (metadata.get("last_update_field") or "").strip()
    connection = (metadata.get("connection") or "").strip()

    if connection:
        lines.append(f"# preserved.connection={connection!r}")
        lines.append(
            f"# WARNING: CombinationLookup '{step_name}': connection {connection!r} "
            f"mapped to Spark/UC table {full!r} (not JDBC)."
        )

    _preserve_lines(
        lines,
        metadata,
        "commit_size",
        "cache_size",
        "preload_cache",
        "use_hash",
        "hash_field",
        "sequence_name",
        "last_update_field",
    )
    if _as_bool(metadata.get("use_hash")):
        lines.append(
            f"# WARNING: CombinationLookup '{step_name}': CRC/hash cache "
            f"({metadata.get('hash_field')!r}) is database-specific — "
            "business-key equi-join used instead; metadata preserved."
        )

    _tech_key_warnings(
        lines,
        "CombinationLookup",
        step_name,
        creation,
        str(metadata.get("sequence_name") or ""),
        use_autoinc,
    )
    lines.append(
        '# Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", '
        '"true")  # additive columns only'
    )

    if not keys:
        msg = (
            f"CombinationLookup '{step_name}': missing business keys — "
            "cannot MERGE; passthrough with null surrogate key"
        )
        lines.append(f"# WARNING: {msg}")
        lines.append(
            f"{out_var} = {in_df}.withColumn({tech_key!r}, lit(None).cast(\"long\"))"
        )
        logger.warning(msg)
        return lines, "partial"

    lines.append(
        "# Edge cases: null business keys skipped from insert; "
        "duplicate combinations deduplicated before MERGE"
    )

    lkp_var = f"_combo_dim_{safe}"
    miss_var = f"_combo_miss_{safe}"
    insert_var = f"_combo_ins_{safe}"
    insert_view = f"_combo_insert_src_{safe}"

    do_broadcast, bc_comments = _should_broadcast(metadata)
    key_table_cols = [t for _, t in keys]
    key_select = _select_expr([tech_key] + key_table_cols)
    lines.append(f"{lkp_var} = spark.table({full!r}).select({key_select})")
    lines.extend(bc_comments)
    if do_broadcast:
        lines.append(f"{lkp_var} = broadcast({lkp_var})")

    rename_parts: list[str] = []
    for stream_f, table_f in keys:
        if stream_f != table_f:
            rename_parts.append(f'col("{table_f}").alias("{stream_f}")')
        else:
            rename_parts.append(f'col("{table_f}")')
    rename_parts.append(f'col("{tech_key}")')
    lines.append(f"{lkp_var} = {lkp_var}.select({', '.join(rename_parts)})")

    on_cols = [f'"{s}"' for s, _ in keys]
    on_expr = f"[{', '.join(on_cols)}]" if len(on_cols) > 1 else on_cols[0]
    lines.append(f"_combo_joined = {in_df}.join({lkp_var}, on={on_expr}, how='left')")

    null_bk = _null_bk_predicate(keys)
    lines.append(
        f"{miss_var} = _combo_joined.filter("
        f'col("{tech_key}").isNull() & ~({null_bk}))'
    )
    bk_list = ", ".join(f'"{s}"' for s, _ in keys)
    lines.append(f"{miss_var} = {miss_var}.dropDuplicates([{bk_list}])")

    select_exprs: list[str] = []
    insert_cols: list[str] = []
    for stream_f, table_f in keys:
        select_exprs.append(f'col("{stream_f}").alias("{table_f}")')
        insert_cols.append(table_f)
    if last_update:
        lines.append(f"if {last_update!r} in {miss_var}.columns:")
        lines.append(
            f"    {insert_var} = {miss_var}.select("
            f"{', '.join(select_exprs)}, col({last_update!r}).alias({last_update!r}))"
        )
        lines.append("else:")
        lines.append(
            f"    {insert_var} = {miss_var}.select("
            f"{', '.join(select_exprs)}, current_timestamp().alias({last_update!r}))"
        )
        insert_cols.append(last_update)
    else:
        lines.append(f"{insert_var} = {miss_var}.select({', '.join(select_exprs)})")
    insert_cols.append(tech_key)

    lines.extend(
        _assign_surrogate_key_lines(
            df_var=insert_var,
            out_var=insert_var,
            full_table=full,
            tech_key=tech_key,
        )
    )

    merge_cond = " AND ".join(
        f"t.`{table_f}` <=> s.`{table_f}`" for _, table_f in keys
    )
    values_map = {c: f"s.`{c}`" for c in insert_cols}
    lines.append(f"{insert_var}.createOrReplaceTempView({insert_view!r})")
    _emit_delta_not_matched_insert(
        lines,
        full=full,
        source_view=insert_view,
        on_sql=merge_cond,
        values_map=values_map,
    )

    # Avoid second full table scan: union prior lookup keys with newly inserted keys
    lines.append(
        "# Attach TK without re-scanning dimension: union prior keys with inserts "
        "(map table fields back to stream names)"
    )
    new_key_parts: list[str] = []
    for stream_f, table_f in keys:
        if stream_f != table_f:
            new_key_parts.append(f'col("{table_f}").alias("{stream_f}")')
        else:
            new_key_parts.append(f'col("{table_f}")')
    new_key_parts.append(f'col("{tech_key}")')
    lines.append(
        f"_combo_new_keys = {insert_var}.select({', '.join(new_key_parts)})"
    )
    lines.append(f"{lkp_var} = {lkp_var}.unionByName(_combo_new_keys)")
    if do_broadcast:
        lines.append(f"{lkp_var} = broadcast({lkp_var})")
    lines.append(f"{out_var} = {in_df}.join({lkp_var}, on={on_expr}, how='left')")
    lines.append(
        "# Null surrogate keys after MERGE indicate unresolved/null business keys"
    )
    if replace_fields:
        drop_cols = ", ".join(f'"{s}"' for s, _ in keys)
        lines.append(f"{out_var} = {out_var}.drop({drop_cols})")

    logger.info(
        "CombinationLookup '%s' → DeltaTable.merge %s on %s keys",
        step_name,
        full,
        len(keys),
    )
    return lines, "converted"


def convert_dimension_lookup_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate SCD-aware Dimension Lookup/Update Delta MERGE PySpark."""
    lines = [f"# Dimension Lookup/Update: {step_name}"]
    safe = _safe_ident(out_var)

    if not in_df:
        msg = f"DimensionLookup '{step_name}': requires one input stream"
        lines.append(f"# WARNING: {msg}")
        lines.append(
            f"{out_var} = spark.createDataFrame([], '_dimension_lookup_unresolved STRING')"
        )
        logger.warning(msg)
        return lines, "partial"

    full = _qualified_table(metadata, context)
    if not full:
        msg = f"DimensionLookup '{step_name}': dimension table missing"
        lines.append(f"# WARNING: {msg}")
        lines.append(f"{out_var} = {in_df}")
        logger.warning(msg)
        return lines, "partial"

    keys = _business_keys(metadata)
    fields = _dim_fields(metadata)
    update_mode = _as_bool(metadata.get("update"), default=True)
    tech_key = (metadata.get("technical_key") or "").strip() or "technical_key"
    tech_rename = (metadata.get("technical_key_rename") or "").strip() or tech_key
    version_field = (metadata.get("version_field") or "").strip()
    stream_date = (metadata.get("stream_datefield") or "").strip()
    date_from = (metadata.get("date_from") or "").strip()
    date_to = (metadata.get("date_to") or "").strip()
    min_year = int(metadata.get("min_year") or 1900)
    max_year = int(metadata.get("max_year") or 2199)
    creation = (metadata.get("tech_key_creation") or "tablemax").strip().lower()
    use_autoinc = _as_bool(metadata.get("use_autoinc"))
    connection = (metadata.get("connection") or "").strip()
    use_start_alt = _as_bool(metadata.get("use_start_date_alternative"))
    start_alt = _normalize_start_date_alt(metadata.get("start_date_alternative"))
    start_date_field = (metadata.get("start_date_field_name") or "").strip()

    type1_fields = [f for f in fields if f["update_type"] in _TYPE1]
    type2_fields = [f for f in fields if f["update_type"] in _TYPE2]
    punch_fields = [f for f in fields if f["update_type"] in _PUNCH]
    technical_fields = [f for f in fields if f["update_type"] in _TECHNICAL]
    current_flag_field = next(
        (f["table_field"] for f in technical_fields if f["update_type"] == "LastVersion"),
        "",
    )

    if connection:
        lines.append(f"# preserved.connection={connection!r}")
        lines.append(
            f"# WARNING: DimensionLookup '{step_name}': connection {connection!r} "
            f"mapped to Spark/UC table {full!r} (not JDBC)."
        )

    _preserve_lines(
        lines,
        metadata,
        "commit_size",
        "cache_size",
        "preload_cache",
        "sequence_name",
        "use_start_date_alternative",
        "start_date_alternative",
        "start_date_field_name",
        "use_batch",
        "min_year",
        "max_year",
    )
    lines.append(
        f"# SCD mode: {'update' if update_mode else 'lookup-only'}; "
        f"Type1={len(type1_fields)} "
        f"Type2={len(type2_fields)} PunchThrough={len(punch_fields)} "
        f"technical={len(technical_fields)}"
    )

    _tech_key_warnings(
        lines,
        "DimensionLookup",
        step_name,
        creation,
        str(metadata.get("sequence_name") or ""),
        use_autoinc,
    )
    _emit_delta_tips(lines, date_from=date_from, date_to=date_to, keys=keys)

    if not keys:
        msg = (
            f"DimensionLookup '{step_name}': missing business keys — "
            "cannot lookup/MERGE; attaching null technical key"
        )
        lines.append(f"# WARNING: {msg}")
        lines.append(
            f"{out_var} = {in_df}.withColumn({tech_rename!r}, lit(None).cast(\"long\"))"
        )
        logger.warning(msg)
        return lines, "partial"

    dim_var = f"_dim_{safe}"
    active_var = f"_dim_active_{safe}"
    do_broadcast, bc_comments = _should_broadcast(metadata)

    attr_table_fields = [
        f["table_field"]
        for f in (type1_fields + type2_fields + punch_fields + technical_fields)
    ]
    lookup_cols = _dim_read_columns(
        tech_key=tech_key,
        keys=keys,
        date_from=date_from,
        date_to=date_to,
        version_field=version_field,
        current_flag_field=current_flag_field,
        extra_table_fields=attr_table_fields,
    )
    cmp_cols = _dim_read_columns(
        tech_key=tech_key,
        keys=keys,
        date_from="",
        date_to="",
        version_field=version_field,
        current_flag_field=current_flag_field,
        extra_table_fields=[f["table_field"] for f in type2_fields],
    )
    rejoin_cols = _dim_read_columns(
        tech_key=tech_key,
        keys=keys,
        date_from=date_from,
        date_to=date_to,
        version_field=version_field,
        current_flag_field=current_flag_field,
        extra_table_fields=attr_table_fields,
    )

    if not update_mode:
        # Lookup-only: build join (only path that needs the pre-MERGE lookup join)
        lines.append(
            f"{dim_var} = spark.table({full!r}).select({_select_expr(lookup_cols)})"
        )
        use_date_between = bool(date_from and date_to and stream_date)
        # Predicate pushdown: filter active rows before join when not date-between
        already_filtered_flag = False
        if not use_date_between and current_flag_field:
            lines.append(
                f"{dim_var} = {dim_var}.filter("
                f"col({current_flag_field!r}) == lit(True))"
            )
            already_filtered_flag = True
        lines.extend(bc_comments)
        if do_broadcast:
            lines.append(f"{dim_var} = broadcast({dim_var})")

        _emit_active_dim(
            lines,
            dim_var=dim_var,
            active_var=active_var,
            keys=keys,
            date_from=date_from,
            date_to=date_to,
            stream_date=stream_date,
            # Skip re-filter when already pushed down above
            current_flag_field="" if already_filtered_flag else current_flag_field,
            version_field=version_field,
            for_join=True,
        )

        join_parts: list[str] = [
            f'({in_df}["{stream_f}"] == {active_var}["{table_f}"])'
            for stream_f, table_f in keys
        ]
        if use_date_between:
            lines.append(
                "# Late-arriving / expired / overlap: filter to version covering stream date"
            )
            join_parts.append(
                f'({in_df}["{stream_date}"] >= {active_var}["{date_from}"])'
            )
            join_parts.append(
                f'({in_df}["{stream_date}"] < {active_var}["{date_to}"])'
            )
        join_expr = " & ".join(join_parts)
        lines.append(
            f"_dim_joined = {in_df}.join({active_var}, on=({join_expr}), how='left')"
        )
        if tech_rename != tech_key:
            lines.append(
                f"_dim_joined = _dim_joined.withColumnRenamed("
                f"{tech_key!r}, {tech_rename!r})"
            )
        lines.append(f"{out_var} = _dim_joined")
        lines.append(
            f"# Lookup-only: null {tech_rename!r} indicates cache miss / unknown BK"
        )
        logger.info("DimensionLookup '%s' lookup-only against %s", step_name, full)
        return lines, "converted"

    # --- Update mode: skip initial _dim_joined (wasted before MERGEs) ---
    lines.append(
        "# Update mode: skip pre-MERGE lookup join; re-join after MERGEs below"
    )

    src_view = f"_dim_scd_src_{safe}"
    new_view = f"_dim_scd_new_{safe}"
    src_var = f"_dim_src_{safe}"
    cmp_var = f"_dim_cmp_{safe}"
    new_var = f"_dim_new_{safe}"

    # Bounds MUST be defined before effective-date expr (null alt uses _scd_min_ts)
    lines.append(
        f"_scd_max_ts = lit(\"{max_year}-12-31 23:59:59.999\").cast(\"timestamp\")"
    )
    lines.append(
        f"_scd_min_ts = lit(\"{min_year}-01-01 00:00:00\").cast(\"timestamp\")"
    )
    lines.append("# preserved.scd_min_ts / scd_max_ts for open-ended version bounds")

    date_expr, date_comments = _effective_date_expr(
        stream_date=stream_date,
        alt=start_alt,
        start_date_field=start_date_field,
        min_year=min_year,
        use_alternative=use_start_alt,
    )
    lines.extend(date_comments)
    lines.append(f"{src_var} = {in_df}.withColumn(\"_scd_effective\", {date_expr})")

    null_bk = _null_bk_predicate(keys)
    bk_stream_list = ", ".join(f'"{s}"' for s, _ in keys)
    lines.append(
        f"{src_var} = {src_var}.filter(~({null_bk})).dropDuplicates([{bk_stream_list}])"
    )
    lines.append(
        "# Null business keys skipped; duplicate BK rows deduplicated before MERGE"
    )
    lines.append(f"{src_var}.createOrReplaceTempView({src_view!r})")

    merge_on = " AND ".join(
        f"t.`{table_f}` <=> s.`{stream_f}`" for stream_f, table_f in keys
    )
    if current_flag_field:
        merge_on_active = merge_on + f" AND t.`{current_flag_field}` = true"
    elif date_to:
        merge_on_active = (
            merge_on
            + f" AND t.`{date_to}` >= TIMESTAMP '{max_year}-12-31 23:59:59.999'"
        )
        lines.append(
            "# Active version predicate approximated via date_to >= max_year boundary"
        )
    else:
        merge_on_active = merge_on

    # Snapshot active for change detection (column prune + predicate pushdown)
    lines.append(
        f"_dim_cmp_active = spark.table({full!r}).select({_select_expr(cmp_cols)})"
    )
    if current_flag_field:
        lines.append(
            f"_dim_cmp_active = _dim_cmp_active.filter("
            f"col({current_flag_field!r}) == lit(True))"
        )
    elif date_to:
        lines.append(
            f"_dim_cmp_active = _dim_cmp_active.filter(col({date_to!r}) >= _scd_max_ts)"
        )
    lines.extend(bc_comments)
    if do_broadcast:
        lines.append("_dim_cmp_active = broadcast(_dim_cmp_active)")

    # Align dim BK columns to stream names for equi-join with source
    rename_cmp: list[str] = []
    for stream_f, table_f in keys:
        if stream_f != table_f:
            rename_cmp.append(f'col("{table_f}").alias("{stream_f}")')
        else:
            rename_cmp.append(f'col("{table_f}")')
    rename_cmp.append(f'col("{tech_key}").alias("_prior_tk")')
    helper_drops = ["_prior_tk"]
    if version_field:
        rename_cmp.append(f'col("{version_field}").alias("_prior_version")')
        helper_drops.append("_prior_version")
    for f in type2_fields:
        alias = f"_prior_{f['table_field']}"
        rename_cmp.append(f'col("{f["table_field"]}").alias("{alias}")')
        helper_drops.append(alias)
    lines.append(
        f"_dim_cmp_active = _dim_cmp_active.select({', '.join(rename_cmp)})"
    )
    bk_on = (
        f"[{', '.join(repr(s) for s, _ in keys)}]"
        if len(keys) > 1
        else repr(keys[0][0])
    )
    lines.append(
        f"{cmp_var} = {src_var}.join(_dim_cmp_active, on={bk_on}, how='left')"
    )

    # --- Type 1 MERGE (active row only) ---
    type1_set_map: dict[str, str] = {}
    for f in type1_fields:
        type1_set_map[f["table_field"]] = f"s.`{f['stream_field']}`"
    for f in technical_fields:
        if f["update_type"] in ("DateUpdated", "DateInsertedOrUpdated"):
            type1_set_map[f["table_field"]] = "s.`_scd_effective`"
    type1_changed = [
        f"NOT (t.`{f['table_field']}` <=> s.`{f['stream_field']}`)"
        for f in type1_fields
    ]
    if type1_set_map:
        match_cond = f"({' OR '.join(type1_changed)})" if type1_changed else None
        lines.append(f"# Type 1 overwrite (SCD1) MERGE for '{step_name}'")
        _emit_delta_matched_update(
            lines,
            full=full,
            source_view=src_view,
            on_sql=merge_on_active,
            set_map=type1_set_map,
            match_condition=match_cond,
        )

    # --- PunchThrough MERGE (ALL versions; BK only, no active filter) ---
    if punch_fields:
        punch_set_map = {
            f["table_field"]: f"s.`{f['stream_field']}`" for f in punch_fields
        }
        lines.append(
            f"# PunchThrough ({len(punch_fields)} fields): UPDATE all historical versions"
        )
        _emit_delta_matched_update(
            lines,
            full=full,
            source_view=src_view,
            on_sql=merge_on,
            set_map=punch_set_map,
        )

    # --- Type 2 expire active when date_to and/or current_flag exist ---
    expire_set_map: dict[str, str] = {}
    if date_to:
        expire_set_map[date_to] = "s.`_scd_effective`"
    if current_flag_field:
        expire_set_map[current_flag_field] = "false"
    if version_field:
        lines.append(f"# Version field preserved: {version_field}")

    changed_pred_parts = [
        f"NOT (t.`{f['table_field']}` <=> s.`{f['stream_field']}`)"
        for f in type2_fields
    ]
    change_pred = " OR ".join(changed_pred_parts) if changed_pred_parts else "false"

    if expire_set_map and type2_fields:
        lines.append("# Type 2 expire active version when attributes change")
        _emit_delta_matched_update(
            lines,
            full=full,
            source_view=src_view,
            on_sql=merge_on_active,
            set_map=expire_set_map,
            match_condition=f"({change_pred})",
        )

    # --- Insert candidates: new BK / type2 change / re-insert after expire ---
    type2_change_exprs = [
        f'(~col("_prior_{f["table_field"]}").eqNullSafe(col("{f["stream_field"]}")))'
        for f in type2_fields
    ]
    filter_parts = ['col("_prior_tk").isNull()']
    if type2_change_exprs:
        type2_or = " | ".join(type2_change_exprs)
        filter_parts.append(f"({type2_or})")
        if expire_set_map:
            filter_parts.append(
                f'(col("_prior_tk").isNotNull() & ({type2_or}))'
            )
    insert_filter = " | ".join(dict.fromkeys(filter_parts))
    lines.append("# Build insert candidates (new BK and/or Type 2 attribute changes)")
    lines.append(f"{new_var} = {cmp_var}.filter({insert_filter})")

    if version_field:
        lines.append(
            f"{new_var} = {new_var}.withColumn("
            f"{version_field!r}, "
            f"(coalesce(col(\"_prior_version\"), lit(0)) + lit(1)).cast(\"long\"))"
        )
    else:
        lines.append("# No version field — new rows inserted without version bump")

    if helper_drops:
        lines.append(
            f"{new_var} = {new_var}.drop({', '.join(repr(c) for c in helper_drops)})"
        )

    lines.append("# Assign surrogate keys for new / Type2 rows (tablemax)")
    lines.extend(
        _assign_surrogate_key_lines(
            df_var=new_var,
            out_var=new_var,
            full_table=full,
            tech_key=tech_key,
        )
    )

    insert_values: dict[str, str] = {tech_key: f"s.`{tech_key}`"}
    for stream_f, table_f in keys:
        insert_values[table_f] = f"s.`{stream_f}`"
    for f in fields:
        if f["update_type"] in _TECHNICAL:
            continue
        if f["table_field"] not in insert_values:
            insert_values[f["table_field"]] = f"s.`{f['stream_field']}`"
    if date_from:
        insert_values[date_from] = "s.`_scd_effective`"
    if date_to:
        insert_values[date_to] = f"TIMESTAMP '{max_year}-12-31 23:59:59.999'"
    if version_field and version_field not in insert_values:
        insert_values[version_field] = f"s.`{version_field}`"
    if current_flag_field and current_flag_field not in insert_values:
        insert_values[current_flag_field] = "true"
    for f in technical_fields:
        if f["update_type"] in (
            "DateInserted",
            "DateInsertedOrUpdated",
            "DateUpdated",
        ):
            if f["table_field"] not in insert_values:
                insert_values[f["table_field"]] = "s.`_scd_effective`"

    lines.append(f"{new_var}.createOrReplaceTempView({new_view!r})")
    lines.append(
        "# Single MERGE INSERT on technical key (new SK never matches existing)"
    )
    _emit_delta_not_matched_insert(
        lines,
        full=full,
        source_view=new_view,
        on_sql=f"t.`{tech_key}` <=> s.`{tech_key}`",
        values_map=insert_values,
    )

    lines.append(
        "# Schema evolution: add new attribute columns with ALTER TABLE before MERGE"
    )
    lines.append(
        "# Effective date overlaps / multiple actives: enforce with constraints or "
        "dedupe window on (business_keys, date_from)"
    )

    # ALWAYS re-join after MERGEs (never reuse pre-update _dim_joined)
    lines.extend(bc_comments)
    _rejoin_dimension(
        lines,
        in_df=in_df,
        out_var=out_var,
        dim_var=dim_var,
        full=full,
        keys=keys,
        date_from=date_from,
        date_to=date_to,
        stream_date=stream_date,
        current_flag_field=current_flag_field,
        version_field=version_field,
        tech_key=tech_key,
        tech_rename=tech_rename,
        broadcast=do_broadcast,
        select_cols=rejoin_cols,
    )

    lines.append(
        f"# Null {tech_rename!r} after update indicates unresolved BK / null keys"
    )
    logger.info(
        "DimensionLookup '%s' SCD update against %s (T1=%d T2=%d Punch=%d)",
        step_name,
        full,
        len(type1_fields),
        len(type2_fields),
        len(punch_fields),
    )
    return lines, "converted"
