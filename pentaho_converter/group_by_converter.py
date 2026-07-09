"""Convert Pentaho Group By step metadata to PySpark groupBy().agg() code."""

from __future__ import annotations

from typing import Any

_AGGREGATE_ALIASES: dict[str, str] = {
    "AVERAGE": "AVG",
    "COUNT ANY": "COUNT_ALL",
    "COUNT_ANY": "COUNT_ALL",
    "COUNT DISTINCT": "COUNT_DISTINCT",
}


def _normalize_agg_type(raw: str) -> str:
    text = (raw or "SUM").strip().upper()
    return _AGGREGATE_ALIASES.get(text, text)


def _col(field_name: str) -> str:
    if not field_name:
        return "lit(None)"
    return f'col("{field_name}")'


def _bool_setting(metadata: dict[str, Any], key: str) -> bool:
    if key in metadata:
        val = metadata[key]
        if isinstance(val, bool):
            return val
        return str(val or "").strip().upper() in ("Y", "YES", "TRUE", "1", "T")
    attrs = metadata.get("attributes") or {}
    aval = attrs.get(key)
    if aval is None:
        return False
    if isinstance(aval, bool):
        return aval
    return str(aval or "").strip().upper() in ("Y", "YES", "TRUE", "1", "T")


def _aggregates_from_fields(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    derived: list[dict[str, Any]] = []
    for field in metadata.get("fields") or []:
        agg_type = (field.get("aggregate") or "").strip()
        name = (field.get("name") or "").strip()
        if not name or not agg_type:
            continue
        entry = {k: v for k, v in field.items() if v not in (None, "")}
        entry.setdefault("subject", field.get("subject") or field.get("valuefield") or name)
        derived.append(entry)
    return derived


def _aggregates_from_metadata(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    aggregates = list(metadata.get("aggregates") or metadata.get("aggregate_fields") or [])
    if aggregates:
        return aggregates
    return _aggregates_from_fields(metadata)


def _group_keys_from_metadata(
    metadata: dict[str, Any],
    fallback_group_keys: list[str] | None = None,
) -> list[str]:
    keys = list(metadata.get("group_keys") or [])
    if keys:
        return keys

    for field in metadata.get("fields") or []:
        name = (field.get("name") or "").strip()
        aggregate = (field.get("aggregate") or "").strip()
        if name and not aggregate and name not in keys:
            keys.append(name)

    if keys:
        return keys

    agg_names = {
        (agg.get("name") or "").strip()
        for agg in _aggregates_from_metadata(metadata)
        if agg.get("name")
    }
    fallback = [k for k in (fallback_group_keys or []) if k not in agg_names]
    return fallback


def _field_type_meta(metadata: dict[str, Any], output_name: str) -> dict[str, str]:
    merged: dict[str, str] = {}
    for field in metadata.get("fields") or []:
        if field.get("name") == output_name:
            merged.update({k: str(v) for k, v in field.items() if v not in (None, "")})
    for agg in _aggregates_from_metadata(metadata):
        if agg.get("name") == output_name:
            merged.update({k: str(v) for k, v in agg.items() if v not in (None, "")})
    return merged


def _subject_column(agg_meta: dict[str, Any]) -> str:
    valuefield = (agg_meta.get("valuefield") or "").strip()
    if valuefield:
        return valuefield
    subject = (agg_meta.get("subject") or "").strip()
    name = (agg_meta.get("name") or "").strip()
    if subject and subject != name:
        return subject
    return subject or name


def _resolve_output_type(type_meta: dict[str, str]) -> str:
    explicit = (type_meta.get("type") or "").strip()
    if explicit:
        return explicit.lower()

    # GroupByField.type_name defaults to String when XML omits <type>; skip that default.
    if "type" in type_meta and not explicit:
        return ""

    type_name = (type_meta.get("type_name") or "").strip()
    if type_name and type_name.lower() != "string":
        return type_name.lower()
    return ""


def _apply_output_cast(expr: str, agg_meta: dict[str, Any], metadata: dict[str, Any]) -> str:
    output_name = (agg_meta.get("name") or "").strip()
    type_meta = _field_type_meta(metadata, output_name) if output_name else agg_meta
    type_name = _resolve_output_type(type_meta)
    if not type_name:
        return expr

    length = (type_meta.get("length") or type_meta.get("value_length") or "").strip()
    precision = (type_meta.get("precision") or type_meta.get("value_precision") or "").strip()

    if type_name in ("integer", "int"):
        return f"({expr}).cast('int')"
    if type_name in ("number", "bignumber", "float", "double"):
        if precision.isdigit() and length.isdigit():
            return f"({expr}).cast('decimal({length},{precision})')"
        return f"({expr}).cast('double')"
    if type_name == "string":
        return f"({expr}).cast('string')"
    if type_name in ("date", "timestamp", "datetime"):
        return f"({expr}).cast('timestamp')"
    if type_name in ("boolean", "bool"):
        return f"({expr}).cast('boolean')"
    return expr


def _aggregate_core_expr(agg_type: str, column: str) -> str | None:
    mapping: dict[str, str] = {
        "SUM": f"sum({column})",
        "AVG": f"avg({column})",
        "MIN": f"min({column})",
        "MAX": f"max({column})",
        "COUNT": f"count({column})",
        "COUNT_ALL": "count(lit(1))",
        "COUNT_DISTINCT": f"countDistinct({column})",
        "FIRST": f"first({column}, ignorenulls=True)",
        "FIRST_INCL_NULL": f"first({column}, ignorenulls=False)",
        "LAST": f"last({column}, ignorenulls=True)",
        "LAST_INCL_NULL": f"last({column}, ignorenulls=False)",
    }
    return mapping.get(agg_type)


def _aggregate_expr(agg_meta: dict[str, Any], metadata: dict[str, Any]) -> str:
    agg_type = _normalize_agg_type(str(agg_meta.get("aggregate", "")))
    subject = _subject_column(agg_meta)
    column = _col(subject)

    expr = _aggregate_core_expr(agg_type, column)
    if expr is None:
        expr = f"sum({column})  # unknown aggregate: {agg_type}"

    return _apply_output_cast(expr, agg_meta, metadata)


def _window_aggregate_expr(
    agg_meta: dict[str, Any],
    metadata: dict[str, Any],
    window_frame: str,
) -> str:
    agg_type = _normalize_agg_type(str(agg_meta.get("aggregate", "")))
    subject = _subject_column(agg_meta)
    column = _col(subject)

    core = _aggregate_core_expr(agg_type, column)
    if core is None:
        core = f"sum({column})  # unknown aggregate: {agg_type}"

    expr = f"{core}.over({window_frame})"
    return _apply_output_cast(expr, agg_meta, metadata)


def _default_empty_row_selectors(
    group_keys: list[str],
    aggregates: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> list[str]:
    selectors: list[str] = []
    for key in group_keys:
        selectors.append(f"lit(None).alias({key!r})")
    for agg in aggregates:
        agg_type = _normalize_agg_type(str(agg.get("aggregate", "")))
        name = (agg.get("name") or "").strip()
        if not name:
            continue
        if agg_type in ("COUNT", "COUNT_ALL", "COUNT_ANY", "COUNT_DISTINCT"):
            expr = "lit(0)"
        else:
            expr = "lit(None)"
        selectors.append(f"{_apply_output_cast(expr, agg, metadata)}.alias({name!r})")
    return selectors


def _group_part(group_keys: list[str]) -> str:
    if not group_keys:
        return "groupBy()"
    return f"groupBy({', '.join(repr(k) for k in group_keys)})"


def _all_rows_flag_field(metadata: dict[str, Any]) -> str | None:
    attrs = metadata.get("attributes") or {}
    if not _bool_setting(metadata, "all_rows"):
        return None
    ignore_aggregate = str(attrs.get("ignore_aggregate", "")).strip().upper() in (
        "Y",
        "YES",
        "TRUE",
        "1",
        "T",
    )
    if not ignore_aggregate:
        return None
    flag = (attrs.get("field_ignore") or "").strip()
    return flag or "_aggregate_row"


def convert_aggregate(agg_meta: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
    """Convert one aggregate field metadata entry to a PySpark agg expression."""
    return _aggregate_expr(agg_meta, metadata or {})


def convert_group_by_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
    fallback_group_keys: list[str] | None = None,
) -> list[str]:
    """Generate PySpark lines for a Group By step from propagated metadata."""
    lines = [f"# Group By: {step_name}"]
    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines

    group_keys = _group_keys_from_metadata(metadata, fallback_group_keys)
    aggregates = _aggregates_from_metadata(metadata)
    named_aggregates = [agg for agg in aggregates if agg.get("name")]
    give_back_row = _bool_setting(metadata, "give_back_row")
    all_rows = _bool_setting(metadata, "all_rows")
    aggregate_flag_field = _all_rows_flag_field(metadata)

    if all_rows and named_aggregates:
        if group_keys:
            partition = ", ".join(repr(k) for k in group_keys)
            lines.append(f"_w_gb = Window.partitionBy({partition})")
        else:
            lines.append("_w_gb = Window.partitionBy(lit(1))")
        window_frame = "_w_gb.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)"
        lines.append(f"{out_var} = {in_df}")
        for agg in named_aggregates:
            expr = _window_aggregate_expr(agg, metadata, window_frame)
            lines.append(f"{out_var} = {out_var}.withColumn({agg['name']!r}, {expr})")
        if aggregate_flag_field:
            lines.append(
                f"{out_var} = {out_var}.withColumn({aggregate_flag_field!r}, lit(False))"
            )
        return lines

    if named_aggregates:
        agg_exprs = [
            f"{_aggregate_expr(agg, metadata)}.alias({agg['name']!r})"
            for agg in named_aggregates
        ]
        group_part = _group_part(group_keys)
        agg_var = f"_{out_var}_gb_agg" if give_back_row else out_var
        lines.append(f"{agg_var} = {in_df}.{group_part}.agg({', '.join(agg_exprs)})")
        if give_back_row:
            default_select = _default_empty_row_selectors(group_keys, named_aggregates, metadata)
            lines.append(f"_gb_src_count = {in_df}.agg(count(lit(1)).alias('_gb_n'))")
            lines.append(
                f"_gb_default = _gb_src_count.filter(col('_gb_n') == 0)"
                f".select({', '.join(default_select)})"
            )
            lines.append(f"{out_var} = {agg_var}.unionByName(_gb_default)")
        elif agg_var != out_var:
            lines.append(f"{out_var} = {agg_var}")
        return lines

    if give_back_row and group_keys:
        partition = ", ".join(repr(k) for k in group_keys)
        lines.append(f"_w_gb = Window.partitionBy({partition}).orderBy(monotonically_increasing_id())")
        lines.append(
            f"{out_var} = {in_df}.withColumn('_gb_rn', row_number().over(_w_gb))"
            f".filter(col('_gb_rn') == 1).drop('_gb_rn')"
        )
        return lines

    if group_keys:
        select_cols = ", ".join(f'col("{key}")' for key in group_keys)
        lines.append(f"{out_var} = {in_df}.select({select_cols}).distinct()")
        return lines

    lines.append(f"{out_var} = {in_df}.groupBy().agg(count(lit(1)).alias('row_count'))")
    return lines
