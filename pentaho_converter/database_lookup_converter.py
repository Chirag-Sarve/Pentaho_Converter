"""Convert Pentaho Database Lookup step metadata to PySpark lookup join code."""

from __future__ import annotations

from typing import Any

from .step_context import StepContext
from .step_xml import JoinKeyPair, format_spark_join_on
from .value_mapper_converter import _target_literal


def _join_keys_from_metadata(metadata: dict[str, Any]) -> list[JoinKeyPair]:
    """Build join key pairs from propagated parser metadata only."""
    keys: list[JoinKeyPair] = []

    for source in (metadata.get("join_keys"), metadata.get("keys")):
        if not source:
            continue
        for pair in source:
            if not isinstance(pair, dict):
                continue
            left = (pair.get("left") or pair.get("stream_field") or "").strip()
            right = (pair.get("right") or pair.get("table_field") or left).strip()
            if left:
                keys.append(JoinKeyPair(left=left, right=right or left))
        if keys:
            return keys

    return keys


def _return_fields_from_metadata(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for item in metadata.get("return_fields") or []:
        if isinstance(item, dict) and item.get("name"):
            fields.append(item)
    return fields


def _lookup_table_ref(metadata: dict[str, Any]) -> tuple[str, str]:
    schema = (metadata.get("schema") or "").strip()
    table = (metadata.get("table") or "").strip()
    if not table:
        attrs = metadata.get("attributes") or {}
        table = (attrs.get("table") or "").strip()
        schema = schema or (attrs.get("schema") or "").strip()
    qualified = f"{schema}.{table}" if schema else table
    return qualified, table


def _should_broadcast(metadata: dict[str, Any]) -> bool:
    cached = metadata.get("cached")
    if isinstance(cached, bool):
        if cached:
            return True
    elif str(cached or "").strip().upper() in ("Y", "YES", "TRUE", "1", "T"):
        return True

    cache_size = metadata.get("cache_size", 0)
    try:
        return int(cache_size or 0) > 0
    except (TypeError, ValueError):
        return False


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


def _spark_cast_type(type_name: str) -> str:
    t = (type_name or "").strip().lower()
    mapping = {
        "integer": "int",
        "int": "int",
        "long": "long",
        "number": "double",
        "bignumber": "decimal(38,18)",
        "float": "float",
        "double": "double",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "datetime": "timestamp",
        "binary": "binary",
    }
    return mapping.get(t, "string" if t == "string" else t)


def _apply_cast(expr: str, type_name: str) -> str:
    cast_type = _spark_cast_type(type_name)
    if not cast_type or cast_type == "string":
        return expr
    return f"{expr}.cast({cast_type!r})"


def _lookup_missed_condition(field: dict[str, Any]) -> str:
    lookup_name = (field.get("name") or "").strip()
    if not lookup_name:
        return "lit(True)"
    return f'col("{lookup_name}").isNull()'


def _lookup_needed_columns(
    keys: list[JoinKeyPair],
    return_fields: list[dict[str, Any]],
) -> list[str]:
    needed: list[str] = []
    for key in keys:
        column = key.right or key.left
        if column and column not in needed:
            needed.append(column)
    for field in return_fields:
        name = (field.get("name") or "").strip()
        if name and name not in needed:
            needed.append(name)
    return needed


def _input_columns(context: StepContext) -> list[str]:
    lineage_map = context.extra.get("lineage_map", {})
    preds = context.dag.predecessors(context.step.name)
    if not preds:
        return []
    columns = lineage_map.get(preds[0])
    if isinstance(columns, dict):
        return sorted(columns.keys())
    return []


def _return_field_expr(
    field: dict[str, Any],
) -> str:
    lookup_name = (field.get("name") or "").strip()
    out_name = (field.get("rename") or "").strip() or lookup_name
    default = field.get("default")
    default = "" if default is None else str(default)
    type_name = field.get("type_name") or field.get("type") or "String"

    lookup_col = f'col("{lookup_name}")'
    lookup_missed = _lookup_missed_condition(field)
    if default:
        value_expr = (
            f"when({lookup_missed}, {_target_literal(default, type_name)})"
            f".otherwise({lookup_col})"
        )
    else:
        value_expr = lookup_col

    value_expr = _apply_cast(value_expr, type_name)
    return f"{value_expr}.alias({out_name!r})"


def _unresolved_lines(step_name: str, out_var: str, message: str) -> list[str]:
    return [
        f"# WARNING: DatabaseLookup '{step_name}': {message}",
        f"{out_var} = spark.createDataFrame([], '_database_lookup_unresolved STRING')",
    ]


def convert_database_lookup_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate PySpark lines for a Database Lookup step from propagated metadata."""
    lines = [f"# Database Lookup: {step_name}"]

    if not in_df:
        lines.extend(
            _unresolved_lines(step_name, out_var, "requires one input stream, found none")
        )
        return lines, "partial"

    qualified_table, table = _lookup_table_ref(metadata)
    if not table:
        lines.extend(
            _unresolved_lines(step_name, out_var, "lookup table missing from metadata")
        )
        return lines, "partial"

    keys = _join_keys_from_metadata(metadata)
    if not keys:
        lines.extend(
            _unresolved_lines(step_name, out_var, "no lookup keys in metadata — join not generated")
        )
        return lines, "partial"

    return_fields = _return_fields_from_metadata(metadata)
    lkp_var = f"_lkp_{out_var}"
    joined_var = f"_joined_{out_var}"
    connection = (metadata.get("connection") or "").strip()

    if connection:
        lines.append(f"# preserved.connection={connection!r}")
        lines.append(
            f"# WARNING: DatabaseLookup '{step_name}': connection {connection!r} "
            "is not opened via JDBC here — reading from Spark catalog "
            f"({qualified_table!r}). Map the Pentaho connection to a UC table "
            "or replace with spark.read.jdbc(...) if external."
        )

    lines.append(f"{lkp_var} = spark.table({qualified_table!r})")

    # Preserve residual cache / fail options not otherwise shown
    if metadata.get("cached") is not None:
        lines.append(f"# preserved.cached={metadata.get('cached')!r}")
    if metadata.get("cache_size"):
        lines.append(f"# preserved.cache_size={metadata.get('cache_size')!r}")
    if metadata.get("orderby"):
        lines.append(f"# preserved.orderby={metadata.get('orderby')!r}")

    # BETWEEN / name2 style keys are not expressible as equi-join pairs
    for key in metadata.get("keys") or []:
        if isinstance(key, dict) and (key.get("name2") or "").strip():
            lines.append(
                f"# WARNING: DatabaseLookup '{step_name}': BETWEEN/name2 key "
                f"{key!r} is not supported — equi-join on primary key only"
            )

    needed_cols = _lookup_needed_columns(keys, return_fields)
    if needed_cols:
        col_list = ", ".join(f'"{column}"' for column in needed_cols)
        lines.append(f"{lkp_var} = {lkp_var}.select({col_list})")

    orderby = (metadata.get("orderby") or "").strip()
    if orderby:
        order_field = orderby.split()[0].strip()
        if order_field:
            lines.append(f"{lkp_var} = {lkp_var}.orderBy(col({order_field!r}))")

    key_subset = ", ".join(f'"{key.right or key.left}"' for key in keys)
    lines.append(f"{lkp_var} = {lkp_var}.dropDuplicates([{key_subset}])")

    join_target = f"broadcast({lkp_var})" if _should_broadcast(metadata) else lkp_var
    on_arg, use_on = format_spark_join_on(in_df, lkp_var, keys)
    if use_on:
        lines.append(
            f"{joined_var} = {in_df}.join({join_target}, on={on_arg}, how='left')"
        )
    else:
        lines.append(f"{joined_var} = {in_df}.join({join_target}, {on_arg}, 'left')")

    if _bool_setting(metadata, "eat_row_on_failure"):
        match_cond = " & ".join(
            f'col("{key.right or key.left}").isNotNull()' for key in keys
        )
        lines.append(f"{joined_var} = {joined_var}.filter({match_cond})")

    return_exprs = [_return_field_expr(field) for field in return_fields]

    main_cols = _input_columns(context) if context is not None else []
    if main_cols:
        select_parts = [f'{in_df}["{column}"]' for column in main_cols]
        select_parts.extend(return_exprs)
        lines.append(f"{out_var} = {joined_var}.select({', '.join(select_parts)})")
    elif return_exprs:
        lines.append(
            f"# WARNING: DatabaseLookup '{step_name}': column lineage unavailable — "
            "preserving main-stream columns via runtime schema"
        )
        lines.append(f"_main_cols_{out_var} = {in_df}.columns")
        return_names = ", ".join(return_exprs)
        lines.append(
            f"{out_var} = {joined_var}.select("
            f"*[col(c) for c in _main_cols_{out_var}], {return_names})"
        )
    else:
        lines.append(
            f"# WARNING: DatabaseLookup '{step_name}': column lineage unavailable — "
            "join output may contain duplicate lookup columns"
        )
        lines.append(f"{out_var} = {joined_var}")

    if _bool_setting(metadata, "fail_on_multiple"):
        lines.append(
            f"# WARNING: DatabaseLookup '{step_name}': fail_on_multiple is set — "
            "multiple lookup matches are deduplicated via dropDuplicates; "
            "runtime duplicate detection is not emitted"
        )

    return lines, "converted"
