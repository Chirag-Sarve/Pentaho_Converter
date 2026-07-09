"""Convert Pentaho Merge Join step metadata to PySpark DataFrame join code."""

from __future__ import annotations

from typing import Any

from .step_context import StepContext
from .step_xml import JoinKeyPair, format_spark_join_on

_JOIN_TYPE_MAP: dict[str, str] = {
    "inner": "inner",
    "left outer": "left",
    "left": "left",
    "right outer": "right",
    "right": "right",
    "full outer": "outer",
    "full": "outer",
    "cross": "cross",
}


def _join_keys_from_metadata(metadata: dict[str, Any]) -> list[JoinKeyPair]:
    """Build join key pairs from propagated parser metadata only."""
    keys: list[JoinKeyPair] = []

    for source in (metadata.get("join_keys"), metadata.get("keys")):
        if not source:
            continue
        for pair in source:
            if not isinstance(pair, dict):
                continue
            left = (
                (pair.get("left") or pair.get("stream_field") or "").strip()
            )
            right = (
                (pair.get("right") or pair.get("table_field") or left).strip()
            )
            if left:
                keys.append(JoinKeyPair(left=left, right=right or left))
        if keys:
            return keys

    keys_1 = list(metadata.get("keys_1") or [])
    keys_2 = list(metadata.get("keys_2") or [])
    if keys_1 and keys_2:
        pair_count = max(len(keys_1), len(keys_2))
        for index in range(pair_count):
            left = keys_1[min(index, len(keys_1) - 1)]
            right = keys_2[min(index, len(keys_2) - 1)]
            if left:
                keys.append(JoinKeyPair(left=left, right=right or left))

    return keys


def _join_type_from_metadata(metadata: dict[str, Any]) -> str:
    join_type = (metadata.get("join_type") or "").strip()
    if not join_type:
        join_type = (metadata.get("attributes") or {}).get("join_type", "")
    return (join_type or "INNER").strip()


def _spark_how(join_type: str) -> str:
    normalized = join_type.strip().lower()
    return _JOIN_TYPE_MAP.get(normalized, normalized)


def _df_name_for_step(context: StepContext, step_name: str) -> str:
    safe = step_name.replace(" ", "_").replace("-", "_")
    return context.df_variable_map.get(step_name, f"df_{safe}")


def _resolve_join_streams(
    metadata: dict[str, Any],
    context: StepContext,
    input_dfs: list[str],
) -> tuple[str | None, str | None]:
    if len(input_dfs) < 2:
        return None, None

    step1 = (metadata.get("step1") or "").strip()
    step2 = (metadata.get("step2") or "").strip()
    if step1 and step2:
        return _df_name_for_step(context, step1), _df_name_for_step(context, step2)

    return input_dfs[0], input_dfs[1]


def _stream_columns(context: StepContext, step_name: str) -> list[str]:
    lineage_map = context.extra.get("lineage_map", {})
    columns = lineage_map.get(step_name)
    if isinstance(columns, dict):
        return sorted(columns.keys())
    return []


def _lineage_map_empty(context: StepContext) -> bool:
    return not context.extra.get("lineage_map")


def _has_complete_stream_lineage(left_cols: list[str], right_cols: list[str]) -> bool:
    return bool(left_cols) and bool(right_cols)


def _overlapping_non_key_columns(
    left_cols: list[str],
    right_cols: list[str],
    keys: list[JoinKeyPair],
) -> set[str]:
    left_keys = {k.left for k in keys}
    right_keys = {k.right for k in keys}
    left_names = set(left_cols) - left_keys
    right_names = set(right_cols) - right_keys
    return left_names & right_names


def _needs_explicit_select(
    keys: list[JoinKeyPair],
    overlaps: set[str],
    *,
    use_on_keyword: bool,
) -> bool:
    if overlaps:
        return True
    if use_on_keyword:
        return False
    return any(k.left != k.right for k in keys)


def _select_after_join_lines(
    joined_var: str,
    out_var: str,
    left_var: str,
    right_var: str,
    left_cols: list[str],
    right_cols: list[str],
    keys: list[JoinKeyPair],
    overlaps: set[str],
    *,
    use_on_keyword: bool,
) -> list[str]:
    left_keys = {k.left for k in keys}
    right_keys = {k.right for k in keys}
    same_key_names = bool(keys) and all(k.left == k.right for k in keys)

    select_exprs: list[str] = []
    emitted: set[str] = set()

    for column in left_cols:
        select_exprs.append(f'{left_var}["{column}"]')
        emitted.add(column)

    for column in right_cols:
        if use_on_keyword and same_key_names and column in left_keys:
            continue
        if column in emitted:
            if column in overlaps:
                alias = f"{column}_right"
                select_exprs.append(f'{right_var}["{column}"].alias("{alias}")')
            continue
        if column in right_keys and column in left_cols and column not in overlaps:
            select_exprs.append(f'{right_var}["{column}"]')
        elif column in overlaps:
            select_exprs.append(f'{right_var}["{column}"].alias("{column}_right")')
        else:
            select_exprs.append(f'{right_var}["{column}"]')

    if not select_exprs:
        return [f"{out_var} = {joined_var}"]

    return [f"{out_var} = {joined_var}.select({', '.join(select_exprs)})"]


def _unresolved_lines(step_name: str, out_var: str, message: str) -> list[str]:
    return [
        f"# WARNING: MergeJoin '{step_name}': {message}",
        f"{out_var} = spark.createDataFrame([], '_merge_join_unresolved STRING')",
    ]


def convert_merge_join_step(
    metadata: dict[str, Any],
    input_dfs: list[str],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate PySpark lines for a Merge Join step from propagated metadata."""
    lines = [f"# Merge Join: {step_name}"]

    if len(input_dfs) < 2:
        lines.extend(
            _unresolved_lines(
                step_name,
                out_var,
                f"requires two input streams, found {len(input_dfs)}",
            )
        )
        return lines, "partial"

    left, right = (
        _resolve_join_streams(metadata, context, input_dfs)
        if context is not None
        else (input_dfs[0], input_dfs[1])
    )
    if not left or not right:
        lines.extend(
            _unresolved_lines(step_name, out_var, "could not resolve left/right input streams")
        )
        return lines, "partial"

    keys = _join_keys_from_metadata(metadata)
    how = _spark_how(_join_type_from_metadata(metadata))
    joined_var = f"_joined_{out_var}"

    if keys:
        on_arg, use_on = format_spark_join_on(left, right, keys)
        if use_on:
            lines.append(f"{joined_var} = {left}.join({right}, on={on_arg}, how={how!r})")
        else:
            lines.append(f"{joined_var} = {left}.join({right}, {on_arg}, how={how!r})")
    elif how == "cross":
        lines.append(f"{out_var} = {left}.crossJoin({right})")
        return lines, "converted"
    else:
        lines.extend(
            _unresolved_lines(
                step_name,
                out_var,
                "no join keys in metadata — cannot emit keyless join",
            )
        )
        return lines, "partial"

    left_cols: list[str] = []
    right_cols: list[str] = []
    if context is not None:
        step1 = (metadata.get("step1") or "").strip()
        step2 = (metadata.get("step2") or "").strip()
        preds = context.dag.predecessors(context.step.name)
        left_step = step1 or (preds[0] if preds else "")
        right_step = step2 or (preds[1] if len(preds) > 1 else "")
        left_cols = _stream_columns(context, left_step)
        right_cols = _stream_columns(context, right_step)

    overlaps = _overlapping_non_key_columns(left_cols, right_cols, keys)
    needs_select = _needs_explicit_select(keys, overlaps, use_on_keyword=use_on)
    lineage_empty = context is not None and _lineage_map_empty(context)

    if context is not None and needs_select:
        if _has_complete_stream_lineage(left_cols, right_cols):
            lines.extend(
                _select_after_join_lines(
                    joined_var,
                    out_var,
                    left,
                    right,
                    left_cols,
                    right_cols,
                    keys,
                    overlaps,
                    use_on_keyword=use_on,
                )
            )
        elif left_cols or right_cols:
            lines.extend(
                _unresolved_lines(
                    step_name,
                    out_var,
                    "incomplete column lineage for join streams — cannot disambiguate join output columns",
                )
            )
            return lines, "partial"
        else:
            if lineage_empty:
                lines.append(
                    f"# WARNING: MergeJoin '{step_name}': column lineage unavailable — "
                    "cannot disambiguate join output columns"
                )
            lines.append(f"{out_var} = {joined_var}")
    else:
        if lineage_empty:
            lines.append(
                f"# WARNING: MergeJoin '{step_name}': column lineage unavailable — "
                "join output may contain ambiguous duplicate column names"
            )
        lines.append(f"{out_var} = {joined_var}")

    return lines, "converted"
