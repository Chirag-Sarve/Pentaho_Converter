"""Convert Pentaho Join category steps to Databricks-compatible PySpark."""

from __future__ import annotations

import logging
from typing import Any

from .filter_converter import convert_filter_condition_from_metadata
from .merge_join_converter import _JOIN_TYPE_MAP, _join_keys_from_metadata
from .step_context import StepContext
from .step_xml import JoinKeyPair, format_spark_join_on

logger = logging.getLogger(__name__)


def _df_name_for_step(context: StepContext, step_name: str) -> str:
    safe = step_name.replace(" ", "_").replace("-", "_")
    return context.df_variable_map.get(step_name, f"df_{safe}")


def _spark_how(join_type: str) -> str:
    normalized = (join_type or "INNER").strip().lower()
    return _JOIN_TYPE_MAP.get(normalized, normalized if normalized else "inner")


def _preserve_comment(lines: list[str], key: str, value: Any) -> None:
    if value in (None, "", [], {}, False):
        return
    lines.append(f"# preserved.{key}={value!r}")


def _unresolved(step_label: str, step_name: str, out_var: str, message: str) -> list[str]:
    return [
        f"# WARNING: {step_label} '{step_name}': {message}",
        f"{out_var} = spark.createDataFrame([], '_join_unresolved STRING')",
    ]


def _stream_columns(context: StepContext | None, step_name: str) -> list[str]:
    if context is None or not step_name:
        return []
    lineage_map = context.extra.get("lineage_map", {})
    columns = lineage_map.get(step_name)
    if isinstance(columns, dict):
        return sorted(columns.keys())
    return []


def _emit_join_edge_case_warnings(
    lines: list[str],
    *,
    label: str,
    step_name: str,
    keys: list[JoinKeyPair] | None = None,
    left_cols: list[str] | None = None,
    right_cols: list[str] | None = None,
) -> None:
    """Document common join edge cases when exact Spark/PDI parity is limited."""
    if keys:
        lines.append(
            f"# WARNING: {label} '{step_name}': null join keys do not match under "
            "Spark equality; duplicate keys expand to a product within the key group"
        )
        left_names = {k.left for k in keys}
        right_names = {k.right or k.left for k in keys}
        if left_cols and right_cols:
            missing_left = sorted(left_names - set(left_cols))
            missing_right = sorted(right_names - set(right_cols))
            if missing_left or missing_right:
                lines.append(
                    f"# WARNING: {label} '{step_name}': join key(s) missing from "
                    f"stream lineage left={missing_left} right={missing_right} "
                    "(possible schema mismatch / rename)"
                )
            left_non_keys = set(left_cols) - left_names
            right_non_keys = set(right_cols) - right_names
            overlap = sorted(left_non_keys & right_non_keys)
            if overlap:
                lines.append(
                    f"# WARNING: {label} '{step_name}': duplicate non-key column "
                    f"names across streams: {overlap} — rename or select explicitly"
                )
    if left_cols == [] or right_cols == []:
        # Explicit empty lineage lists mean schema known-empty; skip noise when unknown (None).
        pass
    if left_cols is not None and right_cols is not None and not left_cols and not right_cols:
        lines.append(
            f"# NOTE: {label} '{step_name}': empty stream schemas in lineage — "
            "join/union with empty inputs yields empty output"
        )


def convert_join_rows_step(
    metadata: dict[str, Any],
    input_dfs: list[str],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate Cartesian product via DataFrame.crossJoin() for Join Rows."""
    lines = [f"# Join Rows (Cartesian Product): {step_name}"]
    status = "converted"

    directory = (metadata.get("directory") or "").strip()
    prefix = (metadata.get("prefix") or "").strip()
    cache_size = metadata.get("cache_size", 500)
    main_step = (metadata.get("main_step") or metadata.get("main") or "").strip()
    condition = metadata.get("condition") or metadata.get("filter_condition")

    _preserve_comment(lines, "directory", directory)
    _preserve_comment(lines, "prefix", prefix)
    _preserve_comment(lines, "cache_size", cache_size)
    _preserve_comment(lines, "main_step", main_step)

    if directory or prefix or cache_size not in (None, "", 0):
        lines.append(
            "# NOTE: Join Rows temp-directory / prefix / cache_size are PDI spill "
            "options — Spark executes in-memory/distributed (no direct equivalent)."
        )

    if len(input_dfs) < 2:
        if len(input_dfs) == 1:
            lines.append(
                f"# WARNING: JoinRows '{step_name}': only one input stream — "
                "Cartesian product requires 2+ streams"
            )
            lines.append(f"{out_var} = {input_dfs[0]}")
            return lines, "partial"
        lines.extend(
            _unresolved("JoinRows", step_name, out_var, "no input streams for Cartesian join")
        )
        return lines, "partial"

    ordered = list(input_dfs)
    if main_step and context is not None:
        main_df = _df_name_for_step(context, main_step)
        if main_df in ordered:
            ordered = [main_df] + [df for df in ordered if df != main_df]
        else:
            lines.append(
                f"# WARNING: JoinRows '{step_name}': main step {main_step!r} "
                "not found among input streams — using hop order"
            )
            status = "partial"

    left_stream, right_streams = ordered[0], ordered[1:]
    _preserve_comment(lines, "left_stream", left_stream)
    _preserve_comment(lines, "right_streams", right_streams)
    lines.append(
        f"# WARNING: JoinRows '{step_name}': Cartesian product (crossJoin) can be "
        "very expensive on large datasets; empty input streams yield empty output; "
        "duplicate column names across streams are preserved as-is"
    )
    logger.warning("JoinRows '%s': emitting crossJoin (Cartesian product)", step_name)

    current = ordered[0]
    for index, right in enumerate(ordered[1:], start=1):
        tmp = f"_cross_{out_var}_{index}" if index < len(ordered) - 1 or condition else out_var
        if index < len(ordered) - 1 or condition:
            lines.append(f"{tmp} = {current}.crossJoin({right})")
            current = tmp
        else:
            lines.append(f"{out_var} = {current}.crossJoin({right})")
            current = out_var

    if condition:
        result = convert_filter_condition_from_metadata(condition)
        expr = result.expr if result.expr and result.expr != "lit(False)" else ""
        for warning in result.warnings:
            lines.append(f"# WARNING: JoinRows '{step_name}': {warning}")
            status = "partial"
        if expr:
            lines.append(f"{out_var} = {current}.filter({expr})")
        else:
            lines.append(
                f"# WARNING: JoinRows '{step_name}': condition present but could not "
                "be converted — preserving unfiltered Cartesian product"
            )
            if current != out_var:
                lines.append(f"{out_var} = {current}")
            status = "partial"
    elif current != out_var:
        lines.append(f"{out_var} = {current}")

    return lines, status


def convert_merge_rows_step(
    metadata: dict[str, Any],
    input_dfs: list[str],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate Merge Rows (diff) comparison with insert/update/delete flags."""
    lines = [f"# Merge Rows (Diff): {step_name}"]
    status = "converted"

    flag_field = (metadata.get("flag_field") or "flagfield").strip() or "flagfield"
    reference = (metadata.get("reference") or "").strip()
    compare = (metadata.get("compare") or "").strip()
    value_fields = [
        str(v).strip()
        for v in (metadata.get("value_fields") or [])
        if str(v).strip()
    ]
    key_fields = [
        str(k).strip()
        for k in (metadata.get("key_fields") or [])
        if str(k).strip()
    ]

    keys = _join_keys_from_metadata(metadata)
    if not keys and key_fields:
        keys = [JoinKeyPair(left=k, right=k) for k in key_fields]

    _preserve_comment(lines, "flag_field", flag_field)
    _preserve_comment(lines, "reference", reference)
    _preserve_comment(lines, "compare", compare)
    _preserve_comment(lines, "key_fields", key_fields)
    _preserve_comment(lines, "value_fields", value_fields)

    if len(input_dfs) < 2:
        if len(input_dfs) == 1:
            lines.append(f"{out_var} = {input_dfs[0]}")
            lines.append(
                f"{out_var} = {out_var}.withColumn({flag_field!r}, lit('identical'))"
            )
            return lines, "partial"
        lines.extend(_unresolved("MergeRows", step_name, out_var, "requires two input streams"))
        return lines, "partial"

    ref_df, cmp_df = input_dfs[0], input_dfs[1]
    if context is not None and reference and compare:
        ref_df = _df_name_for_step(context, reference)
        cmp_df = _df_name_for_step(context, compare)

    ref_a, cmp_a = f"_ref_{out_var}", f"_cmp_{out_var}"
    lines.append(f'{ref_a} = {ref_df}.alias("r")')
    lines.append(f'{cmp_a} = {cmp_df}.alias("c")')

    left_cols: list[str] = []
    right_cols: list[str] = []
    if context is not None:
        ref_step = reference or (
            context.dag.predecessors(context.step.name)[0]
            if context.dag.predecessors(context.step.name)
            else ""
        )
        cmp_step = compare or (
            context.dag.predecessors(context.step.name)[1]
            if len(context.dag.predecessors(context.step.name)) > 1
            else ""
        )
        left_cols = _stream_columns(context, ref_step)
        right_cols = _stream_columns(context, cmp_step)
        if left_cols and right_cols and set(left_cols) != set(right_cols):
            lines.append(
                f"# WARNING: MergeRows '{step_name}': reference/compare schemas differ "
                f"(ref={sorted(set(left_cols) - set(right_cols))} only, "
                f"cmp={sorted(set(right_cols) - set(left_cols))} only) — "
                "unionByName/select coerces missing columns to null"
            )
            status = "partial"

    if not keys:
        lines.append(
            f"# WARNING: MergeRows '{step_name}': no key fields — falling back to "
            "unionByName (cannot classify new/changed/deleted)"
        )
        lines.append(
            f"{out_var} = {ref_df}.unionByName({cmp_df}, allowMissingColumns=True)"
        )
        lines.append(
            f"{out_var} = {out_var}.withColumn({flag_field!r}, lit('identical'))"
        )
        return lines, "partial"

    _emit_join_edge_case_warnings(
        lines,
        label="MergeRows",
        step_name=step_name,
        keys=keys,
        left_cols=left_cols or None,
        right_cols=right_cols or None,
    )
    on_arg, use_on = format_spark_join_on(ref_a, cmp_a, keys)
    # Aliased frames: prefer Column expressions so r./c. scopes stay unambiguous.
    if use_on and all(k.left == k.right for k in keys):
        join_cond = " & ".join(
            f'(col("r.{k.left}") == col("c.{k.right}"))' for k in keys
        )
        lines.append(f"{out_var} = {ref_a}.join({cmp_a}, {join_cond}, 'full_outer')")
    elif use_on:
        lines.append(f"{out_var} = {ref_a}.join({cmp_a}, on={on_arg}, how='full_outer')")
    else:
        lines.append(f"{out_var} = {ref_a}.join({cmp_a}, {on_arg}, how='full_outer')")

    key0_ref = keys[0].left
    key0_cmp = keys[0].right or keys[0].left

    if value_fields and left_cols and right_cols:
        missing_vals = [
            f for f in value_fields if f not in left_cols or f not in right_cols
        ]
        if missing_vals:
            lines.append(
                f"# WARNING: MergeRows '{step_name}': compare field(s) missing from "
                f"one stream lineage: {missing_vals}"
            )
            status = "partial"

    if value_fields:
        # Null-safe inequality: NULL vs value is changed; NULL vs NULL is identical.
        changed_parts = [
            f'(~col("r.{field}").eqNullSafe(col("c.{field}")))'
            for field in value_fields
        ]
        changed_expr = " | ".join(changed_parts)
        flag_expr = (
            f'when(col("c.{key0_cmp}").isNull(), lit("deleted"))'
            f'.when(col("r.{key0_ref}").isNull(), lit("new"))'
            f".when({changed_expr}, lit(\"changed\"))"
            f'.otherwise(lit("identical"))'
        )
    else:
        lines.append(
            f"# WARNING: MergeRows '{step_name}': no compare/value fields — "
            "cannot emit 'changed'; only new/deleted/identical"
        )
        status = "partial"
        flag_expr = (
            f'when(col("c.{key0_cmp}").isNull(), lit("deleted"))'
            f'.when(col("r.{key0_ref}").isNull(), lit("new"))'
            f'.otherwise(lit("identical"))'
        )

    lines.append(f"{out_var} = {out_var}.withColumn({flag_field!r}, {flag_expr})")

    # Prefer compare-side values for new/changed rows; reference for deleted/identical.
    select_parts: list[str] = []
    emitted: set[str] = set()
    for key in keys:
        left = key.left
        right = key.right or key.left
        alias = left
        if alias in emitted:
            continue
        select_parts.append(
            f'coalesce(col("c.{right}"), col("r.{left}")).alias({alias!r})'
        )
        emitted.add(alias)
    for field in value_fields:
        if field in emitted:
            continue
        select_parts.append(
            f'coalesce(col("c.{field}"), col("r.{field}")).alias({field!r})'
        )
        emitted.add(field)
    if select_parts:
        select_parts.append(f'col({flag_field!r})')
        lines.append(
            f"# NOTE: MergeRows '{step_name}': output prefers compare values "
            "(CDC-style); deleted rows keep reference values"
        )
        lines.append(f"{out_var} = {out_var}.select({', '.join(select_parts)})")
    else:
        lines.append(
            f"# WARNING: MergeRows '{step_name}': duplicate column names may remain "
            "after full_outer join — add value/key field metadata for a clean select"
        )
        status = "partial"

    lines.append(
        f"# NOTE: MergeRows flags — deleted / new / changed / identical "
        f"(requires pre-sorted inputs in PDI; Spark join does not enforce sort order)"
    )
    return lines, status


def _split_multiway_keys(raw: str) -> list[str]:
    return [part.strip() for part in (raw or "").split(",") if part.strip()]


def convert_multiway_merge_join_step(
    metadata: dict[str, Any],
    input_dfs: list[str],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate chained DataFrame joins for Multiway Merge Join."""
    lines = [f"# Multiway Merge Join: {step_name}"]
    status = "converted"

    join_type = (metadata.get("join_type") or "INNER").strip()
    how = _spark_how(join_type)
    input_steps = [
        str(s).strip() for s in (metadata.get("input_steps") or []) if str(s).strip()
    ]
    key_fields = [
        str(k) for k in (metadata.get("key_fields") or []) if str(k).strip()
    ]

    _preserve_comment(lines, "join_type", join_type)
    _preserve_comment(lines, "input_steps", input_steps)
    _preserve_comment(lines, "key_fields", key_fields)
    _preserve_comment(lines, "number_input", metadata.get("number_input"))
    lines.append(
        "# NOTE: PDI Multiway Merge Join requires pre-sorted streams — "
        "Spark chained join() does not enforce sort order"
    )

    stream_dfs: list[str] = []
    if input_steps and context is not None:
        stream_dfs = [_df_name_for_step(context, name) for name in input_steps]
    if len(stream_dfs) < 2:
        stream_dfs = list(input_dfs)

    if len(stream_dfs) < 2:
        lines.extend(
            _unresolved(
                "MultiwayMergeJoin",
                step_name,
                out_var,
                f"requires 2+ input streams, found {len(stream_dfs)}",
            )
        )
        return lines, "partial"

    if how not in ("inner", "outer", "left", "right", "cross"):
        lines.append(
            f"# WARNING: MultiwayMergeJoin '{step_name}': unsupported join_type "
            f"{join_type!r} — defaulting to inner"
        )
        how = "inner"
        status = "partial"

    _preserve_comment(lines, "merge_order", stream_dfs)
    lines.append(
        f"# WARNING: MultiwayMergeJoin '{step_name}': null join keys do not match; "
        "duplicate keys expand per key group; ensure key data types align across streams"
    )

    current = stream_dfs[0]
    for index, right in enumerate(stream_dfs[1:], start=1):
        left_keys_raw = key_fields[min(index - 1, len(key_fields) - 1)] if key_fields else ""
        right_keys_raw = key_fields[min(index, len(key_fields) - 1)] if key_fields else ""
        left_keys = _split_multiway_keys(left_keys_raw)
        right_keys = _split_multiway_keys(right_keys_raw)
        if not left_keys and right_keys:
            left_keys = list(right_keys)
        if not right_keys and left_keys:
            right_keys = list(left_keys)

        tmp = f"_mw_{out_var}_{index}" if index < len(stream_dfs) - 1 else out_var

        if not left_keys or not right_keys:
            if how == "cross":
                lines.append(f"{tmp} = {current}.crossJoin({right})")
            else:
                lines.append(
                    f"# WARNING: MultiwayMergeJoin '{step_name}': missing keys for "
                    f"stream pair {index - 1}/{index} — cannot emit join"
                )
                lines.extend(
                    _unresolved(
                        "MultiwayMergeJoin",
                        step_name,
                        out_var,
                        "incomplete key_fields for chained join",
                    )
                )
                return lines, "partial"
        else:
            pairs = [
                JoinKeyPair(
                    left=left_keys[min(i, len(left_keys) - 1)],
                    right=right_keys[min(i, len(right_keys) - 1)],
                )
                for i in range(max(len(left_keys), len(right_keys)))
            ]
            on_arg, use_on = format_spark_join_on(current, right, pairs)
            if use_on:
                lines.append(f"{tmp} = {current}.join({right}, on={on_arg}, how={how!r})")
            else:
                lines.append(f"{tmp} = {current}.join({right}, {on_arg}, how={how!r})")
            if any(p.left != p.right for p in pairs):
                lines.append(
                    f"# WARNING: MultiwayMergeJoin '{step_name}': differing key names "
                    "across streams may produce duplicate columns"
                )
                status = "partial"
        current = tmp

    return lines, status


def convert_sorted_merge_step(
    metadata: dict[str, Any],
    input_dfs: list[str],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Merge pre-sorted streams via union + orderBy (Spark equivalent)."""
    del context  # hop order already resolved into input_dfs
    lines = [f"# Sorted Merge: {step_name}"]
    status = "converted"

    sort_fields = list(metadata.get("sort_fields") or [])
    _preserve_comment(lines, "sort_fields", sort_fields)
    _preserve_comment(lines, "merge_order", list(input_dfs))
    lines.append(
        "# NOTE: PDI Sorted Merge zipper-merges pre-sorted inputs; Spark equivalent is "
        "unionByName + orderBy (re-sorts; does not require inputs pre-sorted)"
    )
    lines.append(
        f"# WARNING: SortedMerge '{step_name}': schema mismatches across streams are "
        "tolerated via allowMissingColumns=True (missing columns become null); "
        "empty streams contribute no rows"
    )

    if not input_dfs:
        lines.extend(_unresolved("SortedMerge", step_name, out_var, "no input streams"))
        return lines, "partial"

    if len(input_dfs) == 1:
        current = input_dfs[0]
        lines.append(
            f"# WARNING: SortedMerge '{step_name}': only one input stream — "
            "merge is a no-op before ordering"
        )
        status = "partial"
    else:
        current = input_dfs[0]
        for index, right in enumerate(input_dfs[1:], start=1):
            tmp = f"_sm_{out_var}_{index}" if index < len(input_dfs) - 1 else f"_sm_{out_var}"
            lines.append(
                f"{tmp} = {current}.unionByName({right}, allowMissingColumns=True)"
            )
            current = tmp

    order_parts: list[str] = []
    for item in sort_fields:
        if isinstance(item, dict):
            name = (item.get("name") or "").strip()
            ascending = item.get("ascending", True)
            if not name:
                continue
            order_parts.append(
                f'col({name!r}).{"asc" if ascending else "desc"}()'
            )
        elif isinstance(item, (list, tuple)) and item:
            name = str(item[0]).strip()
            ascending = bool(item[1]) if len(item) > 1 else True
            if name:
                order_parts.append(
                    f'col({name!r}).{"asc" if ascending else "desc"}()'
                )

    if order_parts:
        lines.append(f"{out_var} = {current}.orderBy({', '.join(order_parts)})")
    else:
        lines.append(
            f"# WARNING: SortedMerge '{step_name}': no ordering fields — "
            "emitting unordered union of input streams"
        )
        lines.append(f"{out_var} = {current}")
        status = "partial"

    return lines, status


def convert_xml_join_step(
    metadata: dict[str, Any],
    input_dfs: list[str],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Approximate XML Join by aggregating source fragments into the target row."""
    lines = [f"# XML Join: {step_name}"]
    status = "converted"

    value_field = (
        metadata.get("value_xml_field")
        or metadata.get("valueXMLfield")
        or "result_xml"
    )
    target_step = (metadata.get("target_xml_step") or metadata.get("targetXMLstep") or "").strip()
    target_field = (
        metadata.get("target_xml_field") or metadata.get("targetXMLfield") or ""
    ).strip()
    source_step = (metadata.get("source_xml_step") or metadata.get("sourceXMLstep") or "").strip()
    source_field = (
        metadata.get("source_xml_field") or metadata.get("sourceXMLfield") or ""
    ).strip()
    target_xpath = (metadata.get("target_xpath") or metadata.get("targetXPath") or "").strip()
    join_compare = (
        metadata.get("join_compare_field") or metadata.get("joinCompareField") or ""
    ).strip()
    encoding = (metadata.get("encoding") or "").strip()
    complex_join = bool(metadata.get("complex_join") or metadata.get("complexJoin"))
    omit_header = bool(metadata.get("omit_xml_header") or metadata.get("omitXMLHeader"))
    omit_nulls = bool(metadata.get("omit_null_values") or metadata.get("omitNullValues"))

    for key, value in (
        ("value_xml_field", value_field),
        ("target_xml_step", target_step),
        ("target_xml_field", target_field),
        ("source_xml_step", source_step),
        ("source_xml_field", source_field),
        ("target_xpath", target_xpath),
        ("join_compare_field", join_compare),
        ("encoding", encoding),
        ("complex_join", complex_join),
        ("omit_xml_header", omit_header),
        ("omit_null_values", omit_nulls),
    ):
        _preserve_comment(lines, key, value)

    if len(input_dfs) < 1:
        lines.extend(_unresolved("XMLJoin", step_name, out_var, "no input streams"))
        return lines, "partial"

    target_df = input_dfs[0]
    source_df = input_dfs[1] if len(input_dfs) > 1 else input_dfs[0]
    if context is not None:
        if target_step:
            target_df = _df_name_for_step(context, target_step)
        if source_step:
            source_df = _df_name_for_step(context, source_step)

    if not target_field or not source_field:
        lines.append(
            f"# WARNING: XMLJoin '{step_name}': missing target/source XML field metadata"
        )
        lines.append(f"{out_var} = {target_df}")
        return lines, "partial"

    lines.append(
        "# NOTE: Exact PDI XML DOM/XPath node insertion has no first-class Spark "
        "equivalent — aggregating source fragments and concatenating into the target "
        "XML string (approximate)"
    )
    if complex_join:
        lines.append(
            f"# WARNING: XMLJoin '{step_name}': complexJoin with XPath '?' placeholder "
            "is only partially supported — fragments are grouped by joinCompareField "
            "when present, otherwise concatenated globally"
        )
        status = "partial"
    if omit_header:
        lines.append("# preserved.omit_xml_header=True (apply in post-processing if needed)")
    if omit_nulls:
        lines.append(
            f"# WARNING: XMLJoin '{step_name}': omitNullValues requires DOM-level "
            "filtering — not applied in string aggregation"
        )
        status = "partial"
    if target_xpath:
        _preserve_comment(lines, "root_node_xpath", target_xpath)
        lines.append(
            f"# WARNING: XMLJoin '{step_name}': targetXPath {target_xpath!r} "
            "(root/child insert node) is preserved but Spark string aggregation "
            "does not insert at XPath nodes"
        )
        status = "partial"
    if join_compare:
        _preserve_comment(lines, "grouping_field", join_compare)

    src_tmp = f"_xml_src_{out_var}"
    if omit_nulls:
        lines.append(
            f"{src_tmp} = {source_df}.filter(col({source_field!r}).isNotNull())"
        )
    else:
        lines.append(f"{src_tmp} = {source_df}")

    frag_tmp = f"_xml_frag_{out_var}"
    if complex_join and join_compare:
        lines.append(
            f"{frag_tmp} = {src_tmp}.groupBy(col({join_compare!r})).agg("
            f"concat_ws('', collect_list(col({source_field!r}))).alias('_xml_fragments'))"
        )
        joined = f"_xml_joined_{out_var}"
        lines.append(
            f"{joined} = {target_df}.join({frag_tmp}, on=[{join_compare!r}], how='left')"
        )
        lines.append(
            f"{out_var} = {joined}.withColumn("
            f"{value_field!r}, "
            f"concat(coalesce(col({target_field!r}), lit('')), "
            f"coalesce(col('_xml_fragments'), lit('')))"
            f").drop('_xml_fragments')"
        )
    else:
        lines.append(
            f"{frag_tmp} = {src_tmp}.agg("
            f"concat_ws('', collect_list(col({source_field!r}))).alias('_xml_fragments'))"
        )
        lines.append(
            f"{out_var} = {target_df}.crossJoin({frag_tmp}).withColumn("
            f"{value_field!r}, "
            f"concat(coalesce(col({target_field!r}), lit('')), "
            f"coalesce(col('_xml_fragments'), lit('')))"
            f").drop('_xml_fragments')"
        )
        if complex_join and not join_compare:
            lines.append(
                f"# WARNING: XMLJoin '{step_name}': complexJoin enabled but "
                "joinCompareField missing — used global fragment aggregation"
            )
            status = "partial"

    return lines, status
