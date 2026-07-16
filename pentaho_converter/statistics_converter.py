"""Convert Pentaho Statistics steps to Databricks-compatible PySpark code."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _safe_ident(name: str) -> str:
    return (name or "col").replace(" ", "_").replace("-", "_")


def _df_var_for_step(step_name: str) -> str:
    return f"df_{step_name.replace(' ', '_').replace('-', '_')}"


def convert_analytic_query_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Analytic Query → Spark Window lead/lag (partition + order preserved)."""
    lines = [f"# Analytic Query: {step_name}"]
    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    group_fields = list(
        metadata.get("group_fields")
        or metadata.get("partition_fields")
        or []
    )
    # Deduplicate partition keys
    seen: set[str] = set()
    partitions: list[str] = []
    for key in group_fields:
        if key and key not in seen:
            seen.add(key)
            partitions.append(key)

    order_fields = list(metadata.get("order_fields") or [])
    analytic_fields = list(
        metadata.get("analytic_fields") or metadata.get("fields") or []
    )

    order_exprs: list[str] = []
    for item in order_fields:
        name = item.get("name") if isinstance(item, dict) else None
        if not name:
            continue
        asc = item.get("ascending", True) if isinstance(item, dict) else True
        order_exprs.append(f'col("{name}").{"asc" if asc else "desc"}()')

    # Per-field order overrides (extension); fall back to stream order
    if not order_exprs:
        for field in analytic_fields:
            of = (field.get("order_field") or "").strip()
            if of:
                asc = field.get("ascending", True)
                order_exprs.append(f'col("{of}").{"asc" if asc else "desc"}()')
                break
    if not order_exprs:
        lines.append(
            "# WARNING: AnalyticQuery uses pre-sorted input in Pentaho; "
            "no order fields configured — ordering by monotonically_increasing_id()"
        )
        order_exprs = ["monotonically_increasing_id()"]

    if partitions:
        part = ", ".join(repr(p) for p in partitions)
        lines.append(
            f"_w_aq_{out_var} = Window.partitionBy({part})"
            f".orderBy({', '.join(order_exprs)})"
        )
    else:
        lines.append(
            f"_w_aq_{out_var} = Window.orderBy({', '.join(order_exprs)})"
        )
    lines.append(
        f"# preserved.partition_fields={partitions!r} order={order_exprs!r}"
    )

    if not analytic_fields:
        lines.append(
            f"# WARNING: AnalyticQuery '{step_name}': no analytic functions configured"
        )
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"

    lines.append(f"{out_var} = {in_df}")
    status = "converted"
    for field in analytic_fields:
        out_name = (field.get("name") or "").strip()
        subject = (field.get("subject") or out_name).strip()
        fun = (field.get("function") or field.get("type") or "LAG").strip().upper()
        try:
            offset = int(field.get("offset") or field.get("valuefield") or 1)
        except (TypeError, ValueError):
            offset = 1
        if not out_name or not subject:
            continue

        w = f"_w_aq_{out_var}"
        if fun in ("LAG",):
            expr = f'lag(col("{subject}"), {offset}).over({w})'
        elif fun in ("LEAD",):
            expr = f'lead(col("{subject}"), {offset}).over({w})'
        elif fun in ("FIRST", "FIRST_VALUE"):
            lines.append(
                f"# window frame: rowsBetween(unboundedPreceding, currentRow) for {fun}"
            )
            frame = (
                f"{w}.rowsBetween(Window.unboundedPreceding, Window.currentRow)"
            )
            expr = f'first(col("{subject}"), ignorenulls=True).over({frame})'
        elif fun in ("LAST", "LAST_VALUE"):
            lines.append(
                f"# window frame: rowsBetween(unboundedPreceding, unboundedFollowing) for {fun}"
            )
            frame = (
                f"{w}.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)"
            )
            expr = f'last(col("{subject}"), ignorenulls=True).over({frame})'
        elif fun in ("RANK",):
            expr = f"rank().over({w})"
        elif fun in ("DENSE_RANK", "DENSERANK"):
            expr = f"dense_rank().over({w})"
        elif fun in ("ROW_NUMBER", "ROWNUMBER"):
            expr = f"row_number().over({w})"
        elif fun in ("SUM", "CUMULATIVE_SUM", "RUNNING_SUM"):
            lines.append(
                f"# cumulative: rowsBetween(unboundedPreceding, currentRow) for {fun}"
            )
            frame = (
                f"{w}.rowsBetween(Window.unboundedPreceding, Window.currentRow)"
            )
            expr = f'_sum(col("{subject}")).over({frame})'
        elif fun in ("COUNT", "CUMULATIVE_COUNT"):
            lines.append(
                f"# cumulative count frame for {fun}"
            )
            frame = (
                f"{w}.rowsBetween(Window.unboundedPreceding, Window.currentRow)"
            )
            expr = f'count(col("{subject}")).over({frame})'
        else:
            lines.append(
                f"# WARNING: unsupported AnalyticQuery function {fun!r} "
                f"for {out_name!r}; emitting lag() fallback"
            )
            logger.warning(
                "AnalyticQuery '%s': unsupported function %r for %r",
                step_name,
                fun,
                out_name,
            )
            expr = f'lag(col("{subject}"), {offset}).over({w})'
            status = "partial"

        lines.append(f'{out_var} = {out_var}.withColumn("{out_name}", {expr})')

    return lines, status


def convert_memory_group_by_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
    fallback_group_keys: list[str] | None = None,
) -> tuple[list[str], str]:
    """Memory Group By → same Spark aggregation as Group By, with distributed notes."""
    from .group_by_converter import convert_group_by_step

    lines = convert_group_by_step(
        metadata, in_df, out_var, step_name, fallback_group_keys=fallback_group_keys
    )
    # Prefix comment: clarify Memory Group By vs Group By execution model
    if lines and lines[0].startswith("# Group By:"):
        lines[0] = f"# Memory Group By: {step_name}"
    else:
        lines.insert(0, f"# Memory Group By: {step_name}")
    lines.insert(
        1,
        "# NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; "
        "Spark uses distributed groupBy().agg() — memory pressure shifts to executors, "
        "and result ordering / early-partial-agg timing may differ.",
    )
    if metadata.get("directory") or metadata.get("prefix"):
        lines.insert(
            2,
            f"# preserved.temp_directory={metadata.get('directory')!r} "
            f"prefix={metadata.get('prefix')!r} "
            "(unused in Spark; spill handled by Spark shuffle)",
        )
    return lines, "converted"


def convert_sample_rows_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Sample Rows → row_number filter on line ranges, or sample()/limit()."""
    lines = [f"# Sample Rows: {step_name}"]
    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    percentage = metadata.get("percentage")
    row_count = metadata.get("row_count")
    seed = metadata.get("seed")
    line_ranges = list(metadata.get("line_ranges") or [])
    line_num_field = (metadata.get("line_num_field") or "").strip()
    lines_range = metadata.get("lines_range") or ""

    # Prefer explicit percentage / fixed row-count extensions over line ranges
    if percentage is not None and percentage > 0:
        seed_arg = f", seed={int(seed)}" if seed is not None else ""
        lines.append(
            f"# preserved.lines_range={lines_range!r} "
            f"(using sample fraction={percentage})"
        )
        logger.info(
            "SampleRows '%s': percentage sample fraction=%s seed=%s",
            step_name,
            percentage,
            seed,
        )
        lines.append(
            f"{out_var} = {in_df}.sample(False, {float(percentage)}{seed_arg})"
        )
        if line_num_field:
            lines.append(
                f"_w_sr_{out_var} = Window.orderBy(monotonically_increasing_id())"
            )
            lines.append(
                f'{out_var} = {out_var}.withColumn("{line_num_field}", '
                f"row_number().over(_w_sr_{out_var}))"
            )
        return lines, "converted"

    if row_count is not None and row_count >= 0:
        lines.append(
            f"# preserved.lines_range={lines_range!r} "
            f"(using fixed row_count={row_count})"
        )
        logger.info(
            "SampleRows '%s': fixed row_count=%s seed=%s",
            step_name,
            row_count,
            seed,
        )
        if seed is not None:
            lines.append(
                f"{out_var} = {in_df}.orderBy(rand({int(seed)})).limit({int(row_count)})"
            )
        else:
            lines.append(f"{out_var} = {in_df}.limit({int(row_count)})")
        if line_num_field:
            lines.append(
                f"_w_sr_{out_var} = Window.orderBy(monotonically_increasing_id())"
            )
            lines.append(
                f'{out_var} = {out_var}.withColumn("{line_num_field}", '
                f"row_number().over(_w_sr_{out_var}))"
            )
        return lines, "converted"

    lines.append(f"_w_sr_{out_var} = Window.orderBy(monotonically_increasing_id())")
    lines.append(
        f"{out_var} = {in_df}.withColumn('_sr_rn', row_number().over(_w_sr_{out_var}))"
    )

    if not line_ranges:
        lines.append(
            f"# WARNING: SampleRows '{step_name}': empty/invalid linesrange "
            f"{lines_range!r}; keeping no rows"
        )
        logger.warning(
            "SampleRows '%s': empty/invalid linesrange %r",
            step_name,
            lines_range,
        )
        lines.append(f"{out_var} = {out_var}.filter(lit(False))")
    else:
        preds: list[str] = []
        for start, end in line_ranges:
            if start == end:
                preds.append(f"(col('_sr_rn') == {start})")
            else:
                preds.append(f"((col('_sr_rn') >= {start}) & (col('_sr_rn') <= {end}))")
        lines.append(
            f"# preserved.lines_range={lines_range!r} ranges={line_ranges!r}"
        )
        lines.append(f"{out_var} = {out_var}.filter({' | '.join(preds)})")

    if line_num_field:
        lines.append(
            f'{out_var} = {out_var}.withColumn("{line_num_field}", col("_sr_rn"))'
        )
    lines.append(f"{out_var} = {out_var}.drop('_sr_rn')")
    return lines, "converted"


def convert_reservoir_sampling_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Reservoir Sampling → seeded random order + limit (distributed approximation)."""
    lines = [f"# Reservoir Sampling: {step_name}"]
    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    try:
        sample_size = int(metadata.get("sample_size") or 100)
    except (TypeError, ValueError):
        sample_size = 100
    try:
        seed = int(metadata.get("seed") or 1)
    except (TypeError, ValueError):
        seed = 1
    replacement = bool(metadata.get("replacement"))

    lines.append(
        f"# preserved.sample_size={sample_size} seed={seed} replacement={replacement}"
    )
    lines.append(
        "# NOTE: Classic Vitter reservoir runs single-threaded in Pentaho; "
        "Spark approximates with seeded rand() + limit (or sample withReplacement). "
        "Exact row sets may differ on large/shuffled distributed datasets."
    )
    if sample_size <= 0:
        lines.append(f"{out_var} = {in_df}.limit(0)")
        return lines, "converted"

    if replacement:
        lines.append(
            "# WARNING: with-replacement reservoir not natively available; "
            "using DataFrame.sample(withReplacement=True, fraction) heuristic"
        )
        lines.append(
            f"_rs_n_{out_var} = {in_df}.count()"
        )
        lines.append(
            f"_rs_frac_{out_var} = (float({sample_size}) / float(_rs_n_{out_var})) "
            f"if _rs_n_{out_var} else 0.0"
        )
        lines.append(
            f"{out_var} = {in_df}.sample(True, min(1.0, _rs_frac_{out_var}), {seed})"
            f".limit({sample_size})"
        )
        return lines, "partial"

    # Prefer exact-size reproducible sample via rand + limit
    lines.append(
        f"{out_var} = {in_df}.orderBy(rand({seed})).limit({sample_size})"
    )
    return lines, "converted"


def convert_univariate_stats_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Univariate Statistics → single-row agg of count/min/max/mean/stddev/percentiles."""
    lines = [f"# Univariate Statistics: {step_name}"]
    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"

    stats = list(metadata.get("stats") or metadata.get("fields") or [])
    if not stats:
        lines.append(
            f"# WARNING: UnivariateStats '{step_name}': no fields configured"
        )
        lines.append(f"{out_var} = {in_df}.limit(0)")
        return lines, "partial"

    agg_exprs: list[str] = []
    quantile_jobs: list[tuple[str, float, str]] = []  # (source, p, out_name)

    for item in stats:
        source = (item.get("source_field") or item.get("name") or "").strip()
        if not source:
            continue
        if item.get("calc_n", True):
            agg_exprs.append(
                f'count(col("{source}")).alias("{source}(N)")'
            )
        if item.get("calc_mean", True):
            agg_exprs.append(
                f'avg(col("{source}")).alias("{source}(mean)")'
            )
        if item.get("calc_std_dev", True):
            agg_exprs.append(
                f'stddev_samp(col("{source}")).alias("{source}(stdDev)")'
            )
            # Spark enrichment: variance (Pentaho UnivariateStats has no variance column)
            calc_var = item.get("calc_variance")
            if calc_var is None or calc_var:
                agg_exprs.append(
                    f'variance_samp(col("{source}")).alias("{source}(variance)")'
                )
                lines.append(
                    f"# Spark enrichment: {source}(variance) via variance_samp "
                    "(not emitted by native Pentaho UnivariateStats)"
                )
        if item.get("calc_min", True):
            agg_exprs.append(
                f'_min(col("{source}")).alias("{source}(min)")'
            )
        if item.get("calc_max", True):
            agg_exprs.append(
                f'_max(col("{source}")).alias("{source}(max)")'
            )
        if item.get("calc_median", True):
            agg_exprs.append(
                f'expr("percentile_approx(`{source}`, 0.5)").alias("{source}(median)")'
            )
        # Explicit variance without stddev (custom / extension attribute)
        if item.get("calc_variance") and not item.get("calc_std_dev", True):
            agg_exprs.append(
                f'variance_samp(col("{source}")).alias("{source}(variance)")'
            )
        try:
            percentile = float(item.get("percentile", -1))
        except (TypeError, ValueError):
            percentile = -1.0
        if percentile >= 0:
            # Pentaho stores 0..1 (or percent/100)
            p = percentile if percentile <= 1.0 else percentile / 100.0
            pct_label = f"{p * 100:.2f}".rstrip("0").rstrip(".")
            out_col = f"{source}({pct_label}th percentile)"
            interpolate = bool(item.get("interpolate", True))
            if interpolate:
                agg_exprs.append(
                    f'expr("percentile_approx(`{source}`, {p})").alias("{out_col}")'
                )
            else:
                # Discrete percentile still uses approxQuantile path documented below
                quantile_jobs.append((source, p, out_col))
                lines.append(
                    f"# preserved.interpolate=N for {source!r} p={p}; "
                    "using approxQuantile (Spark has no exact discrete percentile agg)"
                )

    status = "converted"
    if not agg_exprs and not quantile_jobs:
        lines.append(
            f"# WARNING: UnivariateStats '{step_name}': no metrics enabled"
        )
        lines.append(f"{out_var} = {in_df}.limit(0)")
        return lines, "partial"

    lines.append(
        "# null handling: Spark aggregate functions skip nulls "
        "(matches Pentaho numeric stats)"
    )
    lines.append(
        f"# empty DataFrame → agg still returns one row with null/0 counts"
    )

    if agg_exprs:
        lines.append(
            f"{out_var} = {in_df}.agg({', '.join(agg_exprs)})"
        )
    else:
        lines.append(f"{out_var} = spark.range(1).select(lit(1).alias('_u'))")

    for source, p, out_col in quantile_jobs:
        tmp = f"_uq_{_safe_ident(out_var)}_{_safe_ident(source)}"
        lines.append(
            f"{tmp} = {in_df}.approxQuantile({source!r}, [{p}], 0.001)"
        )
        lines.append(
            f"{out_var} = {out_var}.withColumn({out_col!r}, "
            f"lit({tmp}[0] if {tmp} else None))"
        )
        status = "partial"

    return lines, status


def convert_steps_metrics_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
    df_variable_map: dict[str, str] | None = None,
) -> tuple[list[str], str]:
    """Output Steps Metrics → collect available row counts; preserve runtime metrics."""
    lines = [f"# Output Steps Metrics: {step_name}"]
    metric_steps = list(metadata.get("metric_steps") or [])
    name_f = metadata.get("step_name_field") or "Stepname"
    id_f = metadata.get("step_id_field") or "Stepid"
    in_f = metadata.get("lines_input_field") or "Linesinput"
    out_f = metadata.get("lines_output_field") or "Linesoutput"
    read_f = metadata.get("lines_read_field") or "Linesread"
    upd_f = metadata.get("lines_updated_field") or "Linesupdated"
    written_f = metadata.get("lines_written_field") or "Lineswritten"
    err_f = metadata.get("lines_errors_field") or "Lineserrors"
    sec_f = metadata.get("seconds_field") or "Seconds"

    lines.append(
        "# WARNING: Pentaho Steps Metrics reads engine runtime counters "
        "(lines read/written/errors/seconds). Spark cannot observe remote step "
        "counters the same way — emitting count() where a DataFrame exists and "
        "preserving remaining metrics as null/metadata."
    )
    lines.append(
        f"# preserved.fields name={name_f!r} id={id_f!r} input={in_f!r} "
        f"output={out_f!r} read={read_f!r} updated={upd_f!r} "
        f"written={written_f!r} errors={err_f!r} seconds={sec_f!r}"
    )

    df_map = df_variable_map or {}
    if not metric_steps:
        lines.append(
            f"# WARNING: StepsMetrics '{step_name}': no steps configured"
        )
        lines.append(
            f"{out_var} = spark.createDataFrame([], "
            f"'{name_f} STRING, {id_f} STRING, {in_f} BIGINT, {out_f} BIGINT, "
            f"{read_f} BIGINT, {upd_f} BIGINT, {written_f} BIGINT, "
            f"{err_f} BIGINT, {sec_f} BIGINT')"
        )
        return lines, "partial"

    parts: list[str] = []
    for idx, item in enumerate(metric_steps):
        watched = item.get("name") or ""
        copy_nr = item.get("copy_nr") or "0"
        required = bool(item.get("required"))
        df_name = df_map.get(watched) or _df_var_for_step(watched)
        part = f"_sm_{out_var}_{idx}"
        parts.append(part)
        lines.append(
            f"# metric step={watched!r} copyNr={copy_nr!r} required={required} "
            f"source_df={df_name}"
        )
        lines.append(
            f"# NOTE: if {df_name} is not in scope, set _sm_cnt_{idx} = None manually"
        )
        lines.append(f"_sm_cnt_{idx} = {df_name}.count()")
        lines.append(
            f"{part} = spark.createDataFrame([{{"
            f"{name_f!r}: {watched!r}, "
            f"{id_f!r}: {copy_nr!r}, "
            f"{in_f!r}: _sm_cnt_{idx}, "
            f"{out_f!r}: _sm_cnt_{idx}, "
            f"{read_f!r}: _sm_cnt_{idx}, "
            f"{upd_f!r}: None, "
            f"{written_f!r}: _sm_cnt_{idx}, "
            f"{err_f!r}: None, "
            f"{sec_f!r}: None"
            f"}}])"
        )
        lines.append(
            f"# unsupported runtime metrics left null: updated/errors/seconds "
            f"for step {watched!r}"
        )

    lines.append(f"{out_var} = {parts[0]}")
    for part in parts[1:]:
        lines.append(
            f"{out_var} = {out_var}.unionByName({part}, allowMissingColumns=True)"
        )

    if in_df:
        lines.append(
            f"# upstream hop DataFrame {in_df} ignored — Steps Metrics is a "
            "metrics source, not a stream transform"
        )
    return lines, "partial"
