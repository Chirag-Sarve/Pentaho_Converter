"""Handlers for Pentaho Statistics transformations."""

from __future__ import annotations

import logging

from ..metadata_propagation import get_converter_metadata
from ..statistics_converter import (
    convert_analytic_query_step,
    convert_memory_group_by_step,
    convert_reservoir_sampling_step,
    convert_sample_rows_step,
    convert_steps_metrics_step,
    convert_univariate_stats_step,
)
from ..step_xml import (
    get_step_element,
    parse_analytic_query_config,
    parse_group_by_config,
    parse_reservoir_sampling_config,
    parse_sample_rows_config,
    parse_steps_metrics_config,
    parse_univariate_stats_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _merge_parsed(context: StepContext, parser) -> dict:
    metadata = dict(get_converter_metadata(context))
    step_el = get_step_element(context.step)
    if step_el is not None:
        parsed = parser(step_el)
        for key, val in parsed.items():
            if key not in metadata or metadata[key] in (None, "", [], {}):
                metadata[key] = val
            elif isinstance(val, list) and not metadata.get(key):
                metadata[key] = val
    return metadata


def _passthrough_error(context: StepContext, label: str, exc: Exception) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [
        f"# {label}: {context.step.name}",
        f"# ERROR: {exc}",
    ]
    logger.exception("%s '%s' failed: %s", label, context.step.name, exc)
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
    return lines, "partial"


class AnalyticQueryHandler(BaseStepHandler):
    """Analytic Query → Spark Window lead/lag / ranking / cumulative."""

    _TYPES = {"analyticquery"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_analytic_query_config)
            lines, status = convert_analytic_query_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "AnalyticQuery '%s': %s function(s) status=%s",
                context.step.name,
                len(metadata.get("analytic_fields") or metadata.get("fields") or []),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Analytic Query", exc)


class MemoryGroupByHandler(BaseStepHandler):
    """Memory Group By → distributed groupBy().agg() (documents heap vs Spark)."""

    _TYPES = {"memorygroupby"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_group_by_config)
            fallback = [
                f.name for f in self._fields(context) if getattr(f, "name", None)
            ]
            lines, status = convert_memory_group_by_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
                fallback_group_keys=fallback,
            )
            logger.info(
                "MemoryGroupBy '%s': keys=%s aggs=%s",
                context.step.name,
                metadata.get("group_keys"),
                len(metadata.get("aggregates") or []),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Memory Group By", exc)


class SampleRowsHandler(BaseStepHandler):
    """Sample Rows → line-range filter / sample() / limit()."""

    _TYPES = {"samplerows"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_sample_rows_config)
            lines, status = convert_sample_rows_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "SampleRows '%s': ranges=%s pct=%s rows=%s",
                context.step.name,
                metadata.get("line_ranges"),
                metadata.get("percentage"),
                metadata.get("row_count"),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Sample Rows", exc)


class ReservoirSamplingHandler(BaseStepHandler):
    """Reservoir Sampling → seeded rand().orderBy + limit approximation."""

    _TYPES = {"reservoirsampling"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_reservoir_sampling_config)
            lines, status = convert_reservoir_sampling_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "ReservoirSampling '%s': size=%s seed=%s replacement=%s",
                context.step.name,
                metadata.get("sample_size"),
                metadata.get("seed"),
                metadata.get("replacement"),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Reservoir Sampling", exc)


class UnivariateStatsHandler(BaseStepHandler):
    """Univariate Statistics → single-row statistical aggregates."""

    _TYPES = {"univariatestats", "univariatestatistics"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_univariate_stats_config)
            lines, status = convert_univariate_stats_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "UnivariateStats '%s': %s field(s) status=%s",
                context.step.name,
                len(metadata.get("stats") or []),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Univariate Statistics", exc)


class StepsMetricsHandler(BaseStepHandler):
    """Output Steps Metrics → collect counts + preserve unsupported runtime metrics."""

    _TYPES = {"stepsmetrics", "outputstepsmetrics"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_steps_metrics_config)
            lines, status = convert_steps_metrics_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
                df_variable_map=context.df_variable_map,
            )
            logger.info(
                "StepsMetrics '%s': watching %s step(s)",
                context.step.name,
                len(metadata.get("metric_steps") or []),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Output Steps Metrics", exc)


STATISTICS_HANDLERS: list[BaseStepHandler] = [
    AnalyticQueryHandler(),
    MemoryGroupByHandler(),
    SampleRowsHandler(),
    ReservoirSamplingHandler(),
    UnivariateStatsHandler(),
    StepsMetricsHandler(),
]
