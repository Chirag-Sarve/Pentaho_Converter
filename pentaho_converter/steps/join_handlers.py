"""Handlers for Pentaho Join category steps."""

from __future__ import annotations

import logging

from ..join_steps_converter import (
    convert_join_rows_step,
    convert_merge_rows_step,
    convert_multiway_merge_join_step,
    convert_sorted_merge_step,
    convert_xml_join_step,
)
from ..metadata_propagation import get_converter_metadata
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return (step_type or "").strip().lower().replace(" ", "")


class JoinRowsHandler(BaseStepHandler):
    """Join Rows (Cartesian Product) → DataFrame.crossJoin()."""

    _TYPES = {"joinrows", "joiner"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        logger.info("Converting JoinRows step '%s'", context.step.name)
        return convert_join_rows_step(
            metadata,
            context.all_input_df_names(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


class MergeRowsHandler(BaseStepHandler):
    """Merge Rows (Diff) → full_outer join with CDC flags."""

    _TYPES = {"mergerows", "mergerow"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        logger.info("Converting MergeRows step '%s'", context.step.name)
        return convert_merge_rows_step(
            metadata,
            context.all_input_df_names(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


class MultiwayMergeJoinHandler(BaseStepHandler):
    """Multiway Merge Join → chained DataFrame joins."""

    _TYPES = {"multimergejoin", "multiwaymergejoin", "multimerge"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        logger.info("Converting MultiwayMergeJoin step '%s'", context.step.name)
        return convert_multiway_merge_join_step(
            metadata,
            context.all_input_df_names(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


class SortedMergeHandler(BaseStepHandler):
    """Sorted Merge → unionByName + orderBy."""

    _TYPES = {"sortedmerge"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        logger.info("Converting SortedMerge step '%s'", context.step.name)
        return convert_sorted_merge_step(
            metadata,
            context.all_input_df_names(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


class XMLJoinHandler(BaseStepHandler):
    """XML Join → fragment aggregation into target XML field."""

    _TYPES = {"xmljoin"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        logger.info("Converting XMLJoin step '%s'", context.step.name)
        return convert_xml_join_step(
            metadata,
            context.all_input_df_names(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


# MergeJoin remains registered from transform_handlers (first match wins).
JOIN_HANDLERS: list[BaseStepHandler] = [
    JoinRowsHandler(),
    MergeRowsHandler(),
    MultiwayMergeJoinHandler(),
    SortedMergeHandler(),
    XMLJoinHandler(),
]
