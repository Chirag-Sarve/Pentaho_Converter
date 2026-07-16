"""Handlers for Pentaho Mapping (sub-transformation) steps.

Supports:
- Mapping (Sub-transformation)
- Simple Mapping (Sub-transformation)
- Mapping Input Specification
- Mapping Output Specification
"""

from __future__ import annotations

import logging

from ..mapping_converter import (
    convert_mapping_input_step,
    convert_mapping_output_step,
    convert_mapping_step,
)
from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_mapping_config,
    parse_mapping_input_config,
    parse_mapping_output_config,
    parse_simple_mapping_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return (
        (step_type or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "")
        .replace("-", "")
    )


def _meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


def _warn(step_name: str, message: str) -> None:
    logger.warning("Mapping step '%s': %s", step_name, message)


def _info(step_name: str, message: str) -> None:
    logger.info("Mapping step '%s': %s", step_name, message)


def _merge_cfg(meta: dict, cfg: dict) -> dict:
    """Prefer dedicated Mapping XML parser output over any prior metadata merge."""
    for key, val in cfg.items():
        meta[key] = val
    return meta


def _input_df_by_step(context: StepContext) -> dict[str, str]:
    """Map predecessor step names to DataFrame variable names for multi-input Mapping."""
    mapping: dict[str, str] = {}
    for pred in context.dag.predecessors(context.step.name):
        mapping[pred] = context.df_variable_map.get(pred) or f"df_{pred.replace(' ', '_')}"
    # Also expose any known hop sources declared in attributes
    for name, var in context.df_variable_map.items():
        mapping.setdefault(name, var)
    return mapping


class MappingHandler(BaseStepHandler):
    """Mapping (Sub-transformation) → reusable child function invocation."""

    _TYPES = {"mapping", "mappingsubtransformation"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_mapping_config(step_el) if step_el is not None else {}
        meta = _merge_cfg(meta, cfg)
        _info(context.step.name, "converting Mapping (Sub-transformation)")

        lines, status, warnings = convert_mapping_step(
            step_name=context.step.name,
            in_df=in_df,
            out_var=out_var,
            meta=meta,
            label="Mapping (Sub-transformation)",
            simple=False,
            input_df_by_step=_input_df_by_step(context),
        )
        for w in warnings:
            _warn(context.step.name, w)
        return lines, status


class SimpleMappingHandler(BaseStepHandler):
    """Simple Mapping (Sub-transformation) → single-path reusable helper invocation."""

    _TYPES = {
        "simplemapping",
        "simplemappingsubtransformation",
    }

    def can_handle(self, step_type: str) -> bool:
        n = _norm(step_type)
        return n in self._TYPES or n.startswith("simplemapping")

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_simple_mapping_config(step_el) if step_el is not None else {}
        meta = _merge_cfg(meta, cfg)
        _info(context.step.name, "converting Simple Mapping (Sub-transformation)")

        lines, status, warnings = convert_mapping_step(
            step_name=context.step.name,
            in_df=in_df,
            out_var=out_var,
            meta=meta,
            label="Simple Mapping (Sub-transformation)",
            simple=True,
            input_df_by_step=_input_df_by_step(context),
        )
        for w in warnings:
            _warn(context.step.name, w)
        return lines, status


class MappingInputHandler(BaseStepHandler):
    """Mapping Input Specification → input schema validation and field ordering."""

    _TYPES = {"mappinginput", "mappinginputspecification"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_mapping_input_config(step_el) if step_el is not None else {}
        meta = _merge_cfg(meta, cfg)
        _info(context.step.name, "converting Mapping Input Specification")

        lines, status, warnings = convert_mapping_input_step(
            step_name=context.step.name,
            in_df=in_df,
            out_var=out_var,
            meta=meta,
        )
        for w in warnings:
            _warn(context.step.name, w)
        return lines, status


class MappingOutputHandler(BaseStepHandler):
    """Mapping Output Specification → DataFrame projection and output publish."""

    _TYPES = {"mappingoutput", "mappingoutputspecification"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_mapping_output_config(step_el) if step_el is not None else {}
        meta = _merge_cfg(meta, cfg)
        _info(context.step.name, "converting Mapping Output Specification")

        lines, status, warnings = convert_mapping_output_step(
            step_name=context.step.name,
            in_df=in_df,
            out_var=out_var,
            meta=meta,
        )
        for w in warnings:
            _warn(context.step.name, w)
        return lines, status


MAPPING_HANDLERS: list[BaseStepHandler] = [
    MappingHandler(),
    SimpleMappingHandler(),
    MappingInputHandler(),
    MappingOutputHandler(),
]
