"""Handlers for Pentaho Server (BA Server) transformations.

Supports:
- Call Endpoint (CallEndpointStep)
- Get Session Variables (GetSessionVariableStep)
- Set Session Variables (SetSessionVariableStep)
"""

from __future__ import annotations

import logging

from ..metadata_propagation import get_converter_metadata
from ..pentaho_server_converter import (
    convert_call_endpoint_step,
    convert_get_session_variables_step,
    convert_set_session_variables_step,
)
from ..step_xml import (
    get_step_element,
    parse_call_endpoint_config,
    parse_get_session_variable_config,
    parse_set_session_variable_config,
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
        .replace("_", "")
        .replace("-", "")
        .replace("/", "")
    )


def _merge_parsed(context: StepContext, parser) -> dict:
    """Prefer dedicated BA Server XML parser output over any prior metadata merge."""
    metadata = dict(get_converter_metadata(context))
    step_el = get_step_element(context.step)
    if step_el is not None:
        metadata.update(parser(step_el))
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
        lines.append(
            "from pyspark.sql.types import StructType"
        )
        lines.append(f"{out_var} = spark.createDataFrame([], StructType([]))")
    return lines, "partial"


class CallEndpointHandler(BaseStepHandler):
    """Call Endpoint → Python requests against BA Server API URL."""

    _TYPES = {"callendpoint", "callendpointstep"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_call_endpoint_config)
            lines, status = convert_call_endpoint_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
                dict(context.transformation.parameters or {}),
            )
            logger.info(
                "CallEndpoint '%s' status=%s method=%s module=%s",
                context.step.name,
                status,
                metadata.get("http_method"),
                metadata.get("module_name"),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Call Endpoint", exc)


class GetSessionVariablesHandler(BaseStepHandler):
    """Get Session Variables → columns from session store / conf / env / widgets."""

    _TYPES = {
        "getsessionvariable",
        "getsessionvariables",
        "getsessionvariablestep",
    }

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_get_session_variable_config)
            lines, status = convert_get_session_variables_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
                dict(context.transformation.parameters or {}),
            )
            logger.info(
                "GetSessionVariables '%s' status=%s vars=%s",
                context.step.name,
                status,
                len(metadata.get("fields") or []),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Get Session Variables", exc)


class SetSessionVariablesHandler(BaseStepHandler):
    """Set Session Variables → session store / spark.conf / env / widgets."""

    _TYPES = {
        "setsessionvariable",
        "setsessionvariables",
        "setsessionvariablestep",
    }

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_set_session_variable_config)
            lines, status = convert_set_session_variables_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
                dict(context.transformation.parameters or {}),
            )
            logger.info(
                "SetSessionVariables '%s' status=%s vars=%s",
                context.step.name,
                status,
                len(metadata.get("fields") or []),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Set Session Variables", exc)


PENTAHO_SERVER_HANDLERS: list[BaseStepHandler] = [
    CallEndpointHandler(),
    GetSessionVariablesHandler(),
    SetSessionVariablesHandler(),
]
