"""Handlers for Pentaho Experimental transformation steps.

Supports:
- SFTP Put (upload local/stream files to a remote host via SFTP)
- Script (javax.script multi-language: JS / Python / Ruby / Groovy)
"""

from __future__ import annotations

import logging

from ..metadata_propagation import get_converter_metadata
from ..scripting_converter import convert_experimental_script_step
from ..sftp_put_converter import convert_sftp_put_step
from ..step_xml import (
    get_step_element,
    parse_experimental_script_config,
    parse_sftp_put_config,
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
    )


def _merge_parsed(context: StepContext, parser) -> dict:
    metadata = dict(get_converter_metadata(context))
    step_el = get_step_element(context.step)
    if step_el is not None:
        parsed = parser(step_el)
        for key, val in parsed.items():
            if key not in metadata or metadata[key] in (None, "", [], {}):
                metadata[key] = val
            elif key == "extras" and isinstance(val, dict):
                existing = metadata.get("extras") if isinstance(metadata.get("extras"), dict) else {}
                metadata["extras"] = {**val, **existing}
            elif isinstance(val, list) and not metadata.get(key):
                metadata[key] = val
    return metadata


def _passthrough_error(context: StepContext, label: str, exc: Exception) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [
        f"# {label}: {context.step.name}",
        f"# ERROR: {exc}",
        f"# WARNING: {label} conversion failed; continuing pipeline.",
    ]
    logger.exception("%s '%s' failed: %s", label, context.step.name, exc)
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append("from pyspark.sql.types import StructType")
        lines.append(f"{out_var} = spark.createDataFrame([], StructType([]))")
    return lines, "partial"


class SFTPPutHandler(BaseStepHandler):
    """Experimental SFTP Put → Paramiko driver-side upload with Databricks Secrets."""

    _TYPES = {"sftpput", "sftpputfile", "putafilewithsftp", "putsftp"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_sftp_put_config)
            lines, status = convert_sftp_put_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
                parameters=context.transformation.parameters or {},
            )
            logger.info(
                "SFTPPut '%s' status=%s host=%s local_field=%s",
                context.step.name,
                status,
                metadata.get("host"),
                metadata.get("local_filename_field"),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "SFTP Put", exc)


class ExperimentalScriptHandler(BaseStepHandler):
    """Experimental Script → language-aware preserve / JS approximate / Python sketch."""

    # Exact "script" type only — do not collide with ScriptValueMod / Execute SQL Script.
    _TYPES = {"script", "scriptvalues", "experimentalscript"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_experimental_script_config)
            lines, status = convert_experimental_script_step(
                metadata,
                context.input_df_name() or "",
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "ExperimentalScript '%s' status=%s language=%s fields=%s",
                context.step.name,
                status,
                metadata.get("script_language"),
                len(metadata.get("fields") or []),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Experimental Script", exc)


EXPERIMENTAL_HANDLERS: list[BaseStepHandler] = [
    SFTPPutHandler(),
    ExperimentalScriptHandler(),
]
