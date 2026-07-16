"""Factory for wrapping legacy handlers as dedicated BaseStepConverter instances."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..conversion_outcome import (
    STATUS_CONVERTED,
    STATUS_FAILED,
    STATUS_PARTIAL,
    StepConversionOutcome,
    format_display_status,
)
from ..step_context import StepContext
from .base import BaseStepConverter

if TYPE_CHECKING:
    from ..steps.base import BaseStepHandler

_PARTIAL_STATUSES = frozenset({
    "partial",
    "partially_supported",
    "approximated",
    "manual_required",
})

# Handlers may still return "partial" for info-level gaps; when the validator
# confirms executable Spark I/O, keep CONVERTED.
_TEXT_FILE_TYPES = frozenset({
    "textfileinput",
    "oldtextfileinput",
    "textfileoutput",
    "textfileoutputlegacy",
})


def _has_executable_text_file_code(code: str, step_type: str) -> bool:
    st = step_type.strip().lower().replace(" ", "")
    if st in ("textfileinput", "oldtextfileinput"):
        return any(tok in code for tok in (".csv(", ".load(", ".text(", 'format("csv")', "format('csv')"))
    if st in ("textfileoutput", "textfileoutputlegacy"):
        return (
            ('format("csv")' in code or "format('csv')" in code or ".text(" in code)
            and (".save(" in code or ".text(" in code)
        )
    return False


def make_converter(handler: BaseStepHandler, *, name: str | None = None) -> BaseStepConverter:
    """Wrap a BaseStepHandler as a dedicated BaseStepConverter."""
    step_types: frozenset[str] = frozenset(getattr(handler, "_TYPES", set()))
    converter_name = name or handler.__class__.__name__.replace("Handler", "")

    class HandlerStepConverter(BaseStepConverter):
        def handles(self, step_type: str) -> bool:
            return handler.can_handle(step_type)

        def can_handle(self, step_type: str) -> bool:
            return handler.can_handle(step_type)

        def generate(self, context: StepContext, parsed: dict) -> list[str]:
            lines, status = handler.generate_code(context)
            context.extra["_handler_status"] = status
            return lines

        def convert(self, context: StepContext) -> StepConversionOutcome:
            outcome = super().convert(context)
            handler_status = str(context.extra.get("_handler_status") or "").lower()
            step_type = context.step.step_type or ""
            code = "\n".join(outcome.code_lines)
            compact = step_type.strip().lower().replace(" ", "")

            if handler_status not in _PARTIAL_STATUSES:
                outcome.display_status = format_display_status(
                    outcome.status,
                    warnings=outcome.warnings,
                    infos=outcome.infos,
                    errors=outcome.errors,
                )
                return outcome

            gap_tokens = (
                "filename missing",
                "delimiter missing",
                "fixed-width",
                "is_command",
                "run as command",
                "accept_filenames",
                "footer lines",
                "filenameinfield",
            )
            gap_blob = " ".join(outcome.warnings).lower() + "\n" + code.lower()
            has_executable_gap = any(p in ("filename", "delimiter") for p in outcome.properties_missing) or any(
                tok in gap_blob for tok in gap_tokens
            )

            # TextFile*: keep CONVERTED for INFO-only legacy metadata when Spark I/O is complete.
            if (
                compact in _TEXT_FILE_TYPES
                and outcome.status == STATUS_CONVERTED
                and not outcome.errors
                and not has_executable_gap
                and _has_executable_text_file_code(code, step_type)
            ):
                outcome.display_status = format_display_status(
                    outcome.status,
                    warnings=outcome.warnings,
                    infos=outcome.infos,
                    errors=outcome.errors,
                )
                return outcome

            if outcome.status == STATUS_CONVERTED:
                # Downgrade when handler marked partial and executable behavior is incomplete.
                if has_executable_gap:
                    outcome.status = STATUS_PARTIAL
                    if outcome.semantic_score >= 0.95:
                        outcome.semantic_score = 0.85
                outcome.display_status = format_display_status(
                    outcome.status,
                    warnings=outcome.warnings,
                    infos=outcome.infos,
                    errors=outcome.errors,
                )
                return outcome
            if outcome.status == STATUS_FAILED and outcome.code_lines:
                if "WARNING" in code or "TODO" in code or ".csv(" in code or ".save(" in code:
                    outcome.status = STATUS_PARTIAL
                    outcome.semantic_score = max(outcome.semantic_score, 0.6)
                    if outcome.semantic_score >= 0.95:
                        outcome.semantic_score = 0.85
            outcome.display_status = format_display_status(
                outcome.status,
                warnings=outcome.warnings,
                infos=outcome.infos,
                errors=outcome.errors,
            )
            return outcome

    HandlerStepConverter.step_types = step_types
    HandlerStepConverter.converter_name = converter_name
    return HandlerStepConverter()
