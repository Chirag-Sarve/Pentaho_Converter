"""Bridge legacy handlers through the semantic validation pipeline."""

from __future__ import annotations

from typing import Any

from ..conversion_outcome import StepConversionOutcome, derive_status
from ..step_context import StepContext
from ..steps.base import BaseStepHandler
from ..validation.registry import get_validator
from ..validation.step_validators import GenericStepValidator, parse_step_config, register_builtin_validators

_BRIDGE_VALIDATORS_READY = False


def _ensure_validators() -> None:
    global _BRIDGE_VALIDATORS_READY
    if not _BRIDGE_VALIDATORS_READY:
        register_builtin_validators()
        _BRIDGE_VALIDATORS_READY = True


class LegacyHandlerBridge:
    """Wraps an existing BaseStepHandler with parse/generate/validate lifecycle."""

    def __init__(self, handler: BaseStepHandler, dedicated: bool = False) -> None:
        self._handler = handler
        self._dedicated = dedicated
        self.converter_name = handler.__class__.__name__

    def handles(self, step_type: str) -> bool:
        return self._handler.can_handle(step_type)

    def can_handle(self, step_type: str) -> bool:
        return self._handler.can_handle(step_type)

    def parse_xml(self, context: StepContext) -> dict[str, Any]:
        parsed = parse_step_config(context)
        if hasattr(self._handler, "parse"):
            parsed.update(self._handler.parse(context))
        return parsed

    def generate(self, context: StepContext, parsed: dict[str, Any]) -> list[str]:
        lines, _ = self._handler.generate_code(context)
        return lines

    def convert(self, context: StepContext) -> StepConversionOutcome:
        _ensure_validators()
        parsed = self.parse_xml(context)
        code_lines = self.generate(context, parsed)
        validation = self.validate_semantics(context, parsed, code_lines)
        status = derive_status(
            validation.score,
            validation.errors,
            validation.warnings,
            has_dedicated_handler=self._dedicated,
            step_type=context.step.step_type,
        )
        return StepConversionOutcome(
            code_lines=code_lines,
            status=status,
            semantic_score=validation.score,
            detail=context.step.name,
            warnings=validation.warnings,
            errors=validation.errors,
            properties_converted=validation.properties_converted,
            properties_missing=validation.properties_missing,
            output_columns=validation.output_columns,
            syntax_valid=validation.syntax_valid,
            handler_name=context.step.step_type,
        )

    def validate_semantics(self, context, parsed, code_lines):
        validator = get_validator(context.step.step_type)
        if validator is not None and not isinstance(validator, GenericStepValidator):
            return validator.validate(context, parsed, code_lines)
        return GenericStepValidator().validate(context, parsed, code_lines)
