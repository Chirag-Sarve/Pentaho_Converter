"""Base class for dedicated Pentaho step converters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from ..conversion_outcome import StepConversionOutcome, derive_status
from ..step_context import StepContext
from ..validation.base import SemanticValidationResult
from ..validation.registry import get_validator
from ..validation.step_validators import parse_step_config, register_builtin_validators

_VALIDATORS_READY = False


def _ensure_validators() -> None:
    global _VALIDATORS_READY
    if not _VALIDATORS_READY:
        register_builtin_validators()
        _VALIDATORS_READY = True


class BaseStepConverter(ABC):
    """One converter per Pentaho step type: parse → generate → validate."""

    step_types: ClassVar[frozenset[str]] = frozenset()
    converter_name: ClassVar[str] = ""

    def handles(self, step_type: str) -> bool:
        return step_type.strip().lower() in self.step_types

    def can_handle(self, step_type: str) -> bool:
        return self.handles(step_type)

    def convert(self, context: StepContext) -> StepConversionOutcome:
        _ensure_validators()
        parsed = self.parse_xml(context)
        code_lines = self.generate(context, parsed)
        validation = self.validate_semantics(context, parsed, code_lines)
        input_cols = set(context.extra.get("input_columns") or [])
        if input_cols:
            from ..lineage import validate_column_lineage
            le, lw = validate_column_lineage(code_lines, input_cols, context.step.step_type)
            validation.errors.extend(le)
            validation.warnings.extend(lw)
            if le or lw:
                from ..validation.step_validators import _score
                validation.score = _score(validation)
        status = derive_status(
            validation.score,
            validation.errors,
            validation.warnings,
            has_dedicated_handler=True,
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

    def parse_xml(self, context: StepContext) -> dict[str, Any]:
        return parse_step_config(context)

    @abstractmethod
    def generate(self, context: StepContext, parsed: dict[str, Any]) -> list[str]:
        ...

    def validate_semantics(
        self,
        context: StepContext,
        parsed: dict[str, Any],
        code_lines: list[str],
    ) -> SemanticValidationResult:
        validator = get_validator(context.step.step_type)
        if validator is not None:
            return validator.validate(context, parsed, code_lines)
        from ..validation.step_validators import GenericStepValidator
        return GenericStepValidator().validate(context, parsed, code_lines)

    def register(self, registry) -> None:
        registry.register(self)
