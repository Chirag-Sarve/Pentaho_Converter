"""Base class for dedicated Pentaho step converters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from ..conversion_outcome import (
    StepConversionOutcome,
    derive_status,
    format_display_status,
)
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


_TEXTFILE_EXECUTABLE_GAP_TOKENS = (
    "filename missing",
    "delimiter missing",
    "fixed-width",
    "is_command",
    "run as command",
    "accept_filenames",
    "footer lines are not written",
    "filenameinfield",
)


def _has_textfile_executable_gap(validation: SemanticValidationResult, code: str) -> bool:
    if any(p in ("filename", "delimiter") for p in validation.properties_missing):
        return True
    blob = " ".join(validation.warnings).lower() + "\n" + (code or "").lower()
    return any(token in blob for token in _TEXTFILE_EXECUTABLE_GAP_TOKENS)


def _executable_complete(step_type: str, code: str, validation: SemanticValidationResult) -> bool:
    """True when generated code has runnable Spark I/O and no hard errors."""
    if validation.errors or not validation.syntax_valid:
        return False
    st = (step_type or "").strip().lower().replace(" ", "")
    if st in ("textfileinput", "oldtextfileinput"):
        if _has_textfile_executable_gap(validation, code):
            return False
        return any(tok in code for tok in (".csv(", ".load(", ".text(", 'format("csv")', "format('csv')"))
    if st in ("textfileoutput", "textfileoutputlegacy"):
        if _has_textfile_executable_gap(validation, code):
            return False
        return (".save(" in code or ".text(" in code) and (
            'format("csv")' in code or "format('csv')" in code or ".text(" in code
        )
    return validation.score >= 0.95


class BaseStepConverter(ABC):
    """One converter per Pentaho step type: parse → generate → validate."""

    step_types: ClassVar[frozenset[str]] = frozenset()
    converter_name: ClassVar[str] = ""

    def handles(self, step_type: str) -> bool:
        raw = (step_type or "").strip().lower()
        compact = raw.replace(" ", "").replace("(", "").replace(")", "")
        types = {
            t.strip().lower().replace(" ", "").replace("(", "").replace(")", "")
            for t in self.step_types
        }
        return raw in self.step_types or compact in types or raw in types

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
        code = "\n".join(code_lines)
        executable = _executable_complete(context.step.step_type, code, validation)
        status = derive_status(
            validation.score,
            validation.errors,
            validation.warnings,
            has_dedicated_handler=True,
            step_type=context.step.step_type,
            executable_complete=executable,
        )
        display = format_display_status(
            status,
            warnings=validation.warnings,
            infos=validation.infos,
            errors=validation.errors,
        )
        return StepConversionOutcome(
            code_lines=code_lines,
            status=status,
            semantic_score=validation.score,
            detail=context.step.name,
            warnings=validation.warnings,
            errors=validation.errors,
            infos=validation.infos,
            properties_converted=validation.properties_converted,
            properties_missing=validation.properties_missing,
            output_columns=validation.output_columns,
            syntax_valid=validation.syntax_valid,
            handler_name=context.step.step_type,
            display_status=display,
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
