"""Base types for per-step semantic validation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..step_context import StepContext


@dataclass
class SemanticValidationResult:
    """Outcome of validating generated PySpark for one step."""

    score: float = 0.0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    properties_converted: list[str] = field(default_factory=list)
    properties_missing: list[str] = field(default_factory=list)
    output_columns: list[str] = field(default_factory=list)
    syntax_valid: bool = True

    @property
    def passed(self) -> bool:
        return self.score >= 0.95 and not self.errors and self.syntax_valid

    @property
    def partial(self) -> bool:
        return 0.5 <= self.score < 0.95 or bool(self.warnings)

    @property
    def failed(self) -> bool:
        return self.score < 0.5 or bool(self.errors) or not self.syntax_valid


class StepValidator(ABC):
    """Validates that generated code semantically matches Pentaho XML config."""

    step_types: frozenset[str] = frozenset()

    def handles(self, step_type: str) -> bool:
        return step_type.strip().lower() in self.step_types

    @abstractmethod
    def validate(
        self,
        context: StepContext,
        parsed: dict[str, Any],
        code_lines: list[str],
    ) -> SemanticValidationResult:
        ...
