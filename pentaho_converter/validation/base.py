"""Base types for per-step semantic validation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..step_context import StepContext


@dataclass
class SemanticValidationResult:
    """Outcome of validating generated PySpark for one step.

    Warning levels:
    - ``infos``: preserved metadata / formatting notes (do not reduce score)
    - ``warnings``: unsupported runtime behavior or semantic differences
    - ``errors``: missing executable code / broken lineage
    """

    score: float = 0.0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    infos: list[str] = field(default_factory=list)
    properties_converted: list[str] = field(default_factory=list)
    properties_missing: list[str] = field(default_factory=list)
    output_columns: list[str] = field(default_factory=list)
    syntax_valid: bool = True

    @property
    def passed(self) -> bool:
        return self.score >= 0.95 and not self.errors and self.syntax_valid

    @property
    def partial(self) -> bool:
        return 0.5 <= self.score < 0.95 or (bool(self.warnings) and self.score < 0.95)

    @property
    def failed(self) -> bool:
        return self.score < 0.5 or bool(self.errors) or not self.syntax_valid


class StepValidator(ABC):
    """Validates that generated code semantically matches Pentaho XML config."""

    step_types: frozenset[str] = frozenset()

    def handles(self, step_type: str) -> bool:
        raw = (step_type or "").strip().lower()
        compact = raw.replace(" ", "").replace("(", "").replace(")", "")
        types = {t.strip().lower().replace(" ", "").replace("(", "").replace(")", "") for t in self.step_types}
        return raw in self.step_types or compact in types or raw in types

    @abstractmethod
    def validate(
        self,
        context: StepContext,
        parsed: dict[str, Any],
        code_lines: list[str],
    ) -> SemanticValidationResult:
        ...
