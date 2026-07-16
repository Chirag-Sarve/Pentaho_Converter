"""Conversion outcome and reporting models."""

from __future__ import annotations

from dataclasses import dataclass, field

# Step status values (honest reporting)
STATUS_CONVERTED = "converted"
STATUS_PARTIAL = "partial"
STATUS_FAILED = "failed"
STATUS_UNSUPPORTED = "unsupported"
STATUS_MANUAL = "manual_required"
STATUS_PARTIALLY_SUPPORTED = "partially_supported"

STATUS_SCORE_WEIGHTS = {
    STATUS_CONVERTED: 1.0,
    STATUS_PARTIAL: 0.6,
    STATUS_PARTIALLY_SUPPORTED: 0.5,
    STATUS_MANUAL: 0.3,
    STATUS_FAILED: 0.1,
    STATUS_UNSUPPORTED: 0.0,
}


@dataclass
class StepConversionOutcome:
    """Full result of converting one Pentaho step."""

    code_lines: list[str] = field(default_factory=list)
    status: str = STATUS_UNSUPPORTED
    semantic_score: float = 0.0
    detail: str = ""
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    infos: list[str] = field(default_factory=list)
    properties_converted: list[str] = field(default_factory=list)
    properties_missing: list[str] = field(default_factory=list)
    output_columns: list[str] = field(default_factory=list)
    syntax_valid: bool = True
    handler_name: str = ""
    display_status: str = ""


def format_display_status(
    status: str,
    *,
    warnings: list[str] | None = None,
    infos: list[str] | None = None,
    errors: list[str] | None = None,
) -> str:
    """Human-readable validation label for the UI."""
    warnings = warnings or []
    infos = infos or []
    errors = errors or []
    st = (status or "").lower()
    if st == STATUS_CONVERTED:
        if warnings:
            return "CONVERTED WITH WARNINGS"
        if infos:
            return "CONVERTED (Legacy metadata preserved)"
        return "CONVERTED"
    if st in (STATUS_PARTIAL, STATUS_PARTIALLY_SUPPORTED, "approximated"):
        return "PARTIAL"
    if st == STATUS_MANUAL:
        return "MANUAL REVIEW"
    if st in (STATUS_FAILED, STATUS_UNSUPPORTED, "skipped"):
        return "FAILED" if errors or st == STATUS_FAILED else "UNSUPPORTED"
    return (status or "UNKNOWN").upper()


@dataclass
class ConversionReport:
    """Detailed conversion report for a project run."""

    transformation_name: str = ""
    total_steps: int = 0
    steps_with_handler: int = 0
    coverage_percent: float = 0.0
    semantic_accuracy_percent: float = 0.0
    step_outcomes: list[StepConversionOutcome] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "transformation_name": self.transformation_name,
            "coverage_percent": round(self.coverage_percent, 1),
            "semantic_accuracy_percent": round(self.semantic_accuracy_percent, 1),
            "total_steps": self.total_steps,
            "steps_with_handler": self.steps_with_handler,
            "step_results": [
                {
                    "name": o.detail or "",
                    "step_type": o.handler_name,
                    "status": o.status,
                    "display_status": o.display_status
                    or format_display_status(
                        o.status,
                        warnings=o.warnings,
                        infos=o.infos,
                        errors=o.errors,
                    ),
                    "semantic_score": round(o.semantic_score * 100, 1),
                    "warnings": o.warnings,
                    "errors": o.errors,
                    "infos": o.infos,
                    "has_logic": o.status == STATUS_CONVERTED,
                    "detail": "; ".join(o.errors + o.warnings) or o.detail,
                }
                for o in self.step_outcomes
            ],
            "warnings": self.warnings,
        }


def derive_status(
    semantic_score: float,
    errors: list[str],
    warnings: list[str],
    has_dedicated_handler: bool,
    step_type: str,
    manual_types: frozenset[str] | None = None,
    *,
    executable_complete: bool = False,
) -> str:
    """Map validation outcome to an honest status label.

    When ``executable_complete`` is True (Spark read/write path fully emitted),
    preserve CONVERTED even if informational review notes exist. Only genuine
    missing executable behavior or hard errors yield PARTIAL/FAILED.
    """
    manual_types = manual_types or frozenset({
        "scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue",
        "userdefinedjavaclass",
        "ruleaccumulator", "rulesaccumulator",
        "ruleexecutor", "rulesexecutor",
    })
    st = step_type.lower().replace(" ", "")

    if st in manual_types:
        return STATUS_MANUAL if semantic_score < 0.95 else STATUS_PARTIAL

    if errors and semantic_score < 0.5:
        return STATUS_FAILED

    if semantic_score >= 0.95 and not errors:
        return STATUS_CONVERTED

    # Executable Spark behavior present and no hard errors → CONVERTED
    # (warnings may still lower the score / show "WITH WARNINGS" in the UI).
    if executable_complete and not errors and semantic_score >= 0.85:
        return STATUS_CONVERTED

    if not has_dedicated_handler:
        if semantic_score >= 0.75:
            return STATUS_PARTIALLY_SUPPORTED
        return STATUS_UNSUPPORTED

    if semantic_score >= 0.5:
        return STATUS_PARTIAL
    return STATUS_FAILED
