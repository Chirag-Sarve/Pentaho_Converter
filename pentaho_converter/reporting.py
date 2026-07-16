"""Build project-level conversion reports."""

from __future__ import annotations

from .conversion_outcome import (
    STATUS_CONVERTED,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_PARTIALLY_SUPPORTED,
    STATUS_SCORE_WEIGHTS,
    STATUS_UNSUPPORTED,
    ConversionReport,
    StepConversionOutcome,
    format_display_status,
)
from .models import ConversionStats, StepConversionResult


def build_conversion_report(
    stats: ConversionStats,
    transformation_name: str = "",
) -> ConversionReport:
    """Aggregate step outcomes into coverage + semantic accuracy metrics."""
    outcomes = getattr(stats, "step_outcomes", []) or []
    if not outcomes and stats.step_results:
        outcomes = _legacy_results_to_outcomes(stats.step_results)

    total = len(outcomes) or len(stats.step_results) or 1
    with_handler = sum(
        1 for o in outcomes
        if o.handler_name and o.status != STATUS_UNSUPPORTED
    )
    coverage = (with_handler / total * 100) if total else 0.0

    if outcomes:
        semantic_accuracy = sum(o.semantic_score for o in outcomes) / len(outcomes) * 100
    else:
        semantic_accuracy = _legacy_semantic_accuracy(stats)

    return ConversionReport(
        transformation_name=transformation_name,
        total_steps=total,
        steps_with_handler=with_handler,
        coverage_percent=coverage,
        semantic_accuracy_percent=semantic_accuracy,
        step_outcomes=outcomes,
        warnings=list(stats.warnings),
    )


def _legacy_results_to_outcomes(results: list[StepConversionResult]) -> list[StepConversionOutcome]:
    outcomes: list[StepConversionOutcome] = []
    for r in results:
        weight = STATUS_SCORE_WEIGHTS.get(r.status, 0.5)
        outcomes.append(
            StepConversionOutcome(
                status=r.status,
                semantic_score=weight,
                detail=r.step_name,
                handler_name=r.step_type,
            )
        )
    return outcomes


def _legacy_semantic_accuracy(stats: ConversionStats) -> float:
    steps = stats.step_results
    if not steps:
        return 0.0
    total = len(steps)
    converted = sum(1 for s in steps if s.status == STATUS_CONVERTED)
    partial = sum(1 for s in steps if s.status in (STATUS_PARTIAL, STATUS_PARTIALLY_SUPPORTED, "approximated"))
    failed = sum(1 for s in steps if s.status in (STATUS_FAILED, STATUS_UNSUPPORTED, "unsupported", "skipped"))
    score = (converted * 1.0 + partial * 0.6 + failed * 0.1) / total
    return score * 100


def sync_stats_from_outcomes(stats: ConversionStats, outcomes: list[StepConversionOutcome]) -> None:
    """Update ConversionStats counters from semantic outcomes."""
    stats.steps_converted = 0
    stats.steps_approximated = 0
    stats.steps_skipped = 0
    stats.step_results = []
    stats.step_outcomes = outcomes

    for o in outcomes:
        display = o.display_status or format_display_status(
            o.status, warnings=o.warnings, infos=o.infos, errors=o.errors
        )
        detail_bits = o.errors + o.warnings + (o.infos[:2] if o.infos and not o.warnings else [])
        stats.step_results.append(
            StepConversionResult(
                step_name=o.detail,
                step_type=o.handler_name,
                status=o.status,
                detail="; ".join(detail_bits) if detail_bits else o.detail,
                semantic_score=o.semantic_score,
                warnings=o.warnings,
                errors=o.errors,
                infos=list(o.infos or []),
                display_status=display,
            )
        )
        if o.status == STATUS_CONVERTED:
            stats.steps_converted += 1
        elif o.status in (STATUS_PARTIAL, STATUS_PARTIALLY_SUPPORTED, "approximated"):
            stats.steps_approximated += 1
        else:
            stats.steps_skipped += 1
        for w in o.warnings:
            if w not in stats.warnings:
                stats.warnings.append(w)
