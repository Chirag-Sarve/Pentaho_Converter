"""Domain models for Pentaho jobs and transformations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class PentahoHop:
    """Directed edge between steps or job entries."""

    from_name: str
    to_name: str
    enabled: bool = True
    evaluation: str | None = None  # job hop: true/false/unconditional


@dataclass
class PentahoField:
    """Column/field metadata within a step."""

    name: str
    type_name: str = "String"
    length: str = ""
    precision: str = ""
    format: str = ""
    currency: str = ""
    decimal: str = ""
    group: str = ""
    null_if: str = ""
    default: str = ""
    rename: str = ""


@dataclass
class PentahoStep:
    """A single transformation step."""

    name: str
    step_type: str
    attributes: dict[str, str] = field(default_factory=dict)
    fields: list[PentahoField] = field(default_factory=list)
    raw_element: Any = None
    parsed_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class PentahoTransformation:
    """Parsed .ktr transformation."""

    name: str
    file_path: Path
    steps: list[PentahoStep] = field(default_factory=list)
    hops: list[PentahoHop] = field(default_factory=list)
    parameters: dict[str, str] = field(default_factory=dict)
    variables: dict[str, str] = field(default_factory=dict)


@dataclass
class PentahoJobEntry:
    """A single job entry (node in a .kjb workflow)."""

    name: str
    entry_type: str
    filename: str = ""
    transname: str = ""
    attributes: dict[str, str] = field(default_factory=dict)
    is_start: bool = False


@dataclass
class PentahoJob:
    """Parsed .kjb job."""

    name: str
    file_path: Path
    entries: list[PentahoJobEntry] = field(default_factory=list)
    hops: list[PentahoHop] = field(default_factory=list)


@dataclass
class ScanResult:
    """Result of scanning an extracted Pentaho project."""

    root: Path
    job_files: list[Path] = field(default_factory=list)
    transformation_files: list[Path] = field(default_factory=list)
    other_files: list[Path] = field(default_factory=list)


@dataclass
class StepConversionResult:
    """Outcome of converting a single step."""

    step_name: str
    step_type: str
    status: str  # converted | partial | failed | unsupported | manual_required | partially_supported
    detail: str = ""
    semantic_score: float = 0.0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class ConversionStats:
    """Aggregate statistics for a conversion run."""

    jobs_found: int = 0
    transformations_found: int = 0
    steps_converted: int = 0
    steps_approximated: int = 0
    steps_skipped: int = 0
    warnings: list[str] = field(default_factory=list)
    step_results: list[StepConversionResult] = field(default_factory=list)
    step_outcomes: list[Any] = field(default_factory=list)
    coverage_percent: float = 0.0
    semantic_accuracy_percent: float = 0.0


@dataclass
class ConversionResult:
    """Final output of a Pentaho project conversion."""

    files: dict[str, str] = field(default_factory=dict)
    logs: list[str] = field(default_factory=list)
    stats: ConversionStats = field(default_factory=ConversionStats)
    main_workflow: str | None = None
