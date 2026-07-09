"""Metadata and column lineage models for the conversion pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnSchema:
    """Column name and Pentaho type metadata."""

    name: str
    type_name: str = "String"
    length: str = ""
    precision: str = ""
    format: str = ""
    source_step: str = ""


@dataclass
class ColumnLineage:
    """Column-level lineage for one step execution."""

    step_name: str
    step_type: str
    input_df: str | None = None
    output_df: str = ""
    input_columns: dict[str, ColumnSchema] = field(default_factory=dict)
    output_columns: dict[str, ColumnSchema] = field(default_factory=dict)
    added_columns: set[str] = field(default_factory=set)
    removed_columns: set[str] = field(default_factory=set)
    modified_columns: set[str] = field(default_factory=set)
    renamed_columns: dict[str, str] = field(default_factory=dict)

    @property
    def input_column_names(self) -> set[str]:
        return set(self.input_columns)

    @property
    def output_column_names(self) -> set[str]:
        return set(self.output_columns)

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_name": self.step_name,
            "step_type": self.step_type,
            "input_df": self.input_df,
            "output_df": self.output_df,
            "input_columns": {k: v.type_name for k, v in self.input_columns.items()},
            "output_columns": {k: v.type_name for k, v in self.output_columns.items()},
            "added_columns": sorted(self.added_columns),
            "removed_columns": sorted(self.removed_columns),
            "modified_columns": sorted(self.modified_columns),
            "renamed_columns": dict(self.renamed_columns),
        }


@dataclass
class StepMetadataBundle:
    """Complete metadata propagated from XML parser to converters."""

    step_name: str
    step_type: str
    attributes: dict[str, str] = field(default_factory=dict)
    parsed_config: dict[str, Any] = field(default_factory=dict)
    converter_metadata: dict[str, Any] = field(default_factory=dict)
    propagation_trace: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_name": self.step_name,
            "step_type": self.step_type,
            "attributes": self.attributes,
            "parsed_config": self.parsed_config,
            "converter_metadata": self.converter_metadata,
            "propagation_trace": self.propagation_trace,
        }


@dataclass
class LineageValidationResult:
    """Result of pre-converter column lineage checks."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors
