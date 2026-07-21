"""Runtime context for Pentaho step conversion."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .graph import StepDAG
    from .metadata_models import ColumnLineage, StepMetadataBundle
    from .models import PentahoStep, PentahoTransformation


@dataclass
class StepContext:
    """Runtime context passed to each step handler during code generation."""

    transformation: PentahoTransformation
    step: PentahoStep
    dag: StepDAG
    df_variable_map: dict[str, str] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def input_df_name(self) -> str | None:
        # Prefer FilterRows/JavaFilter/SwitchCase branch stream over the filter's
        # primary output so true/false targets never share the wrong DataFrame.
        try:
            from .filter_converter import resolve_incoming_branch_df

            branch = resolve_incoming_branch_df(self)
            if branch:
                return branch
        except Exception:
            pass
        preds = self.dag.predecessors(self.step.name)
        if not preds:
            return None
        return self.df_variable_map.get(preds[0], f"df_{preds[0]}")

    def all_input_df_names(self) -> list[str]:
        names: list[str] = []
        for pred in self.dag.predecessors(self.step.name):
            pred_step = self.dag.steps.get(pred)
            pst = (
                (pred_step.step_type or "").strip().lower().replace(" ", "")
                if pred_step is not None
                else ""
            )
            if pst in {"filterrows", "javafilter", "switchcase"}:
                try:
                    from .filter_converter import resolve_incoming_branch_df

                    branch = resolve_incoming_branch_df(self)
                    if branch:
                        names.append(branch)
                        continue
                except Exception:
                    pass
            names.append(self.df_variable_map.get(pred, f"df_{pred}"))
        return names

    def output_df_name(self) -> str:
        safe = self.step.name.replace(" ", "_").replace("-", "_")
        return self.df_variable_map.get(self.step.name, f"df_{safe}")

    @property
    def metadata_bundle(self) -> StepMetadataBundle | None:
        return self.extra.get("metadata_bundle")

    @property
    def converter_metadata(self) -> dict[str, Any]:
        return self.extra.get("converter_metadata", {})

    @property
    def column_lineage(self) -> ColumnLineage | None:
        return self.extra.get("column_lineage")

    @property
    def input_column_names(self) -> list[str]:
        lineage = self.extra.get("lineage_map", {})
        names: set[str] = set()
        for pred in self.dag.predecessors(self.step.name):
            names.update(lineage.get(pred, {}).keys())
        if not names:
            names.update(self.extra.get("input_columns") or [])
        return sorted(names)
