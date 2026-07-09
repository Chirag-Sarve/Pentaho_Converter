"""Intelligent fallback handler — always produces runnable PySpark."""

from __future__ import annotations

from ..step_xml import get_step_element, parse_join_keys
from .base import BaseStepHandler, StepContext


class FallbackHandler(BaseStepHandler):
    """Infers Spark semantics for unknown step types and never marks unsupported."""

    def can_handle(self, step_type: str) -> bool:
        return True

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        step_type = (step.step_type or "").strip()
        step_el = get_step_element(step)

        lines = [f"# {step_type}: {step.name}"]

        inferred = self._infer_handler_kind(step_type)
        if inferred == "join" and len(inputs) >= 2:
            keys = parse_join_keys(step_el) if step_el is not None else []
            if keys:
                on_cols = ", ".join(f'"{k.left}"' for k in keys)
                lines.append(
                    f"{out_var} = {inputs[0]}.join({inputs[1]}, on=[{on_cols}], how='left')"
                )
            else:
                lines.append(f"{out_var} = {inputs[0]}.join({inputs[1]}, how='left')")
            return lines, "converted"

        if inferred == "output" and in_df:
            target = self._attr(context, "table", "") or self._attr(context, "filename", "output")
            lines.append(f"{in_df}.write.format('delta').mode('append').saveAsTable({target!r})")
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"

        if inferred == "input":
            path = self._attr(context, "filename", "") or self._attr(context, "sql", "")
            if path:
                lines.append(f"{out_var} = spark.read.format('csv').option('header', 'true').load({path!r})")
                return lines, "converted"

        if in_df:
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"

        if inputs:
            lines.append(f"{out_var} = {inputs[0]}")
            return lines, "converted"

        lines.append(f"{out_var} = spark.range(1).select(lit(1).alias('_row'))")
        return lines, "converted"

    @staticmethod
    def _infer_handler_kind(step_type: str) -> str:
        t = step_type.lower()
        if any(k in t for k in ("join", "lookup", "merge")):
            return "join"
        if any(k in t for k in ("output", "writer", "export")):
            return "output"
        if any(k in t for k in ("input", "reader", "load")):
            return "input"
        return "transform"
