"""Handlers for Pentaho transformation steps."""

from __future__ import annotations

from ..calculator_converter import (
    CalculationConvertResult,
    calculations_from_metadata,
    convert_calculation_result,
)
from ..expression_converter import convert_formula
from ..filter_converter import convert_filter_rows_step
from ..group_by_converter import convert_group_by_step
from ..database_lookup_converter import convert_database_lookup_step
from ..merge_join_converter import convert_merge_join_step
from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    _child_text,
    format_spark_join_on,
    get_step_element,
    parse_calculations,
    parse_join_keys,
    parse_sort_fields,
)
from .base import BaseStepHandler, StepContext


class FilterRowsHandler(BaseStepHandler):
    _TYPES = {"filterrows"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        return convert_filter_rows_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


class SelectValuesHandler(BaseStepHandler):
    _TYPES = {"selectvalues"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def _resolve_select_fields(self, context: StepContext) -> list[tuple[str, str]]:
        """Return (source_name, output_name) pairs for the select projection."""
        metadata = get_converter_metadata(context)
        fields: list[tuple[str, str]] = []
        seen: set[str] = set()

        def _add(name: str, rename: str = "") -> None:
            name = (name or "").strip()
            if not name:
                return
            rename = (rename or "").strip()
            if name in seen:
                if rename:
                    for idx, (existing_name, existing_rename) in enumerate(fields):
                        if existing_name == name:
                            fields[idx] = (name, rename)
                            break
                return
            seen.add(name)
            fields.append((name, rename))

        for item in metadata.get("fields") or []:
            if isinstance(item, dict) and item.get("name"):
                _add(item["name"], item.get("rename", ""))

        step_el = get_step_element(context.step)
        if step_el is not None:
            for meta_el in step_el.findall("meta"):
                _add(_child_text(meta_el, "name"), _child_text(meta_el, "rename"))

        if not fields:
            for f in self._fields(context):
                if f.name:
                    _add(f.name, f.rename)

        if step_el is not None:
            remove_names = {
                _child_text(rem_el, "name")
                for rem_el in step_el.findall("remove")
                if _child_text(rem_el, "name")
            }
            if remove_names:
                if not fields:
                    for col_name in context.input_column_names:
                        _add(col_name)
                fields = [(name, rename) for name, rename in fields if name not in remove_names]

        if not fields:
            for col_name in metadata.get("select_columns") or []:
                _add(col_name)

        return fields

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        select_fields = self._resolve_select_fields(context)
        col_exprs: list[str] = []

        lines = [f"# Select Values: {step.name}"]
        for name, rename in select_fields:
            out_name = rename or name
            if out_name != name:
                col_exprs.append(f'col("{name}").alias("{out_name}")')
            else:
                col_exprs.append(f'col("{name}")')

        if in_df and col_exprs:
            lines.append(f"{out_var} = {in_df}.select({', '.join(col_exprs)})")
        elif in_df:
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"
        return lines, "converted"


class SortRowsHandler(BaseStepHandler):
    _TYPES = {"sortrows"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(step)
        sort_fields = parse_sort_fields(step_el) if step_el is not None else []
        fields = self._fields(context)

        lines = [f"# Sort Rows: {step.name}"]
        if in_df and sort_fields:
            sort_cols = []
            for name, ascending in sort_fields:
                direction = "asc()" if ascending else "desc()"
                sort_cols.append(f'col("{name}").{direction}')
            lines.append(f"{out_var} = {in_df}.orderBy({', '.join(sort_cols)})")
        elif in_df and fields:
            sort_cols = []
            for f in fields:
                direction = "desc()" if self._attr(context, "ascending", "Y").upper() == "N" else "asc()"
                sort_cols.append(f'col("{f.name}").{direction}')
            lines.append(f"{out_var} = {in_df}.orderBy({', '.join(sort_cols)})")
        elif in_df:
            lines.append(f"{out_var} = {in_df}.orderBy(monotonically_increasing_id())")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"
        return lines, "converted"


class GroupByHandler(BaseStepHandler):
    _TYPES = {"groupby"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        fallback_keys = [f.name for f in self._fields(context) if f.name]
        lines = convert_group_by_step(
            metadata, in_df, out_var, step.name, fallback_group_keys=fallback_keys
        )
        return lines, "converted"


class CalculatorHandler(BaseStepHandler):
    _TYPES = {"calculator"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)

        lines = [f"# Calculator: {step.name}"]
        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        calculations = calculations_from_metadata(metadata)
        if not calculations:
            step_el = get_step_element(step)
            if step_el is not None:
                calculations = parse_calculations(step_el)
        if not calculations:
            calc_type = self._attr(context, "calc_type", "")
            field_a = self._attr(context, "field_a", "")
            if calc_type and field_a:
                from ..step_xml import CalculationSpec

                lines.append(
                    f"# WARNING: Calculator '{step.name}': using legacy flat attributes; "
                    "parsed calculations metadata is missing"
                )
                calculations = [
                    CalculationSpec(
                        field_name=self._attr(context, "field_name", "calc_result"),
                        calc_type=calc_type,
                        field_a=field_a,
                        field_b=self._attr(context, "field_b"),
                        value_type=self._attr(context, "value_type"),
                    )
                ]

        if not calculations:
            lines.append(
                f"# WARNING: Calculator '{step.name}': no calculation metadata found"
            )
            lines.append(
                f"{out_var} = spark.createDataFrame([], '_calculator_unresolved STRING')"
            )
            return lines, "partial"

        lines.append(f"{out_var} = {in_df}")
        status = "converted"
        for calc in calculations:
            result: CalculationConvertResult = convert_calculation_result(calc)
            if result.warning:
                lines.append(f"# WARNING: {result.warning}")
            if not result.supported:
                status = "partial"
            lines.append(
                f'{out_var} = {out_var}.withColumn("{calc.field_name}", {result.expr})'
            )
            if calc.remove:
                drop_cols = [c for c in (calc.field_a, calc.field_b, calc.field_c) if c]
                if drop_cols:
                    quoted = ", ".join(f'"{c}"' for c in drop_cols)
                    lines.append(f"{out_var} = {out_var}.drop({quoted})")

        return lines, status


class FormulaHandler(BaseStepHandler):
    _TYPES = {"formula"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        formula = self._attr(context, "formula", "")
        field_name = self._attr(context, "field_name", "formula_result")

        lines = [f"# Formula: {step.name}"]
        if in_df and formula:
            lines.append(
                f"{out_var} = {in_df}.withColumn({field_name!r}, {convert_formula(formula)})"
            )
            return lines, "converted"
        elif in_df:
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"


class ReplaceNullHandler(BaseStepHandler):
    _TYPES = {"replacenull"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        fields = self._fields(context)
        replace_value = self._attr(context, "replace_value", "''")

        lines = [f"# Replace Null Values: {step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}")
            for f in fields:
                if f.name:
                    lines.append(
                        f'{out_var} = {out_var}.withColumn("{f.name}", '
                        f'when(col("{f.name}").isNull(), {replace_value}).otherwise(col("{f.name}")))'
                    )
            return lines, "converted" if fields else "converted"
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"


class MergeJoinHandler(BaseStepHandler):
    _TYPES = {"mergejoin"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        return convert_merge_join_step(
            metadata,
            inputs,
            out_var,
            context.step.name,
            context=context,
        )


class StreamLookupHandler(BaseStepHandler):
    _TYPES = {"streamlookup"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        step_el = get_step_element(step)
        keys = parse_join_keys(step_el) if step_el is not None else []

        lines = [f"# Stream Lookup: {step.name}"]
        if len(inputs) >= 2:
            main_df, lookup_df = inputs[0], inputs[1]
            if keys:
                on_arg, use_on = format_spark_join_on(main_df, lookup_df, keys)
                lines.append(f"_lkp_{out_var} = broadcast({lookup_df})")
                if use_on:
                    lines.append(
                        f"{out_var} = {main_df}.join(_lkp_{out_var}, on={on_arg}, how='left')"
                    )
                else:
                    lines.append(
                        f"{out_var} = {main_df}.join(_lkp_{out_var}, {on_arg}, 'left')"
                    )
            else:
                lines.append(
                    f"# StreamLookup '{step.name}': no join keys — lookup join not generated"
                )
                lines.append(f"{out_var} = {main_df}")
            return lines, "converted"
        elif len(inputs) == 1:
            lines.append(f"{out_var} = {inputs[0]}")
            return lines, "converted"
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"


class DatabaseLookupHandler(BaseStepHandler):
    _TYPES = {"databaselookup"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        return convert_database_lookup_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )
