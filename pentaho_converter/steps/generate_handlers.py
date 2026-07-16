"""Handlers for Pentaho row generation and constant steps."""

from __future__ import annotations

from ..step_xml import (
    _child_text,
    get_step_element,
    parse_constant_fields,
    parse_data_grid_rows,
    parse_row_generator_fields,
)
from .base import BaseStepHandler, StepContext


def _format_python_value(value: str, type_name: str) -> str:
    """Format a Pentaho field value as a Python literal for createDataFrame."""
    if value is None or value == "":
        return "None"
    t = (type_name or "String").lower()
    if "bool" in t:
        return "True" if value.upper() in ("Y", "TRUE", "1", "T") else "False"
    if t in ("integer", "int"):
        try:
            return str(int(value))
        except ValueError:
            return repr(value)
    if t in ("number", "bignumber", "float", "double"):
        try:
            num = float(value)
            return str(int(num)) if num == int(num) and "int" not in t else str(num)
        except ValueError:
            return repr(value)
    return repr(value)


def _format_row_tuple(values: list[str], types: list[str]) -> str:
    parts = [_format_python_value(v, t) for v, t in zip(values, types)]
    return "(" + ", ".join(parts) + ")"


def _constant_lit_expr(
    value: str,
    type_name: str,
    set_empty_string: bool = False,
    *,
    format_mask: str = "",
    decimal: str = "",
    group: str = "",
) -> str:
    """Build a PySpark lit()/to_date()/to_timestamp() expression for a constant."""
    if set_empty_string:
        return 'lit("")'
    if value is None or value == "":
        return "lit(None)"
    t = (type_name or "String").lower()
    if "bool" in t:
        return f"lit({value.upper() in ('Y', 'TRUE', '1', 'T')})"
    if t in ("integer", "int"):
        cleaned = value
        if group:
            cleaned = cleaned.replace(group, "")
        try:
            return f"lit({int(float(cleaned))})"
        except ValueError:
            return f"lit({value!r})"
    if t in ("number", "bignumber", "float", "double"):
        cleaned = value
        if group:
            cleaned = cleaned.replace(group, "")
        if decimal and decimal != ".":
            cleaned = cleaned.replace(decimal, ".")
        try:
            return f"lit({float(cleaned)})"
        except ValueError:
            return f"lit({value!r})"
    if t == "date":
        if format_mask:
            return f'to_date(lit({value!r}), {format_mask!r})'
        return f'to_date(lit({value!r}))'
    if t == "timestamp":
        if format_mask:
            return f'to_timestamp(lit({value!r}), {format_mask!r})'
        return f'to_timestamp(lit({value!r}))'
    return f"lit({value!r})"


class RowGeneratorHandler(BaseStepHandler):
    """Converts Pentaho Generate Rows (RowGenerator) and Data Grid steps."""

    _TYPES = {"rowgenerator", "datagrid"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        out_var = context.output_df_name()
        step_el = get_step_element(step)
        lines = [f"# Generate Rows: {step.name}"]

        if step_el is None:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        columns, grid_rows = parse_data_grid_rows(step_el)
        fields = parse_row_generator_fields(step_el)

        if not columns and fields:
            columns = [f.name for f in fields]
        types = [f.type_name for f in fields] if fields else ["String"] * len(columns)

        # Data Grid: explicit multi-row data
        if grid_rows:
            row_tuples = []
            for row in grid_rows:
                padded = row + [""] * max(0, len(columns) - len(row))
                row_tuples.append(_format_row_tuple(padded[: len(columns)], types))
            col_list = ", ".join(repr(c) for c in columns)
            lines.append("data = [")
            lines.extend(f"    {rt}," for rt in row_tuples)
            lines.append("]")
            lines.append(f"{out_var} = spark.createDataFrame(data, [{col_list}])")
            return lines, "converted"

        # RowGenerator: repeat configured field values for `limit` rows
        if fields:
            values = [f.value for f in fields]
            types = [f.type_name for f in fields]
            limit_str = _child_text(step_el, "limit", step.attributes.get("limit", "1"))
            try:
                limit = max(1, int(limit_str))
            except ValueError:
                limit = 1

            row_tuple = _format_row_tuple(values, types)
            col_list = ", ".join(repr(f.name) for f in fields)
            if limit == 1:
                lines.append(f"data = [{row_tuple}]")
            else:
                lines.append(f"data = [{row_tuple}] * {limit}")
            lines.append(f"{out_var} = spark.createDataFrame(data, [{col_list}])")
            return lines, "converted"

        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "converted"


class ConstantHandler(BaseStepHandler):
    """Converts Pentaho Add Constants step."""

    _TYPES = {"constant", "addconstants", "addconstant"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..metadata_propagation import get_converter_metadata

        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(step)
        lines = [f"# Add Constants: {step.name}"]

        if step_el is None:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        metadata = get_converter_metadata(context)
        constants = parse_constant_fields(step_el)
        if not constants and metadata.get("constants"):
            from ..step_xml import ConstantField

            constants = [
                ConstantField(
                    name=c.get("name", ""),
                    type_name=c.get("type_name") or c.get("type", "String"),
                    value=c.get("value", ""),
                    set_empty_string=bool(c.get("set_empty_string")),
                    format=c.get("format", ""),
                    currency=c.get("currency", ""),
                    decimal=c.get("decimal", ""),
                    group=c.get("group", ""),
                    length=c.get("length", ""),
                    precision=c.get("precision", ""),
                )
                for c in metadata["constants"]
                if c.get("name")
            ]

        if not constants:
            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(
                f'{out_var} = spark.range(1).select(lit(None).alias("_init")).drop("_init")'
            )

        for const in constants:
            expr = _constant_lit_expr(
                const.value,
                const.type_name,
                const.set_empty_string,
                format_mask=const.format,
                decimal=const.decimal,
                group=const.group,
            )
            lines.append(f'{out_var} = {out_var}.withColumn("{const.name}", {expr})')
            preserved = []
            if const.format:
                preserved.append(f"format={const.format!r}")
            if const.currency:
                preserved.append(f"currency={const.currency!r}")
            if const.length:
                preserved.append(f"length={const.length!r}")
            if const.precision:
                preserved.append(f"precision={const.precision!r}")
            if preserved:
                lines.append(f"# preserved.{const.name}: {', '.join(preserved)}")

        return lines, "converted"
