"""Handlers for Pentaho input steps."""

from __future__ import annotations

from ..lineage import substitute_pentaho_variables
from ..metadata_propagation import get_converter_metadata
from ..text_file_input_converter import convert_text_file_input_step
from .base import BaseStepHandler, StepContext


class TableInputHandler(BaseStepHandler):
    _TYPES = {"tableinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        out_var = context.output_df_name()
        params = context.transformation.parameters
        sql = substitute_pentaho_variables(self._attr(context, "sql", ""), params)
        connection = self._attr(context, "connection", "")
        schema = self._attr(context, "schema", "")
        table = self._attr(context, "table", "")

        lines = [f"# Table Input: {step.name}"]
        if sql:
            lines.append(f"{out_var} = spark.sql({sql!r})")
        elif table:
            full = f"{schema}.{table}" if schema else table
            lines.append(f"{out_var} = spark.table({full!r})")
        else:
            lines.append(
                f"{out_var} = spark.read.format('jdbc').option('url', 'jdbc:...').load()"
            )
            return lines, "converted"
        return lines, "converted"


class CsvInputHandler(BaseStepHandler):
    _TYPES = {"csvinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", "") or self._attr(context, "file", "")
        delimiter = self._attr(context, "separator", ",") or ","
        header = self._attr(context, "header", "Y").upper() == "Y"

        lines = [f"# CSV Input: {step.name}"]
        lines.append(f"{out_var} = (")
        lines.append(f"    spark.read.format('csv')")
        lines.append(f"    .option('header', {header!r})")
        lines.append(f"    .option('delimiter', {delimiter!r})")
        lines.append(f"    .load({filename!r})")
        lines.append(")")
        return lines, "converted" if filename else "converted"


class ExcelInputHandler(BaseStepHandler):
    _TYPES = {"excelinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", "") or self._attr(context, "file", "")
        sheet = self._attr(context, "sheetname", "Sheet1")

        lines = [f"# Excel Input: {step.name}"]
        lines.append(f"{out_var} = (")
        lines.append(f"    spark.read.format('com.crealytics.spark.excel')")
        lines.append(f"    .option('sheetName', {sheet!r})")
        lines.append(f"    .option('header', 'true')")
        lines.append(f"    .load({filename!r})")
        lines.append(")")
        return lines, "converted"


class TextFileInputHandler(BaseStepHandler):
    _TYPES = {"textfileinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        return convert_text_file_input_step(
            metadata,
            context.output_df_name(),
            context.step.name,
            context=context,
        )
