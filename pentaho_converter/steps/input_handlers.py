"""Handlers for Pentaho input steps.

Core Inputs: TableInput, CsvInput, ExcelInput, TextFileInput.
Additional Inputs (Fixed/GZIP/S3/YAML/Property/Access/…) live in
``extra_input_handlers`` and are registered via ``EXTRA_INPUT_HANDLERS``.
"""

from __future__ import annotations

from ..lineage import substitute_pentaho_variables
from ..metadata_propagation import get_converter_metadata
from ..path_utils import spark_load_path_expr
from ..schema_utils import fields_to_schema_ddl
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
        params = context.transformation.parameters
        raw_path = self._attr(context, "filename", "") or self._attr(context, "file", "")
        filename = substitute_pentaho_variables(raw_path, params)
        delimiter = self._attr(context, "separator", ",") or ","
        header = self._attr(context, "header", "Y").upper() == "Y"
        enclosure = self._attr(context, "enclosure", "") or self._attr(context, "quote", "")

        metadata = get_converter_metadata(context)
        fields = list(metadata.get("fields") or [])
        if not fields:
            fields = [
                {"name": f.name, "type": f.type_name}
                for f in self._fields(context)
                if f.name
            ]
        schema_ddl = fields_to_schema_ddl(fields)

        lines = [f"# CSV Input: {step.name}"]
        lines.append(f"{out_var} = (")
        lines.append("    spark.read.format('csv')")
        lines.append(f"    .option('header', {header!r})")
        lines.append(f"    .option('sep', {delimiter!r})")
        if enclosure:
            lines.append(f"    .option('quote', {enclosure!r})")
        if schema_ddl:
            lines.append("    .option('inferSchema', False)")
            lines.append(f"    .schema({schema_ddl!r})")
        load_path = spark_load_path_expr(filename)
        lines.append(f"    .load({load_path})")
        lines.append(")")
        return lines, "converted" if filename else "converted"


class ExcelInputHandler(BaseStepHandler):
    _TYPES = {"excelinput", "microsoftexcelinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        # Finish path/variable resolution to match CsvInput / TextFileInput.
        step = context.step
        out_var = context.output_df_name()
        params = context.transformation.parameters
        raw_path = self._attr(context, "filename", "") or self._attr(context, "file", "")
        filename = substitute_pentaho_variables(raw_path, params)
        sheet = self._attr(context, "sheetname", "Sheet1") or "Sheet1"
        header = self._attr(context, "header", "Y").upper() == "Y"
        load_path = spark_load_path_expr(filename)

        lines = [f"# Excel Input: {step.name}"]
        lines.append(f"{out_var} = (")
        lines.append("    spark.read.format('com.crealytics.spark.excel')")
        lines.append(f"    .option('sheetName', {sheet!r})")
        lines.append(f"    .option('header', {str(header).lower()!r})")
        lines.append(f"    .load({load_path})")
        lines.append(")")
        return lines, "converted"


class TextFileInputHandler(BaseStepHandler):
    """Text File Input and Text File Input (Legacy / OldTextFileInput)."""

    _TYPES = {"textfileinput", "oldtextfileinput"}

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