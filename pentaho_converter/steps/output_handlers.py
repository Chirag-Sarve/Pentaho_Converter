"""Handlers for Pentaho output steps."""

from __future__ import annotations

from ..generation_config import GenerationConfig
from ..lineage import substitute_pentaho_variables
from ..table_names import table_write_lines
from .base import BaseStepHandler, StepContext


def _generation_config(context: StepContext) -> GenerationConfig:
    cfg = context.extra.get("generation_config")
    if isinstance(cfg, GenerationConfig):
        return cfg
    return GenerationConfig.defaults()


class TableOutputHandler(BaseStepHandler):
    _TYPES = {"tableoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name() or context.output_df_name()
        schema = self._attr(context, "schema", "")
        table = self._attr(context, "table", "")
        out_var = context.output_df_name()
        if not table:
            lines = [f"# Table Output: {step.name}"]
            lines.append(f"{out_var} = {in_df}")
            lines.append(f"{out_var}.write.format('delta').mode('overwrite').saveAsTable('target_table')")
            return lines, "converted"

        lines = table_write_lines(
            out_var=out_var,
            in_df=in_df,
            table=table,
            source_schema=schema,
            step_name=step.name,
            config=_generation_config(context),
        )
        return lines, "converted"


class TextFileOutputHandler(BaseStepHandler):
    _TYPES = {"textfileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name() or context.output_df_name()
        filename = self._attr(context, "filename", "") or self._attr(context, "file", "output.txt")
        ext = self._attr(context, "extension", "")
        separator = self._attr(context, "separator", ",")
        header = self._attr(context, "header", "Y").upper() == "Y"
        encoding = self._attr(context, "encoding", "utf-8")
        compression = self._attr(context, "compression", "none")
        quote = self._attr(context, "enclosure", "") or self._attr(context, "quote", "")
        append = self._attr(context, "file_appended", "N").upper() == "Y"
        mode = "append" if append else "overwrite"

        out_var = context.output_df_name()
        params = getattr(context.transformation, "parameters", {}) or {}
        lines = [f"# Text File Output: {step.name}"]
        path = substitute_pentaho_variables(filename, params)
        if ext and not path.endswith(f".{ext}") and "." not in path.rsplit("/", 1)[-1]:
            path = f"{path}.{ext}"
        path = substitute_pentaho_variables(path, params)
        lines.append(f"{out_var} = {in_df}")
        lines.append(f"writer = {out_var}.write.format('csv')")
        lines.append(f"writer = writer.option('header', {header!r})")
        lines.append(f"writer = writer.option('sep', {separator!r})")
        lines.append(f"writer = writer.option('encoding', {encoding!r})")
        if quote:
            lines.append(f"writer = writer.option('quote', {quote!r})")
        if compression and compression.lower() not in ("none", ""):
            lines.append(f"writer = writer.option('compression', {compression!r})")
        lines.append(f"writer.mode({mode!r}).save({path!r})")
        return lines, "converted"


class ExcelOutputHandler(BaseStepHandler):
    _TYPES = {"exceloutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name() or context.output_df_name()
        filename = self._attr(context, "filename", "output.xlsx")

        lines = [f"# Excel Output: {step.name}"]
        lines.append(
            f"{in_df}.write.format('com.crealytics.spark.excel')"
            f".mode('overwrite').save({filename!r})"
        )
        return lines, "converted"