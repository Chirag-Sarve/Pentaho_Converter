"""Handlers for core Pentaho output steps (table, text/CSV, excel)."""

from __future__ import annotations

import logging

from ..generation_config import GenerationConfig
from ..lineage import substitute_pentaho_variables
from ..metadata_propagation import get_converter_metadata
from ..path_utils import (
    normalize_text_file_basename,
    spark_save_path_expr,
    spark_text_file_path_expr,
)
from ..table_names import table_write_lines
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _generation_config(context: StepContext) -> GenerationConfig:
    cfg = context.extra.get("generation_config")
    if isinstance(cfg, GenerationConfig):
        return cfg
    return GenerationConfig.defaults()


def _yn(raw: str, default: bool = False) -> bool:
    text = (raw or "").strip().upper()
    if not text:
        return default
    return text in ("Y", "YES", "TRUE", "1")


class TableOutputHandler(BaseStepHandler):
    """Table Output → Delta ``saveAsTable`` (Unity Catalog)."""

    _TYPES = {"tableoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name() or context.output_df_name()
        meta = get_converter_metadata(context)
        schema = self._attr(context, "schema", "") or str(meta.get("schema") or "")
        table = self._attr(context, "table", "") or str(meta.get("table") or "")
        truncate = _yn(self._attr(context, "truncate", "") or str(meta.get("truncate") or "N"))
        # Pentaho truncate ⇒ overwrite; otherwise append to preserve existing rows.
        mode = "overwrite" if truncate else "append"
        out_var = context.output_df_name()

        field_maps = meta.get("fields") or []
        select_cols: list[str] = []
        for item in field_maps:
            if not isinstance(item, dict):
                continue
            stream = (item.get("stream_field") or "").strip()
            target = (item.get("table_field") or stream).strip()
            if stream and target and stream != target:
                select_cols.append(f"col({stream!r}).alias({target!r})")
            elif stream:
                select_cols.append(f"col({stream!r})")

        write_df = in_df
        prep: list[str] = []
        if select_cols:
            prep.append(f"_mapped_{out_var} = {in_df}.select({', '.join(select_cols)})")
            write_df = f"_mapped_{out_var}"

        lines = table_write_lines(
            out_var=out_var,
            in_df=write_df,
            table=table,
            source_schema=schema,
            step_name=step.name,
            config=_generation_config(context),
            mode=mode,
            step_type=step.step_type or "TableOutput",
        )
        if prep:
            lines = [lines[0], *prep, *lines[1:]]
        if table and not truncate:
            lines.insert(1, "# Mode: append (Pentaho truncate=N)")
        return lines, "converted"


class TextFileOutputHandler(BaseStepHandler):
    """Text File Output and Text File Output (Legacy) → delimited CSV / plain text writer."""

    _TYPES = {"textfileoutput", "textfileoutputlegacy"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name() or context.output_df_name()
        meta = get_converter_metadata(context)
        filename = (
            self._attr(context, "filename", "")
            or self._attr(context, "file", "")
            or str(meta.get("filename") or "")
        )
        ext = self._attr(context, "extension", "") or str(meta.get("extension") or "")
        # Do not invent a delimiter — empty means missing from XML.
        separator = self._attr(context, "separator", "")
        if separator == "" and "separator" in meta:
            separator = str(meta.get("separator") or "")
        quote = (
            self._attr(context, "enclosure", "")
            or self._attr(context, "quote", "")
            or str(meta.get("enclosure") or meta.get("quote") or "")
        )
        escape = (
            self._attr(context, "escapechar", "")
            or self._attr(context, "escape", "")
            or str(meta.get("escape") or meta.get("escapechar") or "")
        )
        encoding = self._attr(context, "encoding", "") or str(meta.get("encoding") or "")
        compression = (
            self._attr(context, "compression", "")
            or self._attr(context, "file_compression", "")
            or str(meta.get("compression") or "")
        ).strip()
        header_raw = self._attr(context, "header", "")
        if header_raw == "" and "header" in meta:
            header_raw = "Y" if meta.get("header") in (True, "Y", "y", "true", "1") else (
                "N" if meta.get("header") in (False, "N", "n", "false", "0") else ""
            )
        has_header_meta = header_raw != ""
        header = _yn(header_raw, default=True) if has_header_meta else True

        append = _yn(
            self._attr(context, "file_appended", "")
            or self._attr(context, "append", "")
            or str(meta.get("append") or "N")
        )
        mode = "append" if append else "overwrite"
        out_var = context.output_df_name()
        params = getattr(context.transformation, "parameters", {}) or {}
        status = "converted"

        file_type = (
            self._attr(context, "file_type", "")
            or self._attr(context, "filetype", "")
            or str(meta.get("file_type") or meta.get("filetype") or "")
        ).strip().upper()
        padded = _yn(
            self._attr(context, "padded", "")
            or str(meta.get("padded") or "N")
        )
        fast_dump = _yn(
            self._attr(context, "fast_dump", "")
            or str(meta.get("fast_dump") or "N")
        )
        file_as_command = _yn(
            self._attr(context, "file_is_command", "")
            or self._attr(context, "is_command", "")
            or str(meta.get("file_as_command") or "N")
        )
        create_parent = _yn(
            self._attr(context, "create_parent_folder", "")
            or str(meta.get("create_parent_folder") or "N")
        )
        file_name_in_field = _yn(
            self._attr(context, "fileNameInField", "")
            or str(meta.get("file_name_in_field") or "N")
        )
        add_date = _yn(str(meta.get("add_date") or "N"))
        add_time = _yn(str(meta.get("add_time") or "N"))
        split_every = str(meta.get("split_every") or meta.get("splitevery") or "").strip()
        footer = _yn(str(meta.get("footer") or "N"))

        missing_name = not (filename or "").strip()
        path = substitute_pentaho_variables(filename, params) if filename else ""
        if path:
            path = substitute_pentaho_variables(path, params)
        original_pentaho_path = path
        path = normalize_text_file_basename(path, ext)
        path_expr = spark_text_file_path_expr(
            path or original_pentaho_path, placeholder="<output_name>"
        )

        output_fields = meta.get("output_fields") or meta.get("fields") or []
        field_names: list[str] = []
        for item in output_fields:
            if isinstance(item, dict):
                name = (item.get("name") or item.get("stream_field") or "").strip()
                if name:
                    field_names.append(name)
            elif isinstance(item, str) and item.strip():
                field_names.append(item.strip())

        step_label = step.step_type or "TextFileOutput"
        lines = [
            f"# Pentaho step: {step.name} (type: {step_label})",
        ]
        logger.info("Converting Text File Output '%s' (type=%s)", step.name, step_label)
        if missing_name:
            lines.append("# WARNING: Output filename missing from Pentaho metadata.")
            logger.warning("Text File Output '%s': filename missing from metadata", step.name)
            status = "partial"
        else:
            if original_pentaho_path:
                lines.append(f"# Pentaho filename: {original_pentaho_path}")
            lines.append(
                "# NOTE: Spark CSV/text writers create a directory at this path "
                "(not a single flat file); subsequent Text File Input must load the same path"
            )
            lines.append(
                "# NOTE: empty input DataFrames write zero data files; "
                "missing parent paths are usually created by the filesystem"
            )

        if file_as_command:
            lines.append(
                "# WARNING: TextFileOutputLegacy 'Run as command' (is_command) is not "
                "supported on Databricks — writing to a normal file path instead. "
                "Reimplement the shell pipe outside Spark if required."
            )
            status = "partial"

        if file_name_in_field:
            fname_field = str(meta.get("file_name_field") or "")
            lines.append(
                f"# WARNING: preserved unsupported option: fileNameInField "
                f"(field={fname_field!r}) — Spark writes a single destination path"
            )
            status = "partial"

        if add_date or add_time or meta.get("specify_format") or meta.get("date_time_format"):
            lines.append(
                "# WARNING: preserved date/time file-naming options "
                f"(add_date={add_date}, add_time={add_time}, "
                f"format={meta.get('date_time_format')!r}) — "
                "append timestamps in the path expression manually if required"
            )
            status = "partial"

        if split_every:
            lines.append(
                f"# INFO: preserved split_every={split_every!r} — "
                "use repartition/coalesce to control output file count"
            )

        if footer:
            lines.append(
                "# WARNING: Text File Output footer lines are not written by Spark CSV writer"
            )
            status = "partial"

        ended_line = str(meta.get("ended_line") or "").strip()
        if ended_line:
            lines.append(
                f"# INFO: preserved endedLine={ended_line!r} — "
                "Spark CSV writer does not append a custom end-of-file line"
            )

        if meta.get("servlet_output"):
            lines.append(
                "# INFO: preserved servlet_output — not applicable on Databricks file writers"
            )

        if meta.get("do_not_open_new_file_init"):
            lines.append(
                "# INFO: preserved do_not_open_new_file_init — Spark opens the writer at save()"
            )

        if create_parent:
            lines.append(
                "# INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders"
            )

        # Preserve per-field formatting metadata that Spark CSV cannot express 1:1.
        for item in output_fields:
            if not isinstance(item, dict):
                continue
            extras = {
                k: item.get(k)
                for k in ("format", "nullif", "currency", "decimal", "group", "trim_type", "length", "precision")
                if item.get(k)
            }
            if extras:
                lines.append(
                    f"# INFO: preserved.field_format name={item.get('name')!r} options={extras!r}"
                )

        lines.append(f"{out_var} = {in_df}")

        is_fixed = file_type in ("FIXED", "FIXEDWIDTH") or (
            padded and not separator and len(field_names) > 0
        )
        # Plain text: one configured field and no delimiter (or fast dump).
        is_plain_text = (
            not is_fixed
            and len(field_names) == 1
            and (fast_dump or not separator)
        )
        # Delimited CSV/TSV/pipe when a separator is present or multi-field / no field list.
        is_delimited = not is_fixed and not is_plain_text

        if is_fixed:
            lines.append("# TODO: Fixed-width output requires manual implementation.")
            lines.append(
                "# WARNING: Spark has no native fixed-width writer — "
                "emitting closest delimited CSV approximation for review."
            )
            status = "partial"
            is_delimited = True

        if is_plain_text:
            col_name = field_names[0]
            lines.append(
                f"{out_var}.select({col_name!r}) \\\n"
                f"    .write \\\n"
                f"    .mode({mode!r}) \\\n"
                f"    .text(\n"
                f"        {path_expr}\n"
                f"    )"
            )
            if status == "partial":
                logger.warning(
                    "Text File Output '%s' (type=%s) migrated partially (plain text)",
                    step.name,
                    step_label,
                )
            else:
                logger.info(
                    "Text File Output '%s' (type=%s) migrated as plain text writer",
                    step.name,
                    step_label,
                )
            return lines, status

        # --- Delimited CSV / TSV / pipe writer ---
        if not separator:
            lines.append("# WARNING: Output delimiter missing from Pentaho metadata.")
            status = "partial"
        if not encoding:
            lines.append("# INFO: Output encoding missing from Pentaho metadata.")
        # Missing header meta: default header=True is still executable Spark — stay CONVERTED.

        write_df = out_var
        if field_names:
            cols = ", ".join(repr(c) for c in field_names)
            lines.append(f"selected_output_df = {out_var}.select({cols})")
            write_df = "selected_output_df"

        lines.append("(")
        lines.append(f"    {write_df}.write")
        lines.append(f"    .mode({mode!r})")
        lines.append(f"    .option(\"header\", {header})")
        if separator:
            lines.append(f"    .option(\"sep\", {separator!r})")
        if quote:
            lines.append(f"    .option(\"quote\", {quote!r})")
        if escape:
            lines.append(f"    .option(\"escape\", {escape!r})")
        if encoding:
            lines.append(f"    .option(\"encoding\", {encoding!r})")
        if compression and compression.lower() not in ("none", "empty", ""):
            spark_comp = compression.lower().replace(" ", "")
            if spark_comp == "zip":
                lines.append(
                    "# INFO: Pentaho Zip compression mapped to gzip for Spark CSV writer"
                )
                spark_comp = "gzip"
            lines.append(f"    .option(\"compression\", {spark_comp!r})")
        if meta.get("enclosure_forced"):
            lines.append(
                "# INFO: enclosure_forced has no direct Spark CSV equivalent"
            )
        lines.append(f"    .csv({path_expr})")
        lines.append(")")
        if status == "partial":
            logger.warning(
                "Text File Output '%s' (type=%s) migrated partially — review generated WARNINGs",
                step.name,
                step_label,
            )
        else:
            logger.info(
                "Text File Output '%s' (type=%s) migrated successfully",
                step.name,
                step_label,
            )
        return lines, status


def generate_excel_workbook_write(
    context: StepContext,
    *,
    label: str,
    default_extension: str = "xls",
    attr_fn=None,
) -> tuple[list[str], str]:
    """Shared Microsoft Excel Output / Writer → spark-excel code generation.

    Used by both the deprecated Excel Output step and Excel Writer so write logic
    is not duplicated. Formatting, templates, and sheet protection remain PARTIAL.
    """
    _get = attr_fn or (lambda ctx, key, default="": "")
    step = context.step
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    meta = get_converter_metadata(context)
    params = context.transformation.parameters or {}
    status = "converted"

    raw_filename = (
        _get(context, "filename", "")
        or _get(context, "file", "")
        or str(meta.get("filename") or "")
    )
    filename = substitute_pentaho_variables(raw_filename, params)
    extension = (
        _get(context, "extension", "")
        or str(meta.get("extension") or default_extension)
        or default_extension
    ).lstrip(".")
    if filename and extension and not filename.lower().endswith(f".{extension.lower()}"):
        filename = f"{filename}.{extension}"

    raw_sheet = (
        _get(context, "sheetname", "")
        or _get(context, "sheet_name", "")
        or str(meta.get("sheetname") or "Sheet1")
        or "Sheet1"
    )
    sheet = substitute_pentaho_variables(raw_sheet, params) or "Sheet1"
    starting_cell = (
        _get(context, "startingCell", "")
        or _get(context, "starting_cell", "")
        or str(meta.get("starting_cell") or "A1")
        or "A1"
    )

    header_raw = _get(context, "header", "") or str(meta.get("header") or "Y")
    header = _yn(header_raw, default=True)
    append = _yn(_get(context, "append", "") or str(meta.get("append") or "N"))
    encoding = _get(context, "encoding", "") or str(meta.get("encoding") or "")
    null_is_blank = _yn(
        _get(context, "nullisblank", "") or str(meta.get("nullisblank") or "N")
    )
    auto_size = _yn(
        _get(context, "autosizecolums", "")
        or _get(context, "autosizecolumns", "")
        or str(meta.get("autosizecolums") or meta.get("autosizecolumns") or "N")
    )
    footer = _yn(_get(context, "footer", "") or str(meta.get("footer") or "N"))
    protect = _yn(
        _get(context, "protect_sheet", "") or str(meta.get("protect_sheet") or "N")
    )
    password = str(meta.get("password") or _get(context, "password", "") or "")
    template_enabled = _yn(str(meta.get("template_enabled") or "N"))
    template_append = _yn(str(meta.get("template_append") or "N"))
    template_filename = str(meta.get("template_filename") or meta.get("template") or "")
    use_temp = _yn(str(meta.get("usetempfiles") or "N"))
    compression = str(meta.get("compression") or "")
    add_date = _yn(str(meta.get("add_date") or "N"))
    add_time = _yn(str(meta.get("add_time") or "N"))
    split_flag = _yn(str(meta.get("split") or "N"))
    split_every = str(meta.get("splitevery") or "0")
    create_parent = _yn(str(meta.get("create_parent_folder") or "N"))
    add_to_result = _yn(str(meta.get("add_to_result_filenames") or "N"))
    do_not_open_init = _yn(str(meta.get("do_not_open_newfile_init") or "N"))
    streaming = _yn(
        _get(context, "sstream", "")
        or _get(context, "streaming", "")
        or str(meta.get("streaming") or "N")
    )
    if_file_exists = str(meta.get("if_file_exists") or "")
    custom = meta.get("custom") if isinstance(meta.get("custom"), dict) else {}

    fields = meta.get("fields") if isinstance(meta.get("fields"), list) else []
    field_names = [
        str(f.get("name"))
        for f in fields
        if isinstance(f, dict) and f.get("name")
    ]

    lines = [
        f"# {label}: {step.name}",
        "# Requires spark-excel (com.crealytics.spark.excel) on the cluster classpath.",
    ]

    if not filename:
        lines.append("# WARNING: Output workbook/filename missing from Pentaho metadata.")
        status = "partial"
    if not in_df:
        lines.append("# WARNING: Empty Excel output — no upstream DataFrame; write skipped.")
        lines.append(
            f"{out_var} = spark.range(0).select(lit(1).alias('_excel_output')).limit(0)"
        )
        logger.warning("%s '%s' has no input DataFrame", label, step.name)
        return lines, "partial"

    path_expr = spark_save_path_expr(filename or "", placeholder=f"<workbook.{extension}>")
    mode = "append" if append else "overwrite"
    data_address = f"'{sheet}'!{starting_cell}"

    write_df = in_df
    if field_names:
        cols = ", ".join(repr(c) for c in field_names)
        lines.append(f"_excel_{out_var} = {in_df}.select({cols})")
        write_df = f"_excel_{out_var}"
        lines.append(
            "# NOTE: Field ordering follows Pentaho Excel field list; "
            "runtime schema mismatch will fail the select."
        )

    lines.append(f"{out_var} = {in_df}")
    lines.append("(")
    lines.append(f"    {write_df}.write.format('com.crealytics.spark.excel')")
    lines.append(f"    .option('dataAddress', {data_address!r})")
    lines.append(f"    .option('header', {str(header).lower()!r})")
    if null_is_blank:
        lines.append("    .option('treatEmptyValuesAsNulls', 'false')")
    lines.append(f"    .mode({mode!r})")
    lines.append(f"    .save({path_expr})")
    lines.append(")")

    # ---- Preserve every Pentaho property Spark cannot express 1:1 ----
    lines.append(f"# preserved.filename={filename!r}")
    lines.append(f"# preserved.sheetname={sheet!r}")
    lines.append(f"# preserved.extension={extension!r}")
    lines.append(f"# preserved.header={header_raw!r}")
    lines.append(f"# preserved.append={('Y' if append else 'N')!r}")
    lines.append(f"# preserved.starting_cell={starting_cell!r}")
    if encoding:
        lines.append(f"# preserved.encoding={encoding!r}")
        lines.append(
            "# NOTE: Excel charset is library-managed; encoding preserved for documentation."
        )
    if footer:
        lines.append("# preserved.footer='Y'")
        lines.append("# WARNING: Excel footer rows are not supported by spark-excel write.")
        status = "partial"
    if auto_size:
        lines.append("# preserved.autosizecolums='Y'")
        lines.append("# WARNING: Auto-size columns not mapped to spark-excel options.")
        status = "partial"
    if protect:
        lines.append("# preserved.protect_sheet='Y'")
        lines.append(
            f"# preserved.password_set={bool(password)!r}  # value redacted"
        )
        lines.append("# WARNING: Sheet password protection has no Databricks equivalent.")
        status = "partial"
    elif password:
        lines.append("# preserved.password_set=True  # value redacted")
        status = "partial"
    if template_enabled or template_filename:
        lines.append(f"# preserved.template_enabled={('Y' if template_enabled else 'N')!r}")
        lines.append(f"# preserved.template_append={('Y' if template_append else 'N')!r}")
        if template_filename:
            lines.append(
                f"# preserved.template_filename="
                f"{substitute_pentaho_variables(template_filename, params)!r}"
            )
        lines.append("# WARNING: Excel templates are not applied by spark-excel write.")
        status = "partial"
    if use_temp:
        lines.append("# preserved.usetempfiles='Y'")
        if meta.get("tempdirectory"):
            lines.append(f"# preserved.tempdirectory={meta.get('tempdirectory')!r}")
    if compression:
        lines.append(f"# preserved.compression={compression!r}")
        lines.append(
            "# WARNING: Excel compression/archive flags are not supported on spark-excel."
        )
        status = "partial"
    if add_date or add_time or meta.get("SpecifyFormat") in ("Y", "y", True):
        lines.append(
            f"# preserved.file_naming add_date={add_date} add_time={add_time} "
            f"date_time_format={meta.get('date_time_format')!r}"
        )
        lines.append(
            "# WARNING: Dynamic date/time file naming must be applied before .save()."
        )
        status = "partial"
    if split_flag or (split_every and split_every not in ("0", "")):
        lines.append(f"# preserved.split={('Y' if split_flag else 'N')!r}")
        lines.append(f"# preserved.splitevery={split_every!r}")
        lines.append("# WARNING: Split-every-N-rows files not emitted by Spark Excel writer.")
        status = "partial"
    if create_parent:
        lines.append("# NOTE: create_parent_folder=Y — object stores typically create parents.")
    if add_to_result:
        lines.append("# preserved.add_to_result_filenames='Y'")
        lines.append(
            "# WARNING: Add filenames to result has no Carte/result-file equivalent on Databricks."
        )
        status = "partial"
    if do_not_open_init:
        lines.append("# preserved.do_not_open_newfile_init='Y'")
        lines.append("# WARNING: do_not_open_newfile_init — Spark opens the writer at save().")
        status = "partial"
    if streaming:
        lines.append("# preserved.streaming='Y'")
        lines.append("# NOTE: Streaming workbook writes depend on spark-excel / xlsx library support.")
        status = "partial"
    if if_file_exists:
        lines.append(f"# preserved.if_file_exists={if_file_exists!r}")
        status = "partial"
    if custom:
        lines.append(f"# preserved.formatting={custom!r}")
        status = "partial"
    for item in fields:
        if not isinstance(item, dict):
            continue
        extras = {
            k: item.get(k)
            for k in ("type", "format", "nullif", "currency", "decimal", "group", "length", "precision")
            if item.get(k)
        }
        if extras.get("format") or (extras.get("type") and extras["type"] != "String") or len(extras) > 1:
            lines.append(
                f"# preserved.field_format name={item.get('name')!r} options={extras!r}"
            )
            status = "partial"

    if status == "partial":
        lines.insert(
            2,
            "# PARTIAL: fonts, templates, sheet protection, and per-cell formats "
            "have no 1:1 Spark mapping.",
        )
        logger.warning(
            "%s '%s' migrated partially (formatting/template limitations)",
            label,
            step.name,
        )
    else:
        logger.info("%s '%s' migrated via spark-excel writer", label, step.name)

    return lines, status


class ExcelOutputHandler(BaseStepHandler):
    """Microsoft Excel Output (Deprecated) → spark-excel write with preserved metadata."""

    _TYPES = {"exceloutput", "microsoftexceloutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return generate_excel_workbook_write(
            context,
            label="Microsoft Excel Output (Deprecated)",
            default_extension="xls",
            attr_fn=self._attr,
        )
