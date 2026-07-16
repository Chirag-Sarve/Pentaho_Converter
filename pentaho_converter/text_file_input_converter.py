"""Convert Pentaho Text File Input step metadata to PySpark file readers."""

from __future__ import annotations

import logging
from typing import Any

from .lineage import substitute_pentaho_variables
from .path_utils import normalize_text_file_basename, spark_text_file_path_expr
from .schema_utils import fields_to_schema_ddl
from .step_context import StepContext

logger = logging.getLogger(__name__)


def _attrs(metadata: dict[str, Any]) -> dict[str, Any]:
    attrs = metadata.get("attributes")
    return attrs if isinstance(attrs, dict) else {}


def _meta_value(metadata: dict[str, Any], *keys: str, default: str = "") -> str:
    """Read a scalar from propagated metadata (top-level or attributes)."""
    attrs = _attrs(metadata)
    for key in keys:
        for source in (metadata, attrs):
            if key not in source:
                continue
            val = source[key]
            if val is None:
                continue
            text = str(val)
            # Preserve whitespace-only separators (e.g. tab).
            if text.strip() == "" and text != "":
                return text
            text = text.strip()
            if text:
                return text
        prefixed = attrs.get(f"file_{key}")
        if prefixed is not None:
            text = str(prefixed)
            if text.strip() == "" and text != "":
                return text
            text = text.strip()
            if text:
                return text
    return default


def _meta_present(metadata: dict[str, Any], *keys: str) -> bool:
    """True when a key exists with a non-empty value (does not invent defaults)."""
    attrs = _attrs(metadata)
    for key in keys:
        for source in (metadata, attrs):
            if key not in source:
                continue
            val = source[key]
            if val is None:
                continue
            text = str(val)
            if text.strip() == "" and text != "":
                return True
            if text.strip():
                return True
    return False


def _bool_setting(metadata: dict[str, Any], *keys: str, default: bool = False) -> bool:
    raw = _meta_value(metadata, *keys, default="")
    if isinstance(raw, bool):
        return raw
    if not raw:
        return default
    return raw.strip().upper() in ("Y", "YES", "TRUE", "1", "T")


def _int_setting(metadata: dict[str, Any], *keys: str, default: int = 0) -> int:
    raw = _meta_value(metadata, *keys, default="")
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _fields_from_metadata(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for item in metadata.get("fields") or []:
        if isinstance(item, dict) and item.get("name"):
            fields.append(item)
    return fields


def _sanitize_path(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    if text.startswith("<"):
        return ""
    if text.startswith("{") and ("'line'" in text or '"line"' in text):
        return ""
    return text


def _file_path(metadata: dict[str, Any]) -> str:
    params = metadata.get("transformation_parameters") or {}
    raw = _sanitize_path(
        _meta_value(metadata, "filename", "file", "file_path", "name", default="")
    )
    if not raw:
        return ""
    path = substitute_pentaho_variables(raw, params)
    # Match Text File Output basename rules (append extension when name has no suffix).
    ext = _meta_value(metadata, "extension", "extention", default="")
    return normalize_text_file_basename(path, ext)


def _path_expr(path: str) -> str:
    """Databricks-compatible path expression; never emits Windows local paths."""
    return spark_text_file_path_expr(path, placeholder="<input_file>")


def _file_type(metadata: dict[str, Any]) -> str:
    return _meta_value(metadata, "file_type", "filetype", "type", default="").upper()


def _reader_mode(metadata: dict[str, Any]) -> str:
    file_type = _file_type(metadata)
    separator = _meta_value(metadata, "separator", default="")

    if file_type in ("FIXED", "FIXEDWIDTH"):
        return "fixed"
    if file_type == "CSV" or separator:
        return "csv"

    path = _file_path(metadata).lower()
    if path.endswith((".csv", ".tsv", ".psv")):
        return "csv"
    return "text"


def _spark_compression(metadata: dict[str, Any]) -> str:
    raw = _meta_value(metadata, "file_compression", "compression", default="")
    if not raw:
        return ""
    normalized = raw.strip().lower().replace(" ", "")
    mapping = {
        "none": "",
        "gzip": "gzip",
        "bzip2": "bzip2",
        "snappy": "snappy",
        "lzo": "lzo",
        "deflate": "deflate",
    }
    return mapping.get(normalized, normalized)


def _spark_cast_type(type_name: str) -> str:
    from .schema_utils import spark_cast_type

    return spark_cast_type(type_name)


def _spark_schema_ddl(fields: list[dict[str, Any]]) -> str | None:
    return fields_to_schema_ddl(fields)


def _generate_csv_read(
    lines: list[str],
    out_var: str,
    path_expr: str,
    metadata: dict[str, Any],
    fields: list[dict[str, Any]],
    *,
    missing_delimiter: bool,
    missing_encoding: bool,
    missing_schema: bool,
) -> None:
    header = _bool_setting(metadata, "header", default=False)
    # Only emit sep when known; if missing, Spark default applies after a WARNING.
    separator = _meta_value(metadata, "separator", default="")
    quote = _meta_value(metadata, "enclosure", "quote", default="")
    escape = _meta_value(metadata, "escapechar", "escape", default="")
    encoding = _meta_value(metadata, "encoding", default="")
    compression = _spark_compression(metadata)
    schema_ddl = _spark_schema_ddl(fields)
    comment = _meta_value(metadata, "comment", default="")

    lines.append(f"{out_var} = (")
    lines.append("    spark.read")
    lines.append(f"    .option(\"header\", {header})")
    if separator:
        lines.append(f"    .option(\"sep\", {separator!r})")
    elif missing_delimiter:
        pass  # WARNING already emitted by convert_text_file_input_step
    if quote:
        lines.append(f"    .option(\"quote\", {quote!r})")
    if escape:
        lines.append(f"    .option(\"escape\", {escape!r})")
    if encoding:
        lines.append(f"    .option(\"encoding\", {encoding!r})")
    elif missing_encoding:
        lines.append("    # INFO: Input encoding missing from Pentaho metadata.")
    if compression:
        lines.append(f"    .option(\"compression\", {compression!r})")
    if comment:
        lines.append(f"    .option(\"comment\", {comment!r})")

    null_values = sorted(
        {
            str(field.get("null_if") or field.get("nullif") or "").strip()
            for field in fields
            if str(field.get("null_if") or field.get("nullif") or "").strip()
        }
    )
    if null_values:
        lines.append(f"    .option(\"nullValue\", {null_values[0]!r})")
        if len(null_values) > 1:
            lines.append(
                f"    # INFO: additional Pentaho null sentinels not mapped: {null_values[1:]!r}"
            )

    if _bool_setting(metadata, "enclosure_breaks", "lazy_quotes", default=False):
        lines.append('    .option("multiLine", "true")')

    if _bool_setting(metadata, "error_ignored", "skip_bad_files", default=False):
        lines.append('    .option("mode", "PERMISSIVE")')
        lines.append(
            "    # INFO: corrupted/invalid rows kept as nulls (Pentaho error_ignored/skip_bad_files)"
        )

    if schema_ddl:
        lines.append('    .option("inferSchema", False)')
        lines.append(f"    .schema({schema_ddl!r})")
    else:
        if missing_schema:
            lines.append("    # INFO: Schema unavailable — using inferSchema.")
        lines.append(f'    .option("inferSchema", {header})')

    lines.append(f"    .csv({path_expr})")
    lines.append(")")


def _generate_text_read(
    lines: list[str],
    out_var: str,
    path_expr: str,
    metadata: dict[str, Any],
    *,
    missing_encoding: bool,
) -> None:
    encoding = _meta_value(metadata, "encoding", default="")
    compression = _spark_compression(metadata)
    if encoding or compression or missing_encoding:
        lines.append(f"{out_var} = (")
        lines.append("    spark.read.format(\"text\")")
        if encoding:
            lines.append(f"    .option(\"encoding\", {encoding!r})")
        elif missing_encoding:
            lines.append("    # INFO: Input encoding missing from Pentaho metadata.")
        if compression:
            lines.append(f"    .option(\"compression\", {compression!r})")
        lines.append(f"    .load({path_expr})")
        lines.append(")")
    else:
        lines.append(f"{out_var} = spark.read.text({path_expr})")


def _generate_fixed_read(
    lines: list[str],
    out_var: str,
    path_expr: str,
    metadata: dict[str, Any],
    fields: list[dict[str, Any]],
    *,
    missing_encoding: bool,
) -> None:
    raw_var = f"_tfi_raw_{out_var}"
    if not fields:
        lines.append("# TODO: Fixed-width input requires field lengths from Pentaho metadata.")
        lines.append("# WARNING: Schema unavailable. Manual review required.")
    _generate_text_read(
        lines, raw_var, path_expr, metadata, missing_encoding=missing_encoding
    )

    select_exprs: list[str] = []
    position = 1
    for field in fields:
        name = field.get("name")
        if not name:
            continue
        try:
            length = int(field.get("length") or 0)
        except (TypeError, ValueError):
            length = 0
        if length > 0:
            select_exprs.append(
                f'substring(col("value"), {position}, {length}).alias({name!r})'
            )
            position += length
        else:
            select_exprs.append(f'col("value").alias({name!r})')
            lines.append(
                f"# WARNING: Fixed-width field {name!r} missing length — using full line."
            )

    if select_exprs:
        lines.append(f"{out_var} = {raw_var}.select({', '.join(select_exprs)})")
    else:
        lines.append(f"{out_var} = {raw_var}")


def _apply_field_defaults(
    lines: list[str],
    out_var: str,
    fields: list[dict[str, Any]],
) -> None:
    casts: list[str] = []
    for field in fields:
        name = field.get("name")
        if not name:
            continue
        expr = f"col({name!r})"
        null_if = str(field.get("null_if") or field.get("nullif") or "").strip()
        default = field.get("default")
        if default is None:
            default = field.get("ifnull")
        if null_if:
            expr = f"when({expr} == lit({null_if!r}), lit(None)).otherwise({expr})"
        cast_type = _spark_cast_type(field.get("type", "String"))
        if cast_type != "string":
            expr = f"{expr}.cast({cast_type!r})"
        if default is not None and str(default).strip() != "":
            expr = f"coalesce({expr}, lit({str(default)!r}))"
        casts.append(f"{expr}.alias({name!r})")

    if casts:
        lines.append(f"{out_var} = {out_var}.select({', '.join(casts)})")


def _apply_skip_header_lines(
    lines: list[str],
    out_var: str,
    metadata: dict[str, Any],
    mode: str,
) -> None:
    attrs = _attrs(metadata)
    has_header = _bool_setting(metadata, "header", default=False)
    nr_header = _int_setting(metadata, "nr_headerlines", default=1)
    skip_lines = _int_setting(metadata, "nr_lines_doc_header", default=0)
    nr_header_explicit = "nr_headerlines" in metadata or "nr_headerlines" in attrs

    extra_skip = skip_lines
    if has_header and nr_header > 1:
        extra_skip += nr_header - 1
    elif not has_header and nr_header_explicit and nr_header > 0:
        extra_skip += nr_header

    if extra_skip <= 0:
        return

    if mode == "csv" and has_header and nr_header <= 1 and skip_lines == 0:
        return

    lines.append(
        f"# Pentaho skip lines: header={has_header!r}, "
        f"nr_headerlines={nr_header}, doc_header={skip_lines}"
    )
    lines.append("_tfi_skip_w = Window.orderBy(monotonically_increasing_id())")
    lines.append(
        f"{out_var} = {out_var}.withColumn('_tfi_skip_rn', row_number().over(_tfi_skip_w))"
    )
    lines.append(
        f"{out_var} = {out_var}.filter(col('_tfi_skip_rn') > {extra_skip}).drop('_tfi_skip_rn')"
    )


def _apply_footer_lines(lines: list[str], out_var: str, metadata: dict[str, Any]) -> None:
    if not _bool_setting(metadata, "footer", default=False):
        return
    nr_footer = _int_setting(metadata, "nr_footerlines", default=1)
    if nr_footer <= 0:
        return

    lines.append(f"# Pentaho footer: exclude last {nr_footer} row(s)")
    lines.append("_tfi_footer_w = Window.orderBy(monotonically_increasing_id())")
    lines.append(
        f"{out_var} = {out_var}.withColumn('_tfi_footer_rn', row_number().over(_tfi_footer_w))"
    )
    lines.append(
        f"_tfi_footer_max = {out_var}.agg(_max(col('_tfi_footer_rn'))).collect()[0][0]"
    )
    lines.append(
        f"{out_var} = {out_var}.filter(col('_tfi_footer_rn') <= _tfi_footer_max - {nr_footer})"
        ".drop('_tfi_footer_rn')"
    )


def _apply_row_limit(lines: list[str], out_var: str, metadata: dict[str, Any]) -> None:
    row_limit = _int_setting(metadata, "limit", "rowLimit", "row_limit", default=0)
    if row_limit > 0:
        lines.append(f"{out_var} = {out_var}.limit({row_limit})")


def _emit_legacy_input_preservation(
    lines: list[str],
    metadata: dict[str, Any],
    status: str,
) -> str:
    """Preserve Legacy Text File Input options as INFO / WARNING comments.

    Informational metadata (locale, storage fields, formatting) never downgrades
    status. Only genuine unsupported runtime behaviors mark the step PARTIAL.
    """
    info_notes: list[str] = []
    runtime_gaps: list[str] = []

    if _bool_setting(metadata, "accept_filenames", default=False):
        accept_field = _meta_value(metadata, "accept_field", default="")
        accept_step = _meta_value(metadata, "accept_stepname", default="")
        runtime_gaps.append(
            f"accept_filenames (field={accept_field!r}, step={accept_step!r}) — "
            "resolve paths upstream before spark.read"
        )

    if _bool_setting(metadata, "passing_through_fields", default=False):
        runtime_gaps.append(
            "passing_through_fields — join upstream fields after the read if required"
        )

    if _bool_setting(metadata, "line_wrapped", default=False):
        runtime_gaps.append(
            f"line_wrapped (nr_wraps={_meta_value(metadata, 'nr_wraps', default='')!r})"
        )

    if _bool_setting(metadata, "layout_paged", default=False):
        runtime_gaps.append(
            "layout_paged "
            f"(nr_lines_per_page={_meta_value(metadata, 'nr_lines_per_page', default='')!r})"
        )

    filters = metadata.get("filters") or []
    if filters:
        runtime_gaps.append(f"filters={filters!r} — apply DataFrame filters manually if needed")

    filemask = _meta_value(metadata, "filemask", default="")
    exclude_mask = _meta_value(metadata, "exclude_filemask", default="")
    if filemask or exclude_mask:
        info_notes.append(
            f"file masks (filemask={filemask!r}, exclude={exclude_mask!r}) — "
            "expand globs in the load path or via an upstream Get File Names step"
        )

    if _bool_setting(metadata, "include_subfolders", default=False):
        info_notes.append("include_subfolders — use recursive path globs (e.g. path/**/*.csv)")

    entries = metadata.get("file_entries") or []
    if isinstance(entries, list) and len(entries) > 1:
        extra = [
            e.get("name") for e in entries[1:]
            if isinstance(e, dict) and e.get("name")
        ]
        if extra:
            runtime_gaps.append(
                f"additional file paths={extra!r} — Spark load path uses the first file; "
                "union additional reads if required"
            )

    error_dirs = {
        "bad_line_files_destination_directory": _meta_value(
            metadata, "bad_line_files_destination_directory", default=""
        ),
        "error_line_files_destination_directory": _meta_value(
            metadata, "error_line_files_destination_directory", default=""
        ),
        "line_number_files_destination_directory": _meta_value(
            metadata, "line_number_files_destination_directory", default=""
        ),
    }
    active_error_dirs = {k: v for k, v in error_dirs.items() if v}
    if active_error_dirs:
        info_notes.append(
            "error-handling side files preserved "
            f"(dirs={active_error_dirs!r})"
        )
    elif _bool_setting(metadata, "error_ignored", "skip_bad_files", default=False):
        info_notes.append(
            "error_ignored/skip_bad_files — PERMISSIVE mode applied when configured"
        )

    for key in (
        "shortFileFieldName",
        "pathFieldName",
        "hiddenFieldName",
        "lastModificationTimeFieldName",
        "uriNameFieldName",
        "rootUriNameFieldName",
        "extensionFieldName",
        "sizeFieldName",
    ):
        val = _meta_value(metadata, key, default="")
        if val:
            info_notes.append(f"{key}={val!r} (file metadata field not mapped)")

    locale = _meta_value(metadata, "date_format_locale", default="")
    if locale:
        info_notes.append(f"date_format_locale={locale!r}")

    if _meta_present(metadata, "date_format_lenient"):
        info_notes.append(
            f"date_format_lenient={_meta_value(metadata, 'date_format_lenient', default='')!r}"
        )

    for note in info_notes:
        lines.append(f"# INFO: preserved Legacy Text File Input option: {note}")
        logger.info("Legacy Text File Input option preserved (info): %s", note)

    for note in runtime_gaps:
        lines.append(f"# WARNING: preserved unsupported Legacy Text File Input option: {note}")
        logger.warning("Legacy Text File Input unsupported option preserved: %s", note)
        status = "partial"

    return status


def _preserve_input_field_formats(
    lines: list[str],
    fields: list[dict[str, Any]],
) -> None:
    """Emit preserve comments for per-field formats Spark schema DDL cannot express."""
    for field in fields:
        name = field.get("name")
        if not name:
            continue
        extras = {
            k: field.get(k)
            for k in (
                "format",
                "currency",
                "decimal",
                "group",
                "trim_type",
                "precision",
                "position",
                "repeat",
            )
            if field.get(k) not in (None, "", 0, "0", "none", "Neither")
        }
        if extras:
            lines.append(f"# INFO: preserved.field_format name={name!r} options={extras!r}")


def _apply_include_filename_and_rownum(
    lines: list[str],
    out_var: str,
    metadata: dict[str, Any],
) -> None:
    if _bool_setting(metadata, "include", default=False):
        field = _meta_value(metadata, "include_field", default="") or "filename"
        lines.append(
            f"{out_var} = {out_var}.withColumn({field!r}, expr('input_file_name()'))"
        )
    if _bool_setting(metadata, "rownum", default=False):
        field = _meta_value(metadata, "rownum_field", default="") or "rownum"
        lines.append(
            f"{out_var} = {out_var}.withColumn("
            f"{field!r}, monotonically_increasing_id())"
        )


def convert_text_file_input_step(
    metadata: dict[str, Any],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate PySpark lines for a Text File Input step from propagated metadata."""
    step_type = "TextFileInput"
    if context is not None and getattr(context, "step", None) is not None:
        step_type = context.step.step_type or step_type
    lines = [
        f"# Pentaho step: {step_name} (type: {step_type})",
    ]
    status = "converted"
    logger.info("Converting Text File Input '%s' (type=%s)", step_name, step_type)

    buffer_size = _meta_value(metadata, "buffer_size", "buffersize", default="")
    if buffer_size:
        lines.append(f"# Pentaho buffer_size={buffer_size!r} (no Spark CSV/text reader equivalent)")

    if _bool_setting(metadata, "lazy_conversion", "lazyConversionActive", "lazy_conversion_active", default=False):
        lines.append(
            "# Pentaho lazy conversion enabled (applied at Pentaho runtime, not in Spark reader)"
        )

    status = _emit_legacy_input_preservation(lines, metadata, status)

    # Preserve original Pentaho path as metadata before basename normalization.
    params = metadata.get("transformation_parameters") or {}
    original_pentaho_path = substitute_pentaho_variables(
        _sanitize_path(
            _meta_value(metadata, "filename", "file", "file_path", "name", default="")
        ),
        params,
    )
    path = _file_path(metadata)
    # Combine directory + filemask when the primary path looks like a folder pattern.
    filemask = _meta_value(metadata, "filemask", default="")
    if path and filemask and "*" not in path and "?" not in path:
        # Prefer leaving path as-is when it already points at a file; only annotate.
        lines.append(
            f"# NOTE: Pentaho filemask={filemask!r} — confirm Spark load path covers matching files"
        )

    path_expr = _path_expr(path)
    if not path:
        lines.append("# WARNING: Input filename missing from Pentaho metadata.")
        logger.warning("Text File Input '%s': filename missing from metadata", step_name)
        status = "partial"
    else:
        if original_pentaho_path:
            lines.append(f"# Pentaho filename: {original_pentaho_path}")
        lines.append(
            "# NOTE: Spark CSV outputs are directories — load the same path written by "
            "Text File Output (not an individual part-*.csv file)"
        )
        lines.append(
            "# NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime "
            "(use PERMISSIVE mode / upstream path checks as needed)"
        )

    fields = _fields_from_metadata(metadata)
    mode = _reader_mode(metadata)
    logger.debug("Text File Input '%s' reader mode=%s field_count=%s", step_name, mode, len(fields))

    # Detect missing optional metadata without inventing values.
    missing_delimiter = mode == "csv" and not _meta_present(metadata, "separator")
    missing_encoding = not _meta_present(metadata, "encoding")
    missing_schema = mode in ("csv", "fixed") and not fields

    if missing_delimiter:
        lines.append("# WARNING: Input delimiter missing from Pentaho metadata.")
        status = "partial"
    # Missing encoding/schema are INFO comments emitted next to reader options —
    # they do not downgrade status when spark.read remains fully executable.

    if mode == "csv":
        _generate_csv_read(
            lines,
            out_var,
            path_expr,
            metadata,
            fields,
            missing_delimiter=missing_delimiter,
            missing_encoding=missing_encoding,
            missing_schema=missing_schema,
        )
    elif mode == "fixed":
        lines.append(
            "# TODO: Fixed-width input approximated via substring parsing."
        )
        status = "partial"
        _generate_fixed_read(
            lines,
            out_var,
            path_expr,
            metadata,
            fields,
            missing_encoding=missing_encoding,
        )
    else:
        if missing_encoding:
            lines.append("# INFO: Input encoding missing from Pentaho metadata.")
        _generate_text_read(
            lines, out_var, path_expr, metadata, missing_encoding=missing_encoding
        )

    if fields:
        _preserve_input_field_formats(lines, fields)

    if mode in ("csv", "fixed") and fields:
        _apply_field_defaults(lines, out_var, fields)

    _apply_skip_header_lines(lines, out_var, metadata, mode)
    _apply_row_limit(lines, out_var, metadata)
    _apply_footer_lines(lines, out_var, metadata)

    if _bool_setting(metadata, "noempty", default=False):
        if mode == "text":
            lines.append(f"{out_var} = {out_var}.filter(length(col('value')) > 0)")
        else:
            # Best-effort: drop rows where every selected field is null/blank.
            if fields:
                conds = " & ".join(
                    f"(col({f['name']!r}).isNull() | (length(trim(col({f['name']!r}).cast('string'))) == 0))"
                    for f in fields
                    if f.get("name")
                )
                if conds:
                    lines.append(f"{out_var} = {out_var}.filter(~({conds}))")

    _apply_include_filename_and_rownum(lines, out_var, metadata)

    if status == "partial":
        logger.warning(
            "Text File Input '%s' (type=%s) migrated partially — review generated WARNINGs",
            step_name,
            step_type,
        )
    else:
        logger.info("Text File Input '%s' (type=%s) migrated successfully", step_name, step_type)

    return lines, status
