"""Convert Pentaho Text File Input step metadata to PySpark file readers."""

from __future__ import annotations

from typing import Any

from .lineage import substitute_pentaho_variables
from .step_context import StepContext


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
            text = str(val).strip()
            if text:
                return text
        prefixed = attrs.get(f"file_{key}")
        if prefixed is not None:
            text = str(prefixed).strip()
            if text:
                return text
    return default


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
    return substitute_pentaho_variables(raw, params)


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
    if path.endswith(".csv") or path.endswith(".tsv"):
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
    t = (type_name or "String").strip().lower()
    mapping = {
        "integer": "int",
        "int": "int",
        "long": "bigint",
        "number": "double",
        "bignumber": "decimal(38,18)",
        "float": "float",
        "double": "double",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "datetime": "timestamp",
        "binary": "binary",
    }
    return mapping.get(t, "string")


def _spark_schema_ddl(fields: list[dict[str, Any]]) -> str | None:
    named = [field for field in fields if field.get("name")]
    if not named:
        return None

    ddl_parts: list[str] = []
    for field in named:
        ddl_parts.append(f"{field['name']} {_spark_cast_type(field.get('type', 'String')).upper()}")
    return ", ".join(ddl_parts)


def _append_option(lines: list[str], var: str, key: str, value: Any) -> str:
    lines.append(f"{var} = {var}.option({key!r}, {value!r})")
    return var


def _append_load(lines: list[str], var: str, path: str) -> None:
    lines.append(f"{var} = {var}.load({path!r})")


def _shared_reader_options(
    lines: list[str],
    reader_var: str,
    metadata: dict[str, Any],
    *,
    include_encoding: bool = True,
) -> str:
    encoding = _meta_value(metadata, "encoding", default="")
    compression = _spark_compression(metadata)
    if include_encoding and encoding:
        reader_var = _append_option(lines, reader_var, "encoding", encoding)
    if compression:
        reader_var = _append_option(lines, reader_var, "compression", compression)
    return reader_var


def _csv_options(
    lines: list[str],
    reader_var: str,
    metadata: dict[str, Any],
    fields: list[dict[str, Any]],
) -> str:
    separator = _meta_value(metadata, "separator", default=",") or ","
    quote = _meta_value(metadata, "enclosure", "quote", default="")
    escape = _meta_value(metadata, "escapechar", "escape", default="")
    comment = _meta_value(metadata, "comment", default="")
    header = _bool_setting(metadata, "header", default=False)
    schema_ddl = _spark_schema_ddl(fields)

    reader_var = _append_option(lines, reader_var, "sep", separator)
    if quote:
        reader_var = _append_option(lines, reader_var, "quote", quote)
    if escape:
        reader_var = _append_option(lines, reader_var, "escape", escape)
    if comment:
        reader_var = _append_option(lines, reader_var, "comment", comment)

    null_values = sorted(
        {
            str(field.get("null_if") or field.get("nullif") or "").strip()
            for field in fields
            if str(field.get("null_if") or field.get("nullif") or "").strip()
        }
    )
    if null_values:
        reader_var = _append_option(lines, reader_var, "nullValue", null_values[0])
        if len(null_values) > 1:
            lines.append(
                f"# NOTE: additional Pentaho null sentinels not mapped: {null_values[1:]!r}"
            )

    if _bool_setting(metadata, "enclosure_breaks", "lazy_quotes", default=False):
        reader_var = _append_option(lines, reader_var, "multiLine", "true")

    if schema_ddl:
        reader_var = _append_option(lines, reader_var, "header", "true" if header else "false")
        reader_var = _append_option(lines, reader_var, "inferSchema", "false")
        lines.append(f"{reader_var} = {reader_var}.schema({schema_ddl!r})")
    else:
        reader_var = _append_option(lines, reader_var, "header", "true" if header else "false")
        reader_var = _append_option(
            lines,
            reader_var,
            "inferSchema",
            "true" if header else "false",
        )

    return reader_var


def _generate_csv_read(
    lines: list[str],
    out_var: str,
    path: str,
    metadata: dict[str, Any],
    fields: list[dict[str, Any]],
) -> None:
    reader_var = f"_tfi_reader_{out_var}"
    lines.append(f"{reader_var} = spark.read.format('csv')")
    reader_var = _shared_reader_options(lines, reader_var, metadata)
    reader_var = _csv_options(lines, reader_var, metadata, fields)
    _append_load(lines, reader_var, path)
    lines.append(f"{out_var} = {reader_var}")


def _generate_text_read(
    lines: list[str],
    out_var: str,
    path: str,
    metadata: dict[str, Any],
) -> None:
    encoding = _meta_value(metadata, "encoding", default="")
    compression = _spark_compression(metadata)
    if encoding or compression:
        reader_var = f"_tfi_reader_{out_var}"
        lines.append(f"{reader_var} = spark.read.format('text')")
        reader_var = _shared_reader_options(lines, reader_var, metadata)
        _append_load(lines, reader_var, path)
        lines.append(f"{out_var} = {reader_var}")
    else:
        lines.append(f"{out_var} = spark.read.text({path!r})")


def _generate_fixed_read(
    lines: list[str],
    out_var: str,
    path: str,
    metadata: dict[str, Any],
    fields: list[dict[str, Any]],
) -> None:
    raw_var = f"_tfi_raw_{out_var}"
    _generate_text_read(lines, raw_var, path, metadata)

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
        expr = f'col({name!r})'
        null_if = str(field.get("null_if") or field.get("nullif") or "").strip()
        default = field.get("default")
        if default is None:
            default = field.get("ifnull")
        if null_if:
            expr = f'when({expr} == lit({null_if!r}), lit(None)).otherwise({expr})'
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
    lines.append(f"_tfi_skip_w = Window.orderBy(monotonically_increasing_id())")
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
    lines.append(f"_tfi_footer_max = {out_var}.agg(max('_tfi_footer_rn')).collect()[0][0]")
    lines.append(
        f"{out_var} = {out_var}.filter(col('_tfi_footer_rn') <= _tfi_footer_max - {nr_footer})"
        ".drop('_tfi_footer_rn')"
    )


def _apply_row_limit(lines: list[str], out_var: str, metadata: dict[str, Any]) -> None:
    row_limit = _int_setting(metadata, "limit", "rowLimit", "row_limit", default=0)
    if row_limit > 0:
        lines.append(f"{out_var} = {out_var}.limit({row_limit})")


def _unresolved_lines(step_name: str, out_var: str, message: str) -> list[str]:
    return [
        f"# WARNING: TextFileInput '{step_name}': {message}",
        f"{out_var} = spark.createDataFrame([], '_text_file_input_unresolved STRING')",
    ]


def convert_text_file_input_step(
    metadata: dict[str, Any],
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate PySpark lines for a Text File Input step from propagated metadata."""
    lines = [f"# Text File Input: {step_name}"]

    buffer_size = _meta_value(metadata, "buffer_size", "buffersize", default="")
    if buffer_size:
        lines.append(f"# Pentaho buffer_size={buffer_size!r} (no Spark CSV/text reader equivalent)")

    if _bool_setting(metadata, "lazy_conversion", "lazyConversionActive", default=False):
        lines.append("# Pentaho lazy conversion enabled (applied at Pentaho runtime, not in Spark reader)")

    path = _file_path(metadata)
    if not path:
        lines.extend(_unresolved_lines(step_name, out_var, "file path missing from metadata"))
        return lines, "partial"

    fields = _fields_from_metadata(metadata)
    mode = _reader_mode(metadata)

    if mode == "csv":
        _generate_csv_read(lines, out_var, path, metadata, fields)
    elif mode == "fixed":
        _generate_fixed_read(lines, out_var, path, metadata, fields)
    else:
        _generate_text_read(lines, out_var, path, metadata)

    if mode in ("csv", "fixed") and fields:
        _apply_field_defaults(lines, out_var, fields)

    _apply_skip_header_lines(lines, out_var, metadata, mode)
    _apply_row_limit(lines, out_var, metadata)
    _apply_footer_lines(lines, out_var, metadata)

    if _bool_setting(metadata, "noempty", default=False) and mode == "text":
        lines.append(f"{out_var} = {out_var}.filter(length(col('value')) > 0)")

    return lines, "converted"
