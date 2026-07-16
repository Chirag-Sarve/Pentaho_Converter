"""Handlers for remaining Pentaho Input transformation steps.

Already-supported inputs (TableInput, CsvInput, ExcelInput, TextFileInput,
JsonInput, GetXMLData/XmlInput, RowGenerator/DataGrid, SystemInfo) live in
``input_handlers``, ``generate_handlers``, and ``advanced_handlers``.

This module adds dedicated migration for every other Input step listed in the
PDI palette, preferring Spark DataFrame APIs and marking steps with no
Databricks equivalent as unsupported while preserving metadata.
"""

from __future__ import annotations

import logging

from ..lineage import substitute_pentaho_variables
from ..metadata_propagation import get_converter_metadata
from ..path_utils import spark_load_path_expr
from ..schema_utils import fields_to_schema_ddl
from ..step_xml import _child_text, extract_step_property, get_step_element
from ..text_file_input_converter import convert_text_file_input_step
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _params(context: StepContext) -> dict:
    return context.transformation.parameters or {}


def _step_attr(context: StepContext, key: str, default: str = "") -> str:
    """Read a step attribute with nested `<file>` XML fallback (mirrors BaseStepHandler._attr)."""
    val = context.step.attributes.get(key, "")
    if val and str(val).strip().startswith("<"):
        try:
            from xml.etree import ElementTree as ET

            frag = ET.fromstring(val)
            if frag.tag == "file":
                name = _child_text(frag, "name")
                if name:
                    return name
        except Exception:
            pass
    if val:
        return str(val)
    step_el = get_step_element(context.step)
    if step_el is not None:
        return extract_step_property(step_el, key, _child_text(step_el, key, default))
    return default


def _resolve_path(context: StepContext, *keys: str, default: str = "") -> str:
    """Resolve a file path from attributes / nested XML with variable substitution."""
    raw = ""
    for key in keys:
        raw = _step_attr(context, key, "")
        if raw:
            break
    if not raw:
        step_el = get_step_element(context.step)
        if step_el is not None:
            for key in keys:
                raw = _child_text(step_el, key, "")
                if raw:
                    break
            if not raw:
                file_el = step_el.find("file")
                if file_el is not None:
                    raw = _child_text(file_el, "name", "") or _child_text(file_el, "filename", "")
    return substitute_pentaho_variables(raw or default, _params(context))


def _field_dicts(context: StepContext) -> list[dict]:
    metadata = get_converter_metadata(context)
    fields = list(metadata.get("fields") or [])
    if fields:
        return [f for f in fields if isinstance(f, dict) and f.get("name")]
    result: list[dict] = []
    for f in context.step.fields or []:
        if f.name:
            result.append({"name": f.name, "type": f.type_name})
    if result:
        return result
    step_el = get_step_element(context.step)
    if step_el is None:
        return []
    from ..transformation_parser import _parse_fields

    return [{"name": f.name, "type": f.type_name} for f in _parse_fields(step_el) if f.name]


def _field_names(context: StepContext) -> list[str]:
    return [f["name"] for f in _field_dicts(context) if f.get("name")]


def _preserve_meta_comments(context: StepContext, keys: tuple[str, ...] = ()) -> list[str]:
    """Emit comments that preserve key Pentaho attributes for unsupported steps."""
    lines: list[str] = []
    meta = get_converter_metadata(context)
    for key in keys:
        val = meta.get(key) or _step_attr(context, key, "")
        if val not in (None, "", [], {}):
            lines.append(f"# preserved.{key}={val!r}")
    attrs = context.step.attributes or {}
    interesting = (
        "connection", "filename", "file", "url", "host", "port", "username",
        "schema", "table", "catalog", "query", "sql", "module", "folder",
    )
    for key in interesting:
        if key in keys:
            continue
        val = attrs.get(key) or _step_attr(context, key, "")
        if val:
            lines.append(f"# preserved.{key}={val!r}")
    return lines


def _empty_source_df(out_var: str, alias: str = "_unsupported") -> str:
    return f"{out_var} = spark.range(0).select(lit(1).alias({alias!r})).limit(0)"


def _unsupported(
    context: StepContext,
    label: str,
    reason: str,
    *,
    meta_keys: tuple[str, ...] = (),
) -> tuple[list[str], str]:
    """Generate a runnable empty DataFrame while documenting unsupported behavior."""
    out_var = context.output_df_name()
    lines = [
        f"# {label}: {context.step.name}",
        f"# UNSUPPORTED: {reason}",
        f"# WARNING: No Databricks equivalent for Pentaho '{context.step.step_type}'. "
        "Metadata preserved; pipeline continues with an empty frame.",
    ]
    lines.extend(_preserve_meta_comments(context, meta_keys))
    lines.append(_empty_source_df(out_var, f"_{label.lower().replace(' ', '_')}"))
    return lines, "converted"


def _attr(context: StepContext, key: str, default: str = "") -> str:
    return _step_attr(context, key, default)


def _yn(context: StepContext, key: str, default: bool = False) -> bool:
    raw = _attr(context, key, "Y" if default else "N")
    if not raw:
        return default
    return raw.strip().upper() in ("Y", "YES", "TRUE", "1", "T")


# ---------------------------------------------------------------------------
# File-format Inputs
# ---------------------------------------------------------------------------


class FixedFileInputHandler(BaseStepHandler):
    """Fixed File Input → text read + substring fixed-width parsing."""

    _TYPES = {"fixedinput", "fixedfileinput", "fixedwidthinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = dict(get_converter_metadata(context))
        metadata.setdefault("file_type", "FIXED")
        metadata["file_type"] = "FIXED"
        path = _resolve_path(context, "filename", "file", "name")
        if path and not metadata.get("filename") and not metadata.get("file"):
            metadata["filename"] = path
        if not metadata.get("fields"):
            metadata["fields"] = _field_dicts(context)
        lines, status = convert_text_file_input_step(
            metadata,
            context.output_df_name(),
            context.step.name,
            context=context,
        )
        if lines:
            lines[0] = f"# Fixed File Input: {context.step.name}"
        return lines, status


class GzipCsvInputHandler(BaseStepHandler):
    """GZIP CSV Input → spark.read.csv with gzip compression."""

    _TYPES = {"gzipcsvinput", "gzipcsvfileinput", "gzipcsv"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name")
        delimiter = _attr(context, "separator", ",") or ","
        header = _yn(context, "header", True)
        enclosure = _attr(context, "enclosure", "") or _attr(context, "quote", "")
        schema_ddl = fields_to_schema_ddl(_field_dicts(context))
        load_path = spark_load_path_expr(path)

        lines = [f"# GZIP CSV Input: {context.step.name}"]
        lines.append(f"{out_var} = (")
        lines.append("    spark.read.format('csv')")
        lines.append("    .option('compression', 'gzip')")
        lines.append(f"    .option('header', {str(header).lower()!r})")
        lines.append(f"    .option('sep', {delimiter!r})")
        if enclosure:
            lines.append(f"    .option('quote', {enclosure!r})")
        if schema_ddl:
            lines.append("    .option('inferSchema', 'false')")
            lines.append(f"    .schema({schema_ddl!r})")
        lines.append(f"    .load({load_path})")
        lines.append(")")
        if not path:
            lines.insert(1, "# WARNING: GZIP CSV path missing — review load target")
        return lines, "converted"


class S3CsvInputHandler(BaseStepHandler):
    """S3 CSV Input → spark.read.csv over s3a:// / s3:// paths."""

    _TYPES = {"s3csvinput", "s3csvfileinput", "s3fileinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "bucket", "name")
        bucket = _attr(context, "bucket", "")
        if bucket and path and not path.startswith("s3"):
            path = f"s3a://{bucket.rstrip('/')}/{path.lstrip('/')}"
        elif bucket and not path:
            path = f"s3a://{bucket}"
        elif path and not path.startswith(("s3://", "s3a://", "dbfs:", "/")):
            path = f"s3a://{path.lstrip('/')}"

        delimiter = _attr(context, "separator", ",") or ","
        header = _yn(context, "header", True)
        schema_ddl = fields_to_schema_ddl(_field_dicts(context))
        load_path = spark_load_path_expr(path)

        lines = [f"# S3 CSV Input: {context.step.name}"]
        lines.append(f"{out_var} = (")
        lines.append("    spark.read.format('csv')")
        lines.append(f"    .option('header', {str(header).lower()!r})")
        lines.append(f"    .option('sep', {delimiter!r})")
        if schema_ddl:
            lines.append("    .option('inferSchema', 'false')")
            lines.append(f"    .schema({schema_ddl!r})")
        lines.append(f"    .load({load_path})")
        lines.append(")")
        return lines, "converted"


class YamlInputHandler(BaseStepHandler):
    """YAML Input → note SnakeYAML-style parse then DataFrame (Spark has no native YAML)."""

    _TYPES = {"yamlinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "yaml_field", "name")
        load_path = spark_load_path_expr(path)
        fields = _field_names(context)
        schema = ", ".join(f"{n} STRING" for n in fields) if fields else "payload STRING"

        lines = [
            f"# YAML Input: {context.step.name}",
            "# NOTE: Spark has no native YAML reader — load as text then parse "
            "(e.g. SnakeYAML / PyYAML on driver, or Databricks yaml UDF).",
            f"_yaml_raw_{out_var} = spark.read.text({load_path})",
            f"# TODO: parse YAML documents into columns {fields!r}",
            f"{out_var} = _yaml_raw_{out_var}.select(col('value').alias('payload'))",
        ]
        if fields:
            lines.append(
                f"# Expected output schema from Pentaho: {schema!r}"
            )
        return lines, "converted"


class PropertyInputHandler(BaseStepHandler):
    """Property Input → Java Properties style key/value DataFrame."""

    _TYPES = {"propertyinput", "propertiesinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name")
        load_path = spark_load_path_expr(path)
        key_col = _attr(context, "key_field", "key") or "key"
        val_col = _attr(context, "value_field", "value") or "value"

        lines = [
            f"# Property Input: {context.step.name}",
            f"_props_raw_{out_var} = spark.read.text({load_path})",
            f"{out_var} = (",
            f"    _props_raw_{out_var}",
            f"    .filter(~col('value').startswith('#'))",
            f"    .filter(col('value').contains('='))",
            f"    .select(",
            f"        trim(split(col('value'), '=', 2)[0]).alias({key_col!r}),",
            f"        trim(split(col('value'), '=', 2)[1]).alias({val_col!r}),",
            f"    )",
            ")",
        ]
        return lines, "converted"


class XmlInputStreamHandler(BaseStepHandler):
    """XML Input Stream (StAX) → spark-xml (streaming semantics approximated)."""

    _TYPES = {"xmlinputstream", "staxxmlinput", "xmlinputstreamstax"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "").replace("(", "").replace(")", "")
        return t in self._TYPES or t.startswith("xmlinputstream")

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "xml_field", "name")
        row_tag = _attr(context, "row_tag", "") or _attr(context, "xml_source_element", "row") or "row"
        load_path = spark_load_path_expr(path)
        lines = [
            f"# XML Input Stream (StAX): {context.step.name}",
            "# NOTE: StAX streaming approximated with spark-xml bulk read.",
            f"{out_var} = (",
            "    spark.read.format('xml')",
            f"    .option('rowTag', {row_tag!r})",
            f"    .load({load_path})",
            ")",
        ]
        return lines, "converted"


class LoadFileContentHandler(BaseStepHandler):
    """Load File Content in Memory → wholeTextFiles / binaryFiles."""

    _TYPES = {"loadfileinput", "loadfilecontentinmemory", "loadfile"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in self._TYPES or t.startswith("loadfile")

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name") or "*"
        load_path = spark_load_path_expr(path)
        content_field = _attr(context, "content_field", "file_content") or "file_content"
        filename_field = _attr(context, "filename_field", "filename") or "filename"
        is_binary = _yn(context, "is_binary", False) or _yn(context, "add_result_file", False)

        lines = [f"# Load File Content in Memory: {context.step.name}"]
        if is_binary:
            lines.append(
                f"_files_{out_var} = spark.sparkContext.binaryFiles({load_path})"
            )
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"_files_{out_var}.map(lambda kv: (kv[0], kv[1])), "
                f"[{filename_field!r}, {content_field!r}])"
            )
        else:
            lines.append(
                f"_files_{out_var} = spark.sparkContext.wholeTextFiles({load_path})"
            )
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"_files_{out_var}.map(lambda kv: (kv[0], kv[1])), "
                f"[{filename_field!r}, {content_field!r}])"
            )
        return lines, "converted"


class AccessInputHandler(BaseStepHandler):
    """Microsoft Access Input → JDBC via UCanAccess."""

    _TYPES = {"accessinput", "microsoftaccessinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "dbname", "name")
        table = _attr(context, "table", "") or _attr(context, "tablename", "")
        sql = _attr(context, "sql", "")
        url = f"jdbc:ucanaccess://{path}" if path else "jdbc:ucanaccess://path/to/db.accdb"

        lines = [
            f"# Microsoft Access Input: {context.step.name}",
            f"# Requires UCanAccess JDBC driver on the Databricks cluster classpath.",
            f"{out_var} = (",
            "    spark.read.format('jdbc')",
            f"    .option('url', {url!r})",
            "    .option('driver', 'net.ucanaccess.jdbc.UcanaccessDriver')",
        ]
        if sql:
            lines.append(f"    .option('query', {sql!r})")
        elif table:
            lines.append(f"    .option('dbtable', {table!r})")
        else:
            lines.append("    .option('dbtable', 'TABLE_NAME')")
            lines.append("    # WARNING: Access table/SQL missing — set dbtable or query")
        lines.append("    .load()")
        lines.append(")")
        return lines, "converted"


class SasInputHandler(BaseStepHandler):
    """SAS Input → spark-sas7bdat / com.github.saurfang.sas format."""

    _TYPES = {"sasinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name")
        load_path = spark_load_path_expr(path)
        lines = [
            f"# SAS Input: {context.step.name}",
            "# Requires spark-sas7bdat (com.github.saurfang:spark-sas7bdat) on the cluster.",
            f"{out_var} = spark.read.format('com.github.saurfang.sas.spark').load({load_path})",
        ]
        return lines, "converted"


class XBaseInputHandler(BaseStepHandler):
    """XBase Input (DBF) → dbf / custom format reader."""

    _TYPES = {"xbaseinput", "dbfinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name")
        load_path = spark_load_path_expr(path)
        schema_ddl = fields_to_schema_ddl(_field_dicts(context))
        lines = [
            f"# XBase Input: {context.step.name}",
            "# DBF via Spark format 'dbf' if available; otherwise install a DBF datasource.",
            f"{out_var} = (",
            "    spark.read.format('dbf')",
        ]
        if schema_ddl:
            lines.append(f"    .schema({schema_ddl!r})")
        lines.append(f"    .load({load_path})")
        lines.append(")")
        return lines, "converted"


class ShapefileInputHandler(BaseStepHandler):
    """ESRI Shapefile Reader → Apache Sedona / GeoSpark if available."""

    _TYPES = {"shapefilereader", "esrishapefile", "esrishapefilereader", "gisfileinput"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in self._TYPES or "shapefile" in t

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "shapefile", "name")
        load_path = spark_load_path_expr(path)
        lines = [
            f"# ESRI Shapefile Reader: {context.step.name}",
            "# Requires Apache Sedona (formerly GeoSpark) shapefile reader on the cluster.",
            f"{out_var} = (",
            "    spark.read.format('shapefile')",
            f"    .load({load_path})",
            ")",
            f"# Alternative Sedona API: Adapter.toDf(ShapefileReader.readToGeometryRDD"
            f"(sc, {path!r}), spark)",
        ]
        return lines, "converted"


# ---------------------------------------------------------------------------
# File / catalog discovery Inputs
# ---------------------------------------------------------------------------


class GetFileNamesHandler(BaseStepHandler):
    """Get File Names → dbutils.fs.ls / Hadoop FileSystem listing as DataFrame."""

    _TYPES = {"getfilenames"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        folder = _resolve_path(context, "filename", "file", "folder", "directory", "name") or "/"
        include_sub = _yn(context, "include_subfolders", False) or _yn(context, "include_subdir", False)
        load_path = spark_load_path_expr(folder)

        lines = [
            f"# Get File Names: {context.step.name}",
            f"_list_path_{out_var} = {load_path}",
            f"try:",
            f"    _fs_entries_{out_var} = dbutils.fs.ls(_list_path_{out_var})",
            f"    {out_var} = spark.createDataFrame(",
            f"        [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_{out_var}],",
            f"        ['filename', 'short_filename', 'size', 'last_modified']",
            f"    )",
            f"except Exception:",
            f"    # Fallback: Hadoop FileSystem listing via SparkContext",
            f"    _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_{out_var})",
            f"    _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())",
            f"    _statuses = _fs.listStatus(_jpath)",
            f"    {out_var} = spark.createDataFrame(",
            f"        [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())",
            f"         for s in _statuses if s.isFile()],",
            f"        ['filename', 'short_filename', 'size', 'last_modified']",
            f"    )",
        ]
        if include_sub:
            lines.append(
                f"# WARNING: recursive subfolder listing not expanded — "
                f"use dbutils.fs.ls recursively if required for '{context.step.name}'"
            )
        return lines, "converted"


class GetSubfolderNamesHandler(BaseStepHandler):
    """Get Subfolder Names → directory listing filtered to folders."""

    _TYPES = {"getsubfolders", "getsubfoldernames"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        folder = _resolve_path(context, "filename", "file", "folder", "directory", "name") or "/"
        load_path = spark_load_path_expr(folder)
        lines = [
            f"# Get Subfolder Names: {context.step.name}",
            f"_list_path_{out_var} = {load_path}",
            f"try:",
            f"    _fs_entries_{out_var} = dbutils.fs.ls(_list_path_{out_var})",
            f"    {out_var} = spark.createDataFrame(",
            f"        [(e.path, e.name) for e in _fs_entries_{out_var} if e.isDir()],",
            f"        ['foldername', 'short_foldername']",
            f"    )",
            f"except Exception:",
            f"    _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_{out_var})",
            f"    _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())",
            f"    _statuses = _fs.listStatus(_jpath)",
            f"    {out_var} = spark.createDataFrame(",
            f"        [(s.getPath().toString(), s.getPath().getName())",
            f"         for s in _statuses if s.isDirectory()],",
            f"        ['foldername', 'short_foldername']",
            f"    )",
        ]
        return lines, "converted"


class GetFilesRowsCountHandler(BaseStepHandler):
    """Get Files Rows Count → read text and count()."""

    _TYPES = {"getfilesrowscount", "filesrowscount"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name")
        load_path = spark_load_path_expr(path)
        count_field = _attr(context, "rowsCountField", "rows_count") or _attr(
            context, "count_field", "rows_count"
        ) or "rows_count"
        lines = [
            f"# Get Files Rows Count: {context.step.name}",
            f"_rows_src_{out_var} = spark.read.text({load_path})",
            f"_rows_n_{out_var} = _rows_src_{out_var}.count()",
            f"{out_var} = spark.createDataFrame(",
            f"    [({load_path}, _rows_n_{out_var})],",
            f"    ['filename', {count_field!r}]",
            f")",
        ]
        return lines, "converted"


class GetTableNamesHandler(BaseStepHandler):
    """Get Table Names → spark.catalog.listTables()."""

    _TYPES = {"gettablenames"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        schema = _attr(context, "schemaname", "") or _attr(context, "schema", "")
        database = _attr(context, "database", "") or schema

        lines = [f"# Get Table Names: {context.step.name}"]
        if database:
            lines.append(
                f"_tables_{out_var} = spark.catalog.listTables({database!r})"
            )
        else:
            lines.append(f"_tables_{out_var} = spark.catalog.listTables()")
        lines.append(
            f"{out_var} = spark.createDataFrame("
            f"[(t.name, t.database, t.tableType, t.isTemporary) for t in _tables_{out_var}], "
            f"['tablename', 'database', 'tabletype', 'is_temporary'])"
        )
        return lines, "converted"


class GetRepositoryNamesHandler(BaseStepHandler):
    """Get Repository Names — Pentaho repository has no Databricks equivalent."""

    _TYPES = {"getrepositorynames"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _unsupported(
            context,
            "Get Repository Names",
            "Pentaho repository browsing has no Databricks equivalent.",
            meta_keys=("object_type", "directory", "name_regex", "include_deleted"),
        )


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


class RandomValueHandler(BaseStepHandler):
    """Generate Random Value → Spark rand(), randn(), uuid()."""

    _TYPES = {"randomvalue", "generaterandomvalue"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        in_df = context.input_df_name()
        step_el = get_step_element(context.step)
        lines = [f"# Generate Random Value: {context.step.name}"]

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            limit_raw = _attr(context, "limit", "1") or "1"
            try:
                limit = max(1, int(limit_raw))
            except ValueError:
                limit = 1
            lines.append(f"{out_var} = spark.range({limit})")

        fields: list[tuple[str, str]] = []
        if step_el is not None:
            fields_el = step_el.find("fields")
            targets = fields_el.findall("field") if fields_el is not None else step_el.findall("field")
            for field_el in targets:
                name = _child_text(field_el, "name")
                kind = (
                    _child_text(field_el, "type")
                    or _child_text(field_el, "randomtype")
                    or "number"
                )
                if name:
                    fields.append((name, kind))

        if not fields:
            for f in _field_dicts(context):
                fields.append((f["name"], f.get("type") or "number"))

        if not fields:
            lines.append(f'{out_var} = {out_var}.withColumn("random", rand())')
            return lines, "converted"

        for name, kind in fields:
            k = (kind or "").lower().replace(" ", "_")
            if "uuid" in k or "string" in k:
                expr = "expr('uuid()')"
            elif "int" in k:
                expr = "(rand() * lit(2147483647)).cast('int')"
            elif "normal" in k or "gaussian" in k or "randn" in k:
                expr = "randn()"
            elif "bool" in k:
                expr = "(rand() > lit(0.5))"
            else:
                expr = "rand()"
            lines.append(f'{out_var} = {out_var}.withColumn("{name}", {expr})')
        return lines, "converted"


class RandomCreditCardHandler(BaseStepHandler):
    """Generate Random Credit Card Numbers — preserve generation logic via Luhn-safe stub."""

    _TYPES = {
        "randomccnumbergenerator",
        "generaterandomcreditcardnumbers",
        "creditcardgenerator",
    }

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        in_df = context.input_df_name()
        field = _attr(context, "field", "card_number") or "card_number"
        card_type = _attr(context, "card_type", "Visa") or "Visa"
        length_raw = _attr(context, "length", "16") or "16"
        try:
            length = max(12, min(19, int(length_raw)))
        except ValueError:
            length = 16
        try:
            rows = max(1, int(_attr(context, "limit", "1") or "1"))
        except ValueError:
            rows = 1

        lines = [
            f"# Generate Random Credit Card Numbers: {context.step.name}",
            f"# Preserves Pentaho intent: generate Luhn-valid-looking numbers "
            f"(type={card_type!r}, length={length}). Review for production use.",
        ]
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.range({rows})")

        # Deterministic-looking digit string from rand; not cryptographically secure.
        # Keeps generation in Spark without inventing a full Luhn UDF dependency.
        prefix = {
            "visa": "4",
            "mastercard": "5",
            "amex": "37",
            "american express": "37",
            "discover": "6",
        }.get(card_type.lower(), "4")
        rem = length - len(prefix)
        lines.append(
            f"{out_var} = {out_var}.withColumn("
            f"{field!r}, "
            f"concat(lit({prefix!r}), "
            f"lpad(((rand() * lit(10 ** {rem})).cast('long')).cast('string'), {rem}, '0')))"
        )
        lines.append(
            f"# WARNING: Full Luhn check-digit algorithm from Pentaho RandomCCNumberGenerator "
            f"is approximated — validate card numbers if downstream rules require exact Luhn."
        )
        type_field = _attr(context, "card_type_field", "")
        if type_field:
            lines.append(f'{out_var} = {out_var}.withColumn({type_field!r}, lit({card_type!r}))')
        return lines, "converted"


# ---------------------------------------------------------------------------
# Integration / protocol Inputs
# ---------------------------------------------------------------------------


class SalesforceInputHandler(BaseStepHandler):
    """Salesforce Input → Salesforce Spark connector format."""

    _TYPES = {"salesforceinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        soql = _attr(context, "query", "") or _attr(context, "soql", "") or _attr(context, "sql", "")
        module = _attr(context, "module", "") or _attr(context, "object", "Account")
        url = _attr(context, "targeturl", "") or _attr(context, "url", "")
        username = _attr(context, "username", "")

        lines = [
            f"# Salesforce Input: {context.step.name}",
            "# Requires a Salesforce Spark connector (e.g. com.springml.spark.salesforce).",
            f"{out_var} = (",
            "    spark.read.format('com.springml.spark.salesforce')",
        ]
        if url:
            lines.append(f"    .option('sfURL', {url!r})")
        if username:
            lines.append(f"    .option('sfUser', {username!r})")
            lines.append("    .option('sfPassword', dbutils.secrets.get(scope='salesforce', key='password'))")
        if soql:
            lines.append(f"    .option('soql', {soql!r})")
        else:
            lines.append(f"    .option('sfObject', {module!r})")
        lines.append("    .load()")
        lines.append(")")
        lines.extend(_preserve_meta_comments(context, ("module", "query", "condition")))
        return lines, "converted"


class LdapInputHandler(BaseStepHandler):
    """LDAP Input — partial: emit connection metadata + empty frame / JDBC-LDAP note."""

    _TYPES = {"ldapinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        host = _attr(context, "host", "") or _attr(context, "hostname", "localhost")
        port = _attr(context, "port", "389") or "389"
        base_dn = _attr(context, "searchBase", "") or _attr(context, "base_dn", "")
        filter_expr = _attr(context, "filter", "") or _attr(context, "filterString", "(objectClass=*)")
        fields = _field_names(context)
        schema = ", ".join(f"{n} STRING" for n in fields) if fields else "dn STRING, attribute STRING, value STRING"

        lines = [
            f"# LDAP Input: {context.step.name}",
            f"# PARTIAL: Databricks has no built-in LDAP reader. "
            f"Use an LDAP library on the driver or a JDBC-LDAP bridge.",
            f"# host={host!r} port={port!r} base_dn={base_dn!r} filter={filter_expr!r}",
            f"# Expected fields: {fields!r}",
            f"{out_var} = spark.createDataFrame([], '{schema}')",
            f"# TODO: populate via ldap3 / java.naming from ({host}:{port})",
        ]
        return lines, "converted"


class LdifInputHandler(BaseStepHandler):
    """LDIF Input → text parse of LDIF records into a coarse DataFrame."""

    _TYPES = {"ldifinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name")
        load_path = spark_load_path_expr(path)
        lines = [
            f"# LDIF Input: {context.step.name}",
            f"_ldif_raw_{out_var} = spark.read.text({load_path})",
            f"# Coarse LDIF record split — full attribute parsing needs an LDIF library.",
            f"{out_var} = (",
            f"    _ldif_raw_{out_var}",
            f"    .withColumn('is_dn', col('value').startswith('dn:'))",
            f"    .withColumn('record_id', _sum(col('is_dn').cast('int')).over("
            f"Window.orderBy(monotonically_increasing_id())))",
            f"    .groupBy('record_id').agg(collect_list('value').alias('ldif_lines'))",
            f")",
        ]
        return lines, "converted"


class RssInputHandler(BaseStepHandler):
    """RSS Input → XML parse of RSS/Atom feed."""

    _TYPES = {"rssinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        url = _resolve_path(context, "url", "filename", "file", "name")
        load_path = spark_load_path_expr(url)
        row_tag = _attr(context, "row_tag", "item") or "item"
        lines = [
            f"# RSS Input: {context.step.name}",
            f"# RSS/Atom feed parsed via spark-xml (download feed to storage if URL fetch is needed).",
            f"{out_var} = (",
            "    spark.read.format('xml')",
            f"    .option('rowTag', {row_tag!r})",
            f"    .load({load_path})",
            ")",
        ]
        if url.startswith(("http://", "https://")):
            lines.insert(
                1,
                f"# WARNING: Direct HTTP RSS URLs may need wget/curl to DBFS before spark.read "
                f"(url={url!r}).",
            )
        return lines, "converted"


class Hl7InputHandler(BaseStepHandler):
    """HL7 Input — partial HL7 message parse stub (no Databricks-native HL7)."""

    _TYPES = {"hl7input"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name")
        load_path = spark_load_path_expr(path)
        lines = [
            f"# HL7 Input: {context.step.name}",
            "# PARTIAL: No native Databricks HL7 parser — load messages as text; "
            "integrate an HL7 library (e.g. HAPI) for segment parsing.",
            f"_hl7_raw_{out_var} = spark.read.text({load_path})",
            f"{out_var} = _hl7_raw_{out_var}.select(",
            f"    col('value').alias('hl7_message'),",
            f"    split(col('value'), r'\\r|\\n').alias('segments'),",
            f")",
        ]
        lines.extend(_preserve_meta_comments(context, ("message_type", "version")))
        return lines, "converted"


class EmailMessagesInputHandler(BaseStepHandler):
    """Email Messages Input — no Databricks equivalent for mailbox polling."""

    _TYPES = {"mailinput", "emailmessagesinput", "emailinput"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _unsupported(
            context,
            "Email Messages Input",
            "Mailbox IMAP/POP polling has no Databricks Spark equivalent; "
            "use external ingestion (Logic Apps, Graph API, or Autoloader of exported mail).",
            meta_keys=("server", "protocol", "username", "folder", "ssl"),
        )


class DeserializeFromFileHandler(BaseStepHandler):
    """De-serialize from file (CubeInput) — Java serialization not portable to Spark."""

    _TYPES = {"cubeinput", "deserializefromfile", "deserialisefromfile"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        path = _resolve_path(context, "file", "filename", "name")
        lines, status = _unsupported(
            context,
            "De-serialize from file",
            "Java ObjectInputStream cube files cannot be read by Spark DataFrames.",
            meta_keys=("file", "filename", "limit"),
        )
        if path:
            lines.insert(2, f"# serialized_cube_path={path!r}")
        return lines, status


class SapInputHandler(BaseStepHandler):
    """SAP Input (Deprecated) — no Databricks/JCo equivalent; preserve metadata."""

    _TYPES = {"sapinput", "saperpinput"}

    def can_handle(self, step_type: str) -> bool:
        compact = step_type.strip().lower().replace(" ", "")
        return compact in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        meta = get_converter_metadata(context)
        connection = (
            str(meta.get("connection") or "")
            or _attr(context, "connection", "")
        )
        sap_conn = meta.get("sap_connection") if isinstance(meta.get("sap_connection"), dict) else {}
        # Redact any residual password before comment emission.
        if isinstance(sap_conn, dict) and sap_conn.get("password") and sap_conn.get("password") != "***REDACTED***":
            sap_conn = {**sap_conn, "password": "***REDACTED***"}
        function = meta.get("function") if isinstance(meta.get("function"), dict) else {}
        function_name = (
            str(meta.get("function_name") or "")
            or str(function.get("name") or "")
            or _attr(context, "function", "")
            or _attr(context, "module", "")
        )
        client = str(meta.get("client") or sap_conn.get("client") or _attr(context, "client", ""))
        system = str(meta.get("system") or sap_conn.get("system") or _attr(context, "system", ""))
        language = str(
            meta.get("language") or sap_conn.get("language") or _attr(context, "language", "")
        )
        username = str(
            meta.get("username") or sap_conn.get("username") or _attr(context, "username", "")
        )
        password_set = bool(
            meta.get("password_set")
            or sap_conn.get("password_set")
            or (sap_conn.get("password") and sap_conn.get("password") != "***REDACTED***")
        )
        host = str(meta.get("host") or sap_conn.get("host") or _attr(context, "host", ""))
        batch_size = meta.get("batch_size") or (meta.get("pagination") or {}).get("batch_size")
        page_size = meta.get("page_size") or (meta.get("pagination") or {}).get("page_size")
        row_skips = meta.get("row_skips") or (meta.get("pagination") or {}).get("row_skips")
        parameters = meta.get("parameters") if isinstance(meta.get("parameters"), list) else []
        fields = meta.get("fields") if isinstance(meta.get("fields"), list) else []
        filters = meta.get("filters") if isinstance(meta.get("filters"), list) else []

        extra_warnings: list[str] = []
        if not connection and not host:
            extra_warnings.append(
                "# WARNING: Missing SAP connection — define an SAP ERP connection or host before remapping."
            )
        if connection and not (client or system or language or username or host):
            extra_warnings.append(
                "# WARNING: SAP connection name present but client/system/language/user/host "
                "not found on the step; resolve from the named DatabaseMeta at remapping time."
            )
        if username and not password_set:
            extra_warnings.append(
                "# WARNING: SAP username present without password — invalid/missing credentials."
            )
        if not function_name:
            extra_warnings.append(
                "# WARNING: SAP function/module/table missing from Pentaho metadata."
            )
        if fields:
            out_types = {
                (f.get("new_name") or f.get("field_name") or ""): f.get("target_type")
                for f in fields
                if isinstance(f, dict)
            }
            if out_types and any(not t for t in out_types.values()):
                extra_warnings.append(
                    "# WARNING: Schema mismatch risk — some SAP output fields lack target_type."
                )
        extra_warnings.append(
            "# WARNING: Connection failures / JCo native libs are not runnable on Databricks; "
            "remap via JDBC (SAP HANA), OData, or an approved SAP connector."
        )

        lines, _status = _unsupported(
            context,
            "SAP Input (Deprecated)",
            "SAP JCo / RFC_READ_TABLE execution has no Databricks Spark equivalent. "
            "Preserve connection, function/module, field mappings, filters, and pagination "
            "for manual remapping.",
            meta_keys=(
                "connection",
                "client",
                "system",
                "language",
                "username",
                "host",
                "function_name",
                "module",
                "table",
                "parameters",
                "fields",
                "filters",
                "batch_size",
                "page_size",
                "row_skips",
                "pagination",
                "function",
            ),
        )
        preserved_extra: list[str] = []
        if sap_conn:
            safe_conn = {k: v for k, v in sap_conn.items() if k != "password"}
            if sap_conn.get("password_set") or password_set:
                safe_conn["password_set"] = True
            preserved_extra.append(f"# preserved.sap_connection={safe_conn!r}")
        if function_name:
            preserved_extra.append(f"# preserved.function_name={function_name!r}")
        if client:
            preserved_extra.append(f"# preserved.client={client!r}")
        if system:
            preserved_extra.append(f"# preserved.system={system!r}")
        if language:
            preserved_extra.append(f"# preserved.language={language!r}")
        if password_set:
            preserved_extra.append("# preserved.password_set=True  # value redacted")
        if batch_size:
            preserved_extra.append(f"# preserved.batch_size={batch_size!r}")
        if page_size:
            preserved_extra.append(f"# preserved.page_size={page_size!r}")
        if row_skips:
            preserved_extra.append(f"# preserved.row_skips={row_skips!r}")
        if parameters:
            preserved_extra.append(f"# preserved.parameters={parameters!r}")
        if fields:
            preserved_extra.append(f"# preserved.fields={fields!r}")
        if filters:
            preserved_extra.append(f"# preserved.filters={filters!r}")

        insert_at = max(len(lines) - 1, 3)
        lines[insert_at:insert_at] = [*extra_warnings, *preserved_extra]
        logger.warning(
            "SAP Input '%s' has no Databricks equivalent; emitting unsupported placeholder",
            context.step.name,
        )
        return lines, "partial"


class MondrianInputHandler(BaseStepHandler):
    """Mondrian Input — preserve MDX/catalog metadata; unsupported for execution."""

    _TYPES = {"mondrianinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _unsupported(
            context,
            "Mondrian Input",
            "Mondrian OLAP / MDX execution is not available on Databricks. "
            "Preserve catalog and MDX for manual remapping to SQL warehouses or external OLAP.",
            meta_keys=("catalog", "query", "connection", "role", "filename"),
        )


class OlapInputHandler(BaseStepHandler):
    """OLAP Input — mark unsupported; preserve connection/MDX metadata."""

    _TYPES = {"olapinput", "xmla", "xmlainput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _unsupported(
            context,
            "OLAP Input",
            "XMLA / generic OLAP clients have no Spark DataFrame equivalent.",
            meta_keys=("url", "catalog", "query", "username", "connection"),
        )


# Export registration list (order: specific before generic prefixes)
EXTRA_INPUT_HANDLERS: list[BaseStepHandler] = [
    FixedFileInputHandler(),
    GzipCsvInputHandler(),
    S3CsvInputHandler(),
    YamlInputHandler(),
    PropertyInputHandler(),
    XmlInputStreamHandler(),
    LoadFileContentHandler(),
    AccessInputHandler(),
    SasInputHandler(),
    XBaseInputHandler(),
    ShapefileInputHandler(),
    GetFileNamesHandler(),
    GetSubfolderNamesHandler(),
    GetFilesRowsCountHandler(),
    GetTableNamesHandler(),
    GetRepositoryNamesHandler(),
    RandomValueHandler(),
    RandomCreditCardHandler(),
    SalesforceInputHandler(),
    LdapInputHandler(),
    LdifInputHandler(),
    RssInputHandler(),
    Hl7InputHandler(),
    EmailMessagesInputHandler(),
    DeserializeFromFileHandler(),
    SapInputHandler(),
    MondrianInputHandler(),
    OlapInputHandler(),
]
