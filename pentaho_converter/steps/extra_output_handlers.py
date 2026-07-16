"""Handlers for remaining Pentaho Output transformation steps.

Core outputs (TableOutput, TextFileOutput, ExcelOutput) live in ``output_handlers``.
DB MERGE outputs and JSON/XML writers live in ``extended_handlers`` /
``advanced_handlers``.

This module adds dedicated migration for every other Output step in the PDI
palette: Excel Writer, Access, S3, SQL File, Properties, RSS, LDAP, Salesforce*,
Serialize, AutoDoc, and Pentaho Reporting.
"""

from __future__ import annotations

from ..generation_config import GenerationConfig
from ..lineage import substitute_pentaho_variables
from ..metadata_propagation import get_converter_metadata
from ..path_utils import spark_load_path_expr
from ..step_xml import _child_text, extract_step_property, get_step_element
from ..table_names import qualify_table_name
from .advanced_handlers import _passthrough
from .base import BaseStepHandler, StepContext


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _params(context: StepContext) -> dict:
    return context.transformation.parameters or {}


def _step_attr(context: StepContext, key: str, default: str = "") -> str:
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


def _attr(context: StepContext, key: str, default: str = "") -> str:
    return _step_attr(context, key, default)


def _yn(context: StepContext, key: str, default: bool = False) -> bool:
    raw = _attr(context, key, "Y" if default else "N")
    if not raw:
        return default
    return raw.strip().upper() in ("Y", "YES", "TRUE", "1")


def _resolve_path(context: StepContext, *keys: str, default: str = "") -> str:
    raw = ""
    for key in keys:
        raw = _step_attr(context, key, "")
        if raw:
            break
    if not raw:
        meta = get_converter_metadata(context)
        for key in keys:
            val = meta.get(key)
            if val:
                raw = str(val)
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


def _preserve_meta_comments(context: StepContext, keys: tuple[str, ...] = ()) -> list[str]:
    lines: list[str] = []
    meta = get_converter_metadata(context)
    for key in keys:
        val = meta.get(key) or _step_attr(context, key, "")
        if val not in (None, "", [], {}):
            lines.append(f"# preserved.{key}={val!r}")
    attrs = context.step.attributes or {}
    interesting = (
        "connection", "filename", "file", "url", "host", "port", "username",
        "schema", "table", "module", "sheetname", "extension", "bucket",
    )
    for key in interesting:
        if key in keys:
            continue
        val = attrs.get(key) or _step_attr(context, key, "")
        if val:
            lines.append(f"# preserved.{key}={val!r}")
    return lines


def _passthrough_unsupported(
    context: StepContext,
    label: str,
    reason: str,
    *,
    meta_keys: tuple[str, ...] = (),
) -> tuple[list[str], str]:
    """Document unsupported Output while continuing the pipeline with input DF."""
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [
        f"# {label}: {context.step.name}",
        f"# UNSUPPORTED: {reason}",
        f"# WARNING: No Databricks equivalent for Pentaho '{context.step.step_type}'. "
        "Metadata preserved; pipeline continues without performing the write.",
    ]
    lines.extend(_preserve_meta_comments(context, meta_keys))
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(
            f"{out_var} = spark.range(0).select(lit(1).alias('_{label.lower().replace(' ', '_')}')).limit(0)"
        )
    return lines, "converted"


def _generation_config(context: StepContext) -> GenerationConfig:
    cfg = context.extra.get("generation_config")
    if isinstance(cfg, GenerationConfig):
        return cfg
    return GenerationConfig.defaults()


def _salesforce_write(
    context: StepContext,
    *,
    label: str,
    write_mode: str,
) -> tuple[list[str], str]:
    """Shared Salesforce connector write for Insert/Update/Upsert/Delete."""
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    meta = get_converter_metadata(context)
    module = (
        _attr(context, "module", "")
        or _attr(context, "sfObject", "")
        or _attr(context, "object", "")
        or str(meta.get("module") or "Account")
    )
    url = _attr(context, "targeturl", "") or _attr(context, "url", "") or str(meta.get("url") or "")
    username = _attr(context, "username", "") or str(meta.get("username") or "")
    upsert_field = (
        _attr(context, "upsertfield", "")
        or _attr(context, "externalIdFieldName", "")
        or _attr(context, "upsert_field", "")
        or str(meta.get("upsert_field") or "")
    )
    batch = _attr(context, "batchSize", "") or _attr(context, "batch_size", "") or str(meta.get("batch_size") or "")

    lines = [
        f"# {label}: {context.step.name}",
        "# Requires a Salesforce Spark connector (e.g. com.springml.spark.salesforce).",
    ]
    if not in_df:
        return _passthrough(context, label)

    lines.append(f"{out_var} = {in_df}")
    lines.append("(")
    lines.append(f"    {out_var}.write.format('com.springml.spark.salesforce')")
    lines.append(f"    .option('sfObject', {module!r})")
    lines.append(f"    .mode({write_mode!r})")
    if url:
        lines.append(f"    .option('sfURL', {url!r})")
    if username:
        lines.append(f"    .option('sfUser', {username!r})")
        lines.append(
            "    .option('sfPassword', dbutils.secrets.get(scope='salesforce', key='password'))"
        )
    if upsert_field and write_mode.lower() in ("upsert", "update"):
        lines.append(f"    .option('upsertKey', {upsert_field!r})")
    if batch:
        lines.append(f"    .option('batchSize', {batch!r})")
    if write_mode.lower() == "delete":
        lines.append("    # Salesforce Delete uses Id (or external id) columns from the DataFrame")
    lines.append("    .save()")
    lines.append(")")
    lines.extend(_preserve_meta_comments(context, ("module", "upsert_field", "batch_size")))
    return lines, "converted"


# ---------------------------------------------------------------------------
# File / format Outputs
# ---------------------------------------------------------------------------


class ExcelWriterHandler(BaseStepHandler):
    """Microsoft Excel Writer — reuses shared spark-excel write from Excel Output."""

    _TYPES = {"excelwriter", "typeexcelwriter", "microsoftexcelwriter"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from .output_handlers import generate_excel_workbook_write

        return generate_excel_workbook_write(
            context,
            label="Microsoft Excel Writer",
            default_extension="xlsx",
            attr_fn=_attr,
        )



class AccessOutputHandler(BaseStepHandler):
    """Microsoft Access Output → JDBC write via UCanAccess."""

    _TYPES = {"accessoutput", "microsoftaccessoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "dbname", "name", default="output.accdb")
        table = _attr(context, "table", "") or _attr(context, "tablename", "export")
        create = _yn(context, "create_file", True) or _yn(context, "create", True)
        url = f"jdbc:ucanaccess://{path}"
        lines = [
            f"# Microsoft Access Output: {context.step.name}",
            "# Requires UCanAccess JDBC driver on the Databricks cluster classpath.",
            "# PARTIAL: Access writes via JDBC may need an interactive/driver cluster.",
        ]
        if not in_df:
            return _passthrough(context, "Microsoft Access Output")
        lines.append(f"{out_var} = {in_df}")
        mode = "overwrite" if create else "append"
        lines.append("(")
        lines.append(f"    {out_var}.write.format('jdbc')")
        lines.append(f"    .option('url', {url!r})")
        lines.append("    .option('driver', 'net.ucanaccess.jdbc.UcanaccessDriver')")
        lines.append(f"    .option('dbtable', {table!r})")
        lines.append(f"    .mode({mode!r})")
        lines.append("    .save()")
        lines.append(")")
        return lines, "converted"


class S3FileOutputHandler(BaseStepHandler):
    """S3 File Output → DataFrame write to ``s3a://`` / ``s3://`` paths."""

    _TYPES = {"s3fileoutput", "s3output"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name", default="")
        bucket = _attr(context, "bucket", "")
        fmt = (_attr(context, "file_format", "") or _attr(context, "format", "csv") or "csv").lower()
        separator = _attr(context, "separator", ",") or ","
        header = _yn(context, "header", True)

        if bucket and path and not path.startswith(("s3://", "s3a://", "dbfs:")):
            path = f"s3a://{bucket.rstrip('/')}/{path.lstrip('/')}"
        elif bucket and not path:
            path = f"s3a://{bucket}/output"
        elif path and not path.startswith(("s3://", "s3a://", "dbfs:", "/")):
            path = f"s3a://{path}"
        elif not path:
            path = "s3a://bucket/output"

        if fmt in ("txt", "text", "csv", "delimited"):
            spark_fmt = "csv"
        elif fmt in ("json", "parquet", "orc", "avro"):
            spark_fmt = fmt
        else:
            spark_fmt = "csv"

        lines = [f"# S3 File Output: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "S3 File Output")
        lines.append(f"{out_var} = {in_df}")
        lines.append("(")
        lines.append(f"    {out_var}.write")
        lines.append("    .mode('overwrite')")
        if spark_fmt == "csv":
            lines.append(f"    .option('header', {header!r})")
            lines.append(f"    .option('sep', {separator!r})")
            lines.append(f"    .csv({path!r})")
        else:
            lines.append(f"    .format({spark_fmt!r})")
            lines.append(f"    .save({path!r})")
        lines.append(")")
        return lines, "converted"


class SqlFileOutputHandler(BaseStepHandler):
    """SQL File Output → generate INSERT scripts written as text files."""

    _TYPES = {"sqlfileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name", default="output.sql")
        schema = _attr(context, "schema", "")
        table = _attr(context, "table", "") or _attr(context, "tablename", "target_table")
        full = qualify_table_name(table, schema, config=_generation_config(context))
        path_expr = spark_load_path_expr(path)

        lines = [
            f"# SQL File Output: {context.step.name}",
            "# Generates one INSERT … VALUES line per row as a text file.",
        ]
        if not in_df:
            return _passthrough(context, "SQL File Output")

        lines.extend([
            f"{out_var} = {in_df}",
            f"_tbl_{out_var} = {full!r}",
            f"_col_list_{out_var} = ', '.join(f'`{{c}}`' for c in {out_var}.columns)",
            f"_val_expr_{out_var} = concat_ws(', ', *[",
            f"    concat(lit(\"'\"), coalesce(col(c).cast('string'), lit('')), lit(\"'\"))",
            f"    for c in {out_var}.columns",
            f"])",
            f"_sql_lines_{out_var} = {out_var}.select(",
            f"    concat(",
            f"        lit('INSERT INTO '), lit(_tbl_{out_var}), lit(' ('),",
            f"        lit(_col_list_{out_var}), lit(') VALUES ('),",
            f"        _val_expr_{out_var}, lit(');')",
            f"    ).alias('value')",
            f")",
            f"_sql_lines_{out_var}.write.mode('overwrite').text({path_expr})",
        ])
        return lines, "converted"


class PropertiesOutputHandler(BaseStepHandler):
    """Properties Output → ``key=value`` text writer."""

    _TYPES = {"propertyoutput", "propertiesoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name", default="output.properties")
        key_col = _attr(context, "key_field", "") or _attr(context, "keyfield", "key") or "key"
        val_col = _attr(context, "value_field", "") or _attr(context, "valuefield", "value") or "value"
        comment = _attr(context, "comment", "")
        path_expr = spark_load_path_expr(path)

        lines = [f"# Properties Output: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Properties Output")
        lines.append(f"{out_var} = {in_df}")
        if comment:
            lines.append(f"# comment={comment!r}")
        lines.append(
            f"_props_{out_var} = {out_var}.select("
            f"concat(col({key_col!r}).cast('string'), lit('='), "
            f"coalesce(col({val_col!r}).cast('string'), lit(''))).alias('value'))"
        )
        lines.append(f"_props_{out_var}.write.mode('overwrite').text({path_expr})")
        return lines, "converted"


class RssOutputHandler(BaseStepHandler):
    """RSS Output → spark-xml RSS/Atom channel generation."""

    _TYPES = {"rssoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name", default="feed.xml")
        channel = _attr(context, "channel_title", "") or _attr(context, "title", "feed")
        row_tag = _attr(context, "item_tag", "item") or "item"
        path_expr = spark_load_path_expr(path)

        lines = [
            f"# RSS Output: {context.step.name}",
            "# PARTIAL: Emits XML suitable for an RSS item list via spark-xml; "
            "channel wrappers may need post-processing.",
            f"# channel_title={channel!r}",
        ]
        if not in_df:
            return _passthrough(context, "RSS Output")
        lines.append(f"{out_var} = {in_df}")
        lines.append("(")
        lines.append(f"    {out_var}.write.format('xml')")
        lines.append("    .option('rootTag', 'channel')")
        lines.append(f"    .option('rowTag', {row_tag!r})")
        lines.append("    .mode('overwrite')")
        lines.append(f"    .save({path_expr})")
        lines.append(")")
        return lines, "converted"


class LdapOutputHandler(BaseStepHandler):
    """LDAP Output — no native Spark writer; preserve connection metadata."""

    _TYPES = {"ldapoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        host = _attr(context, "host", "") or _attr(context, "hostname", "localhost")
        port = _attr(context, "port", "389") or "389"
        base_dn = _attr(context, "searchBase", "") or _attr(context, "base_dn", "")
        operation = _attr(context, "operation", "upsert") or "upsert"

        lines = [
            f"# LDAP Output: {context.step.name}",
            "# PARTIAL: Databricks has no built-in LDAP writer. "
            "Use ldap3 / JNDI on the driver, or an external identity sync job.",
            f"# host={host!r} port={port!r} base_dn={base_dn!r} operation={operation!r}",
            "# WARNING: LDAP write side-effects are not executed by generated Spark code.",
        ]
        lines.extend(_preserve_meta_comments(context, ("host", "port", "searchBase", "operation", "dnField")))
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], 'dn STRING').limit(0)")
        lines.append(
            f"# TODO: apply LDAP {operation} for each row in {out_var} against {host}:{port}"
        )
        return lines, "converted"


# ---------------------------------------------------------------------------
# Salesforce Outputs
# ---------------------------------------------------------------------------


class SalesforceInsertHandler(BaseStepHandler):
    _TYPES = {"salesforceinsert"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _salesforce_write(context, label="Salesforce Insert", write_mode="append")


class SalesforceUpdateHandler(BaseStepHandler):
    _TYPES = {"salesforceupdate"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _salesforce_write(context, label="Salesforce Update", write_mode="update")


class SalesforceUpsertHandler(BaseStepHandler):
    _TYPES = {"salesforceupsert"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _salesforce_write(context, label="Salesforce Upsert", write_mode="upsert")


class SalesforceDeleteHandler(BaseStepHandler):
    _TYPES = {"salesforcedelete"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _salesforce_write(context, label="Salesforce Delete", write_mode="delete")


# ---------------------------------------------------------------------------
# Unsupported / metadata-preserving Outputs
# ---------------------------------------------------------------------------


class SerializeToFileHandler(BaseStepHandler):
    """Serialize to File (CubeOutput) — Java serialization not portable to Spark."""

    _TYPES = {"cubeoutput", "serializetofile", "serialisetofile"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        path = _resolve_path(context, "file", "filename", "name")
        lines, status = _passthrough_unsupported(
            context,
            "Serialize to File",
            "Java ObjectOutputStream cube files have no Spark DataFrame equivalent. "
            "Prefer Parquet/Delta for interop, or pickle only for single-node Python jobs.",
            meta_keys=("file", "filename"),
        )
        if path:
            lines.insert(2, f"# serialized_cube_path={path!r}")
        return lines, status


class AutomaticDocumentationOutputHandler(BaseStepHandler):
    """Automatic Documentation Output — generate a markdown/text artifact from metadata."""

    _TYPES = {
        "autodoc",
        "automaticdocumentationoutput",
        "autodocoutput",
    }

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        path = _resolve_path(context, "filename", "file", "name", default="transformation_docs.md")
        target_type = _attr(context, "output_type", "") or _attr(context, "target_type", "markdown")
        path_expr = spark_load_path_expr(path)
        trans_name = context.transformation.name if context.transformation else context.step.name

        lines = [
            f"# Automatic Documentation Output: {context.step.name}",
            "# Emits a lightweight documentation text artifact from transformation metadata.",
            "# Full Pentaho AutoDoc PDF/HTML diagrams are not reproduced on Databricks.",
            f"_doc_rows_{out_var} = spark.createDataFrame([",
            f"    ('# Migration documentation: {trans_name}',),",
            f"    ('Source step: {context.step.name}',),",
            f"    ('Target format hint: {target_type}',),",
            "], ['value'])",
            f"_doc_rows_{out_var}.write.mode('overwrite').text({path_expr})",
        ]
        lines.extend(_preserve_meta_comments(context, ("filename", "output_type", "include_name")))
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = _doc_rows_{out_var}")
        return lines, "converted"


class PentahoReportingOutputHandler(BaseStepHandler):
    """Pentaho Reporting Output — no Databricks equivalent for PRPT rendering."""

    _TYPES = {
        "pentahoreportingoutput",
        "reportExport",
        "reportexport",
        "prptoutput",
        "pentahoreporting",
    }

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in {x.lower() for x in self._TYPES}

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        return _passthrough_unsupported(
            context,
            "Pentaho Reporting Output",
            "PRPT / Pentaho Reporting engine is not available on Databricks. "
            "Preserve report definition paths and render externally or rebuild with a BI tool.",
            meta_keys=("filename", "file", "report_file", "output_type", "parameter"),
        )


# Export registration list
EXTRA_OUTPUT_HANDLERS: list[BaseStepHandler] = [
    ExcelWriterHandler(),
    AccessOutputHandler(),
    S3FileOutputHandler(),
    SqlFileOutputHandler(),
    PropertiesOutputHandler(),
    RssOutputHandler(),
    LdapOutputHandler(),
    SalesforceInsertHandler(),
    SalesforceUpdateHandler(),
    SalesforceUpsertHandler(),
    SalesforceDeleteHandler(),
    SerializeToFileHandler(),
    AutomaticDocumentationOutputHandler(),
    PentahoReportingOutputHandler(),
]
