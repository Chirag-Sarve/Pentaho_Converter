"""Handlers for additional Pentaho step types (DB ops, file formats, integrations)."""

from __future__ import annotations

import logging

from ..generation_config import GenerationConfig
from ..metadata_propagation import get_converter_metadata
from ..table_names import qualify_table_name
from ..step_xml import _child_text, get_step_element
from .advanced_handlers import _passthrough
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _generation_config(context: StepContext) -> GenerationConfig:
    cfg = context.extra.get("generation_config")
    if isinstance(cfg, GenerationConfig):
        return cfg
    return GenerationConfig.defaults()


def _qualified_table(context: StepContext, schema: str, table: str) -> str:
    return qualify_table_name(table, schema, config=_generation_config(context))


class CsvFileOutputHandler(BaseStepHandler):
    """CSV Output → Spark CSV writer preserving Pentaho options."""

    _TYPES = {"csvoutput", "csvfileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..path_utils import spark_save_path_expr

        in_df = context.input_df_name() or context.output_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        filename = (
            self._attr(context, "filename", "")
            or self._attr(context, "file", "")
            or str(meta.get("filename") or "")
        )
        separator = (
            self._attr(context, "separator", "")
            or str(meta.get("separator") or ",")
            or ","
        )
        header_raw = self._attr(context, "header", "") or str(meta.get("header") or "Y")
        header = header_raw.strip().upper() != "N"
        quote = (
            self._attr(context, "enclosure", "")
            or self._attr(context, "quote", "")
            or str(meta.get("enclosure") or meta.get("quote") or '"')
            or '"'
        )
        encoding = self._attr(context, "encoding", "") or str(meta.get("encoding") or "")
        append = (self._attr(context, "append", "N") or str(meta.get("append") or "N")).upper() == "Y"
        mode = "append" if append else "overwrite"
        path_expr = spark_save_path_expr(filename)

        lines = [
            f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'CsvOutput'})",
        ]
        if not filename:
            lines.append("# WARNING: Output filename missing from Pentaho metadata.")
        lines.append(f"{out_var} = {in_df}")
        lines.append("(")
        lines.append(f"    {out_var}.write")
        lines.append(f"    .mode({mode!r})")
        lines.append(f"    .option(\"header\", {header})")
        lines.append(f"    .option(\"sep\", {separator!r})")
        lines.append(f"    .option(\"quote\", {quote!r})")
        if encoding:
            lines.append(f"    .option(\"encoding\", {encoding!r})")
        lines.append(f"    .csv({path_expr})")
        lines.append(")")
        return lines, "converted"


def _db_output_keys(context: StepContext) -> list[tuple[str, str]]:
    """Return (stream_field, table_field) pairs for MERGE ON conditions."""
    meta = get_converter_metadata(context)
    keys = meta.get("keys") or []
    pairs: list[tuple[str, str]] = []
    for item in keys:
        if isinstance(item, dict):
            stream = (item.get("stream_field") or "").strip()
            table = (item.get("table_field") or stream).strip()
            if stream or table:
                pairs.append((stream or table, table or stream))
    if pairs:
        return pairs
    return [(f.name, f.name) for f in context.step.fields if f.name]


def _db_update_set_clause(context: StepContext) -> str:
    """Build MERGE UPDATE SET assignments from mapped value fields."""
    meta = get_converter_metadata(context)
    values = meta.get("values") or []
    assignments: list[str] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        if item.get("update") is False:
            continue
        stream = (item.get("stream_field") or "").strip()
        table = (item.get("table_field") or stream).strip()
        if table and stream:
            assignments.append(f"t.`{table}` = s.`{stream}`")
    return ", ".join(assignments) if assignments else "*"


def _db_insert_columns(context: StepContext) -> tuple[str, str]:
    """Return (column_list, source_list) for MERGE INSERT, or ('*', '*')."""
    meta = get_converter_metadata(context)
    values = meta.get("values") or []
    cols: list[str] = []
    srcs: list[str] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        stream = (item.get("stream_field") or "").strip()
        table = (item.get("table_field") or stream).strip()
        if table and stream:
            cols.append(f"`{table}`")
            srcs.append(f"s.`{stream}`")
    if not cols:
        return "*", "*"
    return ", ".join(cols), ", ".join(srcs)


class InsertUpdateHandler(BaseStepHandler):
    """Insert / Update → Delta ``MERGE INTO`` upsert."""

    _TYPES = {"insertupdate"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        schema = self._attr(context, "schema", "") or str(meta.get("schema") or "")
        table = self._attr(context, "table", "") or str(meta.get("table") or "target_table")
        full = _qualified_table(context, schema, table)
        key_pairs = _db_output_keys(context)
        lines = [f"# Insert/Update: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Insert/Update")
        lines.append(f"_upsert_src = {in_df}")
        lines.append(f"_upsert_src.createOrReplaceTempView('_upsert_src')")
        if key_pairs:
            merge_cond = " AND ".join(
                f"t.`{table_f}` = s.`{stream_f}`" for stream_f, table_f in key_pairs
            )
            set_clause = _db_update_set_clause(context)
            insert_cols, insert_vals = _db_insert_columns(context)
            if insert_cols == "*":
                insert_sql = "INSERT *"
            else:
                insert_sql = f"INSERT ({insert_cols}) VALUES ({insert_vals})"
            update_sql = (
                f"UPDATE SET {set_clause}" if set_clause != "*" else "UPDATE SET *"
            )
            lines.append(
                f"spark.sql('''MERGE INTO {full} t USING _upsert_src s "
                f"ON {merge_cond} WHEN MATCHED THEN {update_sql} "
                f"WHEN NOT MATCHED THEN {insert_sql}''')"
            )
        else:
            lines.append(
                f"# WARNING: No lookup keys — falling back to append into {full}"
            )
            lines.append(f"_upsert_src.write.format('delta').mode('append').saveAsTable({full!r})")
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class UpdateHandler(BaseStepHandler):
    """Update → Delta ``MERGE INTO … WHEN MATCHED THEN UPDATE``."""

    _TYPES = {"update"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        schema = self._attr(context, "schema", "") or str(meta.get("schema") or "")
        table = self._attr(context, "table", "") or str(meta.get("table") or "target_table")
        full = _qualified_table(context, schema, table)
        key_pairs = _db_output_keys(context)
        lines = [f"# Update: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Update")
        lines.append(f"_update_src = {in_df}")
        lines.append("_update_src.createOrReplaceTempView('_update_src')")
        if key_pairs:
            merge_cond = " AND ".join(
                f"t.`{table_f}` = s.`{stream_f}`" for stream_f, table_f in key_pairs
            )
            set_clause = _db_update_set_clause(context)
            update_sql = (
                f"UPDATE SET {set_clause}" if set_clause != "*" else "UPDATE SET *"
            )
            lines.append(
                f"spark.sql('''MERGE INTO {full} t USING _update_src s "
                f"ON {merge_cond} WHEN MATCHED THEN {update_sql}''')"
            )
        else:
            lines.append(
                f"# WARNING: No key fields — overwrite of {full} may be incorrect"
            )
            lines.append(
                f"_update_src.write.format('delta').mode('overwrite').saveAsTable({full!r})"
            )
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class DeleteHandler(BaseStepHandler):
    """Delete → Delta ``MERGE INTO … WHEN MATCHED THEN DELETE``."""

    _TYPES = {"delete"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        schema = self._attr(context, "schema", "") or str(meta.get("schema") or "")
        table = self._attr(context, "table", "") or str(meta.get("table") or "target_table")
        full = _qualified_table(context, schema, table)
        key_pairs = _db_output_keys(context)
        lines = [f"# Delete: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Delete")
        lines.append(f"_delete_src = {in_df}")
        lines.append("_delete_src.createOrReplaceTempView('_delete_src')")
        if key_pairs:
            merge_cond = " AND ".join(
                f"t.`{table_f}` = s.`{stream_f}`" for stream_f, table_f in key_pairs
            )
            lines.append(
                f"spark.sql('''MERGE INTO {full} t USING _delete_src s "
                f"ON {merge_cond} WHEN MATCHED THEN DELETE''')"
            )
        else:
            lines.append(
                f"# WARNING: No delete keys configured for {context.step.name} — "
                f"review MERGE against {full}"
            )
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class JsonOutputHandler(BaseStepHandler):
    """JSON Output → ``DataFrame.write.json``."""

    _TYPES = {"jsonoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..path_utils import spark_save_path_expr

        in_df = context.input_df_name() or context.output_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        filename = (
            self._attr(context, "filename", "")
            or self._attr(context, "fileName", "")
            or self._attr(context, "file", "")
            or str(meta.get("filename") or "")
        )
        ext = self._attr(context, "extension", "") or str(meta.get("extension") or "")
        append = (self._attr(context, "append", "N") or str(meta.get("append") or "N")).upper() == "Y"
        mode = "append" if append else "overwrite"
        if filename and ext and not filename.endswith(f".{ext}") and "." not in filename.rsplit("/", 1)[-1]:
            filename = f"{filename}.{ext}"
        path_expr = spark_save_path_expr(filename)
        lines = [
            f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'JsonOutput'})",
        ]
        if not filename:
            lines.append("# WARNING: Output filename missing from Pentaho metadata.")
        lines.append(f"{out_var} = {in_df}")
        lines.append(
            f"{out_var}.write \\\n"
            f"    .mode({mode!r}) \\\n"
            f"    .json(\n"
            f"        {path_expr}\n"
            f"    )"
        )
        if meta.get("json_bloc") or meta.get("output_value"):
            lines.append(
                f"# preserved.json_bloc={meta.get('json_bloc')!r} "
                f"output_value={meta.get('output_value')!r}"
            )
        return lines, "converted"


class XmlOutputHandler(BaseStepHandler):
    """XML Output → spark-xml writer (``com.databricks.spark.xml``)."""

    _TYPES = {"xmloutput", "xmlpad", "xmlwriter"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..path_utils import spark_load_path_expr

        in_df = context.input_df_name() or context.output_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        filename = (
            self._attr(context, "filename", "")
            or self._attr(context, "file", "")
            or str(meta.get("filename") or "output.xml")
        )
        root_tag = (
            self._attr(context, "root_tag", "")
            or self._attr(context, "mainElement", "")
            or str(meta.get("root_tag") or "rows")
        )
        row_tag = (
            self._attr(context, "row_tag", "")
            or self._attr(context, "repeatElement", "")
            or str(meta.get("row_tag") or "row")
        )
        path_expr = spark_load_path_expr(filename)
        lines = [
            f"# XML Output: {context.step.name}",
            "# Requires spark-xml (com.databricks.spark.xml) on the cluster classpath.",
            f"{out_var} = {in_df}",
            f"(",
            f"    {out_var}.write.format('xml')",
            f"    .option('rootTag', {root_tag!r})",
            f"    .option('rowTag', {row_tag!r})",
            "    .mode('overwrite')",
            f"    .save({path_expr})",
            ")",
        ]
        return lines, "converted"


class ParquetInputHandler(BaseStepHandler):
    _TYPES = {"parquetinput", "parquetfileinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", ""))
        lines = [f"# Parquet Input: {context.step.name}"]
        lines.append(f"{out_var} = spark.read.parquet({filename!r})")
        return lines, "converted" if filename else "converted"


class ParquetOutputHandler(BaseStepHandler):
    """Parquet Output → ``DataFrame.write.parquet``."""

    _TYPES = {"parquetoutput", "parquetfileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..path_utils import spark_save_path_expr

        in_df = context.input_df_name() or context.output_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        filename = (
            self._attr(context, "filename", "")
            or self._attr(context, "file", "")
            or str(meta.get("filename") or "")
        )
        compression = self._attr(context, "compression", "") or str(meta.get("compression") or "")
        path_expr = spark_save_path_expr(filename)
        lines = [
            f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'ParquetOutput'})",
        ]
        if not filename:
            lines.append("# WARNING: Output filename missing from Pentaho metadata.")
        lines.append(f"{out_var} = {in_df}")
        if compression and compression.lower() not in ("none", ""):
            lines.append(f"# preserved.compression={compression!r}")
        lines.append(
            f"{out_var}.write \\\n"
            f"    .mode('overwrite') \\\n"
            f"    .parquet(\n"
            f"        {path_expr}\n"
            f"    )"
        )
        return lines, "converted"


class DeltaFileOutputHandler(BaseStepHandler):
    """Delta File Output → ``format('delta').save`` (path write, not table)."""

    _TYPES = {"deltaoutput", "deltafileoutput", "writeoutdelta"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..path_utils import spark_save_path_expr

        in_df = context.input_df_name() or context.output_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        filename = (
            self._attr(context, "filename", "")
            or self._attr(context, "file", "")
            or self._attr(context, "path", "")
            or str(meta.get("filename") or meta.get("path") or "")
        )
        append = (self._attr(context, "append", "N") or str(meta.get("append") or "N")).upper() == "Y"
        mode = "append" if append else "overwrite"
        path_expr = spark_save_path_expr(filename)
        lines = [
            f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'DeltaOutput'})",
        ]
        if not filename:
            lines.append("# WARNING: Output filename missing from Pentaho metadata.")
        lines.append(f"{out_var} = {in_df}")
        lines.append(
            f"{out_var}.write \\\n"
            f"    .format(\"delta\") \\\n"
            f"    .mode({mode!r}) \\\n"
            f"    .save(\n"
            f"        {path_expr}\n"
            f"    )"
        )
        return lines, "converted"


class OrcInputHandler(BaseStepHandler):
    _TYPES = {"orcinput", "orcfileinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", ""))
        lines = [f"# ORC Input: {context.step.name}"]
        lines.append(f"{out_var} = spark.read.format('orc').load({filename!r})")
        return lines, "converted" if filename else "converted"


class OrcOutputHandler(BaseStepHandler):
    _TYPES = {"orcoutput", "orcfileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name() or context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", "output.orc"))
        lines = [f"# ORC Output: {context.step.name}"]
        lines.append(f"{in_df}.write.format('orc').mode('overwrite').save({filename!r})")
        return lines, "converted"


class AvroInputHandler(BaseStepHandler):
    """Avro Input → ``spark.read.format('avro')`` with schema/compression preserved."""

    _TYPES = {"avroinput", "avrofileinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..path_utils import spark_load_path_expr

        out_var = context.output_df_name()
        try:
            meta = get_converter_metadata(context)
            filename = (
                self._attr(context, "filename", "")
                or self._attr(context, "file", "")
                or self._attr(context, "dataLocation", "")
                or str(meta.get("filename") or meta.get("file") or "")
            )
            schema_filename = (
                self._attr(context, "schemaFilename", "")
                or self._attr(context, "schema_filename", "")
                or self._attr(context, "schemaLocation", "")
                or str(meta.get("schema_filename") or "")
            )
            schema_json = str(meta.get("schema_json") or "")
            compression = (
                self._attr(context, "compression", "")
                or self._attr(context, "codec", "")
                or str(meta.get("compression") or "")
            )
            encoding = self._attr(context, "encoding", "") or str(meta.get("encoding") or "")
            recursive = str(
                meta.get("recursive")
                or self._attr(context, "include_subfolders", "")
                or self._attr(context, "recursive", "")
                or ""
            ).strip().upper() in ("Y", "YES", "TRUE", "1")
            path_expr = spark_load_path_expr(filename)

            lines = [
                f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'AvroInput'})",
                "# Native Databricks Avro read (spark-avro).",
            ]
            if not filename:
                logger.warning("Avro Input '%s': missing input path", context.step.name)
                lines.append("# WARNING: Avro input path missing from Pentaho metadata.")
                lines.append(
                    f"{out_var} = spark.read.format('avro').load('')  "
                    "# TODO: set Avro path before running on Databricks"
                )
                return lines, "converted"

            lines.append(f"{out_var} = (")
            lines.append("    spark.read.format('avro')")
            if schema_filename:
                schema_expr = spark_load_path_expr(schema_filename)
                lines.append(f"    .option('avroSchemaUrl', {schema_expr})")
            elif schema_json:
                safe = schema_json.replace("\\", "\\\\").replace("'", "\\'")
                lines.append(f"    .option('avroSchema', '{safe}')")
            else:
                lines.append(
                    "# WARNING: No Avro schema in Pentaho metadata — "
                    "Spark will infer schema from data files."
                )
            if compression and compression.lower() not in ("none", ""):
                lines.append(f"    .option('compression', {compression!r})")
                lines.append(f"# preserved.compression={compression!r}")
            if encoding:
                lines.append(f"# preserved.encoding={encoding!r}")
            binary = str(
                meta.get("is_binary_encoded")
                or self._attr(context, "isDataBinaryEncoded", "")
                or ""
            )
            if binary:
                lines.append(f"# preserved.is_binary_encoded={binary!r}")
            if recursive:
                lines.append("    .option('recursiveFileLookup', 'true')")
                lines.append("# preserved.recursive=True")
            filemask = str(meta.get("filemask") or self._attr(context, "filemask", "") or "")
            if filemask:
                lines.append(f"# preserved.filemask={filemask!r}")
            if meta.get("schema_in_field") or meta.get("data_in_field"):
                lines.append(
                    "# WARNING: Avro-decode-from-stream-field is not emitted as native "
                    "format('avro') — field-level decode requires from_avro UDF mapping."
                )
                lines.append(
                    f"# preserved.schema_in_field={meta.get('schema_in_field')!r} "
                    f"data_in_field={meta.get('data_in_field')!r}"
                )
            lines.append(f"    .load({path_expr})")
            lines.append(")")
            fields = meta.get("fields") or []
            if fields:
                names = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
                if names:
                    lines.append(f"# preserved.avro_fields={names!r}")
            logger.info(
                "AvroInput %s path=%s schema=%s compression=%s",
                context.step.name,
                filename,
                schema_filename or ("embedded" if schema_json else "infer"),
                compression or "none",
            )
            return lines, "converted"
        except Exception as exc:
            logger.exception("AvroInput failed for %s", context.step.name)
            return [
                f"# Avro Input: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.read.format('avro').load('')",
            ], "partial"


class AvroOutputHandler(BaseStepHandler):
    """Avro Output → ``DataFrame.write.format('avro')`` with mode/compression."""

    _TYPES = {"avrooutput", "avrofileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..path_utils import spark_save_path_expr, spark_load_path_expr

        in_df = context.input_df_name() or context.output_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        filename = (
            self._attr(context, "filename", "")
            or self._attr(context, "file", "")
            or str(meta.get("filename") or meta.get("file") or "")
        )
        append = (
            self._attr(context, "append", "")
            or str(meta.get("append") or "N")
        ).strip().upper() in ("Y", "YES", "TRUE", "1")
        overwrite_raw = (
            self._attr(context, "overwrite", "")
            or str(meta.get("overwrite") or "")
        ).strip().upper()
        if append:
            mode = "append"
        elif overwrite_raw in ("N", "NO", "FALSE", "0"):
            mode = "errorifexists"
        else:
            mode = "overwrite"
        compression = (
            self._attr(context, "compression", "")
            or self._attr(context, "codec", "")
            or str(meta.get("compression") or "")
        )
        schema_filename = str(meta.get("schema_filename") or "")
        schema_json = str(meta.get("schema_json") or "")
        schema_evolution = str(meta.get("schema_evolution") or "")
        path_expr = spark_save_path_expr(filename)

        lines = [
            f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'AvroOutput'})",
            "# Native Databricks Avro write (spark-avro).",
        ]
        if not filename:
            lines.append("# WARNING: Avro output path missing from Pentaho metadata.")
        if not in_df:
            return _passthrough(context, "Avro Output")

        lines.append(f"{out_var} = {in_df}")
        lines.append("(")
        lines.append(f"    {out_var}.write.format('avro')")
        if mode == "errorifexists":
            lines.append("    .mode('errorifexists')")
            lines.append(
                "# preserved.overwrite='N' — Spark SaveMode ErrorIfExists "
                "(fails if path already exists)."
            )
        else:
            lines.append(f"    .mode({mode!r})")
        if compression and compression.lower() not in ("none", ""):
            lines.append(f"    .option('compression', {compression.lower()!r})")
            lines.append(f"# preserved.compression={compression!r}")
        if schema_filename:
            lines.append(
                f"    .option('avroSchemaUrl', {spark_load_path_expr(schema_filename)})"
            )
        elif schema_json:
            safe = schema_json.replace("\\", "\\\\").replace("'", "\\'")
            lines.append(f"    .option('avroSchema', '{safe}')")
        if schema_evolution:
            lines.append(f"# preserved.schema_evolution={schema_evolution!r}")
            lines.append(
                "# NOTE: Spark Avro schema evolution is controlled by the writer "
                "schema / nullability; review nullable fields when migrating."
            )
        for key in ("namespace", "record_name", "doc", "encoding"):
            val = meta.get(key)
            if val:
                lines.append(f"# preserved.{key}={val!r}")
        lines.append(f"    .save({path_expr})")
        lines.append(")")
        return lines, "converted"


def _mongo_meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


def _mongo_yn(value: object) -> bool:
    return str(value or "").strip().upper() in ("Y", "YES", "TRUE", "1")


def _mongo_option_lines(meta: dict) -> list[str]:
    """Shared `.option(...)` lines for Mongo Spark connector reads/writes."""
    lines: list[str] = []
    uri = str(meta.get("connection_uri") or "").strip()
    database = str(meta.get("database") or "").strip()
    collection = str(meta.get("collection") or "").strip()
    if uri:
        # Prefer secrets for credentials embedded as ${mongodb_password}
        if "${mongodb_password}" in uri:
            lines.append(
                "    # Replace ${mongodb_password} via dbutils.secrets or cluster env."
            )
            lines.append(
                "    .option('connection.uri', "
                f"{uri!r}.replace('${{mongodb_password}}', "
                "dbutils.secrets.get(scope='mongodb', key='password')))"
            )
        else:
            lines.append(f"    .option('connection.uri', {uri!r})")
    if database:
        lines.append(f"    .option('database', {database!r})")
    if collection:
        lines.append(f"    .option('collection', {collection!r})")
    return lines


class MongoDBInputHandler(BaseStepHandler):
    """MongoDB Input → Spark MongoDB Connector ``format('mongodb')`` read."""

    _TYPES = {"mongodbinput", "mongoinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        meta = _mongo_meta(context)
        # Prefer live XML attrs when metadata cache is empty
        for key, aliases in (
            ("hostname", ("hostname", "host")),
            ("port", ("port",)),
            ("database", ("db_name", "database", "dbname")),
            ("collection", ("collection", "collection_name")),
            ("query", ("query", "json_query")),
            ("projection", ("fields_name", "fields_expression", "projection")),
            ("aggregation_pipeline", ("agg_pipeline", "aggregation_pipeline", "pipeline")),
            ("auth_user", ("auth_user", "username")),
            ("read_preference", ("read_preference", "read_pref")),
            ("batch_size", ("batch_size", "batchSize", "size")),
            ("connection_uri", ("connection_uri", "connection_string", "uri")),
        ):
            if not meta.get(key):
                for alias in aliases:
                    val = self._attr(context, alias, "")
                    if val:
                        meta[key] = val
                        break

        # Rebuild URI if only host pieces present
        if not meta.get("connection_uri") and meta.get("hostname"):
            from ..step_xml import _mongodb_connection_uri

            meta["connection_uri"] = _mongodb_connection_uri(meta)

        uri = str(meta.get("connection_uri") or "").strip()
        database = str(meta.get("database") or "").strip()
        collection = str(meta.get("collection") or "").strip()
        query = str(meta.get("query") or "").strip()
        projection = str(meta.get("projection") or "").strip()
        pipeline = str(meta.get("aggregation_pipeline") or "").strip()
        is_pipeline = _mongo_yn(meta.get("query_is_pipeline"))
        read_pref = str(meta.get("read_preference") or "").strip()
        batch_size = str(meta.get("batch_size") or "").strip()
        output_json = _mongo_yn(meta.get("output_json"))
        json_field = str(meta.get("json_field") or "json").strip() or "json"

        lines = [
            f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'MongoDbInput'})",
            "# Requires MongoDB Spark Connector (format 'mongodb') on the cluster.",
        ]

        if not uri and not (database and collection):
            lines.append("# WARNING: MongoDB connection URI / database / collection missing.")
            empty_col = json_field if output_json else "_id"
            lines.append(
                "# Template: spark.read.format('mongodb')"
                ".option('connection.uri', ...).option('database', ...)"
                ".option('collection', ...).load()"
            )
            lines.append(
                f"{out_var} = spark.createDataFrame([], "
                f"'{empty_col} STRING').limit(0)"
            )
            lines.append(
                "# TODO: configure connection.uri, database, and collection before run."
            )
            for key in (
                "hostname", "database", "collection", "query", "projection",
                "auth_user", "auth_database", "auth_mechanism", "read_preference",
            ):
                if meta.get(key):
                    lines.append(f"# preserved.{key}={meta.get(key)!r}")
            if meta.get("hostname") and meta.get("port"):
                lines.append(f"# preserved.port={meta.get('port')!r}")
            return lines, "converted"

        if not collection:
            lines.append("# WARNING: MongoDB collection missing — query may fail at runtime.")
        if meta.get("auth_user") and "${mongodb_password}" not in uri and ":@" not in uri.replace("://", ""):
            lines.append(
                "# WARNING: Auth user present — ensure credentials are supplied via "
                "connection.uri or Databricks secrets (auth failures otherwise)."
            )

        lines.append(f"{out_var} = (")
        lines.append("    spark.read.format('mongodb')")
        lines.extend(_mongo_option_lines(meta))
        if is_pipeline and pipeline:
            lines.append(f"    .option('aggregation.pipeline', {pipeline!r})")
            lines.append(f"# preserved.aggregation_pipeline={pipeline!r}")
        else:
            stages: list[str] = []
            if query and not query.strip().startswith("["):
                stages.append(f'{{"$match": {query}}}')
                lines.append(f"# preserved.query={query!r}")
            elif query:
                lines.append(f"# preserved.query={query!r}")
            if projection and projection.strip().startswith("{"):
                stages.append(f'{{"$project": {projection}}}')
                lines.append(f"# preserved.projection={projection!r}")
            elif projection:
                lines.append(f"# preserved.projection={projection!r}")
            if stages:
                combined = "[" + ", ".join(stages) + "]"
                lines.append(f"    .option('aggregation.pipeline', {combined!r})")
            elif query and query.strip().startswith("["):
                lines.append(f"    .option('aggregation.pipeline', {query!r})")
        if read_pref:
            lines.append(f"    .option('readPreference.name', {read_pref.replace(' ', '')!r})")
            lines.append(f"# preserved.read_preference={read_pref!r}")
        if batch_size:
            lines.append(f"    .option('batchSize', {batch_size!r})")
            lines.append(f"# preserved.batch_size={batch_size!r}")
        for key in (
            "auth_mechanism", "auth_database", "connect_timeout", "socket_timeout",
            "tag_sets", "use_ssl", "kerberos", "use_all_replica_members",
        ):
            if meta.get(key):
                lines.append(f"# preserved.{key}={meta.get(key)!r}")
        lines.append("    .load()")
        lines.append(")")

        if output_json:
            lines.append(
                f"# Pentaho 'output single JSON field' — coerce row to JSON string column {json_field!r}."
            )
            lines.append(
                f"{out_var} = {out_var}.select("
                f"to_json(struct('*')).alias({json_field!r}))"
            )
        fields = meta.get("fields") or []
        if fields and not output_json:
            names = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
            if names:
                lines.append(f"# preserved.mongo_fields={names!r}")
                # Select known fields when present; missing columns handled at runtime
                cols = ", ".join(repr(n) for n in names)
                lines.append(
                    f"# Optional field projection from Pentaho Fields tab: "
                    f"{out_var}.select({cols})"
                )
        lines.append(
            "# NOTE: Empty collections return an empty DataFrame; "
            "null BSON values map to Spark nulls."
        )
        return lines, "converted"


class MongoDBOutputHandler(BaseStepHandler):
    """MongoDB Output → Spark MongoDB Connector ``format('mongodb')`` write."""

    _TYPES = {"mongodboutput", "mongooutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name() or context.output_df_name()
        out_var = context.output_df_name()
        meta = _mongo_meta(context)
        for key, aliases in (
            ("hostname", ("hostname", "host")),
            ("port", ("port",)),
            ("database", ("db_name", "database", "dbname")),
            ("collection", ("collection", "collection_name")),
            ("auth_user", ("auth_user", "username")),
            ("batch_size", ("batch_insert_size", "batch_size", "batchSize")),
            ("connection_uri", ("connection_uri", "connection_string", "uri")),
            ("truncate", ("truncate", "drop")),
            ("upsert", ("upsert", "modifier_update")),
            ("update", ("update", "do_updates")),
        ):
            if not meta.get(key):
                for alias in aliases:
                    val = self._attr(context, alias, "")
                    if val:
                        meta[key] = val
                        break

        if not meta.get("connection_uri") and meta.get("hostname"):
            from ..step_xml import _mongodb_connection_uri

            meta["connection_uri"] = _mongodb_connection_uri(meta)

        uri = str(meta.get("connection_uri") or "").strip()
        database = str(meta.get("database") or "").strip()
        collection = str(meta.get("collection") or "").strip()
        truncate = _mongo_yn(meta.get("truncate"))
        upsert = _mongo_yn(meta.get("upsert"))
        update = _mongo_yn(meta.get("update"))
        batch_size = str(meta.get("batch_size") or "").strip()
        match_fields = list(meta.get("match_fields") or [])
        write_concern = str(meta.get("write_concern") or "").strip()

        lines = [
            f"# Pentaho step: {context.step.name} (type: {context.step.step_type or 'MongoDbOutput'})",
            "# Requires MongoDB Spark Connector (format 'mongodb') on the cluster.",
        ]
        if not in_df:
            return _passthrough(context, "MongoDB Output")

        if not uri and not (database and collection):
            lines.append("# WARNING: MongoDB connection URI / database / collection missing.")
            lines.append("# UNSUPPORTED write skipped — metadata preserved; pipeline continues.")
            lines.append(
                "# Template: df.write.format('mongodb')"
                ".option('connection.uri', ...).option('database', ...)"
                ".option('collection', ...).mode('append').save()"
            )
            for key in (
                "hostname", "database", "collection", "auth_user",
                "batch_size", "upsert", "update", "truncate",
            ):
                if meta.get(key):
                    lines.append(f"# preserved.{key}={meta.get(key)!r}")
            if meta.get("hostname") and meta.get("port"):
                lines.append(f"# preserved.port={meta.get('port')!r}")
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"

        if not collection:
            lines.append(
                "# WARNING: Invalid/missing collection — write will fail at runtime "
                "until collection is set."
            )

        if truncate:
            mode = "overwrite"
        elif upsert or update:
            mode = "append"  # connector update/upsert via operationType
        else:
            mode = "append"

        lines.append(f"{out_var} = {in_df}")
        lines.append("(")
        lines.append(f"    {out_var}.write.format('mongodb')")
        lines.extend(_mongo_option_lines(meta))
        lines.append(f"    .mode({mode!r})")
        if upsert or update:
            op = "update"
            lines.append(f"    .option('operationType', {op!r})")
            if upsert:
                lines.append("    .option('upsertDocument', 'true')")
                lines.append("# preserved.upsert=True")
            id_fields = match_fields or ["_id"]
            lines.append(f"    .option('idFieldList', {','.join(id_fields)!r})")
            lines.append(
                "# NOTE: Upsert match fields from Pentaho mapped to connector idFieldList."
            )
        if batch_size:
            lines.append(f"    .option('maxBatchSize', {batch_size!r})")
            lines.append(f"# preserved.batch_size={batch_size!r}")
        if write_concern:
            lines.append(f"# preserved.write_concern={write_concern!r}")
            lines.append(
                "# WARNING: Custom writeConcern is not uniformly supported — "
                "configure via connection.uri query params if required."
            )
        for key in (
            "auth_mechanism", "auth_database", "multi", "modifier_update",
            "use_ssl", "kerberos", "retry_writes",
        ):
            if meta.get(key):
                lines.append(f"# preserved.{key}={meta.get(key)!r}")
        fields = meta.get("fields") or []
        if fields:
            names = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
            if names:
                lines.append(f"# preserved.mongo_fields={names!r}")
        lines.append("    .save()")
        lines.append(")")
        lines.append(
            "# NOTE: Null Spark values become BSON null; empty input DataFrames write zero docs."
        )
        return lines, "converted"


class HadoopFileInputHandler(BaseStepHandler):
    _TYPES = {"hadoopfileinput", "hadoopfileinputplugin", "hadoopfileoutputplugin"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", ""))
        file_format = self._attr(context, "file_format", "text")
        lines = [f"# Hadoop File Input: {context.step.name}"]
        lines.append(f"{out_var} = spark.read.format({file_format!r}).load({filename!r})")
        return lines, "converted" if filename else "converted"


# Kafka Consumer/Producer live in streaming_handlers.STREAMING_HANDLERS.
# Join Rows / ExecSQL / REST / HTTP live in join_handlers / scripting_handlers / lookup_handlers.


EXTENDED_HANDLERS: list[BaseStepHandler] = [
    CsvFileOutputHandler(),
    InsertUpdateHandler(),
    UpdateHandler(),
    DeleteHandler(),
    JsonOutputHandler(),
    XmlOutputHandler(),
    ParquetInputHandler(),
    ParquetOutputHandler(),
    DeltaFileOutputHandler(),
    OrcInputHandler(),
    OrcOutputHandler(),
    AvroInputHandler(),
    AvroOutputHandler(),
    MongoDBInputHandler(),
    MongoDBOutputHandler(),
    HadoopFileInputHandler(),
]
