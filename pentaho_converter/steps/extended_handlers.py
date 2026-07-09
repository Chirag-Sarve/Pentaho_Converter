"""Handlers for additional Pentaho step types (DB ops, file formats, integrations)."""

from __future__ import annotations

from ..step_xml import _child_text, format_spark_join_on, get_step_element, parse_join_keys
from .advanced_handlers import _passthrough
from .base import BaseStepHandler, StepContext


def _write_csv(context: StepContext, filename: str, separator: str = ",") -> tuple[list[str], str]:
    in_df = context.input_df_name() or context.output_df_name()
    lines = [f"# CSV Output: {context.step.name}"]
    header = context.step.attributes.get("header", "Y").upper() == "Y"
    encoding = context.step.attributes.get("encoding", "utf-8")
    lines.append(f"_out_{in_df} = {in_df}")
    lines.append(f"writer = _out_{in_df}.write.format('csv')")
    lines.append(f"writer = writer.option('header', {header!r})")
    lines.append(f"writer = writer.option('sep', {separator!r})")
    lines.append(f"writer = writer.option('encoding', {encoding!r})")
    lines.append(f"writer.mode('overwrite').save({filename!r})")
    return lines, "converted"


class CsvFileOutputHandler(BaseStepHandler):
    _TYPES = {"csvoutput", "csvfileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        filename = self._attr(context, "filename", self._attr(context, "file", "output.csv"))
        separator = self._attr(context, "separator", ",")
        return _write_csv(context, filename, separator)


class JoinRowsHandler(BaseStepHandler):
    """Join Rows — equi-join on configured keys (distinct from Merge Join / Merge Rows)."""

    _TYPES = {"joinrows", "joiner"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        join_type = self._attr(context, "join_type", "INNER").lower()
        join_map = {
            "inner": "inner",
            "left outer": "left",
            "left": "left",
            "right outer": "right",
            "right": "right",
            "full outer": "outer",
            "full": "outer",
            "cross": "cross",
        }
        how = join_map.get(join_type, "inner")
        keys = parse_join_keys(step_el) if step_el is not None else []
        lines = [f"# Join Rows: {context.step.name}"]
        if len(inputs) >= 2:
            left, right = inputs[0], inputs[1]
            if keys:
                on_arg, use_on = format_spark_join_on(left, right, keys)
                if use_on:
                    lines.append(
                        f"{out_var} = {left}.join({right}, on={on_arg}, how={how!r})"
                    )
                else:
                    lines.append(f"{out_var} = {left}.join({right}, {on_arg}, {how!r})")
            elif how == "cross":
                lines.append(f"{out_var} = {left}.crossJoin({right})")
            else:
                lines.append(f"# JoinRows '{context.step.name}': no join keys in XML")
                lines.append(f"{out_var} = {left}")
            return lines, "converted"
        if len(inputs) == 1:
            lines.append(f"{out_var} = {inputs[0]}")
            return lines, "converted"
        return _passthrough(context, "Join Rows")


class InsertUpdateHandler(BaseStepHandler):
    _TYPES = {"insertupdate"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        schema = self._attr(context, "schema", "")
        table = self._attr(context, "table", "target_table")
        full = f"{schema}.{table}" if schema else table
        key_fields = [f.name for f in self._fields(context) if f.name]
        lines = [f"# Insert/Update: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Insert/Update")
        lines.append(f"_upsert_src = {in_df}")
        if key_fields:
            merge_cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in key_fields)
            lines.append(
                f"spark.sql(f'''MERGE INTO {full} t USING _upsert_src s "
                f"ON {merge_cond} WHEN MATCHED THEN UPDATE SET * "
                f"WHEN NOT MATCHED THEN INSERT *''')"
            )
        else:
            lines.append(f"_upsert_src.write.format('delta').mode('append').saveAsTable({full!r})")
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class UpdateHandler(BaseStepHandler):
    _TYPES = {"update"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        schema = self._attr(context, "schema", "")
        table = self._attr(context, "table", "target_table")
        full = f"{schema}.{table}" if schema else table
        key_fields = [f.name for f in self._fields(context) if f.name]
        lines = [f"# Update: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Update")
        lines.append(f"_update_src = {in_df}")
        if key_fields:
            merge_cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in key_fields)
            lines.append(
                f"spark.sql(f'''MERGE INTO {full} t USING _update_src s "
                f"ON {merge_cond} WHEN MATCHED THEN UPDATE SET *''')"
            )
        else:
            lines.append(f"_update_src.write.format('delta').mode('overwrite').saveAsTable({full!r})")
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class DeleteHandler(BaseStepHandler):
    _TYPES = {"delete"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        schema = self._attr(context, "schema", "")
        table = self._attr(context, "table", "target_table")
        full = f"{schema}.{table}" if schema else table
        key_fields = [f.name for f in self._fields(context) if f.name]
        lines = [f"# Delete: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Delete")
        lines.append(f"_delete_src = {in_df}")
        if key_fields:
            merge_cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in key_fields)
            lines.append(
                f"spark.sql(f'''MERGE INTO {full} t USING _delete_src s "
                f"ON {merge_cond} WHEN MATCHED THEN DELETE''')"
            )
        else:
            lines.append(f"# Review delete keys — no key fields configured for {context.step.name}")
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class ExecuteSQLHandler(BaseStepHandler):
    _TYPES = {"execsql", "executesql", "sql"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        in_df = context.input_df_name()
        step_el = get_step_element(context.step)
        sql = self._attr(context, "sql", "")
        if step_el is not None:
            sql = sql or _child_text(step_el, "sql")
        lines = [f"# Execute SQL: {context.step.name}"]
        if sql:
            if in_df:
                lines.append(f"{in_df}.createOrReplaceTempView('_exec_sql_input')")
                lines.append(f"spark.sql({sql!r})")
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = spark.sql({sql!r})")
            return lines, "converted"
        return _passthrough(context, "Execute SQL")


class JsonOutputHandler(BaseStepHandler):
    _TYPES = {"jsonoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name() or context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", "output.json"))
        lines = [f"# JSON Output: {context.step.name}"]
        lines.append(
            f"{in_df}.write.format('json').mode('overwrite').save({filename!r})"
        )
        return lines, "converted"


class XmlOutputHandler(BaseStepHandler):
    _TYPES = {"xmloutput", "xmlpad", "xmlwriter"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name() or context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", "output.xml"))
        root_tag = self._attr(context, "root_tag", "rows")
        lines = [f"# XML Output: {context.step.name}"]
        lines.append(
            f"{in_df}.write.format('xml').option('rootTag', {root_tag!r})"
            f".mode('overwrite').save({filename!r})"
        )
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
    _TYPES = {"parquetoutput", "parquetfileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name() or context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", "output.parquet"))
        compression = self._attr(context, "compression", "snappy")
        lines = [f"# Parquet Output: {context.step.name}"]
        lines.append(f"writer = {in_df}.write.format('parquet')")
        if compression and compression.lower() not in ("none", ""):
            lines.append(f"writer = writer.option('compression', {compression!r})")
        lines.append(f"writer.mode('overwrite').save({filename!r})")
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
    _TYPES = {"avroinput", "avrofileinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", ""))
        lines = [f"# Avro Input: {context.step.name}"]
        lines.append(f"{out_var} = spark.read.format('avro').load({filename!r})")
        return lines, "converted" if filename else "converted"


class AvroOutputHandler(BaseStepHandler):
    _TYPES = {"avrooutput", "avrofileoutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name() or context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", "output.avro"))
        lines = [f"# Avro Output: {context.step.name}"]
        lines.append(f"{in_df}.write.format('avro').mode('overwrite').save({filename!r})")
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


class RestClientHandler(BaseStepHandler):
    _TYPES = {"rest", "restclient"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        url = self._attr(context, "url", self._attr(context, "rest_url", ""))
        method = self._attr(context, "method", "GET").upper()
        lines = [f"# REST Client: {context.step.name}"]
        lines.append(
            f"{out_var} = spark.createDataFrame("
            f"[{{'url': {url!r}, 'method': {method!r}, 'body': None}}], "
            f"'url STRING, method STRING, body STRING')"
        )
        return lines, "converted"


class HttpHandler(BaseStepHandler):
    _TYPES = {"http", "httppost", "httpget"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        url = self._attr(context, "url", "")
        lines = [f"# HTTP: {context.step.name}"]
        lines.append(f"{out_var} = spark.createDataFrame([{{'url': {url!r}}}], 'url STRING')")
        return lines, "converted"


class KafkaConsumerHandler(BaseStepHandler):
    _TYPES = {"kafkaconsumerinput", "kafkastreaminput", "kafka", "kafkaproduceroutput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        topic = self._attr(context, "topic", "")
        bootstrap = self._attr(context, "bootstrap_servers", "localhost:9092")
        is_output = "output" in context.step.step_type.lower() or "producer" in context.step.step_type.lower()
        lines = [f"# Kafka: {context.step.name}"]
        if is_output and in_df:
            lines.append(f"{in_df}.write.format('kafka')")
            lines.append(f"    .option('kafka.bootstrap.servers', {bootstrap!r})")
            lines.append(f"    .option('topic', {topic!r}).save()")
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.read.format('kafka')")
            lines.append(f"    .option('kafka.bootstrap.servers', {bootstrap!r})")
            lines.append(f"    .option('subscribe', {topic!r}).load()")
        return lines, "converted"


EXTENDED_HANDLERS: list[BaseStepHandler] = [
    CsvFileOutputHandler(),
    JoinRowsHandler(),
    InsertUpdateHandler(),
    UpdateHandler(),
    DeleteHandler(),
    ExecuteSQLHandler(),
    JsonOutputHandler(),
    XmlOutputHandler(),
    ParquetInputHandler(),
    ParquetOutputHandler(),
    OrcInputHandler(),
    OrcOutputHandler(),
    AvroInputHandler(),
    AvroOutputHandler(),
    HadoopFileInputHandler(),
    RestClientHandler(),
    HttpHandler(),
    KafkaConsumerHandler(),
]
