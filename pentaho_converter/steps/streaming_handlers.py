"""Handlers for Pentaho Streaming transformation steps.

Supports:
- Get Records from Stream
- Kafka Consumer / Kafka Producer (Spark Structured Streaming)
- JMS Consumer / JMS Producer (metadata preserved; no native Databricks sink)
- MQTT Consumer / MQTT Producer (metadata preserved; optional Bahir-style hints)
"""

from __future__ import annotations

import logging
import re

from ..metadata_propagation import get_converter_metadata
from ..schema_utils import fields_to_schema_ddl
from ..step_xml import (
    get_step_element,
    parse_jms_consumer_config,
    parse_jms_producer_config,
    parse_kafka_consumer_config,
    parse_kafka_producer_config,
    parse_mqtt_consumer_config,
    parse_mqtt_producer_config,
    parse_records_from_stream_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return step_type.strip().lower().replace(" ", "").replace("(", "").replace(")", "")


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


_SKIPPED_ATTR_TAGS = frozenset({
    "name", "type", "description", "distribute", "custom_distribution",
    "copies", "partitioning", "remotesteps", "GUI", "draw", "attributes",
})


def _preserve_comments(
    meta: dict,
    keys: tuple[str, ...],
    *,
    context: StepContext | None = None,
) -> list[str]:
    """Preserve curated keys plus residual options / step attributes."""
    lines: list[str] = []
    seen: set[str] = set()
    for key in keys:
        val = meta.get(key)
        if val not in (None, "", [], {}):
            lines.append(f"# preserved.{key}={val!r}")
            seen.add(key)

    options = meta.get("options")
    if isinstance(options, dict):
        for key, val in options.items():
            if val in (None, ""):
                continue
            tag = f"options.{key}"
            if tag not in seen:
                lines.append(f"# preserved.{tag}={val!r}")
                seen.add(tag)

    attrs = meta.get("attributes") if isinstance(meta.get("attributes"), dict) else {}
    if context is not None and not attrs:
        attrs = dict(context.step.attributes or {})
    for key, val in attrs.items():
        if key in seen or key in _SKIPPED_ATTR_TAGS:
            continue
        if val in (None, "", [], {}):
            continue
        # Skip huge nested XML blobs already re-parsed into structured keys
        if isinstance(val, str) and val.strip().startswith("<") and len(val) > 120:
            continue
        lines.append(f"# preserved.attr.{key}={val!r}")
        seen.add(key)
    return lines


_KAFKA_OPTION_SKIP = frozenset({
    "bootstrap.servers",
    "kafka.bootstrap.servers",
    "group.id",
    "kafka.group.id",
    "subscribe",
    "topic",
    "topics",
    "startingoffsets",
    "startingoffset",
    "auto.offset.reset",
    "checkpointlocation",
})


def _kafka_client_options(meta: dict, *, include_security: bool = True) -> list[str]:
    """Emit spark .option('kafka.<prop>', ...) for security and client settings."""
    lines: list[str] = []
    security = meta.get("security") or {}
    options = meta.get("options") or {}
    merged = {**options, **security}

    # Explicit producer/consumer fields that map to kafka.* options
    for field_key, opt_key in (
        ("client_id", "client.id"),
        ("acks", "acks"),
        ("compression", "compression.type"),
    ):
        val = meta.get(field_key)
        if val not in (None, ""):
            merged.setdefault(opt_key, val)

    emitted: set[str] = set()
    for key, val in merged.items():
        if val in (None, ""):
            continue
        low = str(key).lower()
        if low in _KAFKA_OPTION_SKIP:
            continue
        is_security = any(
            tok in low
            for tok in (
                "security", "sasl", "ssl", "truststore", "keystore",
                "jaas", "protocol", "mechanism",
            )
        )
        is_client = any(
            tok in low
            for tok in (
                "acks", "compression", "client.id", "retries", "linger",
                "batch.size", "buffer.memory", "max.request", "request.timeout",
                "session.timeout", "heartbeat", "max.poll", "fetch.",
                "enable.auto.commit", "isolation.level",
            )
        )
        if is_security and not include_security:
            continue
        if not is_security and not is_client:
            # Still preserve unknown kafka.* props when already prefixed
            if not low.startswith("kafka.") and "." not in low:
                continue
        spark_key = key if str(key).startswith("kafka.") else f"kafka.{key}"
        if spark_key in emitted:
            continue
        emitted.add(spark_key)
        lines.append(f"    .option({spark_key!r}, {str(val)!r})")
    return lines


def _checkpoint_option(meta: dict, step_name: str) -> tuple[str, list[str]]:
    """Return (checkpoint path used, warning/comment lines)."""
    path = (meta.get("checkpoint_location") or "").strip()
    notes: list[str] = []
    if not path:
        path = f"/tmp/checkpoints/{_safe_ident(step_name)}"
        notes.append(
            f"# WARNING: streaming checkpoint not set in Pentaho — using default {path!r}"
        )
    notes.append(
        "# NOTE: Structured Streaming requires a durable checkpointLocation on Databricks"
    )
    return path, notes


def _field_schema_ddl(fields: list[dict]) -> str:
    if not fields:
        return "key STRING, value STRING"
    try:
        ddl = fields_to_schema_ddl(
            [{"name": f.get("name"), "type": f.get("type", "String")} for f in fields if f.get("name")]
        )
        if ddl:
            return ddl
    except Exception:
        pass
    parts = []
    for f in fields:
        name = f.get("name")
        if not name:
            continue
        parts.append(f"{name} STRING")
    return ", ".join(parts) if parts else "key STRING, value STRING"


# ---------------------------------------------------------------------------
# Get Records from Stream
# ---------------------------------------------------------------------------


class RecordsFromStreamHandler(BaseStepHandler):
    """Child-stream entry: pass through upstream rows and preserve field metadata."""

    _TYPES = {"recordsfromstream", "getrecordsfromstream"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            in_df = context.input_df_name()
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = (
                parse_records_from_stream_config(step_el)
                if step_el is not None
                else {}
            )
            fields = metadata.get("fields") or cfg.get("fields") or []
            source_step = (
                metadata.get("source_step") or cfg.get("source_step") or ""
            ).strip()

            lines = [f"# Get Records from Stream: {context.step.name}"]
            lines.extend(
                _preserve_comments(
                    {**cfg, **metadata, "source_step": source_step, "fields": fields},
                    ("source_step", "message_field", "key_field", "fields"),
                    context=context,
                )
            )

            # Prefer explicit source_step DF when present in the variable map
            source_df = ""
            if source_step:
                source_df = context.df_variable_map.get(source_step, "")
                if not source_df:
                    for name, var in context.df_variable_map.items():
                        if name.strip().lower() == source_step.lower():
                            source_df = var
                            break
                if source_df:
                    lines.append(
                        f"# source_step {source_step!r} resolved to {source_df}"
                    )
                else:
                    lines.append(
                        f"# WARNING: source_step={source_step!r} not found in DF map; "
                        "using hop upstream if available"
                    )

            feed = source_df or in_df
            if feed:
                lines.append(f"{out_var} = {feed}")
                lines.append(
                    "# Pass-through: parent streaming step feeds this child entry; "
                    "schema evolution / nulls retained from upstream DataFrame"
                )
                logger.info(
                    "RecordsFromStream %s source_step=%r feed=%s fields=%d",
                    context.step.name,
                    source_step,
                    feed,
                    len(fields) if isinstance(fields, list) else 0,
                )
                return lines, "converted"

            ddl = _field_schema_ddl(fields if isinstance(fields, list) else [])
            lines.append(
                "# WARNING: no upstream hop — emitting empty frame with preserved field schema "
                "(empty stream / standalone child transformation)"
            )
            lines.append(f"{out_var} = spark.createDataFrame([], '{ddl}')")
            logger.info(
                "RecordsFromStream %s empty stream schema fields=%d",
                context.step.name,
                len(fields) if isinstance(fields, list) else 0,
            )
            return lines, "converted"
        except Exception as exc:
            logger.exception("RecordsFromStream failed for %s", context.step.name)
            return [
                f"# Get Records from Stream: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.createDataFrame([], '_records_from_stream STRING')",
            ], "partial"


# ---------------------------------------------------------------------------
# Kafka Consumer / Producer
# ---------------------------------------------------------------------------


class KafkaConsumerHandler(BaseStepHandler):
    """Kafka Consumer → spark.readStream.format('kafka')."""

    _TYPES = {
        "kafkaconsumer",
        "kafkaconsumerinput",
        "kafkastreaminput",
        "kafka",
    }

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_kafka_consumer_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            bootstrap = (metadata.get("bootstrap_servers") or "").strip()
            topics = metadata.get("topics") or []
            if isinstance(topics, str):
                topics = [t.strip() for t in topics.split(",") if t.strip()]
            topic = (metadata.get("topic") or ",".join(topics)).strip()
            group = (metadata.get("consumer_group") or "").strip()
            starting = (metadata.get("starting_offsets") or "latest").strip() or "latest"
            status = "converted"

            lines = [f"# Kafka Consumer: {context.step.name}"]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "bootstrap_servers",
                        "topic",
                        "topics",
                        "consumer_group",
                        "starting_offsets",
                        "starting_offsets_raw",
                        "connection_type",
                        "cluster_name",
                        "batch_size",
                        "batch_duration",
                        "auto_commit",
                        "sub_transformation",
                        "options",
                        "security",
                        "fields",
                        "key_field",
                        "message_field",
                        "checkpoint_location",
                    ),
                    context=context,
                )
            )

            if not bootstrap:
                lines.append("# WARNING: missing Kafka bootstrap servers — using localhost:9092")
                bootstrap = "localhost:9092"
                status = "partial"
            if not topic:
                lines.append("# WARNING: missing Kafka topic(s) — subscribe option left empty")
                status = "partial"

            checkpoint, cp_notes = _checkpoint_option(metadata, context.step.name)
            lines.extend(cp_notes)

            lines.append(
                f"{out_var} = (spark.readStream.format('kafka')"
            )
            lines.append(f"    .option('kafka.bootstrap.servers', {bootstrap!r})")
            if topic:
                lines.append(f"    .option('subscribe', {topic!r})")
            if group:
                lines.append(f"    .option('kafka.group.id', {group!r})")
            lines.append(f"    .option('startingOffsets', {starting!r})")
            lines.extend(_kafka_client_options(metadata))
            lines.append("    .load())")

            key_field = metadata.get("key_field") or "key"
            msg_field = metadata.get("message_field") or "value"
            lines.append(
                f"{out_var} = {out_var}.selectExpr("
                f"'CAST(key AS STRING) AS `{key_field}`', "
                f"'CAST(value AS STRING) AS `{msg_field}`', "
                f"'topic', 'partition', 'offset', 'timestamp')"
            )
            lines.append(
                "# WARNING: schema evolution — Kafka payload is STRING; apply from_json()/schema "
                "if Pentaho typed fields are required downstream"
            )
            lines.append(
                "# WARNING: null Kafka keys/values stay null after CAST; empty streams yield empty micro-batches"
            )
            lines.append(
                f"# NOTE: attach a sink with .writeStream...option('checkpointLocation', {checkpoint!r}) "
                "before .start(); raw streaming DF returned for composition"
            )
            if metadata.get("sub_transformation"):
                lines.append(
                    f"# WARNING: Pentaho sub-transformation {metadata['sub_transformation']!r} "
                    "is not inlined — migrate Get Records from Stream child separately"
                )

            logger.info(
                "KafkaConsumer %s bootstrap=%s topic=%s group=%s starting=%s",
                context.step.name,
                bootstrap,
                topic,
                group,
                starting,
            )
            return lines, status
        except Exception as exc:
            logger.exception("KafkaConsumer failed for %s", context.step.name)
            return [
                f"# Kafka Consumer: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.createDataFrame([], 'key STRING, value STRING')",
            ], "partial"


class KafkaProducerHandler(BaseStepHandler):
    """Kafka Producer → writeStream.format('kafka') when possible."""

    _TYPES = {"kafkaproducer", "kafkaproduceroutput"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            in_df = context.input_df_name()
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_kafka_producer_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            bootstrap = (metadata.get("bootstrap_servers") or "").strip()
            topic = (metadata.get("topic") or "").strip()
            if not topic:
                topics = metadata.get("topics") or []
                if isinstance(topics, list) and topics:
                    topic = topics[0]
            key_field = (metadata.get("key_field") or "key").strip() or "key"
            msg_field = (metadata.get("message_field") or "value").strip() or "value"
            status = "converted"

            lines = [f"# Kafka Producer: {context.step.name}"]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "bootstrap_servers",
                        "topic",
                        "topics",
                        "key_field",
                        "message_field",
                        "options",
                        "security",
                        "client_id",
                        "acks",
                        "compression",
                        "connection_type",
                        "cluster_name",
                        "checkpoint_location",
                        "fields",
                    ),
                    context=context,
                )
            )

            if not in_df:
                lines.append("# WARNING: Kafka Producer has no upstream DataFrame")
                lines.append(f"{out_var} = spark.createDataFrame([], 'key STRING, value STRING')")
                return lines, "partial"

            if not bootstrap:
                lines.append("# WARNING: missing Kafka bootstrap servers — using localhost:9092")
                bootstrap = "localhost:9092"
                status = "partial"
            if not topic:
                lines.append("# WARNING: missing Kafka topic")
                status = "partial"

            checkpoint, cp_notes = _checkpoint_option(metadata, context.step.name)
            lines.extend(cp_notes)

            # Spark Kafka serialization: CAST key/value columns to STRING / BINARY-compatible STRING
            lines.append(
                f"_kp_payload_{out_var} = {in_df}.selectExpr("
                f"'CAST(`{key_field}` AS STRING) AS key', "
                f"'CAST(`{msg_field}` AS STRING) AS value')"
            )
            lines.append(
                f"_kp_query_{out_var} = (_kp_payload_{out_var}.writeStream.format('kafka')"
            )
            lines.append(f"    .option('kafka.bootstrap.servers', {bootstrap!r})")
            if topic:
                lines.append(f"    .option('topic', {topic!r})")
            lines.extend(_kafka_client_options(metadata))
            lines.append(f"    .option('checkpointLocation', {checkpoint!r})")
            lines.append("    .start())")
            lines.append(
                "# WARNING: null key/value rows are written as null Kafka records; "
                "filter before write if Pentaho skipped nulls"
            )
            lines.append(
                "# NOTE: call _kp_query_*.awaitTermination() in a streaming job; "
                "batch notebooks may prefer .write.format('kafka').save() instead"
            )
            lines.append(f"{out_var} = {in_df}")
            logger.info(
                "KafkaProducer %s bootstrap=%s topic=%s key=%s value=%s",
                context.step.name,
                bootstrap,
                topic,
                key_field,
                msg_field,
            )
            return lines, status
        except Exception as exc:
            logger.exception("KafkaProducer failed for %s", context.step.name)
            return [
                f"# Kafka Producer: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.createDataFrame([], 'key STRING, value STRING')",
            ], "partial"


# ---------------------------------------------------------------------------
# JMS Consumer / Producer (no native Databricks equivalent)
# ---------------------------------------------------------------------------


class JmsConsumerHandler(BaseStepHandler):
    """JMS Consumer — preserve metadata; no Spark Structured Streaming JMS source."""

    _TYPES = {"jmsconsumer", "jmsconsumerinput", "activemqconsumer"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_jms_consumer_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            fields = metadata.get("fields") or []
            ddl = _field_schema_ddl(fields if isinstance(fields, list) else [])

            lines = [
                f"# JMS Consumer: {context.step.name}",
                "# UNSUPPORTED: Databricks / Spark has no built-in JMS Structured Streaming source.",
                "# WARNING: Configuration preserved for manual migration "
                "(e.g. Event Hubs / Kafka bridge, or foreachBatch JMS client).",
            ]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "destination",
                        "destination_type",
                        "connection_factory",
                        "url",
                        "username",
                        "password",
                        "receive_timeout",
                        "message_selector",
                        "client_id",
                        "durable",
                        "transacted",
                        "acknowledge_mode",
                        "ssl",
                        "options",
                        "fields",
                        "message_field",
                        "sub_transformation",
                    ),
                    context=context,
                )
            )
            if not metadata.get("url") and not metadata.get("connection_factory"):
                lines.append("# WARNING: missing JMS broker URL / connection factory")
            if not metadata.get("destination"):
                lines.append("# WARNING: missing JMS queue/topic destination")
            if metadata.get("password"):
                lines.append(
                    "# WARNING: password present in metadata — move to Databricks secrets; "
                    "do not commit credentials"
                )

            lines.append(f"{out_var} = spark.createDataFrame([], '{ddl}')")
            lines.append(
                "# Empty stream placeholder; replace with a supported messaging source after bridge setup"
            )
            logger.warning(
                "JMS Consumer %s has no Databricks equivalent; metadata preserved",
                context.step.name,
            )
            return lines, "partial"
        except Exception as exc:
            logger.exception("JMS Consumer failed for %s", context.step.name)
            return [
                f"# JMS Consumer: {context.step.name}",
                f"# ERROR: {exc}",
                "# UNSUPPORTED: JMS has no Databricks equivalent",
                f"{out_var} = spark.createDataFrame([], '_jms_consumer STRING')",
            ], "partial"


class JmsProducerHandler(BaseStepHandler):
    """JMS Producer — preserve destination / delivery options; emit migration warnings."""

    _TYPES = {"jmsproducer", "jmsproduceroutput", "activemqproducer"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            in_df = context.input_df_name()
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_jms_producer_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            lines = [
                f"# JMS Producer: {context.step.name}",
                "# UNSUPPORTED: Databricks / Spark has no built-in JMS sink.",
                "# WARNING: Preserve destination and delivery options; publish via external "
                "client (foreachBatch) or migrate to Kafka / Event Hubs.",
            ]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "destination",
                        "destination_type",
                        "connection_factory",
                        "url",
                        "username",
                        "password",
                        "message_field",
                        "delivery_mode",
                        "priority",
                        "time_to_live",
                        "persistent",
                        "ssl",
                        "options",
                        "fields",
                    ),
                    context=context,
                )
            )
            if not metadata.get("destination"):
                lines.append("# WARNING: missing JMS destination")
            if not metadata.get("url") and not metadata.get("connection_factory"):
                lines.append("# WARNING: missing JMS broker URL / connection factory")

            if in_df:
                lines.append(f"{out_var} = {in_df}")
                lines.append(
                    "# Passthrough for pipeline continuity — no JMS write emitted"
                )
            else:
                lines.append(
                    f"{out_var} = spark.createDataFrame([], '_jms_producer STRING')"
                )
            logger.warning(
                "JMS Producer %s has no Databricks equivalent; metadata preserved",
                context.step.name,
            )
            return lines, "partial"
        except Exception as exc:
            logger.exception("JMS Producer failed for %s", context.step.name)
            return [
                f"# JMS Producer: {context.step.name}",
                f"# ERROR: {exc}",
                "# UNSUPPORTED: JMS has no Databricks equivalent",
                f"{out_var} = spark.createDataFrame([], '_jms_producer STRING')",
            ], "partial"


# ---------------------------------------------------------------------------
# MQTT Consumer / Producer
# ---------------------------------------------------------------------------


class MqttConsumerHandler(BaseStepHandler):
    """MQTT Consumer — preserve config; optional Apache Bahir-style stream (not on DBX by default)."""

    _TYPES = {"mqttconsumer", "mqttconsumerinput", "mqttclient"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_mqtt_consumer_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            broker = (metadata.get("broker_url") or "").strip()
            topic = (metadata.get("topic") or "").strip()
            topics = metadata.get("topics") or []
            if not topic and topics:
                topic = topics[0] if isinstance(topics, list) else str(topics)
            qos = str(metadata.get("qos") or "0")
            fields = metadata.get("fields") or []
            ddl = _field_schema_ddl(fields if isinstance(fields, list) else [])

            lines = [
                f"# MQTT Consumer: {context.step.name}",
                "# WARNING: Databricks has no first-class MQTT Structured Streaming source.",
                "# UNSUPPORTED (default runtime): Apache Bahir MQTT provider is illustrative only "
                "and typically unavailable on Databricks — prefer Kafka / Event Hubs bridge.",
            ]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "broker_url",
                        "topic",
                        "topics",
                        "qos",
                        "username",
                        "password",
                        "client_id",
                        "clean_session",
                        "keep_alive",
                        "ssl",
                        "options",
                        "fields",
                        "message_field",
                        "sub_transformation",
                        "checkpoint_location",
                    ),
                    context=context,
                )
            )

            if not broker:
                lines.append("# WARNING: missing MQTT broker URL")
            if not topic:
                lines.append("# WARNING: missing MQTT topic")
            if metadata.get("password"):
                lines.append(
                    "# WARNING: password present in metadata — move to Databricks secrets"
                )

            checkpoint, cp_notes = _checkpoint_option(metadata, context.step.name)
            lines.extend(cp_notes)

            if broker and topic:
                lines.append(
                    "# Optional Bahir MQTT sketch (verify JARs / connectivity before enabling):"
                )
                lines.append(
                    f"# {out_var} = (spark.readStream.format("
                    f"'org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider')"
                )
                lines.append(f"#     .option('brokerUrl', {broker!r})")
                lines.append(f"#     .option('topic', {topic!r})")
                lines.append(f"#     .option('qos', {qos!r})")
                if metadata.get("username"):
                    lines.append(f"#     .option('username', {metadata['username']!r})")
                if metadata.get("password"):
                    lines.append("#     .option('password', dbutils.secrets.get(...))  # do not hardcode")
                if metadata.get("client_id"):
                    lines.append(f"#     .option('clientId', {metadata['client_id']!r})")
                if metadata.get("ssl"):
                    lines.append(f"#     .option('ssl', {str(metadata['ssl'])!r})")
                lines.append(f"#     .option('checkpointLocation', {checkpoint!r})")
                lines.append("#     .load())")

            lines.append(f"{out_var} = spark.createDataFrame([], '{ddl}')")
            lines.append(
                "# Empty stream placeholder; authentication / SSL / QoS settings preserved above"
            )
            logger.warning(
                "MQTT Consumer %s: no native Databricks source; metadata preserved",
                context.step.name,
            )
            return lines, "partial"
        except Exception as exc:
            logger.exception("MQTT Consumer failed for %s", context.step.name)
            return [
                f"# MQTT Consumer: {context.step.name}",
                f"# ERROR: {exc}",
                "# UNSUPPORTED: MQTT has no native Databricks source",
                f"{out_var} = spark.createDataFrame([], '_mqtt_consumer STRING')",
            ], "partial"


class MqttProducerHandler(BaseStepHandler):
    """MQTT Producer — preserve broker/topic/QoS/payload mapping."""

    _TYPES = {"mqttproducer", "mqttproduceroutput"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            in_df = context.input_df_name()
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_mqtt_producer_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            lines = [
                f"# MQTT Producer: {context.step.name}",
                "# UNSUPPORTED: Databricks has no built-in MQTT sink.",
                "# WARNING: Preserve broker, topic, QoS, and payload mapping; "
                "publish via foreachBatch MQTT client or bridge to Kafka.",
            ]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "broker_url",
                        "topic",
                        "topics",
                        "qos",
                        "username",
                        "password",
                        "client_id",
                        "message_field",
                        "retained",
                        "ssl",
                        "options",
                        "fields",
                    ),
                    context=context,
                )
            )
            if not metadata.get("broker_url"):
                lines.append("# WARNING: missing MQTT broker URL")
            if not metadata.get("topic"):
                lines.append("# WARNING: missing MQTT topic")
            if metadata.get("password"):
                lines.append(
                    "# WARNING: password present in metadata — move to Databricks secrets"
                )

            msg_field = metadata.get("message_field") or ""
            if msg_field and in_df:
                lines.append(f"# preserved.payload_column={msg_field!r}")
                lines.append(
                    f"# HINT: foreachBatch publish {in_df}.select('{msg_field}') to MQTT "
                    f"topic {metadata.get('topic')!r} at QoS {metadata.get('qos')!r}"
                )

            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(
                    f"{out_var} = spark.createDataFrame([], '_mqtt_producer STRING')"
                )
            logger.warning(
                "MQTT Producer %s: no native Databricks sink; metadata preserved",
                context.step.name,
            )
            return lines, "partial"
        except Exception as exc:
            logger.exception("MQTT Producer failed for %s", context.step.name)
            return [
                f"# MQTT Producer: {context.step.name}",
                f"# ERROR: {exc}",
                "# UNSUPPORTED: MQTT has no native Databricks sink",
                f"{out_var} = spark.createDataFrame([], '_mqtt_producer STRING')",
            ], "partial"


STREAMING_HANDLERS: list[BaseStepHandler] = [
    RecordsFromStreamHandler(),
    KafkaConsumerHandler(),
    KafkaProducerHandler(),
    JmsConsumerHandler(),
    JmsProducerHandler(),
    MqttConsumerHandler(),
    MqttProducerHandler(),
]
