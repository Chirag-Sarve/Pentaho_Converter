"""Tests for Pentaho Streaming step migration (Kafka, JMS, MQTT, RecordsFromStream)."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_jms_consumer_config,
    parse_jms_producer_config,
    parse_kafka_consumer_config,
    parse_kafka_producer_config,
    parse_mqtt_consumer_config,
    parse_mqtt_producer_config,
    parse_records_from_stream_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.steps.streaming_handlers import STREAMING_HANDLERS
from pentaho_converter.validation.step_validators import parse_step_config
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    trans = PentahoTransformation(name="StreamTrans", file_path=Path("stream.ktr"))
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Input", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(code: str) -> bool:
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False


class TestStreamingParsers(unittest.TestCase):
    def test_parse_kafka_consumer_full(self):
        xml = """
        <step>
          <DIRECT_BOOTSTRAP_SERVERS>broker1:9092,broker2:9092</DIRECT_BOOTSTRAP_SERVERS>
          <CONSUMER_GROUP>cg-orders</CONSUMER_GROUP>
          <Topic>
            <topic>orders</topic>
            <topic>returns</topic>
          </Topic>
          <Options>
            <Option>
              <property>auto.offset.reset</property>
              <value>earliest</value>
            </Option>
            <Option>
              <property>security.protocol</property>
              <value>SASL_SSL</value>
            </Option>
            <Option>
              <property>sasl.mechanism</property>
              <value>PLAIN</value>
            </Option>
          </Options>
          <FIELDS>
            <field>
              <InputName>key</InputName>
              <OutputName>msg_key</OutputName>
              <Type>String</Type>
            </field>
          </FIELDS>
        </step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        cfg = parse_kafka_consumer_config(el)
        self.assertEqual(cfg["bootstrap_servers"], "broker1:9092,broker2:9092")
        self.assertEqual(cfg["consumer_group"], "cg-orders")
        self.assertIn("orders", cfg["topics"])
        self.assertIn("returns", cfg["topics"])
        self.assertEqual(cfg["starting_offsets"], "earliest")
        self.assertIn("security.protocol", cfg["security"])
        self.assertTrue(cfg["fields"])

    def test_parse_kafka_producer(self):
        xml = """
        <step>
          <bootstrap_servers>localhost:9092</bootstrap_servers>
          <topic>out-topic</topic>
          <key_field>id</key_field>
          <message_field>payload</message_field>
        </step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        cfg = parse_kafka_producer_config(el)
        self.assertEqual(cfg["topic"], "out-topic")
        self.assertEqual(cfg["key_field"], "id")
        self.assertEqual(cfg["message_field"], "payload")

    def test_parse_records_from_stream(self):
        xml = """
        <step>
          <source_step>Kafka consumer</source_step>
          <fields>
            <field><name>key</name><type>String</type></field>
            <field><name>message</name><type>String</type></field>
          </fields>
        </step>
        """
        el = ET.fromstring(textwrap.dedent(xml).strip())
        cfg = parse_records_from_stream_config(el)
        self.assertEqual(cfg["source_step"], "Kafka consumer")
        self.assertEqual(cfg["output_columns"], ["key", "message"])

    def test_parse_jms_and_mqtt(self):
        jms = ET.fromstring(
            "<step><url>tcp://localhost:61616</url><destination>queue.orders</destination>"
            "<connectionFactory>ConnectionFactory</connectionFactory>"
            "<username>u</username><password>p</password></step>"
        )
        jcfg = parse_jms_consumer_config(jms)
        self.assertEqual(jcfg["destination"], "queue.orders")
        self.assertEqual(jcfg["connection_factory"], "ConnectionFactory")

        mqtt = ET.fromstring(
            "<step><brokerUrl>tcp://mqtt:1883</brokerUrl><topic>sensors/#</topic>"
            "<qos>1</qos><username>iot</username></step>"
        )
        mcfg = parse_mqtt_consumer_config(mqtt)
        self.assertEqual(mcfg["broker_url"], "tcp://mqtt:1883")
        self.assertEqual(mcfg["qos"], "1")

    def test_parse_step_metadata_dispatch(self):
        el = ET.fromstring(
            "<step><bootstrap_servers>b:9092</bootstrap_servers><topic>t</topic></step>"
        )
        meta = parse_step_metadata(el, "KafkaConsumerInput")
        self.assertEqual(meta.get("bootstrap_servers"), "b:9092")


class TestStreamingHandlers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_handlers_registered(self):
        self.assertEqual(len(STREAMING_HANDLERS), 7)
        for handler in STREAMING_HANDLERS:
            self.assertTrue(handler._TYPES)

    def test_records_from_stream_passthrough(self):
        xml = """
        <step>
          <fields>
            <field><name>key</name><type>String</type></field>
            <field><name>message</name><type>String</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "RecordsFromStream",
            _ctx(xml, "RecordsFromStream", "Get records from stream"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("Get Records from Stream", code)
        self.assertIn("df_Get_records_from_stream = df_Input", code)
        self.assertIn("preserved.fields=", code)
        self.assertIn(outcome.status, ("converted", "partial"))
        self.assertTrue(_syntax_ok(code))

    def test_records_from_stream_no_input(self):
        xml = """
        <step>
          <fields>
            <field><name>key</name><type>String</type></field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "Get records from stream",
            _ctx(xml, "Get records from stream", "Stream entry", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("createDataFrame", code)
        self.assertIn("empty stream", code.lower())
        self.assertTrue(_syntax_ok(code))

    def test_kafka_consumer_readstream(self):
        xml = """
        <step>
          <DIRECT_BOOTSTRAP_SERVERS>localhost:9092</DIRECT_BOOTSTRAP_SERVERS>
          <CONSUMER_GROUP>g1</CONSUMER_GROUP>
          <topic>events</topic>
          <Options>
            <Option><property>auto.offset.reset</property><value>earliest</value></Option>
            <Option><property>security.protocol</property><value>SASL_SSL</value></Option>
          </Options>
        </step>
        """
        outcome = self.registry.convert_step(
            "KafkaConsumerInput",
            _ctx(xml, "KafkaConsumerInput", "Kafka consumer", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("readStream", code)
        self.assertIn("format('kafka')", code)
        self.assertIn("subscribe", code)
        self.assertIn("events", code)
        self.assertIn("startingOffsets", code)
        self.assertIn("kafka.group.id", code)
        self.assertIn("security.protocol", code)
        self.assertIn("checkpoint", code.lower())
        self.assertNotIn("spark.read.format", code)
        self.assertIn(outcome.status, ("converted", "partial"))
        self.assertTrue(_syntax_ok(code))

    def test_kafka_consumer_missing_broker(self):
        xml = "<step><topic>t</topic></step>"
        outcome = self.registry.convert_step(
            "KafkaConsumerInput",
            _ctx(xml, "KafkaConsumerInput", "KC", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: missing Kafka bootstrap", code)
        self.assertEqual(outcome.status, "partial")

    def test_kafka_producer_writestream(self):
        xml = """
        <step>
          <bootstrap_servers>localhost:9092</bootstrap_servers>
          <topic>out</topic>
          <key_field>key</key_field>
          <message_field>value</message_field>
        </step>
        """
        outcome = self.registry.convert_step(
            "KafkaProducerOutput",
            _ctx(xml, "KafkaProducerOutput", "Kafka producer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("writeStream", code)
        self.assertIn("format('kafka')", code)
        self.assertIn("checkpointLocation", code)
        self.assertIn("topic", code)
        self.assertTrue(_syntax_ok(code))

    def test_jms_consumer_unsupported_preserves_meta(self):
        xml = """
        <step>
          <url>tcp://localhost:61616</url>
          <destination>orders.q</destination>
          <connectionFactory>cf</connectionFactory>
          <username>u</username>
          <password>secret</password>
        </step>
        """
        outcome = self.registry.convert_step(
            "JmsConsumer",
            _ctx(xml, "JmsConsumer", "JMS consumer", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.destination=", code)
        self.assertIn("preserved.connection_factory=", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(code))

    def test_jms_producer_passthrough_warning(self):
        xml = """
        <step>
          <destination>out.q</destination>
          <deliveryMode>PERSISTENT</deliveryMode>
          <message_field>body</message_field>
        </step>
        """
        outcome = self.registry.convert_step(
            "JmsProducer",
            _ctx(xml, "JmsProducer", "JMS producer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.destination=", code)
        self.assertIn("df_JMS_producer = df_Input", code)
        self.assertEqual(outcome.status, "partial")

    def test_mqtt_consumer_preserves_and_warns(self):
        xml = """
        <step>
          <brokerUrl>tcp://mqtt:1883</brokerUrl>
          <topic>devices/+/telemetry</topic>
          <qos>2</qos>
          <username>device</username>
        </step>
        """
        outcome = self.registry.convert_step(
            "MQTTConsumer",
            _ctx(xml, "MQTTConsumer", "MQTT consumer", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING", code)
        self.assertIn("preserved.broker_url=", code)
        self.assertIn("preserved.qos=", code)
        self.assertIn("Bahir", code)
        self.assertEqual(outcome.status, "partial")
        self.assertTrue(_syntax_ok(code))

    def test_mqtt_producer_preserves_payload(self):
        xml = """
        <step>
          <broker_url>tcp://mqtt:1883</broker_url>
          <topic>alerts</topic>
          <qos>1</qos>
          <message_field>payload</message_field>
        </step>
        """
        outcome = self.registry.convert_step(
            "MQTTProducer",
            _ctx(xml, "MQTTProducer", "MQTT producer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("UNSUPPORTED", code)
        self.assertIn("preserved.topic=", code)
        self.assertIn("payload", code)
        self.assertEqual(outcome.status, "partial")

    def test_kafka_replaces_batch_stub(self):
        """Ensure the old batch spark.read Kafka stub is no longer used."""
        xml = """
        <step>
          <bootstrap_servers>b:9092</bootstrap_servers>
          <topic>t</topic>
        </step>
        """
        outcome = self.registry.convert_step(
            "KafkaConsumerInput",
            _ctx(xml, "KafkaConsumerInput", "K", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("readStream", code)
        self.assertNotRegex(code, r"spark\.read\.format\('kafka'\)")

    def test_records_from_stream_source_step_resolution(self):
        xml = """
        <step>
          <source_step>ParentKafka</source_step>
          <fields>
            <field><name>key</name><type>String</type></field>
            <field><name>message</name><type>String</type></field>
          </fields>
        </step>
        """
        step_el = ET.fromstring(textwrap.dedent(xml).strip())
        step = PentahoStep(
            name="Child entry",
            step_type="RecordsFromStream",
            attributes={},
            raw_element=step_el,
        )
        parent = PentahoStep(name="ParentKafka", step_type="KafkaConsumerInput", attributes={})
        other = PentahoStep(name="Other", step_type="RowGenerator", attributes={})
        trans = PentahoTransformation(name="Child", file_path=Path("child.ktr"))
        trans.steps = [parent, other, step]
        hops = [PentahoHop(from_name="Other", to_name="Child entry")]
        dag = StepDAG(trans.steps, hops)
        df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
        ctx = StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)
        outcome = self.registry.convert_step("RecordsFromStream", ctx)
        code = "\n".join(outcome.code_lines)
        self.assertIn("preserved.source_step=", code)
        self.assertIn("df_ParentKafka", code)
        self.assertIn("source_step 'ParentKafka' resolved", code)

    def test_kafka_producer_serialization_options(self):
        xml = """
        <step>
          <bootstrap_servers>localhost:9092</bootstrap_servers>
          <topic>out</topic>
          <key_field>id</key_field>
          <message_field>body</message_field>
          <Options>
            <Option><property>acks</property><value>all</value></Option>
            <Option><property>compression.type</property><value>gzip</value></Option>
            <Option><property>client.id</property><value>pdi-prod</value></Option>
            <Option><property>security.protocol</property><value>SSL</value></Option>
          </Options>
        </step>
        """
        outcome = self.registry.convert_step(
            "KafkaProducerOutput",
            _ctx(xml, "KafkaProducerOutput", "KP"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("writeStream", code)
        self.assertIn("CAST(`id` AS STRING)", code)
        self.assertIn("CAST(`body` AS STRING)", code)
        self.assertIn("kafka.acks", code)
        self.assertIn("kafka.compression.type", code)
        self.assertIn("kafka.client.id", code)
        self.assertIn("kafka.security.protocol", code)
        self.assertTrue(_syntax_ok(code))

    def test_kafka_consumer_missing_topic(self):
        xml = "<step><DIRECT_BOOTSTRAP_SERVERS>b:9092</DIRECT_BOOTSTRAP_SERVERS></step>"
        outcome = self.registry.convert_step(
            "Kafka Consumer",
            _ctx(xml, "Kafka Consumer", "KC2", with_input=False),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: missing Kafka topic", code)
        self.assertEqual(outcome.status, "partial")
        self.assertIn("readStream", code)

    def test_parse_step_config_spaced_display_name(self):
        """Display names with spaces must still enrich parsed config for validators."""
        register_builtin_validators()
        xml = """
        <step>
          <source_step>Upstream</source_step>
          <fields><field><name>a</name><type>String</type></field></fields>
        </step>
        """
        ctx = _ctx(xml, "Get records from stream", "Entry", with_input=False)
        parsed = parse_step_config(ctx)
        self.assertEqual(parsed.get("source_step"), "Upstream")
        self.assertEqual(parsed.get("output_columns"), ["a"])

    def test_jms_producer_parse_and_mqtt_producer_parse(self):
        jms = ET.fromstring(
            "<step><destination>t.out</destination><destinationType>topic</destinationType>"
            "<message_field>body</message_field><deliveryMode>NON_PERSISTENT</deliveryMode>"
            "<priority>4</priority></step>"
        )
        jcfg = parse_jms_producer_config(jms)
        self.assertEqual(jcfg["destination"], "t.out")
        self.assertEqual(jcfg["message_field"], "body")

        mqtt = ET.fromstring(
            "<step><broker_url>ssl://mqtt:8883</broker_url><topic>cmd</topic>"
            "<qos>0</qos><message_field>payload</message_field><retained>Y</retained></step>"
        )
        mcfg = parse_mqtt_producer_config(mqtt)
        self.assertEqual(mcfg["broker_url"], "ssl://mqtt:8883")
        self.assertEqual(mcfg["message_field"], "payload")
        self.assertEqual(mcfg["retained"], "Y")

    def test_validators_attached(self):
        register_builtin_validators()
        for st in (
            "RecordsFromStream",
            "KafkaConsumerInput",
            "KafkaProducerOutput",
            "JmsConsumer",
            "JmsProducer",
            "MQTTConsumer",
            "MQTTProducer",
        ):
            self.assertIsNotNone(get_validator(st), msg=st)

    def test_jms_mqtt_edge_missing_connection(self):
        jms = self.registry.convert_step(
            "JmsConsumer",
            _ctx("<step/>", "JmsConsumer", "J", with_input=False),
        )
        self.assertIn("missing JMS", "\n".join(jms.code_lines))
        mqtt = self.registry.convert_step(
            "MQTTConsumer",
            _ctx("<step/>", "MQTTConsumer", "M", with_input=False),
        )
        code = "\n".join(mqtt.code_lines)
        self.assertIn("missing MQTT broker", code)
        self.assertIn("UNSUPPORTED", code)


if __name__ == "__main__":
    unittest.main()
