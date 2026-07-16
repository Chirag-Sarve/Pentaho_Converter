"""Tests for Pentaho Big Data transformations: Avro and MongoDB I/O."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_avro_input_config,
    parse_avro_output_config,
    parse_mongodb_input_config,
    parse_mongodb_output_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry


def _ctx(step_xml: str, step_type: str, step_name: str, with_input: bool = False) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    trans = PentahoTransformation(name="BigDataTrans", file_path=Path("bigdata.ktr"))
    if with_input:
        inp = PentahoStep(name="Upstream", step_type="RowGenerator", attributes={}, raw_element=None)
        trans.steps = [inp, step]
        hops = [PentahoHop(from_name="Upstream", to_name=step_name)]
    else:
        trans.steps = [step]
        hops = []
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


class TestAvroParsers(unittest.TestCase):
    def test_parse_avro_input_full(self):
        xml = """
        <step>
          <filename>/data/events.avro</filename>
          <schemaFilename>/schemas/events.avsc</schemaFilename>
          <compression>snappy</compression>
          <encoding>UTF-8</encoding>
          <include_subfolders>Y</include_subfolders>
          <filemask>*.avro</filemask>
          <fields>
            <field><name>id</name><path>id</path><type>Integer</type></field>
            <field><name>payload</name><path>body</path><type>String</type></field>
          </fields>
        </step>
        """
        cfg = parse_avro_input_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["filename"], "/data/events.avro")
        self.assertEqual(cfg["schema_filename"], "/schemas/events.avsc")
        self.assertEqual(cfg["compression"], "snappy")
        self.assertEqual(cfg["encoding"], "UTF-8")
        self.assertEqual(cfg["recursive"], "Y")
        self.assertEqual(cfg["filemask"], "*.avro")
        self.assertEqual(cfg["output_columns"], ["id", "payload"])

    def test_parse_avro_output_mode(self):
        xml = """
        <step>
          <filename>/data/out.avro</filename>
          <append>Y</append>
          <compression>deflate</compression>
          <schemaEvolution>Y</schemaEvolution>
          <namespace>com.example</namespace>
        </step>
        """
        cfg = parse_avro_output_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["append"], "Y")
        self.assertEqual(cfg["compression"], "deflate")
        self.assertEqual(cfg["schema_evolution"], "Y")
        self.assertEqual(cfg["namespace"], "com.example")

    def test_parse_step_metadata_routes_avro(self):
        el = ET.fromstring("<step><filename>/a.avro</filename></step>")
        meta = parse_step_metadata(el, "AvroInput")
        self.assertEqual(meta.get("filename"), "/a.avro")


class TestMongoParsers(unittest.TestCase):
    def test_parse_mongodb_input(self):
        xml = """
        <step>
          <hostname>mongo.example.com</hostname>
          <port>27017</port>
          <db_name>analytics</db_name>
          <collection>events</collection>
          <query>{"status": "active"}</query>
          <fields_name>{"name": 1, "status": 1}</fields_name>
          <auth_user>app</auth_user>
          <auth_password>secret</auth_password>
          <auth_db>admin</auth_db>
          <read_preference>secondary preferred</read_preference>
          <batch_size>500</batch_size>
          <output_json>N</output_json>
        </step>
        """
        cfg = parse_mongodb_input_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["database"], "analytics")
        self.assertEqual(cfg["collection"], "events")
        self.assertIn("status", cfg["query"])
        self.assertEqual(cfg["batch_size"], "500")
        self.assertIn("mongodb://", cfg["connection_uri"])
        self.assertIn("app:", cfg["connection_uri"])
        self.assertIn("readPreference=", cfg["connection_uri"])

    def test_parse_mongodb_output_upsert(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <db_name>warehouse</db_name>
          <collection>dim_customer</collection>
          <upsert>Y</upsert>
          <update>Y</update>
          <batch_insert_size>1000</batch_insert_size>
          <fields>
            <field>
              <incoming_field_name>customer_id</incoming_field_name>
              <mongo_doc_path>customer_id</mongo_doc_path>
              <update_match_field>Y</update_match_field>
            </field>
          </fields>
        </step>
        """
        cfg = parse_mongodb_output_config(ET.fromstring(textwrap.dedent(xml).strip()))
        self.assertEqual(cfg["collection"], "dim_customer")
        self.assertEqual(cfg["upsert"], "Y")
        self.assertEqual(cfg["batch_size"], "1000")
        self.assertEqual(cfg["match_fields"], ["customer_id"])


class TestAvroConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_avro_input_native_read(self):
        xml = """
        <step>
          <filename>/Volumes/data/events.avro</filename>
          <schemaFilename>/Volumes/schemas/events.avsc</schemaFilename>
          <compression>snappy</compression>
          <include_subfolders>Y</include_subfolders>
        </step>
        """
        outcome = self.registry.convert_step("AvroInput", _ctx(xml, "AvroInput", "AI"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('avro')", code)
        self.assertIn("/Volumes/data/events.avro", code)
        self.assertIn("avroSchemaUrl", code)
        self.assertIn("recursiveFileLookup", code)
        self.assertIn("snappy", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_avro_input_missing_schema_warns(self):
        xml = "<step><filename>/data/a.avro</filename></step>"
        outcome = self.registry.convert_step("AvroInput", _ctx(xml, "AvroInput", "AI2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('avro')", code)
        self.assertIn("WARNING: No Avro schema", code)

    def test_avro_input_missing_path(self):
        xml = "<step/>"
        outcome = self.registry.convert_step("AvroInput", _ctx(xml, "AvroInput", "AI3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: Avro input path missing", code)
        self.assertIn("format('avro')", code)

    def test_avro_output_append_compression(self):
        xml = """
        <step>
          <filename>/data/out.avro</filename>
          <append>Y</append>
          <compression>snappy</compression>
          <schemaEvolution>Y</schemaEvolution>
        </step>
        """
        outcome = self.registry.convert_step(
            "AvroOutput", _ctx(xml, "AvroOutput", "AO", with_input=True)
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('avro')", code)
        self.assertIn("append", code)
        self.assertIn("snappy", code)
        self.assertIn("schema_evolution", code)
        self.assertIn("PENTAHO_DATA_DIR", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_avro_file_aliases(self):
        xml = "<step><filename>/a.avro</filename></step>"
        for step_type in ("AvroFileInput", "AvroFileOutput"):
            with_input = "Output" in step_type
            outcome = self.registry.convert_step(
                step_type, _ctx(xml, step_type, step_type, with_input=with_input)
            )
            self.assertIn("format('avro')", "\n".join(outcome.code_lines))


class TestMongoConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = build_default_registry()

    def test_mongodb_input_connector(self):
        xml = """
        <step>
          <hostname>mongo.db.local</hostname>
          <port>27017</port>
          <db_name>sales</db_name>
          <collection>orders</collection>
          <query>{"status": "open"}</query>
          <fields_name>{"order_id": 1}</fields_name>
          <auth_user>reader</auth_user>
          <auth_password>pw</auth_password>
          <auth_db>admin</auth_db>
          <read_preference>primary</read_preference>
          <batch_size>200</batch_size>
        </step>
        """
        outcome = self.registry.convert_step("MongoDbInput", _ctx(xml, "MongoDbInput", "MI"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('mongodb')", code)
        self.assertIn("connection.uri", code)
        self.assertIn("sales", code)
        self.assertIn("orders", code)
        self.assertIn("$match", code)
        self.assertIn("batchSize", code)
        self.assertIn("readPreference", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_mongodb_input_aggregation_pipeline(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <db_name>db</db_name>
          <collection>c</collection>
          <query_is_pipeline>Y</query_is_pipeline>
          <agg_pipeline>[{"$group": {"_id": "$k", "n": {"$sum": 1}}}]</agg_pipeline>
        </step>
        """
        outcome = self.registry.convert_step("MongoDbInput", _ctx(xml, "MongoDbInput", "MI2"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("aggregation.pipeline", code)
        self.assertIn("$group", code)

    def test_mongodb_input_missing_connection(self):
        xml = "<step/>"
        outcome = self.registry.convert_step("MongoDbInput", _ctx(xml, "MongoDbInput", "MI3"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: MongoDB connection", code)
        self.assertIn("createDataFrame", code)
        self.assertIn("format('mongodb')", code)
        self.assertEqual(outcome.status, "converted")
        self.assertFalse(outcome.errors)

    def test_mongodb_input_auth_secrets_validates(self):
        xml = """
        <step>
          <hostname>h</hostname>
          <db_name>d</db_name>
          <collection>c</collection>
          <auth_user>u</auth_user>
          <auth_password>p</auth_password>
        </step>
        """
        outcome = self.registry.convert_step("MongoDbInput", _ctx(xml, "MongoDbInput", "AUTH"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("dbutils.secrets", code)
        self.assertEqual(outcome.status, "converted")
        self.assertFalse(outcome.errors)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_mongodb_input_json_output_validates(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <db_name>db</db_name>
          <collection>c</collection>
          <output_json>Y</output_json>
          <json_field>doc</json_field>
        </step>
        """
        outcome = self.registry.convert_step("MongoDbInput", _ctx(xml, "MongoDbInput", "JSONVAL"))
        self.assertEqual(outcome.status, "converted")
        self.assertFalse(outcome.errors)
        self.assertIn("to_json", "\n".join(outcome.code_lines))


    def test_mongodb_output_upsert(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <db_name>warehouse</db_name>
          <collection>customers</collection>
          <upsert>Y</upsert>
          <update>Y</update>
          <batch_insert_size>500</batch_insert_size>
          <fields>
            <field>
              <incoming_field_name>id</incoming_field_name>
              <mongo_doc_path>id</mongo_doc_path>
              <update_match_field>Y</update_match_field>
            </field>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "MongoDbOutput", _ctx(xml, "MongoDbOutput", "MO", with_input=True)
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("format('mongodb')", code)
        self.assertIn("upsertDocument", code)
        self.assertIn("idFieldList", code)
        self.assertIn("maxBatchSize", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_mongodb_output_truncate_overwrite(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <db_name>db</db_name>
          <collection>c</collection>
          <truncate>Y</truncate>
        </step>
        """
        outcome = self.registry.convert_step(
            "MongoDbOutput", _ctx(xml, "MongoDbOutput", "MO2", with_input=True)
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("overwrite", code)

    def test_mongodb_output_missing_connection_passthrough(self):
        xml = "<step/>"
        outcome = self.registry.convert_step(
            "MongoDbOutput", _ctx(xml, "MongoDbOutput", "MO3", with_input=True)
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("WARNING: MongoDB connection", code)
        self.assertIn("format('mongodb')", code)
        self.assertIn("df_Upstream", code)
        self.assertEqual(outcome.status, "converted")
        self.assertFalse(outcome.errors)

    def test_mongodb_input_combines_query_and_projection(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <db_name>db</db_name>
          <collection>c</collection>
          <query>{"status": "open"}</query>
          <fields_name>{"order_id": 1, "status": 1}</fields_name>
        </step>
        """
        outcome = self.registry.convert_step("MongoDbInput", _ctx(xml, "MongoDbInput", "MI5"))
        code = "\n".join(outcome.code_lines)
        self.assertIn("$match", code)
        self.assertIn("$project", code)
        self.assertIn("aggregation.pipeline", code)
        self.assertTrue(_syntax_ok(outcome.code_lines))

    def test_avro_output_overwrite_disabled(self):
        xml = """
        <step>
          <filename>/data/out.avro</filename>
          <overwrite>N</overwrite>
          <append>N</append>
        </step>
        """
        outcome = self.registry.convert_step(
            "AvroOutput", _ctx(xml, "AvroOutput", "AO2", with_input=True)
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("errorifexists", code)

    def test_mongo_aliases(self):
        xml = """
        <step>
          <hostname>localhost</hostname>
          <db_name>db</db_name>
          <collection>c</collection>
        </step>
        """
        for step_type in ("MongoInput", "MongoOutput"):
            with_input = "Output" in step_type
            outcome = self.registry.convert_step(
                step_type, _ctx(xml, step_type, step_type, with_input=with_input)
            )
            self.assertIn("format('mongodb')", "\n".join(outcome.code_lines))


if __name__ == "__main__":
    unittest.main()
