"""Tests for Pentaho Data Warehouse step migration (Dimension / Combination Lookup)."""

from __future__ import annotations

import ast
import textwrap
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import (
    parse_combination_lookup_config,
    parse_dimension_lookup_config,
    parse_step_metadata,
)
from pentaho_converter.steps.base import StepContext, build_default_registry
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _ctx(step_xml: str, step_type: str, step_name: str, *, with_input: bool = True) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(step_xml).strip())
    step = PentahoStep(name=step_name, step_type=step_type, attributes={}, raw_element=step_el)
    step.parsed_config = parse_step_metadata(step_el, step_type)
    trans = PentahoTransformation(name="DWTrans", file_path=Path("dw.ktr"))
    hops: list[PentahoHop] = []
    steps: list[PentahoStep] = []
    if with_input:
        inp = PentahoStep(name="Input", step_type="RowGenerator", attributes={}, raw_element=None)
        steps.append(inp)
        hops.append(PentahoHop(from_name="Input", to_name=step_name))
    steps.append(step)
    trans.steps = steps
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def _syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {line}" for line in lines))
        return True
    except SyntaxError:
        return False


def _has_delta_merge(code: str) -> bool:
    """True if generated code uses SQL MERGE INTO or DeltaTable.merge API."""
    return (
        "MERGE INTO" in code
        or "DeltaTable" in code
        or ".merge(" in code
        or "whenMatchedUpdate" in code
        or "whenNotMatchedInsert" in code
    )


def _not_matched_insert_count(code: str) -> int:
    return code.count("WHEN NOT MATCHED THEN INSERT") + code.count("whenNotMatchedInsert")


_DIM_XML = """
<step>
  <name>Dim Customer</name>
  <type>DimensionLookup</type>
  <connection>dw</connection>
  <schema>mart</schema>
  <table>dim_customer</table>
  <commit>100</commit>
  <update>Y</update>
  <fields>
    <key>
      <name>customer_nk</name>
      <lookup>customer_bk</lookup>
    </key>
    <date>
      <name>change_date</name>
      <from>date_from</from>
      <to>date_to</to>
    </date>
    <field>
      <name>customer_name</name>
      <lookup>name</lookup>
      <update>Insert</update>
    </field>
    <field>
      <name>segment</name>
      <lookup>segment</lookup>
      <update>Update</update>
    </field>
    <field>
      <name></name>
      <lookup>date_last_updated</lookup>
      <update>DateUpdated</update>
    </field>
    <field>
      <name></name>
      <lookup>current_flag</lookup>
      <update>LastVersion</update>
    </field>
    <return>
      <name>customer_sk</name>
      <rename>dim_customer_sk</rename>
      <creation_method>tablemax</creation_method>
      <use_autoinc>N</use_autoinc>
      <version>version</version>
    </return>
  </fields>
  <sequence></sequence>
  <min_year>1900</min_year>
  <max_year>2199</max_year>
  <cache_size>5000</cache_size>
  <preload_cache>Y</preload_cache>
  <use_start_date_alternative>N</use_start_date_alternative>
  <start_date_alternative>none</start_date_alternative>
  <start_date_field_name></start_date_field_name>
  <useBatch>Y</useBatch>
</step>
"""

_COMBO_XML = """
<step>
  <name>Combo Junk</name>
  <type>CombinationLookup</type>
  <connection>dw</connection>
  <schema>mart</schema>
  <table>dim_junk</table>
  <commit>50</commit>
  <cache_size>9999</cache_size>
  <replace>Y</replace>
  <preloadCache>Y</preloadCache>
  <crc>N</crc>
  <crcfield></crcfield>
  <fields>
    <key>
      <name>status_code</name>
      <lookup>status_code</lookup>
    </key>
    <key>
      <name>channel</name>
      <lookup>channel_cd</lookup>
    </key>
    <return>
      <name>junk_sk</name>
      <creation_method>sequence</creation_method>
      <use_autoinc>N</use_autoinc>
    </return>
  </fields>
  <sequence>seq_junk</sequence>
  <last_update_field>last_updated</last_update_field>
</step>
"""


class TestDataWarehouseParsers(unittest.TestCase):
    def test_dimension_lookup_parse(self):
        cfg = parse_dimension_lookup_config(ET.fromstring(_DIM_XML))
        self.assertEqual(cfg["table"], "dim_customer")
        self.assertEqual(cfg["schema"], "mart")
        self.assertTrue(cfg["update"])
        self.assertEqual(cfg["technical_key"], "customer_sk")
        self.assertEqual(cfg["technical_key_rename"], "dim_customer_sk")
        self.assertEqual(cfg["version_field"], "version")
        self.assertEqual(cfg["stream_datefield"], "change_date")
        self.assertEqual(cfg["date_from"], "date_from")
        self.assertEqual(cfg["date_to"], "date_to")
        self.assertEqual(cfg["min_year"], 1900)
        self.assertEqual(cfg["max_year"], 2199)
        self.assertEqual(len(cfg["keys"]), 1)
        self.assertEqual(cfg["keys"][0]["stream_field"], "customer_nk")
        self.assertEqual(cfg["keys"][0]["table_field"], "customer_bk")
        update_types = {f["table_field"]: f["update_type"] for f in cfg["fields"]}
        self.assertEqual(update_types["name"], "Insert")
        self.assertEqual(update_types["segment"], "Update")
        self.assertEqual(update_types["current_flag"], "LastVersion")
        self.assertTrue(cfg["cached"])

    def test_combination_lookup_parse(self):
        cfg = parse_combination_lookup_config(ET.fromstring(_COMBO_XML))
        self.assertEqual(cfg["table"], "dim_junk")
        self.assertEqual(cfg["technical_key"], "junk_sk")
        self.assertEqual(cfg["tech_key_creation"], "sequence")
        self.assertEqual(cfg["sequence_name"], "seq_junk")
        self.assertTrue(cfg["replace_fields"])
        self.assertEqual(cfg["last_update_field"], "last_updated")
        self.assertEqual(len(cfg["keys"]), 2)
        self.assertEqual(cfg["keys"][1]["table_field"], "channel_cd")

    def test_parse_step_metadata_aliases(self):
        dim = parse_step_metadata(ET.fromstring(_DIM_XML), "DimensionLookup")
        self.assertEqual(dim["technical_key"], "customer_sk")
        combo = parse_step_metadata(ET.fromstring(_COMBO_XML), "CombinationLookup")
        self.assertEqual(combo["technical_key"], "junk_sk")


class TestDataWarehouseCodegen(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_dimension_lookup_scd_merge(self):
        outcome = self.registry.convert_step(
            "DimensionLookup",
            _ctx(_DIM_XML, "DimensionLookup", "Dim_Customer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotEqual(outcome.status, "failed")
        self.assertTrue(_has_delta_merge(code), code)
        self.assertIn("spark.table(", code)
        self.assertIn(".join(", code)
        self.assertIn("Type 1", code)
        self.assertIn("Type 2", code)
        self.assertIn("customer_sk", code)
        self.assertIn("dim_customer_sk", code)
        self.assertIn("preserved.cache_size", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_dimension_lookup_only_mode(self):
        xml = _DIM_XML.replace("<update>Y</update>", "<update>N</update>")
        outcome = self.registry.convert_step(
            "DimensionLookup",
            _ctx(xml, "DimensionLookup", "Dim_Lookup_Only"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("lookup-only", code.lower())
        self.assertIn(".join(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_dimension_missing_keys_partial(self):
        xml = """
        <step>
          <connection>dw</connection>
          <table>dim_x</table>
          <update>Y</update>
          <fields>
            <return>
              <name>sk</name>
              <creation_method>tablemax</creation_method>
              <version>ver</version>
            </return>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "DimensionLookup",
            _ctx(xml, "DimensionLookup", "Dim_NoKeys"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("missing business keys", code.lower())
        self.assertIn("lit(None)", code)
        self.assertIn(outcome.status, ("partial", "partially_supported", "converted"))

    def test_combination_lookup_merge(self):
        outcome = self.registry.convert_step(
            "CombinationLookup",
            _ctx(_COMBO_XML, "CombinationLookup", "Combo_Junk"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertNotEqual(outcome.status, "failed")
        self.assertTrue(_has_delta_merge(code), code)
        self.assertTrue(
            "WHEN NOT MATCHED THEN INSERT" in code or "whenNotMatchedInsert" in code,
            code,
        )
        self.assertIn("junk_sk", code)
        self.assertIn("drop(", code)  # replace_fields
        self.assertIn("seq_junk", code)
        self.assertIn("broadcast(", code)
        self.assertIn("unionByName", code)  # avoid second full table scan
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_combination_missing_keys(self):
        xml = """
        <step>
          <table>dim_junk</table>
          <fields>
            <return><name>sk</name><creation_method>tablemax</creation_method></return>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "CombinationLookup",
            _ctx(xml, "CombinationLookup", "Combo_NoKeys"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("missing business keys", code.lower())

    def test_handlers_registered(self):
        for stype, name, xml in (
            ("DimensionLookup", "Dim_Customer", _DIM_XML),
            ("DimensionLookupUpdate", "Dim_Upd", _DIM_XML),
            ("CombinationLookup", "Combo_Junk", _COMBO_XML),
        ):
            lines, status = self.registry.generate_code(stype, _ctx(xml, stype, name))
            self.assertTrue(lines)
            self.assertNotEqual(status, "failed")
            code = "\n".join(lines)
            self.assertTrue(
                _has_delta_merge(code) or ".join(" in code,
                msg=stype,
            )


class TestDataWarehouseValidators(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()

    def test_dimension_validator(self):
        registry = build_default_registry()
        ctx = _ctx(_DIM_XML, "DimensionLookup", "Dim_Customer")
        outcome = registry.convert_step("DimensionLookup", ctx)
        validator = get_validator("dimensionlookup")
        self.assertIsNotNone(validator)
        result = validator.validate(ctx, ctx.step.parsed_config, outcome.code_lines)
        self.assertFalse(result.errors, result.errors)

    def test_combination_validator(self):
        registry = build_default_registry()
        ctx = _ctx(_COMBO_XML, "CombinationLookup", "Combo_Junk")
        outcome = registry.convert_step("CombinationLookup", ctx)
        validator = get_validator("combinationlookup")
        self.assertIsNotNone(validator)
        result = validator.validate(ctx, ctx.step.parsed_config, outcome.code_lines)
        self.assertFalse(result.errors, result.errors)

    def test_dimension_partial_validator_accepts_missing_keys(self):
        registry = build_default_registry()
        xml = """
        <step>
          <table>dim_x</table>
          <update>Y</update>
          <fields>
            <return><name>sk</name><creation_method>tablemax</creation_method></return>
          </fields>
        </step>
        """
        ctx = _ctx(xml, "DimensionLookup", "Dim_NoKeys")
        outcome = registry.convert_step("DimensionLookup", ctx)
        validator = get_validator("dimensionlookup")
        result = validator.validate(ctx, ctx.step.parsed_config, outcome.code_lines)
        self.assertFalse(result.errors, result.errors)
        self.assertTrue(result.warnings)


class TestDataWarehouseEdgeCases(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_punch_through_all_versions(self):
        xml = """
        <step>
          <connection>dw</connection>
          <schema>mart</schema>
          <table>dim_product</table>
          <update>Y</update>
          <fields>
            <key><name>sku</name><lookup>sku</lookup></key>
            <field><name>brand</name><lookup>brand</lookup><update>PunchThrough</update></field>
            <field><name>category</name><lookup>category</lookup><update>Update</update></field>
            <return>
              <name>product_sk</name>
              <creation_method>tablemax</creation_method>
              <version>version</version>
            </return>
          </fields>
          <cache_size>100</cache_size>
        </step>
        """
        outcome = self.registry.convert_step(
            "DimensionLookup", _ctx(xml, "DimensionLookup", "Dim_Punch")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("PunchThrough", code)
        self.assertIn("all historical versions", code.lower())
        # PunchThrough MERGE should appear without active-flag filter on that statement
        self.assertTrue(_has_delta_merge(code), code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_type2_version_bump_and_single_tk_insert(self):
        outcome = self.registry.convert_step(
            "DimensionLookup",
            _ctx(_DIM_XML, "DimensionLookup", "Dim_Customer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("Type 2 expire", code)
        self.assertIn("coalesce(col(\"_prior_version\")", code)
        # Insert MERGE must key on technical key (not duplicate BK MERGEs)
        self.assertIn("t.`customer_sk` <=> s.`customer_sk`", code)
        self.assertEqual(_not_matched_insert_count(code), 1)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_post_update_rejoin_not_stale(self):
        xml = """
        <step>
          <table>dim_x</table>
          <update>Y</update>
          <fields>
            <key><name>id</name><lookup>id</lookup></key>
            <field><name>name</name><lookup>name</lookup><update>Insert</update></field>
            <return>
              <name>sk</name>
              <creation_method>tablemax</creation_method>
              <version>ver</version>
            </return>
          </fields>
        </step>
        """
        outcome = self.registry.convert_step(
            "DimensionLookup", _ctx(xml, "DimensionLookup", "Dim_VerOnly")
        )
        code = "\n".join(outcome.code_lines)
        # Must refresh from spark.table after MERGEs and join again (not assign stale _dim_joined)
        after_insert = code
        for marker in ("whenNotMatchedInsert", "WHEN NOT MATCHED"):
            if marker in code:
                after_insert = code.split(marker)[-1]
                break
        self.assertNotIn("= _dim_joined", after_insert)
        self.assertIn("skip pre-merge lookup join", code.lower())
        self.assertIn("row_number().over(", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_start_date_alternative_null_uses_min_ts(self):
        xml = _DIM_XML.replace(
            "<use_start_date_alternative>N</use_start_date_alternative>",
            "<use_start_date_alternative>Y</use_start_date_alternative>",
        ).replace(
            "<start_date_alternative>none</start_date_alternative>",
            "<start_date_alternative>null</start_date_alternative>",
        )
        outcome = self.registry.convert_step(
            "DimensionLookup", _ctx(xml, "DimensionLookup", "Dim_StartNull")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_scd_min_ts", code)
        self.assertIn("start_date_alternative=null", code)
        # Bounds defined before use
        min_idx = code.index("_scd_min_ts = lit(")
        use_idx = code.index('withColumn("_scd_effective", _scd_min_ts)')
        self.assertLess(min_idx, use_idx)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_dimension_null_bk_and_dedupe(self):
        outcome = self.registry.convert_step(
            "DimensionLookup",
            _ctx(_DIM_XML, "DimensionLookup", "Dim_Customer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("null business keys skipped", code.lower())
        self.assertIn("dropDuplicates", code)
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_unique_temp_views_per_step(self):
        outcome = self.registry.convert_step(
            "DimensionLookup",
            _ctx(_DIM_XML, "DimensionLookup", "Dim_Customer"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_dim_scd_src_df_Dim_Customer", code)
        self.assertIn("_dim_scd_new_df_Dim_Customer", code)

    def test_combination_cache_miss_and_crc_warning(self):
        xml = _COMBO_XML.replace("<crc>N</crc>", "<crc>Y</crc>").replace(
            "<crcfield></crcfield>", "<crcfield>hashcode</crcfield>"
        )
        outcome = self.registry.convert_step(
            "CombinationLookup", _ctx(xml, "CombinationLookup", "Combo_CRC")
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("CRC/hash cache", code)
        self.assertIn("hashcode", code)
        self.assertIn("null surrogate keys after merge", code.lower())
        self.assertTrue(_syntax_ok(outcome.code_lines), code)

    def test_combination_unique_temp_view(self):
        outcome = self.registry.convert_step(
            "CombinationLookup",
            _ctx(_COMBO_XML, "CombinationLookup", "Combo_Junk"),
        )
        code = "\n".join(outcome.code_lines)
        self.assertIn("_combo_insert_src_df_Combo_Junk", code)


if __name__ == "__main__":
    unittest.main()
