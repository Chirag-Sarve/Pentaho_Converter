"""Audit converters for semantic defects."""
from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import get_step_element, parse_join_keys, parse_value_mappings
from pentaho_converter.steps.base import StepContext, build_default_registry


def ctx(xml: str, stype: str, name: str, inputs: list[str] | None = None) -> StepContext:
    step_el = ET.fromstring(textwrap.dedent(xml).strip())
    step = PentahoStep(name=name, step_type=stype, attributes={}, raw_element=step_el)
    trans = PentahoTransformation(name="T", file_path=Path("t.ktr"))
    ins = inputs or ["Input"]
    input_steps = [
        PentahoStep(name=n, step_type="RowGenerator", attributes={}, raw_element=None) for n in ins
    ]
    trans.steps = input_steps + [step]
    hops = [PentahoHop(from_name=n, to_name=name) for n in ins]
    dag = StepDAG(trans.steps, hops)
    df_map = {s.name: f"df_{s.name.replace(' ', '_')}" for s in trans.steps}
    return StepContext(transformation=trans, step=step, dag=dag, df_variable_map=df_map)


def syntax_ok(lines: list[str]) -> bool:
    try:
        ast.parse("def _f():\n" + "\n".join(f"    {l}" for l in lines))
        return True
    except SyntaxError:
        return False


def main() -> None:
    registry = build_default_registry()
    cases = [
        ("MergeJoin", "MergeJoin", "MJ", ["L", "R"], """
        <step><join_type>LEFT OUTER</join_type>
          <key_1>left_id</key_1><value_1>right_id</value_1>
          <key_2>code</key_2><value_2>code</value_2>
        </step>"""),
        ("MergeRows", "MergeRows", "MR", ["Ref", "Cmp"], """
        <step><flag_field>flag</flag_field>
          <key><name>id</name><field>id</field></key>
          <key><name>name</name><field>name</field></key>
        </step>"""),
        ("ValueMapper", "ValueMapper", "VM", None, """
        <step>
          <field_to_use>status</field_to_use><target_field>status_label</target_field>
          <fields><field><source_value>A</source_value><target_value>Active</target_value></field>
          <field><source_value>I</source_value><target_value>Inactive</target_value></field></fields>
        </step>"""),
        ("Formula", "Formula", "F", None, """
        <step><field_name>total</field_name><formula>[price] * [qty]</formula></step>"""),
        ("SelectValues", "SelectValues", "SV", None, """
        <step><fields>
          <field><name>id</name><rename>customer_id</rename></field>
          <field><name>name</name></field></fields></step>"""),
        ("TextFileOutput", "TextFileOutput", "TFO", None, """
        <step>
          <file>/out/data.csv</file><separator>|</separator><header>Y</header>
          <encoding>UTF-8</encoding><compression>gzip</compression>
          <file_appended>Y</file_appended><enclosure>"</enclosure></step>"""),
        ("StreamLookup", "StreamLookup", "SL", ["Main", "Lkp"], """
        <step><key_1>id</key_1><value_1>id</value_1></step>"""),
        ("DatabaseLookup", "DatabaseLookup", "DL", None, """
        <step><schema>dbo</schema><table>lookup</table>
          <key_1>id</key_1><value_1>lookup_id</value_1></step>"""),
    ]
    for label, stype, name, ins, xml in cases:
        c = ctx(xml, stype, name, ins)
        step_el = get_step_element(c.step)
        if stype in ("MergeJoin", "MergeRows", "StreamLookup", "DatabaseLookup"):
            print(f"{label} parsed keys: {parse_join_keys(step_el)}")
        if stype == "ValueMapper":
            print(f"{label} parsed mappings: {parse_value_mappings(step_el)}")
        outcome = registry.convert_step(stype, c)
        code = "\n".join(outcome.code_lines)
        print(f"--- {label}: status={outcome.status} score={outcome.semantic_score:.2f} ---")
        print(code)
        print("errors:", outcome.errors)
        print("warnings:", outcome.warnings)
        print("syntax:", "OK" if syntax_ok(outcome.code_lines) else "FAIL")
        print()


if __name__ == "__main__":
    main()
