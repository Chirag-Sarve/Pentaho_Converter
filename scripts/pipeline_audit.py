"""Deep pipeline audit — parser → converter → PySpark for edge-case XML."""
from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.step_xml import get_step_element, parse_calculations, parse_group_by_fields, parse_value_mappings
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


def main() -> None:
    registry = build_default_registry()
    failures: list[str] = []

    cases = [
        ("GroupBy subject", "GroupBy", "GB", None, """
        <step>
          <group><field><name>region</name></field></group>
          <fields>
            <field>
              <aggregate>SUM</aggregate>
              <subject>amount</subject>
              <name>total_amount</name>
              <type>Number</type>
            </field>
            <field>
              <aggregate>COUNT_ALL</aggregate>
              <subject>order_id</subject>
              <name>order_count</name>
            </field>
          </fields>
        </step>
        """, lambda code: 'col("amount")' in code and "total_amount" in code and "count" in code.lower()),
        ("Calculator MULTIPLY+remove", "Calculator", "Calc", None, """
        <step>
          <calculation>
            <field_name>total</field_name>
            <calc_type>MULTIPLY</calc_type>
            <field_a>price</field_a>
            <field_b>qty</field_b>
            <remove>Y</remove>
          </calculation>
        </step>
        """, lambda code: "*" in code and "drop" in code.lower()),
        ("CsvInput file tag", "CsvInput", "CSV", None, """
        <step><file>/data/in.csv</file><separator>;</separator><header>Y</header></step>
        """, lambda code: "/data/in.csv" in code),
        ("ValueMapper default", "ValueMapper", "VM", None, """
        <step>
          <field_to_use>status</field_to_use>
          <target_field>status_label</target_field>
          <non_match_default>Unknown</non_match_default>
          <fields><field><source_value>A</source_value><target_value>Active</target_value></field></fields>
        </step>
        """, lambda code: "Unknown" in code and "when(" in code),
        ("TextFileInput file tag", "TextFileInput", "TFI", None, """
        <step><file>/data/lines.txt</file></step>
        """, lambda code: "/data/lines.txt" in code),
        ("FilterRows IS NULL", "FilterRows", "F", None, """
        <step><compare><condition>
          <negated>N</negated>
          <leftvalue>email</leftvalue>
          <function>IS NULL</function>
        </condition></compare></step>
        """, lambda code: "isNull()" in code),
        ("FilterRows NOT", "FilterRows", "F2", None, """
        <step><compare><condition>
          <negated>Y</negated>
          <leftvalue>active</leftvalue>
          <function>=</function>
          <value><type>String</type><text>Y</text></value>
        </condition></compare></step>
        """, lambda code: "~" in code and "active" in code),
    ]

    for label, stype, name, ins, xml, check in cases:
        c = ctx(xml, stype, name, ins)
        step_el = get_step_element(c.step)
        if stype == "GroupBy" and step_el is not None:
            print(f"{label} parsed: {parse_group_by_fields(step_el)}")
        if stype == "Calculator" and step_el is not None:
            print(f"{label} parsed: {parse_calculations(step_el)}")
        if stype == "ValueMapper" and step_el is not None:
            print(f"{label} parsed default: {parse_value_mappings(step_el)[3]!r}")

        outcome = registry.convert_step(stype, c)
        code = "\n".join(outcome.code_lines)
        ok = check(code)
        print(f"--- {label}: status={outcome.status} ok={ok} ---")
        print(code)
        if not ok:
            failures.append(label)
        print()

    print("FAILURES:", failures or "none")


if __name__ == "__main__":
    main()
