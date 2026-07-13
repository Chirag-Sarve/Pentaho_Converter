"""Parse nested Pentaho step XML structures from step elements."""

from __future__ import annotations

import html
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any
from xml.etree import ElementTree as ET

# Pentaho CalculatorMetaFunction integer IDs → type names (common subset).
_CALC_TYPE_BY_ID: dict[str, str] = {
    "1": "CONSTANT",
    "2": "COPY_OF_FIELD",
    "3": "ADD",
    "4": "SUBTRACT",
    "5": "MULTIPLY",
    "6": "DIVIDE",
    "7": "SQUARE",
    "8": "SQUARE_ROOT",
    "9": "PERCENT_1",
    "10": "PERCENT_2",
    "11": "PERCENT_3",
    "12": "COMBINATION_1",
    "13": "COMBINATION_2",
    "14": "ROUND_1",
    "15": "ROUND_2",
    "88": "REMAINDER",
}

# Step types whose nested XML is parsed into ``PentahoStep.parsed_config``.
_STRUCTURED_STEP_TYPES = frozenset({
    "calculator",
    "mergejoin",
    "groupby",
    "valuemapper",
    "filterrows",
    "rowgenerator",
    "datagrid",
    "sequence",
    "textfileoutput",
    "tableinput",
    "databaselookup",
    "streamlookup",
    "formula",
    "ifnull",
    "iffieldvaluenull",
})

_KNOWN_GROUP_AGG_TYPES = frozenset({
    "SUM",
    "AVG",
    "AVERAGE",
    "MIN",
    "MAX",
    "COUNT",
    "COUNT_ALL",
    "COUNT_ANY",
    "COUNT_DISTINCT",
    "FIRST",
    "LAST",
    "MEDIAN",
    "PERCENTILE",
    "STDDEV",
    "STD_DEV",
})


def _normalize_agg_token(raw: str) -> str:
    return (raw or "").strip().upper().replace(" ", "_")


def _is_known_aggregate_type(raw: str) -> bool:
    return _normalize_agg_token(raw) in _KNOWN_GROUP_AGG_TYPES


def _text(elem: ET.Element | None, default: str = "") -> str:
    if elem is None or elem.text is None:
        return default
    return elem.text.strip()


def _child_text(parent: ET.Element, tag: str, default: str = "") -> str:
    return _text(parent.find(tag), default)


def _bool_from_yn(value: str, default: bool = False) -> bool:
    if not value:
        return default
    return value.strip().upper() in ("Y", "YES", "TRUE", "1")


def _normalize_calc_type(raw: str) -> str:
    if not raw:
        return ""
    text = raw.strip()
    if text.isdigit():
        return _CALC_TYPE_BY_ID.get(text, text)
    return text


def _element_to_dict(elem: ET.Element) -> dict[str, Any]:
    """Recursively convert an XML element to a JSON-serializable dict."""
    result: dict[str, Any] = {}
    if elem.text and elem.text.strip():
        result["_text"] = elem.text.strip()
    for child in elem:
        val: Any = _text(child) if len(child) == 0 else _element_to_dict(child)
        tag = child.tag
        if tag in result:
            existing = result[tag]
            if isinstance(existing, list):
                existing.append(val)
            else:
                result[tag] = [existing, val]
        else:
            result[tag] = val
    return result


def _metadata_value(obj: Any) -> Any:
    if is_dataclass(obj):
        return {k: _metadata_value(v) for k, v in asdict(obj).items()}
    if isinstance(obj, list):
        return [_metadata_value(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _metadata_value(v) for k, v in obj.items()}
    return obj


@dataclass
class ConstantField:
    name: str
    type_name: str = "String"
    value: str = ""
    set_empty_string: bool = False


@dataclass
class RowGeneratorField:
    name: str
    type_name: str = "String"
    value: str = ""
    length: str = ""
    precision: str = ""
    format: str = ""
    set_empty_string: bool = False


@dataclass
class CalculationSpec:
    field_name: str
    calc_type: str
    field_a: str = ""
    field_b: str = ""
    field_c: str = ""
    value_type: str = ""
    conversion_mask: str = ""
    value: str = ""
    remove: bool = False
    decimal_symbol: str = ""
    grouping_symbol: str = ""
    currency_symbol: str = ""
    value_length: str = ""
    value_precision: str = ""


@dataclass
class StringOpField:
    in_stream_name: str
    out_stream_name: str = ""
    trim_type: str = "none"
    lower_upper: str = "none"
    init_cap: bool = False
    # Optional extended operations (StringCut / ReplaceInString compatibility)
    cut_from: str = ""
    cut_to: str = ""
    replace_string: str = ""
    replace_by_string: str = ""
    use_regex: bool = False


def get_step_element(step) -> ET.Element | None:
    """Return the raw XML element for a parsed step."""
    return step.raw_element


def parse_constant_fields(step_el: ET.Element) -> list[ConstantField]:
    """Parse Add Constants step field definitions."""
    results: list[ConstantField] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        name = _child_text(field_el, "name")
        if not name:
            continue
        value = _child_text(field_el, "value") or _child_text(field_el, "nullif")
        results.append(
            ConstantField(
                name=name,
                type_name=_child_text(field_el, "type", "String"),
                value=value,
                set_empty_string=_child_text(field_el, "set_empty_string", "N").upper() == "Y",
            )
        )
    return results


def parse_row_generator_fields(step_el: ET.Element) -> list[RowGeneratorField]:
    """Parse Generate Rows field definitions (value in default, value, or nullif)."""
    results: list[RowGeneratorField] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        name = _child_text(field_el, "name")
        if not name:
            continue
        value = (
            _child_text(field_el, "default")
            or _child_text(field_el, "value")
            or _child_text(field_el, "string")
            or _child_text(field_el, "nullif")
        )
        if _child_text(field_el, "set_empty_string", "N").upper() == "Y" and value == "":
            value = ""
        results.append(
            RowGeneratorField(
                name=name,
                type_name=_child_text(field_el, "type", "String"),
                value=value,
                length=_child_text(field_el, "length"),
                precision=_child_text(field_el, "precision"),
                format=_child_text(field_el, "format"),
                set_empty_string=_child_text(field_el, "set_empty_string", "N").upper() == "Y",
            )
        )
    return results


def parse_file_block(step_el: ET.Element) -> dict[str, Any]:
    """Parse nested ``<file>`` block (Text File Input/Output)."""
    file_el = step_el.find("file")
    if file_el is None:
        return {}
    block: dict[str, Any] = {}
    for child in file_el:
        if len(child) == 0:
            block[child.tag] = _text(child)
        else:
            block[child.tag] = _element_to_dict(child)
    if not block.get("name") and file_el.text and file_el.text.strip():
        block["name"] = file_el.text.strip()
    return block


def extract_step_property(step_el: ET.Element, key: str, default: str = "") -> str:
    """Read a scalar step property, including nested <file><name> paths."""
    if key in ("file", "filename"):
        file_block = parse_file_block(step_el)
        path = file_block.get("name", "")
        if path:
            return str(path)
        file_el = step_el.find("file")
        if file_el is not None and file_el.text and file_el.text.strip():
            return file_el.text.strip()
    return _child_text(step_el, key, default)


def parse_data_grid_rows(step_el: ET.Element) -> tuple[list[str], list[list[str]]]:
    """Parse Data Grid / embedded line data into column names and row values."""
    columns = [f.name for f in parse_row_generator_fields(step_el) if f.name]
    rows: list[list[str]] = []
    data_el = step_el.find("data")
    if data_el is None:
        return columns, rows
    for line_el in data_el.findall("line"):
        items = [_text(item) for item in line_el.findall("item")]
        if items:
            rows.append(items)
    return columns, rows


def parse_calculations(step_el: ET.Element) -> list[CalculationSpec]:
    """Parse all Calculator step calculation entries."""
    results: list[CalculationSpec] = []
    for calc_el in step_el.findall(".//calculation"):
        field_name = _child_text(calc_el, "field_name")
        if not field_name:
            continue
        results.append(
            CalculationSpec(
                field_name=field_name,
                calc_type=_normalize_calc_type(_child_text(calc_el, "calc_type")),
                field_a=_child_text(calc_el, "field_a"),
                field_b=_child_text(calc_el, "field_b"),
                field_c=_child_text(calc_el, "field_c"),
                value_type=_child_text(calc_el, "value_type"),
                conversion_mask=_child_text(calc_el, "conversion_mask"),
                value=_child_text(calc_el, "value"),
                remove=_child_text(calc_el, "remove", "N").upper() == "Y",
                decimal_symbol=_child_text(calc_el, "decimal_symbol"),
                grouping_symbol=_child_text(calc_el, "grouping_symbol"),
                currency_symbol=_child_text(calc_el, "currency_symbol"),
                value_length=_child_text(calc_el, "value_length"),
                value_precision=_child_text(calc_el, "value_precision"),
            )
        )
    return results


def parse_string_operation_fields(step_el: ET.Element) -> list[StringOpField]:
    """Parse String Operations step field definitions."""
    results: list[StringOpField] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        in_name = _child_text(field_el, "in_stream_name") or _child_text(field_el, "name")
        if not in_name:
            continue
        out_name = _child_text(field_el, "out_stream_name") or in_name
        results.append(
            StringOpField(
                in_stream_name=in_name,
                out_stream_name=out_name,
                trim_type=_child_text(field_el, "trim_type", "none").lower(),
                lower_upper=_child_text(field_el, "lower_upper", "none").lower(),
                init_cap=_child_text(field_el, "initcap", "N").upper() == "Y"
                or _child_text(field_el, "init_cap", "N").upper() == "Y",
                cut_from=_child_text(field_el, "cut_from") or _child_text(field_el, "start"),
                cut_to=_child_text(field_el, "cut_to") or _child_text(field_el, "end"),
                replace_string=_child_text(field_el, "replace_string") or _child_text(field_el, "search"),
                replace_by_string=_child_text(field_el, "replace_by_string")
                or _child_text(field_el, "replace"),
                use_regex=_child_text(field_el, "use_regex", "N").upper() == "Y",
            )
        )
    return results


def parse_filter_compare_element(step_el: ET.Element) -> ET.Element | None:
    """Return the root condition element from a Filter Rows step."""
    compare_el = step_el.find("compare")
    if compare_el is None:
        return None
    condition = compare_el.find("condition")
    if condition is not None:
        return condition
    # Some exports place conditions directly under compare
    if compare_el.find("conditions") is not None:
        return compare_el
    return compare_el


def parse_value_constant(value_el: ET.Element | None) -> tuple[str, str]:
    """Return (type_name, text_value) from a Pentaho condition value element."""
    if value_el is None:
        return "String", ""
    type_name = _child_text(value_el, "type", "String")
    text_val = _child_text(value_el, "text")
    if _child_text(value_el, "isnull", "N").upper() == "Y":
        return type_name, ""
    return type_name, text_val


def unescape_xml(text: str) -> str:
    """Decode XML entities in Pentaho function operators."""
    return html.unescape(text or "").strip()


@dataclass
class GroupByField:
    name: str
    aggregate: str = ""
    subject: str = ""
    type_name: str = "String"
    valuefield: str = ""


@dataclass
class JoinKeyPair:
    left: str
    right: str


@dataclass
class ValueMapping:
    source: str
    target: str


@dataclass
class SequenceConfig:
    field_name: str = "seq"
    start_at: int = 1
    increment_by: int = 1
    max_value: int | None = None


@dataclass
class TextFileOutputConfig:
    filename: str = ""
    extension: str = ""
    separator: str = ","
    header: bool = True
    footer: bool = False
    encoding: str = "utf-8"
    compression: str = "none"
    enclosure: str = ""
    append: bool = False
    create_parent_folder: bool = False
    split: bool = False
    fast_dump: bool = False
    padded: bool = False
    file: dict[str, Any] = field(default_factory=dict)
    output_fields: list[dict[str, str]] = field(default_factory=list)


@dataclass
class TableInputConfig:
    connection: str = ""
    sql: str = ""
    schema: str = ""
    table: str = ""
    limit: int = 0
    execute_each_row: bool = False
    variables_active: bool = False
    lazy_conversion: bool = False
    parameters: list[dict[str, str]] = field(default_factory=list)


@dataclass
class DatabaseLookupKey:
    stream_field: str = ""
    table_field: str = ""
    name2: str = ""


@dataclass
class DatabaseLookupReturnField:
    name: str = ""
    rename: str = ""
    default: str = ""
    type_name: str = ""


@dataclass
class DatabaseLookupConfig:
    connection: str = ""
    schema: str = ""
    table: str = ""
    cached: bool = False
    cache_size: int = 0
    orderby: str = ""
    fail_on_multiple: bool = False
    eat_row_on_failure: bool = False
    keys: list[DatabaseLookupKey] = field(default_factory=list)
    return_fields: list[DatabaseLookupReturnField] = field(default_factory=list)


@dataclass
class MergeJoinConfig:
    join_type: str = "INNER"
    step1: str = ""
    step2: str = ""
    keys: list[JoinKeyPair] = field(default_factory=list)
    keys_1: list[str] = field(default_factory=list)
    keys_2: list[str] = field(default_factory=list)


@dataclass
class SwitchCaseRule:
    value: str
    target_step: str = ""


@dataclass
class RankConfig:
    top_bottom: str = "top"
    rank: bool = True
    sort_size: int = 10
    field_name: str = ""
    rank_field: str = "rank"


def _parse_group_by_aggregate_field(field_el: ET.Element) -> GroupByField | None:
    """Parse one Group By aggregate field from Pentaho XML (both export formats)."""
    name = _child_text(field_el, "name")
    agg_raw = _child_text(field_el, "aggregate")
    type_raw = _child_text(field_el, "type")
    subject = _child_text(field_el, "subject")
    valuefield = _child_text(field_el, "valuefield")

    if not agg_raw:
        return None

    agg_is_type = _is_known_aggregate_type(agg_raw)
    type_is_agg = _is_known_aggregate_type(type_raw)

    if agg_is_type and name:
        output_name = name
        agg_type = agg_raw
        source = subject or valuefield or name
        value_type = type_raw if type_raw and not type_is_agg else "String"
    elif not agg_is_type and type_is_agg:
        output_name = agg_raw
        agg_type = type_raw
        source = subject or valuefield or output_name
        value_type = "String"
    elif name:
        output_name = name
        agg_type = agg_raw if agg_is_type else (type_raw or "SUM")
        source = subject or valuefield or name
        value_type = type_raw if type_raw and not type_is_agg else "String"
    else:
        return None

    if not output_name or not agg_type:
        return None

    return GroupByField(
        name=output_name,
        aggregate=_normalize_agg_token(agg_type),
        subject=source,
        type_name=value_type or "String",
        valuefield=valuefield,
    )


def parse_group_by_fields(step_el: ET.Element) -> tuple[list[str], list[GroupByField]]:
    """Return (group_key_names, aggregate_field_specs) from a Group By step."""
    group_keys: list[str] = []
    aggregates: list[GroupByField] = []

    group_el = step_el.find("group")
    if group_el is not None:
        for field_el in group_el.findall("field"):
            name = _child_text(field_el, "name")
            if name:
                group_keys.append(name)

    for container_tag in ("fields", "aggregates"):
        container = step_el.find(container_tag)
        if container is None:
            continue
        for field_el in container.findall("field"):
            name = _child_text(field_el, "name")
            parsed = _parse_group_by_aggregate_field(field_el)
            if parsed is not None:
                aggregates.append(parsed)
            elif container_tag == "fields" and name and not _child_text(field_el, "aggregate"):
                if name not in group_keys:
                    group_keys.append(name)

    if not group_keys and not aggregates:
        for field_el in (step_el.find("fields") or ET.Element("x")).findall("field"):
            name = _child_text(field_el, "name")
            parsed = _parse_group_by_aggregate_field(field_el)
            if parsed is not None:
                aggregates.append(parsed)
            elif name:
                group_keys.append(name)

    return group_keys, aggregates


def parse_sort_fields(step_el: ET.Element) -> list[tuple[str, bool]]:
    """Return list of (column_name, ascending) from Sort Rows step."""
    results: list[tuple[str, bool]] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        name = _child_text(field_el, "name")
        if not name:
            continue
        ascending = _child_text(field_el, "ascending", "Y").upper() != "N"
        results.append((name, ascending))
    return results


def parse_join_keys(step_el: ET.Element) -> list[JoinKeyPair]:
    """Parse join keys from Merge Join, lookup, and legacy key_N/value_N XML."""
    keys: list[JoinKeyPair] = []

    keys1_el = step_el.find("keys_1")
    keys2_el = step_el.find("keys_2")
    if keys1_el is not None and keys2_el is not None:
        left_keys = [_text(k) for k in keys1_el.findall("key") if _text(k)]
        right_keys = [_text(k) for k in keys2_el.findall("key") if _text(k)]
        if left_keys and right_keys:
            pair_count = max(len(left_keys), len(right_keys))
            for i in range(pair_count):
                left = left_keys[min(i, len(left_keys) - 1)]
                right = right_keys[min(i, len(right_keys) - 1)]
                keys.append(JoinKeyPair(left=left, right=right))
            return keys

    idx = 1
    while idx <= 20:
        left = _child_text(step_el, f"key_{idx}")
        right = _child_text(step_el, f"value_{idx}")
        if not left and not right:
            break
        if left and right:
            keys.append(JoinKeyPair(left=left, right=right))
        idx += 1

    if not keys:
        k1 = _child_text(step_el, "key_1")
        k2 = _child_text(step_el, "key_2")
        if k1 and k2 and not _child_text(step_el, "value_1"):
            keys.append(JoinKeyPair(left=k1, right=k2))

    if not keys:
        for key_block in step_el.findall("key"):
            name = _child_text(key_block, "name")
            field_el = key_block.find("field")
            field_val = _text(field_el) if field_el is not None else ""
            left = name or field_val
            right = field_val or name or left
            if left:
                keys.append(JoinKeyPair(left=left, right=right))

    if not keys:
        lookup_el = step_el.find("lookup")
        if lookup_el is not None:
            for key_el in lookup_el.findall("key"):
                left = _child_text(key_el, "name") or _child_text(key_el, "field")
                right = _child_text(key_el, "lookup") or _child_text(key_el, "name2")
                if left:
                    keys.append(JoinKeyPair(left=left, right=right or left))

    return keys


def format_spark_join_on(
    left_var: str, right_var: str, keys: list[JoinKeyPair]
) -> tuple[str, bool]:
    """Return (join_on_expression, use_on_keyword).

    When both streams use the same column names, returns (\"[\\\"col\\\"]\", True).
    Otherwise returns a boolean column expression for df.join(df, expr, how).
    """
    if not keys:
        return "", True
    if all(k.left == k.right for k in keys):
        names = ", ".join(f'"{k.left}"' for k in keys)
        return f"[{names}]", True
    cond = " & ".join(
        f'({left_var}["{k.left}"] == {right_var}["{k.right or k.left}"])' for k in keys
    )
    return cond, False


def parse_value_mappings(step_el: ET.Element) -> tuple[str, str, list[ValueMapping], str]:
    """Return (source_field, target_field, mappings, default_value)."""
    source = _child_text(step_el, "field_to_use") or _child_text(step_el, "from_field")
    target = _child_text(step_el, "target_field") or _child_text(step_el, "to_field") or source
    default = _child_text(step_el, "non_match_default") or _child_text(step_el, "default")
    mappings: list[ValueMapping] = []
    for vm_el in step_el.findall("valuemap") + step_el.findall("mapping"):
        src = _child_text(vm_el, "source_value") or _child_text(vm_el, "from")
        tgt = _child_text(vm_el, "target_value") or _child_text(vm_el, "to")
        if src is not None:
            mappings.append(ValueMapping(source=src, target=tgt))
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            src = _child_text(field_el, "source_value") or _child_text(field_el, "from")
            tgt = _child_text(field_el, "target_value") or _child_text(field_el, "to")
            if src:
                mappings.append(ValueMapping(source=src, target=tgt))
    return source, target, mappings, default


def parse_sequence_config(step_el: ET.Element) -> SequenceConfig:
    field_name = (
        _child_text(step_el, "valuename")
        or _child_text(step_el, "fieldname")
        or _child_text(step_el, "field_name")
        or "seq"
    )
    start_raw = (
        _child_text(step_el, "start")
        or _child_text(step_el, "start_at")
        or "1"
    )
    incr_raw = (
        _child_text(step_el, "increment")
        or _child_text(step_el, "increment_by")
        or "1"
    )
    try:
        start_at = int(start_raw or "1")
    except ValueError:
        start_at = 1
    try:
        increment_by = int(incr_raw or "1")
    except ValueError:
        increment_by = 1
    max_raw = _child_text(step_el, "max_value")
    max_value = int(max_raw) if max_raw else None
    return SequenceConfig(
        field_name=field_name,
        start_at=start_at,
        increment_by=increment_by,
        max_value=max_value,
    )


def parse_system_info_fields(step_el: ET.Element) -> list[tuple[str, str]]:
    """Return list of (output_field_name, system_type_code)."""
    results: list[tuple[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return results
    for field_el in fields_el.findall("field"):
        name = _child_text(field_el, "name")
        sys_type = _child_text(field_el, "type") or _child_text(field_el, "system_type")
        if name and sys_type:
            results.append((name, sys_type))
    return results


def parse_switch_case_rules(step_el: ET.Element) -> tuple[str, list[SwitchCaseRule], str]:
    """Return (switch_field, case_rules, default_target)."""
    switch_field = _child_text(step_el, "fieldname") or _child_text(step_el, "field_name")
    default_target = _child_text(step_el, "default_target_step")
    rules: list[SwitchCaseRule] = []
    cases_el = step_el.find("cases")
    if cases_el is not None:
        for case_el in cases_el.findall("case"):
            rules.append(
                SwitchCaseRule(
                    value=_child_text(case_el, "value"),
                    target_step=_child_text(case_el, "target_step"),
                )
            )
    return switch_field, rules, default_target


def parse_rank_config(step_el: ET.Element) -> RankConfig:
    return RankConfig(
        top_bottom=_child_text(step_el, "top_bottom", "top").lower(),
        rank=_child_text(step_el, "rank", "Y").upper() == "Y",
        sort_size=int(_child_text(step_el, "sort_size", "10") or "10"),
        field_name=_child_text(step_el, "field_name"),
        rank_field=_child_text(step_el, "rank_name", "rank") or "rank",
    )


def parse_normaliser_type_fields(step_el: ET.Element) -> list[tuple[str, list[str]]]:
    """Parse Row Normaliser: list of (type_field, [value_fields])."""
    results: list[tuple[str, list[str]]] = []
    for type_el in step_el.findall("type") + (step_el.find("types") or ET.Element("x")).findall("type"):
        type_field = _child_text(type_el, "type_field") or _child_text(type_el, "name")
        value_fields = [
            _child_text(vf, "name")
            for vf in type_el.findall("field")
            if _child_text(vf, "name")
        ]
        if type_field:
            results.append((type_field, value_fields))
    return results


def parse_denormaliser_group_fields(step_el: ET.Element) -> tuple[list[str], str, list[str]]:
    """Return (group_fields, target_field, target_fields)."""
    group_fields = [
        _child_text(g, "name")
        for g in (step_el.find("group") or ET.Element("x")).findall("field")
        if _child_text(g, "name")
    ]
    target_field = _child_text(step_el, "target_field") or _child_text(step_el, "type_field")
    target_fields = [
        _child_text(f, "name")
        for f in (step_el.find("fields") or ET.Element("x")).findall("field")
        if _child_text(f, "name")
    ]
    return group_fields, target_field, target_fields


def parse_javascript_script(step_el: ET.Element) -> str:
    for tag in ("jsScripts_script", "script", "javascript"):
        el = step_el.find(tag)
        if el is not None and (el.text or "").strip():
            return (el.text or "").strip()
    scripts_el = step_el.find("jsScripts")
    if scripts_el is not None:
        for script_el in scripts_el.findall("jsScript"):
            body = _child_text(script_el, "jsScript_script") or _text(script_el)
            if body:
                return body
    return _child_text(step_el, "script")


def parse_filter_condition_tree(node: ET.Element | None) -> dict[str, Any] | None:
    """Parse a Filter Rows condition node into a structured dict."""
    if node is None:
        return None
    result: dict[str, Any] = {}
    for tag in ("negated", "operator", "leftvalue", "rightvalue"):
        val = _child_text(node, tag)
        if val:
            result[tag] = val
    func = _child_text(node, "function")
    if func:
        result["function"] = unescape_xml(func)
    value_el = node.find("value")
    if value_el is not None:
        type_name, text_val = parse_value_constant(value_el)
        value_dict: dict[str, str] = {"type": type_name, "text": text_val}
        for extra in ("name", "isnull"):
            extra_val = _child_text(value_el, extra)
            if extra_val:
                value_dict[extra] = extra_val
        result["value"] = value_dict
    conditions_el = node.find("conditions")
    if conditions_el is not None:
        children = [
            parse_filter_condition_tree(child)
            for child in conditions_el.findall("condition")
        ]
        result["conditions"] = [c for c in children if c]
    return result


def parse_filter_rows_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse all Filter Rows XML properties."""
    root = parse_filter_compare_element(step_el)
    return {
        "compare_value": _child_text(step_el, "compare_value"),
        "send_true_to": _child_text(step_el, "send_true_to"),
        "send_false_to": _child_text(step_el, "send_false_to"),
        "condition": parse_filter_condition_tree(root),
    }


def parse_row_generator_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Row Generator / Data Grid step metadata."""
    columns, rows = parse_data_grid_rows(step_el)
    try:
        limit = int(_child_text(step_el, "limit", "1") or "1")
    except ValueError:
        limit = 1
    return {
        "limit": limit,
        "fields": _metadata_value(parse_row_generator_fields(step_el)),
        "columns": columns,
        "rows": rows,
        "never_ending": _bool_from_yn(_child_text(step_el, "never_ending")),
        "interval_in_ms": _child_text(step_el, "interval_in_ms"),
        "row_time_field": _child_text(step_el, "row_time_field"),
        "last_time_field": _child_text(step_el, "last_time_field"),
    }


def parse_merge_join_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Merge Join step metadata including stream names and key lists."""
    keys1_el = step_el.find("keys_1")
    keys2_el = step_el.find("keys_2")
    keys_1 = [_text(k) for k in (keys1_el.findall("key") if keys1_el is not None else []) if _text(k)]
    keys_2 = [_text(k) for k in (keys2_el.findall("key") if keys2_el is not None else []) if _text(k)]
    cfg = MergeJoinConfig(
        join_type=_child_text(step_el, "join_type", "INNER"),
        step1=_child_text(step_el, "step1"),
        step2=_child_text(step_el, "step2"),
        keys=parse_join_keys(step_el),
        keys_1=keys_1,
        keys_2=keys_2,
    )
    return _metadata_value(cfg)


def parse_formula_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Formula step metadata including nested formula_string entries."""
    formulas: list[dict[str, str]] = []
    for formula_el in step_el.findall("formula"):
        field_name = _child_text(formula_el, "field_name")
        formula_string = (
            _child_text(formula_el, "formula_string")
            or _child_text(formula_el, "formula")
        )
        if field_name or formula_string:
            formulas.append({
                "field_name": field_name or "formula_result",
                "formula": unescape_xml(formula_string),
                "value_type": _child_text(formula_el, "value_type"),
            })

    if not formulas:
        flat_formula = unescape_xml(_child_text(step_el, "formula"))
        flat_field = _child_text(step_el, "field_name")
        if flat_formula:
            formulas.append({
                "field_name": flat_field or "formula_result",
                "formula": flat_formula,
                "value_type": _child_text(step_el, "value_type"),
            })

    primary = formulas[0] if formulas else {}
    return {
        "formulas": formulas,
        "field_name": primary.get("field_name", ""),
        "formula": primary.get("formula", ""),
        "value_type": primary.get("value_type", ""),
    }


def parse_ifnull_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse IfNull / If Field Value Is Null replacement field metadata."""
    replacements: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            name = _child_text(field_el, "name")
            if not name:
                continue
            value = _child_text(field_el, "value") or _child_text(field_el, "replacement")
            replacements.append({"name": name, "value": value})

    return {
        "replacements": replacements,
        "replace_all": _child_text(step_el, "replaceAllByValue"),
        "select_fields": _bool_from_yn(_child_text(step_el, "selectFields")),
    }


def parse_group_by_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Group By step metadata."""
    group_keys, aggregates = parse_group_by_fields(step_el)
    all_fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            all_fields.append({
                "name": _child_text(field_el, "name"),
                "type": _child_text(field_el, "type"),
                "aggregate": _child_text(field_el, "aggregate"),
                "subject": _child_text(field_el, "subject"),
                "valuefield": _child_text(field_el, "valuefield"),
            })
    return {
        "group_keys": group_keys,
        "aggregates": _metadata_value(aggregates),
        "fields": all_fields,
        "give_back_row": _bool_from_yn(_child_text(step_el, "give_back_row")),
        "all_rows": _bool_from_yn(_child_text(step_el, "all_rows")),
    }


def parse_value_mapper_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Value Mapper step metadata."""
    source, target, mappings, default = parse_value_mappings(step_el)
    return {
        "field_to_use": source,
        "target_field": target,
        "non_match_default": default,
        "mappings": _metadata_value(mappings),
        "case_sensitive": _bool_from_yn(_child_text(step_el, "case_sensitive")),
        "non_empty": _bool_from_yn(_child_text(step_el, "non_empty")),
    }


def parse_sequence_config_dict(step_el: ET.Element) -> dict[str, Any]:
    """Parse Sequence step metadata as a dict."""
    return _metadata_value(parse_sequence_config(step_el))


def parse_text_file_output_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Text File Output nested file and format properties."""
    file_block = parse_file_block(step_el)
    filename = (
        str(file_block.get("name", ""))
        or extract_step_property(step_el, "filename")
        or _child_text(step_el, "filename")
    )
    extension = (
        str(file_block.get("extention", ""))
        or str(file_block.get("extension", ""))
        or _child_text(step_el, "extension")
    )
    output_fields: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is not None:
        for field_el in fields_el.findall("field"):
            output_fields.append({
                "name": _child_text(field_el, "name"),
                "type": _child_text(field_el, "type"),
                "format": _child_text(field_el, "format"),
                "length": _child_text(field_el, "length"),
                "precision": _child_text(field_el, "precision"),
            })
    cfg = TextFileOutputConfig(
        filename=filename,
        extension=extension,
        separator=_child_text(step_el, "separator", ","),
        header=_bool_from_yn(_child_text(step_el, "header", "Y"), default=True),
        footer=_bool_from_yn(_child_text(step_el, "footer")),
        encoding=_child_text(step_el, "encoding", "utf-8"),
        compression=_child_text(step_el, "compression", "none"),
        enclosure=_child_text(step_el, "enclosure") or _child_text(step_el, "quote"),
        append=_bool_from_yn(file_block.get("append", _child_text(step_el, "file_appended"))),
        create_parent_folder=_bool_from_yn(
            file_block.get("create_parent_folder", _child_text(step_el, "create_parent_folder"))
        ),
        split=_bool_from_yn(file_block.get("split", "")),
        fast_dump=_bool_from_yn(_child_text(step_el, "fast_dump")),
        padded=_bool_from_yn(_child_text(step_el, "padded")),
        file=file_block,
        output_fields=output_fields,
    )
    return _metadata_value(cfg)


def parse_table_input_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Table Input SQL, connection, and parameter metadata."""
    parameters: list[dict[str, str]] = []
    params_el = step_el.find("parameters")
    if params_el is not None:
        for param_el in params_el.findall("parameter"):
            parameters.append({
                "name": _child_text(param_el, "name"),
                "default": _child_text(param_el, "default"),
                "description": _child_text(param_el, "description"),
            })
    try:
        limit = int(_child_text(step_el, "limit", "0") or "0")
    except ValueError:
        limit = 0
    cfg = TableInputConfig(
        connection=_child_text(step_el, "connection"),
        sql=_child_text(step_el, "sql"),
        schema=_child_text(step_el, "schema"),
        table=_child_text(step_el, "table"),
        limit=limit,
        execute_each_row=_bool_from_yn(_child_text(step_el, "execute_each_row")),
        variables_active=_bool_from_yn(
            _child_text(step_el, "variables_active")
            or _child_text(step_el, "variableReplacementActive")
        ),
        lazy_conversion=_bool_from_yn(_child_text(step_el, "lazy_conversion_active")),
        parameters=parameters,
    )
    return _metadata_value(cfg)


def parse_database_lookup_config(step_el: ET.Element) -> dict[str, Any]:
    """Parse Database Lookup keys, return fields, and connection metadata."""
    keys: list[DatabaseLookupKey] = []
    lookup_el = step_el.find("lookup")
    if lookup_el is not None:
        for key_el in lookup_el.findall("key"):
            stream = _child_text(key_el, "name") or _child_text(key_el, "field")
            table_field = (
                _child_text(key_el, "field")
                or _child_text(key_el, "lookup")
                or _child_text(key_el, "name2")
            )
            if stream or table_field:
                keys.append(
                    DatabaseLookupKey(
                        stream_field=stream,
                        table_field=table_field or stream,
                        name2=_child_text(key_el, "name2"),
                    )
                )
    if not keys:
        keys = [
            DatabaseLookupKey(stream_field=pair.left, table_field=pair.right)
            for pair in parse_join_keys(step_el)
        ]

    return_fields: list[DatabaseLookupReturnField] = []
    for value_el in step_el.findall("value"):
        name = _child_text(value_el, "name")
        if not name:
            continue
        return_fields.append(
            DatabaseLookupReturnField(
                name=name,
                rename=_child_text(value_el, "rename"),
                default=_child_text(value_el, "default"),
                type_name=_child_text(value_el, "type"),
            )
        )

    try:
        cache_size = int(_child_text(step_el, "cache_size", "0") or "0")
    except ValueError:
        cache_size = 0

    cfg = DatabaseLookupConfig(
        connection=_child_text(step_el, "connection"),
        schema=_child_text(step_el, "schema"),
        table=_child_text(step_el, "table"),
        cached=_bool_from_yn(_child_text(step_el, "cached")),
        cache_size=cache_size,
        orderby=_child_text(step_el, "orderby"),
        fail_on_multiple=_bool_from_yn(_child_text(step_el, "fail_on_multiple")),
        eat_row_on_failure=_bool_from_yn(_child_text(step_el, "eat_row_on_failure")),
        keys=keys,
        return_fields=return_fields,
    )
    return _metadata_value(cfg)


def parse_step_metadata(step_el: ET.Element, step_type: str) -> dict[str, Any]:
    """Parse all nested XML for a step type into structured metadata."""
    st = (step_type or "").strip().lower().replace(" ", "")
    parsers: dict[str, Any] = {
        "calculator": lambda el: {"calculations": _metadata_value(parse_calculations(el))},
        "mergejoin": parse_merge_join_config,
        "groupby": parse_group_by_config,
        "valuemapper": parse_value_mapper_config,
        "filterrows": parse_filter_rows_config,
        "rowgenerator": parse_row_generator_config,
        "datagrid": parse_row_generator_config,
        "sequence": parse_sequence_config_dict,
        "textfileoutput": parse_text_file_output_config,
        "tableinput": parse_table_input_config,
        "databaselookup": parse_database_lookup_config,
        "streamlookup": parse_database_lookup_config,
        "formula": parse_formula_config,
        "ifnull": parse_ifnull_config,
        "iffieldvaluenull": parse_ifnull_config,
    }
    parser = parsers.get(st)
    if parser is None:
        return {}
    return parser(step_el)


def is_structured_step_type(step_type: str) -> bool:
    """Return True when the step type has a dedicated nested XML parser."""
    return (step_type or "").strip().lower().replace(" ", "") in _STRUCTURED_STEP_TYPES


def aggregate_to_spark(agg: str, col_name: str) -> str:
    """Map Pentaho aggregate name to PySpark agg expression."""
    a = (agg or "SUM").upper()
    c = f'col("{col_name}")'
    mapping = {
        "SUM": f"sum({c})",
        "AVERAGE": f"avg({c})",
        "AVG": f"avg({c})",
        "MIN": f"min({c})",
        "MAX": f"max({c})",
        "COUNT": f"count({c})",
        "COUNT_ALL": "count(lit(1))",
        "COUNT ANY": "count(lit(1))",
        "COUNT_ANY": "count(lit(1))",
        "COUNT DISTINCT": f"countDistinct({c})",
        "COUNT_DISTINCT": f"countDistinct({c})",
        "FIRST": f"first({c})",
        "LAST": f"last({c})",
        "FIRST_INCL_NULL": f"first({c}, ignorenulls=True)",
        "LAST_INCL_NULL": f"last({c}, ignorenulls=True)",
        "MEDIAN": f"expr('percentile_approx(`{col_name}`, 0.5)')",
        "PERCENTILE": f"expr('percentile_approx(`{col_name}`, 0.5)')",
    }
    return mapping.get(a, f"sum({c})")


def system_info_expr(sys_type: str) -> str:
    """Map Pentaho SystemInfo type codes to PySpark expressions."""
    t = (sys_type or "").lower().replace(" ", "_")
    mapping = {
        "system_date_(fixed)": "current_date()",
        "system_date_(variable)": "current_date()",
        "system_datetime_(fixed)": "current_timestamp()",
        "system_datetime_(variable)": "current_timestamp()",
        "system_date": "current_date()",
        "system_datetime": "current_timestamp()",
        "job_name": "lit(spark.sparkContext.appName)",
        "transformation_name": "lit('transformation')",
        "step_name": "lit('step')",
        "hostname": "lit(spark.sparkContext.getConf().get('spark.driver.host', 'localhost'))",
        "hostname_real": "lit(spark.sparkContext.getConf().get('spark.driver.host', 'localhost'))",
        "ip_address": "lit('0.0.0.0')",
        "job_run_id": "expr('uuid()')",
        "batch_id": "expr('uuid()')",
        "parent_job_id": "lit('')",
        "parent_transformation_id": "lit('')",
        "system_info_user": "lit(spark.sparkContext.sparkUser())",
        "user_name": "lit(spark.sparkContext.sparkUser())",
        "username": "lit(spark.sparkContext.sparkUser())",
        "kettle_version": "lit('pyspark')",
        "kettle_build_version": "lit('pyspark')",
        "kettle_build_date": "current_date()",
        "internal_job_run_id": "expr('uuid()')",
        "internal_transformation_run_id": "expr('uuid()')",
        "current_pid": "lit(0)",
        "jvm_max_memory": "lit(0)",
        "jvm_total_memory": "lit(0)",
        "jvm_free_memory": "lit(0)",
    }
    for key, expr in mapping.items():
        if key in t or t in key:
            return expr
    if "date" in t and "time" not in t:
        return "current_date()"
    if "time" in t or "datetime" in t:
        return "current_timestamp()"
    if "uuid" in t or "id" in t:
        return "expr('uuid()')"
    return "lit('')"

