"""Convert Pentaho Value Mapper step metadata to PySpark when/otherwise expressions."""

from __future__ import annotations

from typing import Any
from xml.etree import ElementTree as ET

from .step_xml import _child_text


def _col(field_name: str) -> str:
    if not field_name:
        return "lit(None)"
    return f'col("{field_name}")'


def _normalize_mapping_item(item: dict[str, Any]) -> dict[str, str]:
    source = item.get("source")
    if source is None:
        source = item.get("source_value")
    if source is None:
        source = item.get("from")
    target = item.get("target")
    if target is None:
        target = item.get("target_value")
    if target is None:
        target = item.get("to")
    return {
        "source": "" if source is None else str(source),
        "target": "" if target is None else str(target),
    }


def mappings_from_step_element(step_el: ET.Element | None) -> list[dict[str, str]]:
    """Read Value Mapper field entries that use alternate XML tag names."""
    if step_el is None:
        return []
    mappings: list[dict[str, str]] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return mappings
    for field_el in fields_el.findall("field"):
        src = (
            _child_text(field_el, "source_value")
            or _child_text(field_el, "source")
            or _child_text(field_el, "from")
        )
        tgt = (
            _child_text(field_el, "target_value")
            or _child_text(field_el, "target")
            or _child_text(field_el, "to")
        )
        if src:
            mappings.append({"source": src, "target": tgt})
    return mappings


def _mappings_from_metadata(metadata: dict[str, Any]) -> list[dict[str, str]]:
    """Preserve every mapping entry from propagated metadata in declaration order."""
    raw = metadata.get("mappings")
    if raw is None:
        raw = metadata.get("value_mappings") or []
    normalized: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        normalized.append(_normalize_mapping_item(item))
    return normalized


def _case_sensitive_from_metadata(metadata: dict[str, Any]) -> bool:
    """Pentaho defaults to case-sensitive; only case_insensitive is explicit in XML."""
    attrs = metadata.get("attributes") or {}
    if "case_sensitive" not in attrs:
        return True
    raw = attrs["case_sensitive"]
    if raw is None or str(raw).strip() == "":
        return True
    return str(raw).strip().upper() in ("Y", "YES", "TRUE", "1")


def _default_is_active(default_value: str) -> bool:
    """Pentaho only applies non_match_default when the value is non-empty."""
    return default_value != ""


def _is_empty_source(source: str) -> bool:
    return source == ""


def _is_empty_value_expr(col_expr: str) -> str:
    # Pentaho Utils.isEmpty: null or zero-length string (whitespace is not empty).
    return f"({col_expr}.isNull() | ({col_expr} == lit('')))"


def _target_literal(value: str, value_type: str = "") -> str:
    if value is None or value == "":
        return "lit(None)"

    t = (value_type or "String").strip().lower()
    if t in ("integer", "int"):
        try:
            return f"lit({int(value)})"
        except ValueError:
            return f"lit({value!r})"
    if t == "long":
        try:
            return f"lit({int(value)})"
        except ValueError:
            return f"lit({value!r})"
    if t in ("number", "bignumber", "float", "double"):
        try:
            return f"lit({float(value)})"
        except ValueError:
            return f"lit({value!r})"
    if t in ("decimal", "bigdecimal"):
        return f'lit("{value}").cast("decimal")'
    if t == "boolean":
        return (
            "lit(True)"
            if value.strip().upper() in ("Y", "YES", "TRUE", "1", "T")
            else "lit(False)"
        )
    if t == "date":
        return f"to_date(lit({value!r}))"
    if t in ("timestamp", "datetime"):
        return f"to_timestamp(lit({value!r}))"
    return f"lit({value!r})"


def _match_literal(value: str) -> str:
    return f"lit({value!r})"


def _source_matches(col_expr: str, source_value: str, case_sensitive: bool) -> str:
    match_lit = _match_literal(source_value)
    if case_sensitive:
        return f"({col_expr} == {match_lit})"
    return f"(lower({col_expr}) == lower({match_lit}))"


def _build_mapping_expression(
    source_field: str,
    mappings: list[dict[str, str]],
    default_value: str,
    *,
    create_new_field: bool,
    case_sensitive: bool,
    target_type: str = "",
    default_active: bool = False,
) -> str | None:
    source_expr = _col(source_field)
    empty_expr = _is_empty_value_expr(source_expr)
    empty_target: str | None = None
    regular_mappings: list[tuple[str, str]] = []

    for mapping in mappings:
        src = mapping["source"]
        tgt = mapping["target"]
        if _is_empty_source(src):
            # Pentaho allows one null/empty-source mapping; keep the first received.
            if empty_target is None:
                empty_target = tgt
            continue
        regular_mappings.append((src, tgt))

    when_clauses: list[tuple[str, str]] = []

    if empty_target is not None:
        when_clauses.append((empty_expr, _target_literal(empty_target, target_type)))

    for src, tgt in regular_mappings:
        when_clauses.append(
            (_source_matches(source_expr, src, case_sensitive), _target_literal(tgt, target_type))
        )

    if not when_clauses and not default_active:
        return None

    if when_clauses:
        expr = f"when({when_clauses[0][0]}, {when_clauses[0][1]})"
        for condition, mapped in when_clauses[1:]:
            expr = f"{expr}.when({condition}, {mapped})"

        if empty_target is None:
            # Null/empty values pass through unless explicitly mapped (Pentaho semantics).
            expr = f"{expr}.when({empty_expr}, {source_expr})"

        if default_active:
            expr = f"{expr}.otherwise({_target_literal(default_value, target_type)})"
        elif create_new_field:
            expr = f"{expr}.otherwise(lit(None))"
        else:
            expr = f"{expr}.otherwise({source_expr})"
        return expr

    if default_active:
        return f"when({empty_expr}, {source_expr}).otherwise({_target_literal(default_value, target_type)})"

    return None


def _unresolved_lines(step_name: str, out_var: str, message: str) -> list[str]:
    return [
        f"# WARNING: ValueMapper '{step_name}': {message}",
        f"{out_var} = spark.createDataFrame([], '_value_mapper_unresolved STRING')",
    ]


def convert_value_mapper_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Generate PySpark lines for a Value Mapper step from propagated metadata."""
    lines = [f"# Value Mapper: {step_name}"]

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "converted"

    source_field = (metadata.get("field_to_use") or metadata.get("from_field") or "").strip()
    target_field = (metadata.get("target_field") or metadata.get("to_field") or "").strip()
    default_value = metadata.get("non_match_default")
    if default_value is None:
        default_value = metadata.get("default", "")
    default_value = "" if default_value is None else str(default_value)

    mappings = _mappings_from_metadata(metadata)
    case_sensitive = _case_sensitive_from_metadata(metadata)
    default_active = _default_is_active(default_value)

    if not source_field:
        lines.extend(_unresolved_lines(step_name, out_var, "missing field_to_use in metadata"))
        return lines, "partial"

    if not mappings and not default_active:
        lines.extend(_unresolved_lines(step_name, out_var, "no mappings or default defined in metadata"))
        return lines, "partial"

    output_col = target_field or source_field
    create_new_field = bool(target_field) and target_field != source_field
    target_type = "String" if create_new_field else ""

    mapping_expr = _build_mapping_expression(
        source_field,
        mappings,
        default_value,
        create_new_field=create_new_field,
        case_sensitive=case_sensitive,
        target_type=target_type,
        default_active=default_active,
    )

    if not mapping_expr:
        lines.extend(_unresolved_lines(step_name, out_var, "could not build mapping expression from metadata"))
        return lines, "partial"

    lines.append(f'{out_var} = {in_df}.withColumn("{output_col}", {mapping_expr})')
    return lines, "converted"
