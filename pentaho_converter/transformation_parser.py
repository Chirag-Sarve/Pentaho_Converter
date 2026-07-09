"""Parse Pentaho .ktr transformation XML files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from .models import PentahoField, PentahoHop, PentahoStep, PentahoTransformation
from .step_xml import (
    is_structured_step_type,
    parse_file_block,
    parse_step_metadata,
)

logger = logging.getLogger(__name__)

# Nested tags parsed into structured metadata — not stored as opaque XML strings.
_STRUCTURED_NESTED_TAGS = frozenset({
    "calculation",
    "compare",
    "keys_1",
    "keys_2",
    "lookup",
    "value",
    "parameters",
    "data",
    "cases",
    "jsScripts",
})


def _text(elem: ET.Element | None, default: str = "") -> str:
    if elem is None or elem.text is None:
        return default
    return elem.text.strip()


def _child_text(parent: ET.Element, tag: str, default: str = "") -> str:
    return _text(parent.find(tag), default)


def _parse_fields(step_el: ET.Element) -> list[PentahoField]:
    fields: list[PentahoField] = []
    fields_el = step_el.find("fields")
    if fields_el is None:
        return fields
    for field_el in fields_el:
        tag = field_el.tag.lower()
        if tag in ("field", "r", "c"):
            fields.append(
                PentahoField(
                    name=_child_text(field_el, "name"),
                    type_name=_child_text(field_el, "type", "String"),
                    length=_child_text(field_el, "length"),
                    precision=_child_text(field_el, "precision"),
                    format=_child_text(field_el, "format"),
                    currency=_child_text(field_el, "currency"),
                    decimal=_child_text(field_el, "decimal"),
                    group=_child_text(field_el, "group"),
                    null_if=_child_text(field_el, "nullif"),
                    default=_child_text(field_el, "default"),
                    rename=_child_text(field_el, "rename"),
                )
            )
    return fields


def _collect_scalar_attributes(step_el: ET.Element, step_type: str) -> dict[str, str]:
    """Collect scalar step properties; avoid opaque XML for structured nested tags."""
    attrs: dict[str, str] = {}
    structured = is_structured_step_type(step_type)

    for child in step_el:
        tag = child.tag
        if tag in ("name", "type", "GUI"):
            continue
        if tag == "fields":
            continue
        if tag == "file":
            file_block = parse_file_block(step_el)
            if file_block.get("name"):
                attrs["filename"] = str(file_block["name"])
                attrs["file"] = str(file_block["name"])
            for key, val in file_block.items():
                if key != "name" and isinstance(val, str):
                    attrs[f"file_{key}"] = val
            continue
        if structured and tag in _STRUCTURED_NESTED_TAGS:
            continue
        if len(child) == 0:
            attrs[tag] = _text(child)
        elif structured:
            # Structured steps keep nested config in parsed_config only.
            continue
        else:
            attrs[tag] = ET.tostring(child, encoding="unicode")

    return attrs


def _parse_step(step_el: ET.Element) -> PentahoStep:
    step_type = _child_text(step_el, "type")
    parsed_config: dict[str, Any] = parse_step_metadata(step_el, step_type)
    attrs = _collect_scalar_attributes(step_el, step_type)

    return PentahoStep(
        name=_child_text(step_el, "name"),
        step_type=step_type,
        attributes=attrs,
        fields=_parse_fields(step_el),
        raw_element=step_el,
        parsed_config=parsed_config,
    )


def parse_transformation(path: Path, logs: list[str] | None = None) -> PentahoTransformation:
    """Parse a .ktr file into a :class:`PentahoTransformation`."""
    log = logs if logs is not None else []
    log.append(f"Parsing transformation: {path.name}")

    try:
        tree = ET.parse(path)
    except ET.ParseError as exc:
        raise ValueError(f"XML parsing error in {path.name}: {exc}") from exc

    root = tree.getroot()
    info = root.find("info")
    name = _child_text(info, "name") if info is not None else path.stem

    trans = PentahoTransformation(name=name, file_path=path)

    order_el = root.find("order")
    if order_el is not None:
        for hop_el in order_el.findall("hop"):
            trans.hops.append(
                PentahoHop(
                    from_name=_child_text(hop_el, "from"),
                    to_name=_child_text(hop_el, "to"),
                    enabled=_child_text(hop_el, "enabled", "Y").upper() != "N",
                )
            )

    for step_el in root.findall("step"):
        step = _parse_step(step_el)
        if step.name:
            trans.steps.append(step)

    if info is not None:
        params_el = info.find("parameters")
        if params_el is not None:
            for param in params_el.findall("parameter"):
                key = _child_text(param, "name")
                val = _child_text(param, "default_value")
                if key:
                    trans.parameters[key] = val

    log.append(
        f"Transformation '{name}': {len(trans.steps)} steps, {len(trans.hops)} hops."
    )
    return trans


def shallow_parse_step_attributes(step_el: ET.Element) -> dict[str, str]:
    """Legacy shallow parse (pre-structured-metadata) for before/after comparison."""
    attrs: dict[str, str] = {}
    for child in step_el:
        if child.tag in ("name", "type", "fields", "GUI"):
            continue
        if child.tag == "file":
            path = _child_text(child, "name")
            if path:
                attrs["filename"] = path
                attrs["file"] = path
            continue
        if len(child) == 0:
            attrs[child.tag] = _text(child)
        else:
            attrs[child.tag] = ET.tostring(child, encoding="unicode")
    return attrs
