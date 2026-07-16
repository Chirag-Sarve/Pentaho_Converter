"""Parse Pentaho .kjb job XML files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from .models import PentahoHop, PentahoJob, PentahoJobEntry

logger = logging.getLogger(__name__)


def _text(elem: ET.Element | None, default: str = "") -> str:
    if elem is None or elem.text is None:
        return default
    return elem.text.strip()


def _child_text(parent: ET.Element, tag: str, default: str = "") -> str:
    return _text(parent.find(tag), default)


def _yn(val: str) -> bool | None:
    """Parse Pentaho Y/N flag; empty → None (attribute omitted)."""
    if val == "":
        return None
    return val.upper() == "Y"


def _parse_entry_attributes(entry_el: ET.Element) -> dict[str, Any]:
    """Collect scalar and nested attributes from a job ``<entry>`` element."""
    attrs: dict[str, Any] = {}
    skip = {"name", "type", "start", "attributes", "attributes_kjc"}
    for child in entry_el:
        if child.tag in skip:
            continue
        if child.tag == "fields":
            fields = []
            for field_el in child.findall("field"):
                fields.append(
                    {
                        "variable_name": _child_text(field_el, "variable_name"),
                        "variable_type": _child_text(field_el, "variable_type"),
                        "variable_string": _child_text(field_el, "variable_string"),
                    }
                )
            attrs["fields"] = fields
        elif child.tag == "parameters":
            # JOB/TRANS pass-through parameters block
            attrs["pass_all_parameters"] = _child_text(child, "pass_all_parameters", "N")
            param_list = []
            for p in child.findall("parameter"):
                param_list.append(
                    {
                        "name": _child_text(p, "name"),
                        "value": _child_text(p, "value") or _child_text(p, "default_value"),
                    }
                )
            if param_list:
                attrs["parameters"] = param_list
        elif len(list(child)) == 0:
            attrs[child.tag] = _text(child)
        else:
            # Opaque nested XML — keep serialized children for TODO fidelity
            nested: dict[str, Any] = {}
            for sub in child:
                if len(list(sub)) == 0:
                    nested[sub.tag] = _text(sub)
            if nested:
                attrs[child.tag] = nested
    return attrs


def parse_job(path: Path, logs: list[str] | None = None) -> PentahoJob:
    """Parse a .kjb file into a :class:`PentahoJob`."""
    log = logs if logs is not None else []
    log.append(f"Parsing job: {path.name}")

    try:
        tree = ET.parse(path)
    except ET.ParseError as exc:
        raise ValueError(f"XML parsing error in {path.name}: {exc}") from exc

    root = tree.getroot()
    name = _child_text(root, "name") or path.stem

    job = PentahoJob(name=name, file_path=path)

    params_el = root.find("parameters")
    if params_el is not None:
        for p in params_el.findall("parameter"):
            pname = _child_text(p, "name")
            if pname:
                job.parameters[pname] = _child_text(p, "default_value")

    entries_el = root.find("entries")
    if entries_el is not None:
        for entry_el in entries_el.findall("entry"):
            entry_name = _child_text(entry_el, "name")
            entry_type = _child_text(entry_el, "type")
            is_start = _child_text(entry_el, "start").upper() == "Y"
            attrs = _parse_entry_attributes(entry_el)

            job.entries.append(
                PentahoJobEntry(
                    name=entry_name,
                    entry_type=entry_type,
                    filename=_child_text(entry_el, "filename"),
                    transname=_child_text(entry_el, "transname"),
                    jobname=_child_text(entry_el, "jobname"),
                    attributes=attrs,
                    is_start=is_start,
                )
            )

    hops_el = root.find("hops")
    if hops_el is not None:
        for hop_el in hops_el.findall("hop"):
            job.hops.append(
                PentahoHop(
                    from_name=_child_text(hop_el, "from"),
                    to_name=_child_text(hop_el, "to"),
                    enabled=_child_text(hop_el, "enabled", "Y").upper() != "N",
                    evaluation=_child_text(hop_el, "evaluation") or None,
                    unconditional=_yn(_child_text(hop_el, "unconditional")),
                )
            )

    log.append(f"Job '{name}': {len(job.entries)} entries, {len(job.hops)} hops.")
    return job
