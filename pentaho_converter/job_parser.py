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
            # Generic field rows: Set Variables, Copy/Move Files, Delete Files,
            # Add Result Filenames, DosToUnix, etc. Preserve every child tag.
            fields = []
            for field_el in child.findall("field"):
                field_data: dict[str, Any] = {}
                for sub in field_el:
                    if len(list(sub)) == 0:
                        field_data[sub.tag] = _text(sub)
                # Set Variables aliases (PDI: variable_value; older: variable_string)
                if any(
                    k in field_data
                    for k in ("variable_name", "variable_value", "variable_string")
                ):
                    raw_value = field_data.get("variable_value") or field_data.get(
                        "variable_string", ""
                    )
                    field_data.setdefault("variable_string", raw_value)
                    field_data.setdefault("variable_value", raw_value)
                    field_data.setdefault(
                        "variable_type", field_data.get("variable_type") or "JVM"
                    )
                    field_data.setdefault(
                        "variable_name", field_data.get("variable_name", "")
                    )
                fields.append(field_data)
            attrs["fields"] = fields
        elif child.tag == "parameters":
            # JOB/TRANS pass-through parameters, or XSLT <parameter><name/><field/>
            attrs["pass_all_parameters"] = _child_text(child, "pass_all_parameters", "N")
            param_list = []
            for p in child.findall("parameter"):
                param_list.append(
                    {
                        "name": _child_text(p, "name"),
                        "value": _child_text(p, "value")
                        or _child_text(p, "default_value")
                        or _child_text(p, "field"),
                        "field": _child_text(p, "field"),
                    }
                )
            if param_list:
                attrs["parameters"] = param_list
        elif child.tag == "outputproperties":
            # XSLT job entry — JAXP output properties
            props = []
            for op in child.findall("outputproperty"):
                props.append(
                    {
                        "name": _child_text(op, "name"),
                        "value": _child_text(op, "value"),
                    }
                )
            attrs["outputproperties"] = props
        elif child.tag == "filetypes":
            # MAIL job entry — result-file type filter list
            attrs["filetypes"] = [
                _text(ft) for ft in child.findall("filetype") if _text(ft)
            ]
        elif child.tag == "embeddedimages":
            # MAIL job entry — HTML embedded images
            images = []
            for img in child.findall("embeddedimage"):
                images.append(
                    {
                        "image_name": _child_text(img, "image_name"),
                        "content_id": _child_text(img, "content_id"),
                    }
                )
            attrs["embeddedimages"] = images
        elif child.tag == "headers":
            # HTTP job entry — request headers
            header_list = []
            for hdr in child.findall("header"):
                header_list.append(
                    {
                        "header_name": _child_text(hdr, "header_name"),
                        "header_value": _child_text(hdr, "header_value"),
                    }
                )
            attrs["headers"] = header_list
        elif child.tag == "connections":
            # CHECK_DB_CONNECTIONS — list of connection name + wait settings
            conn_list = []
            for conn_el in child.findall("connection"):
                conn_list.append(
                    {
                        "name": _child_text(conn_el, "name"),
                        "waitfor": _child_text(conn_el, "waitfor"),
                        "waittime": _child_text(conn_el, "waittime"),
                    }
                )
            attrs["connections"] = conn_list
        elif len(list(child)) == 0:
            # Normalize Set Variables replace flag (PDI: replacevars)
            if child.tag == "replacevars":
                attrs["replacevars"] = _text(child)
                attrs.setdefault("replace", _text(child))
            elif child.tag in {"argument", "arguments"}:
                # SHELL may repeat <argument> siblings
                args = attrs.setdefault("arguments", [])
                if not isinstance(args, list):
                    args = [str(args)]
                    attrs["arguments"] = args
                text = _text(child)
                if text:
                    args.append(text)
            else:
                # Repeated scalar tags → list (keep last-write compat via *_list)
                text = _text(child)
                if child.tag in attrs and not isinstance(attrs[child.tag], list):
                    # Preserve first value under the plain key; also build a list
                    attrs[f"{child.tag}_list"] = [attrs[child.tag], text]
                elif f"{child.tag}_list" in attrs and isinstance(
                    attrs[f"{child.tag}_list"], list
                ):
                    attrs[f"{child.tag}_list"].append(text)
                attrs[child.tag] = text
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

    # Job-level DatabaseMeta connections (shared by TABLE_EXISTS / SQL / …)
    for conn_el in root.findall("connection"):
        cname = _child_text(conn_el, "name")
        if not cname:
            continue
        cattrs: dict[str, Any] = {}
        for child in conn_el:
            if len(list(child)) == 0:
                cattrs[child.tag] = _text(child)
        job.connections[cname] = cattrs

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
