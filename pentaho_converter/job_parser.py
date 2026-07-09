"""Parse Pentaho .kjb job XML files."""

from __future__ import annotations

import logging
from pathlib import Path
from xml.etree import ElementTree as ET

from .models import PentahoHop, PentahoJob, PentahoJobEntry

logger = logging.getLogger(__name__)


def _text(elem: ET.Element | None, default: str = "") -> str:
    if elem is None or elem.text is None:
        return default
    return elem.text.strip()


def _child_text(parent: ET.Element, tag: str, default: str = "") -> str:
    return _text(parent.find(tag), default)


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

    entries_el = root.find("entries")
    if entries_el is not None:
        for entry_el in entries_el.findall("entry"):
            entry_name = _child_text(entry_el, "name")
            entry_type = _child_text(entry_el, "type")
            is_start = _child_text(entry_el, "start").upper() == "Y"
            attrs: dict[str, str] = {}
            for child in entry_el:
                if child.tag in ("name", "type", "start"):
                    continue
                attrs[child.tag] = _text(child)

            job.entries.append(
                PentahoJobEntry(
                    name=entry_name,
                    entry_type=entry_type,
                    filename=_child_text(entry_el, "filename"),
                    transname=_child_text(entry_el, "transname"),
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
                )
            )

    log.append(f"Job '{name}': {len(job.entries)} entries, {len(job.hops)} hops.")
    return job
