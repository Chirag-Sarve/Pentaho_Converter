"""Generate Master_ETL graph modules from Master_ETL.kjb."""
from __future__ import annotations

from pathlib import Path
from pprint import pformat
from xml.etree import ElementTree as ET

KJB = Path(
    r"C:\Users\Prateek.Kotian\Desktop\Pentaho\Retail & E-commerce"
    r"\Retail_ETL_Project\jobs\master\Master_ETL.kjb"
)
OUT_DIR = Path(__file__).resolve().parents[1] / (
    "databricks_project/src/pentaho_migration/jobs"
)
CHILDREN_DIR = OUT_DIR / "children"


def _t(el: ET.Element | None, tag: str, default: str = "") -> str:
    if el is None:
        return default
    child = el.find(tag)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def _yn(val: str) -> bool | None:
    if val == "":
        return None
    return val.upper() == "Y"


def main() -> None:
    root = ET.parse(KJB).getroot()

    entries: list[dict] = []
    child_jobs: list[tuple[str, str]] = []
    for e in root.findall("./entries/entry"):
        name = _t(e, "name")
        etype = _t(e, "type")
        attrs: dict = {}
        for child in e:
            if child.tag in {"name", "type", "start", "attributes", "attributes_kjc"}:
                continue
            if child.tag == "fields":
                attrs["fields"] = [
                    {
                        "variable_name": _t(f, "variable_name"),
                        "variable_type": _t(f, "variable_type"),
                        "variable_string": _t(f, "variable_string"),
                    }
                    for f in child.findall("field")
                ]
            elif child.tag == "parameters":
                attrs["pass_all_parameters"] = _t(child, "pass_all_parameters", "N")
            elif len(list(child)) == 0 and child.text and child.text.strip():
                attrs[child.tag] = child.text.strip()
        filename = _t(e, "filename")
        if etype == "JOB":
            child_jobs.append((name, filename))
        entries.append(
            {
                "name": name,
                "entry_type": etype,
                "filename": filename,
                "jobname": _t(e, "jobname"),
                "transname": _t(e, "transname"),
                "is_start": _t(e, "start").upper() == "Y",
                "attributes": attrs,
            }
        )

    hops = [
        {
            "from_name": _t(h, "from"),
            "to_name": _t(h, "to"),
            "enabled": _yn(_t(h, "enabled", "Y")),
            "unconditional": _yn(_t(h, "unconditional")),
            "evaluation": _yn(_t(h, "evaluation")),
        }
        for h in root.findall("./hops/hop")
    ]

    params = {
        _t(p, "name"): _t(p, "default_value")
        for p in root.findall("./parameters/parameter")
    }

    child_registry = {}
    for entry_name, filename in child_jobs:
        stem = Path(filename.replace("\\", "/").split("/")[-1]).stem
        child_registry[entry_name] = (stem.lower(), filename)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    graph_path = OUT_DIR / "_master_etl_graph.py"
    graph_path.write_text(
        "# Auto-generated from Master_ETL.kjb — do not edit by hand.\n"
        f"JOB_PARAMETERS = {pformat(params, width=100)}\n\n"
        f"ENTRY_DEFS = {pformat(entries, width=100)}\n\n"
        f"HOP_DEFS = {pformat(hops, width=100)}\n\n"
        f"CHILD_JOB_REGISTRY = {pformat(child_registry, width=100)}\n",
        encoding="utf-8",
    )
    print("Wrote", graph_path)

    CHILDREN_DIR.mkdir(parents=True, exist_ok=True)
    (CHILDREN_DIR / "__init__.py").write_text(
        '"""Child jobs invoked by Master_ETL JOB entries."""\n',
        encoding="utf-8",
    )
    for entry_name, filename in child_jobs:
        stem = Path(filename.replace("\\", "/").split("/")[-1]).stem
        mod = stem.lower()
        (CHILDREN_DIR / f"{mod}.py").write_text(
            f'''"""Child job stub for Pentaho: {stem}.kjb

Invoked by Master_ETL entry "{entry_name}".
Filename expression: {filename}
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

logger = logging.getLogger("Master_ETL.children.{mod}")

JOB_NAME = {stem!r}
SOURCE_FILENAME = {filename!r}
PARENT_ENTRY = {entry_name!r}


def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    config = dict(config or {{}})
    logger.info(
        "CHILD JOB START | job=%s | parent_entry=%s | RUN_ID=%s | PROJECT_HOME=%s",
        JOB_NAME,
        PARENT_ENTRY,
        config.get("RUN_ID"),
        config.get("PROJECT_HOME"),
    )
    result = {{
        "job": JOB_NAME,
        "parent_entry": PARENT_ENTRY,
        "status": "SUCCESS",
        "config_keys": sorted(config.keys()),
    }}
    logger.info("CHILD JOB END | job=%s | status=SUCCESS", JOB_NAME)
    return result
''',
            encoding="utf-8",
        )
        print("  child", mod)


if __name__ == "__main__":
    main()
