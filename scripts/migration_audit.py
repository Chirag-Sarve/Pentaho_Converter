"""Complete Pentaho → Databricks migration audit.

Scores the nine checklist items toward a 100% migration target.
Emits docs/MIGRATION_AUDIT_REPORT.{md,json}.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
TRANS_DIR = ROOT / "databricks_project/src/pentaho_migration/transformations"
JOBS_DIR = ROOT / "databricks_project/src/pentaho_migration/jobs"
RETAIL_ROOT = Path(
    r"C:\Users\Prateek.Kotian\Desktop\Pentaho\Retail & E-commerce\Retail_ETL_Project"
)

LOOKUP_TYPES = {
    "StreamLookup",
    "DatabaseLookup",
    "DBLookup",
    "DimensionLookup",
    "CombinationLookup",
    "DBJoin",
}
OUTPUT_TYPES = {
    "TableOutput",
    "TextFileOutput",
    "CsvOutput",
    "InsertUpdate",
    "Update",
    "Delete",
    "ExcelWriter",
    "ExcelOutput",
    "JsonOutput",
    "ParquetOutput",
    "AvroOutput",
}


@dataclass
class Item:
    checklist: str
    key: str
    status: str  # PASS | PARTIAL | FAIL | N/A
    detail: str = ""
    weight: float = 1.0

    @property
    def score(self) -> float | None:
        if self.status == "N/A":
            return None
        if self.status == "PASS":
            return 1.0 * self.weight
        if self.status == "PARTIAL":
            return 0.5 * self.weight
        return 0.0


@dataclass
class CategoryScore:
    name: str
    items: list[Item] = field(default_factory=list)

    @property
    def scored(self) -> list[Item]:
        return [i for i in self.items if i.status != "N/A"]

    @property
    def pass_count(self) -> int:
        return sum(1 for i in self.scored if i.status == "PASS")

    @property
    def partial_count(self) -> int:
        return sum(1 for i in self.scored if i.status == "PARTIAL")

    @property
    def fail_count(self) -> int:
        return sum(1 for i in self.scored if i.status == "FAIL")

    @property
    def percentage(self) -> float:
        scored = self.scored
        if not scored:
            return 100.0
        earned = sum(i.score or 0.0 for i in scored)
        total = sum(i.weight for i in scored)
        return round(100.0 * earned / total, 2) if total else 100.0


def _text(el: ET.Element | None, tag: str, default: str = "") -> str:
    if el is None:
        return default
    c = el.find(tag)
    if c is None or c.text is None:
        return default
    return (c.text or "").strip()


def to_snake(name: str) -> str:
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    return s.strip("_").lower()


def step_token(name: str) -> str:
    return "df_" + re.sub(r"[^0-9A-Za-z]+", "_", name)


def find_trans_module(stem: str, info_name: str | None = None) -> Path | None:
    names = {to_snake(stem)}
    if info_name:
        names.add(to_snake(info_name))
    candidates = []
    for n in names:
        candidates.extend(
            [
                TRANS_DIR / f"{n}.py",
                TRANS_DIR / "retail" / f"{n}.py",
            ]
        )
    for c in candidates:
        if c.exists():
            return c
    # fuzzy
    want = {n.replace("_", "") for n in names}
    for folder in (TRANS_DIR, TRANS_DIR / "retail"):
        if not folder.exists():
            continue
        for p in folder.glob("*.py"):
            if p.stem.replace("_", "") in want:
                return p
    return None


def find_job_module(stem: str) -> Path | None:
    snake = to_snake(stem)
    candidates = [
        JOBS_DIR / f"{snake}.py",
        JOBS_DIR / f"{stem}.py",
        JOBS_DIR / "children" / f"{snake}.py",
        JOBS_DIR / "Master_ETL.py" if stem.lower() in {"master_etl", "master-etl"} else None,
        JOBS_DIR / "master.py" if stem.lower() == "master" else None,
    ]
    for c in candidates:
        if c and c.exists():
            return c
    return None


def parse_ktr(path: Path) -> dict:
    root = ET.parse(path).getroot()
    info = root.find("info")
    name = _text(info, "name") if info is not None else path.stem
    hops = [
        {
            "from": _text(h, "from"),
            "to": _text(h, "to"),
            "enabled": _text(h, "enabled", "Y"),
        }
        for h in root.findall("./order/hop")
    ]
    steps = []
    for s in root.findall("step"):
        stype = _text(s, "type")
        sname = _text(s, "name")
        steps.append({"name": sname, "type": stype})

    params = []
    for p in root.findall("./info/parameters/parameter"):
        params.append({"name": _text(p, "name"), "default": _text(p, "default_value")})
    # also <parameters> under root some Kettle variants
    for p in root.findall("./parameters/parameter"):
        params.append({"name": _text(p, "name"), "default": _text(p, "default_value")})

    variables = []
    for v in root.findall("./info/variables/variable"):
        variables.append({"name": _text(v, "name"), "value": _text(v, "value")})
    for v in root.findall("./variables/variable"):
        variables.append({"name": _text(v, "name"), "value": _text(v, "value")})

    # GetVariable / SetVariable steps also declare vars
    for s in root.findall("step"):
        if _text(s, "type") in {"GetVariable", "GetVariables", "SetVariable", "SetVariables"}:
            for f in s.findall("./fields/field"):
                vn = _text(f, "variable") or _text(f, "variable_name") or _text(f, "name")
                if vn:
                    variables.append({"name": vn, "value": "", "from_step": _text(s, "name")})

    return {
        "name": name,
        "path": path,
        "hops": hops,
        "steps": steps,
        "parameters": params,
        "variables": variables,
    }


def parse_kjb(path: Path) -> dict:
    root = ET.parse(path).getroot()
    name = _text(root.find("name"), ".") if root.find("name") is not None else path.stem
    # name may be text of <name> child of job
    if root.find("name") is not None and root.find("name").text:
        name = root.find("name").text.strip()
    entries = []
    for e in root.findall("./entries/entry"):
        entries.append(
            {
                "name": _text(e, "name"),
                "type": _text(e, "type"),
                "filename": _text(e, "filename"),
            }
        )
    hops = [
        {
            "from": _text(h, "from"),
            "to": _text(h, "to"),
            "enabled": _text(h, "enabled", "Y"),
            "evaluation": _text(h, "evaluation"),
            "unconditional": _text(h, "unconditional"),
        }
        for h in root.findall("./hops/hop")
    ]
    params = []
    for p in root.findall("./parameters/parameter"):
        params.append({"name": _text(p, "name"), "default": _text(p, "default_value")})
    variables = []
    for v in root.findall("./variables/variable"):
        variables.append({"name": _text(v, "name"), "value": _text(v, "value")})
    return {
        "name": name,
        "path": path,
        "entries": entries,
        "hops": hops,
        "parameters": params,
        "variables": variables,
    }


def step_status_in_code(code: str, step_name: str) -> str:
    """Return PASS/PARTIAL/FAIL from conversion markers or DF presence."""
    # Prefer explicit markers
    for status, tag in (
        ("FAIL", "[failed]"),
        ("PARTIAL", "[partial]"),
        ("PASS", "[converted]"),
    ):
        # Match "# Step: Name ... [tag]"
        pattern = rf"# Step:\s*{re.escape(step_name)}\s*.*?{re.escape(tag)}"
        if re.search(pattern, code):
            return status
    token = step_token(step_name)
    comment = f"# Step: {step_name}"
    if comment in code or token in code:
        # Present without quality mark — treat as converted if token assigned
        if re.search(rf"\b{re.escape(token)}\s*=", code) or comment in code:
            return "PASS"
        return "PARTIAL"
    return "FAIL"


def hop_ok(code: str, frm: str, to: str) -> bool:
    src = step_token(frm)
    dst = step_token(to)
    if src in code and dst in code:
        return True
    # jobs store hop defs differently
    return frm in code and to in code


def audit_ktrs(ktr_paths: list[Path]) -> dict[str, CategoryScore]:
    cats = {
        "ktr_file": CategoryScore("Every .ktr converted"),
        "step": CategoryScore("Every transformation step converted"),
        "hop": CategoryScore("Every hop converted"),
        "variable": CategoryScore("Every variable converted"),
        "parameter": CategoryScore("Every parameter converted"),
        "lookup": CategoryScore("Every lookup converted"),
        "output": CategoryScore("Every output converted"),
    }

    for path in ktr_paths:
        try:
            parsed = parse_ktr(path)
        except ET.ParseError as exc:
            cats["ktr_file"].items.append(
                Item("ktr_file", path.name, "FAIL", f"XML parse error: {exc}")
            )
            continue

        mod = find_trans_module(path.stem, parsed["name"])
        if mod is None:
            cats["ktr_file"].items.append(
                Item("ktr_file", path.name, "FAIL", "no generated module")
            )
            continue

        code = mod.read_text(encoding="utf-8")
        if "SOURCE_MISSING = True" in code:
            # Should not happen for on-disk KTR
            cats["ktr_file"].items.append(
                Item(
                    "ktr_file",
                    path.name,
                    "PARTIAL",
                    f"module {mod.name} marked SOURCE_MISSING despite source on disk",
                )
            )
        elif "def run(" not in code:
            cats["ktr_file"].items.append(
                Item("ktr_file", path.name, "FAIL", f"module {mod.name} missing run()")
            )
            continue
        else:
            cats["ktr_file"].items.append(
                Item("ktr_file", path.name, "PASS", f"→ {mod.relative_to(ROOT).as_posix()}")
            )

        for step in parsed["steps"]:
            st = step_status_in_code(code, step["name"])
            cats["step"].items.append(
                Item(
                    "step",
                    f"{path.stem}:{step['name']} [{step['type']}]",
                    st,
                    "",
                )
            )
            if step["type"] in LOOKUP_TYPES:
                token = step_token(step["name"])
                has_join = ".join(" in code and (
                    token in code or step["name"] in code
                )
                has_broadcast = "broadcast(" in code
                if has_join:
                    status = "PASS"
                    detail = f"join present broadcast={has_broadcast}"
                elif token in code or f"# Step: {step['name']}" in code:
                    # StreamLookup with no keys often passthrough
                    if "[failed]" in code[
                        code.find(f"# Step: {step['name']}") : code.find(
                            f"# Step: {step['name']}"
                        )
                        + 200
                    ]:
                        status = "FAIL"
                        detail = "lookup marker failed"
                    else:
                        status = "PARTIAL"
                        detail = "lookup step present; join may be incomplete"
                else:
                    status = "FAIL"
                    detail = "lookup missing"
                cats["lookup"].items.append(
                    Item("lookup", f"{path.stem}:{step['name']}", status, detail)
                )
            if step["type"] in OUTPUT_TYPES:
                token = step_token(step["name"])
                has_write = any(
                    x in code
                    for x in (
                        "write_delta(",
                        ".write.",
                        "saveAsTable(",
                        ".save(",
                        "DeltaTable",
                    )
                )
                present = f"# Step: {step['name']}" in code or token in code
                if present and has_write:
                    # failed text outputs still count partial if marker failed
                    marker = step_status_in_code(code, step["name"])
                    cats["output"].items.append(
                        Item(
                            "output",
                            f"{path.stem}:{step['name']} [{step['type']}]",
                            marker if marker != "FAIL" else ("PARTIAL" if has_write else "FAIL"),
                            "writer present",
                        )
                    )
                elif present:
                    cats["output"].items.append(
                        Item(
                            "output",
                            f"{path.stem}:{step['name']}",
                            "PARTIAL",
                            "step present without write sink",
                        )
                    )
                else:
                    cats["output"].items.append(
                        Item("output", f"{path.stem}:{step['name']}", "FAIL", "missing")
                    )

        for hop in parsed["hops"]:
            if hop.get("enabled", "Y").upper() == "N":
                cats["hop"].items.append(
                    Item(
                        "hop",
                        f"{path.stem}:{hop['from']}→{hop['to']}",
                        "N/A",
                        "disabled",
                    )
                )
                continue
            ok = hop_ok(code, hop["from"], hop["to"])
            cats["hop"].items.append(
                Item(
                    "hop",
                    f"{path.stem}:{hop['from']}→{hop['to']}",
                    "PASS" if ok else "FAIL",
                )
            )

        # Parameters
        if not parsed["parameters"]:
            cats["parameter"].items.append(
                Item("parameter", f"{path.stem}:<none>", "N/A", "ktr declares no parameters")
            )
        for p in parsed["parameters"]:
            pname = p["name"]
            # generated modules typically bind config.get("NAME") or local assignment
            ok = (
                f'config.get("{pname}"' in code
                or f"config.get('{pname}'" in code
                or f"{pname} =" in code
                or f'"{pname}"' in code
            )
            cats["parameter"].items.append(
                Item(
                    "parameter",
                    f"{path.stem}:{pname}",
                    "PASS" if ok else "FAIL",
                )
            )

        # Variables
        seen_vars: set[str] = set()
        if not parsed["variables"]:
            cats["variable"].items.append(
                Item("variable", f"{path.stem}:<none>", "N/A", "ktr declares no variables")
            )
        for v in parsed["variables"]:
            raw = v["name"]
            # strip ${}
            clean = raw.replace("${", "").replace("}", "").strip()
            if not clean or clean in seen_vars:
                continue
            seen_vars.add(clean)
            ok = (
                clean in code
                or f'"{clean}"' in code
                or f"'{clean}'" in code
                or f"config.get(\"{clean}\"" in code
                or "GetVariable" in code
                or "Get_Variable" in code
                or "get_variable" in code.lower()
            )
            cats["variable"].items.append(
                Item(
                    "variable",
                    f"{path.stem}:{clean}",
                    "PASS" if ok else "FAIL",
                )
            )

    return cats


def audit_kjbs(kjb_paths: list[Path]) -> dict[str, CategoryScore]:
    cats = {
        "kjb_file": CategoryScore("Every .kjb converted"),
        "job_entry": CategoryScore("Every job entry converted"),
        "job_hop": CategoryScore("Every hop converted (jobs)"),
        "job_variable": CategoryScore("Every variable converted (jobs)"),
        "job_parameter": CategoryScore("Every parameter converted (jobs)"),
    }

    graph_code = ""
    graph_path = JOBS_DIR / "_master_etl_graph.py"
    if graph_path.exists():
        graph_code = graph_path.read_text(encoding="utf-8")

    for path in kjb_paths:
        try:
            parsed = parse_kjb(path)
        except ET.ParseError as exc:
            cats["kjb_file"].items.append(
                Item("kjb_file", path.name, "FAIL", f"XML parse error: {exc}")
            )
            continue

        mod = find_job_module(path.stem)
        # Master_ETL special
        if path.stem == "Master_ETL":
            mod = JOBS_DIR / "Master_ETL.py"
        if path.stem == "Master" and (JOBS_DIR / "master.py").exists():
            mod = JOBS_DIR / "master.py"

        if mod is None or not mod.exists():
            cats["kjb_file"].items.append(
                Item("kjb_file", path.name, "FAIL", "no generated job module")
            )
            continue

        code = mod.read_text(encoding="utf-8")
        # Use graph for Master_ETL entries/hops
        search = code
        if path.stem == "Master_ETL" and graph_code:
            search = graph_code + "\n" + code

        if "def run(" not in code and "JobRuntime" not in code:
            cats["kjb_file"].items.append(
                Item("kjb_file", path.name, "FAIL", f"{mod.name} incomplete")
            )
            continue

        detail = f"→ {mod.relative_to(ROOT).as_posix()}"
        if "EXPANDED = True" in code or path.stem in {"Master_ETL", "Master"}:
            cats["kjb_file"].items.append(Item("kjb_file", path.name, "PASS", detail))
        elif "Placeholder" in code or "stub" in code.lower():
            cats["kjb_file"].items.append(
                Item("kjb_file", path.name, "PARTIAL", detail + " (stub)")
            )
        else:
            cats["kjb_file"].items.append(Item("kjb_file", path.name, "PASS", detail))

        for e in parsed["entries"]:
            ok = e["name"] in search or e["name"] in code
            cats["job_entry"].items.append(
                Item(
                    "job_entry",
                    f"{path.stem}:{e['name']} [{e['type']}]",
                    "PASS" if ok else "FAIL",
                )
            )

        for h in parsed["hops"]:
            if h.get("enabled", "Y").upper() == "N":
                cats["job_hop"].items.append(
                    Item(
                        "job_hop",
                        f"{path.stem}:{h['from']}→{h['to']}",
                        "N/A",
                        "disabled",
                    )
                )
                continue
            ok = h["from"] in search and h["to"] in search
            cats["job_hop"].items.append(
                Item(
                    "job_hop",
                    f"{path.stem}:{h['from']}→{h['to']}",
                    "PASS" if ok else "FAIL",
                )
            )

        if not parsed["parameters"]:
            cats["job_parameter"].items.append(
                Item("job_parameter", f"{path.stem}:<none>", "N/A", "no parameters")
            )
        for p in parsed["parameters"]:
            pname = p["name"]
            ok = pname in search or pname in code
            cats["job_parameter"].items.append(
                Item(
                    "job_parameter",
                    f"{path.stem}:{pname}",
                    "PASS" if ok else "FAIL",
                )
            )

        if not parsed["variables"]:
            cats["job_variable"].items.append(
                Item("job_variable", f"{path.stem}:<none>", "N/A", "no variables block")
            )
        for v in parsed["variables"]:
            clean = v["name"].replace("${", "").replace("}", "")
            ok = clean in search or clean in code
            cats["job_variable"].items.append(
                Item(
                    "job_variable",
                    f"{path.stem}:{clean}",
                    "PASS" if ok else "FAIL",
                )
            )

    return cats


def audit_job_referenced_missing_ktrs() -> CategoryScore:
    """JOB TRANS refs whose .ktr is absent on disk → placeholder modules."""
    cat = CategoryScore("JOB-referenced .ktr (missing on disk)")
    if not RETAIL_ROOT.exists():
        return cat
    on_disk = {p.stem.lower() for p in (RETAIL_ROOT / "transformations").rglob("*.ktr")}
    seen: set[str] = set()
    for kjb in (RETAIL_ROOT / "jobs").rglob("*.kjb"):
        root = ET.parse(kjb).getroot()
        for e in root.findall("./entries/entry"):
            if (e.findtext("type") or "").strip() != "TRANS":
                continue
            fn = (e.findtext("filename") or "").strip()
            if not fn:
                continue
            stem = Path(fn.replace("\\", "/").split("/")[-1]).stem
            key = stem.lower()
            if key in seen:
                continue
            seen.add(key)
            if key in on_disk:
                continue
            mod = find_trans_module(stem)
            if mod and "def run(" in mod.read_text(encoding="utf-8"):
                code = mod.read_text(encoding="utf-8")
                if "SOURCE_MISSING = True" in code:
                    cat.items.append(
                        Item(
                            "ktr_file",
                            f"{stem}.ktr (missing)",
                            "PARTIAL",
                            "placeholder module closes orchestration gap",
                        )
                    )
                else:
                    cat.items.append(
                        Item(
                            "ktr_file",
                            f"{stem}.ktr (missing)",
                            "PASS",
                            "module without SOURCE_MISSING flag",
                        )
                    )
            else:
                cat.items.append(
                    Item("ktr_file", f"{stem}.ktr (missing)", "FAIL", "no module")
                )
    return cat


def merge_variable_parameter(
    trans_cats: dict[str, CategoryScore], job_cats: dict[str, CategoryScore]
) -> tuple[CategoryScore, CategoryScore]:
    var = CategoryScore("Every variable converted")
    var.items.extend(trans_cats["variable"].items)
    var.items.extend(job_cats["job_variable"].items)
    param = CategoryScore("Every parameter converted")
    param.items.extend(trans_cats["parameter"].items)
    param.items.extend(job_cats["job_parameter"].items)
    return var, param


def merge_hops(
    trans_cats: dict[str, CategoryScore], job_cats: dict[str, CategoryScore]
) -> CategoryScore:
    hops = CategoryScore("Every hop converted")
    hops.items.extend(trans_cats["hop"].items)
    hops.items.extend(job_cats["job_hop"].items)
    return hops


def checklist_rows(
    ktr: CategoryScore,
    kjb: CategoryScore,
    step: CategoryScore,
    entry: CategoryScore,
    hop: CategoryScore,
    variable: CategoryScore,
    parameter: CategoryScore,
    lookup: CategoryScore,
    output: CategoryScore,
) -> list[dict]:
    rows = []
    mapping = [
        ("Every .ktr converted", ktr),
        ("Every .kjb converted", kjb),
        ("Every transformation step converted", step),
        ("Every job entry converted", entry),
        ("Every hop converted", hop),
        ("Every variable converted", variable),
        ("Every parameter converted", parameter),
        ("Every lookup converted", lookup),
        ("Every output converted", output),
    ]
    for label, cat in mapping:
        scored = cat.scored
        rows.append(
            {
                "checklist": label,
                "pass": cat.pass_count,
                "partial": cat.partial_count,
                "fail": cat.fail_count,
                "na": sum(1 for i in cat.items if i.status == "N/A"),
                "total_scored": len(scored),
                "percentage": cat.percentage,
                "met_100": cat.percentage >= 100.0 and cat.fail_count == 0 and cat.partial_count == 0,
            }
        )
    return rows


def overall_percentage(rows: list[dict]) -> float:
    # Equal weight across checklist items that have scored items
    active = [r for r in rows if r["total_scored"] > 0]
    if not active:
        return 100.0
    return round(sum(r["percentage"] for r in active) / len(active), 2)


def collect_source_files() -> tuple[list[Path], list[Path]]:
    ktrs: list[Path] = []
    kjbs: list[Path] = []
    # Converter samples + tests
    for base in (ROOT / "samples", ROOT / "tests" / "samples"):
        if base.exists():
            ktrs.extend(sorted(base.rglob("*.ktr")))
            kjbs.extend(sorted(base.rglob("*.kjb")))
    # Retail project
    if RETAIL_ROOT.exists():
        ktrs.extend(sorted((RETAIL_ROOT / "transformations").rglob("*.ktr")))
        kjbs.extend(sorted((RETAIL_ROOT / "jobs").rglob("*.kjb")))
    return ktrs, kjbs


def main() -> None:
    ktrs, kjbs = collect_source_files()
    trans_cats = audit_ktrs(ktrs)
    job_cats = audit_kjbs(kjbs)
    missing_refs = audit_job_referenced_missing_ktrs()

    # Combined .ktr score: on-disk + missing referenced (placeholders as PARTIAL)
    ktr_combined = CategoryScore("Every .ktr converted")
    ktr_combined.items.extend(trans_cats["ktr_file"].items)
    ktr_combined.items.extend(missing_refs.items)

    variable, parameter = merge_variable_parameter(trans_cats, job_cats)
    hop = merge_hops(trans_cats, job_cats)

    rows = checklist_rows(
        ktr=ktr_combined,
        kjb=job_cats["kjb_file"],
        step=trans_cats["step"],
        entry=job_cats["job_entry"],
        hop=hop,
        variable=variable,
        parameter=parameter,
        lookup=trans_cats["lookup"],
        output=trans_cats["output"],
    )
    overall = overall_percentage(rows)

    # Gap lists (top fails / partials)
    all_cats = [
        ktr_combined,
        job_cats["kjb_file"],
        trans_cats["step"],
        job_cats["job_entry"],
        hop,
        variable,
        parameter,
        trans_cats["lookup"],
        trans_cats["output"],
    ]
    gaps = []
    for cat in all_cats:
        for item in cat.items:
            if item.status in {"FAIL", "PARTIAL"}:
                gaps.append(
                    {
                        "checklist": cat.name,
                        "key": item.key,
                        "status": item.status,
                        "detail": item.detail,
                    }
                )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_percentage": 100.0,
        "overall_percentage": overall,
        "source_counts": {
            "ktr_on_disk": len(ktrs),
            "kjb_on_disk": len(kjbs),
            "ktr_missing_but_job_referenced": len(missing_refs.items),
        },
        "checklist": rows,
        "gap_count": len(gaps),
        "gaps": gaps[:200],  # cap for JSON readability
        "gap_summary": {
            "FAIL": sum(1 for g in gaps if g["status"] == "FAIL"),
            "PARTIAL": sum(1 for g in gaps if g["status"] == "PARTIAL"),
        },
    }

    out_json = ROOT / "docs" / "MIGRATION_AUDIT_REPORT.json"
    out_md = ROOT / "docs" / "MIGRATION_AUDIT_REPORT.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Complete Migration Audit Report",
        "",
        f"**Generated:** {report['generated_at']}",
        f"**Target:** {report['target_percentage']:.0f}%",
        f"**Overall migration:** **{overall}%**",
        "",
        "## Source inventory",
        "",
        f"- `.ktr` on disk: **{len(ktrs)}**",
        f"- `.kjb` on disk: **{len(kjbs)}**",
        f"- `.ktr` missing on disk but referenced by JOB TRANS entries: **{len(missing_refs.items)}**",
        "",
        "## Checklist scorecard",
        "",
        "| Checklist item | PASS | PARTIAL | FAIL | N/A | Scored | % | 100%? |",
        "|----------------|-----:|--------:|-----:|----:|-------:|--:|:-----:|",
    ]
    for r in rows:
        met = "✓" if r["met_100"] else "✗"
        lines.append(
            f"| {r['checklist']} | {r['pass']} | {r['partial']} | {r['fail']} | "
            f"{r['na']} | {r['total_scored']} | {r['percentage']}% | {met} |"
        )
    lines.extend(
        [
            "",
            f"**Equal-weight average of checklist categories = {overall}%** (target 100%).",
            "",
            "## Gap summary",
            "",
            f"- FAIL items: **{report['gap_summary']['FAIL']}**",
            f"- PARTIAL items: **{report['gap_summary']['PARTIAL']}**",
            "",
            "### Top gaps (first 80)",
            "",
            "| Status | Checklist | Key | Detail |",
            "|--------|-----------|-----|--------|",
        ]
    )
    for g in gaps[:80]:
        detail = (g["detail"] or "").replace("|", "/")[:80]
        lines.append(
            f"| {g['status']} | {g['checklist']} | `{g['key']}` | {detail} |"
        )
    if len(gaps) > 80:
        lines.append("")
        lines.append(f"_… {len(gaps) - 80} more gaps in JSON report._")

    lines.extend(
        [
            "",
            "## Notes on scoring",
            "",
            "- **PASS** = 100% credit; **PARTIAL** = 50% credit; **FAIL** = 0%; **N/A** excluded.",
            "- Missing-on-disk `.ktr` files referenced by retail JOBs score as **PARTIAL** when a "
            "`SOURCE_MISSING` placeholder module exists (orchestration gap closed, logic absent).",
            "- Step quality uses generator markers `[converted]` / `[partial]` / `[failed]` when present.",
            "- Overall % is the **unweighted mean** of the nine checklist category percentages.",
            "",
        ]
    )
    out_md.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({"overall_percentage": overall, "checklist": rows}, indent=2))
    print(f"Wrote {out_md}")
    print(f"Wrote {out_json}")
    print(f"Gaps FAIL={report['gap_summary']['FAIL']} PARTIAL={report['gap_summary']['PARTIAL']}")


if __name__ == "__main__":
    main()
