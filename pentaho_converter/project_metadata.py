"""Build project inventory and lineage metadata for the UI."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from .dependency_resolver import build_transformation_index, resolve_transformation_reference
from .models import ConversionStats, PentahoJob, PentahoJobEntry, PentahoTransformation, ScanResult

_CONTROL_ENTRY_TYPES = frozenset({"SPECIAL", "SUCCESS", "DUMMY", "START"})


def _normalize_stem(path_or_name: str) -> str:
    name = Path(path_or_name.replace("\\", "/")).name
    for suffix in (".ktr", ".kjb", ".KTR", ".KJB"):
        if name.lower().endswith(suffix.lower()):
            return name[: -len(suffix)]
    return Path(name).stem


def _step_results_for_transformation(
    trans: PentahoTransformation, stats: ConversionStats
) -> list:
    step_names = {s.name for s in trans.steps}
    return [s for s in stats.step_results if s.step_name in step_names]


def _conversion_rate(steps: list) -> int:
    if not steps:
        return 0
    scores = [getattr(s, "semantic_score", 0.0) or 0.0 for s in steps]
    return int(round(sum(scores) / len(scores) * 100))


def _complexity_from_steps(step_count: int, hop_count: int, conversion_rate: int) -> int:
    base = step_count * 4 + hop_count * 2
    penalty = max(0, 100 - conversion_rate) // 5
    return min(100, max(5, base + penalty))


def _job_complexity(job: PentahoJob, child_count: int) -> int:
    actionable = [
        e for e in job.entries if e.entry_type.upper() not in _CONTROL_ENTRY_TYPES
    ]
    return min(100, max(5, len(actionable) * 8 + len(job.hops) * 3 + child_count * 5))


def _status_from_conversion(rate: int, has_output: bool) -> str:
    if not has_output:
        return "pending"
    if rate >= 80:
        return "converted"
    if rate >= 50:
        return "needs_review"
    return "failed"


def _resolve_job_file(reference: str, scan: ScanResult) -> str | None:
    if not reference:
        return None
    ref_name = Path(reference.replace("\\", "/")).name
    for job_path in scan.job_files:
        if job_path.name.lower() == ref_name.lower():
            return job_path.name
        if _normalize_stem(job_path.name).lower() == _normalize_stem(ref_name).lower():
            return job_path.name
    return ref_name if ref_name.lower().endswith(".kjb") else None


def _resolve_trans_file(
    filename: str,
    transname: str,
    index: dict[str, PentahoTransformation],
    scan: ScanResult,
) -> str | None:
    trans = resolve_transformation_reference(filename, transname, index, scan)
    if trans is not None:
        return trans.file_path.name
    ref = filename or transname
    if not ref:
        return None
    ref_name = Path(ref.replace("\\", "/")).name
    for ktr_path in scan.transformation_files:
        if ktr_path.name.lower() == ref_name.lower():
            return ktr_path.name
        if _normalize_stem(ktr_path.name).lower() == _normalize_stem(ref_name).lower():
            return ktr_path.name
    return ref_name if ref_name.lower().endswith(".ktr") else None


def _build_hop_adjacency(job: PentahoJob) -> tuple[dict[str, list[str]], dict[str, PentahoJobEntry]]:
    """Build hop adjacency in XML hop order (supports branches)."""
    entries_by_name = {e.name: e for e in job.entries}
    adj: dict[str, list[str]] = defaultdict(list)
    for hop in job.hops:
        if not hop.enabled:
            continue
        if hop.from_name in entries_by_name and hop.to_name in entries_by_name:
            adj[hop.from_name].append(hop.to_name)
    return adj, entries_by_name


def _start_entry_names(job: PentahoJob, adj: dict[str, list[str]]) -> list[str]:
    """Return job entry names where Pentaho execution begins."""
    starts = [e.name for e in job.entries if e.is_start]
    if starts:
        return starts

    in_degree: dict[str, int] = {e.name: 0 for e in job.entries}
    for from_name, targets in adj.items():
        for to_name in targets:
            in_degree[to_name] = in_degree.get(to_name, 0) + 1

    roots = [name for name, deg in in_degree.items() if deg == 0]
    if roots:
        return roots

    special = [
        e.name
        for e in job.entries
        if e.entry_type.upper() == "SPECIAL" or e.name.lower() == "start"
    ]
    return special or ([job.entries[0].name] if job.entries else [])


def _entry_target(
    entry: PentahoJobEntry,
    scan: ScanResult,
    index: dict[str, PentahoTransformation],
) -> tuple[str | None, str]:
    etype = entry.entry_type.upper()
    if etype == "TRANS":
        return _resolve_trans_file(entry.filename, entry.transname, index, scan), "transformation"
    if etype == "JOB":
        return _resolve_job_file(entry.filename, scan), "job"
    return None, ""


def _find_root_jobs(
    jobs_by_file: dict[str, PentahoJob],
    scan: ScanResult,
    *,
    primary_job_name: str | None = None,
) -> list[str]:
    """Identify orchestrator .kjb files that are not nested children of other jobs."""
    referenced: set[str] = set()
    for job in jobs_by_file.values():
        for entry in job.entries:
            if entry.entry_type.upper() == "JOB":
                ref = _resolve_job_file(entry.filename, scan)
                if ref:
                    referenced.add(ref)

    roots = [f for f in jobs_by_file if f not in referenced]
    if not roots:
        starters = [f for f, j in jobs_by_file.items() if any(e.is_start for e in j.entries)]
        roots = starters or list(jobs_by_file.keys())

    if primary_job_name:
        primary_file = next(
            (f for f, j in jobs_by_file.items() if j.name == primary_job_name),
            None,
        )
        if primary_file:
            roots = [primary_file] + [r for r in roots if r != primary_file]

    return roots


class _LineageBuilder:
    """Traverse Pentaho job hops to build execution-faithful lineage."""

    def __init__(
        self,
        scan: ScanResult,
        jobs_by_file: dict[str, PentahoJob],
        trans_by_file: dict[str, PentahoTransformation],
        stats: ConversionStats,
        index: dict[str, PentahoTransformation],
        pyspark_notebook: str,
    ) -> None:
        self.scan = scan
        self.jobs_by_file = jobs_by_file
        self.trans_by_file = trans_by_file
        self.stats = stats
        self.index = index
        self.pyspark_notebook = pyspark_notebook
        self.nodes: list[dict] = []
        self.edges: list[dict] = []
        self.node_index: dict[str, dict] = {}
        self.edge_seen: set[tuple[str, str]] = set()
        self.expanded_jobs: set[str] = set()
        self._sequence = 0
        self._incoming: dict[str, int] = defaultdict(int)

    def _trans_metrics(self, trans: PentahoTransformation) -> tuple[int, int, str]:
        steps = _step_results_for_transformation(trans, self.stats)
        rate = _conversion_rate(steps)
        complexity = _complexity_from_steps(len(trans.steps), len(trans.hops), rate)
        status = _status_from_conversion(rate, bool(steps or self.stats.step_results))
        return complexity, rate, status

    def _job_metrics(self, job: PentahoJob, job_file: str) -> tuple[int, int, str]:
        child_files = {
            e["to"] for e in self.edges if e["from"] == job_file and e["to"] != job_file
        }
        child_rates: list[int] = []
        for child in child_files:
            if child in self.trans_by_file:
                _, rate, _ = self._trans_metrics(self.trans_by_file[child])
                child_rates.append(rate)
            elif child in self.node_index:
                child_rates.append(self.node_index[child].get("conversion_pct", 0))
        rate = (
            int(round(sum(child_rates) / len(child_rates)))
            if child_rates
            else (85 if self.stats.step_results else 0)
        )
        complexity = _job_complexity(job, len(child_files))
        status = _status_from_conversion(rate, bool(self.stats.step_results))
        return complexity, rate, status

    def ensure_node(
        self,
        file_name: str,
        *,
        file_type: str,
        display_name: str,
        complexity: int,
        conversion_pct: int,
        status: str,
        description: str = "",
    ) -> dict:
        if file_name not in self.node_index:
            node = {
                "id": file_name,
                "name": display_name,
                "file": file_name,
                "type": file_type,
                "parent": None,
                "children": [],
                "complexity": complexity,
                "conversion_pct": conversion_pct,
                "status": status,
                "description": description,
                "pyspark_notebook": self.pyspark_notebook if status != "pending" else "",
            }
            self.node_index[file_name] = node
            self.nodes.append(node)
        else:
            node = self.node_index[file_name]
            node["complexity"] = complexity
            node["conversion_pct"] = conversion_pct
            node["status"] = status
            if description:
                node["description"] = description
        return self.node_index[file_name]

    def _add_edge(self, from_file: str, to_file: str, via_entry: str) -> None:
        key = (from_file, to_file)
        if key in self.edge_seen:
            return
        self.edge_seen.add(key)
        self.edges.append({
            "from": from_file,
            "to": to_file,
            "via_entry": via_entry,
            "sequence": self._sequence,
        })
        self._sequence += 1
        self._incoming[to_file] += 1
        parent = self.node_index.get(from_file)
        if parent and to_file not in parent["children"]:
            parent["children"].append(to_file)
        child = self.node_index.get(to_file)
        if child and not child.get("parent"):
            child["parent"] = from_file

    def _ensure_job_node(self, job_file: str) -> None:
        job = self.jobs_by_file[job_file]
        complexity, rate, status = self._job_metrics(job, job_file)
        self.ensure_node(
            job_file,
            file_type="job",
            display_name=job.name,
            complexity=complexity,
            conversion_pct=rate,
            status=status,
            description=f"Job: {job.name}",
        )

    def _ensure_trans_node(self, trans_file: str) -> None:
        trans = self.trans_by_file[trans_file]
        complexity, rate, status = self._trans_metrics(trans)
        self.ensure_node(
            trans_file,
            file_type="transformation",
            display_name=trans.name,
            complexity=complexity,
            conversion_pct=rate,
            status=status,
            description=f"Transformation: {trans.name} ({len(trans.steps)} steps)",
        )

    def _walk_entry(
        self,
        entry_name: str,
        last_actionable_file: str,
        job_file: str,
        adj: dict[str, list[str]],
        entries_by_name: dict[str, PentahoJobEntry],
        stack: set[str],
    ) -> None:
        if entry_name in stack:
            return

        entry = entries_by_name.get(entry_name)
        if entry is None:
            return

        stack.add(entry_name)
        current_last = last_actionable_file
        target_file, target_type = _entry_target(entry, self.scan, self.index)

        if target_file:
            if target_type == "transformation" and target_file in self.trans_by_file:
                self._ensure_trans_node(target_file)
            elif target_type == "job" and target_file in self.jobs_by_file:
                self._ensure_job_node(target_file)

            self._add_edge(last_actionable_file, target_file, entry.name)
            current_last = target_file

            if target_type == "job" and target_file in self.jobs_by_file:
                self._expand_job(target_file)

        for next_entry in adj.get(entry_name, []):
            self._walk_entry(next_entry, current_last, job_file, adj, entries_by_name, stack)

        stack.remove(entry_name)

    def _expand_job(self, job_file: str) -> None:
        if job_file in self.expanded_jobs:
            return
        if job_file not in self.jobs_by_file:
            return

        self.expanded_jobs.add(job_file)
        self._ensure_job_node(job_file)

        job = self.jobs_by_file[job_file]
        adj, entries_by_name = _build_hop_adjacency(job)
        for start_name in _start_entry_names(job, adj):
            self._walk_entry(start_name, job_file, job_file, adj, entries_by_name, set())

    def build_from_roots(self, root_jobs: list[str]) -> None:
        for root_file in root_jobs:
            if root_file in self.jobs_by_file:
                self._expand_job(root_file)

    def add_unreachable_transformations(self) -> None:
        """Include .ktr files not referenced by any job execution path."""
        reachable = {n["file"] for n in self.nodes if n["type"] == "transformation"}
        for trans_file, trans in self.trans_by_file.items():
            if trans_file in reachable:
                continue
            self._ensure_trans_node(trans_file)


def build_project_metadata(
    scan: ScanResult,
    jobs: dict[str, PentahoJob],
    transformations: dict[str, PentahoTransformation],
    stats: ConversionStats,
    *,
    main_workflow: str | None = None,
    primary_job_name: str | None = None,
) -> tuple[list[dict], dict]:
    """Return (inventory rows, lineage graph) for all .kjb and .ktr files."""
    index = build_transformation_index(transformations)
    jobs_by_file = {job.file_path.name: job for job in jobs.values()}
    trans_by_file = {t.file_path.name: t for t in transformations.values()}
    pyspark_notebook = main_workflow or ""

    builder = _LineageBuilder(
        scan, jobs_by_file, trans_by_file, stats, index, pyspark_notebook
    )

    root_jobs = _find_root_jobs(jobs_by_file, scan, primary_job_name=primary_job_name)
    builder.build_from_roots(root_jobs)
    builder.add_unreachable_transformations()

    # Refresh job metrics now that edges exist
    for job_file, job in jobs_by_file.items():
        if job_file in builder.node_index:
            complexity, rate, status = builder._job_metrics(job, job_file)
            node = builder.node_index[job_file]
            node["complexity"] = complexity
            node["conversion_pct"] = rate
            node["status"] = status

    inventory: list[dict] = []
    for trans_file, trans in trans_by_file.items():
        complexity, rate, status = builder._trans_metrics(trans)
        deps = builder._incoming.get(trans_file, 0)
        inventory.append({
            "file": trans_file,
            "name": trans.name,
            "type": ".ktr",
            "complexity": complexity,
            "conversion_rate": rate,
            "status": status,
            "dependencies": deps,
            "description": f"Pentaho transformation with {len(trans.steps)} step(s)",
        })

    for job_file, job in jobs_by_file.items():
        complexity, rate, status = builder._job_metrics(job, job_file)
        deps = builder._incoming.get(job_file, 0)
        inventory.append({
            "file": job_file,
            "name": job.name,
            "type": ".kjb",
            "complexity": complexity,
            "conversion_rate": rate,
            "status": status,
            "dependencies": deps,
            "description": f"Pentaho job with {len(job.entries)} entries",
        })

    lineage = {
        "root_jobs": root_jobs,
        "nodes": builder.nodes,
        "edges": builder.edges,
    }
    return inventory, lineage
