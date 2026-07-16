"""Resolve dependencies between Pentaho jobs and transformations."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from pathlib import Path

from .models import PentahoHop, PentahoJob, PentahoJobEntry, PentahoTransformation, ScanResult

logger = logging.getLogger(__name__)


def _normalize_name(path_or_name: str) -> str:
    """Return lowercase stem for matching transformation references."""
    name = Path(path_or_name.replace("\\", "/")).name
    if name.lower().endswith(".ktr"):
        name = name[:-4]
    return name.lower()


def build_transformation_index(
    transformations: dict[str, PentahoTransformation],
) -> dict[str, PentahoTransformation]:
    """Build lookup by normalized name and filename stem."""
    index: dict[str, PentahoTransformation] = {}
    for trans in transformations.values():
        index[_normalize_name(trans.name)] = trans
        index[_normalize_name(trans.file_path.name)] = trans
        index[_normalize_name(str(trans.file_path))] = trans
    return index


def resolve_transformation_reference(
    reference: str,
    transname: str,
    index: dict[str, PentahoTransformation],
    scan: ScanResult,
) -> PentahoTransformation | None:
    """Locate a transformation referenced from a job entry."""
    candidates = []
    if reference:
        candidates.append(reference)
    if transname:
        candidates.append(transname)

    for ref in candidates:
        key = _normalize_name(ref)
        if key in index:
            return index[key]

    ref_stem = _normalize_name(reference or transname)
    for path in scan.transformation_files:
        if _normalize_name(path.name) == ref_stem or _normalize_name(path.stem) == ref_stem:
            return index.get(_normalize_name(path.name))

    return None


class JobExecutionPlan:
    """Ordered list of transformations to execute for a job."""

    def __init__(self, job: PentahoJob) -> None:
        self.job = job
        self.ordered_entries: list[PentahoJobEntry] = []
        self.missing_references: list[str] = []
        self._build_order()

    def _build_order(self) -> None:
        entries_by_name = {e.name: e for e in self.job.entries}
        hops = [h for h in self.job.hops if h.enabled]

        adj: dict[str, set[str]] = defaultdict(set)
        in_degree: dict[str, int] = {e.name: 0 for e in self.job.entries}

        for hop in hops:
            if hop.from_name in entries_by_name and hop.to_name in entries_by_name:
                if hop.to_name not in adj[hop.from_name]:
                    adj[hop.from_name].add(hop.to_name)
                    in_degree[hop.to_name] = in_degree.get(hop.to_name, 0) + 1

        queue: deque[str] = deque()
        start_entries = [e.name for e in self.job.entries if e.is_start]
        if start_entries:
            queue.extend(sorted(start_entries))
        else:
            queue.extend(sorted(n for n, d in in_degree.items() if d == 0))

        visited: set[str] = set()
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            entry = entries_by_name.get(node)
            if entry:
                self.ordered_entries.append(entry)
            for neighbor in sorted(adj.get(node, [])):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] <= 0:
                    queue.append(neighbor)

        for entry in self.job.entries:
            if entry.name not in visited:
                self.ordered_entries.append(entry)


def resolve_job_dependencies(
    job: PentahoJob,
    scan: ScanResult,
    transformations: dict[str, PentahoTransformation],
    logs: list[str],
) -> tuple[list[PentahoTransformation], list[str]]:
    """Return transformations referenced by *job* in execution order."""
    index = build_transformation_index(transformations)
    plan = JobExecutionPlan(job)
    ordered: list[PentahoTransformation] = []
    missing: list[str] = []
    seen: set[str] = set()

    for entry in plan.ordered_entries:
        if entry.entry_type.upper() != "TRANS":
            continue
        ref = entry.filename or entry.transname
        trans = resolve_transformation_reference(
            entry.filename, entry.transname, index, scan
        )
        if trans is None:
            missing.append(ref or entry.name)
            logs.append(
                f"WARNING: Job '{job.name}' references missing transformation: "
                f"{ref or entry.name}"
            )
            continue
        key = str(trans.file_path)
        if key not in seen:
            ordered.append(trans)
            seen.add(key)

    if missing:
        logs.append(f"Missing references in job '{job.name}': {', '.join(missing)}")

    return ordered, missing


def detect_circular_job_hops(job: PentahoJob) -> bool:
    """Return True if the job hop graph contains a cycle."""
    adj: dict[str, set[str]] = defaultdict(set)
    nodes: set[str] = set()
    for hop in job.hops:
        if hop.enabled:
            adj[hop.from_name].add(hop.to_name)
            nodes.add(hop.from_name)
            nodes.add(hop.to_name)

    visited: set[str] = set()
    stack: set[str] = set()

    def dfs(node: str) -> bool:
        visited.add(node)
        stack.add(node)
        for neighbor in adj.get(node, []):
            if neighbor not in visited:
                if dfs(neighbor):
                    return True
            elif neighbor in stack:
                return True
        stack.remove(node)
        return False

    for node in nodes:
        if node not in visited and dfs(node):
            return True
    return False


def resolve_project(
    scan: ScanResult,
    jobs: dict[str, PentahoJob],
    transformations: dict[str, PentahoTransformation],
    logs: list[str],
) -> tuple[list[PentahoTransformation], str | None]:
    """Determine conversion order for the entire project.

    Returns (ordered_transformations, primary_job_name).
    """
    logs.append("Dependency resolution started.")

    if jobs:
        # Prefer Master / Master_ETL as the project root when present.
        primary_job = next(iter(jobs.values()))
        for job in jobs.values():
            stem = (job.file_path.stem or job.name).lower()
            if stem == "master_etl":
                primary_job = job
                break
            if stem == "master" or stem.startswith("master"):
                primary_job = job
        if len(jobs) > 1:
            logs.append(
                f"Multiple jobs found ({len(jobs)}); using '{primary_job.name}' as primary workflow."
            )

        if detect_circular_job_hops(primary_job):
            logs.append(
                f"WARNING: Circular dependency detected in job '{primary_job.name}'."
            )

        ordered, _ = resolve_job_dependencies(primary_job, scan, transformations, logs)

        for job in jobs.values():
            if job.name == primary_job.name:
                continue
            extra, _ = resolve_job_dependencies(job, scan, transformations, logs)
            for trans in extra:
                if trans not in ordered:
                    ordered.append(trans)

        for trans in transformations.values():
            if trans not in ordered:
                ordered.append(trans)

        logs.append("Dependency resolution completed.")
        return ordered, primary_job.name

    logs.append("No jobs found; converting each transformation independently.")
    logs.append("Dependency resolution completed.")
    return list(transformations.values()), None
