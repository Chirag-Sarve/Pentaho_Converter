"""Hop-based DAG for Pentaho transformation step ordering."""

from __future__ import annotations

import logging
from collections import defaultdict, deque

from .models import PentahoHop, PentahoStep

logger = logging.getLogger(__name__)


class StepDAG:
    """Directed acyclic graph of transformation steps connected by hops."""

    def __init__(self, steps: list[PentahoStep], hops: list[PentahoHop]) -> None:
        self.steps = {s.name: s for s in steps}
        self.hops = [h for h in hops if h.enabled]
        self._adj: dict[str, set[str]] = defaultdict(set)
        self._in_degree: dict[str, int] = {}
        self._all_nodes: set[str] = set()
        self._build()

    def _build(self) -> None:
        for name in self.steps:
            self._all_nodes.add(name)
            self._in_degree.setdefault(name, 0)

        for hop in self.hops:
            src, dst = hop.from_name, hop.to_name
            if src not in self.steps or dst not in self.steps:
                continue
            if dst not in self._adj[src]:
                self._adj[src].add(dst)
                self._in_degree.setdefault(src, 0)
                self._in_degree[dst] = self._in_degree.get(dst, 0) + 1
                self._all_nodes.add(src)
                self._all_nodes.add(dst)

    def topological_sort(self) -> list[str]:
        """Return step names in dependency order (Kahn's algorithm)."""
        in_deg = dict(self._in_degree)
        queue: deque[str] = deque(
            sorted(n for n in self._all_nodes if in_deg.get(n, 0) == 0)
        )
        result: list[str] = []

        while queue:
            node = queue.popleft()
            result.append(node)
            for neighbor in sorted(self._adj.get(node, [])):
                in_deg[neighbor] -= 1
                if in_deg[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) < len(self._all_nodes):
            remaining = self._all_nodes - set(result)
            logger.warning("Cycle detected in transformation involving: %s", remaining)
            result.extend(sorted(remaining))

        return result

    def predecessors(self, step_name: str) -> list[str]:
        preds: list[str] = []
        for src, dsts in self._adj.items():
            if step_name in dsts:
                preds.append(src)
        return preds

    def successors(self, step_name: str) -> list[str]:
        return sorted(self._adj.get(step_name, []))
