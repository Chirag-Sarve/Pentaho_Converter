"""Pentaho job graph models (entries, hops, results)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class HopKind(str, Enum):
    """How a job hop is evaluated after an entry finishes."""

    UNCONDITIONAL = "unconditional"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"


@dataclass(frozen=True)
class JobHop:
    """One directed hop from a .kjb ``<hop>`` element."""

    from_name: str
    to_name: str
    enabled: bool = True
    unconditional: bool | None = None
    evaluation: bool | None = None  # True = success, False = failure (when not unconditional)

    def kind(self, *, from_entry_type: str) -> HopKind:
        """Resolve hop kind using explicit XML flags, else PDI defaults."""
        if not self.enabled:
            raise ValueError("Disabled hops have no kind")
        if self.unconditional is True:
            return HopKind.UNCONDITIONAL
        if self.unconditional is False:
            if self.evaluation is False:
                return HopKind.ON_FAILURE
            return HopKind.ON_SUCCESS
        if from_entry_type.upper() in {"SPECIAL", "START"}:
            return HopKind.UNCONDITIONAL
        if self.evaluation is False:
            return HopKind.ON_FAILURE
        return HopKind.ON_SUCCESS

    def fires(self, *, entry_succeeded: bool, from_entry_type: str) -> bool:
        if not self.enabled:
            return False
        kind = self.kind(from_entry_type=from_entry_type)
        if kind is HopKind.UNCONDITIONAL:
            return True
        if kind is HopKind.ON_SUCCESS:
            return entry_succeeded
        return not entry_succeeded


@dataclass
class JobEntry:
    """One ``<entry>`` from a .kjb file."""

    name: str
    entry_type: str
    filename: str = ""
    transname: str = ""
    jobname: str = ""
    is_start: bool = False
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class EntryResult:
    """Outcome of executing one job entry (mirrors PDI entry result)."""

    name: str
    success: bool
    result: Any = None
    error: BaseException | None = None


def entries_from_defs(entry_defs: list[dict[str, Any]]) -> list[JobEntry]:
    """Build ``JobEntry`` objects from generated ENTRY_DEFS metadata."""
    return [
        JobEntry(
            name=d["name"],
            entry_type=d["entry_type"],
            filename=d.get("filename", ""),
            transname=d.get("transname", ""),
            jobname=d.get("jobname", ""),
            is_start=bool(d.get("is_start")),
            attributes=dict(d.get("attributes") or {}),
        )
        for d in entry_defs
    ]


def hops_from_defs(hop_defs: list[dict[str, Any]]) -> list[JobHop]:
    """Build ``JobHop`` objects from generated HOP_DEFS metadata."""
    return [
        JobHop(
            from_name=h["from_name"],
            to_name=h["to_name"],
            enabled=bool(h.get("enabled", True)),
            unconditional=h.get("unconditional"),
            evaluation=h.get("evaluation"),
        )
        for h in hop_defs
    ]
