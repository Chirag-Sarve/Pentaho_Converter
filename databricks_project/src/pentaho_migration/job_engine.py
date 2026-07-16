"""Pentaho job hop / entry execution semantics (PDI-faithful).

Implements success hops, failure hops, unconditional hops, variables, and
parameters without collapsing the job graph into a simplified linear script.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping


_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


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
        """Resolve hop kind using explicit XML flags, else PDI defaults.

        Spoon defaults when attributes are omitted:
        - outbound hops from Start (SPECIAL/start) → unconditional
        - all other hops → on-success (unconditional=N, evaluation=Y)
        """
        if not self.enabled:
            raise ValueError("Disabled hops have no kind")
        if self.unconditional is True:
            return HopKind.UNCONDITIONAL
        if self.unconditional is False:
            if self.evaluation is False:
                return HopKind.ON_FAILURE
            return HopKind.ON_SUCCESS
        # Attributes omitted in XML — apply Spoon defaults.
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


def substitute_variables(text: str, variables: Mapping[str, Any]) -> str:
    """Replace ``${VAR}`` placeholders the way Kettle job entries resolve them."""

    if not text or "${" not in text:
        return text

    def _repl(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        # PDI date formats: ${%yyyy-MM-dd}, ${%yyyyMMdd_HHmmss}
        if key.startswith("%"):
            from datetime import datetime

            fmt = (
                key[1:]
                .replace("yyyy", "%Y")
                .replace("MM", "%m")
                .replace("dd", "%d")
                .replace("HH", "%H")
                .replace("mm", "%M")
                .replace("ss", "%S")
            )
            return datetime.now().strftime(fmt)
        if key in variables and variables[key] is not None:
            return str(variables[key])
        return match.group(0)

    # Multi-pass so ${A} expanded from ${B} can resolve nested refs.
    out = text
    for _ in range(8):
        nxt = _VAR_PATTERN.sub(_repl, out)
        if nxt == out:
            break
        out = nxt
    return out


class JobExecutionError(RuntimeError):
    """Raised when the job aborts or ends without recoverable hops."""


EntryHandler = Callable[["JobRuntime", JobEntry], EntryResult]


class JobRuntime:
    """Execute a Pentaho job graph entry-by-entry following hop evaluation rules."""

    def __init__(
        self,
        *,
        name: str,
        entries: list[JobEntry],
        hops: list[JobHop],
        parameters: Mapping[str, str] | None = None,
        variables: Mapping[str, Any] | None = None,
        handlers: Mapping[str, EntryHandler],
        allow_reentry: bool = True,
    ) -> None:
        self.name = name
        self.entries = {e.name: e for e in entries}
        self.hops = list(hops)
        self.parameters: dict[str, str] = dict(parameters or {})
        self.variables: dict[str, Any] = dict(variables or {})
        self.handlers = dict(handlers)
        self.results: dict[str, EntryResult] = {}
        self.executed: list[str] = []
        self.allow_reentry = allow_reentry

    def outbound(self, entry_name: str) -> list[JobHop]:
        return [h for h in self.hops if h.from_name == entry_name and h.enabled]

    def run(self, *, start_entry: str | None = None) -> EntryResult:
        if start_entry:
            starts = [self.entries[start_entry]]
        else:
            starts = [e for e in self.entries.values() if e.is_start]
        if not starts:
            raise JobExecutionError(
                f"Job '{self.name}' has no Start entry (is_start=Y). "
                "Dummy SPECIAL entries must not be treated as Start."
            )

        queue: list[str] = [starts[0].name]
        # Track in-queue membership to avoid duplicate pending nodes; re-entry after
        # completion is allowed when allow_reentry=True (retry paths).
        pending: set[str] = set(queue)
        terminal: EntryResult | None = None

        while queue:
            name = queue.pop(0)
            pending.discard(name)
            entry = self.entries[name]
            result = self._execute_entry(entry)
            self.results[name] = result
            self.executed.append(name)

            if entry.entry_type.upper() == "SUCCESS" and result.success:
                terminal = result

            if entry.entry_type.upper() == "ABORT":
                raise JobExecutionError(
                    f"Job '{self.name}' aborted at entry '{name}': "
                    f"{(result.error or result.result)}"
                ) from result.error

            for hop in self.outbound(name):
                if hop.fires(
                    entry_succeeded=result.success,
                    from_entry_type=entry.entry_type,
                ):
                    target = hop.to_name
                    already_done = target in self.results
                    if target in pending:
                        continue
                    if already_done and not self.allow_reentry:
                        continue
                    queue.append(target)
                    pending.add(target)

            if not result.success:
                remaining_fireable = [
                    hop
                    for hop in self.outbound(name)
                    if hop.fires(
                        entry_succeeded=False,
                        from_entry_type=entry.entry_type,
                    )
                ]
                if not remaining_fireable:
                    raise JobExecutionError(
                        f"Job '{self.name}' failed at entry '{name}' "
                        f"with no failure/unconditional hop"
                    ) from result.error

        if terminal is not None:
            return terminal
        if not self.executed:
            raise JobExecutionError(f"Job '{self.name}' executed no entries")
        return self.results[self.executed[-1]]

    def _execute_entry(self, entry: JobEntry) -> EntryResult:
        entry_type = entry.entry_type.upper()
        handler = self.handlers.get(entry_type) or self.handlers.get(entry.entry_type)
        if handler is None:
            raise JobExecutionError(
                f"No handler registered for job entry type '{entry.entry_type}' "
                f"(entry '{entry.name}')"
            )
        try:
            return handler(self, entry)
        except JobExecutionError:
            raise
        except Exception as exc:  # noqa: BLE001 — mirror PDI: entry failure becomes result=false
            return EntryResult(name=entry.name, success=False, error=exc)
