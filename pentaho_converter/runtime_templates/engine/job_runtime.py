"""Pentaho job graph executor (PDI-faithful hop evaluation)."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from .job_models import EntryResult, JobEntry, JobHop


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
        parent_variables: dict[str, Any] | None = None,
        root_variables: dict[str, Any] | None = None,
        variable_scopes: list[dict[str, Any]] | None = None,
    ) -> None:
        self.name = name
        self.entries = {e.name: e for e in entries}
        self.hops = list(hops)
        self.parameters: dict[str, str] = dict(parameters or {})
        # Prefer a shared dict reference when the caller already owns one
        # (nested JOB inheritance / ROOT_JOB + PARENT_JOB scopes).
        if variables is None:
            self.variables = {}
        elif isinstance(variables, dict) and variable_scopes is not None:
            self.variables = variables
        else:
            self.variables = dict(variables)
        self.parent_variables = parent_variables
        self.root_variables = root_variables if root_variables is not None else self.variables
        # scopes[0] = current job, scopes[-1] = root job (PDI parent chain)
        if variable_scopes is not None:
            self.variable_scopes = list(variable_scopes)
        else:
            self.variable_scopes = [self.variables]
        self.handlers = dict(handlers)
        self.results: dict[str, EntryResult] = {}
        self.executed: list[str] = []
        self.allow_reentry = allow_reentry
        self.config: dict[str, Any] = {}
        # PDI ResultFile list — paths accumulated by ADD_RESULT_FILENAMES / file ops
        self.result_filenames: list[dict[str, Any]] = []
        # Optional Spark session + named DB connections for Conditions entries
        self.spark: Any = None
        self.connections: dict[str, dict[str, Any]] = {}

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
                    # No failure/unconditional hop: surface the original exception
                    # (and its traceback) instead of replacing it with a generic
                    # JobExecutionError that hides the root cause.
                    if result.error is not None:
                        raise result.error
                    raise JobExecutionError(
                        f"Job '{self.name}' failed at entry '{name}' "
                        f"with no failure/unconditional hop"
                    )

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
        except Exception as exc:  # noqa: BLE001
            return EntryResult(name=entry.name, success=False, error=exc)
