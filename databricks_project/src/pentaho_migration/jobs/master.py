"""Python workflow migrated from Pentaho job: Master.

Source: samples/Jobs/Master.kjb

This module is a 1:1 workflow for Master.kjb. It does not flatten or omit hops.
Execution follows Pentaho job semantics:

- enabled hops only
- Start outbound hop → unconditional (Spoon default when XML omits flags)
- TRANS → TRANS / TRANS → SUCCESS hops → on-success
  (evaluation=Y, unconditional=N when XML omits flags; Master.kjb has no
  failure hops and no parameters/variable blocks)
- variables include ``Internal.Job.Filename.Directory`` used in TRANS filenames
- each TRANS entry calls the previously generated transformation module
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    get_logger,
    log_event,
    timed,
)
from pentaho_migration.job_engine import (
    EntryResult,
    JobEntry,
    JobExecutionError,
    JobHop,
    JobRuntime,
    substitute_variables,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# ---------------------------------------------------------------------------
# Exact inventory from Master.kjb (nothing omitted)
# ---------------------------------------------------------------------------

JOB_NAME = "Master"

_LOG = get_logger("Master")
JOB_SOURCE = "samples/Jobs/Master.kjb"

# Master.kjb declares no <parameters> — empty dict preserved intentionally.
JOB_PARAMETERS: dict[str, str] = {}

# Master.kjb declares no <variables> block — only built-ins are seeded at runtime.
JOB_VARIABLE_DEFAULTS: dict[str, str] = {}

ENTRIES: list[JobEntry] = [
    JobEntry(name="Start", entry_type="SPECIAL", is_start=True),
    JobEntry(
        name="Customer Load",
        entry_type="TRANS",
        filename="${Internal.Job.Filename.Directory}/../Transformations/Customer_Load.ktr",
        transname="Customer_Load",
    ),
    JobEntry(
        name="Sales Load",
        entry_type="TRANS",
        filename="${Internal.Job.Filename.Directory}/../Transformations/Sales_Load.ktr",
        transname="Sales_Load",
    ),
    JobEntry(name="Success", entry_type="SUCCESS"),
]

# Hops exactly as in Master.kjb. XML provides only from/to/enabled.
# unconditional / evaluation were omitted in the source — JobHop.kind() applies
# Spoon defaults (Start→unconditional; others→on_success). No failure hops exist
# in this job; a TRANS failure therefore stops the job (no ON_FAILURE successor).
HOPS: list[JobHop] = [
    JobHop(from_name="Start", to_name="Customer Load", enabled=True),
    JobHop(from_name="Customer Load", to_name="Sales Load", enabled=True),
    JobHop(from_name="Sales Load", to_name="Success", enabled=True),
]


def _trans_modules() -> dict[str, Any]:
    """Lazy import of previously generated transformation modules."""
    from pentaho_migration.transformations import customer_load, sales_load

    return {
        "Customer_Load": customer_load,
        "Sales_Load": sales_load,
    }


def _merge_config(
    config: Mapping[str, Any] | None,
    runtime: JobRuntime,
) -> dict[str, Any]:
    """Build the config passed into transformation ``run(spark, config)``.

    Order (later wins for overlapping keys except nested variable maps):
    job parameters → runtime variables → caller config.
    """
    merged: dict[str, Any] = {}
    merged.update(runtime.parameters)
    merged.update({k: v for k, v in runtime.variables.items() if v is not None})
    if config:
        merged.update(dict(config))
    return merged


def _handle_special(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """SPECIAL / Start — always succeeds; seeds no row data."""
    return EntryResult(name=entry.name, success=True, result=True)


def _handle_success(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """SUCCESS entry — marks job completion (PDI JobEntrySuccess)."""
    return EntryResult(name=entry.name, success=True, result=True)


def _handle_trans(
    runtime: JobRuntime,
    entry: JobEntry,
    *,
    spark: SparkSession,
    config: Mapping[str, Any] | None,
) -> EntryResult:
    """TRANS entry — resolve filename variables and call the migrated module."""
    resolved_filename = substitute_variables(entry.filename, runtime.variables)
    transname = entry.transname or Path(resolved_filename).stem
    module = _trans_modules().get(transname)
    if module is None:
        return EntryResult(
            name=entry.name,
            success=False,
            error=JobExecutionError(
                f"No migrated transformation module for '{transname}' "
                f"(entry '{entry.name}', filename='{resolved_filename}')"
            ),
        )

    # Preserve absolute filename resolution side-effect into variables (audit).
    runtime.variables[f"Internal.Entry.{entry.name}.Filename"] = resolved_filename
    runtime.variables[f"Internal.Entry.{entry.name}.TransName"] = transname

    try:
        df = module.run(spark, _merge_config(config, runtime))
        return EntryResult(name=entry.name, success=True, result=df)
    except Exception as exc:  # noqa: BLE001 — entry failure → success=False for hop eval
        return EntryResult(name=entry.name, success=False, error=exc)


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame | bool:
    """Execute Pentaho job ``Master`` with full hop / variable / parameter semantics.

    Parameters
    ----------
    spark:
        Active SparkSession passed through to TRANS modules.
    config:
        Runtime configuration forwarded to transformations. Also accepts:

        - ``Internal.Job.Filename.Directory`` — directory of the .kjb (required
          for faithful filename resolution; default mirrors samples/Jobs)
        - job parameter overrides (none declared in Master.kjb)
        - standard transformation keys (``catalog``, ``schema``, ``data_dir``, …)

    Returns
    -------
    DataFrame | bool
        DataFrame from the last successful TRANS entry when available; otherwise
        the SUCCESS entry boolean result.
    """
    config = dict(config or {})
    if spark is not None:
        apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "job_start", job="Master")

    # Repo root: .../Pentahon_Converter (jobs → pentaho_migration → src → databricks_project → root)
    _repo_root = Path(__file__).resolve().parents[4]
    # Built-in variables PDI sets for jobs — preserve exact names.
    job_dir = config.get(
        "Internal.Job.Filename.Directory",
        str(_repo_root / "samples" / "Jobs"),
    )
    variables: dict[str, Any] = {
        **JOB_VARIABLE_DEFAULTS,
        "Internal.Job.Filename.Directory": job_dir,
        "Internal.Job.Name": JOB_NAME,
        "Internal.Job.Filename.Name": "Master.kjb",
    }
    # Caller may inject additional kettle-style Internal.* variables.
    for key, value in config.items():
        if key.startswith("Internal."):
            variables[key] = value

    # Master.kjb declares no <parameters>; keep empty and allow future overrides.
    parameters = {
        key: str(config.get(key, default)) for key, default in JOB_PARAMETERS.items()
    }

    def handle_trans(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
        return _handle_trans(runtime, entry, spark=spark, config=config)

    runtime = JobRuntime(
        name=JOB_NAME,
        entries=list(ENTRIES),
        hops=list(HOPS),
        parameters=parameters,
        variables=variables,
        handlers={
            "SPECIAL": _handle_special,
            "SUCCESS": _handle_success,
            "TRANS": handle_trans,
        },
    )

    final = runtime.run()
    log_event(
        _LOG,
        "job_end",
        success=final.success,
        last=final.name,
        steps=len(runtime.executed),
    )
    if not final.success:
        raise JobExecutionError(
            f"Job '{JOB_NAME}' finished unsuccessfully at '{final.name}'"
        ) from final.error

    # Prefer last TRANS DataFrame for callers; SUCCESS itself returns bool.
    for name in reversed(runtime.executed):
        result = runtime.results[name]
        # Avoid importing pyspark at module import time.
        if result.result is not None and not isinstance(result.result, bool):
            return result.result
    return True


def run_workflow(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame | bool:
    """Alias for Databricks / notebook entrypoints."""
    return run(spark, config)
