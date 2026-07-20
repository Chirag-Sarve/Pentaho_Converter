"""High-level job execution API used by generated job modules."""

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping

from .handlers import build_handlers
from .job_models import entries_from_defs, hops_from_defs
from .job_runtime import JobExecutionError, JobRuntime

TransRunner = Callable[..., Any]


def execute_registered_job(
    job_key: str,
    *,
    spark: Any = None,
    config_overrides: Mapping[str, Any] | None = None,
    trans_runners: Mapping[str, TransRunner] | None = None,
) -> dict[str, Any]:
    """Execute a generated job using its engine-owned graph specification."""
    from .job_specs import JOB_SPECS

    try:
        spec = JOB_SPECS[job_key]
    except KeyError as exc:
        raise KeyError(f"No generated job specification for {job_key!r}") from exc

    return execute_job(
        spark=spark,
        config_overrides=config_overrides,
        job_name=str(spec["name"]),
        job_source=str(spec["source"]),
        job_parameters=spec.get("parameters") or {},
        entry_defs=spec.get("entries") or [],
        hop_defs=spec.get("hops") or [],
        trans_runners=trans_runners or {},
        child_job_modules=spec.get("child_job_modules") or {},
        conversion_todos=spec.get("conversion_todos") or [],
    )


def execute_job(
    *,
    spark: Any = None,
    config_overrides: Mapping[str, Any] | None = None,
    job_name: str,
    job_source: str,
    job_parameters: Mapping[str, Any],
    entry_defs: list[dict[str, Any]],
    hop_defs: list[dict[str, Any]],
    trans_runners: Mapping[str, TransRunner],
    child_job_modules: Mapping[str, tuple[str, str]] | None = None,
    conversion_todos: list[str] | None = None,
) -> dict[str, Any]:
    """Run a Pentaho job graph using engine-owned job metadata."""
    import config as _cfg_mod

    cfg = _cfg_mod.merge_config(dict(config_overrides or {}))
    if spark is not None:
        _cfg_mod.apply_spark_runtime_hints(spark, cfg)
        if hasattr(_cfg_mod, "ensure_data_dir"):
            _cfg_mod.ensure_data_dir(spark, cfg)

    logging.info("Job start: %s (%s)", job_name, job_source)
    for todo in conversion_todos or []:
        logging.warning("CONVERSION TODO | %s", todo)

    parameters = {key: str(default) for key, default in job_parameters.items()}
    for key in list(parameters):
        if key in cfg and cfg[key] not in (None, ""):
            parameters[key] = str(cfg[key])

    variables: dict[str, Any] = {
        "Internal.Job.Name": job_name,
        "Internal.Job.Filename.Name": job_source,
        "Internal.Job.Filename.Directory": str(
            cfg.get("Internal.Job.Filename.Directory", "")
        ),
        "Internal.Entry.Current.Directory": str(
            cfg.get(
                "Internal.Entry.Current.Directory",
                cfg.get("Internal.Job.Filename.Directory", ""),
            )
        ),
        **parameters,
    }
    for key, value in cfg.items():
        if str(key).startswith("Internal.") or key in parameters:
            variables[key] = value
        elif isinstance(value, (str, int, float, bool)):
            variables[key] = value

    entries = entries_from_defs(entry_defs)
    entry_types = {(e.entry_type or "").upper() for e in entries if e.entry_type}
    handlers = build_handlers(
        spark=spark,
        cfg=cfg,
        entry_types=entry_types,
        trans_runners=trans_runners,
        child_job_modules=child_job_modules or {},
    )

    runtime = JobRuntime(
        name=job_name,
        entries=entries,
        hops=hops_from_defs(hop_defs),
        parameters=parameters,
        variables=variables,
        handlers=handlers,
        allow_reentry=True,
    )
    runtime.config = dict(cfg)
    final = runtime.run()
    logging.info(
        "Job end: %s | success=%s | last=%s | steps=%s",
        job_name,
        final.success,
        final.name,
        len(runtime.executed),
    )
    if not final.success:
        # Prefer the original exception so callers see the real root cause
        # (e.g. missing column / bad path) rather than a generic wrapper.
        if final.error is not None:
            raise final.error
        raise JobExecutionError(f"Job {job_name} failed at {final.name}")
    return {
        "job": job_name,
        "success": True,
        "executed": list(runtime.executed),
        "variables": dict(runtime.variables),
        "result": final.result,
    }
