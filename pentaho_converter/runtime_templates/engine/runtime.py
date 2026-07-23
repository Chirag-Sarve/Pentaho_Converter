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
        connections=spec.get("connections") or {},
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
    connections: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run a Pentaho job graph using engine-owned job metadata."""
    import config as _cfg_mod

    cfg = _cfg_mod.merge_config(dict(config_overrides or {}))
    if hasattr(_cfg_mod, "configure_logging"):
        _cfg_mod.configure_logging(cfg)
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

    parent_variables = cfg.get("__parent_variables__")
    root_variables = cfg.get("__root_variables__")
    parent_scopes = cfg.get("__variable_scopes__")

    variables: dict[str, Any] = {
        "Internal.Job.Name": job_name,
        "Internal.Job.Filename.Name": job_source,
        # Default job directory to {data}/jobs so SET_VARIABLES paths like
        # ${Internal.Job.Filename.Directory}/../output → {data}/output
        # (Pentaho Spoon substitutes the real .kjb folder; Databricks has no .kjb path).
        "Internal.Job.Filename.Directory": str(
            cfg.get(
                "Internal.Job.Filename.Directory",
                (str(cfg.get("PENTAHO_DATA_DIR") or "").rstrip("/") + "/jobs")
                if cfg.get("PENTAHO_DATA_DIR")
                else "",
            )
        ),
        "Internal.Entry.Current.Directory": str(
            cfg.get(
                "Internal.Entry.Current.Directory",
                cfg.get(
                    "Internal.Job.Filename.Directory",
                    (str(cfg.get("PENTAHO_DATA_DIR") or "").rstrip("/") + "/jobs")
                    if cfg.get("PENTAHO_DATA_DIR")
                    else "",
                ),
            )
        ),
        **parameters,
    }
    for key, value in cfg.items():
        if str(key).startswith("__"):
            continue
        if str(key).startswith("Internal.") or key in parameters:
            variables[key] = value
        elif isinstance(value, (str, int, float, bool)):
            variables[key] = value

    # Nested JOB: inherit parent values, then allow child parameters/config to override
    if isinstance(parent_variables, dict):
        merged = dict(parent_variables)
        merged.update(variables)
        variables = merged

    if isinstance(root_variables, dict):
        root_ref = root_variables
    else:
        root_ref = variables

    if isinstance(parent_scopes, list) and parent_scopes:
        # Child current scope first, then parent chain (already current→root)
        variable_scopes: list[dict[str, Any]] = [variables, *parent_scopes]
    else:
        variable_scopes = [variables]
        if root_ref is not variables and root_ref not in variable_scopes:
            variable_scopes.append(root_ref)

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
        parent_variables=parent_variables if isinstance(parent_variables, dict) else None,
        root_variables=root_ref,
        variable_scopes=variable_scopes,
    )
    runtime.config = dict(cfg)
    runtime.spark = spark
    # Prefer explicit job connections; allow config override map
    merged_conns = dict(connections or {})
    cfg_conns = cfg.get("connections") or cfg.get("__connections__")
    if isinstance(cfg_conns, Mapping):
        merged_conns.update(dict(cfg_conns))
    runtime.connections = {str(k): dict(v) for k, v in merged_conns.items()}
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
