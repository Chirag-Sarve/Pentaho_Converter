"""Shared Pentaho job execution engine for generated Databricks projects.

Job modules keep only transformation code and a small ``run`` entry point.
Generated job graph metadata, hop evaluation, entry handlers, and variable
substitution live here so every job file stays readable.
"""

from .df_guards import (
    describe_dataframe,
    log_exception_diagnostics,
    log_step_dataframe,
    require_dataframe,
)
from .handlers import build_handlers
from .job_models import EntryResult, HopKind, JobEntry, JobHop
from .job_runtime import EntryHandler, JobExecutionError, JobRuntime
from .runtime import execute_job, execute_registered_job
from .variables import substitute_variables

__all__ = [
    "EntryHandler",
    "EntryResult",
    "HopKind",
    "JobEntry",
    "JobExecutionError",
    "JobHop",
    "JobRuntime",
    "build_handlers",
    "describe_dataframe",
    "execute_job",
    "execute_registered_job",
    "log_exception_diagnostics",
    "log_step_dataframe",
    "require_dataframe",
    "substitute_variables",
]
