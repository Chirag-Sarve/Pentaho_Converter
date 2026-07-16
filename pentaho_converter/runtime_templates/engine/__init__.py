"""Shared Pentaho job execution engine for generated Databricks projects.

Job modules keep only transformation code and a small ``run`` entry point.
Generated job graph metadata, hop evaluation, entry handlers, and variable
substitution live here so every job file stays readable.
"""

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
    "execute_job",
    "execute_registered_job",
    "substitute_variables",
]
