"""Shim entrypoint for Master_ETL.kjb workflow.

Prefer::

    from pentaho_migration.jobs.Master_ETL import run
"""

from pentaho_migration.jobs.Master_ETL import (  # noqa: F401
    ENTRIES,
    HOPS,
    JOB_NAME,
    JOB_PARAMETERS,
    JOB_SOURCE,
    run,
    run_workflow,
)

__all__ = [
    "ENTRIES",
    "HOPS",
    "JOB_NAME",
    "JOB_PARAMETERS",
    "JOB_SOURCE",
    "run",
    "run_workflow",
]


if __name__ == "__main__":
    run(None, {})
