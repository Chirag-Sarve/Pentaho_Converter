"""One Python workflow module per Pentaho .kjb job."""

from __future__ import annotations

import importlib

__all__ = ["master", "Master_ETL"]


def __getattr__(name: str):
    if name == "master":
        return importlib.import_module("pentaho_migration.jobs.master")
    if name == "Master_ETL":
        return importlib.import_module("pentaho_migration.jobs.Master_ETL")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
