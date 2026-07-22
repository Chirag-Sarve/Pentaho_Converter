"""Project-root bootstrap for generated Master_ETL entrypoints.

Resolves the package root for local scripts, Databricks Repos / Workspace
notebooks, and Job clusters (SparkFiles), configures ``sys.path``, and
validates that required modules are importable.

``Master_ETL.py`` should call::

    from engine.bootstrap import initialize_project

    _ROOT = initialize_project(primary_module="jobs.jb_master")
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
from collections.abc import Sequence
from pathlib import Path

# Master_ETL historically used parents_up=1 (project root = entry file's parent).
_PARENTS_UP = 1


def _workspace_path_variants(path: Path) -> list[Path]:
    """Databricks notebook paths may omit or include the ``/Workspace`` FS prefix."""
    raw = str(path).replace("\\", "/")
    out: list[Path] = []
    seen: set[str] = set()

    def _add(p: Path) -> None:
        key = str(p).replace("\\", "/")
        if key not in seen:
            seen.add(key)
            out.append(p)

    _add(Path(raw))
    if raw.startswith("/Workspace/"):
        _add(Path(raw[len("/Workspace") :]))
    elif raw.startswith(("/Users/", "/Repos/", "/Shared/")):
        _add(Path("/Workspace" + raw))
    return out


def _is_project_root(path: Path) -> bool:
    try:
        return (
            (path / "jobs").is_dir()
            and (path / "engine").is_dir()
            and (path / "config.py").is_file()
        )
    except Exception:
        return False


def _climb_to_project_root(start: Path) -> Path | None:
    cur = start
    for _ in range(8):
        for variant in _workspace_path_variants(cur):
            if _is_project_root(variant):
                return variant
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def _notebook_path() -> str | None:
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        _spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        _nb = (
            DBUtils(_spark)
            .notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        return str(_nb) if _nb else None
    except Exception:
        return None


def _spark_files_root() -> Path | None:
    try:
        from pyspark.files import SparkFiles

        root = Path(SparkFiles.getRootDirectory())
        return root if root.exists() else None
    except Exception:
        return None


def _project_root(*, entry_file: object = "<undefined>") -> Path:
    """Resolve package root for local scripts, Repos, Workspace, and Job clusters."""
    anchors: list[Path] = []

    # 1) __file__ (local / Repos / Workspace Files run as .py)
    if isinstance(entry_file, str) and entry_file != "<undefined>":
        try:
            anchors.append(Path(entry_file).resolve())
        except Exception:
            pass

    # 2) Databricks notebook path (Master_ETL uploaded as a notebook)
    _nb = _notebook_path()
    if _nb:
        anchors.append(Path(str(_nb)))

    # 3) SparkFiles root (job cluster --py-files / distributed artifacts)
    _sf = _spark_files_root()
    if _sf is not None:
        anchors.append(_sf)

    for anchor in anchors:
        # Walk parents_up from the file/notebook, then climb further if needed
        root = anchor
        for _ in range(_PARENTS_UP):
            root = root.parent
        found = _climb_to_project_root(root)
        if found is not None:
            return found
        found = _climb_to_project_root(anchor)
        if found is not None:
            return found

    # Last resort: only accept cwd if it (or an ancestor) is a real project root
    _cwd = Path.cwd()
    found = _climb_to_project_root(_cwd)
    if found is not None:
        return found

    print("ERROR: could not resolve project root containing jobs/, engine/, config.py")
    print("  cwd           =", _cwd)
    print("  sys.path      =", list(sys.path))
    print("  __file__      =", entry_file)
    print("  notebook path =", _nb)
    print("  SparkFiles    =", _sf)
    print("  anchors       =", [str(a) for a in anchors])
    raise ModuleNotFoundError(
        "Project root not found. Upload the full package (Master_ETL.py, config.py, "
        "engine/, jobs/) and run Master_ETL from that folder."
    )


def _diagnose_imports(*, root: Path, entry_file: object = "<undefined>") -> None:
    """Print cwd / sys.path / project root / notebook path when imports fail."""
    _nb = None
    try:
        _nb = _notebook_path()
    except Exception:
        _nb = None
    print("IMPORT DIAGNOSTICS")
    print("  cwd            =", Path.cwd())
    print("  sys.path       =", list(sys.path))
    print("  project root   =", root)
    print("  notebook path  =", _nb)
    print("  __file__       =", entry_file)


def _require_modules(
    names: list[str],
    *,
    root: Path,
    entry_file: object = "<undefined>",
) -> None:
    missing = [n for n in names if importlib.util.find_spec(n) is None]
    if not missing:
        return
    _diagnose_imports(root=root, entry_file=entry_file)
    raise ModuleNotFoundError(
        "Not importable (file may exist but project root is not on sys.path): "
        + ", ".join(missing)
    )


def _normalize_primary_modules(
    primary_module: str | Sequence[str] | None,
) -> list[str]:
    if primary_module is None:
        return []
    if isinstance(primary_module, str):
        return [primary_module] if primary_module else []
    return [m for m in primary_module if m]


def initialize_project(
    primary_module: str | Sequence[str] | None = None,
) -> Path:
    """Resolve project root, configure ``sys.path``, and validate required modules.

    Parameters
    ----------
    primary_module:
        Extra module(s) that must be importable after path setup, e.g.
        ``\"jobs.jb_master\"`` or a sequence of job module paths. ``jobs``,
        ``engine``, and ``config`` are always required.

    Returns
    -------
    Path
        Absolute project root (same value historically exposed as ``_ROOT``).
    """
    # Master_ETL caller globals (``__file__`` is undefined in Databricks notebooks).
    frame = inspect.currentframe()
    try:
        caller = frame.f_back if frame is not None else None
        entry_file = (
            caller.f_globals.get("__file__", "<undefined>")
            if caller is not None
            else "<undefined>"
        )
    finally:
        del frame

    root = _project_root(entry_file=entry_file)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    required = ["jobs", "engine", "config"] + _normalize_primary_modules(primary_module)
    _require_modules(required, root=root, entry_file=entry_file)
    return root
