"""Generate a multi-file Databricks-ready Python project from parsed Pentaho assets.

Output layout::

    Retail_ETL/
    ├── Master_ETL.py
    ├── config.py
    ├── requirements.txt
    ├── VALIDATION_REPORT.md
    ├── engine/                     # shared Pentaho hop / entry runtime
    │   ├── bootstrap.py            # Master_ETL path / import initialization
    │   ├── runtime.py
    │   ├── handlers.py
    │   ├── job_runtime.py
    │   ├── job_models.py
    │   └── variables.py
    └── jobs/
        ├── Load_Customer_Data.py   # inlined .ktr steps + thin run()
        └── ...
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .code_generator import PySparkCodeGenerator
from .dependency_resolver import (
    build_transformation_index,
    resolve_transformation_reference,
)
from .generation_config import GenerationConfig
from .models import (
    ConversionStats,
    PentahoHop,
    PentahoJob,
    PentahoJobEntry,
    PentahoTransformation,
    ScanResult,
)
from .naming import safe_module_name, safe_package_root


def _templates_dir() -> Path:
    return Path(__file__).resolve().parent / "runtime_templates"


def _project_root_bootstrap(parents_up: int = 1) -> str:
    """Emit notebook-safe ``_ROOT`` / ``sys.path`` bootstrap for generated modules.

    Does **not** assume ``Path.cwd()`` is the project root (Job clusters often start
    in ``/databricks/driver``). Resolve the project root from, in order:
      1. ``__file__`` when running as a normal ``.py`` script / Repo file
      2. the current notebook workspace path (Databricks Workspace / Jobs)
      3. ``SparkFiles.getRootDirectory()`` when the project was distributed to the cluster

    A candidate is accepted only when it contains ``jobs/``, ``engine/``, and
    ``config.py`` (or a parent of the candidate does). Workspace path variants
    with and without the ``/Workspace`` prefix are tried.

    ``parents_up`` is 1 for ``Master_ETL.py`` (project root = file's parent) and
    2 for ``jobs/*.py`` (project root = file's parent's parent).
    """
    if parents_up < 1:
        raise ValueError("parents_up must be >= 1")
    # Use a plain string + placeholder so braces in type hints are not f-string exprs.
    return '''def _workspace_path_variants(path: Path) -> list[Path]:
    """Databricks notebook paths may omit or include the ``/Workspace`` FS prefix."""
    raw = str(path).replace("\\\\", "/")
    out: list[Path] = []
    seen: set[str] = set()

    def _add(p: Path) -> None:
        key = str(p).replace("\\\\", "/")
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


def _project_root() -> Path:
    """Resolve package root for local scripts, Repos, Workspace, and Job clusters."""
    anchors: list[Path] = []

    # 1) __file__ (local / Repos / Workspace Files run as .py)
    try:
        anchors.append(Path(__file__).resolve())
    except NameError:
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
        for _ in range(__PARENTS_UP__):
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
    print("  __file__      =", globals().get("__file__", "<undefined>"))
    print("  notebook path =", _nb)
    print("  SparkFiles    =", _sf)
    print("  anchors       =", [str(a) for a in anchors])
    raise ModuleNotFoundError(
        "Project root not found. Upload the full package (Master_ETL.py, config.py, "
        "engine/, jobs/) and run Master_ETL from that folder."
    )


_ROOT = _project_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
'''.replace("__PARENTS_UP__", str(parents_up))


def _eval_flag(raw: str | None) -> bool | None:
    if raw is None or raw == "":
        return None
    return raw.upper() == "Y"


def _hop_def(hop: PentahoHop) -> dict[str, Any]:
    return {
        "from_name": hop.from_name,
        "to_name": hop.to_name,
        "enabled": hop.enabled,
        "unconditional": hop.unconditional,
        "evaluation": _eval_flag(hop.evaluation),
    }


def _entry_def(entry: PentahoJobEntry) -> dict[str, Any]:
    return {
        "name": entry.name,
        "entry_type": entry.entry_type,
        "filename": entry.filename,
        "transname": entry.transname,
        "jobname": entry.jobname,
        "is_start": entry.is_start,
        "attributes": dict(entry.attributes or {}),
    }


def _job_module_stem(job: PentahoJob) -> str:
    return safe_module_name(job.file_path.stem or job.name)


def _trans_module_stem(trans: PentahoTransformation) -> str:
    return safe_module_name(trans.file_path.stem or trans.name)


def _build_job_index(jobs: dict[str, PentahoJob]) -> dict[str, PentahoJob]:
    index: dict[str, PentahoJob] = {}
    for job in jobs.values():
        index[_job_module_stem(job).lower()] = job
        index[safe_module_name(job.name).lower()] = job
        index[job.file_path.stem.lower()] = job
    return index


def _resolve_job_reference(
    entry: PentahoJobEntry,
    job_index: dict[str, PentahoJob],
) -> PentahoJob | None:
    candidates = [
        entry.jobname,
        Path((entry.filename or "").replace("\\", "/")).stem,
        entry.name,
    ]
    for ref in candidates:
        if not ref:
            continue
        key = safe_module_name(ref).lower()
        if key in job_index:
            return job_index[key]
        stem = Path(str(ref).replace("\\", "/")).stem.lower()
        if stem in job_index:
            return job_index[stem]
    return None


def select_primary_job(
    jobs: dict[str, PentahoJob],
    primary_job_name: str | None,
) -> PentahoJob | None:
    """Pick the root orchestrator job (prefer Master* names / unreferenced roots)."""
    if not jobs:
        return None

    def _is_master(job: PentahoJob) -> bool:
        stem = (job.file_path.stem or job.name).lower()
        name = (job.name or "").lower()
        return stem in {"master_etl", "master"} or stem.startswith("master") or name in {
            "master_etl",
            "master",
        }

    masters = [j for j in jobs.values() if _is_master(j)]
    if masters:
        for job in masters:
            if (job.file_path.stem or "").lower() == "master_etl":
                return job
        return masters[0]

    job_index = _build_job_index(jobs)
    referenced: set[str] = set()
    for job in jobs.values():
        for entry in job.entries:
            if (entry.entry_type or "").upper() != "JOB":
                continue
            child = _resolve_job_reference(entry, job_index)
            if child is not None:
                referenced.add(str(child.file_path))

    roots = [j for j in jobs.values() if str(j.file_path) not in referenced]
    pool = roots or list(jobs.values())

    by_name = {j.name: j for j in jobs.values()}
    if primary_job_name and primary_job_name in by_name:
        return by_name[primary_job_name]
    return pool[0]


def _job_child_order(primary: PentahoJob, job_index: dict[str, PentahoJob]) -> list[PentahoJob]:
    """Walk primary job hops from Start and collect unique child JOB entries in order."""
    by_name = {e.name: e for e in primary.entries}
    starts = [e for e in primary.entries if e.is_start]
    if not starts:
        return []

    hops_from: dict[str, list[PentahoHop]] = {}
    for hop in primary.hops:
        if not hop.enabled:
            continue
        hops_from.setdefault(hop.from_name, []).append(hop)

    ordered: list[PentahoJob] = []
    seen: set[str] = set()
    queue = [starts[0].name]
    visited: set[str] = set()

    while queue:
        name = queue.pop(0)
        if name in visited:
            continue
        visited.add(name)
        entry = by_name.get(name)
        if entry and (entry.entry_type or "").upper() == "JOB":
            child = _resolve_job_reference(entry, job_index)
            if child is not None:
                key = str(child.file_path)
                if key not in seen:
                    seen.add(key)
                    ordered.append(child)
        for hop in hops_from.get(name, []):
            if hop.to_name not in visited:
                queue.append(hop.to_name)
    return ordered


_SPARK_IMPORTS = '''from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, when, expr, count, coalesce, broadcast
from delta.tables import DeltaTable
from pyspark.sql.functions import upper, lower, trim, ltrim, rtrim, initcap, length
from pyspark.sql.functions import substring, round, abs, sqrt, ceil, floor, pow
from pyspark.sql.functions import concat, concat_ws, isnull, regexp_replace, regexp_extract, explode, explode_outer, array
from pyspark.sql.functions import split, element_at, collect_list, from_csv
from pyspark.sql.functions import md5, sha1, sha2, crc32, hex, unhex, soundex, lag, lead, rand, randn
from pyspark.sql.functions import lpad, rpad, greatest, conv, dayofyear, quarter, hour, minute, second
from pyspark.sql.functions import to_date, to_timestamp, datediff, date_add, add_months, date_format
from pyspark.sql.functions import unix_timestamp, from_unixtime, current_date, current_timestamp
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, repeat
from pyspark.sql.functions import row_number, rank, dense_rank, monotonically_increasing_id
from pyspark.sql.functions import countDistinct, first, last, levenshtein, sum as _sum, avg, max as _max, min as _min
from pyspark.sql.functions import stddev_samp, var_samp as variance_samp, to_json, struct
'''


class DatabricksProjectGenerator:
    """Emit a Retail_ETL-style project with readable job modules and KTRs inlined."""

    def __init__(
        self,
        *,
        generation_config: GenerationConfig | None = None,
        code_generator: PySparkCodeGenerator | None = None,
    ) -> None:
        self.generation_config = generation_config or GenerationConfig.defaults()
        self.code_generator = code_generator or PySparkCodeGenerator(
            generation_config=self.generation_config
        )

    def generate(
        self,
        *,
        project_name: str,
        scan: ScanResult,
        jobs: dict[str, PentahoJob],
        transformations: dict[str, PentahoTransformation],
        ordered_transformations: list[PentahoTransformation],
        primary_job_name: str | None,
        stats: ConversionStats,
        logs: list[str],
    ) -> dict[str, str]:
        root = safe_package_root(project_name)
        files: dict[str, str] = {}
        logs.append(f"Generating Databricks project package: {root}/")

        files[f"{root}/config.py"] = self._generate_config(project_name)
        files[f"{root}/requirements.txt"] = self._generate_requirements()
        files.update(self._generate_engine_package(root))

        files[f"{root}/jobs/__init__.py"] = (
            '"""Generated Pentaho jobs — one module per .kjb; transformations inlined."""\n'
        )

        job_index = _build_job_index(jobs)
        trans_index = build_transformation_index(transformations)
        primary = select_primary_job(jobs, primary_job_name)
        files[f"{root}/engine/job_specs.py"] = self._generate_job_specs(
            jobs=jobs,
            job_index=job_index,
            scan=scan,
            trans_index=trans_index,
        )

        # Track which transformations were inlined into at least one job
        inlined_trans_paths: set[str] = set()

        for job in jobs.values():
            stem = _job_module_stem(job)
            rel = f"{root}/jobs/{stem}.py"
            content, used_paths, _ = self._generate_job_module(
                job=job,
                stem=stem,
                scan=scan,
                job_index=job_index,
                trans_index=trans_index,
                transformations=transformations,
                stats=stats,
                logs=logs,
                step_counter=1,
            )
            files[rel] = content
            inlined_trans_paths.update(used_paths)
            logs.append(f"Generated job module (KTRs inlined): {rel}")

        # Orphan KTRs (not referenced by any job)
        orphans = [
            t for t in transformations.values()
            if str(t.file_path) not in inlined_trans_paths
        ]
        if orphans and not jobs:
            # No .kjb files — emit each KTR as a runnable job module so the project stays executable
            for trans in orphans:
                stem = _trans_module_stem(trans)
                rel = f"{root}/jobs/{stem}.py"
                files[rel] = self._generate_orphan_ktr_job(
                    trans=trans,
                    stem=stem,
                    stats=stats,
                    logs=logs,
                    step_counter=1,
                )
                logs.append(f"Generated standalone transformation job module: {rel}")
        elif orphans:
            for trans in orphans:
                msg = (
                    f"Orphan transformation '{trans.name}' ({trans.file_path.name}) "
                    "was not referenced by any job — see VALIDATION_REPORT.md "
                    "(not emitted as a separate Python file)"
                )
                logs.append(msg)
                stats.warnings.append(msg)

        files[f"{root}/Master_ETL.py"] = self._generate_master_etl(
            root=root,
            primary=primary,
            jobs=jobs,
            job_index=job_index,
            transformations=list(transformations.values()),
            ordered_transformations=ordered_transformations,
            logs=logs,
        )
        logs.append(f"Generated entry point: {root}/Master_ETL.py")

        return files

    def _generate_config(self, project_name: str) -> str:
        catalog = self.generation_config.catalog or "workspace"
        schema = self.generation_config.schema or "default"
        data_dir = (
            self.generation_config.data_dir or "/Volumes/workspace/default/rawdata"
        )
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return f'''"""Project configuration for Databricks Free Edition / Workspace.

Generated from Pentaho project: {project_name}
Generated: {ts}

Edit TARGET_CATALOG / TARGET_SCHEMA / PENTAHO_DATA_DIR to match your workspace.

Free Edition: use a Unity Catalog Volume path such as
``/Volumes/workspace/default/pentaho_data`` (Spark cannot write ETL data under
``/Workspace/...``). Paid workspaces may use ``main`` or another catalog.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

# Databricks Unity Catalog targets
TARGET_CATALOG = {catalog!r}
TARGET_SCHEMA = {schema!r}

# Spark-readable/writable data root (UC Volume recommended on Free Edition)
PENTAHO_DATA_DIR = {data_dir!r}

# Optional JDBC / secret-backed connection placeholders (override via widgets / job params)
JDBC_URL = ""
JDBC_USER = ""
JDBC_PASSWORD = ""

DEFAULT_CONFIG: dict[str, Any] = {{
    "TARGET_CATALOG": TARGET_CATALOG,
    "TARGET_SCHEMA": TARGET_SCHEMA,
    "PENTAHO_DATA_DIR": PENTAHO_DATA_DIR,
    "spark_aqe": True,
}}


def merge_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return DEFAULT_CONFIG merged with runtime overrides."""
    cfg = dict(DEFAULT_CONFIG)
    if overrides:
        cfg.update({{k: v for k, v in overrides.items() if v is not None}})
    return cfg


def resolve_data_path(path: str, cfg: Mapping[str, Any] | None = None) -> str:
    """Resolve a Pentaho-relative or absolute path against PENTAHO_DATA_DIR.

    Matches Text File Output remapping: paths under ``output/`` / ``rejects/`` /
    ``logs/`` / ``archive/`` keep that parent folder so CREATE_FOLDER (OUTPUT_PATH)
    and Text File Output deliverables resolve to the same location. Other local
    paths collapse to ``{{PENTAHO_DATA_DIR}}/<basename>``.
    """
    cfg = cfg or {{}}
    base = str(cfg.get("PENTAHO_DATA_DIR") or PENTAHO_DATA_DIR).rstrip("/")
    text = (path or "").strip().replace("\\\\", "/")
    if not text:
        return base

    def _collapse(segments: list[str]) -> list[str]:
        out: list[str] = []
        for seg in segments:
            if not seg or seg == ".":
                continue
            if seg == "..":
                if out:
                    out.pop()
                continue
            out.append(seg)
        return out

    if text.startswith(("dbfs:", "s3://", "abfss://", "wasbs://", "file:")):
        return text
    if text.startswith("/Volumes/"):
        parts = _collapse([p for p in text.split("/")])
        return "/" + "/".join(parts) if parts else text
    if text.startswith("/data/") or text == "/data":
        return base + text[len("/data") :]
    preserved = {{
        "output", "outputs", "rejects", "reject", "logs", "log",
        "archive", "archives", "staging", "export", "exports",
    }}
    cleaned = text
    for token in (
        "${{Internal.Transformation.Filename.Directory}}",
        "${{Internal.Job.Filename.Directory}}",
        "${{Internal.Entry.Current.Directory}}",
    ):
        cleaned = cleaned.replace(token, "")
    while "//" in cleaned:
        cleaned = cleaned.replace("//", "/")
    parts = _collapse([p for p in cleaned.split("/")])
    if not parts:
        return base
    name = parts[-1]
    if len(parts) >= 2 and parts[-2].lower() in preserved:
        return f"{{base}}/{{parts[-2]}}/{{name}}"
    return f"{{base}}/{{name}}"


def ensure_data_dir(spark: Any = None, cfg: Mapping[str, Any] | None = None) -> str:
    """Create the UC volume (if needed) and data directory for Free Edition / paid.

    ``/Volumes/<catalog>/<schema>/<volume>/...`` requires the volume object to
    exist before Spark or dbutils can write. Workspace paths are left alone.
    """
    cfg = cfg or {{}}
    path = str(cfg.get("PENTAHO_DATA_DIR") or PENTAHO_DATA_DIR).rstrip("/") or PENTAHO_DATA_DIR
    if path.startswith("/Volumes/"):
        parts = [p for p in path.split("/") if p]
        # ["Volumes", catalog, schema, volume, ...]
        if len(parts) >= 4:
            catalog, schema, volume = parts[1], parts[2], parts[3]
            try:
                if spark is not None:
                    spark.sql(
                        f"CREATE VOLUME IF NOT EXISTS `{{catalog}}`.`{{schema}}`.`{{volume}}`"
                    )
                    logging.info(
                        "Ensured UC volume %s.%s.%s for data dir %s",
                        catalog,
                        schema,
                        volume,
                        path,
                    )
            except Exception as exc:
                logging.warning(
                    "Could not CREATE VOLUME %s.%s.%s (%s). "
                    "Create it in Catalog Explorer if writes fail.",
                    catalog,
                    schema,
                    volume,
                    exc,
                )
    try:
        from pathlib import Path as _Path

        _Path(path).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        _spark = spark or SparkSession.getActiveSession()
        if _spark is not None:
            DBUtils(_spark).fs.mkdirs(path)
    except Exception as exc:
        logging.warning("ensure_data_dir mkdirs soft-fail for %s: %s", path, exc)
    return path


def apply_spark_runtime_hints(spark: Any, cfg: Mapping[str, Any] | None = None) -> None:
    """Apply optional Spark session hints when the runtime allows them.

    Paid / classic clusters accept most of these keys. Databricks Free Edition
    and serverless often reject them with CONFIG_NOT_AVAILABLE — skip quietly
    so the same generated project runs on both.
    """
    cfg = cfg or {{}}

    def _set(key: str, value: str) -> None:
        try:
            spark.conf.set(key, value)
        except Exception:
            # Free Edition / serverless / restricted runtimes may forbid the key.
            pass

    shuffle = cfg.get("spark_shuffle_partitions")
    if shuffle:
        _set("spark.sql.shuffle.partitions", str(int(shuffle)))
    aqe = cfg.get("spark_aqe", True)
    _set("spark.sql.adaptive.enabled", "true" if aqe else "false")
    _set("spark.databricks.delta.optimizeWrite.enabled", "true")
    _set("spark.databricks.delta.autoCompact.enabled", "true")
'''

    def _generate_requirements(self) -> str:
        return (
            "# Databricks Runtime already provides pyspark / delta-spark.\n"
            "# Pin only extras needed for local syntax checks or CI.\n"
            "pyspark>=3.4.0\n"
            "delta-spark>=2.4.0\n"
        )

    def _generate_engine_package(self, root: str) -> dict[str, str]:
        """Copy shared engine templates into ``{root}/engine/``."""
        engine_dir = _templates_dir() / "engine"
        out: dict[str, str] = {}
        for path in sorted(engine_dir.rglob("*.py")):
            rel = path.relative_to(engine_dir).as_posix()
            out[f"{root}/engine/{rel}"] = path.read_text(encoding="utf-8")
        return out

    def _generate_job_specs(
        self,
        *,
        jobs: dict[str, PentahoJob],
        job_index: dict[str, PentahoJob],
        scan: ScanResult,
        trans_index: dict[str, PentahoTransformation],
    ) -> str:
        """Emit job graph definitions beside the shared execution runtime.

        Job modules deliberately stay free of Pentaho graph metadata so their
        contents read as the transformation logic they execute.  The runtime
        resolves the corresponding specification by module stem.
        """
        specs: dict[str, dict[str, Any]] = {}
        for job in jobs.values():
            child_job_modules: dict[str, tuple[str, str]] = {}
            for entry in job.entries:
                if (entry.entry_type or "").upper() != "JOB":
                    continue
                child = _resolve_job_reference(entry, job_index)
                if child is not None:
                    child_job_modules[entry.name] = (
                        _job_module_stem(child),
                        child.file_path.stem,
                    )

            unsupported_types = sorted(
                {
                    (entry.entry_type or "").upper()
                    for entry in job.entries
                    if entry.entry_type
                    and (entry.entry_type or "").upper()
                    not in {
                        "SPECIAL",
                        "START",
                        "SUCCESS",
                        "ABORT",
                        "TRANS",
                        "JOB",
                        "WRITE_TO_LOG",
                        "MSGBOX_INFO",
                        "PING",
                        "TELNET",
                        "SYSLOG",
                        "SEND_NAGIOS_PASSIVE_CHECK",
                        "SNMP_TRAP",
                        "TRUNCATE_TABLES",
                        "HL7MLLPINPUT",
                        "HL7MLLPACKNOWLEDGE",
                        "CONNECTED_TO_REPOSITORY",
                        "EXPORT_REPOSITORY",
                        "FTP",
                        "FTP_PUT",
                        "FTP_DELETE",
                        "FTPS_GET",
                        "FTPS_PUT",
                        "SFTP",
                        "SFTPPUT",
                        "PGP_ENCRYPT_FILES",
                        "PGP_DECRYPT_FILES",
                        "PGP_VERIFY_FILES",
                        "SET_VARIABLES",
                        "SHELL",
                        "SQL",
                        "EVAL",
                        "CREATE_FOLDER",
                        "CREATE_FILE",
                        "WRITE_TO_FILE",
                        "SIMPLE_EVAL",
                        "DELAY",
                        "WAIT_FOR",
                        "MAIL",
                        "GET_POP",
                        "MAIL_VALIDATOR",
                        "FILE_EXISTS",
                        "FILES_EXIST",
                        "FOLDER_IS_EMPTY",
                        "CHECK_FILES_LOCKED",
                        "WEBSERVICE_AVAILABLE",
                        "TABLE_EXISTS",
                        "COLUMNS_EXIST",
                        "EVAL_TABLE_CONTENT",
                        "EVAL_FILES_METRICS",
                        "WAIT_FOR_SQL",
                        "CHECK_DB_CONNECTIONS",
                        "MYSQL_BULK_FILE",
                        "MYSQL_BULK_LOAD",
                        "MSSQL_BULK_LOAD",
                        "XML_WELL_FORMED",
                        "DTD_VALIDATOR",
                        "XSD_VALIDATOR",
                        "XSLT",
                        "WAIT_FOR_FILE",
                        "ZIP_FILE",
                        "UNZIP",
                        "UNZIP_FILE",
                        "COPY_FILES",
                        "MOVE_FILES",
                        "DELETE_FILE",
                        "DELETE_FILES",
                        "DELETE_FOLDERS",
                        "DELETE_FOLDER",
                        "FILE_COMPARE",
                        "FOLDERS_COMPARE",
                        "DOS_UNIX_CONVERTER",
                        "ADD_RESULT_FILENAMES",
                        "DELETE_RESULT_FILENAMES",
                        "COPY_MOVE_RESULT_FILENAMES",
                        "HTTP",
                        "DUMMY",
                    }
                }
            )
            todos = [
                (
                    f"Job entry type '{entry_type}' has a generic TODO handler — "
                    "verify behaviour against Pentaho"
                )
                for entry_type in unsupported_types
            ]
            for entry in job.entries:
                if (entry.entry_type or "").upper() == "JOB" and entry.name not in child_job_modules:
                    todos.append(
                        f"JOB entry '{entry.name}' references missing "
                        f"job {entry.filename or entry.jobname!r}"
                    )
                if (entry.entry_type or "").upper() == "TRANS" and (
                    resolve_transformation_reference(
                        entry.filename, entry.transname, trans_index, scan
                    )
                    is None
                ):
                    todos.append(
                        f"TRANS entry '{entry.name}' references missing "
                        f"transformation {entry.filename or entry.transname!r}"
                    )

            stem = _job_module_stem(job)
            specs[stem] = {
                "name": job.name,
                "source": job.file_path.name,
                "parameters": dict(job.parameters),
                "connections": dict(job.connections),
                "entries": [_entry_def(entry) for entry in job.entries],
                "hops": [_hop_def(hop) for hop in job.hops],
                "child_job_modules": child_job_modules,
                "conversion_todos": todos,
            }

        return f'''"""Generated Pentaho job specifications consumed by ``engine.runtime``.

This module intentionally contains the parsed job graph metadata. Generated
job modules contain only executable transformation logic and a small ``run``
function that binds their local transformation callables.
"""

from __future__ import annotations

from typing import Any

JOB_SPECS: dict[str, dict[str, Any]] = {specs!r}
'''

    def _generate_orphan_ktr_job(
        self,
        *,
        trans: PentahoTransformation,
        stem: str,
        stats: ConversionStats,
        logs: list[str],
        step_counter: int,
    ) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        block, _, run_name = self.code_generator.generate_inlined_transformation_block(
            trans, stats, logs, step_counter=step_counter, run_func_name=f"run_{stem}"
        )
        return f'''"""Orphan Pentaho transformation emitted as a runnable job module.

Source KTR: {trans.file_path.name}
Generated: {ts}

This .ktr was not referenced by any .kjb in the project ZIP.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Mapping

{_project_root_bootstrap(2)}

{_SPARK_IMPORTS}
import config
from engine.df_guards import log_step_dataframe, require_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TARGET_CATALOG = config.TARGET_CATALOG
TARGET_SCHEMA = config.TARGET_SCHEMA
PENTAHO_DATA_DIR = config.PENTAHO_DATA_DIR

{chr(10).join(block)}

def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Run orphan transformation ``{trans.name}``."""
    import config as _cfg_mod
    cfg = __merge(config)
    if spark is not None:
        _cfg_mod.apply_spark_runtime_hints(spark, cfg)
    return {run_name}(spark, cfg)


def __merge(overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    return config.merge_config(dict(overrides or {{}}))


if __name__ == "__main__":
    _spark = SparkSession.builder.appName({stem!r}).getOrCreate()
    run(_spark, None)
'''

    def _generate_missing_trans_stub_funcs(self, stem: str, reason: str) -> list[str]:
        return [
            f"def run_{stem}(spark, config=None):",
            f'    """TODO: transformation XML was not present in the project ZIP."""',
            "    msg = (",
            f"        'TODO: transformation {stem!r} was referenced by a job but '",
            f"        'no .ktr file was found in the uploaded Pentaho project. ({reason})'",
            "    )",
            "    logging.error(msg)",
            "    raise NotImplementedError(msg)",
            "",
        ]

    def _generate_job_module(
        self,
        *,
        job: PentahoJob,
        stem: str,
        scan: ScanResult,
        job_index: dict[str, PentahoJob],
        trans_index: dict[str, PentahoTransformation],
        transformations: dict[str, PentahoTransformation],
        stats: ConversionStats,
        logs: list[str],
        step_counter: int,
    ) -> tuple[str, set[str], int]:
        # entry_name → (run_func_name, ktr_stem_or_ref)
        trans_runners: dict[str, tuple[str, str]] = {}
        used_trans_paths: set[str] = set()
        inlined_blocks: list[str] = []
        counter = step_counter
        seen_run_funcs: set[str] = set()

        for entry in job.entries:
            et = (entry.entry_type or "").upper()
            if et == "TRANS":
                trans = resolve_transformation_reference(
                    entry.filename, entry.transname, trans_index, scan
                )
                if trans is None:
                    ref = entry.transname or Path(
                        (entry.filename or "").replace("\\", "/")
                    ).stem or entry.name
                    tstem = safe_module_name(ref)
                    reason = (
                        f"TRANS entry '{entry.name}' references missing "
                        f"transformation {entry.filename or entry.transname!r}"
                    )
                    run_fn = f"run_{tstem}"
                    if run_fn not in seen_run_funcs:
                        inlined_blocks.extend(
                            self._generate_missing_trans_stub_funcs(tstem, reason)
                        )
                        seen_run_funcs.add(run_fn)
                    trans_runners[entry.name] = (run_fn, Path(str(ref)).stem)
                    continue

                tstem = _trans_module_stem(trans)
                run_fn = f"run_{tstem}"
                used_trans_paths.add(str(trans.file_path))
                if run_fn not in seen_run_funcs:
                    block, counter, run_fn = (
                        self.code_generator.generate_inlined_transformation_block(
                            trans,
                            stats,
                            logs,
                            step_counter=counter,
                            run_func_name=run_fn,
                        )
                    )
                    inlined_blocks.extend(block)
                    seen_run_funcs.add(run_fn)
                trans_runners[entry.name] = (run_fn, trans.file_path.stem)

            elif et == "JOB":
                continue

        inlined_src = "\n".join(inlined_blocks)

        body = f'''"""Databricks job for {job.name}."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Mapping

{_project_root_bootstrap(2)}

{_SPARK_IMPORTS}
import config
from engine.runtime import execute_registered_job
from engine.df_guards import log_step_dataframe, require_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger({stem!r})

TARGET_CATALOG = config.TARGET_CATALOG
TARGET_SCHEMA = config.TARGET_SCHEMA
PENTAHO_DATA_DIR = config.PENTAHO_DATA_DIR

{inlined_src}


def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Run the {job.name!r} transformation flow."""
    return execute_registered_job(
        {stem!r},
        spark=spark,
        config_overrides=config,
        trans_runners={{
            {", ".join(f"{entry_name!r}: {run_fn}" for entry_name, (run_fn, _) in trans_runners.items())}
        }},
    )


if __name__ == "__main__":
    _spark = SparkSession.builder.appName({job.name!r}).getOrCreate()
    run(_spark, None)
'''
        return body, used_trans_paths, counter

    def _generate_master_etl(
        self,
        *,
        root: str,
        primary: PentahoJob | None,
        jobs: dict[str, PentahoJob],
        job_index: dict[str, PentahoJob],
        transformations: list[PentahoTransformation],
        ordered_transformations: list[PentahoTransformation],
        logs: list[str],
    ) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        # (module_path, alias) e.g. ("jobs.jb_prep", "jb_prep_job")
        job_imports: list[tuple[str, str]] = []
        call_lines: list[str] = []
        primary_name = "empty"
        source = "n/a"

        if primary is not None:
            # Always execute the primary job module. Nested JOB entries are run by
            # JobRuntime via child_job_modules — flattening to children-only skips
            # every TRANS / utility entry that follows a nested job in the primary.
            primary_name = primary.name
            source = primary.file_path.name
            pstem = _job_module_stem(primary)
            job_imports.append((f"jobs.{pstem}", "_run_primary"))
            call_lines.append(
                f'    logging.info("Master_ETL starting primary job: %s (%s)", '
                f"{primary_name!r}, {source!r})"
            )
            call_lines.append("    result = _run_primary(spark, cfg)")
            call_lines.append(
                f'    logging.info("Master_ETL completed primary job: %s", {primary_name!r})'
            )
            call_lines.append("    return result")
        elif ordered_transformations or transformations:
            # No jobs — orchestrate orphan KTR job modules in order
            pool = ordered_transformations or transformations
            primary_name = "standalone_transformations"
            source = "jobs/* (from .ktr)"
            for t in pool:
                tstem = _trans_module_stem(t)
                alias = f"{tstem.lower()}_job"
                job_imports.append((f"jobs.{tstem}", alias))
                call_lines.append(f'    logging.info("Running transformation job: {t.name}")')
                call_lines.append(f"    _last = {alias}(spark, cfg)")
            call_lines.append("    return _last")
        else:
            call_lines.append(
                '    raise RuntimeError("No jobs or transformations were generated.")'
            )

        # initialize_project always requires jobs/engine/config; pass job modules only.
        primary_modules = [mod for mod, _ in job_imports]
        if len(primary_modules) == 0:
            init_call = "initialize_project()"
        elif len(primary_modules) == 1:
            init_call = f"initialize_project(primary_module={primary_modules[0]!r})"
        else:
            init_call = f"initialize_project(primary_module={primary_modules!r})"

        safe_import_lines: list[str] = []
        for mod, alias in job_imports:
            # from jobs.jb_prep import run as jb_prep_job — with path recovery
            safe_import_lines.append(
                f"""try:
    from {mod} import run as {alias}
except ModuleNotFoundError:
    import sys
    from pathlib import Path
    try:
        root = Path(__file__).resolve().parent
    except NameError:
        root = _ROOT
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))
    from {mod} import run as {alias}"""
            )
        if not safe_import_lines:
            import_block = "# (no job imports)"
        else:
            import_block = "\n\n".join(safe_import_lines)

        run_body = "\n".join(call_lines)
        job_list = ", ".join(sorted(_job_module_stem(j) for j in jobs.values())) or "(none)"

        return f'''"""Master_ETL - Databricks entry point for the migrated Pentaho project.

Generated: {ts}
Primary job: {primary_name}
Source: {source}

Upload this entire ``{root}/`` folder to Databricks Workspace / Repos and run::

    Master_ETL.py

Jobs: {job_list}
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

from engine.bootstrap import initialize_project

_ROOT = {init_call}

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("Master_ETL")

{import_block}


def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Execute jobs in Pentaho order (success/failure hops preserved inside each job)."""
    import config as _cfg_mod

    cfg = _cfg_mod.merge_config(dict(config or {{}}))
    if spark is not None:
        _cfg_mod.apply_spark_runtime_hints(spark, cfg)
    logging.info("Master_ETL start | package=%s | primary=%s", {root!r}, {primary_name!r})
{run_body}


def run_workflow(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Alias used by some Databricks job wrappers."""
    return run(spark, config)


def main() -> Any:
    """Local / job-cluster entry when executed as a script."""
    from pyspark.sql import SparkSession

    owns = False
    spark = globals().get("spark")
    if spark is None:
        spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("Master_ETL").getOrCreate()
        owns = True
    try:
        return run(spark, None)
    finally:
        if owns:
            spark.stop()


if __name__ == "__main__":
    main()
'''
