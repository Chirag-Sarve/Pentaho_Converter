"""Reusable Pentaho job-entry handlers (SPECIAL, TRANS, SET_VARIABLES, …)."""

from __future__ import annotations

import importlib
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .df_guards import log_exception_diagnostics
from .job_models import EntryResult, JobEntry
from .job_runtime import EntryHandler, JobExecutionError, JobRuntime
from .variables import substitute_variables

TransRunner = Callable[..., Any]


def _resolve(text: str, runtime: JobRuntime) -> str:
    return substitute_variables(text or "", runtime.variables)


def _data_path(raw: str, runtime: JobRuntime) -> str:
    """Map Pentaho local paths to PENTAHO_DATA_DIR (same rules as Text File Output)."""
    text = _resolve(raw, runtime)
    try:
        import config as _cfg  # type: ignore

        cfg = getattr(runtime, "config", None)
        return _cfg.resolve_data_path(text, cfg if isinstance(cfg, Mapping) else None)
    except Exception:
        return text


def _fs_exists(path: str) -> bool:
    """True if path exists as a file, folder, or Spark write directory (_SUCCESS)."""
    if not path:
        return False
    try:
        p = Path(path)
        if p.exists():
            return True
        if (p / "_SUCCESS").exists():
            return True
    except Exception:
        pass
    # Databricks DBFS / Volumes via dbutils when local Path cannot see the object
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            DBUtils(spark).fs.ls(path)
            return True
    except Exception:
        pass
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            # Spark CSV/Parquet outputs are directories; listing via Hadoop FS
            jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)  # type: ignore[attr-defined]
            fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())  # type: ignore[attr-defined]
            if fs.exists(jvm_path):
                return True
    except Exception:
        pass
    return False


def _log_level(name: str) -> int:
    return {
        "ERROR": logging.ERROR,
        "NOTHING": logging.CRITICAL,
        "MINIMAL": logging.WARNING,
        "BASIC": logging.INFO,
        "DETAILED": logging.DEBUG,
        "DEBUG": logging.DEBUG,
        "ROWLEVEL": logging.DEBUG,
    }.get((name or "BASIC").upper(), logging.INFO)


def handle_special(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    return EntryResult(name=entry.name, success=True, result=True)


def handle_success(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    logging.info("ENTRY SUCCESS | name=%s", entry.name)
    return EntryResult(name=entry.name, success=True, result=True)


def handle_abort(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    msg = _resolve(entry.attributes.get("message", "Abort"), runtime)
    logging.error("ENTRY ABORT | name=%s | %s", entry.name, msg)
    return EntryResult(
        name=entry.name,
        success=False,
        result=msg,
        error=JobExecutionError(msg),
    )


def handle_write_to_log(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    msg = _resolve(entry.attributes.get("logmessage", ""), runtime)
    level = _log_level(entry.attributes.get("loglevel", "BASIC"))
    if str(entry.attributes.get("displayHeader", "Y")).upper() == "Y":
        logging.log(level, "======== Write to log: %s ========", entry.name)
    logging.log(level, "%s", msg)
    return EntryResult(name=entry.name, success=True, result=msg)


def handle_set_variables(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    replace = str(entry.attributes.get("replace", "Y")).upper() == "Y"
    applied: dict[str, str] = {}
    for field in entry.attributes.get("fields") or []:
        vname = field.get("variable_name") or ""
        raw = field.get("variable_string") or ""
        if not vname:
            continue
        if re.search(r"\$\{[^}]+\}\s*\+\s*\d+", raw) or raw.replace(" ", "").endswith("+1"):
            left = _resolve(re.sub(r"\s*\+\s*\d+\s*$", "", raw), runtime)
            m_add = re.search(r"\+\s*(\d+)\s*$", raw)
            add = int(m_add.group(1)) if m_add else 1
            try:
                value = str(int(float(left)) + add)
            except ValueError:
                value = _resolve(raw, runtime)
        else:
            value = _resolve(raw, runtime)
        if replace or vname not in runtime.variables:
            runtime.variables[vname] = value
            runtime.parameters[vname] = value
            applied[vname] = value
    logging.info("ENTRY SET_VARIABLES | name=%s | applied=%s", entry.name, applied)
    return EntryResult(name=entry.name, success=True, result=applied)


def handle_shell(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    work = _resolve(entry.attributes.get("work_directory", ""), runtime)
    script = entry.attributes.get("script", "") or entry.attributes.get("filename", "")
    expanded = _resolve(str(script), runtime)
    for key, val in runtime.variables.items():
        expanded = expanded.replace("%" + str(key) + "%", str(val))
    logging.info("ENTRY SHELL | name=%s | cwd=%s | script=%s", entry.name, work, expanded)
    print(expanded)
    return EntryResult(name=entry.name, success=True, result=expanded)


def handle_create_folder(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    folder = _data_path(entry.attributes.get("foldername", ""), runtime)
    fail_if_exists = str(entry.attributes.get("fail_of_folder_exists", "N")).upper() == "Y"
    if _fs_exists(folder) and fail_if_exists:
        return EntryResult(
            name=entry.name,
            success=False,
            error=FileExistsError(f"Folder already exists: {folder}"),
        )
    try:
        path = Path(folder)
        path.mkdir(parents=True, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        # Volumes / DBFS mkdir via dbutils when local Path fails (common on serverless)
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is not None:
                DBUtils(spark).fs.mkdirs(folder)
            else:
                raise exc
        except Exception as exc2:  # noqa: BLE001
            logging.warning(
                "CREATE_FOLDER soft-fail | name=%s | folder=%s | err=%s",
                entry.name,
                folder,
                exc2,
            )
            return EntryResult(name=entry.name, success=True, result=str(folder))
    logging.info("ENTRY CREATE_FOLDER | name=%s | folder=%s", entry.name, folder)
    return EntryResult(name=entry.name, success=True, result=str(folder))


def handle_file_exists(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    filename = _data_path(
        entry.filename or entry.attributes.get("filename", ""),
        runtime,
    )
    exists = _fs_exists(filename)
    logging.info(
        "ENTRY FILE_EXISTS | name=%s | file=%s | exists=%s",
        entry.name,
        filename,
        exists,
    )
    return EntryResult(name=entry.name, success=exists, result=exists)


def handle_wait_for_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    filename = _data_path(
        entry.filename or entry.attributes.get("filename", ""),
        runtime,
    )
    timeout = int(entry.attributes.get("maximumTimeout", "0") or 0)
    cycle = int(entry.attributes.get("checkCycleTime", "1") or 1)
    success_on_timeout = str(entry.attributes.get("successOnTimeout", "N")).upper() == "Y"
    deadline = time.time() + timeout
    while True:
        if _fs_exists(filename):
            return EntryResult(name=entry.name, success=True, result=filename)
        if time.time() >= deadline:
            if success_on_timeout:
                return EntryResult(name=entry.name, success=True, result=None)
            return EntryResult(
                name=entry.name,
                success=False,
                error=TimeoutError(f"File not found before timeout: {filename}"),
            )
        time.sleep(max(cycle, 1))


def handle_simple_eval(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    varname = str(entry.attributes.get("variablename", "") or "")
    compare_raw = entry.attributes.get("comparevalue", "")
    fieldtype = str(entry.attributes.get("fieldtype", "STRING")).upper()
    num_cond = str(entry.attributes.get("successnumbercondition", "EQUAL")).upper()
    left_expr = varname if varname.startswith("${") else ("${" + varname + "}")
    left = _resolve(left_expr, runtime)
    right = _resolve(str(compare_raw), runtime)
    ok = False
    if fieldtype == "NUMBER":
        try:
            lv, rv = float(left), float(right)
        except ValueError:
            return EntryResult(
                name=entry.name,
                success=False,
                error=ValueError(f"SIMPLE_EVAL non-numeric: {left!r} vs {right!r}"),
            )
        if num_cond == "SMALLER":
            ok = lv < rv
        elif num_cond == "SMALLEREQUAL":
            ok = lv <= rv
        elif num_cond == "GREATER":
            ok = lv > rv
        elif num_cond == "GREATEREQUAL":
            ok = lv >= rv
        else:
            ok = lv == rv
    else:
        ok = left == right
    logging.info("ENTRY SIMPLE_EVAL | name=%s → %s", entry.name, ok)
    return EntryResult(name=entry.name, success=ok, result=ok)


def handle_delay(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    timeout = int(entry.attributes.get("maximumTimeout", "0") or 0)
    scale = int(entry.attributes.get("scaletime", "0") or 0)
    seconds = timeout * (60 if scale == 1 else 3600 if scale == 2 else 1)
    logging.info("ENTRY DELAY | name=%s | sleep=%ss", entry.name, seconds)
    time.sleep(seconds)
    return EntryResult(name=entry.name, success=True, result=seconds)


def handle_mail(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    enabled = str(
        runtime.variables.get("MAIL_ENABLED", runtime.parameters.get("MAIL_ENABLED", "N"))
    ).upper()
    subject = _resolve(entry.attributes.get("subject", ""), runtime)
    dest = _resolve(entry.attributes.get("destination", ""), runtime)
    comment = _resolve(entry.attributes.get("comment", ""), runtime)
    if enabled != "Y":
        logging.warning(
            "ENTRY MAIL | name=%s | MAIL_ENABLED=%s — NOT sent | to=%s | subject=%s",
            entry.name,
            enabled,
            dest,
            subject,
        )
        return EntryResult(
            name=entry.name,
            success=True,
            result={"sent": False, "subject": subject, "to": dest, "comment": comment},
        )
    logging.info("ENTRY MAIL | name=%s | to=%s | subject=%s", entry.name, dest, subject)
    return EntryResult(
        name=entry.name,
        success=True,
        result={"sent": True, "to": dest, "subject": subject, "comment": comment},
    )


def handle_zip_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    zipname = _resolve(entry.attributes.get("zipfilename", ""), runtime)
    logging.info("ENTRY ZIP_FILE | name=%s | zip=%s", entry.name, zipname)
    return EntryResult(name=entry.name, success=True, result=zipname)


def handle_copy_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    src = _resolve(entry.attributes.get("source_filefolder", ""), runtime)
    dst = _resolve(entry.attributes.get("destination_filefolder", ""), runtime)
    logging.info("ENTRY COPY_FILES | name=%s | src=%s | dst=%s", entry.name, src, dst)
    return EntryResult(name=entry.name, success=True, result={"src": src, "dst": dst})


def handle_delete_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    filename = _data_path(
        entry.filename or entry.attributes.get("filename", ""),
        runtime,
    )
    logging.info("ENTRY DELETE_FILE | name=%s | file=%s", entry.name, filename)
    try:
        Path(filename).unlink(missing_ok=True)
    except Exception as exc:  # noqa: BLE001
        logging.warning("DELETE_FILE soft-fail | %s | %s", filename, exc)
    return EntryResult(name=entry.name, success=True, result=filename)


def handle_todo(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    msg = (
        f"TODO: unsupported or partially mapped job entry type "
        f"{entry.entry_type!r} at entry {entry.name!r}"
    )
    logging.error(msg)
    return EntryResult(name=entry.name, success=False, error=NotImplementedError(msg))


def make_trans_handler(
    *,
    spark: Any,
    cfg: Mapping[str, Any],
    trans_runners: Mapping[str, TransRunner],
) -> EntryHandler:
    """Build a TRANS handler bound to this job's inlined transformation callables."""

    def handle_trans(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
        runner = trans_runners.get(entry.name)
        if runner is None or not callable(runner):
            return EntryResult(
                name=entry.name,
                success=False,
                error=JobExecutionError(f"No TRANS runner for {entry.name}"),
            )
        try:
            child_cfg = dict(cfg)
            child_cfg.update({k: str(v) for k, v in runtime.variables.items()})
            logging.info("Running transformation entry: %s", entry.name)
            df = runner(spark, child_cfg)
            logging.info("TRANS OK | entry=%s", entry.name)
            return EntryResult(name=entry.name, success=True, result=df)
        except Exception as exc:  # noqa: BLE001
            log_exception_diagnostics(entry_name=entry.name, exc=exc, kind="TRANS")
            return EntryResult(name=entry.name, success=False, error=exc)

    return handle_trans


def make_job_handler(
    *,
    spark: Any,
    cfg: Mapping[str, Any],
    child_job_modules: Mapping[str, tuple[str, str]],
) -> EntryHandler:
    """Build a JOB handler that imports and runs nested ``jobs.<module>`` modules."""

    def handle_job(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
        mapping = child_job_modules.get(entry.name)
        if not mapping:
            return EntryResult(
                name=entry.name,
                success=False,
                error=JobExecutionError(f"No child JOB module mapping for {entry.name}"),
            )
        py_stem, _kjb_stem = mapping
        try:
            module = importlib.import_module(f"jobs.{py_stem}")
            child_cfg = dict(cfg)
            child_cfg.update({k: str(v) for k, v in runtime.variables.items()})
            logging.info("Running child job entry: %s → jobs.%s", entry.name, py_stem)
            result = module.run(spark, child_cfg)
            logging.info("JOB OK | entry=%s | module=%s", entry.name, py_stem)
            return EntryResult(name=entry.name, success=True, result=result)
        except Exception as exc:  # noqa: BLE001
            log_exception_diagnostics(
                entry_name=f"{entry.name}→jobs.{py_stem}",
                exc=exc,
                kind="JOB",
            )
            return EntryResult(name=entry.name, success=False, error=exc)

    return handle_job


def build_handlers(
    *,
    spark: Any,
    cfg: Mapping[str, Any],
    entry_types: set[str],
    trans_runners: Mapping[str, TransRunner],
    child_job_modules: Mapping[str, tuple[str, str]],
) -> dict[str, EntryHandler]:
    """Register all entry handlers used by a generated job module."""
    handlers: dict[str, EntryHandler] = {
        "SPECIAL": handle_special,
        "START": handle_special,
        "DUMMY": handle_special,
        "SUCCESS": handle_success,
        "ABORT": handle_abort,
        "WRITE_TO_LOG": handle_write_to_log,
        "SET_VARIABLES": handle_set_variables,
        "SHELL": handle_shell,
        "CREATE_FOLDER": handle_create_folder,
        "FILE_EXISTS": handle_file_exists,
        "WAIT_FOR_FILE": handle_wait_for_file,
        "SIMPLE_EVAL": handle_simple_eval,
        "DELAY": handle_delay,
        "MAIL": handle_mail,
        "ZIP_FILE": handle_zip_file,
        "COPY_FILES": handle_copy_files,
        "DELETE_FILE": handle_delete_file,
        "DELETE_FILES": handle_delete_file,
        "TRANS": make_trans_handler(spark=spark, cfg=cfg, trans_runners=trans_runners),
        "JOB": make_job_handler(
            spark=spark, cfg=cfg, child_job_modules=child_job_modules
        ),
    }
    for etype in entry_types:
        if etype and etype not in handlers:
            handlers[etype] = handle_todo
    return handlers
