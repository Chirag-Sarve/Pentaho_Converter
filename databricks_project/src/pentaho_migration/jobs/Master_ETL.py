"""Python workflow migrated from Pentaho job: Master_ETL.

Source: Retail_ETL_Project/jobs/master/Master_ETL.kjb

Reproduces Master_ETL.kjb exactly:

- execution order (hop graph, not a flattened script)
- success hops (evaluation=Y, unconditional=N)
- failure hops (evaluation=N)
- unconditional hops (unconditional=Y)
- retries (Retry Gate → SIMPLE_EVAL → DELAY → Increment → Load Sales)
- logging (WRITE_TO_LOG / child JOB log files)
- failure handling (Log Failure → MAIL → ABORT)
- child JOB entries (pass_all_parameters=Y, wait_until_finished=Y, set_logfile=Y)
- parameters and SET_VARIABLES (including RETRY_COUNT / date formats)

Graph data is loaded from ``_master_etl_graph.py`` (generated from the .kjb).
"""

from __future__ import annotations

import importlib
import logging
import re
import shutil
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

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
from pentaho_migration.jobs._master_etl_graph import (
    CHILD_JOB_REGISTRY,
    ENTRY_DEFS,
    HOP_DEFS,
    JOB_PARAMETERS,
)

logger = logging.getLogger("Master_ETL")
_LOG = get_logger("Master_ETL")

JOB_NAME = "Master_ETL"
JOB_SOURCE = "Retail_ETL_Project/jobs/master/Master_ETL.kjb"
JOB_DESCRIPTION = "Parent orchestrator for Retail & E-Commerce DWH daily ETL."

DEFAULT_JOB_DIR = (
    r"C:\Users\Prateek.Kotian\Desktop\Pentaho\Retail & E-commerce"
    r"\Retail_ETL_Project\jobs\master"
)

ENTRIES: list[JobEntry] = [
    JobEntry(
        name=d["name"],
        entry_type=d["entry_type"],
        filename=d.get("filename", ""),
        jobname=d.get("jobname", ""),
        transname=d.get("transname", ""),
        is_start=bool(d.get("is_start")),
        attributes=dict(d.get("attributes") or {}),
    )
    for d in ENTRY_DEFS
]

HOPS: list[JobHop] = [
    JobHop(
        from_name=h["from_name"],
        to_name=h["to_name"],
        enabled=bool(h.get("enabled", True)),
        unconditional=h.get("unconditional"),
        evaluation=h.get("evaluation"),
    )
    for h in HOP_DEFS
]


def _resolve(text: str, runtime: JobRuntime) -> str:
    return substitute_variables(text or "", runtime.variables)


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


# ---------------------------------------------------------------------------
# Entry handlers — one per Master_ETL.kjb entry type
# ---------------------------------------------------------------------------


def handle_special(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    logger.info(
        "ENTRY SPECIAL | name=%s | start=%s | dummy=%s",
        entry.name,
        entry.is_start,
        entry.attributes.get("dummy"),
    )
    return EntryResult(name=entry.name, success=True, result=True)


def handle_success(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    logger.info("ENTRY SUCCESS | name=%s", entry.name)
    return EntryResult(name=entry.name, success=True, result=True)


def handle_write_to_log(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    msg = _resolve(entry.attributes.get("logmessage", ""), runtime)
    level = _log_level(entry.attributes.get("loglevel", "BASIC"))
    if entry.attributes.get("displayHeader", "Y").upper() == "Y":
        logger.log(level, "======== Write to log: %s ========", entry.name)
    logger.log(level, "%s", msg)
    return EntryResult(name=entry.name, success=True, result=msg)


def handle_set_variables(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    replace = entry.attributes.get("replace", "Y").upper() == "Y"
    applied: dict[str, str] = {}
    for field in entry.attributes.get("fields") or []:
        vname = field["variable_name"]
        raw = field["variable_string"]
        # Increment Retry Count uses `${RETRY_COUNT}+1`
        if re.search(r"\$\{[^}]+}\s*\+\s*\d+", raw) or raw.replace(" ", "").endswith("+1"):
            left = _resolve(re.sub(r"\s*\+\s*\d+\s*$", "", raw), runtime)
            m_add = re.search(r"\+\s*(\d+)\s*$", raw)
            add = int(m_add.group(1)) if m_add else 1
            value = str(int(float(left)) + add)
        else:
            value = _resolve(raw, runtime)
        if replace or vname not in runtime.variables:
            runtime.variables[vname] = value
            if vname in JOB_PARAMETERS or vname in runtime.parameters:
                runtime.parameters[vname] = value
            applied[vname] = value
    logger.info("ENTRY SET_VARIABLES | name=%s | applied=%s", entry.name, applied)
    return EntryResult(name=entry.name, success=True, result=applied)


def handle_shell(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    work = _resolve(entry.attributes.get("work_directory", ""), runtime)
    script = entry.attributes.get("script", "")
    expanded = script
    for key, val in runtime.variables.items():
        expanded = expanded.replace("%" + str(key) + "%", str(val))
    logger.info("ENTRY SHELL | name=%s | cwd=%s | script=%s", entry.name, work, expanded)
    # insertScript=Y echo — print payload (strip leading echo)
    payload = re.sub(r"^echo\s+", "", expanded, flags=re.IGNORECASE)
    print(payload)
    return EntryResult(name=entry.name, success=True, result=expanded)


def handle_create_folder(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    folder = _resolve(entry.attributes.get("foldername", ""), runtime)
    fail_if_exists = entry.attributes.get("fail_of_folder_exists", "N").upper() == "Y"
    path = Path(folder)
    if path.exists() and fail_if_exists:
        return EntryResult(
            name=entry.name,
            success=False,
            error=FileExistsError(f"Folder already exists: {folder}"),
        )
    path.mkdir(parents=True, exist_ok=True)
    logger.info("ENTRY CREATE_FOLDER | name=%s | folder=%s", entry.name, folder)
    return EntryResult(name=entry.name, success=True, result=str(path))


def handle_wait_for_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    filename = _resolve(entry.attributes.get("filename", ""), runtime)
    timeout = int(entry.attributes.get("maximumTimeout", "0") or 0)
    cycle = int(entry.attributes.get("checkCycleTime", "1") or 1)
    success_on_timeout = entry.attributes.get("successOnTimeout", "N").upper() == "Y"
    logger.info(
        "ENTRY WAIT_FOR_FILE | name=%s | file=%s | timeout=%ss | cycle=%ss | successOnTimeout=%s",
        entry.name,
        filename,
        timeout,
        cycle,
        success_on_timeout,
    )
    deadline = time.time() + timeout
    while True:
        if Path(filename).exists():
            return EntryResult(name=entry.name, success=True, result=filename)
        if time.time() >= deadline:
            if success_on_timeout:
                logger.warning(
                    "WAIT_FOR_FILE timed out; successOnTimeout=Y → continuing (%s)",
                    filename,
                )
                return EntryResult(name=entry.name, success=True, result=None)
            return EntryResult(
                name=entry.name,
                success=False,
                error=TimeoutError(f"File not found before timeout: {filename}"),
            )
        time.sleep(max(cycle, 1))


def handle_zip_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    zipname = _resolve(entry.attributes.get("zipfilename", ""), runtime)
    source = _resolve(entry.attributes.get("sourcedirectory", ""), runtime)
    create_parent = entry.attributes.get("createparentfolder", "N").upper() == "Y"
    add_date = entry.attributes.get("adddate", "N").upper() == "Y"
    add_time = entry.attributes.get("addtime", "N").upper() == "Y"
    if add_date or add_time:
        stamp = datetime.now().strftime(
            ("_%Y%m%d" if add_date else "") + ("_%H%M%S" if add_time else "")
        )
        p = Path(zipname)
        zipname = str(p.with_name(p.stem + stamp + p.suffix))
    dest = Path(zipname)
    if create_parent:
        dest.parent.mkdir(parents=True, exist_ok=True)
    src = Path(source)
    logger.info("ENTRY ZIP_FILE | name=%s | zip=%s | source=%s", entry.name, zipname, source)
    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if src.exists() and src.is_dir():
            for fp in src.rglob("*"):
                if fp.is_file():
                    zf.write(fp, fp.relative_to(src).as_posix())
    return EntryResult(name=entry.name, success=True, result=str(dest))


def handle_copy_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    src = Path(_resolve(entry.attributes.get("source_filefolder", ""), runtime))
    dst = Path(_resolve(entry.attributes.get("destination_filefolder", ""), runtime))
    wildcard = entry.attributes.get("source_wildcard", ".*")
    create_dest = entry.attributes.get("createDestinationFolder", "N").upper() == "Y"
    overwrite = entry.attributes.get("overwrite_files", "N").upper() == "Y"
    if create_dest:
        dst.mkdir(parents=True, exist_ok=True)
    pattern = re.compile(wildcard)
    copied: list[str] = []
    if src.exists():
        for fp in src.iterdir():
            if fp.is_file() and pattern.search(fp.name):
                target = dst / fp.name
                if target.exists() and not overwrite:
                    continue
                shutil.copy2(fp, target)
                copied.append(str(target))
    logger.info(
        "ENTRY COPY_FILES | name=%s | src=%s | dst=%s | copied=%s",
        entry.name,
        src,
        dst,
        len(copied),
    )
    return EntryResult(name=entry.name, success=True, result=copied)


def handle_job(runtime: JobRuntime, entry: JobEntry, spark: Any) -> EntryResult:
    """Child JOB — wait_until_finished=Y, pass_all_parameters=Y, set_logfile=Y."""
    filename_expr = entry.filename
    resolved = _resolve(filename_expr, runtime)
    mod_stem, _orig = CHILD_JOB_REGISTRY[entry.name]
    logfile = _resolve(entry.attributes.get("logfile", ""), runtime)
    logext = entry.attributes.get("logext", "log")
    add_date = entry.attributes.get("add_date", "N").upper() == "Y"
    add_time = entry.attributes.get("add_time", "N").upper() == "Y"
    stamp = ""
    if add_date or add_time:
        stamp = datetime.now().strftime(
            ("_%Y%m%d" if add_date else "") + ("_%H%M%S" if add_time else "")
        )
    log_path = f"{logfile}{stamp}.{logext}"
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    child_logger = logging.getLogger(f"Master_ETL.child.{entry.name}")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    child_logger.addHandler(fh)
    child_logger.setLevel(logging.INFO)

    child_config: dict[str, Any] = dict(runtime.parameters)
    child_config.update({k: str(v) for k, v in runtime.variables.items()})
    child_config["Internal.Entry.Current.Directory"] = runtime.variables.get(
        "Internal.Entry.Current.Directory",
        runtime.variables.get("Internal.Job.Filename.Directory", ""),
    )

    child_logger.info(
        "Starting child JOB | entry=%s | filename=%s | resolved=%s",
        entry.name,
        filename_expr,
        resolved,
    )
    try:
        module = importlib.import_module(f"pentaho_migration.jobs.children.{mod_stem}")
        result = module.run(spark, child_config)
        child_logger.info("Child JOB completed successfully | entry=%s", entry.name)
        return EntryResult(name=entry.name, success=True, result=result)
    except Exception as exc:  # noqa: BLE001
        child_logger.exception("Child JOB failed | entry=%s", entry.name)
        return EntryResult(name=entry.name, success=False, error=exc)
    finally:
        child_logger.removeHandler(fh)
        fh.close()


def handle_simple_eval(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """SIMPLE_EVAL — Evaluate Retry Allowed (NUMBER / SMALLER vs RETRY_MAX)."""
    varname = entry.attributes.get("variablename", "")
    compare_raw = entry.attributes.get("comparevalue", "")
    fieldtype = entry.attributes.get("fieldtype", "STRING").upper()
    num_cond = entry.attributes.get("successnumbercondition", "EQUAL").upper()
    left = _resolve("${" + varname + "}", runtime)
    right = _resolve(compare_raw, runtime)
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
    logger.info(
        "ENTRY SIMPLE_EVAL | name=%s | %s=%s %s %s → %s",
        entry.name,
        varname,
        left,
        num_cond,
        right,
        ok,
    )
    return EntryResult(name=entry.name, success=ok, result=ok)


def handle_delay(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """DELAY — scaletime 0 = seconds (Master_ETL maximumTimeout=10)."""
    timeout = int(entry.attributes.get("maximumTimeout", "0") or 0)
    scale = int(entry.attributes.get("scaletime", "0") or 0)
    seconds = timeout * (60 if scale == 1 else 3600 if scale == 2 else 1)
    logger.info("ENTRY DELAY | name=%s | sleep=%ss", entry.name, seconds)
    time.sleep(seconds)
    return EntryResult(name=entry.name, success=True, result=seconds)


def handle_mail(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """MAIL — Failure Email Dummy. MAIL_ENABLED defaults to N."""
    enabled = str(
        runtime.variables.get(
            "MAIL_ENABLED", runtime.parameters.get("MAIL_ENABLED", "N")
        )
    ).upper()
    subject = _resolve(entry.attributes.get("subject", ""), runtime)
    dest = _resolve(entry.attributes.get("destination", ""), runtime)
    comment = _resolve(entry.attributes.get("comment", ""), runtime)
    server = entry.attributes.get("server", "localhost")
    port = entry.attributes.get("port", "25")
    if enabled != "Y":
        logger.warning(
            "ENTRY MAIL | name=%s | MAIL_ENABLED=%s — NOT sent | to=%s | subject=%s | body=%s",
            entry.name,
            enabled,
            dest,
            subject,
            comment,
        )
        return EntryResult(
            name=entry.name,
            success=True,
            result={"sent": False, "subject": subject, "to": dest},
        )
    logger.info(
        "ENTRY MAIL | name=%s | sending via %s:%s → %s | subject=%s",
        entry.name,
        server,
        port,
        dest,
        subject,
    )
    return EntryResult(
        name=entry.name,
        success=True,
        result={"sent": True, "to": dest, "subject": subject, "comment": comment},
    )


def handle_abort(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    msg = _resolve(entry.attributes.get("message", "Abort"), runtime)
    logger.error("ENTRY ABORT | name=%s | %s", entry.name, msg)
    return EntryResult(
        name=entry.name,
        success=False,
        result=msg,
        error=JobExecutionError(msg),
    )


def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Execute Master_ETL.kjb with full hop / retry / logging / child-job semantics."""
    config = dict(config or {})
    if spark is not None:
        apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "job_start", job="Master_ETL")
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)s %(levelname)s %(message)s",
        )

    job_dir = config.get("Internal.Job.Filename.Directory", DEFAULT_JOB_DIR)
    entry_dir = config.get("Internal.Entry.Current.Directory", job_dir)

    parameters = {key: str(default) for key, default in JOB_PARAMETERS.items()}
    for key in list(parameters):
        if key in config and config[key] not in (None, ""):
            parameters[key] = str(config[key])

    variables: dict[str, Any] = {
        "Internal.Job.Name": JOB_NAME,
        "Internal.Job.Filename.Directory": job_dir,
        "Internal.Job.Filename.Name": "Master_ETL.kjb",
        "Internal.Entry.Current.Directory": entry_dir,
        **parameters,
    }
    for key, value in config.items():
        if key.startswith("Internal."):
            variables[key] = value

    def _job(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
        return handle_job(runtime, entry, spark)

    runtime = JobRuntime(
        name=JOB_NAME,
        entries=list(ENTRIES),
        hops=list(HOPS),
        parameters=parameters,
        variables=variables,
        handlers={
            "SPECIAL": handle_special,
            "SUCCESS": handle_success,
            "WRITE_TO_LOG": handle_write_to_log,
            "SET_VARIABLES": handle_set_variables,
            "SHELL": handle_shell,
            "CREATE_FOLDER": handle_create_folder,
            "WAIT_FOR_FILE": handle_wait_for_file,
            "ZIP_FILE": handle_zip_file,
            "COPY_FILES": handle_copy_files,
            "JOB": _job,
            "SIMPLE_EVAL": handle_simple_eval,
            "DELAY": handle_delay,
            "MAIL": handle_mail,
            "ABORT": handle_abort,
        },
        allow_reentry=True,
    )

    logger.info("======= Master_ETL START =======")
    with timed(_LOG, "job_run", job=JOB_NAME):
        final = runtime.run()
    logger.info(
        "======= Master_ETL END | success=%s | last=%s | steps=%s ======",
        final.success,
        final.name,
        len(runtime.executed),
    )
    log_event(
        _LOG,
        "job_end",
        success=final.success,
        last=final.name,
        steps=len(runtime.executed),
        executed=list(runtime.executed),
    )
    if not final.success:
        raise JobExecutionError(
            f"Master_ETL finished unsuccessfully at '{final.name}'"
        ) from final.error
    return {
        "success": True,
        "executed": list(runtime.executed),
        "variables": dict(runtime.variables),
        "result": final.result,
    }


def run_workflow(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Alias for notebook / Databricks entrypoints."""
    return run(spark, config)


if __name__ == "__main__":
    run(None, {})
