"""Expanded child job workflow from Pentaho: Cleanup.kjb

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/jobs/utilities/Cleanup.kjb
Full entry/hop graph — not a stub. TRANS entries call retail transformation modules.
"""

from __future__ import annotations

import importlib
import logging
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

logger = logging.getLogger("Master_ETL.children.cleanup")
_LOG = get_logger("Master_ETL.children.cleanup")

JOB_NAME = 'Cleanup'
SOURCE_KJB = 'Cleanup.kjb'
EXPANDED = True

ENTRY_DEFS = [{'attributes': {'DayOfMonth': '1',
                 'description': 'Job entry point',
                 'draw': 'Y',
                 'dummy': 'N',
                 'hour': '12',
                 'intervalMinutes': '60',
                 'intervalSeconds': '0',
                 'minutes': '0',
                 'nr': '0',
                 'parallel': 'N',
                 'repeat': 'N',
                 'schedulerType': '0',
                 'weekDay': '1',
                 'xloc': '48',
                 'yloc': '80'},
  'entry_type': 'SPECIAL',
  'filename': '',
  'is_start': True,
  'jobname': '',
  'name': 'Start',
  'transname': ''},
 {'attributes': {'description': 'Structured execution log',
                 'displayHeader': 'Y',
                 'draw': 'Y',
                 'limitRows': 'N',
                 'limitRowsNumber': '0',
                 'loglevel': 'BASIC',
                 'logmessage': 'AUDIT | EVENT=JOB_STARTED | JOB=Cleanup | RUN_ID=${RUN_ID}',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '200',
                 'yloc': '80'},
  'entry_type': 'WRITE_TO_LOG',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Log Job Started',
  'transname': ''},
 {'attributes': {'description': 'Ensure folder exists: ${OUTPUT_PATH}/staging/temp',
                 'draw': 'Y',
                 'fail_of_folder_exists': 'N',
                 'foldername': '${OUTPUT_PATH}/staging/temp',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '360',
                 'yloc': '80'},
  'entry_type': 'CREATE_FOLDER',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Ensure Temp Folder',
  'transname': ''},
 {'attributes': {'arg_from_previous': 'N',
                 'description': 'Cleanup matched files',
                 'draw': 'Y',
                 'include_subdirs': 'N',
                 'include_subfolders': 'N',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '520',
                 'yloc': '80'},
  'entry_type': 'DELETE_FILES',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Delete Staging Temp Files',
  'transname': ''},
 {'attributes': {'add_result_filesname': 'N',
                 'arg_from_previous': 'N',
                 'createDestinationFolder': 'Y',
                 'description': 'Move files to archive/quarantine',
                 'destination_filefolder': '${REJECT_PATH}/bad_files/${RUN_ID}',
                 'destination_is_afile': 'N',
                 'draw': 'Y',
                 'include_subfolders': 'N',
                 'nr': '0',
                 'overwrite_files': 'Y',
                 'parallel': 'N',
                 'source_filefolder': '${OUTPUT_PATH}/staging/temp',
                 'source_wildcard': '.*\\.bad$',
                 'xloc': '680',
                 'yloc': '80'},
  'entry_type': 'MOVE_FILES',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Quarantine Residual Bad Files',
  'transname': ''},
 {'attributes': {'add_date': 'N',
                 'add_time': 'N',
                 'arg_from_previous': 'N',
                 'description': 'Operating-system helper',
                 'draw': 'Y',
                 'exec_per_row': 'N',
                 'insertScript': 'Y',
                 'loglevel': 'Basic',
                 'nr': '0',
                 'parallel': 'N',
                 'script': 'echo Cleanup listing temp folder & dir "${OUTPUT_PATH}\\staging\\temp"',
                 'set_append_logfile': 'N',
                 'set_logfile': 'N',
                 'work_directory': '${PROJECT_HOME}',
                 'xloc': '840',
                 'yloc': '80'},
  'entry_type': 'SHELL',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Shell List Temp Directory',
  'transname': ''},
 {'attributes': {'description': 'Remove transient folder tree',
                 'draw': 'Y',
                 'fail_if_not_exists': 'N',
                 'foldername': '${OUTPUT_PATH}/staging/temp/work_${RUN_ID}',
                 'limit_folders': 'N',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '1000',
                 'yloc': '80'},
  'entry_type': 'DELETE_FOLDER',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Delete Empty Work Folder Optional',
  'transname': ''},
 {'attributes': {'description': 'Structured execution log',
                 'displayHeader': 'Y',
                 'draw': 'Y',
                 'limitRows': 'N',
                 'limitRowsNumber': '0',
                 'loglevel': 'BASIC',
                 'logmessage': 'AUDIT | EVENT=CLEANUP_COMPLETE | RUN_ID=${RUN_ID} | '
                               'TEMP=${OUTPUT_PATH}/staging/temp',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '1160',
                 'yloc': '80'},
  'entry_type': 'WRITE_TO_LOG',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Log Cleanup Complete',
  'transname': ''},
 {'attributes': {'description': 'Terminal success',
                 'draw': 'Y',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '1320',
                 'yloc': '80'},
  'entry_type': 'SUCCESS',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Success',
  'transname': ''},
 {'attributes': {'description': 'Structured execution log',
                 'displayHeader': 'Y',
                 'draw': 'Y',
                 'limitRows': 'N',
                 'limitRowsNumber': '0',
                 'loglevel': 'ERROR',
                 'logmessage': 'FAILURE | Job=${Internal.Job.Name} | RUN_ID=${RUN_ID} | '
                               'CURRENT_DATE=${CURRENT_DATE} | Check ${REJECT_PATH} and '
                               '${LOG_PATH}',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '480',
                 'yloc': '320'},
  'entry_type': 'WRITE_TO_LOG',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Log Failure',
  'transname': ''},
 {'attributes': {'comment': 'Job ${Internal.Job.Name} failed. RUN_ID=${RUN_ID}. Inspect '
                            '${LOG_PATH}/execution and ${REJECT_PATH}/exception_reports.',
                 'description': 'Dummy notification (mail disabled by default)',
                 'destination': '${MAIL_TO}',
                 'draw': 'Y',
                 'encoding': 'UTF-8',
                 'importance': 'normal',
                 'include_files': 'N',
                 'include_subfolders': 'N',
                 'nr': '0',
                 'only_comment': 'N',
                 'parallel': 'N',
                 'port': '25',
                 'priority': 'normal',
                 'replyto': 'etl-noreply@example.com',
                 'secureconnectiontype': 'SSL',
                 'sender_address': 'etl-noreply@example.com',
                 'sender_name': 'Retail ETL',
                 'sensitivity': 'normal',
                 'server': 'localhost',
                 'subject': '[FAIL] Retail ETL ${Internal.Job.Name} RUN_ID=${RUN_ID}',
                 'use_HTML': 'N',
                 'use_Priority': 'N',
                 'use_auth': 'N',
                 'use_secure_auth': 'N',
                 'xloc': '700',
                 'yloc': '320',
                 'zip_files': 'N'},
  'entry_type': 'MAIL',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Failure Email Dummy',
  'transname': ''},
 {'attributes': {'description': 'Terminal abort on unrecoverable failure',
                 'draw': 'Y',
                 'message': 'Aborting ${Internal.Job.Name} for RUN_ID=${RUN_ID}. See error logs.',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '920',
                 'yloc': '320'},
  'entry_type': 'ABORT',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Abort on Failure',
  'transname': ''}]

HOP_DEFS = [{'enabled': True,
  'evaluation': True,
  'from_name': 'Start',
  'to_name': 'Log Job Started',
  'unconditional': True},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Log Job Started',
  'to_name': 'Ensure Temp Folder',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Ensure Temp Folder',
  'to_name': 'Delete Staging Temp Files',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Delete Staging Temp Files',
  'to_name': 'Quarantine Residual Bad Files',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Quarantine Residual Bad Files',
  'to_name': 'Shell List Temp Directory',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Shell List Temp Directory',
  'to_name': 'Delete Empty Work Folder Optional',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Delete Empty Work Folder Optional',
  'to_name': 'Log Cleanup Complete',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Log Cleanup Complete',
  'to_name': 'Success',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Delete Staging Temp Files',
  'to_name': 'Log Failure',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Quarantine Residual Bad Files',
  'to_name': 'Log Failure',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Shell List Temp Directory',
  'to_name': 'Log Failure',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Log Failure',
  'to_name': 'Failure Email Dummy',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Failure Email Dummy',
  'to_name': 'Abort on Failure',
  'unconditional': True}]

# Parent job entry name -> retail module stem
TRANS_MODULES = {}


def _entries() -> list[JobEntry]:
    return [
        JobEntry(
            name=d["name"],
            entry_type=d["entry_type"],
            filename=d.get("filename", ""),
            transname=d.get("transname", ""),
            jobname=d.get("jobname", ""),
            is_start=bool(d.get("is_start")),
            attributes=dict(d.get("attributes") or {}),
        )
        for d in ENTRY_DEFS
    ]


def _hops() -> list[JobHop]:
    return [
        JobHop(
            from_name=h["from_name"],
            to_name=h["to_name"],
            enabled=bool(h.get("enabled", True)),
            unconditional=h.get("unconditional"),
            evaluation=h.get("evaluation"),
        )
        for h in HOP_DEFS
    ]


def _handle_special(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    return EntryResult(name=entry.name, success=True, result=True)


def _handle_success(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    return EntryResult(name=entry.name, success=True, result=True)


def _handle_abort(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    msg = entry.attributes.get("message") or f"Abort at {entry.name}"
    return EntryResult(
        name=entry.name,
        success=False,
        result=msg,
        error=JobExecutionError(msg),
    )


def _handle_trans(
    runtime: JobRuntime,
    entry: JobEntry,
    *,
    spark: Any,
    config: Mapping[str, Any],
) -> EntryResult:
    mapping = TRANS_MODULES.get(entry.name)
    if not mapping:
        return EntryResult(
            name=entry.name,
            success=False,
            error=JobExecutionError(f"No TRANS module mapping for {entry.name}"),
        )
    py_stem, ktr_stem = mapping
    resolved = substitute_variables(entry.filename, runtime.variables)
    try:
        module = importlib.import_module(
            f"pentaho_migration.transformations.retail.{py_stem}"
        )
        child_cfg = dict(config)
        child_cfg.update({k: str(v) for k, v in runtime.variables.items()})
        df = module.run(spark, child_cfg)
        logger.info(
            "TRANS OK | entry=%s | module=%s | resolved=%s",
            entry.name,
            py_stem,
            resolved,
        )
        return EntryResult(name=entry.name, success=True, result=df)
    except Exception as exc:  # noqa: BLE001
        logger.exception("TRANS FAIL | entry=%s | module=%s", entry.name, py_stem)
        return EntryResult(name=entry.name, success=False, error=exc)


def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Execute expanded child job ``Cleanup``."""
    config = dict(config or {})
    if spark is not None:
        apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "job_start", job=JOB_NAME)
    variables = {
        "Internal.Job.Name": JOB_NAME,
        "Internal.Job.Filename.Name": SOURCE_KJB,
        **{k: str(v) for k, v in config.items()},
    }

    def handle_trans(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
        return _handle_trans(runtime, entry, spark=spark, config=config)

    runtime = JobRuntime(
        name=JOB_NAME,
        entries=_entries(),
        hops=_hops(),
        parameters={k: str(v) for k, v in config.items() if isinstance(v, (str, int, float))},
        variables=variables,
        handlers={
            "SPECIAL": _handle_special,
            "SUCCESS": _handle_success,
            "ABORT": _handle_abort,
            "TRANS": handle_trans,
        },
        allow_reentry=True,
    )
    final = runtime.run()
    log_event(_LOG, "job_end", success=final.success, last=final.name,
              steps=len(runtime.executed))
    if not final.success:
        raise JobExecutionError(
            f"Child job {JOB_NAME} failed at {final.name}"
        ) from final.error
    return {
        "job": JOB_NAME,
        "expanded": True,
        "executed": list(runtime.executed),
        "result": final.result,
    }
