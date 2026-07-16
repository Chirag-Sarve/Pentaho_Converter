"""Expanded child job workflow from Pentaho: Audit.kjb

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/jobs/utilities/Audit.kjb
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

logger = logging.getLogger("Master_ETL.children.audit")
_LOG = get_logger("Master_ETL.children.audit")

JOB_NAME = 'Audit'
SOURCE_KJB = 'Audit.kjb'
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
                 'logmessage': 'AUDIT | EVENT=JOB_STARTED | JOB=Audit | RUN_ID=${RUN_ID}',
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
 {'attributes': {'description': 'Ensure folder exists: ${PROJECT_HOME}/audit/load_audit',
                 'draw': 'Y',
                 'fail_of_folder_exists': 'N',
                 'foldername': '${PROJECT_HOME}/audit/load_audit',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '360',
                 'yloc': '80'},
  'entry_type': 'CREATE_FOLDER',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Create Audit Output',
  'transname': ''},
 {'attributes': {'description': 'Ensure folder exists: ${PROJECT_HOME}/audit/row_counts',
                 'draw': 'Y',
                 'fail_of_folder_exists': 'N',
                 'foldername': '${PROJECT_HOME}/audit/row_counts',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '520',
                 'yloc': '80'},
  'entry_type': 'CREATE_FOLDER',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Create Row Counts Folder',
  'transname': ''},
 {'attributes': {'description': 'Ensure folder exists: ${PROJECT_HOME}/audit/data_quality',
                 'draw': 'Y',
                 'fail_of_folder_exists': 'N',
                 'foldername': '${PROJECT_HOME}/audit/data_quality',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '680',
                 'yloc': '80'},
  'entry_type': 'CREATE_FOLDER',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Create DQ Folder',
  'transname': ''},
 {'attributes': {'description': 'Gate on required source file',
                 'draw': 'Y',
                 'filename': '${LOG_PATH}/execution',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '840',
                 'yloc': '80'},
  'entry_type': 'FILE_EXISTS',
  'filename': '${LOG_PATH}/execution',
  'is_start': False,
  'jobname': '',
  'name': 'File Exists Execution Log Root',
  'transname': ''},
 {'attributes': {'add_date': 'Y',
                 'add_time': 'Y',
                 'arg_from_previous': 'N',
                 'clear_files': 'N',
                 'clear_rows': 'N',
                 'cluster': 'N',
                 'description': 'PLACEHOLDER transformation (not generated yet): '
                                '${PROJECT_HOME}/transformations/utilities/Audit_Collect_Metrics.ktr',
                 'draw': 'Y',
                 'exec_per_row': 'N',
                 'filename': '${PROJECT_HOME}/transformations/utilities/Audit_Collect_Metrics.ktr',
                 'follow_abort_remote': 'Y',
                 'logext': 'log',
                 'logfile': '${LOG_PATH}/execution/Audit_Collect_Metrics',
                 'loglevel': 'Basic',
                 'nr': '0',
                 'parallel': 'N',
                 'params_from_previous': 'N',
                 'set_logfile': 'Y',
                 'specification_method': 'filename',
                 'wait_until_finished': 'Y',
                 'xloc': '1000',
                 'yloc': '80'},
  'entry_type': 'TRANS',
  'filename': '${PROJECT_HOME}/transformations/utilities/Audit_Collect_Metrics.ktr',
  'is_start': False,
  'jobname': '',
  'name': 'Audit_Collect_Metrics',
  'transname': ''},
 {'attributes': {'add_date': 'Y',
                 'add_time': 'Y',
                 'arg_from_previous': 'N',
                 'clear_files': 'N',
                 'clear_rows': 'N',
                 'cluster': 'N',
                 'description': 'PLACEHOLDER transformation (not generated yet): '
                                '${PROJECT_HOME}/transformations/utilities/Audit_Write_Report.ktr',
                 'draw': 'Y',
                 'exec_per_row': 'N',
                 'filename': '${PROJECT_HOME}/transformations/utilities/Audit_Write_Report.ktr',
                 'follow_abort_remote': 'Y',
                 'logext': 'log',
                 'logfile': '${LOG_PATH}/execution/Audit_Write_Report',
                 'loglevel': 'Basic',
                 'nr': '0',
                 'parallel': 'N',
                 'params_from_previous': 'N',
                 'set_logfile': 'Y',
                 'specification_method': 'filename',
                 'wait_until_finished': 'Y',
                 'xloc': '1160',
                 'yloc': '80'},
  'entry_type': 'TRANS',
  'filename': '${PROJECT_HOME}/transformations/utilities/Audit_Write_Report.ktr',
  'is_start': False,
  'jobname': '',
  'name': 'Audit_Write_Report',
  'transname': ''},
 {'attributes': {'description': 'Audit / control SQL (placeholder connection)',
                 'draw': 'Y',
                 'nr': '0',
                 'parallel': 'N',
                 'sendOneStatement': 'N',
                 'sql': '-- Placeholder\n'
                        "-- UPDATE audit_batch_run SET status='SUCCESS', end_ts=CURRENT_TIMESTAMP "
                        "-- WHERE batch_id='${RUN_ID}';",
                 'sqlfromfile': 'N',
                 'useVariableSubstitution': 'Y',
                 'xloc': '1320',
                 'yloc': '80'},
  'entry_type': 'SQL',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'SQL Close Batch Audit',
  'transname': ''},
 {'attributes': {'add_result_filesname': 'N',
                 'arg_from_previous': 'N',
                 'createDestinationFolder': 'Y',
                 'description': 'Archive / stage file copy',
                 'destination_filefolder': '${OUTPUT_PATH}/exports',
                 'destination_is_afile': 'N',
                 'draw': 'Y',
                 'include_subfolders': 'N',
                 'nr': '0',
                 'overwrite_files': 'Y',
                 'parallel': 'N',
                 'remove_source_files': 'N',
                 'source_filefolder': '${PROJECT_HOME}/audit/load_audit',
                 'source_wildcard': '.*${RUN_ID}.*',
                 'xloc': '1480',
                 'yloc': '80'},
  'entry_type': 'COPY_FILES',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Copy Audit Report To Exports',
  'transname': ''},
 {'attributes': {'SpecifyFormat': 'N',
                 'adddate': 'Y',
                 'addfileparent': 'N',
                 'addtime': 'Y',
                 'afterzip': '0',
                 'compressionrate': '1',
                 'createMoveToDirectory': 'N',
                 'createparentfolder': 'Y',
                 'description': 'Zip prior outputs for archive',
                 'draw': 'Y',
                 'iffileexists': '0',
                 'isfromprevious': 'N',
                 'nr': '0',
                 'parallel': 'N',
                 'sourcedirectory': '${PROJECT_HOME}/audit/load_audit',
                 'storedsourcepath': '0',
                 'wildCard': '.*',
                 'xloc': '1640',
                 'yloc': '80',
                 'zipfilename': '${ARCHIVE_PATH}/${CURRENT_DATE}/audit_${RUN_ID}.zip'},
  'entry_type': 'ZIP_FILE',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Zip Audit Pack',
  'transname': ''},
 {'attributes': {'description': 'Structured execution log',
                 'displayHeader': 'Y',
                 'draw': 'Y',
                 'limitRows': 'N',
                 'limitRowsNumber': '0',
                 'loglevel': 'BASIC',
                 'logmessage': 'AUDIT | EVENT=AUDIT_REPORT | RUN_ID=${RUN_ID} | '
                               'PATH=${PROJECT_HOME}/audit/load_audit | '
                               'ROW_COUNTS=${PROJECT_HOME}/audit/row_counts | '
                               'DQ=${PROJECT_HOME}/audit/data_quality',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '1800',
                 'yloc': '80'},
  'entry_type': 'WRITE_TO_LOG',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Log Audit Information',
  'transname': ''},
 {'attributes': {'description': 'Structured execution log',
                 'displayHeader': 'Y',
                 'draw': 'Y',
                 'limitRows': 'N',
                 'limitRowsNumber': '0',
                 'loglevel': 'BASIC',
                 'logmessage': 'AUDIT | EVENT=JOB_COMPLETED | JOB=Audit | RUN_ID=${RUN_ID} | '
                               'STATUS=SUCCESS',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '1960',
                 'yloc': '80'},
  'entry_type': 'WRITE_TO_LOG',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Log Job Completed',
  'transname': ''},
 {'attributes': {'description': 'Terminal success',
                 'draw': 'Y',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '2120',
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
  'to_name': 'Create Audit Output',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Create Audit Output',
  'to_name': 'Create Row Counts Folder',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Create Row Counts Folder',
  'to_name': 'Create DQ Folder',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Create DQ Folder',
  'to_name': 'File Exists Execution Log Root',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'File Exists Execution Log Root',
  'to_name': 'Audit_Collect_Metrics',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Audit_Collect_Metrics',
  'to_name': 'Audit_Write_Report',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Audit_Write_Report',
  'to_name': 'SQL Close Batch Audit',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'SQL Close Batch Audit',
  'to_name': 'Copy Audit Report To Exports',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Copy Audit Report To Exports',
  'to_name': 'Zip Audit Pack',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Zip Audit Pack',
  'to_name': 'Log Audit Information',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Log Audit Information',
  'to_name': 'Log Job Completed',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Log Job Completed',
  'to_name': 'Success',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Audit_Collect_Metrics',
  'to_name': 'Log Failure',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Audit_Write_Report',
  'to_name': 'Log Failure',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'SQL Close Batch Audit',
  'to_name': 'Log Failure',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Copy Audit Report To Exports',
  'to_name': 'Log Failure',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Zip Audit Pack',
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
TRANS_MODULES = {'Audit_Collect_Metrics': ('audit_collect_metrics', 'Audit_Collect_Metrics'),
 'Audit_Write_Report': ('audit_write_report', 'Audit_Write_Report')}


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
    """Execute expanded child job ``Audit``."""
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
