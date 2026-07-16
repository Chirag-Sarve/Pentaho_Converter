"""Expanded child job workflow from Pentaho: Notification.kjb

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/jobs/utilities/Notification.kjb
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

logger = logging.getLogger("Master_ETL.children.notification")
_LOG = get_logger("Master_ETL.children.notification")

JOB_NAME = 'Notification'
SOURCE_KJB = 'Notification.kjb'
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
                 'logmessage': 'AUDIT | EVENT=JOB_STARTED | JOB=Notification | RUN_ID=${RUN_ID} | '
                               'MAIL_ENABLED=${MAIL_ENABLED}',
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
 {'attributes': {'comparevalue': 'Y',
                 'description': 'Conditional branch evaluation',
                 'draw': 'Y',
                 'fieldtype': 'STRING',
                 'nr': '0',
                 'parallel': 'N',
                 'successbooleancondition': 'false',
                 'successcondition': 'EQUAL',
                 'successnumbercondition': 'EQUAL',
                 'successwhenvarset': 'N',
                 'valuetype': 'VARIABLE',
                 'variablename': 'MAIL_ENABLED',
                 'xloc': '360',
                 'yloc': '80'},
  'entry_type': 'SIMPLE_EVAL',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Evaluate Mail Enabled',
  'transname': ''},
 {'attributes': {'comment': 'Retail ETL batch completed successfully.\n'
                            'RUN_ID=${RUN_ID}\n'
                            'CURRENT_DATE=${CURRENT_DATE}\n'
                            'PROJECT_HOME=${PROJECT_HOME}\n'
                            'LOG_PATH=${LOG_PATH}\n'
                            'OUTPUT_PATH=${OUTPUT_PATH}\n'
                            'ARCHIVE_PATH=${ARCHIVE_PATH}\n'
                            'This is a DUMMY mail entry for Spoon/Kitchen wiring.',
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
                 'subject': '[SUCCESS] Retail ETL Master_ETL RUN_ID=${RUN_ID}',
                 'use_HTML': 'N',
                 'use_Priority': 'N',
                 'use_auth': 'N',
                 'use_secure_auth': 'N',
                 'xloc': '560',
                 'yloc': '40',
                 'zip_files': 'N'},
  'entry_type': 'MAIL',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Success Email Dummy',
  'transname': ''},
 {'attributes': {'DayOfMonth': '1',
                 'description': 'Mail skipped when MAIL_ENABLED != Y',
                 'draw': 'Y',
                 'dummy': 'Y',
                 'hour': '12',
                 'intervalMinutes': '60',
                 'intervalSeconds': '0',
                 'minutes': '0',
                 'nr': '0',
                 'parallel': 'N',
                 'repeat': 'N',
                 'schedulerType': '0',
                 'weekDay': '1',
                 'xloc': '560',
                 'yloc': '140'},
  'entry_type': 'SPECIAL',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Skip Mail',
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
                 'script': 'echo NOTIFY SUCCESS RUN_ID=%RUN_ID% DATE=%CURRENT_DATE%',
                 'set_append_logfile': 'N',
                 'set_logfile': 'N',
                 'work_directory': '${PROJECT_HOME}',
                 'xloc': '760',
                 'yloc': '80'},
  'entry_type': 'SHELL',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Shell Notify Console',
  'transname': ''},
 {'attributes': {'description': 'Structured execution log',
                 'displayHeader': 'Y',
                 'draw': 'Y',
                 'limitRows': 'N',
                 'limitRowsNumber': '0',
                 'loglevel': 'BASIC',
                 'logmessage': 'AUDIT | EVENT=NOTIFICATION | CHANNEL=DUMMY_MAIL_OR_CONSOLE | '
                               'RUN_ID=${RUN_ID} | STATUS=SUCCESS',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '920',
                 'yloc': '80'},
  'entry_type': 'WRITE_TO_LOG',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Log Notification Sent',
  'transname': ''},
 {'attributes': {'description': 'Terminal success',
                 'draw': 'Y',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '1080',
                 'yloc': '80'},
  'entry_type': 'SUCCESS',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Success',
  'transname': ''},
 {'attributes': {'description': 'Terminal abort on unrecoverable failure',
                 'draw': 'Y',
                 'message': 'Notification job misconfiguration for RUN_ID=${RUN_ID}',
                 'nr': '0',
                 'parallel': 'N',
                 'xloc': '560',
                 'yloc': '280'},
  'entry_type': 'ABORT',
  'filename': '',
  'is_start': False,
  'jobname': '',
  'name': 'Abort Notification Misconfig',
  'transname': ''}]

HOP_DEFS = [{'enabled': True,
  'evaluation': True,
  'from_name': 'Start',
  'to_name': 'Log Job Started',
  'unconditional': True},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Log Job Started',
  'to_name': 'Evaluate Mail Enabled',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Evaluate Mail Enabled',
  'to_name': 'Success Email Dummy',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Evaluate Mail Enabled',
  'to_name': 'Skip Mail',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Success Email Dummy',
  'to_name': 'Shell Notify Console',
  'unconditional': True},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Skip Mail',
  'to_name': 'Shell Notify Console',
  'unconditional': True},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Shell Notify Console',
  'to_name': 'Log Notification Sent',
  'unconditional': False},
 {'enabled': True,
  'evaluation': True,
  'from_name': 'Log Notification Sent',
  'to_name': 'Success',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Log Job Started',
  'to_name': 'Abort Notification Misconfig',
  'unconditional': False},
 {'enabled': True,
  'evaluation': False,
  'from_name': 'Shell Notify Console',
  'to_name': 'Abort Notification Misconfig',
  'unconditional': False}]

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
    """Execute expanded child job ``Notification``."""
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
