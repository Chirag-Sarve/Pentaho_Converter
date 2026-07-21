"""Reusable Pentaho job-entry handlers (SPECIAL, TRANS, SET_VARIABLES, …)."""

from __future__ import annotations

import importlib
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .df_guards import log_exception_diagnostics
from . import bulk_ops as bops
from . import condition_ops as cops
from . import file_ops as fops
from . import pgp_ops as pops
from . import repository_ops as rops
from . import scripting_ops as sops
from . import transfer_ops as tops
from . import utility_ops as uops
from . import xml_ops as xops
from .job_models import EntryResult, JobEntry
from .job_runtime import EntryHandler, JobExecutionError, JobRuntime
from .mail_ops import (
    get_mails,
    get_mails_config_from_attributes,
    iter_warning_logs,
    mail_config_from_attributes,
    send_smtp_mail,
    validate_email_addresses,
    yn_true,
)
from .variables import substitute_variables

TransRunner = Callable[..., Any]

# Set Variables scopes (JobEntrySetVariables.variableTypeCode)
_SCOPE_ALIASES = {
    "0": "JVM",
    "1": "CURRENT_JOB",
    "2": "PARENT_JOB",
    "3": "ROOT_JOB",
    "JAVA_VIRTUAL_MACHINE": "JVM",
    "VALID_IN_THE_JAVA_VIRTUAL_MACHINE": "JVM",
    "CURRENT": "CURRENT_JOB",
    "CURRENTJOB": "CURRENT_JOB",
    "VALID_IN_THE_CURRENT_JOB": "CURRENT_JOB",
    "PARENT": "PARENT_JOB",
    "PARENTJOB": "PARENT_JOB",
    "VALID_IN_THE_PARENT_JOB": "PARENT_JOB",
    "ROOT": "ROOT_JOB",
    "ROOTJOB": "ROOT_JOB",
    "VALID_IN_THE_ROOT_JOB": "ROOT_JOB",
}

_START_SCHEDULER_ATTRS = (
    "repeat",
    "schedulerType",
    "intervalSeconds",
    "intervalMinutes",
    "hour",
    "minutes",
    "weekDay",
    "DayOfMonth",
)


def _resolve(text: str, runtime: JobRuntime) -> str:
    return substitute_variables(text or "", runtime.variables)


def _normalize_var_scope(raw: str | None) -> str:
    text = (raw or "JVM").strip().upper().replace(" ", "_").replace("-", "_")
    return _SCOPE_ALIASES.get(text, text if text else "JVM")


def _field_value(field: Mapping[str, Any]) -> str:
    return str(
        field.get("variable_string")
        or field.get("variable_value")
        or ""
    )


def _should_replace(entry: JobEntry) -> bool:
    attrs = entry.attributes or {}
    raw = attrs.get("replace", attrs.get("replacevars", "Y"))
    return str(raw).upper() in {"Y", "TRUE", "1"}


def _evaluate_var_expression(raw: str, runtime: JobRuntime) -> str:
    """Resolve ``${var}`` and a small ``${x}+N`` arithmetic convenience."""
    if re.search(r"\$\{[^}]+\}\s*\+\s*\d+", raw) or raw.replace(" ", "").endswith("+1"):
        left = _resolve(re.sub(r"\s*\+\s*\d+\s*$", "", raw), runtime)
        m_add = re.search(r"\+\s*(\d+)\s*$", raw)
        add = int(m_add.group(1)) if m_add else 1
        try:
            return str(int(float(left)) + add)
        except ValueError:
            return _resolve(raw, runtime)
    return _resolve(raw, runtime)


def _apply_variable(
    runtime: JobRuntime,
    *,
    name: str,
    value: str,
    scope: str,
    replace: bool,
) -> None:
    """Apply a Set Variables assignment using PDI scope semantics."""
    scopes = list(getattr(runtime, "variable_scopes", None) or [runtime.variables])
    current = scopes[0]
    parent = scopes[1] if len(scopes) > 1 else None
    root = scopes[-1]

    def _write(target: dict[str, Any]) -> None:
        if replace or name not in target:
            target[name] = value

    if scope == "JVM":
        if replace or name not in os.environ:
            if value is not None:
                os.environ[name] = value
            elif name in os.environ:
                os.environ.pop(name, None)
        for target in scopes:
            _write(target)
        runtime.parameters[name] = value
        return

    if scope == "CURRENT_JOB":
        _write(current)
        runtime.parameters[name] = value
        return

    if scope == "PARENT_JOB":
        # PDI requires a parent of the current job (nesting depth >= 1 with
        # a grandparent check: parentJob.getParentJob() must exist).
        if parent is None:
            raise JobExecutionError(
                f"Unable to set variable '{name}' with PARENT_JOB scope — "
                "current job has no parent job"
            )
        _write(current)
        _write(parent)
        runtime.parameters[name] = value
        return

    if scope == "ROOT_JOB":
        for target in scopes:
            _write(target)
        # Ensure root is covered even if scopes were incomplete
        _write(root)
        runtime.parameters[name] = value
        return

    logging.warning(
        "SET_VARIABLES unknown scope %r for %s — treating as CURRENT_JOB",
        scope,
        name,
    )
    _write(current)
    runtime.parameters[name] = value


def _load_properties_file(path: str) -> dict[str, str]:
    """Load a UTF-8 ``.properties`` file (PDI Set Variables filename support)."""
    props: dict[str, str] = {}
    if not path:
        return props
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise JobExecutionError(
            f"Unable to read Set Variables properties file: {path} ({exc})"
        ) from exc
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("!"):
            continue
        if "=" in stripped:
            key, _, val = stripped.partition("=")
        elif ":" in stripped:
            key, _, val = stripped.partition(":")
        else:
            continue
        props[key.strip()] = val.strip()
    return props


def _child_config_from_entry(
    runtime: JobRuntime,
    entry: JobEntry,
    base_cfg: Mapping[str, Any],
) -> dict[str, Any]:
    """Build child JOB/TRANS config with parameter inheritance + overrides."""
    attrs = entry.attributes or {}
    child_cfg = dict(base_cfg)
    pass_all = str(attrs.get("pass_all_parameters", "Y")).upper() == "Y"
    if pass_all:
        child_cfg.update({k: str(v) for k, v in runtime.variables.items()})
    else:
        # Still forward Internal.* so relative paths resolve
        for key, val in runtime.variables.items():
            if str(key).startswith("Internal."):
                child_cfg[str(key)] = str(val)

    for param in attrs.get("parameters") or []:
        pname = (param.get("name") or "").strip()
        if not pname:
            continue
        child_cfg[pname] = _resolve(str(param.get("value") or ""), runtime)

    if str(attrs.get("exec_per_row", "N")).upper() == "Y":
        logging.warning(
            "ENTRY %s | name=%s | exec_per_row=Y is not supported — "
            "running once with current variables",
            entry.entry_type,
            entry.name,
        )
    if str(attrs.get("wait_until_finished", "Y")).upper() == "N":
        logging.warning(
            "ENTRY %s | name=%s | wait_until_finished=N is not supported — "
            "child always runs synchronously",
            entry.entry_type,
            entry.name,
        )
    return child_cfg


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
    """Start / Dummy pass-through (always succeeds).

    Start is identified by ``is_start=Y`` (type SPECIAL). Dummy may be
    ``type=DUMMY`` or a non-start SPECIAL. Scheduler attributes on Start are
    preserved on the entry but not executed — Databricks jobs own scheduling.
    """
    kind = "START" if entry.is_start else "DUMMY"
    if entry.is_start:
        attrs = entry.attributes or {}
        scheduled = [
            key
            for key in _START_SCHEDULER_ATTRS
            if str(attrs.get(key, "")).strip()
            and str(attrs.get(key, "")).upper() not in {"N", "0", "NOSCHEDULING", ""}
        ]
        # repeat=Y alone is the common Start default — only warn when a real schedule is set
        schedule_type = str(attrs.get("schedulerType", "") or "").strip()
        if schedule_type and schedule_type.upper() not in {"0", "NOSCHEDULING", "N"}:
            logging.warning(
                "ENTRY START | name=%s | schedulerType=%s is not translated — "
                "use Databricks Jobs schedules instead | attrs=%s",
                entry.name,
                schedule_type,
                scheduled,
            )
        logging.info("ENTRY START | name=%s | job=%s", entry.name, runtime.name)
    else:
        logging.info("ENTRY DUMMY | name=%s | pass-through", entry.name)
    return EntryResult(name=entry.name, success=True, result=kind)


def handle_success(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    logging.info("ENTRY SUCCESS | name=%s | job=%s", entry.name, runtime.name)
    return EntryResult(name=entry.name, success=True, result=True)


def handle_abort(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Abort Job — Pentaho ``ABORT``."""
    attrs = entry.attributes or {}
    msg = _resolve(
        str(attrs.get("message") or attrs.get("messageAbort") or "Abort"),
        runtime,
    )
    # PDI Abort has no separate error-code field; preserve custom attrs if present.
    code = str(attrs.get("errorcode") or attrs.get("error_code") or "").strip()
    if code:
        code = _resolve(code, runtime)
        logging.warning(
            "ENTRY ABORT | name=%s | errorcode=%s is non-standard — included in message",
            entry.name,
            code,
        )
        msg = f"[{code}] {msg}"
    logging.error("ENTRY ABORT | name=%s | %s", entry.name, msg)
    return EntryResult(
        name=entry.name,
        success=False,
        result=msg,
        error=JobExecutionError(msg),
    )


def handle_write_to_log(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Write to Log — Pentaho ``WRITE_TO_LOG``."""
    attrs = entry.attributes or {}
    msg = _resolve(str(attrs.get("logmessage") or ""), runtime)
    subject = _resolve(str(attrs.get("logsubject") or entry.name), runtime)
    level = _log_level(str(attrs.get("loglevel") or "BASIC"))
    if str(attrs.get("displayHeader", "Y")).upper() == "Y":
        logging.log(level, "======== Write to log: %s ========", subject)
    logging.log(level, "%s", msg)
    return EntryResult(
        name=entry.name, success=True, result={"message": msg, "subject": subject}
    )


def handle_msgbox_info(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Display MsgBox Info — Pentaho ``MSGBOX_INFO`` (log-only on Databricks)."""
    attrs = entry.attributes or {}
    title = _resolve(uops.attr(attrs, "titremessage", "title"), runtime)
    body = _resolve(uops.attr(attrs, "bodymessage", "message", "body"), runtime)
    outcome = uops.msgbox_info(title, body)
    uops.iter_warning_logs(f"ENTRY MSGBOX_INFO | name={entry.name}", outcome.warnings)
    return EntryResult(name=entry.name, success=True, result=outcome.extra)


def handle_ping(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Ping a Host — Pentaho ``PING``."""
    attrs = entry.attributes or {}
    outcome = uops.ping_host(
        _resolve(uops.attr(attrs, "hostname", "host"), runtime),
        timeout_ms=_resolve(uops.attr(attrs, "timeout", default="3000"), runtime),
        nbr_packets=_resolve(
            uops.attr(attrs, "nbr_packets", "nbrpaquets", default="2"), runtime
        ),
        pingtype=uops.attr(attrs, "pingtype", default="systemPing"),
    )
    uops.iter_warning_logs(f"ENTRY PING | name={entry.name}", outcome.warnings)
    logging.info("ENTRY PING | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_telnet(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Telnet a Host — Pentaho ``TELNET``."""
    attrs = entry.attributes or {}
    outcome = uops.telnet_host(
        _resolve(uops.attr(attrs, "hostname", "host"), runtime),
        _resolve(uops.attr(attrs, "port", default="23"), runtime),
        timeout_ms=_resolve(uops.attr(attrs, "timeout", default="3000"), runtime),
    )
    uops.iter_warning_logs(f"ENTRY TELNET | name={entry.name}", outcome.warnings)
    logging.info("ENTRY TELNET | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_syslog(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Send Information Using Syslog — Pentaho ``SYSLOG``."""
    attrs = entry.attributes or {}
    outcome = uops.send_syslog(
        _resolve(uops.attr(attrs, "servername", "server"), runtime),
        _resolve(uops.attr(attrs, "message"), runtime),
        port=_resolve(uops.attr(attrs, "port", default="514"), runtime),
        facility=uops.attr(attrs, "facility", default="USER"),
        priority=uops.attr(attrs, "priority", default="INFO"),
        date_pattern=_resolve(uops.attr(attrs, "datePattern", "datepattern"), runtime),
        add_timestamp=uops.attr_yn(attrs, "addTimestamp", "addtimestamp", default=True),
        add_hostname=uops.attr_yn(attrs, "addHostname", "addhostname", default=True),
    )
    uops.iter_warning_logs(f"ENTRY SYSLOG | name={entry.name}", outcome.warnings)
    logging.info("ENTRY SYSLOG | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_send_nagios_passive_check(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Send Nagios Passive Check — Pentaho ``SEND_NAGIOS_PASSIVE_CHECK``."""
    attrs = entry.attributes or {}
    outcome = uops.send_nagios_passive_check(
        _resolve(uops.attr(attrs, "servername", "server"), runtime),
        _resolve(uops.attr(attrs, "message"), runtime),
        port=_resolve(uops.attr(attrs, "port", default="5667"), runtime),
        password=_resolve(uops.attr(attrs, "password"), runtime),
        sender_server_name=_resolve(
            uops.attr(attrs, "senderServerName", "senderservername"), runtime
        ),
        sender_service_name=_resolve(
            uops.attr(attrs, "senderServiceName", "senderservicename"), runtime
        ),
        level=uops.attr(attrs, "level", default="0"),
        encryption_mode=uops.attr(attrs, "encryptionMode", "encryptionmode", default="0"),
        response_timeout=_resolve(
            uops.attr(attrs, "responseTimeOut", "responsetimeout", default="10000"),
            runtime,
        ),
        connection_timeout=_resolve(
            uops.attr(attrs, "connectionTimeOut", "connectiontimeout", default="5000"),
            runtime,
        ),
    )
    uops.iter_warning_logs(
        f"ENTRY SEND_NAGIOS_PASSIVE_CHECK | name={entry.name}", outcome.warnings
    )
    logging.info(
        "ENTRY SEND_NAGIOS_PASSIVE_CHECK | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_snmp_trap(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Send SNMP Trap — Pentaho ``SNMP_TRAP``."""
    attrs = entry.attributes or {}
    outcome = uops.send_snmp_trap(
        _resolve(uops.attr(attrs, "servername", "server"), runtime),
        _resolve(uops.attr(attrs, "message"), runtime),
        port=_resolve(uops.attr(attrs, "port", default="162"), runtime),
        oid=_resolve(uops.attr(attrs, "oid"), runtime),
        community=_resolve(uops.attr(attrs, "comstring", "community"), runtime),
        timeout=_resolve(uops.attr(attrs, "timeout", default="5000"), runtime),
        nrretry=_resolve(uops.attr(attrs, "nrretry", default="2"), runtime),
        targettype=uops.attr(attrs, "targettype", default="community"),
        user=_resolve(uops.attr(attrs, "user"), runtime),
        passphrase=_resolve(uops.attr(attrs, "passphrase"), runtime),
        engineid=_resolve(uops.attr(attrs, "engineid"), runtime),
    )
    uops.iter_warning_logs(f"ENTRY SNMP_TRAP | name={entry.name}", outcome.warnings)
    logging.info("ENTRY SNMP_TRAP | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_truncate_tables(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Truncate Tables — Pentaho ``TRUNCATE_TABLES``."""
    attrs = entry.attributes or {}
    tables: list[dict[str, str]] = []
    for row in attrs.get("fields") or []:
        if isinstance(row, Mapping):
            tables.append(
                {
                    "name": _resolve(str(row.get("name") or ""), runtime),
                    "schemaname": _resolve(str(row.get("schemaname") or ""), runtime),
                }
            )
    if uops.attr_yn(attrs, "arg_from_previous"):
        for item in fops.result_paths(runtime):
            # Approximate: path/name field as table name
            tables.append({"name": str(item.get("path") or ""), "schemaname": ""})
        logging.warning(
            "ENTRY TRUNCATE_TABLES | name=%s | arg_from_previous=Y approximated "
            "via result_filenames",
            entry.name,
        )
    conn_name = uops.attr(attrs, "connection")
    conn_meta = dict((runtime.connections or {}).get(conn_name) or {})
    outcome = uops.truncate_tables(
        tables,
        spark=getattr(runtime, "spark", None),
        connection_name=conn_name,
        connection_meta=conn_meta or None,
        catalog=_catalog_from_runtime(runtime),
    )
    uops.iter_warning_logs(f"ENTRY TRUNCATE_TABLES | name={entry.name}", outcome.warnings)
    logging.info("ENTRY TRUNCATE_TABLES | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_hl7_mllp_input(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """HL7 MLLP Input — Pentaho ``HL7MLLPInput``."""
    attrs = entry.attributes or {}
    outcome = uops.hl7_mllp_input(
        _resolve(uops.attr(attrs, "server", "hostname"), runtime),
        _resolve(uops.attr(attrs, "port"), runtime),
        message_variable=_resolve(uops.attr(attrs, "message_variable"), runtime),
        type_variable=_resolve(uops.attr(attrs, "type_variable"), runtime),
        version_variable=_resolve(uops.attr(attrs, "version_variable"), runtime),
    )
    uops.iter_warning_logs(f"ENTRY HL7MLLPInput | name={entry.name}", outcome.warnings)
    logging.info("ENTRY HL7MLLPInput | name=%s | %s", entry.name, outcome.message)
    for key, value in (outcome.extra.get("variables") or {}).items():
        if key:
            runtime.variables[key] = value
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_hl7_mllp_acknowledge(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """HL7 MLLP Acknowledge — Pentaho ``HL7MLLPAcknowledge``."""
    attrs = entry.attributes or {}
    var_name = _resolve(uops.attr(attrs, "variable"), runtime)
    message = ""
    if var_name:
        message = str(runtime.variables.get(var_name) or "")
    outcome = uops.hl7_mllp_acknowledge(
        _resolve(uops.attr(attrs, "server", "hostname"), runtime),
        _resolve(uops.attr(attrs, "port"), runtime),
        message=message,
        variable=var_name,
    )
    uops.iter_warning_logs(
        f"ENTRY HL7MLLPAcknowledge | name={entry.name}", outcome.warnings
    )
    logging.info(
        "ENTRY HL7MLLPAcknowledge | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_connected_to_repository(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Check if Connected to Repository — Pentaho ``CONNECTED_TO_REPOSITORY``."""
    attrs = entry.attributes or {}
    override = _resolve(
        str(
            runtime.variables.get("REPOSITORY_CONNECTED")
            or runtime.parameters.get("REPOSITORY_CONNECTED")
            or attrs.get("repository_connected")
            or ""
        ),
        runtime,
    )
    repo_meta = getattr(runtime, "repository", None)
    if not isinstance(repo_meta, Mapping):
        # Allow config-injected dict via variables JSON-ish keys
        repo_meta = {
            "connected": runtime.variables.get("REPOSITORY_META_CONNECTED", ""),
            "name": runtime.variables.get("REPOSITORY_META_NAME", ""),
            "username": runtime.variables.get("REPOSITORY_META_USERNAME", ""),
        }
        if not any(str(v) for v in repo_meta.values()):
            repo_meta = None
    outcome = rops.check_connected_to_repository(
        isspecificrep=rops.attr_yn(attrs, "isspecificrep"),
        repname=_resolve(rops.attr(attrs, "repname"), runtime),
        isspecificuser=rops.attr_yn(attrs, "isspecificuser"),
        username=_resolve(rops.attr(attrs, "username"), runtime),
        repository_meta=repo_meta,
        connected_override=override,
    )
    rops.iter_warning_logs(
        f"ENTRY CONNECTED_TO_REPOSITORY | name={entry.name}", outcome.warnings
    )
    logging.info(
        "ENTRY CONNECTED_TO_REPOSITORY | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_export_repository(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Export Repository to XML File — Pentaho ``EXPORT_REPOSITORY``."""
    attrs = entry.attributes or {}
    allow_stub = str(
        runtime.variables.get("EXPORT_REPOSITORY_ALLOW_STUB")
        or runtime.parameters.get("EXPORT_REPOSITORY_ALLOW_STUB")
        or "N"
    ).upper() in {"Y", "YES", "TRUE", "1"}
    local_roots: list[str] = []
    staged = runtime.variables.get("REPOSITORY_EXPORT_SOURCE") or runtime.parameters.get(
        "REPOSITORY_EXPORT_SOURCE"
    )
    if staged:
        local_roots.append(_resolve(str(staged), runtime))

    outcome = rops.export_repository_to_xml(
        repositoryname=_resolve(rops.attr(attrs, "repositoryname"), runtime),
        username=_resolve(rops.attr(attrs, "username"), runtime),
        password=_resolve(rops.attr(attrs, "password"), runtime),
        targetfilename=_data_path(rops.attr(attrs, "targetfilename"), runtime),
        iffileexists=rops.attr(attrs, "iffileexists", default="0"),
        export_type=rops.attr(attrs, "export_type", default="Export_All"),
        directory_path=_data_path(rops.attr(attrs, "directoryPath", "directorypath"), runtime),
        add_date=rops.attr_yn(attrs, "add_date"),
        add_time=rops.attr_yn(attrs, "add_time"),
        specify_format=rops.attr_yn(attrs, "SpecifyFormat", "specifyformat"),
        date_time_format=_resolve(
            rops.attr(attrs, "date_time_format", "datetimeformat"), runtime
        ),
        createfolder=rops.attr_yn(attrs, "createfolder", default=True),
        newfolder=rops.attr_yn(attrs, "newfolder"),
        add_result_filesname=rops.attr_yn(attrs, "add_result_filesname"),
        nr_errors_less_than=_resolve(
            rops.attr(attrs, "nr_errors_less_than", default="10"), runtime
        ),
        success_condition=rops.attr(attrs, "success_condition"),
        allow_stub=allow_stub,
        local_object_roots=local_roots,
    )
    rops.iter_warning_logs(
        f"ENTRY EXPORT_REPOSITORY | name={entry.name}", outcome.warnings
    )
    logging.info(
        "ENTRY EXPORT_REPOSITORY | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success and rops.attr_yn(attrs, "add_result_filesname") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def _transfer_result(
    entry: JobEntry,
    outcome: tops.TransferOutcome,
    *,
    label: str,
    runtime: JobRuntime,
    add_paths: bool = True,
) -> EntryResult:
    tops.iter_warning_logs(f"ENTRY {label} | name={entry.name}", outcome.warnings)
    logging.info("ENTRY %s | name=%s | %s", label, entry.name, outcome.message)
    if outcome.success and add_paths and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"paths": outcome.paths, **outcome.extra},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, **outcome.extra},
    )


def handle_ftp_get(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Get a File with FTP — Pentaho ``FTP``."""
    attrs = entry.attributes or {}
    outcome = tops.ftp_get(
        servername=_resolve(tops.attr(attrs, "servername"), runtime),
        port=_resolve(tops.attr(attrs, "port", default="21"), runtime),
        username=_resolve(tops.attr(attrs, "username"), runtime),
        password=_resolve(tops.attr(attrs, "password"), runtime),
        ftpdirectory=_resolve(tops.attr(attrs, "ftpdirectory"), runtime),
        targetdirectory=_data_path(tops.attr(attrs, "targetdirectory"), runtime),
        wildcard=_resolve(tops.attr(attrs, "wildcard"), runtime),
        binary=tops.attr_yn(attrs, "binary", default=True),
        timeout=_resolve(tops.attr(attrs, "timeout", default="30"), runtime),
        remove=tops.attr_yn(attrs, "remove"),
        only_new=tops.attr_yn(attrs, "only_new"),
        active=tops.attr_yn(attrs, "active"),
        control_encoding=_resolve(
            tops.attr(attrs, "control_encoding", default="UTF-8"), runtime
        ),
        isaddresult=tops.attr_yn(attrs, "isaddresult", default=True),
        if_file_exists=tops.attr(attrs, "ifFileExists", "iffileexists", default="0"),
        nr_limit=_resolve(tops.attr(attrs, "nr_limit", default="10"), runtime),
        success_condition=tops.attr(attrs, "success_condition"),
        proxy_host=_resolve(tops.attr(attrs, "proxy_host"), runtime),
        socksproxy_host=_resolve(tops.attr(attrs, "socksproxy_host"), runtime),
        movefiles=tops.attr_yn(attrs, "movefiles"),
        movetodirectory=_resolve(tops.attr(attrs, "movetodirectory"), runtime),
    )
    return _transfer_result(
        entry,
        outcome,
        label="FTP",
        runtime=runtime,
        add_paths=tops.attr_yn(attrs, "isaddresult", default=True),
    )


def handle_ftps_get(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Get a File with FTPS — Pentaho ``FTPS_GET``."""
    attrs = entry.attributes or {}
    outcome = tops.ftps_get(
        servername=_resolve(tops.attr(attrs, "servername"), runtime),
        port=_resolve(tops.attr(attrs, "port", default="21"), runtime),
        username=_resolve(tops.attr(attrs, "username"), runtime),
        password=_resolve(tops.attr(attrs, "password"), runtime),
        ftpdirectory=_resolve(
            tops.attr(attrs, "FTPSdirectory", "ftpsdirectory", "ftpdirectory"), runtime
        ),
        targetdirectory=_data_path(tops.attr(attrs, "targetdirectory"), runtime),
        wildcard=_resolve(tops.attr(attrs, "wildcard"), runtime),
        binary=tops.attr_yn(attrs, "binary", default=True),
        timeout=_resolve(tops.attr(attrs, "timeout", default="30"), runtime),
        remove=tops.attr_yn(attrs, "remove"),
        only_new=tops.attr_yn(attrs, "only_new"),
        active=tops.attr_yn(attrs, "active"),
        isaddresult=tops.attr_yn(attrs, "isaddresult", default=True),
        if_file_exists=tops.attr(attrs, "ifFileExists", "iffileexists", default="0"),
        nr_limit=_resolve(tops.attr(attrs, "nr_limit", default="10"), runtime),
        success_condition=tops.attr(attrs, "success_condition"),
        proxy_host=_resolve(tops.attr(attrs, "proxy_host"), runtime),
        connection_type=tops.attr(attrs, "connection_type"),
        movefiles=tops.attr_yn(attrs, "movefiles"),
        movetodirectory=_resolve(tops.attr(attrs, "movetodirectory"), runtime),
    )
    return _transfer_result(
        entry,
        outcome,
        label="FTPS_GET",
        runtime=runtime,
        add_paths=tops.attr_yn(attrs, "isaddresult", default=True),
    )


def handle_sftp_get(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Get a File with SFTP — Pentaho ``SFTP``."""
    attrs = entry.attributes or {}
    prev: list[str] = []
    if tops.attr_yn(attrs, "copyprevious"):
        prev = [str(i.get("path", "")) for i in fops.result_paths(runtime)]
    outcome = tops.sftp_get(
        servername=_resolve(tops.attr(attrs, "servername"), runtime),
        port=_resolve(tops.attr(attrs, "serverport", "port", default="22"), runtime),
        username=_resolve(tops.attr(attrs, "username"), runtime),
        password=_resolve(tops.attr(attrs, "password"), runtime),
        sftpdirectory=_resolve(tops.attr(attrs, "sftpdirectory"), runtime),
        targetdirectory=_data_path(tops.attr(attrs, "targetdirectory"), runtime),
        wildcard=_resolve(tops.attr(attrs, "wildcard"), runtime),
        remove=tops.attr_yn(attrs, "remove"),
        isaddresult=tops.attr_yn(attrs, "isaddresult", default=True),
        createtargetfolder=tops.attr_yn(attrs, "createtargetfolder", default=True),
        copyprevious=tops.attr_yn(attrs, "copyprevious"),
        previous_names=prev or None,
        usekeyfilename=tops.attr_yn(attrs, "usekeyfilename"),
        keyfilename=_data_path(tops.attr(attrs, "keyfilename"), runtime),
        keyfilepass=_resolve(tops.attr(attrs, "keyfilepass"), runtime),
        compression=tops.attr_yn(attrs, "compression"),
        proxy_host=_resolve(tops.attr(attrs, "proxyHost", "proxy_host"), runtime),
        proxy_type=tops.attr(attrs, "proxyType", "proxytype"),
    )
    return _transfer_result(
        entry,
        outcome,
        label="SFTP",
        runtime=runtime,
        add_paths=tops.attr_yn(attrs, "isaddresult", default=True),
    )


def handle_ftp_put(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Put a File with FTP — Pentaho ``FTP_PUT``."""
    attrs = entry.attributes or {}
    outcome = tops.ftp_put(
        servername=_resolve(tops.attr(attrs, "servername"), runtime),
        port=_resolve(tops.attr(attrs, "serverport", "port", default="21"), runtime),
        username=_resolve(tops.attr(attrs, "username"), runtime),
        password=_resolve(tops.attr(attrs, "password"), runtime),
        remote_directory=_resolve(
            tops.attr(attrs, "remoteDirectory", "remotedirectory"), runtime
        ),
        local_directory=_data_path(
            tops.attr(attrs, "localDirectory", "localdirectory"), runtime
        ),
        wildcard=_resolve(tops.attr(attrs, "wildcard", default="*"), runtime),
        binary=tops.attr_yn(attrs, "binary", default=True),
        timeout=_resolve(tops.attr(attrs, "timeout", default="30"), runtime),
        remove=tops.attr_yn(attrs, "remove"),
        only_new=tops.attr_yn(attrs, "only_new"),
        active=tops.attr_yn(attrs, "active"),
        control_encoding=_resolve(
            tops.attr(attrs, "control_encoding", default="UTF-8"), runtime
        ),
        proxy_host=_resolve(tops.attr(attrs, "proxy_host"), runtime),
        socksproxy_host=_resolve(tops.attr(attrs, "socksproxy_host"), runtime),
    )
    return _transfer_result(entry, outcome, label="FTP_PUT", runtime=runtime, add_paths=False)


def handle_sftp_put(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Put a File with SFTP — Pentaho ``SFTPPUT``."""
    attrs = entry.attributes or {}
    prev = [str(i.get("path", "")) for i in fops.result_paths(runtime)]
    outcome = tops.sftp_put(
        servername=_resolve(tops.attr(attrs, "servername"), runtime),
        port=_resolve(tops.attr(attrs, "serverport", "port", default="22"), runtime),
        username=_resolve(tops.attr(attrs, "username"), runtime),
        password=_resolve(tops.attr(attrs, "password"), runtime),
        sftpdirectory=_resolve(tops.attr(attrs, "sftpdirectory"), runtime),
        localdirectory=_data_path(
            tops.attr(attrs, "localdirectory", "localDirectory"), runtime
        ),
        wildcard=_resolve(tops.attr(attrs, "wildcard", default="*"), runtime),
        copyprevious=tops.attr_yn(attrs, "copyprevious"),
        copypreviousfiles=tops.attr_yn(attrs, "copypreviousfiles"),
        previous_paths=prev,
        add_filename_result=tops.attr_yn(attrs, "addFilenameResut", "addfilenameresult"),
        usekeyfilename=tops.attr_yn(attrs, "usekeyfilename"),
        keyfilename=_data_path(tops.attr(attrs, "keyfilename"), runtime),
        keyfilepass=_resolve(tops.attr(attrs, "keyfilepass"), runtime),
        compression=tops.attr_yn(attrs, "compression"),
        create_remote_folder=tops.attr_yn(attrs, "createRemoteFolder", "createremotefolder"),
        aftersftpput=tops.attr(attrs, "aftersftpput", default="nothing"),
        destinationfolder=_data_path(tops.attr(attrs, "destinationfolder"), runtime),
        success_when_no_file=tops.attr_yn(attrs, "successWhenNoFile", "successwhennofile"),
        proxy_host=_resolve(tops.attr(attrs, "proxyHost", "proxy_host"), runtime),
        proxy_type=tops.attr(attrs, "proxyType", "proxytype"),
    )
    return _transfer_result(
        entry,
        outcome,
        label="SFTPPUT",
        runtime=runtime,
        add_paths=tops.attr_yn(attrs, "addFilenameResut", "addfilenameresult"),
    )


def handle_ftps_put(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Upload Files to FTPS — Pentaho ``FTPS_PUT``."""
    attrs = entry.attributes or {}
    outcome = tops.ftps_put(
        servername=_resolve(tops.attr(attrs, "servername"), runtime),
        port=_resolve(tops.attr(attrs, "serverport", "port", default="21"), runtime),
        username=_resolve(tops.attr(attrs, "username"), runtime),
        password=_resolve(tops.attr(attrs, "password"), runtime),
        remote_directory=_resolve(
            tops.attr(attrs, "remoteDirectory", "remotedirectory"), runtime
        ),
        local_directory=_data_path(
            tops.attr(attrs, "localDirectory", "localdirectory"), runtime
        ),
        wildcard=_resolve(tops.attr(attrs, "wildcard", default="*"), runtime),
        binary=tops.attr_yn(attrs, "binary", default=True),
        timeout=_resolve(tops.attr(attrs, "timeout", default="30"), runtime),
        remove=tops.attr_yn(attrs, "remove"),
        only_new=tops.attr_yn(attrs, "only_new"),
        active=tops.attr_yn(attrs, "active"),
        proxy_host=_resolve(tops.attr(attrs, "proxy_host"), runtime),
        connection_type=tops.attr(attrs, "connection_type"),
    )
    return _transfer_result(entry, outcome, label="FTPS_PUT", runtime=runtime, add_paths=False)


def handle_ftp_delete(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """FTP Delete — Pentaho ``FTP_DELETE`` (FTP / FTPS / SFTP protocols)."""
    attrs = entry.attributes or {}
    prev: list[str] = []
    if tops.attr_yn(attrs, "copyprevious"):
        prev = [
            Path(str(i.get("path", ""))).name for i in fops.result_paths(runtime)
        ]
    outcome = tops.ftp_delete(
        protocol=_resolve(tops.attr(attrs, "protocol", default="FTP"), runtime),
        servername=_resolve(tops.attr(attrs, "servername"), runtime),
        port=_resolve(tops.attr(attrs, "port"), runtime),
        username=_resolve(tops.attr(attrs, "username"), runtime),
        password=_resolve(tops.attr(attrs, "password"), runtime),
        ftpdirectory=_resolve(tops.attr(attrs, "ftpdirectory"), runtime),
        wildcard=_resolve(tops.attr(attrs, "wildcard"), runtime),
        timeout=_resolve(tops.attr(attrs, "timeout", default="30"), runtime),
        active=tops.attr_yn(attrs, "active"),
        useproxy=tops.attr_yn(attrs, "useproxy"),
        proxy_host=_resolve(tops.attr(attrs, "proxy_host"), runtime),
        socksproxy_host=_resolve(tops.attr(attrs, "socksproxy_host"), runtime),
        publicpublickey=tops.attr_yn(attrs, "publicpublickey"),
        keyfilename=_data_path(tops.attr(attrs, "keyfilename"), runtime),
        keyfilepass=_resolve(tops.attr(attrs, "keyfilepass"), runtime),
        ftps_connection_type=tops.attr(attrs, "ftps_connection_type", "connection_type"),
        copyprevious=tops.attr_yn(attrs, "copyprevious"),
        previous_names=prev or None,
        nr_limit_success=_resolve(
            tops.attr(attrs, "nr_limit_success", default="10"), runtime
        ),
        success_condition=tops.attr(attrs, "success_condition"),
    )
    return _transfer_result(
        entry, outcome, label="FTP_DELETE", runtime=runtime, add_paths=False
    )


def _pgp_rows_from_attrs(
    attrs: Mapping[str, Any], runtime: JobRuntime, *, decrypt: bool = False
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in attrs.get("fields") or []:
        if not isinstance(row, Mapping):
            continue
        item = {
            "source_filefolder": _data_path(
                str(row.get("source_filefolder") or row.get("source") or ""), runtime
            ),
            "destination_filefolder": _data_path(
                str(
                    row.get("destination_filefolder")
                    or row.get("destination")
                    or ""
                ),
                runtime,
            ),
            "wildcard": _resolve(str(row.get("wildcard") or ""), runtime),
        }
        if decrypt:
            item["passphrase"] = _resolve(str(row.get("passphrase") or ""), runtime)
        else:
            item["userid"] = _resolve(
                str(row.get("userid") or row.get("recipient") or ""), runtime
            )
            item["action_type"] = str(row.get("action_type") or "encrypt")
        rows.append(item)
    if pops.attr_yn(attrs, "arg_from_previous"):
        for item in fops.result_paths(runtime):
            rows.append(
                {
                    "source_filefolder": str(item.get("path") or ""),
                    "destination_filefolder": _data_path(
                        pops.attr(attrs, "destinationFolder", "destinationfolder"),
                        runtime,
                    ),
                    "wildcard": "",
                    "passphrase": _resolve(pops.attr(attrs, "passphrase"), runtime)
                    if decrypt
                    else "",
                    "userid": _resolve(pops.attr(attrs, "userid"), runtime),
                    "action_type": "encrypt",
                }
            )
    return rows


def handle_pgp_encrypt_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Encrypt Files with PGP — Pentaho ``PGP_ENCRYPT_FILES``."""
    attrs = entry.attributes or {}
    rows = _pgp_rows_from_attrs(attrs, runtime, decrypt=False)
    outcome = pops.encrypt_files(
        rows,
        gpglocation=_data_path(pops.attr(attrs, "gpglocation"), runtime),
        gnupghome=_resolve(
            str(
                runtime.variables.get("GNUPGHOME")
                or runtime.variables.get("GPG_HOME")
                or ""
            ),
            runtime,
        ),
        public_key_file=_data_path(
            pops.attr(
                attrs,
                "publickeyfile",
                "public_key_file",
                "keyfilename",
                "keyfile",
            ),
            runtime,
        ),
        ascii_mode=pops.attr_yn(attrs, "asciiMode", "asciimode", default=True),
        include_subfolders=pops.attr_yn(attrs, "include_subfolders"),
        create_destination_folder=pops.attr_yn(
            attrs, "create_destination_folder", default=True
        ),
        destination_is_a_file=pops.attr_yn(attrs, "destination_is_a_file"),
        iffileexists=pops.attr(attrs, "iffileexists", default="0"),
        add_result_filesname=pops.attr_yn(attrs, "add_result_filesname"),
        nr_errors_less_than=_resolve(
            pops.attr(attrs, "nr_errors_less_than", default="10"), runtime
        ),
        success_condition=pops.attr(attrs, "success_condition"),
        compression=pops.attr(attrs, "compression", "compressionalgorithm"),
    )
    # Warn about move-to options not fully implemented
    if pops.attr(attrs, "destinationFolder"):
        logging.warning(
            "ENTRY PGP_ENCRYPT_FILES | name=%s | destinationFolder / move-after "
            "options are preserved but not fully applied",
            entry.name,
        )
    pops.iter_warning_logs(f"ENTRY PGP_ENCRYPT_FILES | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY PGP_ENCRYPT_FILES | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success and pops.attr_yn(attrs, "add_result_filesname"):
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name, success=True, result={"paths": outcome.paths, **outcome.extra}
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, **outcome.extra},
    )


def handle_pgp_decrypt_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Decrypt Files with PGP — Pentaho ``PGP_DECRYPT_FILES``."""
    attrs = entry.attributes or {}
    rows = _pgp_rows_from_attrs(attrs, runtime, decrypt=True)
    # Passphrase: field rows, attribute, or env (handled inside ops)
    default_pass = _resolve(
        pops.attr(attrs, "passphrase")
        or str(runtime.variables.get("GPG_PASSPHRASE") or ""),
        runtime,
    )
    outcome = pops.decrypt_files(
        rows,
        gpglocation=_data_path(pops.attr(attrs, "gpglocation"), runtime),
        gnupghome=_resolve(
            str(
                runtime.variables.get("GNUPGHOME")
                or runtime.variables.get("GPG_HOME")
                or ""
            ),
            runtime,
        ),
        private_key_file=_data_path(
            pops.attr(
                attrs,
                "privatekeyfile",
                "secretkeyfile",
                "private_key_file",
                "keyfilename",
            ),
            runtime,
        ),
        default_passphrase=default_pass,
        include_subfolders=pops.attr_yn(attrs, "include_subfolders"),
        create_destination_folder=pops.attr_yn(
            attrs, "create_destination_folder", default=True
        ),
        destination_is_a_file=pops.attr_yn(attrs, "destination_is_a_file"),
        iffileexists=pops.attr(attrs, "iffileexists", default="0"),
        add_result_filesname=pops.attr_yn(attrs, "add_result_filesname"),
        nr_errors_less_than=_resolve(
            pops.attr(attrs, "nr_errors_less_than", default="10"), runtime
        ),
        success_condition=pops.attr(attrs, "success_condition"),
        integrity_check=pops.attr_yn(
            attrs, "integritycheck", "integrity_check", default=True
        ),
    )
    pops.iter_warning_logs(f"ENTRY PGP_DECRYPT_FILES | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY PGP_DECRYPT_FILES | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success and pops.attr_yn(attrs, "add_result_filesname"):
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name, success=True, result={"paths": outcome.paths, **outcome.extra}
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, **outcome.extra},
    )


def handle_pgp_verify_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Verify File Signature with PGP — Pentaho ``PGP_VERIFY_FILES``."""
    attrs = entry.attributes or {}
    outcome = pops.verify_signature(
        filename=_data_path(pops.attr(attrs, "filename"), runtime),
        detached_filename=_data_path(
            pops.attr(attrs, "detachedfilename", "detached_filename"), runtime
        ),
        use_detached_signature=pops.attr_yn(
            attrs, "useDetachedSignature", "usedetachedsignature"
        ),
        gpglocation=_data_path(pops.attr(attrs, "gpglocation"), runtime),
        gnupghome=_resolve(
            str(
                runtime.variables.get("GNUPGHOME")
                or runtime.variables.get("GPG_HOME")
                or ""
            ),
            runtime,
        ),
        public_key_file=_data_path(
            pops.attr(attrs, "publickeyfile", "public_key_file", "keyfilename"),
            runtime,
        ),
    )
    pops.iter_warning_logs(f"ENTRY PGP_VERIFY_FILES | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY PGP_VERIFY_FILES | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result=outcome.extra,
    )


def handle_set_variables(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Set Variables job entry — JVM / CURRENT_JOB / PARENT_JOB / ROOT_JOB scopes.

    Also supports optional properties ``filename`` with ``file_variable_type``.
    """
    replace = _should_replace(entry)
    applied: dict[str, str] = {}
    scopes_used: dict[str, str] = {}
    attrs = entry.attributes or {}

    try:
        # 1) Optional properties file (same order as PDI: file first, then fields)
        filename = entry.filename or attrs.get("filename") or ""
        file_vars: list[tuple[str, str, str]] = []
        if filename:
            resolved_path = _data_path(str(filename), runtime)
            file_scope = _normalize_var_scope(
                str(attrs.get("file_variable_type") or "JVM")
            )
            for key, val in _load_properties_file(resolved_path).items():
                file_vars.append((key, val, file_scope))

        # 2) Explicit field mappings (may override file keys when replace=Y)
        field_vars: list[tuple[str, str, str]] = []
        for field in attrs.get("fields") or []:
            vname = (field.get("variable_name") or "").strip()
            if not vname:
                continue
            raw = _field_value(field)
            scope = _normalize_var_scope(field.get("variable_type"))
            field_vars.append((vname, raw, scope))

        # Merge: start from file, then fields (PDI appends fields; replace controls
        # whether an existing file key is overwritten by a field of the same name).
        merged: dict[str, tuple[str, str]] = {}
        for vname, raw, scope in file_vars:
            merged[vname] = (raw, scope)
        for vname, raw, scope in field_vars:
            if vname in merged and not replace:
                continue
            merged[vname] = (raw, scope)

        for vname, (raw, scope) in merged.items():
            if replace:
                resolved_name = _resolve(vname, runtime)
                value = _evaluate_var_expression(raw, runtime)
            else:
                # PDI: replaceVars=false → no environmentSubstitute on name/value
                resolved_name = vname
                value = raw
                if resolved_name in runtime.variables:
                    continue
            _apply_variable(
                runtime,
                name=resolved_name,
                value=value,
                scope=scope,
                replace=True,
            )
            applied[resolved_name] = value
            scopes_used[resolved_name] = scope

    except JobExecutionError as exc:
        logging.error("ENTRY SET_VARIABLES FAIL | name=%s | %s", entry.name, exc)
        return EntryResult(name=entry.name, success=False, error=exc)
    except Exception as exc:  # noqa: BLE001
        logging.error("ENTRY SET_VARIABLES FAIL | name=%s | %s", entry.name, exc)
        return EntryResult(name=entry.name, success=False, error=exc)

    logging.info(
        "ENTRY SET_VARIABLES | name=%s | applied=%s | scopes=%s",
        entry.name,
        applied,
        scopes_used,
    )
    return EntryResult(
        name=entry.name,
        success=True,
        result={"applied": applied, "scopes": scopes_used},
    )


def handle_shell(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    work = _data_path(sops.attr(attrs, "work_directory"), runtime)
    insert_script = sops.attr_yn(attrs, "insertScript", "insertscript", default=True)
    script = sops.attr(attrs, "script")
    filename = entry.filename or sops.attr(attrs, "filename")
    # Resolve ${var} then %VAR%
    script_r = _resolve(script, runtime) if script else ""
    filename_r = _resolve(filename, runtime) if filename else ""
    args_raw = attrs.get("arguments") or []
    if isinstance(args_raw, str):
        args_raw = [args_raw]
    arguments = [_resolve(str(a), runtime) for a in args_raw]

    if sops.attr_yn(attrs, "arg_from_previous"):
        sops.iter_warning_logs(
            f"ENTRY SHELL | name={entry.name}",
            ["arg_from_previous=Y is unsupported — arguments from result rows ignored"],
        )
    if sops.attr_yn(attrs, "exec_per_row"):
        sops.iter_warning_logs(
            f"ENTRY SHELL | name={entry.name}",
            ["exec_per_row=Y is unsupported — running once"],
        )
    if sops.attr_yn(attrs, "set_logfile"):
        sops.iter_warning_logs(
            f"ENTRY SHELL | name={entry.name}",
            ["set_logfile=Y is unsupported — shell output goes to job logger/stdout"],
        )

    timeout_raw = sops.attr(attrs, "timeout", "maximumTimeout")
    timeout = float(timeout_raw) if timeout_raw not in ("", None) else None

    outcome = sops.run_shell(
        script=script_r,
        filename=filename_r,
        insert_script=insert_script or bool(script_r),
        arguments=arguments,
        work_directory=work,
        variables=runtime.variables,
        timeout=timeout,
    )
    sops.iter_warning_logs(f"ENTRY SHELL | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY SHELL | name=%s | success=%s | exit=%s | %s",
        entry.name,
        outcome.success,
        outcome.exit_code,
        outcome.message,
    )
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={
                "stdout": outcome.stdout,
                "stderr": outcome.stderr,
                "exit_code": outcome.exit_code,
                **outcome.extra,
            },
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={
            "stdout": outcome.stdout,
            "stderr": outcome.stderr,
            "exit_code": outcome.exit_code,
            **outcome.extra,
        },
    )


def handle_sql(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    connection = sops.attr(attrs, "connection")
    use_vars = sops.attr_yn(attrs, "useVariableSubstitution", default=True)
    sql_from_file = sops.attr_yn(attrs, "sqlfromfile", "sqlFromFile")
    sql_filename = sops.attr(attrs, "sqlfilename", "sqlFilename")
    send_one = sops.attr_yn(attrs, "sendOneStatement", "sendonestatement")
    raw_sql = sops.attr(attrs, "sql")

    try:
        sql_text, load_warnings = sops.load_sql_text(
            sql=raw_sql,
            sql_from_file=sql_from_file,
            sql_filename=sql_filename,
            resolve=lambda s: _data_path(s, runtime),
        )
    except OSError as exc:
        logging.error("ENTRY SQL FAIL | name=%s | %s", entry.name, exc)
        return EntryResult(name=entry.name, success=False, error=exc)

    if use_vars:
        sql_text = _resolve(sql_text, runtime)
        # Also expand %VAR% style for parity with Shell
        sql_text = sops.expand_shell_percent_vars(sql_text, runtime.variables)

    statements = sops.split_sql_statements(sql_text, send_one_statement=send_one)
    conn_meta = (runtime.connections or {}).get(connection) if connection else None
    outcome = sops.execute_sql_statements(
        statements,
        spark=getattr(runtime, "spark", None),
        connection_meta=conn_meta,
        connection_name=connection,
    )
    outcome.warnings = list(load_warnings) + list(outcome.warnings)
    sops.iter_warning_logs(f"ENTRY SQL | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY SQL | name=%s | connection=%s | stmts=%s | success=%s | %s",
        entry.name,
        connection,
        len(statements),
        outcome.success,
        outcome.message,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result=outcome.extra,
    )


def handle_eval(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """JavaScript job entry (PDI type ``EVAL``)."""
    attrs = entry.attributes or {}
    script = sops.attr(attrs, "script")
    # Allow ${var} inside the script text before JS→Python translation
    script_r = _resolve(script, runtime)
    outcome = sops.evaluate_javascript(script_r, variables=runtime.variables)
    sops.iter_warning_logs(f"ENTRY EVAL | name={entry.name}", outcome.warnings)
    if outcome.extra.get("mode") == "todo":
        logging.warning(
            "ENTRY EVAL TODO | name=%s | original JavaScript preserved:\n%s",
            entry.name,
            script_r,
        )
    logging.info(
        "ENTRY EVAL | name=%s | success=%s | %s",
        entry.name,
        outcome.success,
        outcome.message,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    # Boolean false from a valid script is a normal failure (no exception required)
    if outcome.extra.get("mode") in {"translated", "literal"} and outcome.error is None:
        return EntryResult(name=entry.name, success=False, result=outcome.extra)
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result=outcome.extra,
    )


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
    attrs = entry.attributes or {}
    filename = _data_path(
        entry.filename or fops.attr(attrs, "filename"),
        runtime,
    )
    timeout = float(
        fops.attr(attrs, "maximumTimeout", "maximum_timeout", default="0") or 0
    )
    cycle = float(
        fops.attr(attrs, "checkCycleTime", "check_cycle_time", default="1") or 1
    )
    success_on_timeout = fops.attr_yn(
        attrs, "successOnTimeout", "success_on_timeout"
    )
    file_size_check = fops.attr_yn(attrs, "fileSizeCheck", "file_size_check")
    add_to_result = fops.attr_yn(attrs, "addFilenameResult", "add_filename_result")

    logging.info(
        "ENTRY WAIT_FOR_FILE | name=%s | file=%s | timeout=%ss | cycle=%ss | "
        "successOnTimeout=%s",
        entry.name,
        filename,
        timeout,
        cycle,
        success_on_timeout,
    )
    outcome = fops.wait_for_file(
        filename,
        timeout=timeout,
        cycle=max(cycle, 0.1),
        success_on_timeout=success_on_timeout,
        file_size_check=file_size_check,
        exists_fn=_fs_exists,
    )
    fops.iter_warning_logs(f"ENTRY WAIT_FOR_FILE | name={entry.name}", outcome.warnings)
    if outcome.success and add_to_result and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result=outcome.paths[0] if outcome.paths else None,
        )
    return EntryResult(name=entry.name, success=False, error=outcome.error)


def handle_simple_eval(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    valuetype = cops.attr(attrs, "valuetype", default="variable").lower()
    varname = cops.attr(attrs, "variablename", "fieldname")
    fieldtype = cops.attr(attrs, "fieldtype", default="STRING")
    successwhenvarset = cops.attr_yn(attrs, "successwhenvarset")

    var_is_set: bool | None = None
    if valuetype in {"variable", "1"} or varname:
        key = varname
        # successwhenvarset: variable present (even if empty string after resolve?)
        var_is_set = key in runtime.variables or key in (runtime.parameters or {})
        left_expr = varname if varname.startswith("${") else ("${" + varname + "}")
        left = _resolve(left_expr, runtime)
        # If unresolved ${VAR} remains and var not set, left may still be literal
        if not var_is_set and left == left_expr:
            left = ""
    else:
        left = _resolve(cops.attr(attrs, "fieldname"), runtime)

    outcome = cops.simple_eval(
        left=left,
        compare=_resolve(cops.attr(attrs, "comparevalue"), runtime),
        minvalue=_resolve(cops.attr(attrs, "minvalue"), runtime),
        maxvalue=_resolve(cops.attr(attrs, "maxvalue"), runtime),
        fieldtype=fieldtype,
        successcondition=cops.attr(attrs, "successcondition", default="equal"),
        successnumbercondition=cops.attr(
            attrs, "successnumbercondition", default="equal"
        ),
        successbooleancondition=cops.attr(
            attrs, "successbooleancondition", default="false"
        ),
        successwhenvarset=successwhenvarset,
        mask=_resolve(cops.attr(attrs, "mask"), runtime),
        var_is_set=var_is_set,
    )
    cops.iter_warning_logs(f"ENTRY SIMPLE_EVAL | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY SIMPLE_EVAL | name=%s | left=%r | → %s | %s",
        entry.name,
        left,
        outcome.success,
        outcome.message,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.value)
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error,
        result=outcome.value,
    )


def handle_delay(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    timeout = float(
        cops.attr(attrs, "maximumTimeout", "maximum_timeout", default="0") or 0
    )
    scale = cops.attr(attrs, "scaletime", default="0")
    seconds = cops.delay_seconds(timeout, scale)
    logging.info("ENTRY DELAY | name=%s | sleep=%ss", entry.name, seconds)
    time.sleep(seconds)
    return EntryResult(name=entry.name, success=True, result=seconds)


def handle_folder_is_empty(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    folder = _data_path(cops.attr(attrs, "foldername", "filename"), runtime)
    outcome = cops.folder_is_empty(
        folder,
        include_subfolders=cops.attr_yn(attrs, "include_subfolders"),
        wildcard=_resolve(cops.attr(attrs, "wildcard"), runtime),
        specify_wildcard=cops.attr_yn(attrs, "specify_wildcard"),
    )
    cops.iter_warning_logs(f"ENTRY FOLDER_IS_EMPTY | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY FOLDER_IS_EMPTY | name=%s | folder=%s | empty=%s",
        entry.name,
        folder,
        outcome.success,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=True)
    return EntryResult(name=entry.name, success=False, error=outcome.error, result=False)


def handle_files_exist(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    paths: list[str] = []
    single = entry.filename or cops.attr(attrs, "filename")
    if single:
        paths.append(_data_path(single, runtime))
    for row in attrs.get("fields") or []:
        if isinstance(row, Mapping):
            name = str(row.get("name") or row.get("filename") or "").strip()
            if name:
                paths.append(_data_path(name, runtime))
    outcome = cops.files_exist(paths, exists_fn=_fs_exists)
    logging.info(
        "ENTRY FILES_EXIST | name=%s | paths=%s | ok=%s",
        entry.name,
        len(paths),
        outcome.success,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_check_files_locked(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    pairs: list[dict[str, str]] = []
    for row in attrs.get("fields") or []:
        if isinstance(row, Mapping):
            pairs.append(
                {
                    "source": _data_path(
                        str(row.get("name") or row.get("filename") or ""), runtime
                    ),
                    "wildcard": _resolve(
                        str(row.get("filemask") or row.get("wildcard") or ""), runtime
                    ),
                }
            )
    if not pairs:
        single = cops.attr(attrs, "filename", "foldername")
        if single:
            pairs.append({"source": _data_path(single, runtime), "wildcard": ""})
    if cops.attr_yn(attrs, "arg_from_previous"):
        for item in fops.result_paths(runtime):
            pairs.append({"source": str(item.get("path", "")), "wildcard": ""})
    outcome = cops.check_files_locked(
        pairs, include_subfolders=cops.attr_yn(attrs, "include_subfolders")
    )
    cops.iter_warning_logs(
        f"ENTRY CHECK_FILES_LOCKED | name={entry.name}", outcome.warnings
    )
    logging.info(
        "ENTRY CHECK_FILES_LOCKED | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_webservice_available(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    url = _resolve(cops.attr(attrs, "url"), runtime)
    connect_to = float(cops.attr(attrs, "connectTimeOut", "connecttimeout", default="0") or 0)
    read_to = float(cops.attr(attrs, "readTimeOut", "readtimeout", default="0") or 0)
    # PDI timeouts are often milliseconds
    if connect_to > 1000:
        connect_to /= 1000.0
    if read_to > 1000:
        read_to /= 1000.0
    outcome = cops.webservice_available(
        url, connect_timeout=connect_to, read_timeout=read_to
    )
    logging.info(
        "ENTRY WEBSERVICE_AVAILABLE | name=%s | url=%s | ok=%s",
        entry.name,
        url,
        outcome.success,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_eval_files_metrics(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    paths: list[str] = []
    source_files = cops.attr(attrs, "source_files", default="files").lower()
    if source_files in {"1", "filenamesresult"}:
        paths = [str(i.get("path", "")) for i in fops.result_paths(runtime)]
    elif source_files in {"2", "previousresult"}:
        paths = [str(i.get("path", "")) for i in fops.result_paths(runtime)]
        outcome_w = ["source_files=previousresult approximated via result_filenames"]
        cops.iter_warning_logs(f"ENTRY EVAL_FILES_METRICS | name={entry.name}", outcome_w)
    else:
        src = cops.attr(attrs, "source_filefolder", "filename", "foldername")
        if src:
            paths.append(_data_path(src, runtime))
        for row in attrs.get("fields") or []:
            if isinstance(row, Mapping):
                p = str(row.get("source_filefolder") or row.get("name") or "").strip()
                if p:
                    paths.append(_data_path(p, runtime))
    outcome = cops.eval_files_metrics(
        paths,
        evaluation_type=cops.attr(attrs, "evaluation_type", default="size"),
        comparevalue=_resolve(cops.attr(attrs, "comparevalue", default="0"), runtime),
        minvalue=_resolve(cops.attr(attrs, "minvalue", default="0"), runtime),
        maxvalue=_resolve(cops.attr(attrs, "maxvalue", default="0"), runtime),
        successnumbercondition=cops.attr(
            attrs, "successnumbercondition", default="equal"
        ),
        scale=cops.attr(attrs, "scale", default="bytes"),
        recursive=cops.attr_yn(attrs, "include_subFolders", "include_subfolders"),
        wildcard=_resolve(cops.attr(attrs, "wildcard"), runtime),
    )
    cops.iter_warning_logs(f"ENTRY EVAL_FILES_METRICS | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY EVAL_FILES_METRICS | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def _catalog_from_runtime(runtime: JobRuntime) -> str:
    cfg = runtime.config or {}
    return str(cfg.get("TARGET_CATALOG") or "")


def _schema_default(runtime: JobRuntime) -> str:
    cfg = runtime.config or {}
    return str(cfg.get("TARGET_SCHEMA") or "")


def handle_table_exists(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    table = _resolve(cops.attr(attrs, "tablename"), runtime)
    schema = _resolve(cops.attr(attrs, "schemaname"), runtime) or _schema_default(runtime)
    conn_name = cops.attr(attrs, "connection")
    conn_meta = (runtime.connections or {}).get(conn_name) if conn_name else None
    outcome = cops.table_exists(
        table,
        schema,
        spark=getattr(runtime, "spark", None),
        catalog=_catalog_from_runtime(runtime),
        connection_meta=conn_meta,
    )
    cops.iter_warning_logs(f"ENTRY TABLE_EXISTS | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY TABLE_EXISTS | name=%s | table=%s.%s | exists=%s",
        entry.name,
        schema,
        table,
        outcome.success,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=True)
    return EntryResult(name=entry.name, success=False, error=outcome.error, result=False)


def handle_columns_exist(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    table = _resolve(cops.attr(attrs, "tablename"), runtime)
    schema = _resolve(cops.attr(attrs, "schemaname"), runtime) or _schema_default(runtime)
    columns: list[str] = []
    for row in attrs.get("fields") or []:
        if isinstance(row, Mapping):
            col = str(row.get("name") or "").strip()
            if col:
                columns.append(_resolve(col, runtime))
    outcome = cops.columns_exist(
        table,
        columns,
        schema,
        spark=getattr(runtime, "spark", None),
        catalog=_catalog_from_runtime(runtime),
    )
    logging.info(
        "ENTRY COLUMNS_EXIST | name=%s | table=%s | cols=%s | ok=%s",
        entry.name,
        table,
        columns,
        outcome.success,
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_eval_table_content(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    custom_sql = _resolve(cops.attr(attrs, "custom_sql"), runtime)
    use_vars = cops.attr_yn(attrs, "is_usevars", default=True)
    if not use_vars:
        custom_sql = cops.attr(attrs, "custom_sql")
    outcome = cops.eval_table_row_count(
        table=_resolve(cops.attr(attrs, "tablename"), runtime),
        schema=_resolve(cops.attr(attrs, "schemaname"), runtime) or _schema_default(runtime),
        custom_sql=custom_sql,
        use_custom_sql=cops.attr_yn(attrs, "is_custom_sql"),
        success_condition=cops.attr(
            attrs, "success_condition", default="rows_count_equal"
        ),
        limit=_resolve(cops.attr(attrs, "limit", default="0"), runtime),
        spark=getattr(runtime, "spark", None),
        catalog=_catalog_from_runtime(runtime),
    )
    if cops.attr_yn(attrs, "add_rows_result"):
        outcome.warnings.append("add_rows_result=Y is unsupported — rows not added to result")
    cops.iter_warning_logs(f"ENTRY EVAL_TABLE_CONTENT | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY EVAL_TABLE_CONTENT | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_wait_for_sql(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    custom_sql = _resolve(cops.attr(attrs, "custom_sql"), runtime)
    if not cops.attr_yn(attrs, "is_usevars", default=True):
        custom_sql = cops.attr(attrs, "custom_sql")
    outcome = cops.wait_for_sql(
        table=_resolve(cops.attr(attrs, "tablename"), runtime),
        schema=_resolve(cops.attr(attrs, "schemaname"), runtime) or _schema_default(runtime),
        custom_sql=custom_sql,
        use_custom_sql=cops.attr_yn(attrs, "is_custom_sql"),
        success_condition=cops.attr(
            attrs, "success_condition", default="rows_count_equal"
        ),
        rows_count_value=_resolve(
            cops.attr(attrs, "rows_count_value", "limit", default="0"), runtime
        ),
        maximum_timeout=float(
            cops.attr(attrs, "maximum_timeout", "maximumTimeout", default="0") or 0
        ),
        check_cycle_time=float(
            cops.attr(attrs, "check_cycle_time", "checkCycleTime", default="1") or 1
        ),
        success_on_timeout=cops.attr_yn(
            attrs, "success_on_timeout", "successOnTimeout"
        ),
        spark=getattr(runtime, "spark", None),
        catalog=_catalog_from_runtime(runtime),
    )
    cops.iter_warning_logs(f"ENTRY WAIT_FOR_SQL | name={entry.name}", outcome.warnings)
    logging.info("ENTRY WAIT_FOR_SQL | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def handle_check_db_connections(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    items: list[dict[str, Any]] = []
    for row in attrs.get("connections") or []:
        if isinstance(row, Mapping):
            items.append(
                {
                    "name": _resolve(str(row.get("name") or ""), runtime),
                    "waitfor": _resolve(str(row.get("waitfor") or "0"), runtime),
                    "waittime": str(row.get("waittime") or "second"),
                }
            )
    # Also accept fields-style
    for row in attrs.get("fields") or []:
        if isinstance(row, Mapping) and row.get("name"):
            items.append(
                {
                    "name": _resolve(str(row.get("name")), runtime),
                    "waitfor": str(row.get("waitfor") or "0"),
                    "waittime": str(row.get("waittime") or "second"),
                }
            )
    outcome = cops.check_db_connections(
        items,
        runtime.connections or {},
        spark=getattr(runtime, "spark", None),
    )
    cops.iter_warning_logs(
        f"ENTRY CHECK_DB_CONNECTIONS | name={entry.name}", outcome.warnings
    )
    logging.info(
        "ENTRY CHECK_DB_CONNECTIONS | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success:
        return EntryResult(name=entry.name, success=True, result=outcome.extra)
    return EntryResult(
        name=entry.name, success=False, error=outcome.error, result=outcome.extra
    )


def _bulk_connection(runtime: JobRuntime, attrs: Mapping[str, Any]) -> tuple[str, dict]:
    name = bops.attr(attrs, "connection")
    meta = dict((runtime.connections or {}).get(name) or {})
    return name, meta


def handle_mysql_bulk_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    conn_name, conn_meta = _bulk_connection(runtime, attrs)
    filename = _data_path(bops.attr(attrs, "filename"), runtime)
    outcome = bops.mysql_bulk_file(
        connection_meta=conn_meta,
        connection_name=conn_name,
        schema=_resolve(bops.attr(attrs, "schemaname"), runtime),
        table=_resolve(bops.attr(attrs, "tablename"), runtime),
        filename=filename,
        separator=_resolve(bops.attr(attrs, "separator", default="\t"), runtime),
        enclosed=_resolve(bops.attr(attrs, "enclosed"), runtime),
        optionenclosed=bops.attr_yn(attrs, "optionenclosed"),
        lineterminated=_resolve(bops.attr(attrs, "lineterminated", default="\n"), runtime),
        limitlines=_resolve(bops.attr(attrs, "limitlines", default="0"), runtime),
        listcolumn=_resolve(bops.attr(attrs, "listcolumn"), runtime),
        highpriority=bops.attr_yn(attrs, "highpriority"),
        outdumpvalue=bops.attr(attrs, "outdumpvalue", default="0"),
        iffileexists=bops.attr(attrs, "iffileexists", default="2"),
        spark=getattr(runtime, "spark", None),
    )
    bops.iter_warning_logs(f"ENTRY MYSQL_BULK_FILE | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY MYSQL_BULK_FILE | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success and bops.attr_yn(attrs, "addfiletoresult") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"paths": outcome.paths, "row_count": outcome.row_count, **outcome.extra},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, **outcome.extra},
    )


def handle_mysql_bulk_load(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    conn_name, conn_meta = _bulk_connection(runtime, attrs)
    filename = _data_path(bops.attr(attrs, "filename"), runtime)
    outcome = bops.mysql_bulk_load(
        connection_meta=conn_meta,
        connection_name=conn_name,
        schema=_resolve(bops.attr(attrs, "schemaname"), runtime),
        table=_resolve(bops.attr(attrs, "tablename"), runtime),
        filename=filename,
        separator=_resolve(bops.attr(attrs, "separator", default="\t"), runtime),
        enclosed=_resolve(bops.attr(attrs, "enclosed"), runtime),
        escaped=_resolve(bops.attr(attrs, "escaped", default="\\"), runtime),
        linestarted=_resolve(bops.attr(attrs, "linestarted"), runtime),
        lineterminated=_resolve(bops.attr(attrs, "lineterminated", default="\n"), runtime),
        replacedata=bops.attr_yn(attrs, "replacedata", default=True),
        ignorelines=_resolve(bops.attr(attrs, "ignorelines", default="0"), runtime),
        listattribut=_resolve(bops.attr(attrs, "listattribut"), runtime),
        localinfile=bops.attr_yn(attrs, "localinfile", default=True),
        prorityvalue=bops.attr(attrs, "prorityvalue", default="0"),
        spark=getattr(runtime, "spark", None),
    )
    bops.iter_warning_logs(f"ENTRY MYSQL_BULK_LOAD | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY MYSQL_BULK_LOAD | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success and bops.attr_yn(attrs, "addfiletoresult") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"paths": outcome.paths, "row_count": outcome.row_count, **outcome.extra},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, **outcome.extra},
    )


def handle_mssql_bulk_load(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    conn_name, conn_meta = _bulk_connection(runtime, attrs)
    filename = _data_path(bops.attr(attrs, "filename"), runtime)
    outcome = bops.mssql_bulk_load(
        connection_meta=conn_meta,
        connection_name=conn_name,
        schema=_resolve(bops.attr(attrs, "schemaname"), runtime),
        table=_resolve(bops.attr(attrs, "tablename"), runtime),
        filename=filename,
        datafiletype=bops.attr(attrs, "datafiletype", default="char"),
        fieldterminator=_resolve(
            bops.attr(attrs, "fieldterminator", default=","), runtime
        ),
        lineterminated=_resolve(bops.attr(attrs, "lineterminated", default="\n"), runtime),
        codepage=bops.attr(attrs, "codepage", default="OEM"),
        specificcodepage=bops.attr(attrs, "specificcodepage"),
        formatfilename=_resolve(bops.attr(attrs, "formatfilename"), runtime),
        firetriggers=bops.attr_yn(attrs, "firetriggers"),
        checkconstraints=bops.attr_yn(attrs, "checkconstraints"),
        keepnulls=bops.attr_yn(attrs, "keepnulls"),
        keepidentity=bops.attr_yn(attrs, "keepidentity"),
        tablock=bops.attr_yn(attrs, "tablock"),
        startfile=_resolve(bops.attr(attrs, "startfile", default="0"), runtime),
        endfile=_resolve(bops.attr(attrs, "endfile", default="0"), runtime),
        orderby=_resolve(bops.attr(attrs, "orderby"), runtime),
        orderdirection=bops.attr(attrs, "orderdirection"),
        maxerrors=_resolve(bops.attr(attrs, "maxerrors", default="0"), runtime),
        batchsize=_resolve(bops.attr(attrs, "batchsize", default="0"), runtime),
        rowsperbatch=_resolve(bops.attr(attrs, "rowsperbatch", default="0"), runtime),
        errorfilename=_resolve(bops.attr(attrs, "errorfilename"), runtime),
        adddatetime=bops.attr_yn(attrs, "adddatetime"),
        truncate=bops.attr_yn(attrs, "truncate"),
        spark=getattr(runtime, "spark", None),
    )
    bops.iter_warning_logs(f"ENTRY MSSQL_BULK_LOAD | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY MSSQL_BULK_LOAD | name=%s | %s", entry.name, outcome.message
    )
    if outcome.success and bops.attr_yn(attrs, "addfiletoresult") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"paths": outcome.paths, "row_count": outcome.row_count, **outcome.extra},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, **outcome.extra},
    )


def handle_xml_well_formed(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Check if XML file is well formed — Pentaho ``XML_WELL_FORMED``."""
    attrs = entry.attributes or {}
    pairs: list[dict[str, str]] = []
    for row in attrs.get("fields") or []:
        if not isinstance(row, Mapping):
            continue
        src = str(row.get("source_filefolder") or row.get("source") or "").strip()
        if not src:
            continue
        pairs.append(
            {
                "source": _data_path(src, runtime),
                "wildcard": _resolve(str(row.get("wildcard") or ""), runtime),
            }
        )
    prev_paths: list[str] = []
    if xops.attr_yn(attrs, "arg_from_previous"):
        prev_paths = [str(i.get("path", "")) for i in fops.result_paths(runtime)]
    outcome = xops.xml_well_formed(
        pairs,
        recursive=xops.attr_yn(attrs, "include_subfolders"),
        success_condition=xops.attr(
            attrs, "success_condition", default="success_if_no_errors"
        ),
        nr_errors_less_than=_resolve(
            xops.attr(attrs, "nr_errors_less_than", default="10"), runtime
        ),
        resultfilenames=xops.attr(attrs, "resultfilenames", default="all_filenames"),
        arg_from_previous_paths=prev_paths,
    )
    xops.iter_warning_logs(f"ENTRY XML_WELL_FORMED | name={entry.name}", outcome.warnings)
    for err in outcome.errors:
        logging.error("ENTRY XML_WELL_FORMED | name=%s | %s", entry.name, err)
    logging.info(
        "ENTRY XML_WELL_FORMED | name=%s | %s", entry.name, outcome.message
    )
    for p in outcome.paths:
        fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"paths": outcome.paths, **outcome.extra},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, "errors": outcome.errors, **outcome.extra},
    )


def handle_dtd_validator(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """DTD Validator — Pentaho ``DTD_VALIDATOR``."""
    attrs = entry.attributes or {}
    xml_filename = _data_path(xops.attr(attrs, "xmlfilename"), runtime)
    dtd_filename = _data_path(xops.attr(attrs, "dtdfilename"), runtime)
    outcome = xops.dtd_validate(
        xml_filename,
        dtd_filename,
        dtd_intern=xops.attr_yn(attrs, "dtdintern"),
    )
    xops.iter_warning_logs(f"ENTRY DTD_VALIDATOR | name={entry.name}", outcome.warnings)
    for err in outcome.errors:
        logging.error("ENTRY DTD_VALIDATOR | name=%s | %s", entry.name, err)
    logging.info("ENTRY DTD_VALIDATOR | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(
            name=entry.name, success=True, result={"paths": outcome.paths}
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, "errors": outcome.errors},
    )


def handle_xsd_validator(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """XSD Validator — Pentaho ``XSD_VALIDATOR``."""
    attrs = entry.attributes or {}
    xml_filename = _data_path(xops.attr(attrs, "xmlfilename"), runtime)
    xsd_filename = _data_path(xops.attr(attrs, "xsdfilename"), runtime)
    outcome = xops.xsd_validate(
        xml_filename,
        xsd_filename,
        allow_external_entities=xops.attr_yn(attrs, "allowExternalEntities"),
    )
    xops.iter_warning_logs(f"ENTRY XSD_VALIDATOR | name={entry.name}", outcome.warnings)
    for err in outcome.errors:
        logging.error("ENTRY XSD_VALIDATOR | name=%s | %s", entry.name, err)
    logging.info("ENTRY XSD_VALIDATOR | name=%s | %s", entry.name, outcome.message)
    if outcome.success:
        return EntryResult(
            name=entry.name, success=True, result={"paths": outcome.paths}
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, "errors": outcome.errors},
    )


def handle_xslt(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """XSL Transformation — Pentaho ``XSLT``."""
    attrs = entry.attributes or {}
    warnings: list[str] = []
    xml_filename = _data_path(xops.attr(attrs, "xmlfilename"), runtime)
    xsl_filename = _data_path(xops.attr(attrs, "xslfilename"), runtime)
    output_filename = _data_path(xops.attr(attrs, "outputfilename"), runtime)

    if xops.attr_yn(attrs, "filenamesfromprevious"):
        warnings.append(
            "filenamesfromprevious=Y — previous-row XML/XSL/output triples are "
            "approximated; using configured filenames (and result_filenames as XML inputs)"
        )
        prev = [str(i.get("path", "")) for i in fops.result_paths(runtime)]
        if prev and not xml_filename:
            xml_filename = prev[0]

    params: list[dict[str, str]] = []
    for row in attrs.get("parameters") or []:
        if not isinstance(row, Mapping):
            continue
        name = _resolve(str(row.get("name") or ""), runtime)
        value = _resolve(str(row.get("value") or row.get("field") or ""), runtime)
        if name:
            params.append({"name": name, "value": value, "field": value})

    out_props: list[dict[str, str]] = []
    for row in attrs.get("outputproperties") or []:
        if not isinstance(row, Mapping):
            continue
        out_props.append(
            {
                "name": _resolve(str(row.get("name") or ""), runtime),
                "value": _resolve(str(row.get("value") or ""), runtime),
            }
        )

    outcome = xops.xsl_transform(
        xml_filename,
        xsl_filename,
        output_filename,
        parameters=params,
        output_properties=out_props,
        iffileexists=xops.attr(attrs, "iffileexists", default="1"),
        xsltfactory=xops.attr(attrs, "xsltfactory", default="JAXP"),
    )
    outcome.warnings = list(outcome.warnings) + warnings
    xops.iter_warning_logs(f"ENTRY XSLT | name={entry.name}", outcome.warnings)
    logging.info("ENTRY XSLT | name=%s | %s", entry.name, outcome.message)
    if outcome.success and xops.attr_yn(attrs, "addfiletoresult") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"paths": outcome.paths, **outcome.extra},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or RuntimeError(outcome.message),
        result={"paths": outcome.paths, "errors": outcome.errors, **outcome.extra},
    )


def _resolve_mail_attrs(attrs: Mapping[str, Any], runtime: JobRuntime) -> dict[str, Any]:
    """Deep-resolve string values in MAIL / GET_POP / MAIL_VALIDATOR attributes."""
    resolved: dict[str, Any] = {}
    for key, value in (attrs or {}).items():
        if isinstance(value, str):
            resolved[key] = _resolve(value, runtime)
        elif isinstance(value, list):
            items: list[Any] = []
            for item in value:
                if isinstance(item, Mapping):
                    items.append(
                        {
                            k: _resolve(str(v), runtime) if isinstance(v, str) else v
                            for k, v in item.items()
                        }
                    )
                elif isinstance(item, str):
                    items.append(_resolve(item, runtime))
                else:
                    items.append(item)
            resolved[key] = items
        elif isinstance(value, Mapping):
            resolved[key] = {
                k: _resolve(str(v), runtime) if isinstance(v, str) else v
                for k, v in value.items()
            }
        else:
            resolved[key] = value
    return resolved


def handle_mail(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Send email via SMTP (Pentaho MAIL job entry).

    Optional escape hatch: set variable/parameter ``MAIL_ENABLED=N`` to skip
    sending (success + warning) for Databricks dry-runs. Default is to send.
    """
    enabled = str(
        runtime.variables.get(
            "MAIL_ENABLED", runtime.parameters.get("MAIL_ENABLED", "Y")
        )
    ).upper()
    attrs = _resolve_mail_attrs(entry.attributes or {}, runtime)
    cfg = mail_config_from_attributes(attrs)

    if enabled != "Y":
        logging.warning(
            "ENTRY MAIL | name=%s | MAIL_ENABLED=%s — NOT sent | to=%s | subject=%s",
            entry.name,
            enabled,
            ", ".join(cfg.to),
            cfg.subject,
        )
        return EntryResult(
            name=entry.name,
            success=True,
            result={
                "sent": False,
                "skipped": True,
                "to": cfg.to,
                "subject": cfg.subject,
                "comment": cfg.body,
            },
        )

    try:
        # Resolve attachment paths through the data-dir mapper when present
        resolved_attachments: list[str] = []
        for path in cfg.attachments:
            resolved_attachments.append(_data_path(path, runtime) if path else path)
        cfg.attachments = resolved_attachments

        send_result = send_smtp_mail(cfg)
        iter_warning_logs(f"ENTRY MAIL | name={entry.name}", send_result.warnings)
        logging.info(
            "ENTRY MAIL | name=%s | sent=True | to=%s | subject=%s | server=%s:%s",
            entry.name,
            ", ".join(send_result.recipients),
            cfg.subject,
            cfg.server,
            cfg.port,
        )
        return EntryResult(
            name=entry.name,
            success=True,
            result={
                "sent": True,
                "to": send_result.recipients,
                "subject": cfg.subject,
                "comment": cfg.body,
                "warnings": list(send_result.warnings),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logging.error("ENTRY MAIL FAIL | name=%s | %s", entry.name, exc)
        return EntryResult(name=entry.name, success=False, error=exc)


def handle_get_pop(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Get Mails (POP3/IMAP) — Pentaho ``GET_POP`` job entry."""
    attrs = _resolve_mail_attrs(entry.attributes or {}, runtime)
    cfg = get_mails_config_from_attributes(attrs)
    if cfg.output_directory:
        cfg.output_directory = _data_path(cfg.output_directory, runtime)
    if cfg.attachment_folder:
        cfg.attachment_folder = _data_path(cfg.attachment_folder, runtime)

    try:
        result = get_mails(cfg)
        iter_warning_logs(f"ENTRY GET_POP | name={entry.name}", result.warnings)
        logging.info(
            "ENTRY GET_POP | name=%s | protocol=%s | retrieved=%s | deleted=%s | out=%s",
            entry.name,
            cfg.protocol,
            result.retrieved,
            result.deleted,
            cfg.output_directory,
        )
        return EntryResult(
            name=entry.name,
            success=True,
            result={
                "retrieved": result.retrieved,
                "deleted": result.deleted,
                "saved_messages": list(result.saved_messages),
                "saved_attachments": list(result.saved_attachments),
                "warnings": list(result.warnings),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logging.error("ENTRY GET_POP FAIL | name=%s | %s", entry.name, exc)
        return EntryResult(name=entry.name, success=False, error=exc)


def handle_mail_validator(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    """Mail Validator — Pentaho ``MAIL_VALIDATOR`` job entry."""
    attrs = _resolve_mail_attrs(entry.attributes or {}, runtime)
    address = str(attrs.get("emailAddress") or attrs.get("emailaddress") or "")
    smtp_check = yn_true(attrs.get("smtpCheck") or attrs.get("smtp_check"))
    sender = str(attrs.get("emailSender") or attrs.get("email_sender") or "")
    default_smtp = str(attrs.get("defaultSMTP") or attrs.get("default_smtp") or "")
    try:
        timeout = int(str(attrs.get("timeout") or "0") or "0")
    except ValueError:
        timeout = 0

    if not address.strip():
        err = ValueError("MAIL_VALIDATOR emailAddress is empty")
        logging.error("ENTRY MAIL_VALIDATOR FAIL | name=%s | %s", entry.name, err)
        return EntryResult(name=entry.name, success=False, error=err)

    if smtp_check and not sender.strip():
        err = ValueError("MAIL_VALIDATOR smtpCheck=Y requires emailSender")
        logging.error("ENTRY MAIL_VALIDATOR FAIL | name=%s | %s", entry.name, err)
        return EntryResult(name=entry.name, success=False, error=err)

    outcome = validate_email_addresses(
        address,
        smtp_check=smtp_check,
        sender=sender,
        default_smtp=default_smtp,
        timeout=timeout,
    )
    iter_warning_logs(f"ENTRY MAIL_VALIDATOR | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY MAIL_VALIDATOR | name=%s | address=%s | valid=%s",
        entry.name,
        outcome.address or address,
        outcome.valid,
    )
    if outcome.valid:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"valid": True, "address": outcome.address, "warnings": list(outcome.warnings)},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=ValueError(outcome.error or "Invalid email address"),
        result={"valid": False, "address": outcome.address, "error": outcome.error},
    )


def _resolve_path(raw: str, runtime: JobRuntime) -> str:
    return _data_path(raw, runtime)


def _outcome_result(
    entry: JobEntry,
    outcome: fops.FileOpOutcome,
    *,
    log_label: str,
) -> EntryResult:
    fops.iter_warning_logs(f"ENTRY {log_label} | name={entry.name}", outcome.warnings)
    logging.info(
        "ENTRY %s | name=%s | success=%s | %s",
        log_label,
        entry.name,
        outcome.success,
        outcome.message,
    )
    if outcome.success:
        return EntryResult(
            name=entry.name,
            success=True,
            result={"paths": list(outcome.paths), "message": outcome.message, **outcome.extra},
        )
    return EntryResult(
        name=entry.name,
        success=False,
        error=outcome.error or ValueError(outcome.message or f"{log_label} failed"),
        result={"paths": list(outcome.paths), "message": outcome.message, **outcome.extra},
    )


def handle_zip_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    zipname = _resolve_path(fops.attr(attrs, "zipfilename"), runtime)
    source = _resolve_path(fops.attr(attrs, "sourcedirectory"), runtime)
    from_prev = fops.attr_yn(attrs, "isfromprevious")
    prev_paths: list[str] = []
    if from_prev:
        prev_paths = [str(i.get("path", "")) for i in fops.result_paths(runtime)]
    outcome = fops.zip_files(
        zipname,
        source,
        wildcard=fops.attr(attrs, "wildCard", "wildcard"),
        wildcardexclude=fops.attr(attrs, "wildcardexclude"),
        recursive=fops.attr_yn(attrs, "include_subfolders", default=True),
        compressionrate=fops.attr(attrs, "compressionrate", default="1"),
        create_parent=fops.attr_yn(attrs, "createparentfolder"),
        add_date=fops.attr_yn(attrs, "adddate"),
        add_time=fops.attr_yn(attrs, "addtime"),
        if_zip_exists=fops.attr(attrs, "ifzipfileexists", "iffileexists", default="0"),
        from_previous_paths=prev_paths if from_prev else None,
    )
    if outcome.success and fops.attr_yn(attrs, "addfiletoresult") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    after = fops.attr(attrs, "afterzip", default="0")
    if outcome.success and after in {"1", "delete"} and source:
        src = Path(_resolve_path(source, runtime))
        if src.is_file():
            src.unlink(missing_ok=True)
        elif src.is_dir() and fops.attr(attrs, "movetodirectory"):
            pass  # afterzip move handled lightly via warning
    if after in {"1", "2"} and fops.attr(attrs, "movetodirectory"):
        outcome.warnings.append(
            f"afterzip={after} with movetodirectory is partially supported"
        )
    return _outcome_result(entry, outcome, log_label="ZIP_FILE")


def handle_copy_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    pairs = fops.source_dest_pairs(attrs)
    if fops.attr_yn(attrs, "arg_from_previous"):
        for item in fops.result_paths(runtime):
            pairs.append(
                {
                    "source": str(item.get("path", "")),
                    "destination": fops.attr(
                        attrs, "destination_filefolder", "destination_folder"
                    ),
                    "wildcard": "",
                }
            )
    resolved = [
        {
            "source": _resolve_path(p["source"], runtime),
            "destination": _resolve_path(p["destination"], runtime),
            "wildcard": _resolve(p.get("wildcard", ""), runtime),
        }
        for p in pairs
    ]
    outcome = fops.copy_files(
        resolved,
        overwrite=fops.attr_yn(attrs, "overwrite_files"),
        recursive=fops.attr_yn(attrs, "include_subfolders"),
        create_destination=fops.attr_yn(
            attrs, "createDestinationFolder", "create_destination_folder"
        ),
        remove_source=fops.attr_yn(attrs, "remove_source_files"),
        destination_is_file=fops.attr_yn(
            attrs, "destination_is_afile", "destination_is_a_file"
        ),
    )
    if outcome.success and fops.attr_yn(attrs, "add_result_filesname") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    return _outcome_result(entry, outcome, log_label="COPY_FILES")


def handle_move_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    pairs = fops.source_dest_pairs(attrs)
    if fops.attr_yn(attrs, "arg_from_previous"):
        for item in fops.result_paths(runtime):
            pairs.append(
                {
                    "source": str(item.get("path", "")),
                    "destination": fops.attr(
                        attrs, "destination_filefolder", "destination_folder"
                    ),
                    "wildcard": "",
                }
            )
    resolved = [
        {
            "source": _resolve_path(p["source"], runtime),
            "destination": _resolve_path(p["destination"], runtime),
            "wildcard": _resolve(p.get("wildcard", ""), runtime),
        }
        for p in pairs
    ]
    if_exists = fops.attr(attrs, "iffileexists", default="overwrite")
    overwrite = fops.attr_yn(attrs, "overwrite_files") or if_exists in {
        "0",
        "overwrite",
        "overwrite_file",
    }
    outcome = fops.move_files(
        resolved,
        overwrite=overwrite,
        recursive=fops.attr_yn(attrs, "include_subfolders"),
        create_destination=fops.attr_yn(
            attrs,
            "createDestinationFolder",
            "create_destination_folder",
        ),
        destination_is_file=fops.attr_yn(
            attrs, "destination_is_afile", "destination_is_a_file"
        ),
        if_file_exists=if_exists,
    )
    if outcome.success and fops.attr_yn(attrs, "add_result_filesname") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    if fops.attr_yn(attrs, "simulate"):
        outcome.warnings.append("simulate=Y — files were still moved (simulate ignored)")
    return _outcome_result(entry, outcome, log_label="MOVE_FILES")


def handle_delete_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    filename = _resolve_path(
        entry.filename or fops.attr(attrs, "filename"),
        runtime,
    )
    fail_missing = fops.attr_yn(attrs, "fail_if_file_not_exists")
    outcome = fops.delete_file(filename, fail_if_not_exists=fail_missing)
    return _outcome_result(entry, outcome, log_label="DELETE_FILE")


def handle_delete_files(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    pairs = fops.source_dest_pairs(attrs)
    # DELETE_FILES fields use name + filemask
    if not pairs:
        for row in attrs.get("fields") or []:
            if isinstance(row, dict) and row.get("name"):
                pairs.append(
                    {
                        "source": str(row.get("name", "")),
                        "destination": "",
                        "wildcard": str(row.get("filemask") or row.get("wildcard") or ""),
                    }
                )
    if fops.attr_yn(attrs, "arg_from_previous"):
        for item in fops.result_paths(runtime):
            pairs.append({"source": str(item.get("path", "")), "destination": "", "wildcard": ""})
    resolved = [
        {
            "source": _resolve_path(p["source"], runtime),
            "destination": "",
            "wildcard": _resolve(p.get("wildcard", ""), runtime),
        }
        for p in pairs
    ]
    outcome = fops.delete_files(
        resolved,
        recursive=fops.attr_yn(attrs, "include_subfolders"),
    )
    return _outcome_result(entry, outcome, log_label="DELETE_FILES")


def handle_delete_folders(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    folders: list[str] = []
    single = fops.attr(attrs, "foldername", "filename")
    if single:
        folders.append(single)
    for row in attrs.get("fields") or []:
        if isinstance(row, dict):
            name = str(row.get("name") or row.get("foldername") or "").strip()
            if name:
                folders.append(name)
    if fops.attr_yn(attrs, "arg_from_previous"):
        folders.extend(str(i.get("path", "")) for i in fops.result_paths(runtime))
    if fops.attr_yn(attrs, "limit_folders"):
        outcome_w = fops.FileOpOutcome(
            True, "limit_folders ignored", [], ["limit_folders=Y is not applied"]
        )
        fops.iter_warning_logs(f"ENTRY DELETE_FOLDERS | name={entry.name}", outcome_w.warnings)
    outcome = fops.delete_folders(
        folders,
        fail_if_not_exists=fops.attr_yn(attrs, "fail_if_not_exists"),
        resolve=lambda s: _resolve_path(s, runtime),
    )
    return _outcome_result(entry, outcome, log_label="DELETE_FOLDERS")


def handle_create_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    filename = _resolve_path(
        entry.filename or fops.attr(attrs, "filename"),
        runtime,
    )
    outcome = fops.create_file(
        filename,
        fail_if_exists=fops.attr_yn(attrs, "fail_if_file_exists"),
    )
    if outcome.success and fops.attr_yn(attrs, "add_filename_result") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    return _outcome_result(entry, outcome, log_label="CREATE_FILE")


def handle_write_to_file(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    filename = _resolve_path(fops.attr(attrs, "filename"), runtime)
    content = _resolve(fops.attr(attrs, "content"), runtime)
    outcome = fops.write_to_file(
        filename,
        content,
        append=fops.attr_yn(attrs, "appendFile", "appendfile"),
        create_parent=fops.attr_yn(attrs, "createParentFolder", "createparentfolder", default=True),
        encoding=fops.attr(attrs, "encoding", default="UTF-8") or "UTF-8",
    )
    return _outcome_result(entry, outcome, log_label="WRITE_TO_FILE")


def handle_file_compare(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    f1 = _resolve_path(fops.attr(attrs, "filename1"), runtime)
    f2 = _resolve_path(fops.attr(attrs, "filename2"), runtime)
    outcome = fops.file_compare(f1, f2)
    if outcome.success and fops.attr_yn(attrs, "add_filename_result"):
        fops.add_result_file(runtime, f1)
        fops.add_result_file(runtime, f2)
    return _outcome_result(entry, outcome, log_label="FILE_COMPARE")


def handle_folders_compare(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    f1 = _resolve_path(fops.attr(attrs, "filename1"), runtime)
    f2 = _resolve_path(fops.attr(attrs, "filename2"), runtime)
    outcome = fops.folders_compare(
        f1,
        f2,
        include_subfolders=fops.attr_yn(attrs, "include_subfolders"),
        compare_filesize=fops.attr_yn(attrs, "compare_filesize"),
        compare_content=fops.attr_yn(attrs, "compare_filecontent", default=True),
        compare_only=fops.attr(attrs, "compareonly", default="all"),
        wildcard=_resolve(fops.attr(attrs, "wildcard"), runtime),
    )
    return _outcome_result(entry, outcome, log_label="FOLDERS_COMPARE")


def handle_dos_unix_converter(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    pairs = fops.source_dest_pairs(attrs)
    if fops.attr_yn(attrs, "arg_from_previous"):
        for item in fops.result_paths(runtime):
            pairs.append(
                {"source": str(item.get("path", "")), "destination": "", "wildcard": ""}
            )
    resolved = [
        {
            "source": _resolve_path(p["source"], runtime),
            "destination": "",
            "wildcard": _resolve(p.get("wildcard", ""), runtime),
        }
        for p in pairs
    ]
    outcome = fops.convert_dos_unix(
        resolved,
        conversion_type=fops.attr(attrs, "ConversionType", "conversiontype", default="0"),
        recursive=fops.attr_yn(attrs, "include_subfolders"),
    )
    # resultfilenames: 0=do nothing, 1=add all, 2=add converted only — approximate add all
    rf = fops.attr(attrs, "resultfilenames", default="0")
    if outcome.success and rf not in {"0", ""} and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    return _outcome_result(entry, outcome, log_label="DOS_UNIX_CONVERTER")


def handle_unzip(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    zipname = _resolve_path(fops.attr(attrs, "zipfilename"), runtime)
    target = _resolve_path(
        fops.attr(attrs, "targetdirectory", "sourcedirectory"),
        runtime,
    )
    # Retail samples use sourcedirectory as extract target
    if not fops.attr(attrs, "targetdirectory") and fops.attr(attrs, "sourcedirectory"):
        target = _resolve_path(fops.attr(attrs, "sourcedirectory"), runtime)
    password = _resolve(fops.attr(attrs, "password"), runtime) or None
    if_exists = fops.attr(attrs, "iffileexists", default="0")
    overwrite = if_exists in {"0", "overwrite", ""}
    outcome = fops.unzip_file(
        zipname,
        target,
        wildcard=_resolve(fops.attr(attrs, "wildcard", "wildcardsource"), runtime),
        wildcardexclude=_resolve(fops.attr(attrs, "wildcardexclude"), runtime),
        create_folder=fops.attr_yn(attrs, "createfolder", default=True),
        overwrite=overwrite,
        rootzip=fops.attr_yn(attrs, "rootzip"),
        password=password,
        after_unzip=fops.attr(attrs, "afterunzip", default="0"),
        move_to=_resolve_path(fops.attr(attrs, "movetodirectory"), runtime),
    )
    if outcome.success and fops.attr_yn(attrs, "addfiletoresult") and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    return _outcome_result(entry, outcome, log_label="UNZIP")


def handle_add_result_filenames(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    pairs = fops.source_dest_pairs(attrs)
    if not pairs:
        for row in attrs.get("fields") or []:
            if isinstance(row, dict) and (row.get("name") or row.get("filename")):
                pairs.append(
                    {
                        "source": str(row.get("name") or row.get("filename") or ""),
                        "destination": "",
                        "wildcard": str(row.get("filemask") or row.get("wildcard") or ""),
                    }
                )
    if fops.attr_yn(attrs, "arg_from_previous"):
        # Previous rows already in result — nothing to add from args
        pass
    resolved = [
        {
            "source": _resolve_path(p["source"], runtime),
            "destination": "",
            "wildcard": _resolve(p.get("wildcard", ""), runtime),
        }
        for p in pairs
    ]
    outcome = fops.add_filenames_to_result(
        runtime,
        resolved,
        recursive=fops.attr_yn(attrs, "include_subfolders"),
        delete_all_before=fops.attr_yn(attrs, "delete_all_before"),
    )
    return _outcome_result(entry, outcome, log_label="ADD_RESULT_FILENAMES")


def handle_delete_result_filenames(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    outcome = fops.delete_result_filenames(
        runtime,
        wildcard=_resolve(fops.attr(attrs, "wildcard"), runtime),
        wildcardexclude=_resolve(fops.attr(attrs, "wildcardexclude"), runtime),
        specify_wildcard=fops.attr_yn(attrs, "specify_wildcard"),
    )
    return _outcome_result(entry, outcome, log_label="DELETE_RESULT_FILENAMES")


def handle_process_result_filenames(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    outcome = fops.process_result_filenames(
        runtime,
        action=fops.attr(attrs, "action", default="copy"),
        destination_folder=_resolve_path(
            fops.attr(attrs, "destination_folder", "foldername"),
            runtime,
        ),
        wildcard=_resolve(fops.attr(attrs, "wildcard"), runtime),
        wildcardexclude=_resolve(fops.attr(attrs, "wildcardexclude"), runtime),
        specify_wildcard=fops.attr_yn(attrs, "specify_wildcard"),
        overwrite=fops.attr_yn(attrs, "OverwriteFile", "overwrite_files"),
        create_destination=fops.attr_yn(
            attrs, "CreateDestinationFolder", "create_destination_folder", default=True
        ),
        remove_source_from_result=fops.attr_yn(attrs, "RemovedSourceFilename"),
        add_destination_to_result=fops.attr_yn(attrs, "AddDestinationFilename", default=True),
    )
    return _outcome_result(entry, outcome, log_label="COPY_MOVE_RESULT_FILENAMES")


def handle_http(runtime: JobRuntime, entry: JobEntry) -> EntryResult:
    attrs = entry.attributes or {}
    cfg, warnings = fops.http_config_from_attributes(
        attrs, resolve=lambda s: _resolve(s, runtime)
    )
    if cfg.target_filename:
        cfg.target_filename = _resolve_path(cfg.target_filename, runtime)
    if cfg.upload_filename:
        cfg.upload_filename = _resolve_path(cfg.upload_filename, runtime)
    fops.iter_warning_logs(f"ENTRY HTTP | name={entry.name}", warnings)
    outcome = fops.http_request(cfg)
    outcome.warnings.extend(warnings)
    if outcome.success and cfg.add_filename_result and outcome.paths:
        for p in outcome.paths:
            fops.add_result_file(runtime, p)
    return _outcome_result(entry, outcome, log_label="HTTP")


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
            child_cfg = _child_config_from_entry(runtime, entry, cfg)
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
            child_cfg = _child_config_from_entry(runtime, entry, cfg)
            # Share parent/root variable dicts so PARENT_JOB / ROOT_JOB / JVM
            # scopes written by the child are visible after return.
            child_cfg["__parent_variables__"] = runtime.variables
            child_cfg["__root_variables__"] = getattr(
                runtime, "root_variables", runtime.variables
            )
            child_cfg["__variable_scopes__"] = list(
                getattr(runtime, "variable_scopes", [runtime.variables])
            )
            logging.info("Running child job entry: %s → jobs.%s", entry.name, py_stem)
            result = module.run(spark, child_cfg)
            # Pull back any ROOT/PARENT/JVM mutations already applied via shared
            # dicts; also merge child's returned variables for CURRENT_JOB that
            # were intentionally exported via the result payload.
            if isinstance(result, dict):
                child_vars = result.get("variables")
                if isinstance(child_vars, Mapping):
                    # Child CURRENT_JOB vars stay local unless scope wrote upward.
                    # Surface them under a nested key for debugging only.
                    result = dict(result)
                    result["child_variables"] = dict(child_vars)
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
        "MSGBOX_INFO": handle_msgbox_info,
        "PING": handle_ping,
        "TELNET": handle_telnet,
        "SYSLOG": handle_syslog,
        "SEND_NAGIOS_PASSIVE_CHECK": handle_send_nagios_passive_check,
        "SNMP_TRAP": handle_snmp_trap,
        "TRUNCATE_TABLES": handle_truncate_tables,
        "HL7MLLPINPUT": handle_hl7_mllp_input,
        "HL7MLLPInput": handle_hl7_mllp_input,
        "HL7MLLPACKNOWLEDGE": handle_hl7_mllp_acknowledge,
        "HL7MLLPAcknowledge": handle_hl7_mllp_acknowledge,
        "CONNECTED_TO_REPOSITORY": handle_connected_to_repository,
        "EXPORT_REPOSITORY": handle_export_repository,
        "FTP": handle_ftp_get,
        "FTP_PUT": handle_ftp_put,
        "FTP_DELETE": handle_ftp_delete,
        "FTPS_GET": handle_ftps_get,
        "FTPS_PUT": handle_ftps_put,
        "SFTP": handle_sftp_get,
        "SFTPPUT": handle_sftp_put,
        "PGP_ENCRYPT_FILES": handle_pgp_encrypt_files,
        "PGP_DECRYPT_FILES": handle_pgp_decrypt_files,
        "PGP_VERIFY_FILES": handle_pgp_verify_files,
        "SET_VARIABLES": handle_set_variables,
        "SHELL": handle_shell,
        "SQL": handle_sql,
        "EVAL": handle_eval,
        "CREATE_FOLDER": handle_create_folder,
        "CREATE_FILE": handle_create_file,
        "WRITE_TO_FILE": handle_write_to_file,
        "FILE_EXISTS": handle_file_exists,
        "FILES_EXIST": handle_files_exist,
        "FOLDER_IS_EMPTY": handle_folder_is_empty,
        "CHECK_FILES_LOCKED": handle_check_files_locked,
        "WEBSERVICE_AVAILABLE": handle_webservice_available,
        "TABLE_EXISTS": handle_table_exists,
        "COLUMNS_EXIST": handle_columns_exist,
        "EVAL_TABLE_CONTENT": handle_eval_table_content,
        "EVAL_FILES_METRICS": handle_eval_files_metrics,
        "WAIT_FOR_SQL": handle_wait_for_sql,
        "CHECK_DB_CONNECTIONS": handle_check_db_connections,
        "MYSQL_BULK_FILE": handle_mysql_bulk_file,
        "MYSQL_BULK_LOAD": handle_mysql_bulk_load,
        "MSSQL_BULK_LOAD": handle_mssql_bulk_load,
        "XML_WELL_FORMED": handle_xml_well_formed,
        "DTD_VALIDATOR": handle_dtd_validator,
        "XSD_VALIDATOR": handle_xsd_validator,
        "XSLT": handle_xslt,
        "WAIT_FOR_FILE": handle_wait_for_file,
        "SIMPLE_EVAL": handle_simple_eval,
        "DELAY": handle_delay,
        "WAIT_FOR": handle_delay,  # Spoon "Wait for" alias used in some exports
        "MAIL": handle_mail,
        "GET_POP": handle_get_pop,
        "MAIL_VALIDATOR": handle_mail_validator,
        "ZIP_FILE": handle_zip_file,
        "UNZIP": handle_unzip,
        "UNZIP_FILE": handle_unzip,  # Retail / alias
        "COPY_FILES": handle_copy_files,
        "MOVE_FILES": handle_move_files,
        "DELETE_FILE": handle_delete_file,
        "DELETE_FILES": handle_delete_files,
        "DELETE_FOLDERS": handle_delete_folders,
        "DELETE_FOLDER": handle_delete_folders,  # Retail alias
        "FILE_COMPARE": handle_file_compare,
        "FOLDERS_COMPARE": handle_folders_compare,
        "DOS_UNIX_CONVERTER": handle_dos_unix_converter,
        "ADD_RESULT_FILENAMES": handle_add_result_filenames,
        "DELETE_RESULT_FILENAMES": handle_delete_result_filenames,
        "COPY_MOVE_RESULT_FILENAMES": handle_process_result_filenames,
        "HTTP": handle_http,
        "TRANS": make_trans_handler(spark=spark, cfg=cfg, trans_runners=trans_runners),
        "JOB": make_job_handler(
            spark=spark, cfg=cfg, child_job_modules=child_job_modules
        ),
    }
    for etype in entry_types:
        if etype and etype not in handlers:
            handlers[etype] = handle_todo
    return handlers
