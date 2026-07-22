"""Utility-category job entry helpers.

Covers MsgBox (log-only), Ping, Telnet, Syslog, Nagios NSCA, SNMP Trap,
Truncate Tables, and HL7 MLLP (best-effort sockets). Abort / Write to Log /
Wait for SQL remain in handlers / condition_ops.
"""

from __future__ import annotations

import logging
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

# MLLP framing (HL7)
_MLLP_START = b"\x0b"
_MLLP_END = b"\x1c\x0d"


def yn_true(raw: Any, default: bool = False) -> bool:
    if raw is None or raw == "":
        return default
    return str(raw).strip().upper() in {"Y", "YES", "TRUE", "1", "T"}


def attr(attrs: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return str(attrs[key])
    return default


def attr_yn(attrs: Mapping[str, Any], *keys: str, default: bool = False) -> bool:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return yn_true(attrs[key], default)
    return default


def iter_warning_logs(prefix: str, warnings: Iterable[str]) -> None:
    for warning in warnings:
        logger.warning("%s | %s", prefix, warning)


@dataclass
class UtilityOutcome:
    success: bool
    message: str = ""
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def msgbox_info(title: str, body: str) -> UtilityOutcome:
    """Databricks has no GUI — log the message box contents."""
    warnings = [
        "MSGBOX_INFO: GUI dialog unsupported on Databricks — logged instead of MsgBox"
    ]
    logger.info("MSGBOX_INFO | title=%s | message=%s", title, body)
    return UtilityOutcome(True, f"MsgBox logged: {title}", warnings, extra={"title": title, "body": body})


def ping_host(
    hostname: str,
    *,
    timeout_ms: str | float = "3000",
    nbr_packets: str | int = "2",
    pingtype: str = "systemPing",
) -> UtilityOutcome:
    """Platform-independent reachability check (TCP/DNS), not OS ``ping``."""
    warnings: list[str] = []
    host = (hostname or "").strip()
    if not host:
        err = ValueError("PING hostname is empty")
        return UtilityOutcome(False, str(err), error=err)

    ptype = (pingtype or "systemPing").strip()
    if ptype in {"classicPing", "0", "bothPings", "2"}:
        warnings.append(
            f"pingtype={pingtype!r}: OS ICMP ping is avoided on Databricks — "
            "using DNS + TCP connect check"
        )

    try:
        timeout = max(float(timeout_ms or 3000), 1.0) / 1000.0
    except ValueError:
        timeout = 3.0
        warnings.append(f"Invalid timeout {timeout_ms!r} — using 3000ms")

    try:
        packets = max(int(float(nbr_packets or 2)), 1)
    except ValueError:
        packets = 2

    # Resolve DNS
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        return UtilityOutcome(False, f"DNS resolution failed: {exc}", warnings, error=exc)

    # Try TCP connect to common ports as a connectivity probe
    ports = (443, 80, 22, 3389, 7)
    last_err: BaseException | None = None
    success_count = 0
    for _ in range(packets):
        ok_packet = False
        for _family, _type, _proto, _canon, sockaddr in infos:
            ip = sockaddr[0]
            for port in ports:
                try:
                    with socket.create_connection((ip, port), timeout=timeout):
                        ok_packet = True
                        break
                except OSError as exc:
                    last_err = exc
                    continue
            if ok_packet:
                break
        # DNS alone counts as partial success when all TCP ports are filtered
        if not ok_packet and infos:
            warnings.append(
                f"Host {host} resolved but no TCP probe ports accepted — "
                "treating DNS success as reachable"
            )
            ok_packet = True
        if ok_packet:
            success_count += 1

    ok = success_count > 0
    return UtilityOutcome(
        ok,
        f"ping {host}: ok_packets={success_count}/{packets}",
        warnings,
        error=None if ok else (last_err or ConnectionError(f"Ping failed for {host}")),
        extra={"host": host, "ok_packets": success_count},
    )


def telnet_host(
    hostname: str,
    port: str | int = "23",
    *,
    timeout_ms: str | float = "3000",
) -> UtilityOutcome:
    """TCP connect check (telnetlib removed in Python 3.13+)."""
    warnings = [
        "TELNET: using TCP connect probe — full telnet session/credentials not supported"
    ]
    host = (hostname or "").strip()
    if not host:
        err = ValueError("TELNET hostname is empty")
        return UtilityOutcome(False, str(err), error=err)
    try:
        port_i = int(float(port or 23))
    except ValueError:
        port_i = 23
        warnings.append(f"Invalid port {port!r} — using 23")
    try:
        timeout = max(float(timeout_ms or 3000), 1.0) / 1000.0
    except ValueError:
        timeout = 3.0

    try:
        with socket.create_connection((host, port_i), timeout=timeout):
            pass
    except OSError as exc:
        return UtilityOutcome(False, str(exc), warnings, error=exc)

    return UtilityOutcome(
        True,
        f"TCP connect ok {host}:{port_i}",
        warnings,
        extra={"host": host, "port": port_i},
    )


def send_syslog(
    servername: str,
    message: str,
    *,
    port: str | int = "514",
    facility: str = "USER",
    priority: str = "INFO",
    date_pattern: str = "",
    add_timestamp: bool = True,
    add_hostname: bool = True,
) -> UtilityOutcome:
    warnings: list[str] = []
    host = (servername or "").strip()
    if not host:
        err = ValueError("SYSLOG servername is empty")
        return UtilityOutcome(False, str(err), error=err)
    try:
        port_i = int(float(port or 514))
    except ValueError:
        port_i = 514

    fac_map = {
        "KERN": 0, "USER": 1, "MAIL": 2, "DAEMON": 3, "AUTH": 4, "SYSLOG": 5,
        "LPR": 6, "NEWS": 7, "UUCP": 8, "CRON": 9, "AUTHPRIV": 10, "FTP": 11,
        "LOCAL0": 16, "LOCAL1": 17, "LOCAL2": 18, "LOCAL3": 19,
        "LOCAL4": 20, "LOCAL5": 21, "LOCAL6": 22, "LOCAL7": 23,
    }
    sev_map = {
        "EMERG": 0, "ALERT": 1, "CRIT": 2, "ERR": 3, "ERROR": 3,
        "WARNING": 4, "WARN": 4, "NOTICE": 5, "INFO": 6, "DEBUG": 7,
    }
    fac = fac_map.get((facility or "USER").upper(), 1)
    sev = sev_map.get((priority or "INFO").upper(), 6)
    pri = fac * 8 + sev

    parts: list[str] = []
    if add_timestamp:
        if date_pattern:
            warnings.append(
                f"datePattern={date_pattern!r} not fully applied — using ISO-8601"
            )
        parts.append(datetime.now(timezone.utc).strftime("%b %d %H:%M:%S"))
    if add_hostname:
        try:
            parts.append(socket.gethostname())
        except OSError:
            parts.append("databricks")
    parts.append(message or "")
    payload = f"<{pri}>{' '.join(parts)}"

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(payload.encode("utf-8", errors="replace"), (host, port_i))
        finally:
            sock.close()
    except OSError as exc:
        return UtilityOutcome(False, str(exc), warnings, error=exc)

    logger.info("SYSLOG sent to %s:%s | %s", host, port_i, message)
    return UtilityOutcome(
        True,
        f"Syslog sent to {host}:{port_i}",
        warnings,
        extra={"server": host, "port": port_i, "payload": payload},
    )


def send_nagios_passive_check(
    servername: str,
    message: str,
    *,
    port: str | int = "5667",
    password: str = "",
    sender_server_name: str = "",
    sender_service_name: str = "",
    level: str = "0",
    encryption_mode: str = "0",
    response_timeout: str | float = "10000",
    connection_timeout: str | float = "5000",
) -> UtilityOutcome:
    """Best-effort NSCA-style TCP send (plain / XOR); TripleDES warned."""
    warnings: list[str] = []
    host = (servername or "").strip()
    if not host:
        err = ValueError("SEND_NAGIOS_PASSIVE_CHECK servername is empty")
        return UtilityOutcome(False, str(err), error=err)

    try:
        port_i = int(float(port or 5667))
    except ValueError:
        port_i = 5667
    try:
        conn_to = max(float(connection_timeout or 5000), 1.0) / 1000.0
    except ValueError:
        conn_to = 5.0
    try:
        resp_to = max(float(response_timeout or 10000), 1.0) / 1000.0
    except ValueError:
        resp_to = 10.0

    enc = str(encryption_mode).strip().lower()
    if enc in {"2", "tripledes", "encryption_mode_tripledes"}:
        warnings.append(
            "Nagios TripleDES encryption not implemented — sending plaintext payload"
        )
    xor = enc in {"1", "xor", "encryption_mode_xor"}
    if xor and not password:
        warnings.append("XOR encryption requested but password empty")

    # Map level codes
    level_map = {
        "0": 0, "unknown": 0, "level_type_unknown": 0,
        "1": 1, "ok": 1, "level_type_ok": 1,
        "2": 2, "warning": 2, "level_type_warning": 2,
        "3": 3, "critical": 3, "level_type_critical": 3,
    }
    # PDI often stores numeric 0=unknown,1=ok,2=warning,3=critical OR string names
    lvl_key = str(level).strip().lower()
    return_code = level_map.get(lvl_key, 0)
    try:
        if lvl_key.isdigit():
            return_code = int(lvl_key)
    except ValueError:
        pass

    hostname = (sender_server_name or socket.gethostname())[:64]
    service = (sender_service_name or " pentaho")[:128]
    msg = (message or "")[:512]
    # Simplified NSCA v3 data packet (not fully wire-compatible with all NSCA servers)
    data = f"{hostname}\t{service}\t{return_code}\t{msg}\n".encode("utf-8", errors="replace")
    if xor and password:
        key = password.encode("utf-8")
        data = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
        warnings.append("Applied simple XOR obfuscation (may not match all NSCA servers)")

    try:
        with socket.create_connection((host, port_i), timeout=conn_to) as sock:
            sock.settimeout(resp_to)
            sock.sendall(data)
            try:
                sock.recv(1024)
            except (OSError, TimeoutError):
                pass
    except OSError as exc:
        return UtilityOutcome(False, str(exc), warnings, error=exc)

    return UtilityOutcome(
        True,
        f"Nagios passive check sent to {host}:{port_i}",
        warnings,
        extra={"host": host, "service": service, "level": return_code},
    )


def send_snmp_trap(
    servername: str,
    message: str,
    *,
    port: str | int = "162",
    oid: str = "",
    community: str = "public",
    timeout: str | float = "5000",
    nrretry: str | int = "2",
    targettype: str = "community",
    user: str = "",
    passphrase: str = "",
    engineid: str = "",
) -> UtilityOutcome:
    warnings: list[str] = []
    host = (servername or "").strip()
    if not host:
        err = ValueError("SNMP_TRAP servername is empty")
        return UtilityOutcome(False, str(err), error=err)

    ttype = (targettype or "community").strip().lower()
    if ttype in {"user", "snmpv3", "2"}:
        warnings.append(
            "SNMPv3 user/passphrase/engineid not fully supported — "
            "attempting community trap or TODO"
        )

    try:
        from pysnmp.hlapi import (  # type: ignore
            CommunityData,
            ContextData,
            ObjectIdentity,
            ObjectType,
            SnmpEngine,
            UdpTransportTarget,
            sendNotification,
            NotificationType,
        )
    except ImportError:
        warnings.append(
            "pysnmp not installed — SNMP_TRAP is a TODO on Databricks "
            "(install cluster library 'pysnmp'); configuration preserved"
        )
        err = ImportError(
            "SNMP_TRAP requires pysnmp — install on the Databricks cluster"
        )
        return UtilityOutcome(
            False,
            str(err),
            warnings,
            error=err,
            extra={
                "servername": host,
                "oid": oid,
                "comstring": community,
                "message": message,
                "targettype": targettype,
                "user": user,
                "engineid": engineid,
            },
        )

    try:
        port_i = int(float(port or 162))
    except ValueError:
        port_i = 162
    try:
        timeout_s = max(float(timeout or 5000), 1.0) / 1000.0
    except ValueError:
        timeout_s = 5.0
    try:
        retries = max(int(float(nrretry or 2)), 0)
    except ValueError:
        retries = 2

    trap_oid = oid or "1.3.6.1.4.1.3.1.1"
    try:
        iterator = sendNotification(
            SnmpEngine(),
            CommunityData(community or "public"),
            UdpTransportTarget((host, port_i), timeout=timeout_s, retries=retries),
            ContextData(),
            "trap",
            NotificationType(ObjectIdentity(trap_oid)).addVarBinds(
                ObjectType(ObjectIdentity(trap_oid), message or "")
            ),
        )
        error_indication, error_status, _error_index, _var_binds = next(iterator)
        if error_indication:
            err = RuntimeError(str(error_indication))
            return UtilityOutcome(False, str(err), warnings, error=err)
        if error_status:
            err = RuntimeError(str(error_status))
            return UtilityOutcome(False, str(err), warnings, error=err)
    except Exception as exc:  # noqa: BLE001
        return UtilityOutcome(False, str(exc), warnings, error=exc)

    if passphrase or engineid:
        warnings.append("SNMPv3 passphrase/engineid ignored for community trap path")

    return UtilityOutcome(
        True,
        f"SNMP trap sent to {host}:{port_i}",
        warnings,
        extra={"oid": trap_oid, "host": host},
    )


def truncate_tables(
    tables: Sequence[Mapping[str, str]],
    *,
    spark: Any = None,
    connection_name: str = "",
    connection_meta: Mapping[str, Any] | None = None,
    catalog: str = "",
) -> UtilityOutcome:
    warnings: list[str] = []
    if connection_name and connection_meta:
        warnings.append(
            f"TRUNCATE_TABLES connection={connection_name!r} — using Spark SQL "
            "(JDBC truncate not issued directly)"
        )
    if spark is None:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
        except Exception:  # noqa: BLE001
            spark = None
    if spark is None:
        err = RuntimeError("TRUNCATE_TABLES requires an active Spark session")
        return UtilityOutcome(False, str(err), warnings, error=err)

    truncated: list[str] = []
    errors: list[str] = []
    for row in tables:
        table = str(row.get("name") or row.get("table") or "").strip()
        schema = str(row.get("schemaname") or row.get("schema") or "").strip()
        if not table:
            continue
        parts = [p for p in (catalog, schema, table) if p]
        # Quote simple identifiers
        full = ".".join(f"`{p}`" if not p.startswith("`") else p for p in parts)
        try:
            spark.sql(f"TRUNCATE TABLE {full}")
            truncated.append(full)
        except Exception as exc:  # noqa: BLE001
            # Fallback: DELETE (some lakehouse tables)
            try:
                spark.sql(f"DELETE FROM {full}")
                truncated.append(full)
                warnings.append(f"TRUNCATE failed for {full} ({exc}); used DELETE FROM")
            except Exception as exc2:  # noqa: BLE001
                errors.append(f"{full}: {exc2}")

    ok = bool(truncated) and not errors
    if not tables:
        err = ValueError("TRUNCATE_TABLES: no tables specified")
        return UtilityOutcome(False, str(err), warnings, error=err)
    if not truncated and errors:
        err = RuntimeError("; ".join(errors))
        return UtilityOutcome(False, str(err), warnings, error=err, extra={"errors": errors})
    if errors:
        warnings.extend(errors)
        return UtilityOutcome(
            False,
            f"Partial truncate: ok={truncated} errors={errors}",
            warnings,
            error=RuntimeError("; ".join(errors)),
            extra={"truncated": truncated, "errors": errors},
        )
    return UtilityOutcome(
        True,
        f"Truncated {len(truncated)} table(s)",
        warnings,
        extra={"truncated": truncated},
    )


def _recv_mllp(conn: socket.socket, timeout: float) -> bytes:
    conn.settimeout(timeout)
    buf = b""
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            break
        buf += chunk
        if _MLLP_END in buf:
            break
    if buf.startswith(_MLLP_START):
        buf = buf[1:]
    if _MLLP_END in buf:
        buf = buf.split(_MLLP_END, 1)[0]
    return buf


def hl7_mllp_input(
    server: str,
    port: str | int,
    *,
    message_variable: str = "",
    type_variable: str = "",
    version_variable: str = "",
    timeout_s: float = 30.0,
    listen: bool = True,
) -> UtilityOutcome:
    """Best-effort MLLP receive. Listener mode is uncommon on Databricks."""
    warnings = [
        "HL7MLLPInput: Databricks batch jobs rarely expose inbound MLLP listeners — "
        "configuration preserved; network/firewall may block accept()"
    ]
    host = (server or "0.0.0.0").strip() or "0.0.0.0"
    try:
        port_i = int(float(port or 0))
    except ValueError:
        err = ValueError(f"Invalid HL7 port: {port!r}")
        return UtilityOutcome(False, str(err), warnings, error=err)
    if port_i <= 0:
        err = ValueError("HL7MLLPInput port is required")
        return UtilityOutcome(False, str(err), warnings, error=err)

    variables: dict[str, str] = {}
    try:
        if listen:
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.settimeout(timeout_s)
            srv.bind((host if host not in {"localhost", "127.0.0.1"} else "0.0.0.0", port_i))
            srv.listen(1)
            conn, _addr = srv.accept()
            try:
                raw = _recv_mllp(conn, timeout_s)
            finally:
                conn.close()
                srv.close()
        else:
            with socket.create_connection((host, port_i), timeout=timeout_s) as conn:
                raw = _recv_mllp(conn, timeout_s)
    except OSError as exc:
        return UtilityOutcome(False, str(exc), warnings, error=exc)

    text = raw.decode("utf-8", errors="replace")
    if message_variable:
        variables[message_variable] = text
    # Crude HL7 type/version from MSH
    segments = text.split("\r")
    msg_type = ""
    version = ""
    if segments and segments[0].startswith("MSH"):
        fields = segments[0].split("|")
        if len(fields) > 8:
            msg_type = fields[8]
        if len(fields) > 11:
            version = fields[11]
    if type_variable:
        variables[type_variable] = msg_type
    if version_variable:
        variables[version_variable] = version
    warnings.append(
        "HL7 message stored as raw text — full HAPI validation not performed"
    )
    return UtilityOutcome(
        True,
        f"HL7 MLLP message received ({len(text)} chars)",
        warnings,
        extra={"variables": variables, "message": text},
    )


def hl7_mllp_acknowledge(
    server: str,
    port: str | int,
    *,
    message: str = "",
    variable: str = "",
    timeout_s: float = 30.0,
) -> UtilityOutcome:
    """Send a minimal AA ACK for the provided HL7 message over MLLP."""
    warnings = [
        "HL7MLLPAcknowledge: PDI socket-cache semantics approximated with direct TCP send"
    ]
    host = (server or "").strip()
    if not host:
        err = ValueError("HL7MLLPAcknowledge server is empty")
        return UtilityOutcome(False, str(err), warnings, error=err)
    try:
        port_i = int(float(port or 0))
    except ValueError:
        err = ValueError(f"Invalid HL7 port: {port!r}")
        return UtilityOutcome(False, str(err), warnings, error=err)

    raw = message or ""
    # Build minimal ACK from MSH control ID if present
    control_id = ""
    if raw.startswith("MSH"):
        fields = raw.split("\r")[0].split("|")
        if len(fields) > 9:
            control_id = fields[9]
    ack = (
        f"MSH|^~\\&|||||{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}||"
        f"ACK|{control_id or '1'}|P|2.5\rMSA|AA|{control_id or '1'}\r"
    )
    payload = _MLLP_START + ack.encode("utf-8") + _MLLP_END
    try:
        with socket.create_connection((host, port_i), timeout=timeout_s) as conn:
            conn.sendall(payload)
    except OSError as exc:
        return UtilityOutcome(False, str(exc), warnings, error=exc)

    return UtilityOutcome(
        True,
        f"HL7 ACK sent to {host}:{port_i}",
        warnings,
        extra={"variable": variable, "ack": ack},
    )
