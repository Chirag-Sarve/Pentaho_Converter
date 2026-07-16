"""Handlers for Pentaho Inline transformation steps.

Supports:
- Injector → spark.createDataFrame() with preserved field schema / optional literals
- Socket Reader → Spark Structured Streaming socket source (text) with migration warnings
- Socket Writer → metadata preserved; no native Databricks socket sink
"""

from __future__ import annotations

import logging

from ..metadata_propagation import get_converter_metadata
from ..schema_utils import fields_to_schema_ddl, spark_cast_type
from ..step_xml import (
    get_step_element,
    parse_injector_config,
    parse_socket_reader_config,
    parse_socket_writer_config,
)
from .base import BaseStepHandler, StepContext
from .generate_handlers import _format_python_value
from .streaming_handlers import _preserve_comments, _safe_ident

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return step_type.strip().lower().replace(" ", "").replace("(", "").replace(")", "")


def _meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


def _field_ddl(fields: list[dict]) -> str:
    if not fields:
        return "_injector STRING"
    try:
        ddl = fields_to_schema_ddl(
            [
                {
                    "name": f.get("name"),
                    "type": f.get("type") or f.get("type_name") or "String",
                }
                for f in fields
                if f.get("name")
            ]
        )
        if ddl:
            return ddl
    except Exception:
        pass
    parts = []
    for f in fields:
        name = f.get("name")
        if not name:
            continue
        t = spark_cast_type(f.get("type") or f.get("type_name") or "String").upper()
        parts.append(f"{name} {t}")
    return ", ".join(parts) if parts else "_injector STRING"


def _validate_port(port: str) -> tuple[bool, str]:
    """Return (ok, message). Empty port is invalid."""
    raw = (port or "").strip()
    if not raw:
        return False, "missing port"
    # Allow Pentaho variables like ${SOCKET_PORT}
    if "${" in raw or raw.startswith("@"):
        return True, ""
    try:
        n = int(raw)
    except ValueError:
        return False, f"invalid port {raw!r}"
    if n < 1 or n > 65535:
        return False, f"port out of range {n}"
    return True, ""


def _validate_host(host: str) -> tuple[bool, str]:
    raw = (host or "").strip()
    if not raw:
        return False, "missing hostname"
    if "${" in raw or raw.startswith("@"):
        return True, ""
    # Reject obviously bad hosts (spaces, bare empty after strip already caught)
    if " " in raw and "://" not in raw:
        return False, f"invalid hostname {raw!r}"
    return True, ""


# ---------------------------------------------------------------------------
# Injector
# ---------------------------------------------------------------------------


class InjectorHandler(BaseStepHandler):
    """Injector → empty typed DataFrame (runtime API inject) or createDataFrame literals."""

    _TYPES = {"injector"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_injector_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            fields = metadata.get("fields") or []
            if not isinstance(fields, list):
                fields = []
            columns = metadata.get("columns") or [f.get("name") for f in fields if f.get("name")]
            grid_rows = metadata.get("rows") or []
            ddl = _field_ddl(fields)

            lines = [f"# Injector: {context.step.name}"]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "fields",
                        "columns",
                        "rows",
                        "has_row_values",
                        "inject_at_runtime",
                        "options",
                    ),
                    context=context,
                )
            )

            # Preserve per-field length/precision/null/residual metadata
            for f in fields:
                name = f.get("name")
                if not name:
                    continue
                preserved = []
                if f.get("length") not in (None, "", "-1", "-2"):
                    preserved.append(f"length={f['length']!r}")
                if f.get("precision") not in (None, "", "-1", "-2"):
                    preserved.append(f"precision={f['precision']!r}")
                if f.get("format"):
                    preserved.append(f"format={f['format']!r}")
                if f.get("null"):
                    preserved.append(f"null={f['null']!r}")
                if f.get("type") or f.get("type_name"):
                    preserved.append(f"type={(f.get('type') or f.get('type_name'))!r}")
                extras = f.get("field_options") if isinstance(f.get("field_options"), dict) else {}
                for ek, ev in extras.items():
                    if ev not in (None, ""):
                        preserved.append(f"{ek}={ev!r}")
                if preserved:
                    lines.append(f"# preserved.field.{name}: {', '.join(preserved)}")

            # Explicit data grid rows (custom / DataGrid-like Injector variants)
            if grid_rows and columns:
                type_by_col = {
                    f.get("name"): (f.get("type") or f.get("type_name") or "String")
                    for f in fields
                    if f.get("name")
                }
                empty_as_str = {
                    f.get("name") for f in fields if f.get("set_empty_string") and f.get("name")
                }
                row_tuples = []
                for row in grid_rows:
                    padded = list(row) + [""] * max(0, len(columns) - len(row))
                    parts = []
                    for col, raw in zip(columns, padded[: len(columns)]):
                        t = type_by_col.get(col, "String")
                        if raw == "" and col not in empty_as_str:
                            parts.append("None")
                        else:
                            parts.append(_format_python_value(raw, t))
                    row_tuples.append("(" + ", ".join(parts) + ")")
                col_list = ", ".join(repr(c) for c in columns)
                lines.append("data = [")
                lines.extend(f"    {rt}," for rt in row_tuples)
                lines.append("]")
                lines.append(f"{out_var} = spark.createDataFrame(data, [{col_list}])")
                lines.append(f"# Schema DDL (reference): {ddl}")
                logger.info(
                    "Injector %s createDataFrame rows=%d cols=%d",
                    context.step.name,
                    len(row_tuples),
                    len(columns),
                )
                return lines, "converted"

            # Single-row literals when XML embeds values
            if metadata.get("has_row_values") and fields:
                parts = []
                for f in fields:
                    t = f.get("type") or f.get("type_name") or "String"
                    if f.get("set_empty_string"):
                        parts.append('""')
                        continue
                    v = f.get("value")
                    if v is None or v == "":
                        parts.append("None")
                    else:
                        parts.append(_format_python_value(str(v), t))
                row_tuple = "(" + ", ".join(parts) + ")"
                col_list = ", ".join(repr(f["name"]) for f in fields if f.get("name"))
                lines.append(f"data = [{row_tuple}]")
                lines.append(f"{out_var} = spark.createDataFrame(data, [{col_list}])")
                lines.append(f"# Schema DDL (reference): {ddl}")
                logger.info(
                    "Injector %s literal row cols=%d",
                    context.step.name,
                    len(fields),
                )
                return lines, "converted"

            # Native Injector: schema only - rows injected at runtime via Kettle API
            lines.append(
                "# NOTE: Pentaho Injector has no static row data in XML; rows are injected "
                "at runtime via Trans.addRowProducer() / RowProducer.putRow()."
            )
            lines.append(
                "# WARNING: empty injected dataset - migrate by loading a source DataFrame "
                f"and casting to this schema, or keep createDataFrame([], '{ddl}') as a stub."
            )
            if not fields:
                lines.append(
                    "# WARNING: invalid/empty Injector schema - no field definitions found"
                )
                lines.append(f"{out_var} = spark.createDataFrame([], '_injector STRING')")
                logger.warning("Injector %s: empty schema", context.step.name)
                return lines, "partial"

            lines.append(f"{out_var} = spark.createDataFrame([], '{ddl}')")
            logger.info(
                "Injector %s empty schema fields=%d ddl=%s",
                context.step.name,
                len(fields),
                ddl,
            )
            return lines, "converted"
        except Exception as exc:
            logger.exception("Injector failed for %s", context.step.name)
            return [
                f"# Injector: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.createDataFrame([], '_injector STRING')",
            ], "partial"


# ---------------------------------------------------------------------------
# Socket Reader
# ---------------------------------------------------------------------------


class SocketReaderHandler(BaseStepHandler):
    """Socket Reader → spark.readStream.format('socket') when text-protocol suitable."""

    _TYPES = {"socketreader"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_socket_reader_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            host = (metadata.get("hostname") or metadata.get("host") or "").strip()
            port = str(metadata.get("port") or "").strip()
            buffer_size = str(metadata.get("buffer_size") or "3000")
            protocol = (metadata.get("protocol") or "kettle").strip().lower()
            encoding = (metadata.get("encoding") or "").strip()
            delimiter = metadata.get("delimiter") or ""
            timeout = str(metadata.get("timeout") or "").strip()
            compressed = metadata.get("compressed")
            fields = metadata.get("fields") or []
            status = "converted"

            lines = [f"# Socket Reader: {context.step.name}"]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "hostname",
                        "host",
                        "port",
                        "buffer_size",
                        "compressed",
                        "compressed_raw",
                        "protocol",
                        "encoding",
                        "delimiter",
                        "enclosure",
                        "timeout",
                        "fields",
                        "options",
                    ),
                    context=context,
                )
            )

            host_ok, host_msg = _validate_host(host)
            port_ok, port_msg = _validate_port(port)
            if not host_ok:
                if not host:
                    lines.append(f"# WARNING: {host_msg} - defaulting host to 'localhost'")
                    host = "localhost"
                else:
                    lines.append(f"# WARNING: {host_msg}")
                status = "partial"
            if not port_ok:
                lines.append(f"# WARNING: {port_msg}")
                status = "partial"

            lines.append(
                "# UNSUPPORTED: native Pentaho Socket Reader uses proprietary Kettle "
                "binary row serialization (often compressed) between Socket Writer <-> Reader."
            )
            lines.append(
                "# WARNING: Spark Structured Streaming socket source reads UTF-8 text lines "
                "only - not Kettle compressed row packets. Use for text-line feeds only."
            )
            if compressed:
                lines.append(
                    "# WARNING: compressed=Y has no Spark socket equivalent - ignored at runtime"
                )
                status = "partial"
            if buffer_size:
                lines.append(
                    "# NOTE: Spark socket source has no buffer_size option - value preserved above"
                )
            if encoding:
                if encoding.upper() not in ("UTF-8", "UTF8", "UTF_8"):
                    lines.append(
                        f"# WARNING: encoding={encoding!r} - Spark socket source is UTF-8; "
                        "encoding mismatches may corrupt text"
                    )
                    status = "partial"
                else:
                    lines.append(
                        f"# NOTE: encoding={encoding!r} matches Spark socket UTF-8 text source"
                    )
            if delimiter:
                lines.append(
                    "# NOTE: delimiter preserved above - apply split/from_csv after read"
                )
            if timeout:
                lines.append(
                    f"# WARNING: timeout={timeout!r} - Spark socket source has no connect "
                    "timeout option; connection failures / network interruptions fail the query"
                )
                status = "partial"

            # Prefer structured-streaming socket when host+port valid
            text_protocol = protocol in (
                "", "kettle", "tcp", "text", "socket", "line", "lines", "utf8", "utf-8"
            )
            if host and port_ok:
                checkpoint = f"/tmp/checkpoints/{_safe_ident(context.step.name)}"
                lines.append(
                    "# NOTE: Spark socket source is for testing/dev; production prefers Kafka/Event Hubs"
                )
                lines.append(f"{out_var} = (spark.readStream.format('socket')")
                lines.append(f"    .option('host', {host!r})")
                lines.append(f"    .option('port', {port!r})")
                lines.append("    .load())")
                lines.append(
                    f"# value column is STRING line payload; "
                    f"checkpointLocation={checkpoint!r} required on writeStream"
                )
                if fields:
                    ddl = _field_ddl(fields if isinstance(fields, list) else [])
                    lines.append(
                        f"# WARNING: Pentaho typed fields preserved as schema hint only: {ddl}"
                    )
                    lines.append(
                        "# Apply from_json()/split() to map 'value' into typed columns as needed"
                    )
                if protocol in ("kettle", "") or compressed:
                    lines.append(
                        "# WARNING: protocol incompatibility - this is a best-effort text socket "
                        "migration, not a byte-compatible Kettle Socket Reader replacement"
                    )
                    status = "partial"
                elif not text_protocol:
                    lines.append(
                        f"# WARNING: protocol={protocol!r} may not map to Spark socket source"
                    )
                    status = "partial"
                logger.info(
                    "SocketReader %s host=%s port=%s protocol=%s status=%s",
                    context.step.name,
                    host,
                    port,
                    protocol,
                    status,
                )
                return lines, status

            # Cannot generate streaming source - empty placeholder with preserved metadata
            ddl = _field_ddl(fields if isinstance(fields, list) else [])
            lines.append(
                "# WARNING: invalid host/port - emitting empty frame; fix connection settings"
            )
            lines.append(f"{out_var} = spark.createDataFrame([], '{ddl}')")
            logger.warning(
                "SocketReader %s missing/invalid endpoint host=%r port=%r",
                context.step.name,
                host,
                port,
            )
            return lines, "partial"
        except Exception as exc:
            logger.exception("SocketReader failed for %s", context.step.name)
            return [
                f"# Socket Reader: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.createDataFrame([], '_socket_reader STRING')",
            ], "partial"


# ---------------------------------------------------------------------------
# Socket Writer
# ---------------------------------------------------------------------------


class SocketWriterHandler(BaseStepHandler):
    """Socket Writer — preserve listen/buffer/flush metadata; no native Spark socket sink."""

    _TYPES = {"socketwriter"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        try:
            in_df = context.input_df_name()
            step_el = get_step_element(context.step)
            metadata = _meta(context)
            cfg = parse_socket_writer_config(step_el) if step_el is not None else {}
            for key, val in cfg.items():
                metadata.setdefault(key, val)

            host = (metadata.get("hostname") or metadata.get("host") or "").strip()
            port = str(metadata.get("port") or "").strip()
            buffer_size = str(metadata.get("buffer_size") or "2000")
            flush_interval = str(metadata.get("flush_interval") or "5000")
            encoding = (metadata.get("encoding") or "").strip() or "UTF-8"
            output_format = (metadata.get("output_format") or "").strip()
            delimiter = metadata.get("delimiter") or ""
            compressed = metadata.get("compressed")

            lines = [
                f"# Socket Writer: {context.step.name}",
                "# UNSUPPORTED: Databricks / Spark has no built-in socket listen sink "
                "equivalent to Pentaho Socket Writer (server socket + Kettle row protocol).",
                "# WARNING: Preserve host/port/encoding/buffer/flush; emit rows via "
                "foreachBatch TCP client, Kafka bridge, or custom Structured Streaming sink.",
            ]
            lines.extend(
                _preserve_comments(
                    metadata,
                    (
                        "hostname",
                        "host",
                        "port",
                        "buffer_size",
                        "flush_interval",
                        "compressed",
                        "compressed_raw",
                        "encoding",
                        "output_format",
                        "delimiter",
                        "fields",
                        "options",
                    ),
                    context=context,
                )
            )

            port_ok, port_msg = _validate_port(port)
            if not port_ok:
                lines.append(f"# WARNING: {port_msg}")
            if host:
                host_ok, host_msg = _validate_host(host)
                if not host_ok:
                    lines.append(f"# WARNING: {host_msg}")
            else:
                lines.append(
                    "# NOTE: native Socket Writer binds a server socket on port "
                    "(no hostname in PDI XML); clients connect inbound"
                )

            if compressed:
                lines.append(
                    "# WARNING: compressed=Y (Kettle GZIP row packets) has no Spark sink equivalent"
                )
            if encoding.upper() not in ("UTF-8", "UTF8", "UTF_8"):
                lines.append(
                    f"# WARNING: encoding={encoding!r} - ensure foreachBatch encodes accordingly"
                )
            lines.append(
                f"# preserved.flush_behavior: buffer_size={buffer_size!r}, "
                f"flush_interval_ms={flush_interval!r}"
            )
            if output_format:
                lines.append(
                    f"# NOTE: output_format={output_format!r} - serialize in foreachBatch accordingly"
                )
            if delimiter:
                lines.append(
                    f"# NOTE: delimiter={delimiter!r} - join columns before socket send if needed"
                )

            # Practical sketch: foreachBatch line writer (commented — not auto-run)
            endpoint = f"{host or '0.0.0.0'}:{port or '<port>'}"
            feed = in_df or out_var
            lines.append("# Practical text-line sketch (enable only for non-Kettle text peers):")
            lines.append("# import socket")
            lines.append(f"# def _socket_write_batch(batch_df, batch_id):")
            lines.append(f"#     # WARNING: connection failures / timeouts / network interruptions")
            lines.append(f"#     # must be handled by the caller (retries, backoff).")
            lines.append(f"#     host, port = {host or 'localhost'!r}, int({port or '0'!r})")
            lines.append(
                f"#     payload = batch_df.toJSON().collect()  "
                f"# encoding={encoding!r}; format={output_format or 'json'!r}"
            )
            lines.append("#     with socket.create_connection((host, port), timeout=30) as sock:")
            lines.append(
                f"#         for line in payload:"
            )
            lines.append(
                f"#             sock.sendall((line + '\\n').encode({encoding!r}))"
            )
            lines.append("#             # flush interval / buffering is application-level")
            lines.append(
                f"# # ({feed}).writeStream.foreachBatch(_socket_write_batch)"
                f".option('checkpointLocation', "
                f"'/tmp/checkpoints/{_safe_ident(context.step.name)}').start()"
            )
            lines.append(
                f"# NOTE: endpoint={endpoint!r}; do not use for Kettle Socket Reader peers"
            )

            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(
                    f"{out_var} = spark.createDataFrame([], '_socket_writer STRING')"
                )
            logger.warning(
                "SocketWriter %s: no native sink; port=%s buffer=%s flush=%s",
                context.step.name,
                port,
                buffer_size,
                flush_interval,
            )
            return lines, "partial"
        except Exception as exc:
            logger.exception("SocketWriter failed for %s", context.step.name)
            return [
                f"# Socket Writer: {context.step.name}",
                f"# ERROR: {exc}",
                "# UNSUPPORTED: no native Databricks socket sink",
                f"{out_var} = spark.createDataFrame([], '_socket_writer STRING')",
            ], "partial"


INLINE_HANDLERS: list[BaseStepHandler] = [
    InjectorHandler(),
    SocketReaderHandler(),
    SocketWriterHandler(),
]
