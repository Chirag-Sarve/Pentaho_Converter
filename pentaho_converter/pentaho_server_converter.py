"""Convert Pentaho Server (BA Server) steps to Databricks-compatible Python.

Supports:
- Call Endpoint → ``requests`` HTTP against the constructed BA API URL
- Get Session Variables → notebook session dict / Spark conf / env / widgets
- Set Session Variables → same runtime stores (overwrite semantics preserved)

Stock Call Endpoint has no headers/timeout/retry/SSL schema; when present as
extension tags they are honoured. BA session authentication
(``isBypassingAuthentication``) has no Databricks equivalent and is documented
as a migration limitation with HTTP basic-auth fallback when credentials exist.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .lineage import substitute_pentaho_variables
from .schema_utils import spark_cast_type

logger = logging.getLogger(__name__)

# Matches PDI SessionHelper.SIMULATED_SESSION_PREFIX used outside BA Server.
_SIMULATED_SESSION_PREFIX = "_FAKE_SESSION_"
_SESSION_CONF_PREFIX = "pentaho.session."
_SESSION_DICT = "_pentaho_session_vars"

_CALL_PRESERVE = (
    "server_url",
    "url",
    "username",
    "password_configured",
    "password_secret_ref",
    "use_session_authentication",
    "is_bypassing_authentication",
    "module_name",
    "module_from_field",
    "endpoint_path",
    "http_method",
    "endpoint_from_field",
    "result_field",
    "status_code_field",
    "response_time_field",
    "parameters",
    "request_parameters",
    "headers",
    "timeout",
    "connection_timeout",
    "read_timeout",
    "retries",
    "retry_delay",
    "verify_ssl",
    "ssl_settings",
    "request_body",
    "content_type",
    "encoding",
    "variable_substitution",
    "error_target_step",
    "logging_level",
)


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _empty_df(out_var: str) -> list[str]:
    return [
        "from pyspark.sql.types import StructType",
        f"{out_var} = spark.createDataFrame([], StructType([]))",
    ]


def _preserve_safe(metadata: dict[str, Any], keys: tuple[str, ...] = ()) -> list[str]:
    lines: list[str] = []
    # Explicit keys always emit (including ``fields``); skip only applies to the scan tail.
    skip = frozenset({
        "extras", "step_type", "step_name", "attributes",
        "transformation_parameters", "_propagated_keys", "_propagation_trace",
        "password",
    })
    redact_tokens = ("password", "secret", "token", "credential")
    seen: set[str] = set()
    explicit = frozenset(keys)

    def _emit(key: str, val: object) -> None:
        low = key.lower()
        is_safe = (
            key.endswith("_configured")
            or key.endswith("_ref")
            or key.endswith("_field")
            or "in_field" in low
            or "from_field" in low
        )
        if any(tok in low for tok in redact_tokens) and not is_safe:
            lines.append(f"# preserved.{key}=<redacted>")
        else:
            lines.append(f"# preserved.{key}={val!r}")

    for key in keys:
        if key in seen:
            continue
        val = metadata.get(key)
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)

    extras = metadata.get("extras")
    if isinstance(extras, dict):
        for key, val in extras.items():
            tag = f"extras.{key}"
            if tag in seen or val in (None, "", [], {}):
                continue
            seen.add(tag)
            _emit(tag, val)

    for key, val in metadata.items():
        if key in seen or key in skip or key in explicit:
            continue
        if key == "fields":
            # Already emitted when explicitly requested; otherwise avoid dumping large lists twice.
            continue
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    return lines


def _timeout_seconds(raw: Any, default: float = 30.0) -> float:
    try:
        if raw in (None, ""):
            return default
        val = float(raw)
        # Pentaho sometimes stores milliseconds
        if val > 1000:
            return val / 1000.0
        return val
    except (TypeError, ValueError):
        return default


def _session_key(variable_string: str) -> str:
    """Bare session attribute name from ${VAR} / %%VAR%% / bare identifier."""
    text = (variable_string or "").strip()
    if not text:
        return ""
    m = re.fullmatch(r"\$\{([^}]+)\}", text)
    if m:
        return m.group(1).strip()
    m = re.fullmatch(r"%%([^%]+)%%", text)
    if m:
        return m.group(1).strip()
    return text


def _ensure_session_store(lines: list[str]) -> None:
    lines.append(
        f"{_SESSION_DICT} = globals().setdefault({_SESSION_DICT!r}, {{}})"
    )


def _build_endpoint_url(server_url: str, module_name: str, endpoint_path: str) -> str:
    """Mirror HttpConnectionHelper.invokeEndpoint URL composition."""
    base = (server_url or "").rstrip("/") + "/"
    module = (module_name or "").strip()
    path = endpoint_path or ""
    if module == "platform":
        base = base + "api"
    else:
        base = base + "plugin/" + module + "/api"
    if path.startswith("/"):
        return base + path
    return base + "/" + path if path else base


def convert_call_endpoint_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    parameters: dict[str, str] | None = None,
) -> tuple[list[str], str]:
    """Generate Python ``requests`` code for Call Endpoint."""
    params_map = dict(parameters or {})
    status = "converted"
    lines = [f"# Call Endpoint: {step_name}"]
    lines.extend(_preserve_safe(metadata, _CALL_PRESERVE))

    server_url = substitute_pentaho_variables(
        str(metadata.get("server_url") or metadata.get("url") or ""), params_map
    )
    module_name = substitute_pentaho_variables(
        str(metadata.get("module_name") or ""), params_map
    )
    endpoint_path = substitute_pentaho_variables(
        str(metadata.get("endpoint_path") or ""), params_map
    )
    http_method_raw = substitute_pentaho_variables(
        str(metadata.get("http_method") or "GET"), params_map
    ).strip() or "GET"
    module_from_field = bool(metadata.get("module_from_field"))
    endpoint_from_field = bool(metadata.get("endpoint_from_field"))
    # Stock CallEndpointStep: when isEndpointFromField, module/path/method are field names.
    # isModuleFromField alone only affects the Spoon dialog; runtime uses endpoint_from_field.
    field_mode = endpoint_from_field or module_from_field
    http_method = http_method_raw if field_mode else http_method_raw.upper()
    http_method_literal = "GET" if field_mode else http_method
    use_session_auth = bool(
        metadata.get("use_session_authentication")
        or metadata.get("is_bypassing_authentication")
    )
    result_field = metadata.get("result_field") or "result"
    code_field = metadata.get("status_code_field") or ""
    time_field = metadata.get("response_time_field") or ""
    req_params = metadata.get("parameters") or metadata.get("request_parameters") or []
    headers_cfg = metadata.get("headers") or []
    timeout = _timeout_seconds(metadata.get("timeout") or metadata.get("connection_timeout"), 30.0)
    read_timeout_raw = metadata.get("read_timeout")
    read_timeout = (
        _timeout_seconds(read_timeout_raw, timeout) if read_timeout_raw not in (None, "") else timeout
    )
    retries_raw = metadata.get("retries")
    try:
        retries = int(retries_raw) if retries_raw not in (None, "") else 0
    except (TypeError, ValueError):
        retries = 0
    retry_delay = _timeout_seconds(metadata.get("retry_delay"), 1.0)
    verify_ssl = metadata.get("verify_ssl")
    if verify_ssl is None:
        verify_ssl = True
    request_body = substitute_pentaho_variables(
        str(metadata.get("request_body") or ""), params_map
    )
    content_type = str(metadata.get("content_type") or "")
    username = substitute_pentaho_variables(str(metadata.get("username") or ""), params_map)
    password_configured = bool(metadata.get("password_configured"))
    password_ref = metadata.get("password_secret_ref") or (
        "dbutils.secrets.get(scope='pentaho_server', key='password')"
        if password_configured
        else ""
    )

    if not server_url and not field_mode:
        lines.append("# WARNING: missing endpoint server URL")
        status = "partial"
        logger.warning("Call Endpoint '%s': missing server URL", step_name)
    elif server_url and not (
        server_url.startswith("http://")
        or server_url.startswith("https://")
        or "${" in server_url
        or "%%" in server_url
    ):
        lines.append(f"# WARNING: URL may be invalid (missing scheme): {server_url!r}")
        status = "partial"
        logger.warning("Call Endpoint '%s': invalid URL scheme %r", step_name, server_url)

    if not module_name and not field_mode:
        lines.append("# WARNING: missing BA Server module name")
        status = "partial"
        logger.warning("Call Endpoint '%s': missing module name", step_name)
    if not endpoint_path and not field_mode:
        lines.append("# WARNING: missing endpoint path")
        status = "partial"
        logger.warning("Call Endpoint '%s': missing endpoint path", step_name)

    if use_session_auth:
        lines.append(
            "# LIMITATION: isBypassingAuthentication / BA Server session auth has no "
            "Databricks equivalent — falling back to HTTP basic auth when credentials "
            "are configured; in-process servlet dispatch is unsupported."
        )
        status = "partial"
        logger.warning(
            "Call Endpoint '%s': BA session authentication unsupported on Databricks",
            step_name,
        )

    if metadata.get("error_target_step"):
        lines.append(
            f"# LIMITATION: error hop target {metadata.get('error_target_step')!r} is "
            "preserved as metadata — row-level HTTP failures stay in status/result columns"
        )
        status = "partial"

    if metadata.get("variable_substitution"):
        lines.append(
            "# NOTE: ${VAR} / %%VAR%% placeholders resolve from transformation parameters "
            "at migration time; remaining tokens resolve from widgets/env/spark.conf/"
            f"{_SESSION_DICT} at runtime (failures yield empty string)"
        )

    lines.append("import os, time, requests")
    lines.append("from requests.auth import HTTPBasicAuth")
    lines.append(f"_{out_var}_method = {http_method_literal!r}")
    lines.append(f"_{out_var}_timeout = ({timeout!r}, {read_timeout!r})")
    lines.append(f"_{out_var}_verify_ssl = {bool(verify_ssl)!r}")
    lines.append(f"_{out_var}_retries = {retries}")
    lines.append(f"_{out_var}_retry_delay = {retry_delay!r}")

    static_headers = {
        (h.get("name") or ""): (h.get("value") or "")
        for h in headers_cfg
        if isinstance(h, dict) and h.get("name") and h.get("value")
    }
    if content_type:
        static_headers.setdefault("Content-Type", content_type)
    lines.append(f"_{out_var}_headers = {static_headers!r}")

    if password_configured and password_ref:
        lines.append(f"_{out_var}_password = None")
        lines.append("try:")
        lines.append(f"    _{out_var}_password = {password_ref}")
        lines.append("except Exception as _secret_err:")
        lines.append(
            f"    _{out_var}_password = os.environ.get('PENTAHO_SERVER_PASSWORD')"
        )
        lines.append(
            f"    _ = _secret_err  # fall back to env if Secrets scope missing"
        )
    else:
        lines.append(f"_{out_var}_password = os.environ.get('PENTAHO_SERVER_PASSWORD')")

    lines.append(f"_{out_var}_user = {username!r} or os.environ.get('PENTAHO_SERVER_USER', '')")
    lines.append(
        f"_{out_var}_auth = HTTPBasicAuth(_{out_var}_user, _{out_var}_password) "
        f"if (_{out_var}_user or _{out_var}_password) else None"
    )

    # Runtime variable substitution helper for leftover ${VAR} tokens
    lines.append(f"def _{out_var}_subst(_text):")
    lines.append("    import re as _re_ce")
    lines.append("    if not _text or ('${' not in str(_text) and '%%' not in str(_text)):")
    lines.append("        return _text")
    lines.append("    def _lookup(_m):")
    lines.append("        _k = _m.group(1)")
    lines.append("        _v = None")
    lines.append("        _dbu = globals().get('dbutils')")
    lines.append("        if _dbu is not None and hasattr(_dbu, 'widgets'):")
    lines.append("            try:")
    lines.append("                _v = _dbu.widgets.get(_k)")
    lines.append("            except Exception:")
    lines.append("                _v = None")
    lines.append("        if _v in (None, ''):")
    lines.append("            _v = os.environ.get(_k)")
    lines.append("        if _v in (None, ''):")
    lines.append("            try:")
    lines.append(f"                _v = spark.conf.get('pentaho.var.' + _k)")
    lines.append("            except Exception:")
    lines.append("                _v = None")
    lines.append("        if _v in (None, ''):")
    lines.append(f"            _v = {_SESSION_DICT}.get(_k) if '{_SESSION_DICT}' in globals() else None")
    lines.append("        return '' if _v is None else str(_v)")
    lines.append("    _out = _re_ce.sub(r'\\$\\{([^}]+)\\}', _lookup, str(_text))")
    lines.append("    return _re_ce.sub(r'%%([^%]+)%%', _lookup, _out)")

    lines.append(f"def _{out_var}_build_url(server, module, path):")
    lines.append("    server = str(server or '').rstrip('/') + '/'")
    lines.append("    module = str(module or '').strip()")
    lines.append("    path = str(path or '')")
    lines.append("    if module == 'platform':")
    lines.append("        base = server + 'api'")
    lines.append("    else:")
    lines.append("        base = server + 'plugin/' + module + '/api'")
    lines.append("    if path.startswith('/'):")
    lines.append("        return base + path")
    lines.append("    return base + '/' + path if path else base")

    lines.append(
        f"def _{out_var}_request(url, method='GET', params=None, data=None, headers=None):"
    )
    lines.append("    t0 = time.time()")
    lines.append("    last_exc = None")
    lines.append(f"    for _attempt in range(max(1, _{out_var}_retries + 1)):")
    lines.append("        try:")
    lines.append(
        f"            resp = requests.request("
        f"str(method).upper(), str(url), params=params or {{}}, data=data, "
        f"headers=headers or {{}}, timeout=_{out_var}_timeout, "
        f"auth=_{out_var}_auth, verify=_{out_var}_verify_ssl)"
    )
    lines.append(
        "            return resp.status_code, (resp.text if resp.text is not None else ''), "
        "int((time.time() - t0) * 1000)"
    )
    lines.append("        except requests.exceptions.SSLError as exc:")
    lines.append("            last_exc = ('SSL_ERROR', str(exc))")
    lines.append("            break")
    lines.append("        except requests.exceptions.Timeout as exc:")
    lines.append("            last_exc = ('TIMEOUT', str(exc))")
    lines.append(f"            if _attempt < _{out_var}_retries:")
    lines.append(f"                time.sleep(_{out_var}_retry_delay)")
    lines.append("                continue")
    lines.append("            break")
    lines.append("        except requests.exceptions.RequestException as exc:")
    lines.append("            last_exc = ('REQUEST_ERROR', str(exc))")
    lines.append(f"            if _attempt < _{out_var}_retries:")
    lines.append(f"                time.sleep(_{out_var}_retry_delay)")
    lines.append("                continue")
    lines.append("            break")
    lines.append("    code, body = (last_exc if last_exc else (None, 'EMPTY_RESPONSE'))")
    lines.append("    return code, body, int((time.time() - t0) * 1000)")
    lines.append(
        "# EDGE: auth failures (401/403), HTTP errors, empty bodies, timeouts, and SSL "
        "errors are returned as status/body columns rather than raising"
    )

    if not in_df:
        if not server_url or not module_name or not endpoint_path:
            lines.append("# WARNING: cannot call endpoint without URL/module/path")
            lines.extend(_empty_df(out_var))
            return lines, "partial"

        url = _build_endpoint_url(server_url, module_name, endpoint_path)
        query: dict[str, str] = {}
        for p in req_params:
            if not isinstance(p, dict):
                continue
            pname = p.get("parameter") or ""
            if pname:
                query[pname] = substitute_pentaho_variables(
                    str(p.get("default_value") or ""), params_map
                )
        lines.append(f"_{out_var}_url = _{out_var}_subst({url!r})")
        lines.append(f"_{out_var}_params = {query!r}")
        body_arg = "None"
        if request_body:
            lines.append(f"_{out_var}_body = _{out_var}_subst({request_body!r})")
            body_arg = f"_{out_var}_body"
        elif http_method in ("POST", "PUT", "PATCH") and query:
            lines.append(
                f"_{out_var}_body = '&'.join("
                f"f'{{k}}={{v}}' for k, v in _{out_var}_params.items())"
            )
            lines.append(f"_{out_var}_params = {{}}")
            lines.append(
                f"_{out_var}_headers.setdefault("
                f"'Content-Type', 'application/x-www-form-urlencoded')"
            )
            body_arg = f"_{out_var}_body"
        lines.append(
            f"_code, _body, _ms = _{out_var}_request("
            f"_{out_var}_url, _{out_var}_method, _{out_var}_params, "
            f"{body_arg}, _{out_var}_headers)"
        )
        cols = [f"{result_field!r}: _body"]
        if code_field:
            cols.append(f"{code_field!r}: _code")
        if time_field:
            cols.append(f"{time_field!r}: _ms")
        lines.append(f"{out_var} = spark.createDataFrame([{{{', '.join(cols)}}}])")
        return lines, status

    lines.append(
        "# NOTE: row-wise HTTP on the driver; scale with mapPartitions / Session for volume"
    )
    lines.append("from pyspark.sql import Row")
    lines.append(f"_ce_rows_{out_var} = []")
    lines.append(f"for _row in {in_df}.toLocalIterator():")
    lines.append("    _d = _row.asDict(recursive=True)")
    if field_mode:
        lines.append(
            f"    _module = _{out_var}_subst(str(_d.get({module_name!r}) or {module_name!r}))"
        )
        lines.append(
            f"    _path = _{out_var}_subst(str(_d.get({endpoint_path!r}) or {endpoint_path!r}))"
        )
        lines.append(
            f"    _method = str(_d.get({http_method_raw!r}) or _{out_var}_method).upper()"
        )
    else:
        lines.append(f"    _module = _{out_var}_subst({module_name!r})")
        lines.append(f"    _path = _{out_var}_subst({endpoint_path!r})")
        lines.append(f"    _method = _{out_var}_method")
    lines.append(f"    _server = _{out_var}_subst({server_url!r})")
    lines.append(f"    if not _server or not _module or not _path:")
    lines.append(
        "        _code, _body, _ms = None, 'MISSING_ENDPOINT', 0"
    )
    lines.append("    else:")
    lines.append(f"        _url = _{out_var}_build_url(_server, _module, _path)")
    lines.append("        _params = {}")
    for p in req_params:
        if not isinstance(p, dict):
            continue
        pname = p.get("parameter") or ""
        field = p.get("field_name") or ""
        default = substitute_pentaho_variables(str(p.get("default_value") or ""), params_map)
        if not pname:
            continue
        if field:
            lines.append(f"        _pval = _d.get({field!r})")
            lines.append(f"        if _pval is None or _pval == '':")
            lines.append(f"            _pval = _{out_var}_subst({default!r})")
            lines.append(
                f"        _params[{pname!r}] = None if _pval is None else str(_pval)"
            )
        else:
            lines.append(f"        _params[{pname!r}] = _{out_var}_subst({default!r})")
    lines.append(f"        _hdrs = dict(_{out_var}_headers)")
    for h in headers_cfg:
        if not isinstance(h, dict):
            continue
        name = h.get("name") or ""
        field = h.get("field") or ""
        if name and field and not h.get("value"):
            lines.append(f"        _hdrs[{name!r}] = _d.get({field!r})")
    if request_body:
        lines.append(f"        _data = _{out_var}_subst({request_body!r})")
    else:
        lines.append("        _data = None")
        lines.append(
            "        if str(_method).upper() in ('POST', 'PUT', 'PATCH') "
            "and _params and _data is None:"
        )
        lines.append(
            "            _data = '&'.join(f'{k}={v}' for k, v in list(_params.items()))"
        )
        lines.append("            _params = {}")
        lines.append(
            "            _hdrs.setdefault('Content-Type', 'application/x-www-form-urlencoded')"
        )
    lines.append(
        f"        _code, _body, _ms = _{out_var}_request("
        f"_url, _method, _params, _data, _hdrs)"
    )
    lines.append("    _d = dict(_d)")
    lines.append(f"    _d[{result_field!r}] = _body")
    if code_field:
        lines.append(f"    _d[{code_field!r}] = _code")
    if time_field:
        lines.append(f"    _d[{time_field!r}] = _ms")
    lines.append(f"    _ce_rows_{out_var}.append(Row(**_d))")
    lines.append(f"if _ce_rows_{out_var}:")
    lines.append(f"    {out_var} = spark.createDataFrame(_ce_rows_{out_var})")
    lines.append("else:")
    lines.append(f"    {out_var} = {in_df}")
    lines.append(
        f"# EDGE: empty responses / HTTP errors / timeouts / SSL / auth failures surface in "
        f"{result_field!r} / status columns rather than failing the job"
    )
    logger.info(
        "Call Endpoint '%s' method=%s module=%s path=%s",
        step_name,
        http_method,
        module_name,
        endpoint_path,
    )
    return lines, status


def convert_get_session_variables_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    parameters: dict[str, str] | None = None,
) -> tuple[list[str], str]:
    """Resolve BA session variables via notebook store / conf / env / widgets."""
    params_map = dict(parameters or {})
    status = "converted"
    fields = metadata.get("fields") or []
    if not isinstance(fields, list):
        fields = []

    lines = [f"# Get Session Variables: {step_name}"]
    lines.extend(_preserve_safe(metadata, (
        "fields", "session_variables", "variable_names", "output_columns",
        "scope", "variable_inheritance",
    )))
    lines.append(
        "# LIMITATION: Pentaho BA HTTP session attributes have no exact Databricks "
        "equivalent — using notebook-scoped session dict + spark.conf "
        f"({_SESSION_CONF_PREFIX}*) + env/widgets. Variable inheritance across "
        "BA user sessions is unsupported."
    )
    status = "partial"
    lines.append("import os")
    _ensure_session_store(lines)

    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.range(1).select(lit(1).alias('_row'))")

    if not fields:
        lines.append("# WARNING: no session variables configured")
        return lines, "partial"

    for field in fields:
        if not isinstance(field, dict):
            continue
        field_name = (field.get("name") or "").strip()
        variable_str = field.get("variable") or field.get("variable_name") or ""
        if not field_name:
            continue
        default = substitute_pentaho_variables(
            str(field.get("default_value") or ""), params_map
        )
        session_key = _session_key(variable_str)
        type_name = field.get("type") or field.get("type_name") or "String"
        trim_type = (field.get("trim_type") or "none").lower()
        safe = _safe_ident(field_name)
        conf_key = f"{_SESSION_CONF_PREFIX}{session_key}" if session_key else ""
        fake_key = f"{_SIMULATED_SESSION_PREFIX}{session_key}" if session_key else ""

        lines.append(
            f"# field {field_name!r} session variable {variable_str!r} "
            f"scope={field.get('scope') or 'BA_SESSION'!r}"
        )
        for meta_key in (
            "format", "currency", "decimal", "group", "length", "precision", "trim_type", "type",
        ):
            meta_val = field.get(meta_key)
            if meta_val not in (None, "", -1, "-1"):
                lines.append(f"# preserved.field.{field_name}.{meta_key}={meta_val!r}")

        if not session_key:
            lines.append(
                f"# WARNING: missing/undefined session variable for field {field_name!r} "
                f"— using default {default!r}"
            )
            logger.warning(
                "Get Session Variables '%s': missing variable for field %s",
                step_name,
                field_name,
            )
            lines.append(f"_{safe}_resolved = {default!r}")
        else:
            # Lookup: session dict → simulated prefix → widgets → env → spark.conf → default
            lines.append(f"_{safe}_resolved = {_SESSION_DICT}.get({session_key!r})")
            lines.append(f"if _{safe}_resolved is None:")
            lines.append(f"    _{safe}_resolved = {_SESSION_DICT}.get({fake_key!r})")
            lines.append(f"if _{safe}_resolved in (None, ''):")
            lines.append(f"    _dbu_{safe} = globals().get('dbutils')")
            lines.append(
                f"    if _dbu_{safe} is not None and hasattr(_dbu_{safe}, 'widgets'):"
            )
            lines.append(f"        try:")
            lines.append(
                f"            _{safe}_resolved = _dbu_{safe}.widgets.get({session_key!r})"
            )
            lines.append(f"        except Exception:")
            lines.append(f"            _{safe}_resolved = None")
            lines.append(f"if _{safe}_resolved in (None, ''):")
            lines.append(f"    _{safe}_resolved = os.environ.get({session_key!r})")
            lines.append(f"if _{safe}_resolved in (None, ''):")
            lines.append(f"    _{safe}_resolved = os.environ.get({fake_key!r})")
            lines.append(f"if _{safe}_resolved in (None, ''):")
            lines.append(f"    try:")
            lines.append(f"        _{safe}_resolved = spark.conf.get({conf_key!r})")
            lines.append(f"    except Exception:")
            lines.append(f"        _{safe}_resolved = None")
            lines.append(f"if _{safe}_resolved in (None, ''):")
            param_default = params_map.get(session_key, default)
            lines.append(f"    _{safe}_resolved = {param_default!r}")
            lines.append(f"if _{safe}_resolved is None:")
            lines.append(f"    _{safe}_resolved = {default!r}")

        if trim_type in ("left", "ltrim"):
            lines.append(
                f"_{safe}_resolved = str(_{safe}_resolved).lstrip() "
                f"if _{safe}_resolved is not None else None"
            )
        elif trim_type in ("right", "rtrim"):
            lines.append(
                f"_{safe}_resolved = str(_{safe}_resolved).rstrip() "
                f"if _{safe}_resolved is not None else None"
            )
        elif trim_type in ("both", "trim"):
            lines.append(
                f"_{safe}_resolved = str(_{safe}_resolved).strip() "
                f"if _{safe}_resolved is not None else None"
            )

        spark_t = spark_cast_type(type_name)
        format_mask = field.get("format") or ""
        if spark_t == "string" or not spark_t:
            lines.append(
                f"{out_var} = {out_var}.withColumn({field_name!r}, lit(_{safe}_resolved))"
            )
        elif type_name.strip().lower() in ("date",) and format_mask:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{field_name!r}, to_date(lit(_{safe}_resolved), {format_mask!r}))"
            )
        elif type_name.strip().lower() in ("timestamp", "datetime") and format_mask:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{field_name!r}, to_timestamp(lit(_{safe}_resolved), {format_mask!r}))"
            )
        else:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{field_name!r}, lit(_{safe}_resolved).cast({spark_t!r}))"
            )

    if not in_df:
        lines.append(f"{out_var} = {out_var}.drop('_row')")

    logger.info("Get Session Variables '%s' fields=%d", step_name, len(fields))
    return lines, status


def convert_set_session_variables_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    parameters: dict[str, str] | None = None,
) -> tuple[list[str], str]:
    """Write BA session variables into notebook session store / conf / env / widgets."""
    params_map = dict(parameters or {})
    status = "converted"
    fields = metadata.get("fields") or []
    if not isinstance(fields, list):
        fields = []
    use_formatting = bool(metadata.get("use_formatting"))
    overwrite = metadata.get("overwrite")
    if overwrite is None:
        overwrite = True

    lines = [f"# Set Session Variables: {step_name}"]
    lines.extend(_preserve_safe(metadata, (
        "fields", "use_formatting", "overwrite", "scope",
        "variable_names", "session_variables",
    )))
    lines.append(
        "# LIMITATION: Pentaho BA HTTP session setAttribute semantics are approximated "
        f"with notebook {_SESSION_DICT}, spark.conf ({_SESSION_CONF_PREFIX}*), "
        "os.environ, and optional Databricks widgets. Cross-request BA sessions and "
        "SessionHelper blacklists are unsupported."
    )
    status = "partial"
    lines.append("import os")
    _ensure_session_store(lines)

    if use_formatting:
        lines.append(
            "# LIMITATION: use_formatting=True — Pentaho ValueMeta masks are not "
            "applied; values are coerced with str()"
        )

    if in_df and fields:
        field_names = sorted({
            f.get("field_name") for f in fields
            if isinstance(f, dict) and f.get("field_name")
        })
        if field_names:
            cols = ", ".join(repr(n) for n in field_names)
            lines.append(
                f"_sess_rows_{out_var} = {in_df}.limit(2).select({cols}).collect()"
            )
            lines.append(
                f"if len(_sess_rows_{out_var}) > 1:"
            )
            lines.append(
                "    raise ValueError("
                "'Set Session Variables received more than one row "
                "(Pentaho accepts only the first row)')"
            )
            lines.append(
                f"_sess_vals_{out_var} = _sess_rows_{out_var}[0].asDict() "
                f"if _sess_rows_{out_var} else {{}}"
            )
        else:
            lines.append(f"_sess_vals_{out_var} = {{}}")
    else:
        lines.append(f"_sess_vals_{out_var} = {{}}")
        if not in_df and fields:
            lines.append(
                "# WARNING: no input row — using default_value for each session variable"
            )

    seen: set[str] = set()
    for field in fields:
        if not isinstance(field, dict):
            continue
        field_name = (field.get("field_name") or "").strip()
        var_raw = (field.get("variable_name") or field.get("variable") or field_name).strip()
        session_key = _session_key(var_raw)
        if not session_key:
            lines.append("# WARNING: empty session variable name — skipped")
            logger.warning(
                "Set Session Variables '%s': empty variable name skipped",
                step_name,
            )
            continue
        if session_key in seen:
            lines.append(
                f"# WARNING: duplicate session variable {session_key!r} — later mapping wins"
            )
        seen.add(session_key)
        default = substitute_pentaho_variables(
            str(field.get("default_value") or ""), params_map
        )
        safe = _safe_ident(session_key)
        conf_key = f"{_SESSION_CONF_PREFIX}{session_key}"
        fake_key = f"{_SIMULATED_SESSION_PREFIX}{session_key}"

        lines.append(
            f"# variable {session_key!r} scope={field.get('scope') or 'BA_SESSION'!r} "
            f"overwrite={bool(overwrite)!r}"
        )
        if field_name:
            lines.append(f"_{safe}_val = _sess_vals_{out_var}.get({field_name!r})")
            lines.append(f"if _{safe}_val is None:")
            lines.append(f"    _{safe}_val = {default!r}")
        else:
            lines.append(f"_{safe}_val = {default!r}")
        lines.append(
            f"_{safe}_str = '' if _{safe}_val is None else str(_{safe}_val)"
        )
        lines.append(
            f"# null/empty resolved values are written as '' "
            f"(BA session attributes accept null; Databricks stores empty string)"
        )

        if overwrite:
            lines.append(f"{_SESSION_DICT}[{session_key!r}] = _{safe}_str")
            lines.append(f"{_SESSION_DICT}[{fake_key!r}] = _{safe}_str")
            lines.append(f"spark.conf.set({conf_key!r}, _{safe}_str)")
            lines.append(f"os.environ[{session_key!r}] = _{safe}_str")
        else:
            lines.append(f"if {session_key!r} not in {_SESSION_DICT}:")
            lines.append(f"    {_SESSION_DICT}[{session_key!r}] = _{safe}_str")
            lines.append(f"    {_SESSION_DICT}[{fake_key!r}] = _{safe}_str")
            lines.append(f"    spark.conf.set({conf_key!r}, _{safe}_str)")
            lines.append(f"    os.environ[{session_key!r}] = _{safe}_str")
            lines.append(
                f"else:"
            )
            lines.append(
                f"    pass  # overwrite=False — kept existing session value for {session_key!r}"
            )

        lines.append(f"_dbu_{safe} = globals().get('dbutils')")
        lines.append(f"if _dbu_{safe} is not None and hasattr(_dbu_{safe}, 'widgets'):")
        lines.append(f"    try:")
        if overwrite:
            lines.append(f"        _dbu_{safe}.widgets.text({session_key!r}, _{safe}_str)")
        else:
            lines.append(f"        _existing = None")
            lines.append(f"        try:")
            lines.append(f"            _existing = _dbu_{safe}.widgets.get({session_key!r})")
            lines.append(f"        except Exception:")
            lines.append(f"            _existing = None")
            lines.append(f"        if _existing in (None, ''):")
            lines.append(f"            _dbu_{safe}.widgets.text({session_key!r}, _{safe}_str)")
        lines.append(f"    except Exception as _widget_err_{safe}:")
        lines.append(f"        _ = _widget_err_{safe}")

    if not fields:
        lines.append("# WARNING: no session variables configured")
        status = "partial"

    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(
            f"{out_var} = spark.createDataFrame([], '_set_session_variables STRING')"
        )

    logger.info("Set Session Variables '%s' vars=%d", step_name, len(seen))
    return lines, status
