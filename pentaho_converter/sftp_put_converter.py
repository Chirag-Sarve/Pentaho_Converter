"""Convert Pentaho Experimental SFTP Put to Databricks-compatible Python.

Prefer Paramiko SFTP on the driver, Databricks Secrets for credentials, and
environment / Spark conf for host and username. Never emit plaintext passwords
or private key material.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .lineage import substitute_pentaho_variables

logger = logging.getLogger(__name__)

_PRESERVE_KEYS = (
    "host",
    "port",
    "username",
    "authentication_method",
    "password_configured",
    "password_secret_ref",
    "use_private_key",
    "private_key_ref",
    "private_key_secret_ref",
    "key_file",
    "passphrase_configured",
    "passphrase_secret_ref",
    "local_filename",
    "local_directory",
    "local_filename_field",
    "remote_filename",
    "remote_filename_field",
    "remote_directory",
    "remote_directory_field",
    "create_remote_directory",
    "overwrite",
    "append",
    "transfer_mode",
    "timeout",
    "proxy_type",
    "proxy_host",
    "proxy_port",
    "proxy_username",
    "proxy_password_configured",
    "proxy_password_secret_ref",
    "compression",
    "variable_substitution",
    "input_is_stream",
    "add_filename_to_result",
    "after_sftp_put",
    "destination_folder",
    "destination_folder_field",
    "create_destination_folder",
    "wildcard",
    "copy_previous",
    "copy_previous_files",
    "success_when_no_file",
    "use_old_ssh_algorithms",
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


def _preserve_safe(metadata: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    skip = frozenset({
        "extras", "step_type", "step_name", "attributes", "fields",
        "transformation_parameters", "_propagated_keys", "_propagation_trace",
        "password", "keyfilepass", "passphrase", "proxyPassword", "proxy_password",
    })
    redact_tokens = ("password", "passphrase", "private_key", "keyfilepass")
    seen: set[str] = set()

    def _emit(key: str, val: object) -> None:
        low = key.lower()
        is_safe = (
            key.endswith("_configured")
            or key.endswith("_ref")
            or key.endswith("_field")
            or "secret_ref" in low
            or key.startswith("use_")
            or key in ("key_file", "private_key_ref", "authentication_method")
        )
        if any(tok in low for tok in redact_tokens) and not is_safe:
            lines.append(f"# preserved.{key}=<redacted>")
        else:
            lines.append(f"# preserved.{key}={val!r}")

    for key in keys:
        if key in seen or key in skip:
            continue
        val = metadata.get(key)
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    for key, val in metadata.items():
        if key in seen or key in skip:
            continue
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    return lines


def _subst(value: str, parameters: dict[str, str] | None) -> str:
    if not value:
        return ""
    return substitute_pentaho_variables(value, parameters or {})


def convert_sftp_put_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    *,
    parameters: dict[str, str] | None = None,
) -> tuple[list[str], str]:
    """Emit driver-side Paramiko SFTP Put with Databricks Secrets placeholders."""
    fn = _safe_ident(step_name)
    params = parameters or {}
    lines = [f"# SFTP Put (Experimental): {step_name}"]

    host = _subst(str(metadata.get("host") or ""), params)
    port = _subst(str(metadata.get("port") or "22"), params) or "22"
    username = _subst(str(metadata.get("username") or ""), params)
    use_key = bool(metadata.get("use_private_key"))
    key_file = _subst(str(metadata.get("key_file") or metadata.get("private_key_ref") or ""), params)
    local_field = (metadata.get("local_filename_field") or "").strip()
    local_file = _subst(str(metadata.get("local_filename") or ""), params)
    local_dir = _subst(str(metadata.get("local_directory") or ""), params)
    remote_dir = _subst(str(metadata.get("remote_directory") or ""), params)
    remote_dir_field = (metadata.get("remote_directory_field") or "").strip()
    remote_name = _subst(str(metadata.get("remote_filename") or ""), params)
    remote_name_field = (metadata.get("remote_filename_field") or "").strip()
    create_remote = bool(metadata.get("create_remote_directory"))
    overwrite = metadata.get("overwrite", True)
    append = bool(metadata.get("append"))
    transfer_mode = (metadata.get("transfer_mode") or "binary").strip().lower()
    timeout_raw = str(metadata.get("timeout") or "0").strip() or "0"
    compression = (metadata.get("compression") or "none").strip().lower()
    proxy_type = (metadata.get("proxy_type") or "").strip()
    proxy_host = _subst(str(metadata.get("proxy_host") or ""), params)
    proxy_port = _subst(str(metadata.get("proxy_port") or ""), params)
    proxy_user = _subst(str(metadata.get("proxy_username") or ""), params)
    input_is_stream = bool(metadata.get("input_is_stream"))
    after_action = (metadata.get("after_sftp_put") or "nothing").strip().lower()
    dest_folder = _subst(str(metadata.get("destination_folder") or ""), params)
    dest_folder_field = (metadata.get("destination_folder_field") or "").strip()
    create_dest = bool(metadata.get("create_destination_folder"))
    wildcard = _subst(str(metadata.get("wildcard") or ""), params)
    success_when_no_file = bool(metadata.get("success_when_no_file"))
    add_result = bool(metadata.get("add_filename_to_result"))
    error_target = (metadata.get("error_target_step") or "").strip()
    log_level = (metadata.get("logging_level") or "").strip()
    use_old_algos = bool(metadata.get("use_old_ssh_algorithms"))

    password_ref = metadata.get("password_secret_ref") or (
        "dbutils.secrets.get(scope='sftp', key='password')"
        if metadata.get("password_configured")
        else ""
    )
    passphrase_ref = metadata.get("passphrase_secret_ref") or (
        "dbutils.secrets.get(scope='sftp', key='passphrase')"
        if metadata.get("passphrase_configured")
        else ""
    )
    proxy_password_ref = metadata.get("proxy_password_secret_ref") or (
        "dbutils.secrets.get(scope='sftp', key='proxy_password')"
        if metadata.get("proxy_password_configured")
        else ""
    )

    lines.extend(_preserve_safe(metadata, _PRESERVE_KEYS))
    lines.append(
        "# SECURITY: Never embed passwords/private keys. Use Databricks Secrets "
        "(scope='sftp') or cluster env overrides (SFTP_HOST / SFTP_USER)."
    )
    lines.append("# REQUIRED: pip install paramiko (driver / job cluster library)")
    lines.append(
        "# NOTE: SFTP Put runs on the Databricks driver (not Spark executors). "
        "Ensure local paths are reachable from the driver (DBFS/Volumes/local disk)."
    )

    if transfer_mode == "ascii":
        lines.append(
            "# LIMITATION: SFTP has no ASCII/binary mode (unlike FTP); "
            "ASCII transfer mode is ignored - files are uploaded as binary streams"
        )
    if append and overwrite:
        lines.append(
            "# WARNING: both append and overwrite set - Paramiko will use append "
            "when append=True (ChannelSftp.APPEND)"
        )
    if append:
        lines.append(
            "# NOTE: append mode uses paramiko.SFTPClient.put(..., confirm) with "
            "open flags for append when supported"
        )
    if use_old_algos:
        lines.append(
            "# WARNING: use-old-SSH-algorithms has no direct Paramiko flag; "
            "configure disabled algorithms on Transport if needed"
        )
    if proxy_type or proxy_host:
        lines.append(
            "# WARNING: SOCKS5/HTTP proxy support is best-effort via Paramiko "
            "ProxyCommand / socks - review network egress on Databricks"
        )
    if input_is_stream:
        lines.append(
            "# LIMITATION: inputIsStream writes each row payload to a temp file "
            "then uploads; large streams may need chunked/external staging"
        )
    if after_action in ("delete", "move") or after_action.startswith("move"):
        lines.append(
            f"# WARNING: aftersftpput={after_action!r} mutates local files on the "
            "driver filesystem after upload - verify paths/permissions"
        )
    if error_target:
        lines.append(
            f"# preserved.error_handling.target_step={error_target!r} "
            "(route failed uploads manually; Spark has no row-error hops)"
        )
    if log_level:
        lines.append(f"# preserved.logging_level={log_level!r}")

    # Credentials / connection warnings
    if not host:
        lines.append(
            "# WARNING: missing SFTP host - set preserved.host or env SFTP_HOST"
        )
    if not username:
        lines.append(
            "# WARNING: missing SFTP username - set preserved.username or env SFTP_USER"
        )
    if use_key and not key_file:
        lines.append("# WARNING: private-key auth enabled but key file/path missing")
    if use_key:
        lines.append(
            "# SECURITY: Prefer mounting the key path or load key material via "
            "dbutils.secrets.get(scope='sftp', key='private_key') - never embed PEM text"
        )
    if not use_key and not password_ref:
        lines.append(
            "# WARNING: missing credentials - configure password via "
            "dbutils.secrets.get(scope='sftp', key='password') or private key"
        )
    if not local_field and not local_file and not local_dir and not in_df:
        lines.append("# WARNING: no local filename / directory / field configured")

    if not in_df and not local_file and not local_dir:
        lines.append("# WARNING: SFTP Put has no input DataFrame and no static local path")
        lines.extend(_empty_df(out_var))
        return lines, "partial"

    # Resolve put mode
    if append:
        put_mode = "a"
    elif overwrite is False:
        put_mode = "x"  # fail if exists
    else:
        put_mode = "w"

    try:
        timeout_sec = float(timeout_raw)
    except ValueError:
        timeout_sec = 0.0
    if timeout_sec <= 0:
        timeout_sec = 30.0
        lines.append(
            f"# NOTE: timeout {timeout_raw!r} treated as default 30s "
            "(set an explicit positive timeout in seconds)"
        )

    lines.append("import logging")
    lines.append("import os")
    lines.append("import re")
    lines.append("import shutil")
    lines.append("import tempfile")
    lines.append("from pathlib import Path")
    lines.append("import paramiko")
    lines.append(f"_sftp_log_{fn} = logging.getLogger('pentaho.sftp_put.{fn}')")
    if log_level:
        lines.append(
            f"_sftp_log_{fn}.setLevel(getattr(logging, {log_level.upper()!r}, logging.INFO))"
        )

    # Runtime ${VAR} / $VAR resolution (Pentaho environmentSubstitute equivalent).
    lines.append(f"def _sftp_env_subst_{fn}(text):")
    lines.append("    if text is None:")
    lines.append("        return ''")
    lines.append("    value = str(text)")
    lines.append("    def _repl(match):")
    lines.append("        key = match.group(1).strip()")
    lines.append("        return os.environ.get(key, match.group(0))")
    lines.append("    value = re.sub(r'\\$\\{([^}]+)\\}', _repl, value)")
    lines.append("    value = re.sub(r'\\$([A-Za-z_][A-Za-z0-9_]*)', _repl, value)")
    lines.append("    return value")

    lines.append(f"def _sftp_put_run_{fn}(file_jobs):")
    lines.append('    """Upload local files via Paramiko SFTP (driver-side)."""')
    lines.append(
        f"    host = (os.environ.get('SFTP_HOST') or "
        f"_sftp_env_subst_{fn}({host!r}) or '').strip()"
    )
    lines.append(
        f"    port_raw = (os.environ.get('SFTP_PORT') or "
        f"_sftp_env_subst_{fn}({port!r}) or '22').strip()"
    )
    lines.append("    try:")
    lines.append("        port = int(port_raw)")
    lines.append("    except ValueError:")
    lines.append("        port = 22")
    lines.append(
        f"    username = (os.environ.get('SFTP_USER') or "
        f"_sftp_env_subst_{fn}({username!r}) or '').strip()"
    )
    lines.append(f"    use_key = {use_key!r}")
    lines.append(f"    key_file = _sftp_env_subst_{fn}({key_file!r})")
    lines.append(f"    create_remote = {create_remote!r}")
    lines.append(f"    put_mode = {put_mode!r}")
    lines.append(f"    timeout_sec = {timeout_sec!r}")
    lines.append(f"    compression = {compression!r}")
    lines.append(f"    after_action = {after_action!r}")
    lines.append(f"    dest_folder = _sftp_env_subst_{fn}({dest_folder!r})")
    lines.append(f"    create_dest = {create_dest!r}")
    lines.append(f"    success_when_no_file = {success_when_no_file!r}")
    lines.append(f"    default_remote_dir = _sftp_env_subst_{fn}({remote_dir!r})")
    lines.append(f"    default_remote_name = _sftp_env_subst_{fn}({remote_name!r})")
    lines.append(f"    proxy_type = {proxy_type!r}")
    lines.append(f"    proxy_host = _sftp_env_subst_{fn}({proxy_host!r})")
    lines.append(f"    proxy_port = _sftp_env_subst_{fn}({proxy_port!r})")
    lines.append(f"    proxy_user = _sftp_env_subst_{fn}({proxy_user!r})")

    lines.append("    if not host:")
    lines.append(
        "        raise ValueError('SFTP Put: missing host "
        "(set SFTP_HOST or step servername)')"
    )
    lines.append("    if not username:")
    lines.append(
        "        raise ValueError('SFTP Put: missing username "
        "(set SFTP_USER or step username)')"
    )
    lines.append("    if not file_jobs:")
    lines.append("        if success_when_no_file:")
    lines.append(f"            _sftp_log_{fn}.info('SFTP Put: no files - successWhenNoFile')")
    lines.append("            return []")
    lines.append(
        "        raise FileNotFoundError("
        "'SFTP Put: no local files to upload (missing local files)')"
    )

    # Secrets
    lines.append("    password = None")
    lines.append("    passphrase = None")
    lines.append("    proxy_password = None")
    if password_ref:
        lines.append("    try:")
        lines.append(f"        password = {password_ref}  # noqa: F821")
        lines.append("    except Exception as _sec_exc:")
        lines.append(
            f"        _sftp_log_{fn}.warning('SFTP password secret unavailable: %s', _sec_exc)"
        )
    if passphrase_ref:
        lines.append("    try:")
        lines.append(f"        passphrase = {passphrase_ref}  # noqa: F821")
        lines.append("    except Exception as _sec_exc:")
        lines.append(
            f"        _sftp_log_{fn}.warning('SFTP passphrase secret unavailable: %s', _sec_exc)"
        )
    if proxy_password_ref:
        lines.append("    try:")
        lines.append(f"        proxy_password = {proxy_password_ref}  # noqa: F821")
        lines.append("    except Exception as _sec_exc:")
        lines.append(
            f"        _sftp_log_{fn}.warning('SFTP proxy password secret unavailable: %s', _sec_exc)"
        )
    lines.append("    if use_key and not key_file:")
    lines.append(
        "        raise ValueError("
        "'SFTP Put: private key path missing - mount key or set keyfilename')"
    )
    lines.append("    if (not use_key) and not password:")
    lines.append(
        "        raise ValueError("
        "'SFTP Put: authentication failure risk - password secret missing "
        "and private key not configured')"
    )

    lines.append("    sock = None")
    lines.append("    if proxy_host:")
    lines.append(
        f"        _sftp_log_{fn}.warning("
        "'SFTP proxy %s://%s:%s configured - ensure Paramiko/SOCKS egress works', "
        "proxy_type or 'http', proxy_host, proxy_port)"
    )
    lines.append("        # Proxy password available as proxy_password (from Secrets)")
    lines.append("        _ = (proxy_user, proxy_password, sock)")

    lines.append("    transport = None")
    lines.append("    sftp = None")
    lines.append("    uploaded = []")
    lines.append("    try:")
    lines.append("        transport = paramiko.Transport((host, port))")
    lines.append("        transport.banner_timeout = timeout_sec")
    lines.append("        transport.auth_timeout = timeout_sec")
    lines.append("        if compression and compression not in ('none', 'false', '0', ''):")
    lines.append("            try:")
    lines.append("                transport.use_compression(True)")
    lines.append("            except Exception as _cmp_exc:")
    lines.append(
        f"                _sftp_log_{fn}.warning('SFTP compression ignored: %s', _cmp_exc)"
    )
    lines.append("        pkey = None")
    lines.append("        if use_key:")
    lines.append("            key_path = Path(key_file)")
    lines.append("            if not key_path.is_file():")
    lines.append(
        "                raise FileNotFoundError("
        "f'SFTP Put: private key not found: {key_file}')"
    )
    lines.append("            try:")
    lines.append(
        "                pkey = paramiko.RSAKey.from_private_key_file("
        "str(key_path), password=passphrase)"
    )
    lines.append("            except Exception:")
    lines.append("                try:")
    lines.append(
        "                    pkey = paramiko.Ed25519Key.from_private_key_file("
        "str(key_path), password=passphrase)"
    )
    lines.append("                except Exception:")
    lines.append(
        "                    pkey = paramiko.ECDSAKey.from_private_key_file("
        "str(key_path), password=passphrase)"
    )
    lines.append("            transport.connect(username=username, pkey=pkey)")
    lines.append("        else:")
    lines.append("            transport.connect(username=username, password=password)")
    lines.append("        sftp = paramiko.SFTPClient.from_transport(transport)")
    lines.append("        if sftp is None:")
    lines.append("            raise ConnectionError('SFTP Put: failed to open SFTP channel')")
    lines.append("        try:")
    lines.append("            sftp.get_channel().settimeout(timeout_sec)")
    lines.append("        except Exception:")
    lines.append("            pass")

    lines.append("        def _ensure_remote_dir(path):")
    lines.append("            if not path or path in ('.', '/'):")
    lines.append("                return")
    lines.append("            parts = [p for p in path.replace('\\\\', '/').split('/') if p]")
    lines.append("            cur = '' if not path.startswith('/') else '/'")
    lines.append("            for part in parts:")
    lines.append("                cur = f'{cur}/{part}' if cur not in ('', '/') else "
                 "f'{cur}{part}' if cur == '/' else part")
    lines.append("                try:")
    lines.append("                    sftp.stat(cur)")
    lines.append("                except (IOError, OSError, FileNotFoundError):")
    lines.append("                    if not create_remote:")
    lines.append(
        "                        raise FileNotFoundError("
        "f'SFTP Put: missing remote directory: {path}')"
    )
    lines.append("                    try:")
    lines.append("                        sftp.mkdir(cur)")
    lines.append(
        f"                        _sftp_log_{fn}.info("
        "'Created remote directory %s', cur)"
    )
    lines.append("                    except Exception as mkdir_exc:")
    lines.append(
        "                        raise PermissionError("
        "f'SFTP Put: permission denied creating {cur}: {mkdir_exc}') "
        "from mkdir_exc"
    )

    lines.append("        for job in file_jobs:")
    lines.append("            local_path = str(job.get('local') or '')")
    lines.append("            rdir = str(job.get('remote_dir') or default_remote_dir or '')")
    lines.append("            rname = str(job.get('remote_name') or default_remote_name or '')")
    lines.append("            dest_move = str(job.get('dest_folder') or dest_folder or '')")
    lines.append("            if not local_path:")
    lines.append(
        f"                _sftp_log_{fn}.warning('SFTP Put: empty local path skipped')"
    )
    lines.append("                continue")
    lines.append("            lp = Path(local_path)")
    lines.append("            if not lp.is_file():")
    lines.append(
        "                raise FileNotFoundError("
        "f'SFTP Put: missing local file: {local_path}')"
    )
    lines.append("            if lp.stat().st_size == 0:")
    lines.append(
        f"                _sftp_log_{fn}.warning("
        "'SFTP Put: empty file %s - uploading anyway', local_path)"
    )
    lines.append("            if not rname:")
    lines.append("                rname = lp.name")
    lines.append("            _ensure_remote_dir(rdir)")
    lines.append(
        "            remote_path = "
        "(rdir.rstrip('/') + '/' + rname) if rdir else rname"
    )
    lines.append("            if put_mode == 'x':")
    lines.append("                try:")
    lines.append("                    sftp.stat(remote_path)")
    lines.append(
        "                    raise FileExistsError("
        "f'SFTP Put: remote exists and overwrite disabled: {remote_path}')"
    )
    lines.append("                except FileExistsError:")
    lines.append("                    raise")
    lines.append("                except Exception:")
    lines.append("                    pass")
    lines.append("            try:")
    lines.append("                if put_mode == 'a':")
    lines.append(
        "                    with lp.open('rb') as _lf, "
        "sftp.open(remote_path, 'a') as _rf:"
    )
    lines.append("                        shutil.copyfileobj(_lf, _rf)")
    lines.append("                else:")
    lines.append("                    sftp.put(str(lp), remote_path)")
    lines.append(
        f"                _sftp_log_{fn}.info("
        "'Uploaded %s -> %s', local_path, remote_path)"
    )
    lines.append("                uploaded.append("
                 "{'local': local_path, 'remote': remote_path})")
    lines.append("            except PermissionError:")
    lines.append("                raise")
    lines.append("            except Exception as put_exc:")
    lines.append("                err = str(put_exc).lower()")
    lines.append("                if 'permission' in err or 'denied' in err:")
    lines.append(
        "                    raise PermissionError("
        "f'SFTP Put: permission denied for {remote_path}: {put_exc}') "
        "from put_exc"
    )
    lines.append("                if 'timed out' in err or 'timeout' in err:")
    lines.append(
        "                    raise TimeoutError("
        "f'SFTP Put: connection/transfer timeout for {remote_path}: {put_exc}') "
        "from put_exc"
    )
    lines.append(
        "                raise ConnectionError("
        "f'SFTP Put: network/transfer failure for {remote_path}: {put_exc}') "
        "from put_exc"
    )

    # After actions
    lines.append("            if after_action in ('delete', 'removefile', 'remove'):")
    lines.append("                try:")
    lines.append("                    lp.unlink()")
    lines.append(
        f"                    _sftp_log_{fn}.info('Deleted local file %s', local_path)"
    )
    lines.append("                except Exception as del_exc:")
    lines.append(
        f"                    _sftp_log_{fn}.warning("
        "'Failed to delete local %s: %s', local_path, del_exc)"
    )
    lines.append(
        "            elif after_action in ('move', 'movefile') or "
        "str(after_action).startswith('move'):"
    )
    lines.append("                if not dest_move:")
    lines.append(
        "                    raise ValueError("
        "'SFTP Put: aftersftpput=move requires destination folder')"
    )
    lines.append("                dpath = Path(dest_move)")
    lines.append("                if create_dest:")
    lines.append("                    dpath.mkdir(parents=True, exist_ok=True)")
    lines.append("                if not dpath.is_dir():")
    lines.append(
        "                    raise FileNotFoundError("
        "f'SFTP Put: destination folder missing: {dest_move}')"
    )
    lines.append("                target = dpath / lp.name")
    lines.append("                shutil.move(str(lp), str(target))")
    lines.append(
        f"                _sftp_log_{fn}.info("
        "'Moved local %s -> %s', local_path, target)"
    )

    lines.append("        return uploaded")
    lines.append("    except paramiko.AuthenticationException as auth_exc:")
    lines.append(
        "        raise PermissionError("
        "f'SFTP Put: authentication failure: {auth_exc}') from auth_exc"
    )
    lines.append("    except (TimeoutError, paramiko.SSHException) as net_exc:")
    lines.append(
        "        raise TimeoutError("
        "f'SFTP Put: connection timeout / SSH error: {net_exc}') from net_exc"
    )
    lines.append("    finally:")
    lines.append("        try:")
    lines.append("            if sftp is not None:")
    lines.append("                sftp.close()")
    lines.append("        except Exception:")
    lines.append("            pass")
    lines.append("        try:")
    lines.append("            if transport is not None:")
    lines.append("                transport.close()")
    lines.append("        except Exception:")
    lines.append("            pass")

    # Build file job list from DF / static paths
    lines.append(f"_sftp_jobs_{fn} = []")
    if local_file:
        lines.append(
            f"_sftp_jobs_{fn}.append({{'local': {local_file!r}, "
            f"'remote_dir': {remote_dir!r}, 'remote_name': {remote_name!r}, "
            f"'dest_folder': {dest_folder!r}}})"
        )
    if local_dir:
        lines.append(f"_sftp_local_dir_{fn} = Path({local_dir!r})")
        lines.append(f"_sftp_wildcard_{fn} = {wildcard!r} or '*'")
        lines.append(f"if _sftp_local_dir_{fn}.is_dir():")
        lines.append(f"    import fnmatch")
        lines.append(f"    for _p in sorted(_sftp_local_dir_{fn}.iterdir()):")
        lines.append(
            f"        if _p.is_file() and fnmatch.fnmatch(_p.name, _sftp_wildcard_{fn}):"
        )
        lines.append(
            f"            _sftp_jobs_{fn}.append({{'local': str(_p), "
            f"'remote_dir': {remote_dir!r}, 'remote_name': '', "
            f"'dest_folder': {dest_folder!r}}})"
        )
        lines.append("else:")
        lines.append(
            f"    _sftp_log_{fn}.warning("
            f"'SFTP Put: local directory missing: %s', {local_dir!r})"
        )

    if in_df and (local_field or remote_dir_field or remote_name_field or input_is_stream):
        lines.append(f"_sftp_rows_{fn} = {in_df}.collect()")
        lines.append(f"for _row in _sftp_rows_{fn}:")
        if input_is_stream and local_field:
            lines.append(f"    _payload = _row[{local_field!r}] if {local_field!r} in _row.asDict() else None")
            lines.append("    _tmp = tempfile.NamedTemporaryFile(delete=False)")
            lines.append("    try:")
            lines.append("        if isinstance(_payload, (bytes, bytearray)):")
            lines.append("            _tmp.write(_payload)")
            lines.append("        elif _payload is None:")
            lines.append(
                f"            _sftp_log_{fn}.warning('SFTP Put: null stream payload skipped')"
            )
            lines.append("            _tmp.close()")
            lines.append("            continue")
            lines.append("        else:")
            lines.append("            _tmp.write(str(_payload).encode('utf-8'))")
            lines.append("        _tmp.close()")
            lines.append(f"        _local = _tmp.name")
            lines.append("    except Exception:")
            lines.append("        _tmp.close()")
            lines.append("        raise")
        elif local_field:
            lines.append(
                f"    _local = _row[{local_field!r}] if {local_field!r} in _row.asDict() else None"
            )
            lines.append("    if _local is None or str(_local).strip() == '':")
            lines.append(
                f"        _sftp_log_{fn}.warning('SFTP Put: blank local filename field skipped')"
            )
            lines.append("        continue")
            lines.append("    _local = str(_local)")
        else:
            lines.append(f"    _local = {local_file!r}")
            lines.append("    if not _local:")
            lines.append("        continue")

        if remote_dir_field:
            lines.append(
                f"    _rdir = _row[{remote_dir_field!r}] if {remote_dir_field!r} in _row.asDict() "
                f"else {remote_dir!r}"
            )
            lines.append(f"    _rdir = str(_rdir or {remote_dir!r} or '')")
        else:
            lines.append(f"    _rdir = {remote_dir!r}")

        if remote_name_field:
            lines.append(
                f"    _rname = _row[{remote_name_field!r}] if {remote_name_field!r} in _row.asDict() "
                f"else {remote_name!r}"
            )
            lines.append(f"    _rname = str(_rname or {remote_name!r} or '')")
        else:
            lines.append(f"    _rname = {remote_name!r}")

        if dest_folder_field:
            lines.append(
                f"    _dfolder = _row[{dest_folder_field!r}] if {dest_folder_field!r} in _row.asDict() "
                f"else {dest_folder!r}"
            )
            lines.append(f"    _dfolder = str(_dfolder or {dest_folder!r} or '')")
        else:
            lines.append(f"    _dfolder = {dest_folder!r}")

        lines.append(
            f"    _sftp_jobs_{fn}.append({{'local': _local, 'remote_dir': _rdir, "
            "'remote_name': _rname, 'dest_folder': _dfolder})"
        )

    lines.append(f"_sftp_uploaded_{fn} = _sftp_put_run_{fn}(_sftp_jobs_{fn})")
    lines.append(
        f"_sftp_log_{fn}.info('SFTP Put complete: %s file(s)', len(_sftp_uploaded_{fn}))"
    )

    if add_result:
        lines.append(
            f"# addFilenameResut: uploaded paths available in _sftp_uploaded_{fn}"
        )

    if in_df:
        lines.append(f"{out_var} = {in_df}")
        if add_result:
            lines.append(
                f"# Optional: join upload results - len(_sftp_uploaded_{fn}) files transferred"
            )
    else:
        lines.append("from pyspark.sql import Row")
        lines.append(
            f"{out_var} = spark.createDataFrame("
            f"[Row(**r) for r in _sftp_uploaded_{fn}]) if _sftp_uploaded_{fn} else "
            "spark.createDataFrame([], 'local STRING, remote STRING')"
        )

    logger.info(
        "SFTPPut '%s': host=%s user=%s local_field=%s remote_dir=%s",
        step_name, host or "<env>", username or "<env>", local_field, remote_dir,
    )
    return lines, "partial"
