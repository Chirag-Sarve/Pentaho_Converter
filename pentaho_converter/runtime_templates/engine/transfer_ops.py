"""File-Transfer job entry helpers (FTP / FTPS / SFTP get, put, delete).

Uses stdlib ``ftplib`` (and ``FTP_TLS``) plus optional ``paramiko`` for SFTP.
Designed for Databricks driver-side execution — no OS FTP clients.
"""

from __future__ import annotations

import fnmatch
import logging
import re
import socket
from dataclasses import dataclass, field
from ftplib import FTP, FTP_TLS, error_perm, error_temp
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)


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
class TransferOutcome:
    success: bool
    message: str = ""
    paths: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _port(raw: Any, default: int) -> int:
    try:
        return int(float(raw or default))
    except (TypeError, ValueError):
        return default


def _timeout(raw: Any, default: float = 30.0) -> float:
    try:
        val = float(raw or default)
        # PDI often stores milliseconds
        if val > 1000:
            return val / 1000.0
        return max(val, 1.0)
    except (TypeError, ValueError):
        return default


def _match_wildcard(name: str, pattern: str) -> bool:
    text = (pattern or "").strip()
    if not text or text in {"*", ".*"}:
        return True
    # Prefer regex (PDI often uses Java regex); fall back to fnmatch
    try:
        return bool(re.search(text, name))
    except re.error:
        return fnmatch.fnmatch(name, text)


def _proxy_warnings(
    *,
    proxy_host: str = "",
    socks_host: str = "",
    useproxy: bool = False,
) -> list[str]:
    warnings: list[str] = []
    if useproxy or proxy_host:
        warnings.append(
            f"FTP/HTTP proxy {proxy_host!r} is not applied — "
            "connect directly (configure network egress on Databricks)"
        )
    if socks_host:
        warnings.append(
            f"SOCKS proxy {socks_host!r} is not applied — connect directly"
        )
    return warnings


def _success_from_counts(
    transferred: int,
    *,
    nr_limit: str = "10",
    success_condition: str = "",
) -> bool:
    cond = (success_condition or "").strip().lower()
    try:
        limit = int(float(nr_limit or "10"))
    except ValueError:
        limit = 10
    if not cond or cond in {
        "success_if_no_errors",
        "success_when_no_errors",
        "no_errors",
    }:
        return True
    if cond in {
        "success_if_at_least_x_files_downloaded",
        "success_when_at_least",
        "at_least",
    }:
        return transferred >= limit
    if cond in {
        "success_if_errors_less",
        "success_when_errors_less",
    }:
        # Caller tracks errors separately; treat as ok when invoked after clean run
        return True
    return transferred > 0 or cond == ""


# ---------------------------------------------------------------------------
# FTP / FTPS connections
# ---------------------------------------------------------------------------


def _connect_ftp(
    host: str,
    port: int,
    username: str,
    password: str,
    *,
    timeout: float,
    active: bool,
    binary: bool = True,
    encoding: str = "UTF-8",
    tls: bool = False,
    connection_type: str = "",
) -> tuple[FTP, list[str]]:
    warnings: list[str] = []
    if not host:
        raise ValueError("FTP servername/host is empty")

    ctype = (connection_type or "").strip().lower()
    if tls and ctype in {"implicit", "1", "ftps_implicit"}:
        warnings.append(
            "FTPS implicit TLS is approximated via FTP_TLS — "
            "prefer explicit FTPS (AUTH TLS) on Databricks"
        )

    if tls:
        ftp: FTP = FTP_TLS()
    else:
        ftp = FTP()

    ftp.connect(host, port, timeout=timeout)
    ftp.login(username or "anonymous", password or "anonymous@")
    if tls and isinstance(ftp, FTP_TLS):
        try:
            ftp.prot_p()
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"FTPS PROT P failed: {exc}")
    if encoding:
        try:
            ftp.encoding = encoding
        except Exception:  # noqa: BLE001
            pass
    if active:
        ftp.set_pasv(False)
    else:
        ftp.set_pasv(True)
    # binary flag applied per transfer
    _ = binary
    return ftp, warnings


def _ftp_list_names(ftp: FTP, directory: str, wildcard: str) -> list[str]:
    if directory:
        ftp.cwd(directory)
    names: list[str] = []
    try:
        names = ftp.nlst()
    except error_perm:
        # Some servers reject NLST on empty dirs
        names = []
    # Filter out . and ..
    out = []
    for name in names:
        base = name.split("/")[-1]
        if base in {".", ".."}:
            continue
        if _match_wildcard(base, wildcard):
            out.append(base)
    return out


def _ftp_is_dir(ftp: FTP, name: str) -> bool:
    cur = ftp.pwd()
    try:
        ftp.cwd(name)
        ftp.cwd(cur)
        return True
    except error_perm:
        return False
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# SFTP (paramiko)
# ---------------------------------------------------------------------------


def _import_paramiko() -> Any:
    try:
        import paramiko  # type: ignore

        return paramiko
    except ImportError as exc:
        raise ImportError(
            "paramiko is required for SFTP job entries on Databricks — "
            "install cluster library 'paramiko'"
        ) from exc


def _connect_sftp(
    host: str,
    port: int,
    username: str,
    password: str,
    *,
    timeout: float,
    use_key: bool = False,
    key_filename: str = "",
    key_passphrase: str = "",
    compression: bool = False,
) -> tuple[Any, Any, list[str]]:
    """Return (transport, sftp, warnings). Caller must close transport."""
    warnings: list[str] = []
    if not host:
        raise ValueError("SFTP servername/host is empty")
    paramiko = _import_paramiko()
    transport = paramiko.Transport((host, port))
    if compression:
        try:
            transport.use_compression(True)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"SFTP compression ignored: {exc}")
    transport.connect()
    if use_key and key_filename:
        key_path = Path(key_filename)
        if not key_path.exists():
            transport.close()
            raise FileNotFoundError(f"SFTP private key not found: {key_filename}")
        pkey = None
        last_exc: BaseException | None = None
        for loader in (
            paramiko.RSAKey,
            getattr(paramiko, "Ed25519Key", None),
            getattr(paramiko, "ECDSAKey", None),
            getattr(paramiko, "DSSKey", None),
        ):
            if loader is None:
                continue
            try:
                pkey = loader.from_private_key_file(
                    str(key_path), password=key_passphrase or None
                )
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
        if pkey is None:
            transport.close()
            raise RuntimeError(f"Unable to load SFTP private key: {last_exc}")
        transport.auth_publickey(username, pkey)
    else:
        if not password and not use_key:
            warnings.append("SFTP password empty — authentication may fail")
        transport.auth_password(username, password or "")
    sftp = paramiko.SFTPClient.from_transport(transport)
    if sftp is None:
        transport.close()
        raise ConnectionError("Failed to open SFTP channel")
    # Apply socket timeout on underlying channel when possible
    try:
        chan = transport.getpeername()
        _ = chan
        sftp.get_channel().settimeout(timeout)
    except Exception:  # noqa: BLE001
        pass
    return transport, sftp, warnings


def _sftp_mkdir_p(sftp: Any, remote_dir: str) -> None:
    if not remote_dir or remote_dir in {".", "/"}:
        return
    parts = [p for p in remote_dir.replace("\\", "/").split("/") if p]
    cur = "" if not remote_dir.startswith("/") else "/"
    for part in parts:
        cur = f"{cur}/{part}" if cur not in {"", "/"} else f"{cur}{part}" if cur == "/" else part
        if remote_dir.startswith("/") and not cur.startswith("/"):
            cur = "/" + cur
        try:
            sftp.stat(cur)
        except IOError:
            sftp.mkdir(cur)


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def ftp_get(
    *,
    servername: str,
    port: str | int = "21",
    username: str = "",
    password: str = "",
    ftpdirectory: str = "",
    targetdirectory: str = "",
    wildcard: str = "",
    binary: bool = True,
    timeout: str | float = "30",
    remove: bool = False,
    only_new: bool = False,
    active: bool = False,
    control_encoding: str = "UTF-8",
    isaddresult: bool = True,
    if_file_exists: str = "0",
    nr_limit: str = "10",
    success_condition: str = "",
    proxy_host: str = "",
    socksproxy_host: str = "",
    useproxy: bool = False,
    tls: bool = False,
    connection_type: str = "",
    movefiles: bool = False,
    movetodirectory: str = "",
) -> TransferOutcome:
    warnings = _proxy_warnings(
        proxy_host=proxy_host, socks_host=socksproxy_host, useproxy=useproxy
    )
    if movefiles:
        warnings.append(
            f"movefiles=Y / movetodirectory={movetodirectory!r} — "
            "remote move after get is best-effort (RNFR/RNTO)"
        )
    local_root = Path(targetdirectory or ".")
    local_root.mkdir(parents=True, exist_ok=True)

    ftp = None
    downloaded: list[str] = []
    try:
        ftp, w = _connect_ftp(
            servername,
            _port(port, 990 if tls and str(connection_type).lower() in {"implicit", "1"} else 21),
            username,
            password,
            timeout=_timeout(timeout),
            active=active,
            binary=binary,
            encoding=control_encoding or "UTF-8",
            tls=tls,
            connection_type=connection_type,
        )
        warnings.extend(w)
        names = _ftp_list_names(ftp, ftpdirectory, wildcard)
        for name in names:
            if _ftp_is_dir(ftp, name):
                continue
            dest = local_root / name
            if dest.exists() and only_new:
                mode = str(if_file_exists).strip()
                if mode in {"1", "fail"}:
                    raise FileExistsError(f"Local file exists: {dest}")
                if mode in {"2", "do_nothing", "skip"}:
                    warnings.append(f"Skipped existing local file: {dest}")
                    continue
                # unique
                if mode in {"3", "unique"}:
                    n = 1
                    while dest.exists():
                        dest = local_root / f"{Path(name).stem}_{n}{Path(name).suffix}"
                        n += 1
            cmd = "RETR " + name
            if binary:
                with dest.open("wb") as fh:
                    ftp.retrbinary(cmd, fh.write)
            else:
                with dest.open("w", encoding="utf-8", newline="\n") as fh:
                    ftp.retrlines(cmd, lambda line: fh.write(line + "\n"))
            downloaded.append(str(dest))
            if remove:
                try:
                    ftp.delete(name)
                except error_perm as exc:
                    warnings.append(f"Could not delete remote {name}: {exc}")
            if movefiles and movetodirectory:
                try:
                    ftp.rename(name, f"{movetodirectory.rstrip('/')}/{name}")
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Remote move failed for {name}: {exc}")
    except (error_perm, error_temp, OSError, socket.error, ValueError) as exc:
        return TransferOutcome(
            False, str(exc), downloaded, warnings, error=exc, extra={"tls": tls}
        )
    finally:
        if ftp is not None:
            try:
                ftp.quit()
            except Exception:  # noqa: BLE001
                try:
                    ftp.close()
                except Exception:  # noqa: BLE001
                    pass

    ok = _success_from_counts(
        len(downloaded), nr_limit=nr_limit, success_condition=success_condition
    )
    if not downloaded and not ok:
        err = FileNotFoundError("No FTP files matched wildcard")
        return TransferOutcome(False, str(err), [], warnings, error=err)
    # PDI often succeeds with 0 files depending on condition; default success if no errors
    if not success_condition:
        ok = True
    return TransferOutcome(
        ok,
        f"{'FTPS' if tls else 'FTP'} get: {len(downloaded)} file(s)",
        downloaded if isaddresult else [],
        warnings,
        error=None if ok else RuntimeError("FTP get success condition failed"),
        extra={"count": len(downloaded), "tls": tls},
    )


def ftp_put(
    *,
    servername: str,
    port: str | int = "21",
    username: str = "",
    password: str = "",
    remote_directory: str = "",
    local_directory: str = "",
    wildcard: str = "*",
    binary: bool = True,
    timeout: str | float = "30",
    remove: bool = False,
    only_new: bool = False,
    active: bool = False,
    control_encoding: str = "UTF-8",
    proxy_host: str = "",
    socksproxy_host: str = "",
    tls: bool = False,
    connection_type: str = "",
) -> TransferOutcome:
    warnings = _proxy_warnings(proxy_host=proxy_host, socks_host=socksproxy_host)
    local_root = Path(local_directory or ".")
    if not local_root.exists():
        err = FileNotFoundError(f"Local directory not found: {local_directory}")
        return TransferOutcome(False, str(err), error=err, warnings=warnings)

    files = [
        p
        for p in sorted(local_root.iterdir())
        if p.is_file() and _match_wildcard(p.name, wildcard or "*")
    ]
    ftp = None
    uploaded: list[str] = []
    try:
        ftp, w = _connect_ftp(
            servername,
            _port(port, 21),
            username,
            password,
            timeout=_timeout(timeout),
            active=active,
            binary=binary,
            encoding=control_encoding or "UTF-8",
            tls=tls,
            connection_type=connection_type,
        )
        warnings.extend(w)
        if remote_directory:
            try:
                ftp.cwd(remote_directory)
            except error_perm:
                # try mkdir path segments
                cur = ""
                for part in remote_directory.replace("\\", "/").split("/"):
                    if not part:
                        continue
                    cur = f"{cur}/{part}" if cur else part
                    try:
                        ftp.mkd(part)
                    except error_perm:
                        pass
                    ftp.cwd(part)
        remote_names = set()
        try:
            remote_names = set(ftp.nlst())
        except error_perm:
            remote_names = set()
        for fp in files:
            if only_new and fp.name in remote_names:
                warnings.append(f"Remote exists, skipped (only_new): {fp.name}")
                continue
            cmd = "STOR " + fp.name
            if binary:
                with fp.open("rb") as fh:
                    ftp.storbinary(cmd, fh)
            else:
                with fp.open("r", encoding="utf-8") as fh:
                    ftp.storlines(cmd, fh)
            uploaded.append(str(fp))
            if remove:
                try:
                    fp.unlink()
                except OSError as exc:
                    warnings.append(f"Could not remove local {fp}: {exc}")
    except (error_perm, error_temp, OSError, socket.error, ValueError) as exc:
        return TransferOutcome(
            False, str(exc), uploaded, warnings, error=exc, extra={"tls": tls}
        )
    finally:
        if ftp is not None:
            try:
                ftp.quit()
            except Exception:  # noqa: BLE001
                try:
                    ftp.close()
                except Exception:  # noqa: BLE001
                    pass

    return TransferOutcome(
        True,
        f"{'FTPS' if tls else 'FTP'} put: {len(uploaded)} file(s)",
        uploaded,
        warnings,
        extra={"count": len(uploaded), "tls": tls},
    )


def ftp_delete(
    *,
    protocol: str = "FTP",
    servername: str = "",
    port: str | int = "",
    username: str = "",
    password: str = "",
    ftpdirectory: str = "",
    wildcard: str = "",
    timeout: str | float = "30",
    active: bool = False,
    useproxy: bool = False,
    proxy_host: str = "",
    socksproxy_host: str = "",
    publicpublickey: bool = False,
    keyfilename: str = "",
    keyfilepass: str = "",
    ftps_connection_type: str = "",
    copyprevious: bool = False,
    previous_names: Sequence[str] | None = None,
    nr_limit_success: str = "10",
    success_condition: str = "",
) -> TransferOutcome:
    warnings = _proxy_warnings(
        proxy_host=proxy_host, socks_host=socksproxy_host, useproxy=useproxy
    )
    proto = (protocol or "FTP").strip().upper()
    deleted: list[str] = []

    names_filter = list(previous_names or []) if copyprevious else None
    if copyprevious:
        warnings.append(
            "copyprevious=Y — deleting names from previous result (approximated)"
        )

    try:
        if proto in {"SFTP", "SSH"}:
            port_i = _port(port, 22)
            transport, sftp, w = _connect_sftp(
                servername,
                port_i,
                username,
                password,
                timeout=_timeout(timeout),
                use_key=publicpublickey,
                key_filename=keyfilename,
                key_passphrase=keyfilepass,
            )
            warnings.extend(w)
            try:
                if ftpdirectory:
                    sftp.chdir(ftpdirectory)
                listing = sftp.listdir()
                for name in listing:
                    if names_filter is not None and name not in names_filter:
                        continue
                    if not _match_wildcard(name, wildcard):
                        continue
                    try:
                        sftp.remove(name)
                        deleted.append(name)
                    except IOError as exc:
                        warnings.append(f"SFTP delete failed for {name}: {exc}")
            finally:
                sftp.close()
                transport.close()
        else:
            tls = proto in {"FTPS", "SSL"}
            port_i = _port(port, 21 if not tls else 990)
            ftp, w = _connect_ftp(
                servername,
                port_i,
                username,
                password,
                timeout=_timeout(timeout),
                active=active,
                tls=tls,
                connection_type=ftps_connection_type,
            )
            warnings.extend(w)
            try:
                names = _ftp_list_names(ftp, ftpdirectory, wildcard or ".*")
                for name in names:
                    if names_filter is not None and name not in names_filter:
                        continue
                    if _ftp_is_dir(ftp, name):
                        continue
                    try:
                        ftp.delete(name)
                        deleted.append(name)
                    except error_perm as exc:
                        warnings.append(f"FTP delete failed for {name}: {exc}")
            finally:
                try:
                    ftp.quit()
                except Exception:  # noqa: BLE001
                    ftp.close()
    except Exception as exc:  # noqa: BLE001
        return TransferOutcome(
            False, str(exc), deleted, warnings, error=exc, extra={"protocol": proto}
        )

    ok = _success_from_counts(
        len(deleted), nr_limit=nr_limit_success, success_condition=success_condition
    )
    if not success_condition:
        ok = True
    return TransferOutcome(
        ok,
        f"{proto} delete: {len(deleted)} file(s)",
        deleted,
        warnings,
        error=None if ok else RuntimeError("FTP delete success condition failed"),
        extra={"count": len(deleted), "protocol": proto},
    )


def sftp_get(
    *,
    servername: str,
    port: str | int = "22",
    username: str = "",
    password: str = "",
    sftpdirectory: str = "",
    targetdirectory: str = "",
    wildcard: str = "",
    remove: bool = False,
    isaddresult: bool = True,
    createtargetfolder: bool = True,
    copyprevious: bool = False,
    previous_names: Sequence[str] | None = None,
    usekeyfilename: bool = False,
    keyfilename: str = "",
    keyfilepass: str = "",
    compression: bool = False,
    timeout: str | float = "30",
    proxy_host: str = "",
    proxy_type: str = "",
) -> TransferOutcome:
    warnings: list[str] = []
    if proxy_host or proxy_type:
        warnings.append(
            f"SFTP proxyType={proxy_type!r} host={proxy_host!r} not applied — "
            "direct TCP connect"
        )
    if copyprevious:
        warnings.append("copyprevious=Y approximated via previous_names list")

    local_root = Path(targetdirectory or ".")
    if createtargetfolder:
        local_root.mkdir(parents=True, exist_ok=True)

    downloaded: list[str] = []
    transport = None
    sftp = None
    try:
        transport, sftp, w = _connect_sftp(
            servername,
            _port(port, 22),
            username,
            password,
            timeout=_timeout(timeout),
            use_key=usekeyfilename,
            key_filename=keyfilename,
            key_passphrase=keyfilepass,
            compression=compression,
        )
        warnings.extend(w)
        if sftpdirectory:
            sftp.chdir(sftpdirectory)
        listing = sftp.listdir()
        for name in listing:
            if previous_names is not None and name not in previous_names:
                continue
            if not _match_wildcard(name, wildcard):
                continue
            # Skip directories
            try:
                if hasattr(sftp.stat(name), "st_mode"):
                    import stat as statmod

                    if statmod.S_ISDIR(sftp.stat(name).st_mode):
                        continue
            except Exception:  # noqa: BLE001
                pass
            dest = local_root / name
            sftp.get(name, str(dest))
            downloaded.append(str(dest))
            if remove:
                try:
                    sftp.remove(name)
                except IOError as exc:
                    warnings.append(f"Could not remove remote {name}: {exc}")
    except Exception as exc:  # noqa: BLE001
        return TransferOutcome(False, str(exc), downloaded, warnings, error=exc)
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:  # noqa: BLE001
                pass
        if transport is not None:
            try:
                transport.close()
            except Exception:  # noqa: BLE001
                pass

    return TransferOutcome(
        True,
        f"SFTP get: {len(downloaded)} file(s)",
        downloaded if isaddresult else [],
        warnings,
        extra={"count": len(downloaded)},
    )


def sftp_put(
    *,
    servername: str,
    port: str | int = "22",
    username: str = "",
    password: str = "",
    sftpdirectory: str = "",
    localdirectory: str = "",
    wildcard: str = "*",
    copyprevious: bool = False,
    copypreviousfiles: bool = False,
    previous_paths: Sequence[str] | None = None,
    add_filename_result: bool = False,
    usekeyfilename: bool = False,
    keyfilename: str = "",
    keyfilepass: str = "",
    compression: bool = False,
    create_remote_folder: bool = False,
    aftersftpput: str = "nothing",
    destinationfolder: str = "",
    success_when_no_file: bool = False,
    timeout: str | float = "30",
    proxy_host: str = "",
    proxy_type: str = "",
) -> TransferOutcome:
    warnings: list[str] = []
    if proxy_host or proxy_type:
        warnings.append(
            f"SFTP proxyType={proxy_type!r} host={proxy_host!r} not applied"
        )

    files: list[Path] = []
    if copyprevious or copypreviousfiles:
        warnings.append("copyprevious(files)=Y — using previous_paths / result filenames")
        for raw in previous_paths or []:
            p = Path(raw)
            if p.is_file():
                files.append(p)
    else:
        local_root = Path(localdirectory or ".")
        if not local_root.exists():
            if success_when_no_file:
                warnings.append("Local directory missing — successWhenNoFile=Y")
                return TransferOutcome(True, "No files (successWhenNoFile)", warnings=warnings)
            err = FileNotFoundError(f"Local directory not found: {localdirectory}")
            return TransferOutcome(False, str(err), error=err)
        files = [
            p
            for p in sorted(local_root.iterdir())
            if p.is_file() and _match_wildcard(p.name, wildcard or "*")
        ]

    if not files:
        if success_when_no_file:
            return TransferOutcome(True, "No files to upload", warnings=warnings)
        err = FileNotFoundError("SFTP put: no local files matched")
        return TransferOutcome(False, str(err), error=err, warnings=warnings)

    uploaded: list[str] = []
    transport = None
    sftp = None
    try:
        transport, sftp, w = _connect_sftp(
            servername,
            _port(port, 22),
            username,
            password,
            timeout=_timeout(timeout),
            use_key=usekeyfilename,
            key_filename=keyfilename,
            key_passphrase=keyfilepass,
            compression=compression,
        )
        warnings.extend(w)
        remote_dir = sftpdirectory or "."
        if create_remote_folder and remote_dir not in {".", ""}:
            _sftp_mkdir_p(sftp, remote_dir)
        if remote_dir and remote_dir != ".":
            sftp.chdir(remote_dir)
        after = (aftersftpput or "nothing").strip().lower()
        for fp in files:
            remote_name = fp.name
            sftp.put(str(fp), remote_name)
            uploaded.append(str(fp))
            if after in {"delete", "1"}:
                try:
                    fp.unlink()
                except OSError as exc:
                    warnings.append(f"aftersftpput=delete failed for {fp}: {exc}")
            elif after in {"move", "2"}:
                if not destinationfolder:
                    warnings.append("aftersftpput=move requires destinationfolder")
                else:
                    dest_dir = Path(destinationfolder)
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    fp.rename(dest_dir / fp.name)
    except Exception as exc:  # noqa: BLE001
        return TransferOutcome(False, str(exc), uploaded, warnings, error=exc)
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:  # noqa: BLE001
                pass
        if transport is not None:
            try:
                transport.close()
            except Exception:  # noqa: BLE001
                pass

    return TransferOutcome(
        True,
        f"SFTP put: {len(uploaded)} file(s)",
        uploaded if add_filename_result else [],
        warnings,
        extra={"count": len(uploaded)},
    )


def ftps_get(**kwargs: Any) -> TransferOutcome:
    """Get files via FTPS (``FTP_TLS``)."""
    kwargs = dict(kwargs)
    kwargs["tls"] = True
    return ftp_get(**kwargs)


def ftps_put(**kwargs: Any) -> TransferOutcome:
    """Put files via FTPS (``FTP_TLS``)."""
    kwargs = dict(kwargs)
    kwargs["tls"] = True
    return ftp_put(**kwargs)

