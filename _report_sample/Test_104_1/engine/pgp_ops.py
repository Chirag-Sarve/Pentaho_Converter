"""File-Encryption job entry helpers (PGP encrypt / decrypt / verify).

Uses ``python-gnupg`` against a GnuPG binary (``gpglocation`` or PATH).
Sensitive material is never logged; prefer ``${VAR}`` / env / Databricks Secrets.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
from dataclasses import dataclass, field
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
class PgpOutcome:
    success: bool
    message: str = ""
    paths: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _import_gnupg() -> Any:
    try:
        import gnupg  # type: ignore

        return gnupg
    except ImportError as exc:
        raise ImportError(
            "python-gnupg is required for PGP job entries on Databricks — "
            "install cluster libraries 'python-gnupg' and a GnuPG binary"
        ) from exc


def _resolve_gpg_binary(gpglocation: str) -> str:
    raw = (gpglocation or "").strip()
    if raw and Path(raw).exists():
        return raw
    found = shutil.which(raw) if raw else None
    if found:
        return found
    for candidate in ("gpg", "gpg2", "gpg.exe"):
        found = shutil.which(candidate)
        if found:
            return found
    if raw:
        return raw  # let gnupg raise a clear error
    return "gpg"


def _make_gpg(
    gpglocation: str = "",
    *,
    gnupghome: str = "",
) -> tuple[Any, list[str]]:
    warnings: list[str] = []
    gnupg = _import_gnupg()
    binary = _resolve_gpg_binary(gpglocation)
    home = (gnupghome or os.environ.get("GNUPGHOME") or os.environ.get("GPG_HOME") or "").strip()
    kwargs: dict[str, Any] = {"gpgbinary": binary}
    if home:
        kwargs["gnupghome"] = home
    else:
        warnings.append(
            "No GNUPGHOME/GPG_HOME set — using default GnuPG home; "
            "import keys or mount a keyring for Databricks jobs"
        )
    gpg = gnupg.GPG(**kwargs)
    return gpg, warnings


def _match_wildcard(name: str, pattern: str) -> bool:
    text = (pattern or "").strip()
    if not text or text in {"*", ".*"}:
        return True
    try:
        return bool(re.search(text, name))
    except re.error:
        import fnmatch

        return fnmatch.fnmatch(name, text)


def _iter_source_files(
    source: str,
    wildcard: str,
    *,
    recursive: bool = False,
) -> list[Path]:
    root = Path(source)
    if not root.exists():
        return []
    if root.is_file():
        return [root] if _match_wildcard(root.name, wildcard) else []
    it = root.rglob("*") if recursive else root.iterdir()
    out: list[Path] = []
    for fp in it:
        try:
            if fp.is_file() and _match_wildcard(fp.name, wildcard):
                out.append(fp)
        except OSError:
            continue
    return out


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    n = 1
    while True:
        candidate = path.with_name(f"{stem}_{n}{suffix}")
        if not candidate.exists():
            return candidate
        n += 1


def _resolve_output(
    dest: Path,
    *,
    iffileexists: str = "0",
    create_parent: bool = True,
) -> tuple[Path | None, list[str]]:
    warnings: list[str] = []
    if create_parent:
        dest.parent.mkdir(parents=True, exist_ok=True)
    mode = str(iffileexists).strip().lower()
    if not dest.exists():
        return dest, warnings
    if mode in {"2", "fail"}:
        raise FileExistsError(f"Destination exists: {dest}")
    if mode in {"1", "do_nothing", "skip", "donothing"}:
        warnings.append(f"Skipped existing destination: {dest}")
        return None, warnings
    if mode in {"3", "unique", "create_new"}:
        return _unique_path(dest), warnings
    # overwrite / 0
    return dest, warnings


def _import_key_file(gpg: Any, key_file: str, *, private: bool = False) -> list[str]:
    warnings: list[str] = []
    path = Path(key_file)
    if not key_file:
        return warnings
    if not path.exists():
        warnings.append(f"Key file not found (skipped import): {key_file}")
        return warnings
    data = path.read_text(encoding="utf-8", errors="replace")
    result = gpg.import_keys(data)
    count = int(getattr(result, "count", 0) or 0)
    if count <= 0:
        warnings.append(
            f"{'Private' if private else 'Public'} key import reported 0 keys from {path.name}"
        )
    else:
        logger.info("Imported %s key(s) from %s", count, path.name)
    return warnings


def _success_from_counts(
    ok_count: int,
    err_count: int,
    *,
    nr_errors_less_than: str = "10",
    success_condition: str = "",
) -> bool:
    cond = (success_condition or "success_if_no_errors").strip().lower()
    try:
        limit = int(float(nr_errors_less_than or "10"))
    except ValueError:
        limit = 10
    if cond in {"success_when_at_least", "success_if_at_least_x_files_un_zipped"}:
        return ok_count >= limit
    if cond in {"success_if_errors_less", "success_when_errors_less"}:
        return err_count < limit
    return err_count == 0


def encrypt_files(
    rows: Sequence[Mapping[str, str]],
    *,
    gpglocation: str = "",
    gnupghome: str = "",
    public_key_file: str = "",
    ascii_mode: bool = True,
    include_subfolders: bool = False,
    create_destination_folder: bool = True,
    destination_is_a_file: bool = False,
    iffileexists: str = "0",
    add_result_filesname: bool = False,
    nr_errors_less_than: str = "10",
    success_condition: str = "",
    compression: str = "",
) -> PgpOutcome:
    """Encrypt (and optionally sign) files — Pentaho ``PGP_ENCRYPT_FILES``."""
    warnings: list[str] = []
    if compression:
        warnings.append(
            f"compression={compression!r} deferred to GnuPG defaults / config"
        )

    try:
        gpg, w = _make_gpg(gpglocation, gnupghome=gnupghome)
        warnings.extend(w)
    except ImportError as exc:
        return PgpOutcome(False, str(exc), error=exc, warnings=[str(exc)])

    warnings.extend(_import_key_file(gpg, public_key_file, private=False))

    outputs: list[str] = []
    errors: list[str] = []
    for row in rows:
        source = str(row.get("source_filefolder") or row.get("source") or "").strip()
        dest_root = str(
            row.get("destination_filefolder") or row.get("destination") or ""
        ).strip()
        wildcard = str(row.get("wildcard") or "").strip()
        userid = str(row.get("userid") or row.get("recipient") or "").strip()
        action = str(row.get("action_type") or "encrypt").strip().lower()

        if not source:
            continue
        files = _iter_source_files(source, wildcard, recursive=include_subfolders)
        if not files:
            errors.append(f"No files matched under {source!r} wildcard={wildcard!r}")
            continue
        if not userid and not public_key_file:
            errors.append(f"No recipient userid for source {source!r}")
            continue

        for fp in files:
            try:
                if destination_is_a_file and dest_root:
                    out_path = Path(dest_root)
                elif dest_root:
                    out_dir = Path(dest_root)
                    if create_destination_folder:
                        out_dir.mkdir(parents=True, exist_ok=True)
                    suffix = ".asc" if ascii_mode else ".gpg"
                    out_path = out_dir / f"{fp.name}{suffix}"
                else:
                    suffix = ".asc" if ascii_mode else ".gpg"
                    out_path = fp.with_name(fp.name + suffix)

                resolved, w2 = _resolve_output(
                    out_path,
                    iffileexists=iffileexists,
                    create_parent=create_destination_folder,
                )
                warnings.extend(w2)
                if resolved is None:
                    continue

                recipients = [userid] if userid else None
                sign = action in {
                    "sign",
                    "sign_and_encrypt",
                    "action_type_sign",
                    "action_type_sign_and_encrypt",
                    "1",
                    "2",
                }
                encrypt = action not in {"sign", "action_type_sign", "1"}
                # action codes: encrypt / sign / sign_and_encrypt
                if action in {"encrypt", "action_type_encrypt", "0", ""}:
                    sign, encrypt = False, True
                elif action in {"sign", "action_type_sign", "1"}:
                    sign, encrypt = True, False
                    warnings.append(
                        "action_type=sign — signing without encryption "
                        "(requires secret key in keyring)"
                    )
                else:
                    sign, encrypt = True, True
                    warnings.append(
                        "action_type=sign_and_encrypt — requires secret key in keyring"
                    )

                data = fp.read_bytes()
                if encrypt:
                    result = gpg.encrypt(
                        data,
                        recipients or True,
                        armor=ascii_mode,
                        always_trust=True,
                        sign=userid if sign else None,
                        output=str(resolved),
                    )
                else:
                    result = gpg.sign(
                        data,
                        keyid=userid or None,
                        armor=ascii_mode,
                        output=str(resolved),
                    )
                ok = bool(getattr(result, "ok", False) or resolved.exists())
                if not ok:
                    status = getattr(result, "status", result)
                    errors.append(f"Encrypt/sign failed for {fp}: {status}")
                    continue
                outputs.append(str(resolved))
                logger.info("PGP encrypted %s → %s", fp, resolved)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{fp}: {exc}")

    ok = _success_from_counts(
        len(outputs),
        len(errors),
        nr_errors_less_than=nr_errors_less_than,
        success_condition=success_condition,
    )
    for err in errors:
        warnings.append(err)
    return PgpOutcome(
        ok,
        f"PGP encrypt: ok={len(outputs)} errors={len(errors)}",
        outputs if add_result_filesname else outputs,
        warnings,
        error=None if ok else RuntimeError("; ".join(errors) or "PGP encrypt failed"),
        extra={"ok_count": len(outputs), "error_count": len(errors), "errors": errors},
    )


def decrypt_files(
    rows: Sequence[Mapping[str, str]],
    *,
    gpglocation: str = "",
    gnupghome: str = "",
    private_key_file: str = "",
    default_passphrase: str = "",
    include_subfolders: bool = False,
    create_destination_folder: bool = True,
    destination_is_a_file: bool = False,
    iffileexists: str = "0",
    add_result_filesname: bool = False,
    nr_errors_less_than: str = "10",
    success_condition: str = "",
    integrity_check: bool = True,
) -> PgpOutcome:
    """Decrypt files — Pentaho ``PGP_DECRYPT_FILES``."""
    warnings: list[str] = []
    if not integrity_check:
        warnings.append(
            "integrity_check=N — GnuPG still verifies MDC when present in ciphertext"
        )

    # Prefer env / secrets over literals
    env_pass = (
        os.environ.get("GPG_PASSPHRASE")
        or os.environ.get("PGP_PASSPHRASE")
        or ""
    ).strip()
    if default_passphrase and not default_passphrase.startswith("${"):
        # Still usable, but discourage logging
        pass
    if not default_passphrase and env_pass:
        default_passphrase = env_pass
        warnings.append("Using GPG_PASSPHRASE / PGP_PASSPHRASE from environment")

    try:
        gpg, w = _make_gpg(gpglocation, gnupghome=gnupghome)
        warnings.extend(w)
    except ImportError as exc:
        return PgpOutcome(False, str(exc), error=exc, warnings=[str(exc)])

    warnings.extend(_import_key_file(gpg, private_key_file, private=True))

    outputs: list[str] = []
    errors: list[str] = []
    for row in rows:
        source = str(row.get("source_filefolder") or row.get("source") or "").strip()
        dest_root = str(
            row.get("destination_filefolder") or row.get("destination") or ""
        ).strip()
        wildcard = str(row.get("wildcard") or "").strip()
        passphrase = str(row.get("passphrase") or default_passphrase or "").strip()
        # Never leave unresolved ${VAR} as the passphrase silently
        if passphrase.startswith("${") and passphrase.endswith("}"):
            errors.append(
                f"Passphrase variable {passphrase} was not substituted — "
                "set job variable or GPG_PASSPHRASE"
            )
            continue

        if not source:
            continue
        files = _iter_source_files(source, wildcard, recursive=include_subfolders)
        if not files:
            errors.append(f"No files matched under {source!r}")
            continue

        for fp in files:
            try:
                if destination_is_a_file and dest_root:
                    out_path = Path(dest_root)
                elif dest_root:
                    out_dir = Path(dest_root)
                    if create_destination_folder:
                        out_dir.mkdir(parents=True, exist_ok=True)
                    name = fp.name
                    for ext in (".gpg", ".asc", ".pgp", ".bin"):
                        if name.lower().endswith(ext):
                            name = name[: -len(ext)]
                            break
                    out_path = out_dir / name
                else:
                    name = fp.name
                    for ext in (".gpg", ".asc", ".pgp"):
                        if name.lower().endswith(ext):
                            name = name[: -len(ext)]
                            break
                    out_path = fp.with_name(name or (fp.name + ".dec"))

                resolved, w2 = _resolve_output(
                    out_path,
                    iffileexists=iffileexists,
                    create_parent=create_destination_folder,
                )
                warnings.extend(w2)
                if resolved is None:
                    continue

                blob = fp.read_bytes()
                result = gpg.decrypt(
                    blob,
                    passphrase=passphrase or None,
                    output=str(resolved),
                )
                ok = bool(getattr(result, "ok", False) or resolved.exists())
                if not ok:
                    status = getattr(result, "status", result)
                    errors.append(
                        f"Decrypt failed for {fp}: {status} "
                        "(bad passphrase / missing private key / corrupt data)"
                    )
                    if resolved.exists() and resolved.stat().st_size == 0:
                        try:
                            resolved.unlink()
                        except OSError:
                            pass
                    continue
                outputs.append(str(resolved))
                logger.info("PGP decrypted %s → %s", fp, resolved)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{fp}: {exc}")

    ok = _success_from_counts(
        len(outputs),
        len(errors),
        nr_errors_less_than=nr_errors_less_than,
        success_condition=success_condition,
    )
    for err in errors:
        warnings.append(err)
    return PgpOutcome(
        ok,
        f"PGP decrypt: ok={len(outputs)} errors={len(errors)}",
        outputs,
        warnings,
        error=None if ok else RuntimeError("; ".join(errors) or "PGP decrypt failed"),
        extra={"ok_count": len(outputs), "error_count": len(errors), "errors": errors},
    )


def verify_signature(
    *,
    filename: str,
    detached_filename: str = "",
    use_detached_signature: bool = False,
    gpglocation: str = "",
    gnupghome: str = "",
    public_key_file: str = "",
) -> PgpOutcome:
    """Verify PGP signature — Pentaho ``PGP_VERIFY_FILES``."""
    warnings: list[str] = []
    path = Path(filename or "")
    if not filename or not path.exists():
        err = FileNotFoundError(f"Signed file not found: {filename!r}")
        return PgpOutcome(False, str(err), error=err)

    try:
        gpg, w = _make_gpg(gpglocation, gnupghome=gnupghome)
        warnings.extend(w)
    except ImportError as exc:
        return PgpOutcome(False, str(exc), error=exc, warnings=[str(exc)])

    warnings.extend(_import_key_file(gpg, public_key_file, private=False))

    try:
        if use_detached_signature:
            det = Path(detached_filename or "")
            if not detached_filename or not det.exists():
                err = FileNotFoundError(
                    f"Detached signature file missing: {detached_filename!r}"
                )
                return PgpOutcome(False, str(err), error=err, warnings=warnings)
            data = path.read_bytes()
            sig = det.read_bytes()
            # python-gnupg verify_data(signature, data)
            if hasattr(gpg, "verify_data"):
                result = gpg.verify_data(sig, data)
            else:
                # Fallback: write temp and use verify_file patterns
                result = gpg.verify(sig)
                warnings.append(
                    "verify_data unavailable — used gpg.verify on signature bytes only"
                )
        else:
            with path.open("rb") as fh:
                result = gpg.verify_file(fh)

        valid = bool(getattr(result, "valid", False))
        details = {
            "valid": valid,
            "username": getattr(result, "username", None),
            "key_id": getattr(result, "key_id", None),
            "fingerprint": getattr(result, "fingerprint", None),
            "status": getattr(result, "status", None),
            "trust_level": getattr(result, "trust_level", None),
        }
        logger.info("PGP verify %s → %s", path, details)
        if not valid:
            err = ValueError(
                f"Signature verification failed for {path}: {details.get('status')}"
            )
            return PgpOutcome(
                False, str(err), [str(path)], warnings, error=err, extra=details
            )
        return PgpOutcome(
            True,
            f"Signature valid for {path}",
            [str(path)],
            warnings,
            extra=details,
        )
    except Exception as exc:  # noqa: BLE001
        return PgpOutcome(False, str(exc), [str(path)], warnings, error=exc)
