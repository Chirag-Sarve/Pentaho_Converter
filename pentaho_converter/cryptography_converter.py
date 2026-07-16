"""Convert Pentaho Cryptography steps to Databricks-compatible PySpark/Python code.

Security policy:
- Never emit secret keys, passphrases, or IV material as literals.
- Prefer Databricks Secrets (``dbutils.secrets.get``) placeholders.
- Prefer standard libraries: ``cryptography`` for symmetric/keygen, ``python-gnupg``
  (or GPG binary) for PGP stream steps.

Note on PDI ``gpglocation``:
- In stock PGP Encrypt/Decrypt Stream, this is the path to the **gpg executable**,
  not the keyring home. Keyring home (if any) is taken from ``keyring_path`` /
  extension tags, or left to the default GnuPG home.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_SUPPORTED_MODES = frozenset({"ECB", "CBC", "CFB", "OFB", "CTR", "GCM", ""})
_COMPRESS_ALGO_MAP = {
    "ZIP": "ZIP",
    "ZLIB": "ZLIB",
    "BZIP2": "BZIP2",
    "BZIP": "BZIP2",
    "UNCOMPRESSED": "Uncompressed",
    "NONE": "Uncompressed",
    "0": "Uncompressed",
}


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _norm_algo(algorithm: str) -> str:
    raw = (algorithm or "AES").strip().upper().replace("-", "")
    if raw in ("TRIPLEDES", "3DES", "DESEDE"):
        return "DESede"
    if raw == "DES":
        return "DES"
    if raw == "AES":
        return "AES"
    return (algorithm or "AES").strip() or "AES"


def _parse_scheme(schema: str, algorithm: str) -> tuple[str, str, str]:
    """Return (algorithm, mode, padding) from a Java Cipher transformation string."""
    algo = _norm_algo(algorithm)
    parts = [p.strip() for p in (schema or "").replace("_", "/").split("/") if p.strip()]
    mode = ""
    padding = ""
    if parts:
        algo = _norm_algo(parts[0]) if parts[0] else algo
    if len(parts) >= 2:
        mode = parts[1].upper()
    if len(parts) >= 3:
        padding = parts[2]
    if not mode:
        # PDI default schemes are bare algorithm names → Java ECB/PKCS5Padding
        mode = "ECB"
        padding = padding or "PKCS5Padding"
    return algo, mode, padding


def _empty_df(out_var: str) -> list[str]:
    """Empty DataFrame without using the banned ``_placeholder`` pattern."""
    return [
        "from pyspark.sql.types import StructType",
        f"{out_var} = spark.createDataFrame([], StructType([]))",
    ]


def _preserve_safe(metadata: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    """Emit ``# preserved.*`` lines; redact anything secret-like."""
    lines: list[str] = []
    skip = frozenset({
        "extras", "step_type", "step_name", "attributes", "fields",
        "transformation_parameters", "_propagated_keys", "_propagation_trace",
        "secret_key", "passphrase", "passhrase", "password", "iv",
    })
    redact_tokens = ("password", "passphrase", "secret_key", "private_key")
    seen: set[str] = set()

    def _emit(key: str, val: object) -> None:
        low = key.lower()
        is_ref_or_flag = (
            key.endswith("_configured")
            or key.endswith("_ref")
            or key.endswith("_field")
            or "in_field" in low
            or "from_field" in low
        )
        if any(tok in low for tok in redact_tokens) and not is_ref_or_flag:
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


def _gpg_construct_lines(
    *,
    gpg_binary: str,
    gnupg_home: str,
) -> list[str]:
    """Emit python-gnupg construction using executable path + optional home."""
    lines = [
        f"    gpg_binary = {gpg_binary!r} or None",
        f"    gpg_home = {gnupg_home!r} or None",
        "    gpg_kwargs = {}",
        "    if gpg_binary:",
        "        gpg_kwargs['gpgbinary'] = gpg_binary",
        "    if gpg_home:",
        "        gpg_kwargs['gnupghome'] = gpg_home",
        "    gpg = gnupg.GPG(**gpg_kwargs)",
    ]
    return lines


def _compress_extra_args(compression: str) -> list[str]:
    algo = _COMPRESS_ALGO_MAP.get((compression or "").strip().upper(), "")
    if not algo and compression:
        algo = compression.strip()
    if not algo:
        return []
    return [f"--compress-algo={algo}"]


def convert_pgp_encrypt_stream_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """PGP Encrypt Stream → python-gnupg / OpenPGP UDF (partial; requires cluster libs)."""
    fn = _safe_ident(step_name)
    lines = [f"# PGP Encrypt Stream: {step_name}"]
    stream_field = (metadata.get("stream_field") or "").strip()
    result_field = (metadata.get("result_field") or "result").strip()
    gpg_binary = (metadata.get("gpg_location") or "").strip()
    key_name = (metadata.get("key_name") or "").strip()
    keyname_in_field = bool(metadata.get("keyname_in_field"))
    keyname_field = (metadata.get("keyname_field") or "").strip()
    keyring = (metadata.get("keyring_path") or "").strip()
    ascii_armor = metadata.get("ascii_armor", True)
    compression = (metadata.get("compression") or "").strip()
    integrity = metadata.get("integrity_check", True)
    public_key = (metadata.get("public_key") or "").strip()

    lines.extend(_preserve_safe(metadata, (
        "gpg_location", "key_name", "keyname_in_field", "keyname_field",
        "stream_field", "result_field", "keyring_path", "public_key",
        "ascii_armor", "compression", "integrity_check",
    )))
    lines.append(
        "# NOTE: preserved.gpg_location is the PDI gpg *executable* path (gpgbinary), "
        "not the keyring home"
    )

    if not in_df:
        lines.append("# WARNING: PGP Encrypt Stream has no input DataFrame")
        lines.extend(_empty_df(out_var))
        return lines, "partial"

    if not stream_field:
        lines.append("# WARNING: stream field missing — cannot encrypt")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"

    if not keyname_in_field and not key_name and not public_key:
        lines.append("# WARNING: missing public key / key name — encrypt will fail at runtime")
    if not gpg_binary:
        lines.append(
            "# WARNING: gpg executable path (gpglocation) not set — "
            "python-gnupg will use gpg from PATH"
        )
    if public_key:
        lines.append(
            "# NOTE: public_key config preserved; import key material via "
            "Databricks Secrets (scope='pgp', key='public_key') — never embed key text"
        )

    lines.append(
        "# LIMITATION: Stock PDI invokes the gpg binary via GPG helper; "
        "Databricks uses python-gnupg"
    )
    lines.append("# REQUIRED: pip install python-gnupg (and gpg binary on the cluster)")
    lines.append(
        "# SECURITY: Prefer Databricks Secrets for public key material / keyring mounts "
        "(scope='pgp')"
    )

    extra_args: list[str] = []
    extra_args.extend(_compress_extra_args(compression))
    if integrity:
        extra_args.append("--force-mdc")
    else:
        extra_args.append("--disable-mdc")
        lines.append("# WARNING: integrity check disabled — OpenPGP MDC disabled in generated call")

    if compression:
        lines.append(
            f"# preserved.compression mapped to gpg --compress-algo "
            f"({_COMPRESS_ALGO_MAP.get(compression.upper(), compression)!r})"
        )

    lines.append("from pyspark.sql.functions import udf")
    lines.append("from pyspark.sql.types import StringType")
    lines.append(f"def _pgp_encrypt_row_{fn}(plaintext, key_id=None):")
    lines.append("    if plaintext is None:")
    lines.append("        return None")
    lines.append(
        "    text = plaintext if isinstance(plaintext, (bytes, bytearray)) "
        "else str(plaintext)"
    )
    lines.append("    if isinstance(text, str) and text == '':")
    lines.append(
        "        # PDI throws on empty stream; Spark returns '' to avoid failing the whole job"
    )
    lines.append("        return ''")
    lines.append("    try:")
    lines.append("        import gnupg")
    lines.append("    except ImportError as exc:")
    lines.append(
        "        raise ImportError("
        "'python-gnupg is required for PGP Encrypt Stream migration') from exc"
    )
    lines.extend(_gpg_construct_lines(gpg_binary=gpg_binary, gnupg_home=keyring))
    if public_key:
        lines.append(
            "    _pub = dbutils.secrets.get(scope='pgp', key='public_key')  # noqa: F821"
        )
        lines.append("    _imp = gpg.import_keys(_pub)")
        lines.append("    if getattr(_imp, 'count', 0) == 0:")
        lines.append(
            "        raise ValueError('PGP encrypt: public key import failed / invalid key')"
        )
    if keyname_in_field:
        lines.append("    rid = (key_id or '').strip()")
        lines.append("    if not rid:")
        lines.append("        raise ValueError('PGP encrypt: key name field is empty')")
        lines.append("    recipients = [rid]")
    else:
        lines.append(f"    recipients = [{key_name!r}] if {bool(key_name)!r} else []")
        lines.append("    if key_id:")
        lines.append("        recipients = [str(key_id)]")
    lines.append("    if not recipients:")
    lines.append("        raise ValueError('PGP encrypt: no recipient / public key configured')")
    lines.append("    data = text if isinstance(text, bytes) else text.encode('utf-8')")
    lines.append(f"    extra_args = {extra_args!r}")
    lines.append("    try:")
    lines.append(
        f"        encrypted = gpg.encrypt("
        f"data, recipients, armor={bool(ascii_armor)!r}, extra_args=extra_args)"
    )
    lines.append("        if not encrypted.ok:")
    lines.append(
        "            raise ValueError("
        "f'PGP encrypt failed: {getattr(encrypted, \"status\", encrypted)}')"
    )
    lines.append("        return str(encrypted)")
    lines.append("    except Exception as exc:")
    lines.append(
        "        raise RuntimeError("
        "f'PGP encrypt error (corrupt data / missing key): {exc}') from exc"
    )

    lines.append(f"_pgp_encrypt_udf_{fn} = udf(_pgp_encrypt_row_{fn}, StringType())")
    if keyname_in_field and keyname_field:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _pgp_encrypt_udf_{fn}("
            f"col({stream_field!r}), col({keyname_field!r})))"
        )
    else:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _pgp_encrypt_udf_{fn}(col({stream_field!r}), lit(None)))"
        )
    logger.info(
        "PGPEncryptStream '%s': stream=%s result=%s armor=%s compression=%s integrity=%s",
        step_name, stream_field, result_field, ascii_armor, compression or None, integrity,
    )
    return lines, "partial"


def convert_pgp_decrypt_stream_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """PGP Decrypt Stream → python-gnupg UDF with Databricks Secrets passphrase."""
    fn = _safe_ident(step_name)
    lines = [f"# PGP Decrypt Stream: {step_name}"]
    stream_field = (metadata.get("stream_field") or "").strip()
    result_field = (metadata.get("result_field") or "result").strip()
    gpg_binary = (metadata.get("gpg_location") or "").strip()
    keyring = (metadata.get("keyring_path") or "").strip()
    private_key = (metadata.get("private_key") or "").strip()
    passphrase_from_field = bool(metadata.get("passphrase_from_field"))
    passphrase_field = (metadata.get("passphrase_field") or "").strip()
    passphrase_ref = (
        metadata.get("passphrase_secret_ref")
        or "dbutils.secrets.get(scope='pgp', key='passphrase')"
    )
    integrity = metadata.get("integrity_check", True)

    lines.extend(_preserve_safe(metadata, (
        "gpg_location", "passphrase_configured", "passphrase_secret_ref",
        "passphrase_from_field", "passphrase_field", "stream_field",
        "result_field", "keyring_path", "private_key", "integrity_check",
    )))
    lines.append(
        "# NOTE: preserved.gpg_location is the PDI gpg *executable* path (gpgbinary)"
    )

    if not in_df:
        lines.append("# WARNING: PGP Decrypt Stream has no input DataFrame")
        lines.extend(_empty_df(out_var))
        return lines, "partial"

    if not stream_field:
        lines.append("# WARNING: encrypted stream field missing — cannot decrypt")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"

    if not metadata.get("passphrase_configured") and not passphrase_from_field:
        lines.append(
            "# WARNING: no passphrase configured — "
            "store private-key passphrase in Databricks Secrets "
            "(scope='pgp', key='passphrase')"
        )
    if not keyring and not private_key:
        lines.append(
            "# WARNING: no keyring_path / private_key — "
            "import private key via Secrets (scope='pgp', key='private_key') "
            "or mount a keyring volume"
        )
    if not integrity:
        lines.append(
            "# WARNING: integrity verification disabled in source config — "
            "decrypt still verifies MDC when present in ciphertext"
        )
    else:
        lines.append(
            "# Integrity: python-gnupg/gpg verifies MDC on decrypt when ciphertext includes it"
        )

    lines.append("# LIMITATION: Stock PDI invokes gpg; Databricks uses python-gnupg + Secrets")
    lines.append("# REQUIRED: pip install python-gnupg")
    lines.append("# SECURITY: Never hardcode passphrases or private keys; use Databricks Secrets")

    lines.append("from pyspark.sql.functions import udf")
    lines.append("from pyspark.sql.types import StringType")
    lines.append(f"def _pgp_decrypt_row_{fn}(ciphertext, passphrase_override=None):")
    lines.append("    if ciphertext is None:")
    lines.append("        return None")
    lines.append(
        "    blob = ciphertext if isinstance(ciphertext, (bytes, bytearray)) "
        "else str(ciphertext)"
    )
    lines.append("    if isinstance(blob, str) and blob == '':")
    lines.append("        return ''")
    lines.append("    try:")
    lines.append("        import gnupg")
    lines.append("    except ImportError as exc:")
    lines.append(
        "        raise ImportError("
        "'python-gnupg is required for PGP Decrypt Stream migration') from exc"
    )
    lines.extend(_gpg_construct_lines(gpg_binary=gpg_binary, gnupg_home=keyring))
    lines.append(
        "    # Private key material from Databricks Secrets when keyring is not mounted"
    )
    lines.append("    try:")
    lines.append(
        "        _priv = dbutils.secrets.get(scope='pgp', key='private_key')  # noqa: F821"
    )
    lines.append("        if _priv:")
    lines.append("            _imp = gpg.import_keys(_priv)")
    lines.append("            if getattr(_imp, 'count', 0) == 0:")
    lines.append(
        "                raise ValueError('PGP decrypt: private key import failed / invalid key')"
    )
    lines.append("    except Exception as _sec_exc:")
    lines.append(
        "        # Optional when a keyring is mounted or cluster gpg home already has keys"
    )
    lines.append(
        "        _ = _sec_exc  # preserve for debugging; continue with configured keyring/home"
    )
    lines.append("    if passphrase_override is not None and str(passphrase_override) != '':")
    lines.append("        passphrase = str(passphrase_override)")
    lines.append("    else:")
    lines.append(f"        passphrase = {passphrase_ref}  # noqa: F821")
    lines.append("    try:")
    lines.append("        decrypted = gpg.decrypt(blob, passphrase=passphrase)")
    lines.append("        if not decrypted.ok:")
    lines.append(
        "            raise ValueError("
        "'PGP decrypt failed (bad passphrase / corrupt data / missing private key / "
        "integrity failure): ' + str(getattr(decrypted, 'status', decrypted)))"
    )
    lines.append("        return str(decrypted)")
    lines.append("    except Exception as exc:")
    lines.append("        raise RuntimeError(f'PGP decrypt error: {exc}') from exc")

    lines.append(f"_pgp_decrypt_udf_{fn} = udf(_pgp_decrypt_row_{fn}, StringType())")
    if passphrase_from_field and passphrase_field:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _pgp_decrypt_udf_{fn}("
            f"col({stream_field!r}), col({passphrase_field!r})))"
        )
    else:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _pgp_decrypt_udf_{fn}(col({stream_field!r}), lit(None)))"
        )
    logger.info(
        "PGPDecryptStream '%s': stream=%s result=%s passphrase_from_field=%s",
        step_name, stream_field, result_field, passphrase_from_field,
    )
    return lines, "partial"


def convert_secret_key_generator_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Secret Key Generator → cryptography-validated secure key generation."""
    fn = _safe_ident(step_name)
    lines = [f"# Secret Key Generator: {step_name}"]
    key_field = (metadata.get("secret_key_field") or "secretKey").strip()
    len_field = (metadata.get("secret_key_length_field") or "").strip()
    algo_field = (metadata.get("algorithm_field") or "").strip()
    encoding = (metadata.get("encoding") or "hex").strip().lower()
    binary_out = bool(metadata.get("output_key_in_binary")) or encoding == "binary"
    keys = list(metadata.get("keys") or metadata.get("fields") or [])

    lines.extend(_preserve_safe(metadata, (
        "keys", "secret_key_field", "secret_key_length_field",
        "algorithm_field", "encoding", "output_key_in_binary",
    )))

    if not keys:
        lines.append("# WARNING: no key definitions configured")
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.extend(_empty_df(out_var))
        return lines, "partial"

    for item in keys:
        algo = _norm_algo(item.get("algorithm") or "AES")
        if algo not in ("AES", "DES", "DESede"):
            lines.append(
                f"# WARNING: unsupported / unknown algorithm {item.get('algorithm')!r} — "
                "using length-based secure token generation"
            )

    lines.append("# SECURITY: generated keys are sensitive — prefer Databricks Secrets storage")
    lines.append("# REQUIRED: pip install cryptography")
    lines.append(
        "# Uses cryptography for algorithm validation + secrets.token_bytes / "
        "os.urandom for CSPRNG key material"
    )
    lines.append(
        "from pyspark.sql.types import StructType, StructField, StringType, LongType, BinaryType"
    )
    lines.append(f"def _generate_secret_keys_{fn}():")
    lines.append("    import binascii")
    lines.append("    import secrets as _secrets")
    lines.append("    try:")
    lines.append("        from cryptography.hazmat.primitives.ciphers import algorithms as _calgo")
    lines.append("        _crypto_ok = True")
    lines.append("    except ImportError:")
    lines.append("        _calgo = None")
    lines.append("        _crypto_ok = False")
    lines.append("    rows = []")

    for idx, item in enumerate(keys):
        algo = _norm_algo(item.get("algorithm") or "AES")
        scheme = item.get("scheme") or algo
        try:
            key_len = int(item.get("key_length") or 128)
        except (TypeError, ValueError):
            key_len = 128
        try:
            count = int(item.get("key_count") or 1)
        except (TypeError, ValueError):
            count = 1
        if count < 0:
            count = 0
        if algo == "DES":
            byte_len = 8
            key_len = 56
        elif algo == "DESede":
            byte_len = 24 if key_len >= 168 else 16
            if key_len not in (112, 168):
                lines.append(
                    f"    # WARNING: unusual Triple DES length {key_len}; "
                    f"using {byte_len * 8} bits"
                )
        else:
            if key_len not in (128, 192, 256):
                lines.append(
                    f"    # WARNING: invalid AES key size {key_len}; using 128"
                )
                key_len = 128
            byte_len = key_len // 8
        lines.append(f"    # key definition {idx}: algorithm={algo!r} scheme={scheme!r}")
        lines.append(f"    if _crypto_ok:")
        if algo == "AES":
            lines.append(
                f"        _ = _calgo.AES  # validate AES support; size={key_len} bits"
            )
        elif algo in ("DES", "DESede"):
            lines.append(
                "        # DES/DESede are legacy; cryptography may expose them under decrepit APIs"
            )
            lines.append("        try:")
            lines.append(
                "            from cryptography.hazmat.decrepit.ciphers.algorithms "
                "import TripleDES as _TripleDES  # noqa: F401"
            )
            lines.append("        except ImportError:")
            lines.append(
                "            pass  # still emit keys via CSPRNG for migration fidelity"
            )
        else:
            lines.append("        pass  # unknown algorithm — CSPRNG length-based keys")
        lines.append(f"    for _i_{idx} in range({count}):")
        lines.append(f"        _raw_{idx} = _secrets.token_bytes({byte_len})")
        if binary_out:
            lines.append(f"        _key_{idx} = _raw_{idx}")
        else:
            lines.append(
                f"        _key_{idx} = binascii.hexlify(_raw_{idx}).decode('ascii')"
            )
        row_parts = [f"{key_field!r}: _key_{idx}"]
        if algo_field:
            row_parts.append(f"{algo_field!r}: {algo!r}")
        if len_field:
            row_parts.append(f"{len_field!r}: {key_len}")
        lines.append(f"        rows.append({{{', '.join(row_parts)}}})")

    lines.append("    return rows")
    lines.append(f"_secret_key_rows_{fn} = _generate_secret_keys_{fn}()")
    schema_fields = [
        f"StructField({key_field!r}, "
        f"{'BinaryType()' if binary_out else 'StringType()'}, True)"
    ]
    if algo_field:
        schema_fields.append(f"StructField({algo_field!r}, StringType(), True)")
    if len_field:
        schema_fields.append(f"StructField({len_field!r}, LongType(), True)")
    lines.append(f"_secret_key_schema_{fn} = StructType([{', '.join(schema_fields)}])")
    lines.append(
        f"{out_var} = spark.createDataFrame("
        f"_secret_key_rows_{fn}, schema=_secret_key_schema_{fn})"
    )
    if in_df:
        lines.append(
            f"# NOTE: Secret Key Generator replaces hop input; "
            f"upstream {in_df} is not unioned (PDI generator semantics)"
        )
    logger.info(
        "SecretKeyGenerator '%s': %s definition(s) encoding=%s",
        step_name, len(keys), "binary" if binary_out else "hex",
    )
    return lines, "converted"


def convert_symmetric_crypto_step(
    metadata: dict[str, Any],
    in_df: str,
    out_var: str,
    step_name: str,
) -> tuple[list[str], str]:
    """Symmetric Cryptography → cryptography library UDF (encrypt/decrypt)."""
    fn = _safe_ident(step_name)
    lines = [f"# Symmetric Cryptography: {step_name}"]
    operation = (metadata.get("operation_type") or "encrypt").strip().lower()
    algorithm = metadata.get("algorithm") or "AES"
    schema = metadata.get("schema") or metadata.get("scheme") or algorithm
    algo, mode, padding = _parse_scheme(schema, algorithm)
    message_field = (metadata.get("message_field") or "").strip()
    result_field = (metadata.get("result_field") or "result").strip()
    key_in_field = bool(metadata.get("secret_key_in_field"))
    key_field = (metadata.get("secret_key_field") or "").strip()
    read_key_binary = bool(metadata.get("read_key_as_binary"))
    out_binary = bool(metadata.get("output_result_as_binary"))
    iv_field = (metadata.get("iv_field") or "").strip()
    iv_configured = bool(metadata.get("iv_configured"))
    key_ref = (
        metadata.get("secret_key_secret_ref")
        or "dbutils.secrets.get(scope='crypto', key='secret_key')"
    )
    iv_ref = (
        metadata.get("iv_secret_ref")
        or "dbutils.secrets.get(scope='crypto', key='iv')"
    )

    meta = dict(metadata)
    meta["cipher_mode"] = meta.get("cipher_mode") or mode
    meta["padding"] = meta.get("padding") or padding
    meta["algorithm_normalized"] = algo

    lines.extend(_preserve_safe(meta, (
        "operation_type", "algorithm", "schema", "scheme", "cipher_mode",
        "padding", "message_field", "result_field", "secret_key_configured",
        "secret_key_secret_ref", "secret_key_in_field", "secret_key_field",
        "read_key_as_binary", "output_result_as_binary",
        "iv_configured", "iv_secret_ref", "iv_field", "algorithm_normalized",
    )))

    if not in_df:
        lines.append("# WARNING: Symmetric Cryptography has no input DataFrame")
        lines.extend(_empty_df(out_var))
        return lines, "partial"

    if not message_field:
        lines.append("# WARNING: message field missing — cannot encrypt/decrypt")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"

    if algo not in ("AES", "DES", "DESede"):
        lines.append(
            f"# WARNING: invalid / unsupported algorithm {algorithm!r} — "
            "migration emits best-effort mapping"
        )
    if mode and mode.upper() not in _SUPPORTED_MODES:
        lines.append(
            f"# WARNING: invalid / uncommon cipher mode {mode!r} — "
            "verify cryptography backend support"
        )
    if mode.upper() in ("CBC", "CFB", "OFB", "CTR", "GCM") and not iv_field and not iv_configured:
        lines.append(
            "# WARNING: cipher mode requires an IV — stock PDI often omits IV. "
            "Provide IV via Secrets (scope='crypto', key='iv') or iv_field."
        )
    if not key_in_field and not meta.get("secret_key_configured"):
        lines.append(
            "# WARNING: missing secret key — store key hex/bytes in Databricks Secrets "
            "(scope='crypto', key='secret_key')"
        )

    lines.append("# REQUIRED: pip install cryptography")
    lines.append("# SECURITY: Never hardcode keys; use Databricks Secrets")
    lines.append(
        "# EDGE CASES: null → null; empty → empty; "
        "invalid keys / corrupt ciphertext raise in UDF"
    )

    result_type = "BinaryType()" if out_binary else "StringType()"
    empty_ret = "b''" if out_binary else "''"

    lines.append("from pyspark.sql.functions import udf")
    lines.append("from pyspark.sql.types import StringType, BinaryType")
    lines.append(f"def _symmetric_crypto_row_{fn}(message, key_override=None, iv_override=None):")
    lines.append("    import binascii")
    lines.append("    if message is None:")
    lines.append("        return None")
    lines.append("    if isinstance(message, str) and message == '':")
    lines.append(f"        return {empty_ret}")
    lines.append("    try:")
    lines.append(
        "        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes"
    )
    lines.append("        from cryptography.hazmat.primitives import padding as sym_padding")
    lines.append("        from cryptography.hazmat.backends import default_backend")
    lines.append("    except ImportError as exc:")
    lines.append(
        "        raise ImportError("
        "'cryptography is required for Symmetric Cryptography migration') from exc"
    )
    lines.append(f"    algo_name = {algo!r}")
    lines.append(f"    mode_name = {mode.upper()!r}")
    lines.append(f"    do_encrypt = {operation == 'encrypt'!r}")
    lines.append("    if key_override is not None and str(key_override) != '':")
    lines.append("        key_mat = key_override")
    lines.append("    else:")
    lines.append(f"        key_mat = {key_ref}  # noqa: F821")
    if read_key_binary:
        lines.append("    if isinstance(key_mat, str):")
        lines.append("        try:")
        lines.append("            key_bytes = binascii.unhexlify(key_mat.strip())")
        lines.append("        except Exception:")
        lines.append("            key_bytes = key_mat.encode('utf-8')")
        lines.append("    else:")
        lines.append("        key_bytes = bytes(key_mat)")
    else:
        lines.append("    if isinstance(key_mat, (bytes, bytearray)):")
        lines.append("        key_bytes = bytes(key_mat)")
        lines.append("    else:")
        lines.append("        try:")
        lines.append("            key_bytes = binascii.unhexlify(str(key_mat).strip())")
        lines.append("        except Exception as exc:")
        lines.append(
            "            raise ValueError(f'Invalid secret key encoding: {exc}') from exc"
        )
    lines.append("    if not key_bytes:")
    lines.append("        raise ValueError('Symmetric crypto: empty / missing secret key')")
    lines.append("    if algo_name == 'AES' and len(key_bytes) not in (16, 24, 32):")
    lines.append(
        "        raise ValueError("
        "f'Invalid AES key length {len(key_bytes)*8} bits; expected 128/192/256')"
    )
    lines.append("    if algo_name == 'DES' and len(key_bytes) != 8:")
    lines.append("        raise ValueError('Invalid DES key length')")
    lines.append("    if algo_name == 'DESede' and len(key_bytes) not in (16, 24):")
    lines.append("        raise ValueError('Invalid Triple DES key length')")

    lines.append("    if algo_name == 'AES':")
    lines.append("        algo_obj = algorithms.AES(key_bytes)")
    lines.append("        block = 16")
    lines.append("    else:")
    lines.append("        # DES / DESede (legacy) via cryptography decrepit APIs when available")
    lines.append("        try:")
    lines.append(
        "            from cryptography.hazmat.decrepit.ciphers.algorithms "
        "import TripleDES as _TripleDES"
    )
    lines.append("        except ImportError:")
    lines.append("            try:")
    lines.append(
        "                from cryptography.hazmat.primitives.ciphers.algorithms "
        "import TripleDES as _TripleDES"
    )
    lines.append("            except ImportError as exc:")
    lines.append(
        "                raise ValueError("
        "'DES/DESede unavailable in this cryptography build') from exc"
    )
    lines.append("        if algo_name == 'DES':")
    lines.append("            algo_obj = _TripleDES(key_bytes * 3)")
    lines.append("        else:")
    lines.append("            algo_obj = _TripleDES(key_bytes)")
    lines.append("        block = 8")

    lines.append("    iv_bytes = None")
    lines.append("    if iv_override is not None and str(iv_override) != '':")
    lines.append("        iv_raw = iv_override")
    lines.append("        if isinstance(iv_raw, (bytes, bytearray)):")
    lines.append("            iv_bytes = bytes(iv_raw)")
    lines.append("        else:")
    lines.append("            try:")
    lines.append("                iv_bytes = binascii.unhexlify(str(iv_raw).strip())")
    lines.append("            except Exception:")
    lines.append("                iv_bytes = str(iv_raw).encode('utf-8')")
    if iv_configured:
        lines.append("    else:")
        lines.append(f"        _iv_secret = {iv_ref}  # noqa: F821")
        lines.append("        try:")
        lines.append("            iv_bytes = binascii.unhexlify(str(_iv_secret).strip())")
        lines.append("        except Exception:")
        lines.append("            iv_bytes = str(_iv_secret).encode('utf-8')")

    lines.append("    if mode_name in ('', 'ECB'):")
    lines.append("        mode_obj = modes.ECB()")
    lines.append("    elif mode_name == 'CBC':")
    lines.append("        if not iv_bytes:")
    lines.append("            iv_bytes = b'\\x00' * block")
    lines.append("        mode_obj = modes.CBC(iv_bytes[:block])")
    lines.append("    elif mode_name == 'CFB':")
    lines.append("        if not iv_bytes:")
    lines.append("            iv_bytes = b'\\x00' * block")
    lines.append("        mode_obj = modes.CFB(iv_bytes[:block])")
    lines.append("    elif mode_name == 'OFB':")
    lines.append("        if not iv_bytes:")
    lines.append("            iv_bytes = b'\\x00' * block")
    lines.append("        mode_obj = modes.OFB(iv_bytes[:block])")
    lines.append("    elif mode_name == 'CTR':")
    lines.append("        if not iv_bytes:")
    lines.append("            iv_bytes = b'\\x00' * block")
    lines.append("        mode_obj = modes.CTR(iv_bytes[:block])")
    lines.append("    elif mode_name == 'GCM':")
    lines.append("        if not iv_bytes:")
    lines.append("            raise ValueError('GCM mode requires an IV/nonce')")
    lines.append("        mode_obj = modes.GCM(iv_bytes)")
    lines.append("    else:")
    lines.append("        raise ValueError(f'Unsupported cipher mode: {mode_name}')")

    lines.append("    if isinstance(message, (bytes, bytearray)):")
    lines.append("        raw = bytes(message)")
    lines.append("    elif do_encrypt:")
    lines.append("        raw = str(message).encode('utf-8')")
    lines.append("    else:")
    lines.append("        try:")
    lines.append("            raw = binascii.unhexlify(str(message).strip())")
    lines.append("        except Exception:")
    lines.append("            raw = str(message).encode('utf-8')")

    lines.append("    try:")
    lines.append("        cipher = Cipher(algo_obj, mode_obj, backend=default_backend())")
    lines.append("        if do_encrypt:")
    lines.append("            padder = sym_padding.PKCS7(block * 8).padder()")
    lines.append("            padded = padder.update(raw) + padder.finalize()")
    lines.append("            encryptor = cipher.encryptor()")
    lines.append("            out = encryptor.update(padded) + encryptor.finalize()")
    if out_binary:
        lines.append("            return out")
    else:
        lines.append("            return binascii.hexlify(out).decode('ascii')")
    lines.append("        decryptor = cipher.decryptor()")
    lines.append("        padded = decryptor.update(raw) + decryptor.finalize()")
    lines.append("        unpadder = sym_padding.PKCS7(block * 8).unpadder()")
    lines.append("        out = unpadder.update(padded) + unpadder.finalize()")
    if out_binary:
        lines.append("        return out")
    else:
        lines.append("        return out.decode('utf-8', errors='replace')")
    lines.append("    except Exception as exc:")
    lines.append(
        "        raise RuntimeError("
        "f'Symmetric crypto failed (invalid key / corrupt data / bad mode): {exc}') "
        "from exc"
    )

    lines.append(f"_symmetric_udf_{fn} = udf(_symmetric_crypto_row_{fn}, {result_type})")
    if key_in_field and key_field and iv_field:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _symmetric_udf_{fn}(col({message_field!r}), "
            f"col({key_field!r}), col({iv_field!r})))"
        )
    elif key_in_field and key_field:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _symmetric_udf_{fn}(col({message_field!r}), "
            f"col({key_field!r}), lit(None)))"
        )
    elif iv_field:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _symmetric_udf_{fn}(col({message_field!r}), "
            f"lit(None), col({iv_field!r})))"
        )
    else:
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{result_field!r}, _symmetric_udf_{fn}("
            f"col({message_field!r}), lit(None), lit(None)))"
        )

    logger.info(
        "SymmetricCrypto '%s': op=%s algo=%s mode=%s iv_field=%s",
        step_name, operation, algo, mode, iv_field or None,
    )
    return lines, "partial"
