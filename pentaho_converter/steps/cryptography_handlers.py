"""Handlers for Pentaho Cryptography transformations.

Supports:
- PGP Encrypt Stream
- PGP Decrypt Stream
- Secret Key Generator
- Symmetric Cryptography (SymmetricCryptoTrans)
"""

from __future__ import annotations

import logging

from ..cryptography_converter import (
    convert_pgp_decrypt_stream_step,
    convert_pgp_encrypt_stream_step,
    convert_secret_key_generator_step,
    convert_symmetric_crypto_step,
)
from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_pgp_decrypt_stream_config,
    parse_pgp_encrypt_stream_config,
    parse_secret_key_generator_config,
    parse_symmetric_crypto_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return (
        (step_type or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
        .replace("_", "")
    )


def _merge_parsed(context: StepContext, parser) -> dict:
    metadata = dict(get_converter_metadata(context))
    step_el = get_step_element(context.step)
    if step_el is not None:
        parsed = parser(step_el)
        for key, val in parsed.items():
            if key not in metadata or metadata[key] in (None, "", [], {}):
                metadata[key] = val
            elif isinstance(val, list) and not metadata.get(key):
                metadata[key] = val
    return metadata


def _passthrough_error(context: StepContext, label: str, exc: Exception) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [
        f"# {label}: {context.step.name}",
        f"# ERROR: {exc}",
    ]
    logger.exception("%s '%s' failed: %s", label, context.step.name, exc)
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
    return lines, "partial"


class PGPEncryptStreamHandler(BaseStepHandler):
    """PGP Encrypt Stream → python-gnupg UDF (Databricks Secrets for key material)."""

    _TYPES = {"pgpencryptstream", "pgpencrypt"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_pgp_encrypt_stream_config)
            lines, status = convert_pgp_encrypt_stream_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "PGPEncryptStream '%s' status=%s stream=%s",
                context.step.name,
                status,
                metadata.get("stream_field"),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "PGP Encrypt Stream", exc)


class PGPDecryptStreamHandler(BaseStepHandler):
    """PGP Decrypt Stream → python-gnupg UDF (passphrase via Databricks Secrets)."""

    _TYPES = {"pgpdecryptstream", "pgpdecrypt"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_pgp_decrypt_stream_config)
            lines, status = convert_pgp_decrypt_stream_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "PGPDecryptStream '%s' status=%s stream=%s",
                context.step.name,
                status,
                metadata.get("stream_field"),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "PGP Decrypt Stream", exc)


class SecretKeyGeneratorHandler(BaseStepHandler):
    """Secret Key Generator → cryptographically secure token_bytes key rows."""

    _TYPES = {"secretkeygenerator", "secretkeygen"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_secret_key_generator_config)
            lines, status = convert_secret_key_generator_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "SecretKeyGenerator '%s' status=%s keys=%s",
                context.step.name,
                status,
                len(metadata.get("keys") or []),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Secret Key Generator", exc)


class SymmetricCryptoHandler(BaseStepHandler):
    """Symmetric Cryptography → cryptography library encrypt/decrypt UDF."""

    _TYPES = {"symmetriccryptotrans", "symmetriccrypto", "symmetriccryptography"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_symmetric_crypto_config)
            lines, status = convert_symmetric_crypto_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "SymmetricCrypto '%s' status=%s op=%s algo=%s",
                context.step.name,
                status,
                metadata.get("operation_type"),
                metadata.get("algorithm"),
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Symmetric Cryptography", exc)


CRYPTOGRAPHY_HANDLERS: list[BaseStepHandler] = [
    PGPEncryptStreamHandler(),
    PGPDecryptStreamHandler(),
    SecretKeyGeneratorHandler(),
    SymmetricCryptoHandler(),
]
