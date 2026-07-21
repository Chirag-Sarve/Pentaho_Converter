"""Kettle-style ``${VAR}`` / ``%%VAR%%`` substitution utilities."""

from __future__ import annotations

import os
import re
from typing import Any, Mapping

_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")
_PCT_PATTERN = re.compile(r"%%([^%]+)%%")


def _lookup(key: str, variables: Mapping[str, Any]) -> str | None:
    if key in variables and variables[key] is not None:
        return str(variables[key])
    env = os.environ.get(key)
    if env is not None:
        return env
    return None


def substitute_variables(text: str, variables: Mapping[str, Any]) -> str:
    """Replace ``${VAR}`` / ``%%VAR%%`` placeholders the way Kettle resolves them.

    Lookup order: job variables → process environment (JVM-style fallback).
    Nested substitution is limited to 8 passes (PDI-like).
    """

    if not text:
        return text
    if "${" not in text and "%%" not in text:
        return text

    def _dollar(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        if key.startswith("%"):
            from datetime import datetime

            fmt = (
                key[1:]
                .replace("yyyy", "%Y")
                .replace("MM", "%m")
                .replace("dd", "%d")
                .replace("HH", "%H")
                .replace("mm", "%M")
                .replace("ss", "%S")
            )
            return datetime.now().strftime(fmt)
        found = _lookup(key, variables)
        return found if found is not None else match.group(0)

    def _percent(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        found = _lookup(key, variables)
        return found if found is not None else match.group(0)

    out = text
    for _ in range(8):
        nxt = _VAR_PATTERN.sub(_dollar, out)
        nxt = _PCT_PATTERN.sub(_percent, nxt)
        if nxt == out:
            break
        out = nxt
    return out
