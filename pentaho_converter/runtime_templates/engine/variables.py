"""Kettle-style ``${VAR}`` substitution utilities."""

from __future__ import annotations

import re
from typing import Any, Mapping

_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def substitute_variables(text: str, variables: Mapping[str, Any]) -> str:
    """Replace ``${VAR}`` placeholders the way Kettle job entries resolve them."""

    if not text or "${" not in text:
        return text

    def _repl(match: re.Match[str]) -> str:
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
        if key in variables and variables[key] is not None:
            return str(variables[key])
        return match.group(0)

    out = text
    for _ in range(8):
        nxt = _VAR_PATTERN.sub(_repl, out)
        if nxt == out:
            break
        out = nxt
    return out
