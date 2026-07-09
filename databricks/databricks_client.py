"""Minimal Databricks REST client for importing generated notebooks.

Scope:
  * test_connection() -- verify the workspace URL + token work.
  * import_notebook()  -- push generated PySpark into the workspace as a
                          runnable notebook (import-only; never auto-runs).
  * to_notebook_source() -- split a flat .py file into Databricks notebook cells.
"""

from __future__ import annotations

import base64
import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass

_TIMEOUT_SECONDS = 30


@dataclass
class DatabricksResult:
    ok: bool
    message: str
    detail: str = ""


def _normalize_host(host: str) -> str:
    """Return the workspace host as ``https://<host>`` with no trailing slash."""
    h = (host or "").strip().rstrip("/")
    if not h:
        return ""
    if not h.startswith("http://") and not h.startswith("https://"):
        h = "https://" + h
    return h


def _request(host: str, token: str, path: str, payload: dict | None = None,
             method: str = "GET") -> tuple[int, dict]:
    """Perform a Databricks REST request and return (status_code, json_body)."""
    url = _normalize_host(host) + path
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8") or "{}"
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, {"_raw": body}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = {"_raw": raw}
        return exc.code, parsed
    except urllib.error.URLError as exc:
        return 0, {"error_message": f"Could not reach workspace: {exc.reason}"}
    except Exception as exc:  # pragma: no cover - defensive
        return 0, {"error_message": str(exc)}


def test_connection(host: str, token: str) -> DatabricksResult:
    """Verify the host + token by calling a lightweight, read-only endpoint."""
    if not _normalize_host(host):
        return DatabricksResult(False, "Workspace URL is not configured.")
    if not token:
        return DatabricksResult(False, "Access token is not configured.")

    status, body = _request(host, token, "/api/2.0/clusters/spark-versions")
    if status == 200:
        return DatabricksResult(True, "Connected to Databricks.")
    if status in (401, 403):
        return DatabricksResult(False, "Authentication failed (check the token).",
                                detail=str(body))
    if status == 0:
        return DatabricksResult(False, body.get("error_message", "Network error."),
                                detail=str(body))
    return DatabricksResult(
        False,
        f"Unexpected response from Databricks (HTTP {status}).",
        detail=str(body),
    )


def _split_block_at_patterns(block: str, patterns: list[re.Pattern[str]]) -> list[str]:
    """Split *block* at each regex match start, preserving matched content."""
    split_points = sorted({0} | {m.start() for pat in patterns for m in pat.finditer(block)})
    chunks: list[str] = []
    for i, start in enumerate(split_points):
        end = split_points[i + 1] if i + 1 < len(split_points) else len(block)
        chunk = block[start:end].rstrip()
        if chunk:
            chunks.append(chunk)
    return chunks


def to_notebook_source(code: str) -> str:
    """Convert a flat generated .py file into Databricks notebook source format."""
    header = "# Databricks notebook source"
    cell_sep = "\n\n# COMMAND ----------\n\n"

    pentaho_step_pattern = re.compile(r'(?m)^    # Step: ')
    run_func_pattern = re.compile(r'(?m)^def run_[A-Za-z0-9_]+\(')
    main_func_pattern = re.compile(r'(?m)^def main\(\):')
    entrypoint_pattern = re.compile(r'(?m)^if __name__ == ["\']__main__["\']:')

    parts: list[str] = []
    split_patterns = [
        run_func_pattern,
        main_func_pattern,
        pentaho_step_pattern,
        entrypoint_pattern,
    ]
    split_points = sorted(
        {0} | {m.start() for pat in split_patterns for m in pat.finditer(code)}
    )
    for i, start in enumerate(split_points):
        end = split_points[i + 1] if i + 1 < len(split_points) else len(code)
        chunk = code[start:end].rstrip()
        if not chunk:
            continue
        if run_func_pattern.match(chunk) or main_func_pattern.match(chunk):
            inner = _split_block_at_patterns(
                chunk,
                [pentaho_step_pattern, entrypoint_pattern],
            )
            parts.extend(inner)
        else:
            parts.append(chunk)

    if not parts:
        return header + "\n" + code

    return header + "\n\n" + cell_sep.join(p for p in parts if p.strip())


def import_notebook(host: str, token: str, path: str, code: str,
                    overwrite: bool = True) -> DatabricksResult:
    """Import generated code into the workspace as a Python notebook."""
    if not _normalize_host(host):
        return DatabricksResult(False, "Workspace URL is not configured.")
    if not token:
        return DatabricksResult(False, "Access token is not configured.")
    if not path:
        return DatabricksResult(False, "Target notebook path is not configured.")

    notebook_source = to_notebook_source(code)
    encoded = base64.b64encode(notebook_source.encode("utf-8")).decode("ascii")

    payload = {
        "path": path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": overwrite,
    }
    status, body = _request(host, token, "/api/2.0/workspace/import",
                            payload=payload, method="POST")
    if status == 200:
        return DatabricksResult(True, f"Notebook imported to {path}")
    if status in (401, 403):
        return DatabricksResult(False, "Authentication failed (check the token).",
                                detail=str(body))
    if status == 0:
        return DatabricksResult(False, body.get("error_message", "Network error."),
                                detail=str(body))
    return DatabricksResult(
        False,
        body.get("message", f"Import failed (HTTP {status})."),
        detail=str(body),
    )
