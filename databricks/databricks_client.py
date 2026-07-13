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


def _notebook_runner_cell(code: str) -> str:
    """Build a Databricks-friendly runner cell that uses the notebook ``spark`` session."""
    if "def run_workflow(" not in code:
        run_funcs = re.findall(r"^def (run_[A-Za-z0-9_]+)\(", code, re.MULTILINE)
        if not run_funcs:
            return ""
        lines = [
            "# Run workflow",
            "# Uses the Databricks notebook ``spark`` session (serverless/all-purpose).",
            "",
        ]
        for func in run_funcs:
            trans_name = func[4:] if func.startswith("run_") else func
            result_var = f"result_{trans_name}"
            lines.append(f"print('Running transformation: {trans_name}')")
            lines.append(f"{result_var} = {func}(spark)")
        lines.append("print('Workflow completed successfully.')")
        return "\n".join(lines)

    return "\n".join([
        "# Run workflow",
        "# Uses the Databricks notebook ``spark`` session (serverless/all-purpose).",
        "# Do not call main() — it may create an invalid Spark Connect session.",
        "",
        "run_workflow(spark)",
    ])


def to_notebook_source(code: str) -> str:
    """Convert a flat generated .py file into Databricks notebook source format.

    Notebook layout:
      1. Imports / config (everything before the first ``def run_*``)
      2. One cell per ``run_*`` transformation function (keeps ``return`` valid)
      3. A runner cell that calls ``run_*(spark)`` using the platform session

    ``main()`` and ``if __name__`` are omitted from notebooks — they are for
    local ``python script.py`` execution only.
    """
    header = "# Databricks notebook source"
    cell_sep = "\n\n# COMMAND ----------\n\n"

    run_func_pattern = re.compile(r"(?m)^def run_[A-Za-z0-9_]+\(")
    main_func_pattern = re.compile(r"(?m)^def main\(\):")

    main_match = main_func_pattern.search(code)
    notebook_body = code[: main_match.start()].rstrip() if main_match else code.rstrip()

    parts: list[str] = []
    split_points = sorted(
        {0} | {m.start() for m in run_func_pattern.finditer(notebook_body)}
    )
    for i, start in enumerate(split_points):
        end = split_points[i + 1] if i + 1 < len(split_points) else len(notebook_body)
        chunk = notebook_body[start:end].rstrip()
        if chunk:
            parts.append(chunk)

    runner = _notebook_runner_cell(code)
    if runner:
        parts.append(runner)

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
