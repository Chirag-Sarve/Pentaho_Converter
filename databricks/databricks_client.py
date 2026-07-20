"""Minimal Databricks REST client for upload-only workspace deployment.

Compatible with Databricks Free Edition:

  * test_connection()  -- verify Workspace URL + PAT (no cluster check).
  * mkdirs()           -- create destination folders if missing.
  * upload_file()      -- import one generated file into the workspace.
  * deploy_project()   -- create folders + upload every file (never executes).

Deployment never calls Jobs, Runs Submit, or Commands APIs.
"""

from __future__ import annotations

import base64
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

_TIMEOUT_SECONDS = 60
_DEFAULT_BASE = "/Workspace/Pentaho_Migration"


@dataclass
class DatabricksResult:
    ok: bool
    message: str
    detail: str = ""


@dataclass
class DeployResult:
    """Outcome of an upload-only project deployment."""

    ok: bool
    message: str
    location: str = ""
    uploaded: list[str] = field(default_factory=list)
    failed: list[dict[str, str]] = field(default_factory=list)

    @property
    def uploaded_count(self) -> int:
        return len(self.uploaded)

    @property
    def failed_count(self) -> int:
        return len(self.failed)


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


def _auth_error_result(status: int, body: dict) -> DatabricksResult | None:
    if status in (401, 403):
        return DatabricksResult(
            False,
            "Authentication failed (check the token).",
            detail=str(body),
        )
    if status == 0:
        return DatabricksResult(
            False,
            body.get("error_message", "Network error."),
            detail=str(body),
        )
    return None


def test_connection(host: str, token: str) -> DatabricksResult:
    """Verify Workspace URL + PAT only (no Cluster ID check).

    Uses the current-user SCIM endpoint so Free Edition workspaces that do not
    expose cluster APIs still authenticate successfully.
    """
    if not _normalize_host(host):
        return DatabricksResult(False, "Workspace URL is not configured.")
    if not token:
        return DatabricksResult(False, "Access token is not configured.")

    status, body = _request(host, token, "/api/2.0/preview/scim/v2/Me")
    if status == 200:
        return DatabricksResult(True, "Connected successfully.")

    # Fallback for workspaces where SCIM preview is unavailable.
    if status in (404, 405, 501):
        list_path = "/api/2.0/workspace/list?" + urllib.parse.urlencode({"path": "/"})
        status, body = _request(host, token, list_path)

    if status == 200:
        return DatabricksResult(True, "Connected successfully.")

    err = _auth_error_result(status, body)
    if err:
        return err
    return DatabricksResult(
        False,
        f"Unexpected response from Databricks (HTTP {status}).",
        detail=str(body),
    )


def mkdirs(host: str, token: str, path: str) -> DatabricksResult:
    """Create ``path`` and any missing parent folders in the workspace."""
    if not _normalize_host(host):
        return DatabricksResult(False, "Workspace URL is not configured.")
    if not token:
        return DatabricksResult(False, "Access token is not configured.")
    clean = (path or "").strip().rstrip("/")
    if not clean:
        return DatabricksResult(False, "Destination folder path is required.")

    status, body = _request(
        host, token, "/api/2.0/workspace/mkdirs",
        payload={"path": clean}, method="POST",
    )
    if status == 200:
        return DatabricksResult(True, f"Folder ready: {clean}")
    err = _auth_error_result(status, body)
    if err:
        return err
    return DatabricksResult(
        False,
        body.get("message", f"Could not create folder (HTTP {status})."),
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


def _safe_segment(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", (name or "").strip())
    return cleaned or "file"


def _is_entry_notebook(relative_name: str) -> bool:
    """True for the runnable entry notebook (Master_ETL); modules stay as files."""
    name = relative_name.replace("\\", "/").rsplit("/", 1)[-1].lower()
    return name in {"master_etl.py", "master_etl"}


def _strip_package_prefix(relative_name: str, project_name: str) -> str:
    """Avoid ``project/project/...`` when generated keys already include the package root."""
    rel = relative_name.replace("\\", "/").lstrip("/")
    proj = _safe_segment(project_name) if project_name else ""
    if not proj:
        # Common layout: PackageName/Master_ETL.py → strip first segment for deploy root
        parts = rel.split("/")
        if len(parts) > 1 and parts[0]:
            # Keep as-is when no project_name; caller sets location from project_name
            return rel
        return rel
    if rel == proj:
        return ""
    prefix = proj + "/"
    if rel.startswith(prefix):
        return rel[len(prefix):]
    # Also strip if first path segment matches project (case-insensitive)
    parts = rel.split("/", 1)
    if len(parts) == 2 and _safe_segment(parts[0]).lower() == proj.lower():
        return parts[1]
    return rel


def workspace_object_path(base_dir: str, relative_name: str, *, as_file: bool = False) -> str:
    """Build a workspace path that preserves folder structure under ``base_dir``.

    Notebooks omit the ``.py`` extension. Workspace files keep it so
    ``import config`` resolves to ``config.py``.
    """
    base = (base_dir or _DEFAULT_BASE).rstrip("/") or _DEFAULT_BASE
    parts: list[str] = []
    for segment in relative_name.replace("\\", "/").split("/"):
        if not segment or segment in (".", ".."):
            continue
        if as_file:
            safe = _safe_segment(segment)
        else:
            stem = segment[:-3] if segment.lower().endswith(".py") else segment
            safe = _safe_segment(stem)
        if safe:
            parts.append(safe)
    if not parts:
        parts = ["converted_notebook" if not as_file else "module.py"]
    return f"{base}/{'/'.join(parts)}"


def _parent_dirs(path: str) -> list[str]:
    """Return parent directory paths from root-most to immediate parent."""
    clean = (path or "").strip().rstrip("/")
    if not clean or "/" not in clean:
        return []
    parts = [p for p in clean.split("/") if p]
    if len(parts) < 2:
        return []
    absolute = clean.startswith("/")
    parents: list[str] = []
    for i in range(1, len(parts)):
        joined = "/".join(parts[:i])
        parents.append("/" + joined if absolute else joined)
    return parents


def upload_file(host: str, token: str, path: str, content: str,
                overwrite: bool = True) -> DatabricksResult:
    """Upload a workspace *file* (not a notebook) so Python ``import`` works.

    Uses format AUTO and keeps the file extension. Content must NOT start with
    ``# Databricks notebook source`` or Databricks will treat it as a notebook.
    """
    if not _normalize_host(host):
        return DatabricksResult(False, "Workspace URL is not configured.")
    if not token:
        return DatabricksResult(False, "Access token is not configured.")
    if not path:
        return DatabricksResult(False, "Target path is not configured.")
    if content is None:
        return DatabricksResult(False, "File content is empty.")

    # Strip notebook marker if present so AUTO imports as a file.
    body = content
    if body.lstrip().startswith("# Databricks notebook source"):
        lines = body.splitlines()
        if lines and "Databricks notebook source" in lines[0]:
            body = "\n".join(lines[1:]).lstrip("\n")

    file_path = path if path.lower().endswith((".py", ".txt", ".md", ".json", ".yml", ".yaml")) or "." in path.rsplit("/", 1)[-1] else path
    encoded = base64.b64encode(body.encode("utf-8")).decode("ascii")
    payload = {
        "path": file_path,
        "format": "AUTO",
        "content": encoded,
        "overwrite": overwrite,
    }
    status, body_json = _request(host, token, "/api/2.0/workspace/import",
                                 payload=payload, method="POST")
    if status == 200:
        return DatabricksResult(True, f"Uploaded file to {file_path}")
    err = _auth_error_result(status, body_json)
    if err:
        return err
    return DatabricksResult(
        False,
        body_json.get("message", f"Upload failed (HTTP {status})."),
        detail=str(body_json),
    )


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
    notebook_path = path[:-3] if path.lower().endswith(".py") else path

    payload = {
        "path": notebook_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": overwrite,
    }
    status, body = _request(host, token, "/api/2.0/workspace/import",
                            payload=payload, method="POST")
    if status == 200:
        return DatabricksResult(True, f"Notebook imported to {notebook_path}")
    err = _auth_error_result(status, body)
    if err:
        return err
    return DatabricksResult(
        False,
        body.get("message", f"Import failed (HTTP {status})."),
        detail=str(body),
    )


def deploy_project(
    host: str,
    token: str,
    files: dict[str, str],
    *,
    base_dir: str = _DEFAULT_BASE,
    project_name: str = "",
) -> DeployResult:
    """Create destination folders and upload every generated file.

    Upload-only: does not execute notebooks, submit jobs, or run commands.

    ``Master_ETL.py`` is imported as a runnable notebook. All other modules
    (``config.py``, ``jobs/``, ``engine/``, etc.) are uploaded as workspace
    *files* so ``import config`` works in Databricks.
    """
    if not _normalize_host(host):
        return DeployResult(False, "Workspace URL is not configured.")
    if not token:
        return DeployResult(False, "Access token is not configured.")
    if not files:
        return DeployResult(False, "No generated code to upload.")

    root = (base_dir or _DEFAULT_BASE).rstrip("/") or _DEFAULT_BASE
    project = _safe_segment(project_name) if project_name else ""

    # Infer project folder from generated keys when not provided
    if not project:
        for key in files:
            parts = key.replace("\\", "/").split("/")
            if len(parts) > 1 and parts[0]:
                project = _safe_segment(parts[0])
                break

    location = f"{root}/{project}" if project else root

    dirs_needed: set[str] = {root, location}
    target_paths: list[tuple[str, str, str, bool]] = []  # relative, ws_path, content, as_notebook

    for relative_name, content in files.items():
        if not isinstance(content, str):
            continue
        rel = _strip_package_prefix(relative_name, project)
        if not rel:
            continue
        as_notebook = _is_entry_notebook(rel)
        ws_path = workspace_object_path(location, rel, as_file=not as_notebook)
        target_paths.append((relative_name, ws_path, content, as_notebook))
        for parent in _parent_dirs(ws_path):
            if parent and parent != "/":
                dirs_needed.add(parent)

    for folder in sorted(dirs_needed, key=lambda p: (p.count("/"), p)):
        result = mkdirs(host, token, folder)
        if not result.ok:
            return DeployResult(
                False,
                f"Could not create destination folder {folder}: {result.message}",
                location=location,
                failed=[{"file": folder, "error": result.message}],
            )

    uploaded: list[str] = []
    failed: list[dict[str, str]] = []

    for relative_name, ws_path, content, as_notebook in target_paths:
        if as_notebook:
            result = import_notebook(host, token, ws_path, content, overwrite=True)
        else:
            result = upload_file(host, token, ws_path, content, overwrite=True)
        if result.ok:
            uploaded.append(ws_path)
        else:
            failed.append({"file": relative_name, "error": result.message})

    if not uploaded and failed:
        return DeployResult(
            False,
            failed[0]["error"],
            location=location,
            uploaded=uploaded,
            failed=failed,
        )

    if failed:
        message = (
            f"Uploaded: {len(uploaded)} files\n"
            f"Failed: {len(failed)} files"
        )
        return DeployResult(
            True,
            message,
            location=location,
            uploaded=uploaded,
            failed=failed,
        )

    message = (
        "Deployment completed successfully.\n\n"
        "Project uploaded to Databricks Workspace.\n\n"
        "Open Databricks and run Master_ETL.py manually."
    )
    return DeployResult(
        True,
        message,
        location=location,
        uploaded=uploaded,
        failed=failed,
    )
