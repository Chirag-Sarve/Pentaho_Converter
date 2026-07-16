#!/usr/bin/env python3
"""Flask web application for Pentaho to PySpark conversion (Databricks)."""

from __future__ import annotations

import base64
import logging
import re
import traceback

from flask import Flask, jsonify, render_template, request

from databricks import settings as db_settings
from databricks.databricks_client import import_notebook, test_connection
from pentaho_converter.pipeline import convert_pentaho_project, package_files_as_zip

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50 MB
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_SIZE


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/convert", methods=["POST"])
def convert():
    """Accept a Pentaho project ZIP and return generated PySpark files."""
    if not request.content_type or "multipart/form-data" not in request.content_type:
        return jsonify({"error": "Upload a Pentaho project ZIP file (multipart/form-data)."}), 400

    uploaded = request.files.get("file")
    if not uploaded or not uploaded.filename:
        return jsonify({"error": "No ZIP file provided. Upload a Pentaho project ZIP."}), 400

    filename = uploaded.filename
    if not filename.lower().endswith(".zip"):
        return jsonify({"error": "Invalid file type. Only .zip files are accepted."}), 400

    zip_data = uploaded.read()
    if not zip_data:
        return jsonify({"error": "Uploaded file is empty."}), 400

    project_name = filename.rsplit(".", 1)[0]
    catalog = (request.form.get("catalog") or "").strip() or None
    schema = (request.form.get("schema") or "").strip() or None

    try:
        result = convert_pentaho_project(zip_data, project_name, catalog=catalog, schema=schema)
    except Exception as exc:
        logger.error("Conversion error: %s\n%s", exc, traceback.format_exc())
        return jsonify({"error": f"Conversion failed: {exc}"}), 500

    if not result.files:
        error_msg = next(
            (log for log in reversed(result.logs) if log.startswith("ERROR:")),
            "No PySpark files were generated. Check that the ZIP contains .kjb or .ktr files.",
        )
        return jsonify({"error": error_msg, "logs": result.logs}), 422

    zip_bytes = package_files_as_zip(result.files)
    primary_code = ""
    if result.main_workflow and result.main_workflow in result.files:
        primary_code = result.files[result.main_workflow]
    elif result.files:
        primary_code = next(iter(result.files.values()))

    return jsonify({
        "code": primary_code,
        "files": result.files,
        "logs": result.logs,
        "main_workflow": result.main_workflow,
        "zip_base64": base64.b64encode(zip_bytes).decode("ascii"),
        "summary": {
            "jobs_found": result.stats.jobs_found,
            "transformations_found": result.stats.transformations_found,
            "steps_converted": result.stats.steps_converted,
            "steps_approximated": result.stats.steps_approximated,
            "steps_skipped": result.stats.steps_skipped,
            "files_generated": len(result.files),
            "warnings": result.stats.warnings,
            "coverage_percent": round(result.stats.coverage_percent, 1),
            "semantic_accuracy_percent": round(result.stats.semantic_accuracy_percent, 1),
        },
        "analysis": _build_analysis(result),
        "project_inventory": result.project_inventory,
        "lineage": result.lineage,
        "code_navigation": result.code_navigation,
    })


def _build_analysis(result) -> dict:
    """Build a semantic conversion report for the UI."""
    from pentaho_converter.reporting import build_conversion_report

    report = build_conversion_report(result.stats)
    steps = result.stats.step_results
    converted = sum(1 for s in steps if s.status == "converted")
    partial = sum(1 for s in steps if s.status in ("partial", "partially_supported", "approximated"))
    failed = sum(1 for s in steps if s.status in ("failed", "unsupported", "skipped"))
    manual = sum(1 for s in steps if s.status == "manual_required")

    return {
        "accuracy_score": int(report.semantic_accuracy_percent),
        "coverage_score": int(report.coverage_percent),
        "semantic_accuracy_percent": round(report.semantic_accuracy_percent, 1),
        "coverage_percent": round(report.coverage_percent, 1),
        "fully_converted": converted,
        "approximated": partial,
        "unsupported": failed,
        "manual_required": manual,
        "missing_in_output": 0,
        "syntax_valid": all(not s.errors for s in steps) if steps else True,
        "todo_count": partial + failed + manual,
        "transformations": [
            {
                "name": s.step_name,
                "type": s.step_type,
                "status": s.status,
                "display_status": getattr(s, "display_status", "")
                or (
                    "CONVERTED WITH WARNINGS"
                    if s.status == "converted" and getattr(s, "warnings", None)
                    else (
                        "CONVERTED (Legacy metadata preserved)"
                        if s.status == "converted" and getattr(s, "infos", None)
                        else (
                            "CONVERTED"
                            if s.status == "converted"
                            else (
                                "PARTIAL"
                                if s.status in ("partial", "partially_supported", "approximated")
                                else (s.status or "").upper()
                            )
                        )
                    )
                ),
                "semantic_score": int(getattr(s, "semantic_score", 0) * 100),
                "has_logic": s.status == "converted",
                "detail": s.detail,
                "warnings": getattr(s, "warnings", []),
                "errors": getattr(s, "errors", []),
                "infos": getattr(s, "infos", []),
            }
            for s in steps
        ],
        "expression_diffs": [],
        "warnings": [
            {"severity": "warning", "message": w, "location": ""}
            for w in result.stats.warnings
        ],
    }


def _safe_segment(name: str) -> str:
    """Sanitize a string for use as a workspace path segment."""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", (name or "").strip())
    return cleaned or "converted_notebook"


def _notebook_path(notebook_dir: str, relative_name: str) -> str:
    """Build a workspace notebook path from folder + file key (may include subdirs)."""
    base = (notebook_dir or "/Shared/pentaho_converted").rstrip("/")
    parts = []
    for segment in relative_name.replace("\\", "/").split("/"):
        stem = segment.rsplit(".", 1)[0] if segment.endswith(".py") else segment
        safe = _safe_segment(stem)
        if safe:
            parts.append(safe)
    if not parts:
        parts = ["converted_notebook"]
    return f"{base}/{'/'.join(parts)}"


def _databricks_credentials(body: dict) -> tuple[str, str, str]:
    """Read Databricks connection settings from the JSON request body."""
    host = (body.get("host") or "").strip()
    token = (body.get("token") or "").strip()
    notebook_dir = (body.get("notebook_dir") or "/Shared/pentaho_converted").strip().rstrip("/")
    return host, token, notebook_dir


@app.route("/api/databricks/config", methods=["GET"])
def databricks_config():
    """Return non-secret default Databricks settings for the UI."""
    return jsonify(db_settings.public_config())


@app.route("/api/databricks/test", methods=["POST"])
def databricks_test():
    """Test Databricks connection using credentials from the request body."""
    body = request.get_json(silent=True) or {}
    host, token, _ = _databricks_credentials(body)
    if not host:
        return jsonify({"ok": False, "message": "Workspace URL is required."}), 400
    if not token:
        return jsonify({"ok": False, "message": "Personal access token is required."}), 400

    result = test_connection(host, token)
    return jsonify({"ok": result.ok, "message": result.message}), (200 if result.ok else 400)


@app.route("/api/databricks/push", methods=["POST"])
def databricks_push():
    """Import generated PySpark into Databricks as cell-split notebooks."""
    body = request.get_json(silent=True) or {}
    host, token, notebook_dir = _databricks_credentials(body)

    if not host or not token:
        return jsonify({
            "ok": False,
            "message": "Workspace URL and personal access token are required.",
        }), 400

    files: dict[str, str] = {}
    if body.get("files") and isinstance(body["files"], dict):
        files = {k: v for k, v in body["files"].items() if isinstance(v, str) and v.strip()}
    elif body.get("code", "").strip():
        name = body.get("name", "converted_notebook")
        files = {name: body["code"]}

    if not files:
        return jsonify({"ok": False, "message": "No generated code to push."}), 400

    imported: list[str] = []
    errors: list[str] = []
    for relative_name, code in files.items():
        path = _notebook_path(notebook_dir, relative_name)
        result = import_notebook(host, token, path, code, overwrite=True)
        if result.ok:
            imported.append(path)
        else:
            errors.append(f"{relative_name}: {result.message}")

    if errors and not imported:
        return jsonify({"ok": False, "message": errors[0], "errors": errors}), 400

    message = f"Imported {len(imported)} notebook(s) to Databricks."
    if errors:
        message += f" {len(errors)} file(s) failed."
    return jsonify({
        "ok": True,
        "message": message,
        "paths": imported,
        "path": imported[0] if len(imported) == 1 else None,
        "errors": errors,
    })


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
