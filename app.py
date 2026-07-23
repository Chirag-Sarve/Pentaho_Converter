#!/usr/bin/env python3
"""Flask web application for Pentaho to PySpark conversion (Databricks)."""

from __future__ import annotations

import base64
import logging
import traceback

from flask import Flask, jsonify, render_template, request

from databricks import settings as db_settings
from databricks.databricks_client import deploy_project, test_connection
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
    data_dir = (request.form.get("data_dir") or "").strip() or None

    try:
        result = convert_pentaho_project(
            zip_data,
            project_name,
            catalog=catalog,
            schema=schema,
            data_dir=data_dir,
        )
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
    """Build a semantic conversion + assessment report for the UI.

    Assessment fields are reporting/navigation metadata only — they do not
    change conversion, generated PySpark, or execution behaviour.
    """
    from pentaho_converter.assessment_report import build_assessment_payload
    from pentaho_converter.reporting import build_conversion_report

    report = build_conversion_report(result.stats)
    steps = result.stats.step_results
    accuracy = int(report.semantic_accuracy_percent)
    assessment = build_assessment_payload(
        steps,
        code_navigation=getattr(result, "code_navigation", None) or {},
        accuracy_score=accuracy,
    )
    summary = assessment["assessment_summary"]
    transformations = assessment["transformations"]

    return {
        "accuracy_score": accuracy,
        "coverage_score": int(report.coverage_percent),
        "semantic_accuracy_percent": round(report.semantic_accuracy_percent, 1),
        "coverage_percent": round(report.coverage_percent, 1),
        "fully_converted": summary["fully_converted"],
        "approximated": summary["partially_converted"],
        "unsupported": sum(
            1 for s in transformations
            if (s.get("status") or "") in ("failed", "unsupported", "skipped")
        ),
        "manual_required": summary["manual_required"],
        "partially_converted": summary["partially_converted"],
        "missing_in_output": 0,
        "syntax_valid": all(not s.errors for s in steps) if steps else True,
        "todo_count": summary["partially_converted"] + summary["manual_required"],
        "assessment_summary": summary,
        "transformations": transformations,
        "expression_diffs": [],
        "warnings": [
            {"severity": "warning", "message": w, "location": ""}
            for w in result.stats.warnings
        ],
    }


def _databricks_credentials(body: dict) -> tuple[str, str, str]:
    """Read Databricks connection settings from the JSON request body.

    Only Workspace URL and PAT are required for Free Edition upload-only deploy.
    Cluster ID is intentionally ignored.
    """
    host = (body.get("host") or "").strip()
    token = (body.get("token") or "").strip()
    notebook_dir = (
        body.get("notebook_dir") or "/Workspace/Pentaho_Migration"
    ).strip().rstrip("/")
    return host, token, notebook_dir


@app.route("/api/databricks/config", methods=["GET"])
def databricks_config():
    """Return non-secret default Databricks settings for the UI."""
    return jsonify(db_settings.public_config())


@app.route("/api/databricks/test", methods=["POST"])
def databricks_test():
    """Test Workspace URL + PAT only (no Cluster ID)."""
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
    """Upload-only deploy: create folders and import generated project files.

    Never executes notebooks or submits Spark jobs. Cluster ID is not required.
    """
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
        if not str(name).endswith(".py"):
            name = f"{name}.py"
        files = {name: body["code"]}

    if not files:
        return jsonify({"ok": False, "message": "No generated code to upload."}), 400

    project_name = (body.get("project_name") or body.get("name") or "").strip()
    result = deploy_project(
        host,
        token,
        files,
        base_dir=notebook_dir,
        project_name=project_name,
    )

    error_lines = [
        f"{item.get('file', '?')}: {item.get('error', 'upload failed')}"
        for item in result.failed
    ]

    if not result.ok and not result.uploaded:
        return jsonify({
            "ok": False,
            "message": result.message,
            "location": result.location,
            "uploaded": result.uploaded,
            "uploaded_count": result.uploaded_count,
            "failed_count": result.failed_count,
            "errors": error_lines,
            "failed": result.failed,
        }), 400

    success_title = "Deployment Successful"
    success_body = (
        "Your PySpark project has been uploaded successfully.\n\n"
        f"Location:\n{result.location}/\n\n"
        "Next Steps\n"
        "1. Open Databricks Workspace\n"
        "2. Navigate to the uploaded project\n"
        "3. Open Master_ETL.py (or the main notebook)\n"
        "4. Run it manually using Databricks serverless compute."
    )
    if result.failed:
        success_body = (
            f"Uploaded: {result.uploaded_count} files\n"
            f"Failed: {result.failed_count} files\n\n"
            + "\n".join(error_lines)
            + f"\n\nLocation:\n{result.location}/"
        )

    return jsonify({
        "ok": True,
        "message": result.message if result.failed else f"{success_title}\n\n{success_body}",
        "title": success_title,
        "location": result.location,
        "paths": result.uploaded,
        "path": result.location,
        "uploaded": result.uploaded,
        "uploaded_count": result.uploaded_count,
        "failed_count": result.failed_count,
        "errors": error_lines,
        "failed": result.failed,
    })


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
