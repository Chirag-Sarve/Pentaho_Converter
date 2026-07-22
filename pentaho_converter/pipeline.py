"""Orchestration pipeline for Pentaho project conversion."""

from __future__ import annotations

import io
import json
import logging
import zipfile
from pathlib import Path

from .code_generator import PySparkCodeGenerator, _safe_filename
from .dependency_resolver import resolve_project
from .extractor import ZipExtractionError, cleanup_workspace, extract_zip_to_workspace
from .generation_config import GenerationConfig
from .job_parser import parse_job
from .models import ConversionResult, ConversionStats, PentahoJob, PentahoTransformation
from .naming import safe_package_root
from .project_generator import DatabricksProjectGenerator
from .project_validator import validate_generated_project
from .scanner import scan_project
from .transformation_parser import parse_transformation

logger = logging.getLogger(__name__)


def convert_pentaho_project(
    zip_data: bytes,
    project_name: str = "project",
    *,
    catalog: str | None = None,
    schema: str | None = None,
    data_dir: str | None = None,
    single_file: bool = False,
) -> ConversionResult:
    """Convert a Pentaho project ZIP into a Databricks-ready Python project.

    By default emits a multi-file package::

        <Project>/
          Master_ETL.py
          config.py
          requirements.txt
          VALIDATION_REPORT.md
          jobs/          # one .py per .kjb; transformations inlined as step functions

    Pass ``single_file=True`` to keep the legacy combined ``.py`` output.
    """
    result = ConversionResult()
    logs = result.logs
    stats = result.stats
    workspace: Path | None = None

    logs.append(f"ZIP uploaded: {project_name}")
    logs.append("Parsing started.")

    try:
        workspace = extract_zip_to_workspace(zip_data, logs)
        scan = scan_project(workspace, logs)
        stats.jobs_found = len(scan.job_files)
        stats.transformations_found = len(scan.transformation_files)

        if not scan.job_files and not scan.transformation_files:
            logs.append("ERROR: No .kjb or .ktr files found in the project.")
            result.files = {}
            return result

        jobs: dict[str, PentahoJob] = {}
        for job_path in scan.job_files:
            try:
                job = parse_job(job_path, logs)
                jobs[str(job_path)] = job
            except ValueError as exc:
                stats.warnings.append(str(exc))
                logs.append(f"ERROR: {exc}")

        transformations: dict[str, PentahoTransformation] = {}
        for ktr_path in scan.transformation_files:
            try:
                trans = parse_transformation(ktr_path, logs)
                transformations[str(ktr_path)] = trans
            except ValueError as exc:
                stats.warnings.append(str(exc))
                logs.append(f"ERROR: {exc}")

        logs.append("Parsing completed.")

        ordered, primary_job_name = resolve_project(scan, jobs, transformations, logs)

        gen_cfg = GenerationConfig.defaults()
        if catalog or schema or data_dir:
            gen_cfg = GenerationConfig(
                catalog=(catalog or gen_cfg.catalog or "workspace").strip(),
                schema=(schema or gen_cfg.schema or "default").strip(),
                data_dir=(data_dir or gen_cfg.data_dir).strip(),
            )
        generator = PySparkCodeGenerator(generation_config=gen_cfg)

        primary_job = None
        if primary_job_name:
            for job in jobs.values():
                if job.name == primary_job_name:
                    primary_job = job
                    break

        if single_file:
            if ordered:
                output_name = f"{_safe_filename(project_name)}.py"
                single_code = generator.generate_single_file(
                    ordered,
                    stats,
                    logs,
                    job=primary_job,
                    project_name=project_name,
                )
                output_files = {output_name: single_code}
                result.main_workflow = output_name
                logs.append(f"Generated single PySpark file: {output_name}")
            else:
                output_files = {}
        else:
            project_gen = DatabricksProjectGenerator(
                generation_config=gen_cfg,
                code_generator=generator,
            )
            output_files = project_gen.generate(
                project_name=project_name,
                scan=scan,
                jobs=jobs,
                transformations=transformations,
                ordered_transformations=ordered,
                primary_job_name=primary_job_name,
                stats=stats,
                logs=logs,
            )
            root = safe_package_root(project_name)
            result.main_workflow = f"{root}/Master_ETL.py"

            step_status_map = {
                (
                    (
                        sr.detail.split("Transformation: ")[-1]
                        if "Transformation:" in (sr.detail or "")
                        else ""
                    ),
                    sr.step_name,
                ): sr.status
                for sr in stats.step_results
            }
            report = validate_generated_project(
                project_name=project_name,
                files=output_files,
                scan=scan,
                jobs=jobs,
                transformations=transformations,
                step_statuses=step_status_map,
            )
            for line in report.to_log_lines():
                logs.append(line)
            output_files[f"{root}/VALIDATION_REPORT.md"] = report.to_markdown()
            if not report.ok:
                for issue in report.issues:
                    if issue.severity == "error":
                        stats.warnings.append(f"{issue.code}: {issue.message}")

        logs.append("PySpark generation completed.")
        logs.append("Conversion finished.")

        result.files = output_files
        result.stats = stats
        from .reporting import build_conversion_report
        report = build_conversion_report(stats)
        stats.coverage_percent = report.coverage_percent
        stats.semantic_accuracy_percent = report.semantic_accuracy_percent
        logs.append(
            f"Conversion coverage: {report.coverage_percent:.1f}% | "
            f"Semantic accuracy: {report.semantic_accuracy_percent:.1f}%"
        )

        from .project_metadata import build_project_metadata

        inventory, lineage = build_project_metadata(
            scan,
            jobs,
            transformations,
            stats,
            main_workflow=result.main_workflow,
            primary_job_name=primary_job_name,
        )
        result.project_inventory = inventory
        result.lineage = lineage

        from .code_navigation import (
            build_project_code_navigation,
            build_step_to_file,
            enrich_step_results_with_navigation,
            navigation_report_payload,
        )

        # Index all generated job modules once; persist locations for the UI.
        result.code_navigation = build_project_code_navigation(
            output_files,
            lineage,
            inventory,
            step_to_file=build_step_to_file(transformations),
            step_results=stats.step_results,
            main_workflow=result.main_workflow or "",
        )
        enrich_step_results_with_navigation(
            stats.step_results,
            result.code_navigation,
            files=output_files,
        )

        root = safe_package_root(project_name)
        nav_path = f"{root}/CODE_NAVIGATION.json"
        payload = {
            "step_locations": navigation_report_payload(result.code_navigation),
            "indexed_files": result.code_navigation.get("indexed_files") or [],
            "steps_by_name": {
                key: value
                for key, value in (result.code_navigation.get("steps_by_name") or {}).items()
                if "\x00" not in key
            },
        }
        output_files[nav_path] = json.dumps(payload, indent=2)
        result.files = output_files
        logs.append(f"Wrote step navigation metadata: {nav_path}")

    except ZipExtractionError as exc:
        logs.append(f"ERROR: {exc}")
        stats.warnings.append(str(exc))
    except Exception as exc:
        logger.exception("Unexpected conversion error")
        logs.append(f"ERROR: Conversion failed: {exc}")
        stats.warnings.append(str(exc))
    finally:
        if workspace:
            cleanup_workspace(workspace)
            logs.append("Temporary workspace cleaned up.")

    return result


def package_files_as_zip(files: dict[str, str]) -> bytes:
    """Package generated files into a downloadable ZIP archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, content in files.items():
            zf.writestr(path, content)
    return buf.getvalue()
