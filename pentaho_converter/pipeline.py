"""Orchestration pipeline for Pentaho project conversion."""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

from .code_generator import PySparkCodeGenerator, _safe_filename
from .dependency_resolver import resolve_project
from .extractor import ZipExtractionError, cleanup_workspace, extract_zip_to_workspace
from .generation_config import GenerationConfig
from .job_parser import parse_job
from .models import ConversionResult, ConversionStats, PentahoJob, PentahoTransformation
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
) -> ConversionResult:
    """Convert a Pentaho project ZIP into PySpark modules.

    Parameters
    ----------
    zip_data:
        Raw bytes of the uploaded ZIP file.
    project_name:
        Label used in logs and output filenames.

    Returns
    -------
    ConversionResult
        Generated files, logs, and statistics.
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
                catalog=(catalog or gen_cfg.catalog or "main").strip(),
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
