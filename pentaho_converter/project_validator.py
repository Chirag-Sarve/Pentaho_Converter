"""Post-generation validation for Databricks multi-file projects."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .models import PentahoJob, PentahoTransformation, ScanResult
from .naming import safe_module_name, safe_package_root


@dataclass
class ValidationIssue:
    severity: str  # error | warning | info
    code: str
    message: str


@dataclass
class ProjectValidationReport:
    ok: bool = True
    issues: list[ValidationIssue] = field(default_factory=list)
    checked_jobs: int = 0
    checked_transformations: int = 0
    checked_steps: int = 0
    checked_hops: int = 0

    def add(self, severity: str, code: str, message: str) -> None:
        self.issues.append(ValidationIssue(severity, code, message))
        if severity == "error":
            self.ok = False

    def to_log_lines(self) -> list[str]:
        lines = [
            "Project validation started.",
            f"  Jobs checked           : {self.checked_jobs}",
            f"  Transformations checked: {self.checked_transformations}",
            f"  Steps checked          : {self.checked_steps}",
            f"  Hops checked           : {self.checked_hops}",
            f"  Issues                 : {len(self.issues)}",
        ]
        for issue in self.issues:
            lines.append(f"  [{issue.severity.upper()}] {issue.code}: {issue.message}")
        lines.append(
            "Project validation "
            + ("PASSED." if self.ok else "FAILED — see issues above.")
        )
        return lines

    def to_markdown(self) -> str:
        lines = [
            "# Generated Project Validation Report",
            "",
            f"**Status:** {'PASSED' if self.ok else 'FAILED'}",
            "",
            f"- Jobs checked: {self.checked_jobs}",
            f"- Transformations checked: {self.checked_transformations}",
            f"- Steps checked: {self.checked_steps}",
            f"- Hops checked: {self.checked_hops}",
            "",
            "## Issues",
            "",
        ]
        if not self.issues:
            lines.append("None.")
        else:
            for issue in self.issues:
                lines.append(f"- **{issue.severity}** `{issue.code}`: {issue.message}")
        lines.append("")
        return "\n".join(lines)


def validate_generated_project(
    *,
    project_name: str,
    files: dict[str, str],
    scan: ScanResult,
    jobs: dict[str, PentahoJob],
    transformations: dict[str, PentahoTransformation],
    step_statuses: dict[tuple[str, str], str] | None = None,
) -> ProjectValidationReport:
    """Validate that every Pentaho asset is represented in the jobs/ + engine/ layout."""
    report = ProjectValidationReport()
    root = safe_package_root(project_name)
    prefix = f"{root}/"
    file_set = set(files)

    # Required scaffolding (jobs/ + shared engine/, no transformations/ or utils/)
    for required in (
        f"{root}/Master_ETL.py",
        f"{root}/config.py",
        f"{root}/requirements.txt",
        f"{root}/jobs/__init__.py",
        f"{root}/engine/__init__.py",
        f"{root}/engine/runtime.py",
        f"{root}/engine/handlers.py",
        f"{root}/engine/mail_ops.py",
        f"{root}/engine/job_runtime.py",
        f"{root}/engine/job_models.py",
        f"{root}/engine/job_specs.py",
        f"{root}/engine/variables.py",
    ):
        if required not in file_set:
            report.add("error", "MISSING_SCAFFOLD", f"Required file missing: {required}")

    # Forbidden legacy packages
    for path in file_set:
        norm = path.replace("\\", "/")
        if "/transformations/" in norm or "/utils/" in norm:
            report.add(
                "error",
                "LEGACY_LAYOUT",
                f"Unexpected legacy package file (should be inlined): {path}",
            )

    # Every .kjb → jobs/<module>.py with inlined transformations
    for job_path in scan.job_files:
        job = jobs.get(str(job_path))
        stem = safe_module_name(job.file_path.stem if job else job_path.stem)
        rel = f"{root}/jobs/{stem}.py"
        report.checked_jobs += 1
        if rel not in file_set:
            report.add("error", "MISSING_JOB_MODULE", f"No module for job {job_path.name} → {rel}")
            continue
        content = files[rel]
        if "def run(" not in content:
            report.add("error", "MISSING_RUN", f"Job module {rel} lacks def run(...)")
        if "execute_registered_job" not in content:
            report.add(
                "error",
                "MISSING_ENGINE_CALL",
                f"Job module {rel} does not call engine.runtime.execute_registered_job",
            )
        if "class JobRuntime" in content:
            report.add(
                "error",
                "EMBEDDED_ENGINE",
                f"Job module {rel} still embeds JobRuntime — use shared engine/",
            )
        if job:
            for hop in job.hops:
                report.checked_hops += 1
            specs_path = f"{root}/engine/job_specs.py"
            specs_content = files.get(specs_path, "")
            if job.hops and job.name not in specs_content:
                report.add(
                    "error",
                    "MISSING_HOPS",
                    f"Engine job registry lacks metadata for {len(job.hops)} hops in {rel}",
                )
            for entry in job.entries:
                if entry.name and entry.name not in content and entry.name not in specs_content:
                    report.add(
                        "warning",
                        "ENTRY_NOT_FOUND",
                        f"Entry '{entry.name}' may be missing from {rel} and engine registry",
                    )
                et = (entry.entry_type or "").upper()
                if et == "TRANS":
                    ref = entry.transname or Path(
                        (entry.filename or "").replace("\\", "/")
                    ).stem
                    if ref:
                        tstem = safe_module_name(ref)
                        has_runner = f"run_{tstem}" in content
                        has_ktr_marker = (
                            f"Original KTR : {ref}" in content
                            or f"{ref}.ktr" in content
                            or f"Transformation : {ref}" in content
                        )
                        if not has_runner and not has_ktr_marker:
                            report.add(
                                "error",
                                "MISSING_INLINED_TRANS",
                                f"Job {stem} TRANS '{entry.name}' → expected inlined "
                                f"run_{tstem} / KTR markers in {rel}",
                            )
                elif et == "JOB":
                    ref = entry.jobname or Path(
                        (entry.filename or "").replace("\\", "/")
                    ).stem
                    if ref:
                        jmod = safe_module_name(ref)
                        jpath = f"{root}/jobs/{jmod}.py"
                        if jpath not in file_set:
                            report.add(
                                "error",
                                "MISSING_JOB_REF",
                                f"Job {stem} JOB '{entry.name}' → missing {jpath}",
                            )

    # Every .ktr referenced by a job must be inlined; orphans are warnings only
    referenced_ktr_stems: set[str] = set()
    for job in jobs.values():
        for entry in job.entries:
            if (entry.entry_type or "").upper() != "TRANS":
                continue
            ref = entry.transname or Path(
                (entry.filename or "").replace("\\", "/")
            ).stem
            if ref:
                referenced_ktr_stems.add(safe_module_name(ref).lower())

    all_job_contents = "\n".join(
        content
        for path, content in files.items()
        if "/jobs/" in path.replace("\\", "/") and path.endswith(".py")
    )
    for ktr_path in scan.transformation_files:
        trans = transformations.get(str(ktr_path))
        stem = safe_module_name(trans.file_path.stem if trans else ktr_path.stem)
        report.checked_transformations += 1
        ktr_name = ktr_path.name
        is_referenced = stem.lower() in referenced_ktr_stems or (
            trans is not None
            and safe_module_name(trans.name).lower() in referenced_ktr_stems
        )
        found = (
            f"run_{stem}" in all_job_contents
            or ktr_name in all_job_contents
            or f"Original KTR : {ktr_name}" in all_job_contents
            or f"{root}/jobs/{stem}.py" in file_set
        )
        if not found:
            if is_referenced or not jobs:
                report.add(
                    "error",
                    "MISSING_INLINED_TRANS",
                    f"No inlined code for transformation {ktr_name} (expected run_{stem})",
                )
            else:
                report.add(
                    "warning",
                    "ORPHAN_TRANS",
                    f"Transformation {ktr_name} is not referenced by any job "
                    "(not emitted as a separate Python file)",
                )
            continue
        content_for_steps = all_job_contents
        job_file = f"{root}/jobs/{stem}.py"
        if job_file in files:
            content_for_steps = files[job_file]
        else:
            for path, content in files.items():
                if ktr_name in content and "/jobs/" in path.replace("\\", "/"):
                    content_for_steps = content
                    break
        if trans:
            for step in trans.steps:
                report.checked_steps += 1
                marker = f"# Step: {step.name}"
                if (
                    marker not in content_for_steps
                    and f"TODO: unsupported step '{step.name}'" not in content_for_steps
                ):
                    if step.name not in content_for_steps:
                        report.add(
                            "warning",
                            "STEP_NOT_EMITTED",
                            f"Step '{step.name}' ({step.step_type}) not found for {ktr_name}",
                        )
            for _hop in trans.hops:
                report.checked_hops += 1

    master = f"{root}/Master_ETL.py"
    if master in files:
        if "def run(" not in files[master] and "def main(" not in files[master]:
            report.add("error", "MASTER_NO_RUN", "Master_ETL.py lacks def run(...)/main(...)")

    if step_statuses:
        for (trans_name, step_name), status in step_statuses.items():
            if status in ("unsupported", "failed", "skipped", "manual_required"):
                report.add(
                    "warning",
                    "STEP_TODO",
                    f"Transformation '{trans_name}' step '{step_name}' status={status}",
                )

    for path in files:
        if not path.startswith(prefix) and path not in {
            f"{root}/VALIDATION_REPORT.md",
            f"{root}/CODE_NAVIGATION.json",
        }:
            report.add("info", "EXTRA_FILE", f"File outside package root: {path}")

    return report
