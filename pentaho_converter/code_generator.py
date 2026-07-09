"""PySpark code generation for Pentaho transformations and jobs."""

from __future__ import annotations

import re
from datetime import datetime, timezone

from .graph import StepDAG
from .models import (
    ConversionStats,
    PentahoJob,
    PentahoTransformation,
    StepConversionResult,
)
from .step_context import StepContext
from .steps.base import StepRegistry, build_default_registry


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", name.strip())
    return cleaned or "transformation"


def _safe_func_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name.strip())
    if cleaned and cleaned[0].isdigit():
        cleaned = f"trans_{cleaned}"
    return cleaned or "transformation"


class PySparkCodeGenerator:
    """Generates PySpark modules from parsed Pentaho transformations."""

    def __init__(self, registry: StepRegistry | None = None) -> None:
        self.registry = registry or build_default_registry()

    def generate_transformation(
        self,
        transformation: PentahoTransformation,
        stats: ConversionStats,
        logs: list[str],
    ) -> str:
        """Generate a standalone PySpark module for one transformation."""
        lines: list[str] = []
        lines.extend(self._file_header(transformation.name, transformation.file_path.name))
        lines.extend(
            self._transformation_function_lines(transformation, stats, logs)
        )
        return "\n".join(lines)

    def generate_single_file(
        self,
        ordered_transformations: list[PentahoTransformation],
        stats: ConversionStats,
        logs: list[str],
        job: PentahoJob | None = None,
        project_name: str = "project",
    ) -> str:
        """Generate one PySpark module containing all transformations and the workflow."""
        lines: list[str] = []
        source = job.file_path.name if job else "project"
        title = f"Workflow: {job.name}" if job else f"Project: {project_name}"
        lines.extend(self._file_header(title, source))
        lines.append('"""')
        if job:
            lines.append(f"Main workflow for Pentaho job: {job.name}")
        else:
            lines.append(f"Combined PySpark workflow for Pentaho project: {project_name}")
        lines.append("All transformations and orchestration in a single notebook-ready module.")
        lines.append('"""')
        lines.append("")

        for trans in ordered_transformations:
            lines.append(f"# {'=' * 60}")
            lines.append(f"# Transformation: {trans.name}")
            lines.append(f"# Source: {trans.file_path.name}")
            lines.append(f"# {'=' * 60}")
            lines.append("")
            lines.extend(
                self._transformation_function_lines(trans, stats, logs)
            )
            lines.append("")

        lines.extend(self._main_function_lines(ordered_transformations, job))
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append("    main()")

        if job:
            logs.append(f"Generated single-file workflow for job '{job.name}'.")
        else:
            logs.append("Generated single-file standalone workflow (no .kjb found).")
        return "\n".join(lines)

    def _transformation_function_lines(
        self,
        transformation: PentahoTransformation,
        stats: ConversionStats,
        logs: list[str],
    ) -> list[str]:
        """Return lines for one ``run_<name>(spark)`` function (no module header)."""
        dag = StepDAG(transformation.steps, transformation.hops)
        order = dag.topological_sort()
        df_map: dict[str, str] = {}

        for step in transformation.steps:
            safe = step.name.replace(" ", "_").replace("-", "_")
            df_map[step.name] = f"df_{safe}"

        lines: list[str] = []
        func_name = _safe_func_name(transformation.name)
        lines.append(f"def run_{func_name}(spark):")
        lines.append(f'    """Execute transformation: {transformation.name}"""')

        if transformation.parameters:
            lines.append("    # Parameters")
            for key, val in transformation.parameters.items():
                lines.append(f"    {key} = {val!r}")

        lines.append("")
        last_output: str | None = None
        lineage_map: dict[str, set[str]] = {}

        for step_name in order:
            step = next((s for s in transformation.steps if s.name == step_name), None)
            if step is None:
                continue

            input_cols: set[str] = set()
            for pred in dag.predecessors(step_name):
                input_cols |= lineage_map.get(pred, set())

            ctx = StepContext(
                transformation=transformation,
                step=step,
                dag=dag,
                df_variable_map=df_map,
                extra={"input_columns": sorted(input_cols)},
            )
            outcome = self.registry.convert_step(step.step_type, ctx)

            lines.append(f"    # Step: {step.name} ({step.step_type}) [{outcome.status}]")
            out_df = df_map.get(step_name, "")
            for cl in outcome.code_lines:
                lines.append(f"    {cl}")
                stripped = cl.strip()
                if out_df and stripped.startswith(f"{out_df} ="):
                    last_output = out_df

            if not hasattr(stats, "step_outcomes"):
                stats.step_outcomes = []
            stats.step_outcomes.append(outcome)

            stats.step_results.append(
                StepConversionResult(
                    step_name=step.name,
                    step_type=step.step_type,
                    status=outcome.status,
                    detail="; ".join(outcome.errors + outcome.warnings) or f"Transformation: {transformation.name}",
                    semantic_score=outcome.semantic_score,
                    warnings=outcome.warnings,
                    errors=outcome.errors,
                )
            )
            if outcome.status == "converted":
                stats.steps_converted += 1
            elif outcome.status in ("partial", "partially_supported", "approximated", "manual_required"):
                stats.steps_approximated += 1
            else:
                stats.steps_skipped += 1
                logs.append(
                    f"Step '{step.name}' ({step.step_type}): {outcome.status} — "
                    f"{'; '.join(outcome.errors[:2])}"
                )
            for w in outcome.warnings:
                if w not in stats.warnings:
                    stats.warnings.append(w)

            from .lineage import infer_output_columns
            from .validation.step_validators import parse_step_config

            parsed = parse_step_config(ctx)
            lineage_map[step_name] = infer_output_columns(
                step.step_type, parsed, input_cols, outcome.code_lines
            )

            if last_output is None:
                last_output = df_map.get(step_name)
            lines.append("")

        if last_output:
            lines.append(f"    return {last_output}")
        else:
            lines.append("    return spark.createDataFrame([], '_placeholder STRING')")

        lines.append("")
        return lines

    def _main_function_lines(
        self,
        ordered_transformations: list[PentahoTransformation],
        job: PentahoJob | None,
    ) -> list[str]:
        """Return lines for the orchestrating ``main()`` function."""
        app_name = job.name if job else "pentaho_migration"
        lines = [
            f"# {'=' * 60}",
            "# Main workflow",
            f"# {'=' * 60}",
            "",
            "def main():",
            f"    spark = SparkSession.builder.appName({app_name!r}).getOrCreate()",
            "    try:",
        ]
        for trans in ordered_transformations:
            func = _safe_func_name(trans.name)
            var = f"result_{func}"
            label = trans.name
            lines.append(f"        print('Running transformation: {label}')")
            lines.append(f"        {var} = run_{func}(spark)")
        lines.extend([
            "        print('Workflow completed successfully.')",
            "    finally:",
            "        spark.stop()",
            "",
        ])
        return lines

    def generate_workflow(
        self,
        job: PentahoJob,
        ordered_transformations: list[PentahoTransformation],
        logs: list[str],
    ) -> str:
        """Generate a main workflow module that runs transformations in job order."""
        lines: list[str] = []
        lines.extend(self._file_header(f"Workflow: {job.name}", job.file_path.name))
        lines.append('"""')
        lines.append(f"Main workflow for Pentaho job: {job.name}")
        lines.append("Executes transformations in job-defined order.")
        lines.append('"""')
        lines.append("")
        lines.append("from pyspark.sql import SparkSession")
        lines.append("")

        for trans in ordered_transformations:
            module = _safe_filename(trans.name)
            lines.append(f"from transformations.{module} import run_{_safe_func_name(trans.name)}")

        lines.append("")
        lines.append("def main():")
        lines.append("    spark = SparkSession.builder.appName("
                     f"{job.name!r}).getOrCreate()")
        lines.append("    try:")
        for trans in ordered_transformations:
            func = _safe_func_name(trans.name)
            var = f"result_{func}"
            lines.append(f"        print('Running transformation: {trans.name}')")
            lines.append(f"        {var} = run_{func}(spark)")
        lines.append("        print('Workflow completed successfully.')")
        lines.append("    finally:")
        lines.append("        spark.stop()")
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append("    main()")

        logs.append(f"Generated main workflow for job '{job.name}'.")
        return "\n".join(lines)

    def generate_standalone_main(
        self,
        transformations: list[PentahoTransformation],
        logs: list[str],
    ) -> str:
        """Generate a main module when no job file exists."""
        lines: list[str] = []
        lines.extend(self._file_header("Pentaho Project Workflow", "standalone"))
        lines.append("from pyspark.sql import SparkSession")
        lines.append("")

        for trans in transformations:
            module = _safe_filename(trans.name)
            lines.append(f"from transformations.{module} import run_{_safe_func_name(trans.name)}")

        lines.append("")
        lines.append("def main():")
        lines.append("    spark = SparkSession.builder.appName('pentaho_migration').getOrCreate()")
        lines.append("    try:")
        for trans in transformations:
            func = _safe_func_name(trans.name)
            lines.append(f"        print('Running: {trans.name}')")
            lines.append(f"        run_{func}(spark)")
        lines.append("    finally:")
        lines.append("        spark.stop()")
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append("    main()")

        logs.append("Generated standalone main workflow (no .kjb found).")
        return "\n".join(lines)

    @staticmethod
    def _file_header(title: str, source: str) -> list[str]:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return [
            '"""',
            f"Auto-generated PySpark code from Pentaho: {title}",
            f"Source: {source}",
            f"Generated: {ts}",
            '"""',
            "",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.window import Window",
            "from pyspark.sql.functions import col, lit, when, expr, count, coalesce, broadcast",
            "from pyspark.sql.functions import upper, lower, trim, ltrim, rtrim, initcap, length",
            "from pyspark.sql.functions import substring, round, abs, sqrt, ceil, floor, pow",
            "from pyspark.sql.functions import concat, isnull, regexp_replace, explode, array",
            "from pyspark.sql.functions import to_date, to_timestamp, datediff, date_add, add_months",
            "from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear",
            "from pyspark.sql.functions import row_number, rank, dense_rank, monotonically_increasing_id",
            "from pyspark.sql.functions import countDistinct, first, last, levenshtein, sum as _sum, avg",
            "",
        ]
