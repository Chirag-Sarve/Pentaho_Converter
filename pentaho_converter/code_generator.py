"""PySpark code generation for Pentaho transformations and jobs."""

from __future__ import annotations

import re

from .generation_config import GenerationConfig
from .generated_code_style import remove_generator_comments
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
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"trans_{cleaned}"
    return cleaned or "transformation"


def _safe_df_name(step_name: str) -> str:
    """Build a readable, valid Python identifier for a step's DataFrame."""
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", (step_name or "").strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = "step"
    if cleaned[0].isdigit():
        cleaned = f"s_{cleaned}"
    return cleaned


def _transformation_config_keys(transformation: PentahoTransformation) -> list[str]:
    """Ordered, unique parameter/variable names that are valid Python identifiers."""
    keys: list[str] = []
    seen: set[str] = set()
    for mapping in (transformation.parameters, transformation.variables):
        if not mapping:
            continue
        for key in mapping:
            if key in seen or not str(key).isidentifier():
                continue
            seen.add(key)
            keys.append(key)
    return keys


def _config_keys_referenced(code_lines: list[str], known_keys: list[str]) -> list[str]:
    """Return known config keys referenced as identifiers in generated step body code."""
    if not known_keys or not code_lines:
        return []
    text = "\n".join(code_lines)
    return [key for key in known_keys if re.search(rf"\b{re.escape(key)}\b", text)]


def _needs_config_mapping(code_lines: list[str]) -> bool:
    """True when generated step code reads from the ``config`` mapping directly."""
    if not code_lines:
        return False
    text = "\n".join(code_lines)
    return bool(re.search(r"\bconfig\.(?:get|setdefault)\b|\bconfig\[", text))


def _strip_redundant_logging_import(code_lines: list[str]) -> list[str]:
    """Drop bare ``import logging`` from step bodies (any indentation).

    Job/transformation modules already import ``logging`` at file scope. A nested
    ``import logging`` — even inside ``try`` — makes the name local for the whole
    step function, so the leading ``logging.info(...)`` emitted by the generator
    raises UnboundLocalError.
    """
    return [line for line in code_lines if line.strip() != "import logging"]


class PySparkCodeGenerator:
    """Generates PySpark modules from parsed Pentaho transformations."""

    def __init__(
        self,
        registry: StepRegistry | None = None,
        generation_config: GenerationConfig | None = None,
    ) -> None:
        self.registry = registry or build_default_registry()
        self.generation_config = generation_config or GenerationConfig.defaults()

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

    def generate_transformation_module(
        self,
        transformation: PentahoTransformation,
        stats: ConversionStats,
        logs: list[str],
    ) -> str:
        """Generate a standalone module exposing ``run(spark, config)`` (legacy / single-file)."""
        lines: list[str] = []
        lines.extend(
            [
                f'"""PySpark transformation: {transformation.name}."""',
                "",
                "from __future__ import annotations",
                "",
                "import logging",
                "from typing import Any, Mapping",
                "",
                "from pyspark.sql import SparkSession",
                "from pyspark.sql.window import Window",
                "from pyspark.sql.functions import col, lit, when, expr, count, coalesce, broadcast",
                "from delta.tables import DeltaTable",
                "from pyspark.sql.functions import upper, lower, trim, ltrim, rtrim, initcap, length",
                "from pyspark.sql.functions import substring, round, abs, sqrt, ceil, floor, pow",
                "from pyspark.sql.functions import concat, concat_ws, isnull, regexp_replace, regexp_extract, explode, explode_outer, array",
                "from pyspark.sql.functions import split, element_at, collect_list, from_csv",
                "from pyspark.sql.functions import md5, sha1, sha2, crc32, hex, unhex, soundex, lag, lead, rand, randn",
                "from pyspark.sql.functions import lpad, rpad, greatest, conv, dayofyear, quarter, hour, minute, second",
                "from pyspark.sql.functions import to_date, to_timestamp, datediff, date_add, add_months, date_format",
                "from pyspark.sql.functions import unix_timestamp, from_unixtime, current_date, current_timestamp",
                "from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, repeat",
                "from pyspark.sql.functions import row_number, rank, dense_rank, monotonically_increasing_id",
                "from pyspark.sql.functions import countDistinct, first, last, levenshtein, sum as _sum, avg, max as _max, min as _min",
                "from pyspark.sql.functions import stddev_samp, var_samp as variance_samp, to_json, struct",
                "",
                "import config",
                "",
                "logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')",
                f"TRANSFORMATION_NAME = {transformation.name!r}",
                f"SOURCE_KTR = {transformation.file_path.name!r}",
                "",
                "TARGET_CATALOG = config.TARGET_CATALOG",
                "TARGET_SCHEMA = config.TARGET_SCHEMA",
                "PENTAHO_DATA_DIR = config.PENTAHO_DATA_DIR",
                "",
            ]
        )
        lines.extend(
            self._transformation_function_lines(
                transformation,
                stats,
                logs,
                entrypoint="module",
            )
        )
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append('    _spark = SparkSession.builder.appName(TRANSFORMATION_NAME).getOrCreate()')
        lines.append("    run(_spark, None)")
        lines.append("")
        return "\n".join(lines)

    def generate_inlined_transformation_block(
        self,
        transformation: PentahoTransformation,
        stats: ConversionStats,
        logs: list[str],
        *,
        step_counter: int = 1,
        run_func_name: str | None = None,
    ) -> tuple[list[str], int, str]:
        """Emit step functions + ``run_<stem>(spark, config)`` for embedding in a job file.

        Returns ``(code_lines, next_step_counter, run_func_name)``.
        """
        from .naming import safe_module_name

        stem = safe_module_name(transformation.file_path.stem or transformation.name)
        run_name = run_func_name or f"run_{stem}"
        lines, next_counter = self._transformation_function_lines(
            transformation,
            stats,
            logs,
            entrypoint="inlined",
            run_func_name=run_name,
            step_counter=step_counter,
            source_ktr=transformation.file_path.name,
        )
        logs.append(
            f"Inlined transformation '{transformation.name}' as {run_name} "
            f"(steps {step_counter}–{next_counter - 1})"
        )
        return lines, next_counter, run_name

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
        *,
        entrypoint: str = "named",
        run_func_name: str | None = None,
        step_counter: int = 1,
        source_ktr: str | None = None,
    ) -> list[str] | tuple[list[str], int]:
        """Return lines for one transformation entry function (no module header).

        ``entrypoint``:
          - ``named`` → ``def run_<name>(spark):`` (legacy single-file)
          - ``module`` → ``def run(spark, config=None):`` (standalone module)
          - ``inlined`` → per-step functions + ``run_<stem>(spark, config)`` for job files

        For ``inlined``, returns ``(lines, next_step_counter)``.
        """
        dag = StepDAG(transformation.steps, transformation.hops)
        order = dag.topological_sort()
        df_map: dict[str, str] = {}
        used_df_names: set[str] = set()

        for step in transformation.steps:
            base_name = f"{_safe_df_name(step.name).lower()}_df"
            df_name = base_name
            suffix = 2
            while df_name in used_df_names:
                df_name = f"{base_name}_{suffix}"
                suffix += 1
            used_df_names.add(df_name)
            df_map[step.name] = df_name

        if entrypoint == "inlined":
            return self._emit_inlined_steps(
                transformation,
                stats,
                logs,
                dag=dag,
                order=order,
                df_map=df_map,
                run_func_name=run_func_name or f"run_{_safe_func_name(transformation.name)}",
                step_counter=step_counter,
                source_ktr=source_ktr or transformation.file_path.name,
            )

        lines: list[str] = []
        if entrypoint == "module":
            lines.append("def run(spark, config=None):")
            lines.append(f'    """Execute transformation: {transformation.name}"""')
            lines.append("    config = dict(config or {})")
            lines.append("    logging.info('Starting transformation %s', TRANSFORMATION_NAME)")
            if transformation.parameters:
                for key, val in transformation.parameters.items():
                    lines.append(f"    {key} = config.get({key!r}, {val!r})")
            if transformation.variables:
                for key, val in transformation.variables.items():
                    lines.append(f"    {key} = config.get({key!r}, {val!r})")
        else:
            func_name = _safe_func_name(transformation.name)
            lines.append(f"def run_{func_name}(spark):")
            lines.append(f'    """Execute transformation: {transformation.name}"""')
            if transformation.parameters:
                for key, val in transformation.parameters.items():
                    lines.append(f"    {key} = {val!r}")

        lines.append("")
        last_output: str | None = None
        lineage_map: dict[str, set[str]] = {}

        for step_number, step_name in enumerate(order, start=1):
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
                extra={
                    "input_columns": sorted(input_cols),
                    "generation_config": self.generation_config,
                },
            )
            outcome = self.registry.convert_step(step.step_type, ctx)

            lines.append(f"    # Step {step_number} : {step.name}")
            out_df = df_map.get(step_name, "")
            code_lines = remove_generator_comments(list(outcome.code_lines))

            # Continuity safety net: never leave a step without an output DF assignment.
            has_out_assign = any(
                cl.strip().startswith(f"{out_df} =") for cl in code_lines if out_df
            )
            prior_branch_assigned = bool(out_df) and any(
                cl.strip().startswith(f"{out_df} =") for cl in lines
            )
            if out_df and not has_out_assign and not prior_branch_assigned:
                preds = dag.predecessors(step_name)
                upstream = df_map.get(preds[0]) if preds else None
                if upstream:
                    code_lines.append(f"{out_df} = {upstream}")
                else:
                    code_lines.append(f"{out_df} = spark.range(0).limit(0)")
                if outcome.status == "converted":
                    outcome.status = "partial"
                warn = (
                    f"Step '{step.name}' generated no DataFrame assignment; "
                    "upstream preserved for continuity"
                )
                if warn not in outcome.warnings:
                    outcome.warnings.append(warn)

            for cl in _strip_redundant_logging_import(code_lines):
                lines.append(f"    {cl}")
                stripped = cl.strip()
                if out_df and stripped.startswith(f"{out_df} ="):
                    last_output = out_df

            self._record_step_outcome(stats, logs, transformation, step, outcome)

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
            if entrypoint == "module":
                lines.append("    logging.info('Finished transformation %s', TRANSFORMATION_NAME)")
            lines.append(f"    return {last_output}")
        else:
            lines.append("    return spark.createDataFrame([], '_placeholder STRING')")

        lines.append("")
        return lines

    def _record_step_outcome(
        self,
        stats: ConversionStats,
        logs: list[str],
        transformation: PentahoTransformation,
        step,
        outcome,
        *,
        function_name: str = "",
        generated_file: str = "",
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> None:
        if not hasattr(stats, "step_outcomes"):
            stats.step_outcomes = []
        stats.step_outcomes.append(outcome)

        detail_bits = [f"Transformation: {transformation.name}"]
        detail_bits.extend(outcome.errors + outcome.warnings)
        stats.step_results.append(
            StepConversionResult(
                step_name=step.name,
                step_type=step.step_type,
                status=outcome.status,
                detail="; ".join(detail_bits),
                semantic_score=outcome.semantic_score,
                warnings=outcome.warnings,
                errors=outcome.errors,
                infos=list(getattr(outcome, "infos", []) or []),
                display_status=getattr(outcome, "display_status", "") or "",
                transformation_name=transformation.name,
                generated_file=generated_file,
                function_name=function_name,
                start_line=start_line,
                end_line=end_line,
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

    def _emit_inlined_steps(
        self,
        transformation: PentahoTransformation,
        stats: ConversionStats,
        logs: list[str],
        *,
        dag: StepDAG,
        order: list[str],
        df_map: dict[str, str],
        run_func_name: str,
        step_counter: int,
        source_ktr: str,
    ) -> tuple[list[str], int]:
        """Emit per-step functions + a run_* orchestrator preserving Pentaho order.

        Configuration is loaded once in ``run_*``. Step functions receive only the
        config keys (or the ``config`` mapping) that their generated body actually
        references — pure transforms like Filter/Sort/Join get none.
        """
        lines: list[str] = []

        last_output: str | None = None
        lineage_map: dict[str, set[str]] = {}
        # Branch streams produced by FilterRows/JavaFilter: target_step -> df var
        branch_streams: dict[str, str] = {}
        # (step_fn, out_df, pred_dfs, used_keys, needs_config, step_name, branch_returns)
        step_plan: list[
            tuple[str, str, list[str], list[str], bool, str, list[str]]
        ] = []
        counter = step_counter
        known_keys = _transformation_config_keys(transformation)
        trans_name = transformation.name

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
                extra={
                    "input_columns": sorted(input_cols),
                    "generation_config": self.generation_config,
                },
            )
            outcome = self.registry.convert_step(step.step_type, ctx)
            out_df = df_map.get(step_name, "")
            code_lines = remove_generator_comments(list(outcome.code_lines))

            preds = dag.predecessors(step_name)
            pred_dfs: list[str] = []
            for pred in preds:
                if pred not in df_map:
                    continue
                # Prefer Filter/JavaFilter/SwitchCase branch stream when this step
                # is an explicit true/false (or case) target.
                if step_name in branch_streams:
                    pred_step = dag.steps.get(pred)
                    pst = (
                        (pred_step.step_type or "").strip().lower().replace(" ", "")
                        if pred_step is not None
                        else ""
                    )
                    if pst in {"filterrows", "javafilter", "switchcase"}:
                        pred_dfs.append(branch_streams[step_name])
                        continue
                pred_dfs.append(df_map[pred])

            # Also resolve via DAG metadata when branch_streams is not yet filled
            # (e.g. Constant after Filter in the same pass — streams registered below).
            if not pred_dfs:
                pass
            elif len(pred_dfs) == 1:
                try:
                    from .filter_converter import resolve_incoming_branch_df

                    branch = resolve_incoming_branch_df(ctx)
                    if branch:
                        pred_dfs = [branch]
                except Exception:
                    pass

            has_out_assign = any(
                cl.strip().startswith(f"{out_df} =") for cl in code_lines if out_df
            )
            if out_df and not has_out_assign:
                upstream = pred_dfs[0] if pred_dfs else None
                if upstream:
                    code_lines.append(f"{out_df} = {upstream}")
                else:
                    code_lines.append(f"{out_df} = spark.range(0).limit(0)")
                if outcome.status == "converted":
                    outcome.status = "partial"
                warn = (
                    f"Step '{step.name}' generated no DataFrame assignment; "
                    "upstream preserved for continuity"
                )
                if warn not in outcome.warnings:
                    outcome.warnings.append(warn)

            used_keys = _config_keys_referenced(code_lines, known_keys)
            needs_config = _needs_config_mapping(code_lines)

            step_number = counter
            step_fn = f"step_{counter:02d}_{_safe_func_name(step.name)}"
            counter += 1

            # Detect FilterRows dual-branch outputs so the orchestrator can
            # unpack them — inlined step functions otherwise discard locals.
            branch_returns: list[str] = []
            st_norm = (step.step_type or "").strip().lower().replace(" ", "")
            if st_norm in {"filterrows", "javafilter"}:
                try:
                    from .filter_converter import (
                        _branch_stream_name,
                        _connected_branch_targets,
                    )
                    from .metadata_propagation import get_converter_metadata

                    meta = get_converter_metadata(ctx)
                    true_t, false_t = _connected_branch_targets(meta, ctx, step_name)
                    assigned = "\n".join(code_lines)
                    for target in (true_t, false_t):
                        if not target:
                            continue
                        bvar = _branch_stream_name(target)
                        # Only unpack streams the Filter step actually assigned.
                        if f"{bvar} =" not in assigned and f"{bvar}=" not in assigned:
                            continue
                        branch_returns.append(bvar)
                        branch_streams[target] = bvar
                except Exception:
                    branch_returns = []

            param_parts = ["spark"]
            param_parts.extend(pred_dfs)
            param_parts.extend(used_keys)
            if needs_config:
                param_parts.append("config")
            signature = ", ".join(param_parts)

            # Predicted lineage assists validation only — never hard-fail the
            # runner on approximate upstream schemas (joins/lookups/renames).
            req_literal = "[]"

            lines.append(f"# Step {step_number} : {step.name}")
            lines.append(f"def {step_fn}({signature}):")
            lines.append(f'    logging.info("Running {step.step_type}: {step.name}")')
            if pred_dfs:
                primary_in = pred_dfs[0]
                lines.append(
                    f"    {primary_in} = require_dataframe("
                    f"{primary_in}, transformation={trans_name!r}, "
                    f"step_name={step.name!r}, func_name={step_fn!r}, "
                    f"required_columns={req_literal})"
                )
                lines.append(
                    f"    log_step_dataframe("
                    f"{primary_in}, step_name={step.name!r}, phase='before', "
                    f"transformation={trans_name!r}, func_name={step_fn!r})"
                )
                for extra_in in pred_dfs[1:]:
                    lines.append(
                        f"    {extra_in} = require_dataframe("
                        f"{extra_in}, transformation={trans_name!r}, "
                        f"step_name={step.name!r}, func_name={step_fn!r}, "
                        f"required_columns=[])"
                    )
            for cl in _strip_redundant_logging_import(code_lines):
                lines.append(f"    {cl}")
            if out_df:
                lines.append(
                    f"    {out_df} = require_dataframe("
                    f"{out_df}, transformation={trans_name!r}, "
                    f"step_name={step.name!r}, func_name={step_fn!r}, "
                    f"required_columns=[])"
                )
                lines.append(
                    f"    log_step_dataframe("
                    f"{out_df}, step_name={step.name!r}, phase='after', "
                    f"transformation={trans_name!r}, func_name={step_fn!r})"
                )
                if branch_returns:
                    # Keep branch DataFrames alive for true/false target steps.
                    ret_parts = [out_df] + branch_returns
                    lines.append(f"    return {', '.join(ret_parts)}")
                else:
                    lines.append(f"    return {out_df}")
            else:
                lines.append("    return spark.createDataFrame([], '_placeholder STRING')")
            lines.append("")

            step_plan.append(
                (
                    step_fn,
                    out_df or "None",
                    pred_dfs,
                    used_keys,
                    needs_config,
                    step.name,
                    branch_returns,
                )
            )
            if out_df:
                last_output = out_df

            # Record navigation metadata only (file/line filled after module assembly).
            self._record_step_outcome(
                stats,
                logs,
                transformation,
                step,
                outcome,
                function_name=step_fn,
            )

            from .lineage import infer_output_columns
            from .validation.step_validators import parse_step_config

            parsed = parse_step_config(ctx)
            lineage_map[step_name] = infer_output_columns(
                step.step_type, parsed, input_cols, outcome.code_lines
            )

        lines.append(f"def {run_func_name}(spark, config=None):")
        lines.append(f'    """Run {transformation.name}."""')
        lines.append("    config = dict(config or {})")
        lines.append(
            f'    logging.info("Starting transformation: {transformation.name} ({source_ktr})")'
        )
        if transformation.parameters:
            for key, val in transformation.parameters.items():
                lines.append(f"    {key} = config.get({key!r}, {val!r})")
        if transformation.variables:
            for key, val in transformation.variables.items():
                lines.append(f"    {key} = config.get({key!r}, {val!r})")
        lines.append("")

        for (
            step_fn,
            out_df,
            pred_dfs,
            used_keys,
            needs_config,
            step_label,
            branch_returns,
        ) in step_plan:
            call_args = ["spark"] + pred_dfs + used_keys
            if needs_config:
                call_args.append("config")
            for pred_df in pred_dfs:
                # None-check only — do not enforce predicted lineage schemas.
                lines.append(
                    f"    {pred_df} = require_dataframe("
                    f"{pred_df}, transformation={trans_name!r}, "
                    f"step_name={step_label!r}, func_name={step_fn!r}, "
                    f"required_columns=[])"
                )
            if out_df and out_df != "None":
                if branch_returns:
                    unpack = ", ".join([out_df] + branch_returns)
                    lines.append(f"    {unpack} = {step_fn}({', '.join(call_args)})")
                else:
                    lines.append(f"    {out_df} = {step_fn}({', '.join(call_args)})")
                lines.append(
                    f"    {out_df} = require_dataframe("
                    f"{out_df}, transformation={trans_name!r}, "
                    f"step_name={step_label!r}, func_name={step_fn!r}, "
                    f"required_columns=[])"
                )
            else:
                lines.append(f"    {step_fn}({', '.join(call_args)})")

        lines.append("")
        if last_output:
            lines.append(
                f'    logging.info("Finished transformation: {transformation.name}")'
            )
            lines.append(f"    return {last_output}")
        else:
            lines.append("    return spark.createDataFrame([], '_placeholder STRING')")
        lines.append("")
        return lines, counter

    @staticmethod
    def _main_spark_bootstrap(app_name: str) -> list[str]:
        """Lines that bind a Spark session for local CLI runs (not Databricks notebooks)."""
        return [
            "    _owns_spark_session = False",
            '    _spark = globals().get("spark")',
            "    if _spark is None:",
            "        _spark = SparkSession.getActiveSession()",
            "    if _spark is None:",
            f"        _spark = SparkSession.builder.appName({app_name!r}).getOrCreate()",
            "        _owns_spark_session = True",
        ]

    @staticmethod
    def _main_spark_shutdown() -> list[str]:
        return [
            "        if _owns_spark_session:",
            "            _spark.stop()",
        ]

    def _workflow_function_lines(
        self,
        ordered_transformations: list[PentahoTransformation],
        job: PentahoJob | None,
    ) -> list[str]:
        """Return ``run_workflow(spark)`` — the Databricks notebook entry point."""
        job_name = job.name if job else "pentaho_migration"
        lines = [
            "def run_workflow(spark):",
            f'    """Run all Pentaho transformations for job: {job_name}',
            "",
            "    Databricks notebook: run_workflow(spark)",
            '    """',
        ]
        last_var: str | None = None
        for trans in ordered_transformations:
            func = _safe_func_name(trans.name)
            var = f"result_{func}"
            lines.append(f"    print('Running transformation: {trans.name}')")
            lines.append(f"    {var} = run_{func}(spark)")
            last_var = var
        if last_var:
            lines.append(f"    return {last_var}")
        lines.append("")
        return lines

    def _main_function_lines(
        self,
        ordered_transformations: list[PentahoTransformation],
        job: PentahoJob | None,
    ) -> list[str]:
        """Return lines for local CLI ``main()`` (not used in Databricks notebooks)."""
        app_name = job.name if job else "pentaho_migration"
        lines = []
        lines.extend(self._workflow_function_lines(ordered_transformations, job))
        lines.extend([
            "def main():",
            '    """Local execution only — use run_workflow(spark) in Databricks notebooks."""',
        ])
        lines.extend(self._main_spark_bootstrap(app_name))
        lines.extend([
            "    try:",
            "        return run_workflow(_spark)",
            "    finally:",
        ])
        lines.extend(self._main_spark_shutdown())
        lines.append("")
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
        lines.extend(self._main_spark_bootstrap(job.name))
        lines.append("    try:")
        for trans in ordered_transformations:
            func = _safe_func_name(trans.name)
            var = f"result_{func}"
            lines.append(f"        print('Running transformation: {trans.name}')")
            lines.append(f"        {var} = run_{func}(_spark)")
        lines.append("        print('Workflow completed successfully.')")
        lines.append("    finally:")
        lines.extend(self._main_spark_shutdown())
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
        lines.extend(self._main_spark_bootstrap("pentaho_migration"))
        lines.append("    try:")
        for trans in transformations:
            func = _safe_func_name(trans.name)
            lines.append(f"        print('Running: {trans.name}')")
            lines.append(f"        run_{func}(_spark)")
        lines.append("    finally:")
        lines.extend(self._main_spark_shutdown())
        lines.append("")
        lines.append("")
        lines.append('if __name__ == "__main__":')
        lines.append("    main()")

        logs.append("Generated standalone main workflow (no .kjb found).")
        return "\n".join(lines)

    def _file_header(self, title: str, source: str) -> list[str]:
        catalog = self.generation_config.catalog or "workspace"
        schema = self.generation_config.schema or "default"
        data_dir = self.generation_config.data_dir or "/Volumes/workspace/default/rawdata"
        return [
            f'"""PySpark transformation: {title}."""',
            "",
            f"TARGET_CATALOG = {catalog!r}",
            f"TARGET_SCHEMA = {schema!r}",
            f"PENTAHO_DATA_DIR = {data_dir!r}",
            "",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.window import Window",
            "from pyspark.sql.functions import col, lit, when, expr, count, coalesce, broadcast",
            "from delta.tables import DeltaTable",
            "from pyspark.sql.functions import upper, lower, trim, ltrim, rtrim, initcap, length",
            "from pyspark.sql.functions import substring, round, abs, sqrt, ceil, floor, pow",
            "from pyspark.sql.functions import concat, concat_ws, isnull, regexp_replace, regexp_extract, explode, explode_outer, array",
            "from pyspark.sql.functions import split, element_at, collect_list, from_csv",
            "from pyspark.sql.functions import md5, sha1, sha2, crc32, hex, unhex, soundex, lag, lead, rand, randn",
            "from pyspark.sql.functions import lpad, rpad, greatest, conv, dayofyear, quarter, hour, minute, second",
            "from pyspark.sql.functions import to_date, to_timestamp, datediff, date_add, add_months, date_format",
            "from pyspark.sql.functions import unix_timestamp, from_unixtime, current_date, current_timestamp",
            "from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, repeat",
            "from pyspark.sql.functions import row_number, rank, dense_rank, monotonically_increasing_id",
            "from pyspark.sql.functions import countDistinct, first, last, levenshtein, sum as _sum, avg, max as _max, min as _min",
            # Spark exposes sample variance as var_samp (alias kept for generated aggs).
            "from pyspark.sql.functions import stddev_samp, var_samp as variance_samp, to_json, struct",
            "",
        ]
