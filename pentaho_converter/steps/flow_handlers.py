"""Handlers for Pentaho Flow transformation steps.

Supports:
- Abort, Append Streams, Block Until Steps Finish, Blocking Step
- Detect Empty Stream, Dummy (via advanced DummyHandler for DF naming)
- ETL Metadata Injection, Filter Rows (via FilterRowsHandler)
- Identify Last Row, Java Filter, Job Executor
- Prioritize Streams, Single Threader, Switch / Case, Transformation Executor
"""

from __future__ import annotations

import logging
import re

from ..filter_converter import (
    _branch_stream_name,
    _connected_branch_targets,
    _literal_expr,
    convert_simple_condition,
    resolve_incoming_branch_df,
)
from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_abort_config,
    parse_append_streams_config,
    parse_block_until_steps_finish_config,
    parse_blocking_step_config,
    parse_detect_empty_stream_config,
    parse_identify_last_row_config,
    parse_java_filter_config,
    parse_job_executor_config,
    parse_meta_inject_config,
    parse_prioritize_streams_config,
    parse_single_threader_config,
    parse_switch_case_config,
    parse_trans_executor_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return (
        (step_type or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "")
    )


def _meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


def _passthrough(context: StepContext, label: str) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [f"# {label}: {context.step.name}"]
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
    return lines, "converted"


_SKIP_PRESERVE = frozenset({
    "step_type", "step_name", "attributes", "fields", "transformation_parameters",
    "_propagated_keys", "_propagation_trace", "extras",
})


def _preserve(meta: dict, keys: tuple[str, ...] = ()) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()

    def _emit(key: str, val: object) -> None:
        lines.append(f"# preserved.{key}={val!r}")

    for key in keys:
        if key in seen:
            continue
        val = meta.get(key)
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)

    for key, val in meta.items():
        if key in seen or key in _SKIP_PRESERVE:
            continue
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    return lines


def _warn(step_name: str, message: str) -> None:
    logger.warning("Flow step '%s': %s", step_name, message)


def _df_for(context: StepContext, step_name: str) -> str:
    safe = step_name.replace(" ", "_").replace("-", "_")
    return context.df_variable_map.get(step_name, f"df_{safe}")


def _hop_input_dfs(context: StepContext) -> list[str]:
    preds = context.dag.predecessors(context.step.name)
    return [_df_for(context, p) for p in preds]


# ---------------------------------------------------------------------------
# Abort
# ---------------------------------------------------------------------------


class AbortHandler(BaseStepHandler):
    """Stop the pipeline with RuntimeError when abort conditions are met."""

    _TYPES = {"abort"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        # Prefer the Filter/Switch failure (or true) branch stream written for this step.
        # Never bind Abort to the Filter primary/success DataFrame.
        branch_df = resolve_incoming_branch_df(context)
        in_df = branch_df or context.input_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_abort_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        message = meta.get("message") or "Pentaho Abort step triggered"
        try:
            threshold = int(meta.get("row_threshold") or 0)
        except (TypeError, ValueError):
            threshold = 0
        always_log = bool(meta.get("always_log_rows"))
        abort_option = (meta.get("abort_option") or "").strip().lower()

        lines = [f"# Abort: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "row_threshold", "message", "always_log_rows", "abort_option",
        )))

        # Pentaho abort_option variants: Abort / AbortWithError / AbortAndLog
        # Spark/Databricks has no stop-without-error; all paths raise RuntimeError.
        if abort_option:
            lines.append(
                f"# abort_option={abort_option!r}: mapped to RuntimeError "
                f"(Databricks has no silent pipeline stop)"
            )
        stop_exc = "RuntimeError"
        if "error" in abort_option or not abort_option:
            stop_exc = "RuntimeError"
        elif "log" in abort_option:
            lines.append("# AbortAndLog: sample rows logged before raise")

        if in_df:
            if branch_df and branch_df == out_var:
                lines.append(
                    f"# Abort operates on its own failure/branch stream {out_var} "
                    "(already assigned by upstream Filter/Switch; not overwritten)"
                )
            elif in_df != out_var:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = {in_df}")
            if always_log or "log" in abort_option:
                lines.append(
                    f"print('Abort sample for', {context.step.name!r}, "
                    f"{out_var}.limit(100).collect())  # always_log_rows"
                )
            # Raise only when the abort condition is met:
            # - threshold > 0 → abort when row count reaches threshold
            # - threshold <= 0 → abort when any row reaches this step
            lines.append(f"_abort_count_{out_var} = {out_var}.count()")
            if threshold > 0:
                lines.append(
                    f"if _abort_count_{out_var} >= {threshold}:  "
                    f"# Abort after {threshold} row(s)"
                )
            else:
                lines.append(
                    f"if _abort_count_{out_var} > 0:  "
                    f"# Abort when any row reaches this step (threshold<=0)"
                )
            lines.append(f"    raise {stop_exc}({message!r})")
        else:
            # No upstream rows → abort condition is not satisfied; preserve empty DF.
            lines.append(f"{out_var} = spark.range(0).limit(0)")
            lines.append(
                f"# WARNING: Abort '{context.step.name}' has no input stream; "
                "RuntimeError skipped (condition not met)"
            )
            _warn(context.step.name, "Abort with no input stream — condition not met")
        return lines, "converted"


# ---------------------------------------------------------------------------
# Append Streams
# ---------------------------------------------------------------------------


class AppendStreamsHandler(BaseStepHandler):
    """Concatenate head then tail streams with unionByName."""

    _TYPES = {"append", "appendstreams"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_append_streams_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        head = (meta.get("head_name") or "").strip()
        tail = (meta.get("tail_name") or "").strip()
        hop_dfs = _hop_input_dfs(context)

        ordered: list[str] = []
        if head:
            ordered.append(_df_for(context, head))
        if tail:
            ordered.append(_df_for(context, tail))
        if not ordered:
            ordered = list(hop_dfs)

        lines = [f"# Append Streams: {context.step.name}"]
        lines.extend(_preserve(meta, ("head_name", "tail_name", "stream_order")))

        if len(ordered) >= 2:
            lines.append(
                f"# Stream order preserved: head then tail "
                f"(schema mismatch uses allowMissingColumns)"
            )
            expr = ordered[0]
            for nxt in ordered[1:]:
                expr = f"{expr}.unionByName({nxt}, allowMissingColumns=True)"
            lines.append(f"{out_var} = {expr}")
            return lines, "converted"

        if len(ordered) == 1:
            lines.append(f"{out_var} = {ordered[0]}")
            _warn(context.step.name, "Append Streams has only one input stream")
            return lines, "partial"

        lines.append(f"{out_var} = spark.createDataFrame([], '_append STRING').limit(0)")
        _warn(context.step.name, "Append Streams has no input streams")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Block Until Steps Finish
# ---------------------------------------------------------------------------


class BlockUntilStepsFinishHandler(BaseStepHandler):
    """Document wait-for-step dependencies; Spark DAG already serializes ancestors."""

    _TYPES = {
        "blockuntilstepsfinish",
        "blockthisstepuntilstepsfinish",
    }

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_block_until_steps_finish_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        wait_steps = meta.get("wait_steps") or meta.get("steps") or []
        lines = [f"# Block This Step Until Steps Finish: {context.step.name}"]
        lines.extend(_preserve(meta, ("wait_steps", "steps")))
        lines.append(
            "# LIMITATION: Databricks/Spark has no mid-pipeline step barriers; "
            "DAG lineage already waits for ancestor actions."
        )
        lines.append(
            "# Optional: materialize listed dependency DataFrames to approximate "
            "finish-before-continue semantics."
        )

        for entry in wait_steps:
            name = entry.get("name") if isinstance(entry, dict) else str(entry)
            if not name:
                continue
            known = set(context.dag.steps.keys()) | set(context.df_variable_map.keys())
            if known and name not in known:
                lines.append(
                    f"# WARNING: wait step {name!r} not found in transformation DAG"
                )
                _warn(context.step.name, f"wait step {name!r} not in DAG")
            dep_df = _df_for(context, name)
            lines.append(f"_ = {dep_df}.count()  # wait until step {name!r} finishes")

        if not wait_steps:
            lines.append("# WARNING: no wait_steps configured — passthrough only")
            _warn(context.step.name, "Block Until Steps Finish has empty dependency list")

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_block_until STRING').limit(0)")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Blocking Step
# ---------------------------------------------------------------------------


class BlockingStepHandler(BaseStepHandler):
    """Materialize upstream rows before continuing (cache + count)."""

    _TYPES = {"blockingstep", "block"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_blocking_step_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        pass_all = meta.get("pass_all_rows")
        if pass_all is None:
            pass_all = True

        lines = [f"# Blocking Step: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "pass_all_rows", "directory", "prefix", "cache_size", "compress_files",
        )))
        lines.append(
            "# LIMITATION: Pentaho temp-file spill (directory/prefix/compress) "
            "is replaced by Spark cache/persist."
        )

        if not in_df:
            return _passthrough(context, "Blocking Step")

        if pass_all:
            lines.append(f"{out_var} = {in_df}.cache()")
            lines.append(f"_ = {out_var}.count()  # synchronize: wait for all upstream rows")
        else:
            lines.append(
                "# pass_all_rows=N → emit only the last row after all input arrives"
            )
            lines.append(f"_blocked_{out_var} = {in_df}.cache()")
            lines.append(f"_ = _blocked_{out_var}.count()")
            lines.append(
                f"{out_var} = ("
                f"_blocked_{out_var}"
                f".withColumn('_rn_block', row_number().over("
                f"Window.orderBy(monotonically_increasing_id())))"
                f".withColumn('_max_block', _max(col('_rn_block')).over("
                f"Window.partitionBy(lit(1))))"
                f".filter(col('_rn_block') == col('_max_block'))"
                f".drop('_rn_block', '_max_block')"
                f")"
            )
            lines.append(
                "# Ordering note: last-row uses monotonically_increasing_id "
                "as stream-order proxy when no sort key exists"
            )
            _warn(
                context.step.name,
                "pass_all_rows=N uses window last-row over monotonically_increasing_id",
            )
            return lines, "converted"
        return lines, "converted"


# ---------------------------------------------------------------------------
# Detect Empty Stream
# ---------------------------------------------------------------------------


class DetectEmptyStreamHandler(BaseStepHandler):
    """Emit one empty-schema row when input is empty; otherwise emit zero rows."""

    _TYPES = {"detectemptystream", "detectempty"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_detect_empty_stream_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        lines = [f"# Detect Empty Stream: {context.step.name}"]
        lines.extend(_preserve(meta))

        if not in_df:
            lines.append(
                f"{out_var} = spark.createDataFrame([], '_empty STRING')"
            )
            lines.append(
                "# No upstream DataFrame — emitted empty schema; "
                "cannot synthesize a null row without input schema"
            )
            _warn(context.step.name, "Detect Empty Stream has no input DataFrame")
            return lines, "partial"

        flag = f"_empty_flag_{out_var}"
        lines.append(f"{flag} = {in_df}.limit(1).count() == 0")
        lines.append(
            "# Pentaho semantics: if empty → one null row with input schema; "
            "else → empty DataFrame (no rows forwarded)"
        )
        lines.append(f"if {flag}:")
        lines.append(f"    _schema_{out_var} = {in_df}.schema")
        lines.append(f"    if len({in_df}.columns) == 0:")
        lines.append(f"        {out_var} = spark.createDataFrame([], _schema_{out_var})")
        lines.append("    else:")
        lines.append(
            f"        {out_var} = spark.createDataFrame("
            f"[tuple(None for _ in {in_df}.columns)], _schema_{out_var})"
        )
        lines.append("else:")
        lines.append(f"    {out_var} = {in_df}.limit(0)")
        lines.append(
            "# Downstream hops receive this single output stream "
            "(empty-detection row or zero rows)."
        )
        return lines, "converted"


# ---------------------------------------------------------------------------
# ETL Metadata Injection
# ---------------------------------------------------------------------------


class MetaInjectHandler(BaseStepHandler):
    """Preserve injection mappings; emit Databricks metadata placeholders."""

    _TYPES = {"metainject", "etlmetadatainjection"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_meta_inject_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        lines = [f"# ETL Metadata Injection: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "specification_method", "trans_name", "filename", "directory_path",
            "source_step", "target_file", "no_execution",
            "stream_source_step", "stream_target_step", "mappings", "parameters",
        )))
        lines.append(
            "# LIMITATION: Runtime metadata injection into a child transformation "
            "is not available in Spark; mappings preserved as placeholders."
        )

        target = (
            meta.get("filename")
            or meta.get("trans_name")
            or meta.get("directory_path")
            or ""
        )
        if not target:
            _warn(context.step.name, "MetaInject missing child transformation reference")

        lines.append(
            f"_meta_inject_{out_var} = {{"
            f"'target': {target!r}, "
            f"'mappings': {meta.get('mappings')!r}, "
            f"'parameters': {meta.get('parameters')!r}, "
            f"'no_execution': {bool(meta.get('no_execution'))!r}"
            f"}}"
        )
        lines.append(
            f"# TODO: apply _meta_inject_{out_var} mappings before running child notebook/job"
        )

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_metainject STRING').limit(0)")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Identify Last Row
# ---------------------------------------------------------------------------


class IdentifyLastRowHandler(BaseStepHandler):
    """Add a boolean flag marking the last row in the (window-ordered) stream."""

    _TYPES = {"identifylastrow", "identifylastrowinastream"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_identify_last_row_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        result_field = meta.get("result_field") or "result"
        lines = [f"# Identify Last Row in a Stream: {context.step.name}"]
        lines.extend(_preserve(meta, ("result_field",)))

        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_last_row STRING').limit(0)")
            _warn(context.step.name, "Identify Last Row has no input")
            return lines, "partial"

        lines.append(
            "# Ordering: Pentaho marks the last physical stream row; "
            "Spark approximates with row_number over monotonically_increasing_id"
        )
        lines.append(
            f"{out_var} = {in_df}.withColumn('_rn_last', "
            f"row_number().over(Window.orderBy(monotonically_increasing_id())))"
        )
        lines.append(
            f"{out_var} = {out_var}.withColumn("
            f"'{result_field}', "
            f"col('_rn_last') == _max(col('_rn_last')).over(Window.partitionBy(lit(1))))"
        )
        lines.append(f"{out_var} = {out_var}.drop('_rn_last')")
        return lines, "converted"


# ---------------------------------------------------------------------------
# Java Filter
# ---------------------------------------------------------------------------


_JAVA_UNSUPPORTED = re.compile(
    r"\b(new\s+\w+|import\s+|instanceof|System\.|Math\.|String\.|"
    r"\.matches\s*\(|\.startsWith\s*\(|\.endsWith\s*\(|\.length\s*\(|\.substring\s*\()",
    re.I,
)


def _java_condition_to_pyspark(condition: str) -> tuple[str | None, list[str]]:
    """Best-effort conversion of simple Janino/Java filter expressions."""
    warnings: list[str] = []
    raw = (condition or "").strip()
    if not raw:
        return None, ["Java Filter condition is empty"]

    # Direct method mappings before unsupported check
    eq_ign = re.match(
        r'^([A-Za-z_][A-Za-z0-9_]*)\.equalsIgnoreCase\s*\(\s*["\']([^"\']*)["\']\s*\)$',
        raw,
    )
    if eq_ign:
        field, val = eq_ign.group(1), eq_ign.group(2)
        return f'lower(col("{field}")) == lit({val.lower()!r})', warnings

    eq_m = re.match(
        r'^([A-Za-z_][A-Za-z0-9_]*)\.equals\s*\(\s*["\']([^"\']*)["\']\s*\)$',
        raw,
    )
    if eq_m:
        field, val = eq_m.group(1), eq_m.group(2)
        return f'col("{field}") == lit({val!r})', warnings

    contains_m = re.match(
        r'^([A-Za-z_][A-Za-z0-9_]*)\.contains\s*\(\s*["\']([^"\']*)["\']\s*\)$',
        raw,
    )
    if contains_m:
        field, val = contains_m.group(1), contains_m.group(2)
        return f'col("{field}").contains(lit({val!r}))', warnings

    if _JAVA_UNSUPPORTED.search(raw) or re.search(
        r"\.equalsIgnoreCase\s*\(|\.contains\s*\(|\.equals\s*\(", raw, re.I
    ):
        # Complex / nested method usage not handled above
        if re.search(r"\.\w+\s*\(", raw):
            return None, [f"Unsupported Java constructs in condition: {raw!r}"]

    # Normalize Java operators and double-quoted string literals for Filter converter.
    normalized = raw
    normalized = re.sub(
        r'"((?:[^"\\]|\\.)*)"',
        lambda m: "'" + m.group(1).replace("'", "\\'") + "'",
        normalized,
    )
    normalized = normalized.replace("&&", " AND ").replace("||", " OR ")
    normalized = re.sub(r"(?<![!<>=])!(?!=)", " NOT ", normalized)
    normalized = normalized.replace("==", "=").replace("!=", "<>")

    try:
        expr = convert_simple_condition(normalized)
    except Exception as exc:  # noqa: BLE001
        return None, [f"Failed to convert Java condition: {exc}"]

    if expr and expr != "lit(False)" and "col(" in expr:
        return expr, warnings

    # Fallback heuristic for bare field comparisons
    simple = raw
    simple = re.sub(
        r'"((?:[^"\\]|\\.)*)"',
        lambda m: "'" + m.group(1).replace("'", "\\'") + "'",
        simple,
    )

    def _tok(m: re.Match[str]) -> str:
        name = m.group(0)
        lower = name.lower()
        if lower in {"true", "false", "null", "and", "or", "not"}:
            return {
                "true": "lit(True)",
                "false": "lit(False)",
                "null": "lit(None)",
            }.get(lower, name)
        if name.isdigit():
            return name
        return f'col("{name}")'

    parts = re.split(r"('(?:[^'\\]|\\.)*')", simple)
    for i in range(0, len(parts), 2):
        parts[i] = re.sub(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", _tok, parts[i])
        parts[i] = parts[i].replace("&&", " & ").replace("||", " | ")
        parts[i] = re.sub(r"(?<![!<>=])!(?!=)", "~", parts[i])
    attempted = "".join(parts)
    if "col(" in attempted:
        warnings.append("Java Filter converted with heuristic operator mapping")
        return attempted, warnings
    return None, [f"Could not convert Java Filter condition: {raw!r}"]


class JavaFilterHandler(BaseStepHandler):
    """Filter rows using a Java/Janino expression mapped to PySpark where possible."""

    _TYPES = {"javafilter"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_java_filter_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        condition = meta.get("condition") or "true"
        lines = [f"# Java Filter: {context.step.name}"]
        lines.extend(_preserve(meta, ("condition", "send_true_to", "send_false_to")))

        if not in_df:
            return _passthrough(context, "Java Filter")

        expr, warnings = _java_condition_to_pyspark(condition)
        for w in warnings:
            lines.append(f"# WARNING: {w}")
            _warn(context.step.name, w)

        if expr is None:
            lines.append(f"# UNSUPPORTED Java condition preserved: {condition!r}")
            lines.append(f"{out_var} = {in_df}  # migration required: rewrite Java Filter")
            return lines, "partial"

        true_target, false_target = _connected_branch_targets(meta, context, context.step.name)
        if true_target and false_target:
            true_var = _branch_stream_name(true_target)
            false_var = _branch_stream_name(false_target)
            lines.append(f"{true_var} = {in_df}.filter({expr})")
            lines.append(f"{false_var} = {in_df}.filter(~({expr}))")
            if out_var != true_var:
                lines.append(f"{out_var} = {true_var}")
            return lines, "converted" if not warnings else "partial"

        if false_target and not true_target:
            false_var = _branch_stream_name(false_target)
            lines.append(f"{false_var} = {in_df}.filter(~({expr}))")
            if out_var != false_var:
                lines.append(f"{out_var} = {false_var}")
            return lines, "converted" if not warnings else "partial"

        lines.append(f"{out_var} = {in_df}.filter({expr})")
        return lines, "converted" if not warnings else "partial"


# ---------------------------------------------------------------------------
# Job / Transformation Executor + Single Threader
# ---------------------------------------------------------------------------


def _executor_stub(
    context: StepContext,
    *,
    label: str,
    meta: dict,
    preserve_keys: tuple[str, ...],
    child_key: str,
    limitation: str,
) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [f"# {label}: {context.step.name}"]
    lines.extend(_preserve(meta, preserve_keys))
    lines.append(f"# LIMITATION: {limitation}")

    child = (
        meta.get("filename")
        or meta.get(child_key)
        or meta.get("directory_path")
        or ""
    )
    if not child:
        _warn(context.step.name, f"{label} missing child artifact reference")
        lines.append(f"# WARNING: missing child {child_key}/filename")

    lines.append(
        f"_exec_meta_{out_var} = {{"
        f"'child': {child!r}, "
        f"'parameters': {meta.get('parameters')!r}, "
        f"'group_size': {meta.get('group_size')!r}, "
        f"'group_field': {meta.get('group_field')!r}, "
        f"'group_time': {meta.get('group_time')!r}"
        f"}}"
    )
    lines.append(
        f"# TODO: dbutils.notebook.run / Jobs API using _exec_meta_{out_var}"
    )

    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_executor STRING').limit(0)")
    return lines, "partial"


class JobExecutorHandler(BaseStepHandler):
    """Preserve child job config; emit documented execution stub."""

    _TYPES = {"jobexecutor"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_job_executor_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)
        return _executor_stub(
            context,
            label="Job Executor",
            meta=meta,
            preserve_keys=(
                "specification_method", "job_name", "filename", "directory_path",
                "group_size", "group_field", "group_time", "parameters",
                "result_rows_target_step", "result_files_target_step",
                "execution_result_target_step", "inherit_all_variables",
            ),
            child_key="job_name",
            limitation=(
                "Nested PDI Job Executor has no Spark equivalent; "
                "map to Databricks Jobs / workflow orchestration outside the DataFrame pipeline."
            ),
        )


class TransExecutorHandler(BaseStepHandler):
    """Preserve child transformation config; emit documented execution stub."""

    _TYPES = {"transexecutor", "transformationexecutor"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_trans_executor_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)
        return _executor_stub(
            context,
            label="Transformation Executor",
            meta=meta,
            preserve_keys=(
                "specification_method", "trans_name", "filename", "directory_path",
                "group_size", "group_field", "group_time", "parameters",
                "result_rows_target_step", "result_files_target_step",
                "execution_result_target_step", "inherit_all_variables",
            ),
            child_key="trans_name",
            limitation=(
                "Nested Transformation Executor should become a separate notebook/job "
                "invoked with parameter mappings; row-batch grouping is not inlined."
            ),
        )


class SingleThreaderHandler(BaseStepHandler):
    """Preserve single-threaded sub-pipeline config; document distributed limits."""

    _TYPES = {"singlethreader"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_single_threader_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        lines = [f"# Single Threader: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "specification_method", "trans_name", "filename", "directory_path",
            "batch_size", "batch_time", "inject_step", "retrieve_step",
            "parameters", "pass_parameters",
        )))
        lines.append(
            "# LIMITATION: Spark executes distributively; single-threaded inject/retrieve "
            "sub-pipelines cannot be preserved. Inline or call child notebook sequentially."
        )
        child = meta.get("filename") or meta.get("trans_name") or ""
        lines.append(
            f"_single_threader_{out_var} = {{"
            f"'child': {child!r}, "
            f"'inject_step': {meta.get('inject_step')!r}, "
            f"'retrieve_step': {meta.get('retrieve_step')!r}, "
            f"'batch_size': {meta.get('batch_size')!r}, "
            f"'parameters': {meta.get('parameters')!r}"
            f"}}"
        )
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_single_threader STRING').limit(0)")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Prioritize Streams
# ---------------------------------------------------------------------------


class PrioritizeStreamsHandler(BaseStepHandler):
    """Merge multiple inputs preserving priority order via unionByName."""

    _TYPES = {"prioritystream", "prioritizestreams"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_prioritize_streams_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        priority = [s for s in (meta.get("stream_priority") or meta.get("streams") or []) if s]
        hop_preds = context.dag.predecessors(context.step.name)

        ordered_names: list[str] = []
        for name in priority:
            if name not in ordered_names:
                ordered_names.append(name)
        for name in hop_preds:
            if name not in ordered_names:
                ordered_names.append(name)

        dfs = [_df_for(context, n) for n in ordered_names]
        lines = [f"# Prioritize Streams: {context.step.name}"]
        lines.extend(_preserve(meta, ("stream_priority", "streams")))
        lines.append(f"# Merge order (high → low priority): {ordered_names!r}")
        lines.append(
            "# Note: Pentaho priority-drain (consume higher-priority streams first) "
            "is approximated by ordered unionByName + sort by priority tag"
        )

        if len(dfs) >= 2:
            # Tag priority then union so higher-priority rows sort first if needed.
            tagged = []
            for idx, df in enumerate(dfs):
                tagged.append(f"{df}.withColumn('_prio', lit({idx}))")
            expr = tagged[0]
            for nxt in tagged[1:]:
                expr = f"{expr}.unionByName({nxt}, allowMissingColumns=True)"
            lines.append(f"{out_var} = ({expr}).orderBy(col('_prio')).drop('_prio')")
            return lines, "converted"

        if len(dfs) == 1:
            lines.append(f"{out_var} = {dfs[0]}")
            return lines, "partial"

        lines.append(f"{out_var} = spark.createDataFrame([], '_priority STRING').limit(0)")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Switch / Case
# ---------------------------------------------------------------------------


class SwitchCaseHandler(BaseStepHandler):
    """Route rows to case/default branch DataFrames using when()/otherwise()."""

    _TYPES = {"switchcase"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_switch_case_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        switch_field = meta.get("switch_field") or meta.get("fieldname") or ""
        cases = meta.get("cases") or []
        default_target = (meta.get("default_target_step") or "").strip()
        use_contains = bool(meta.get("use_contains"))
        value_type = meta.get("case_value_type") or "String"

        lines = [f"# Switch / Case: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "fieldname", "switch_field", "cases", "default_target_step",
            "use_contains", "case_value_type", "case_value_format",
            "case_value_decimal", "case_value_group",
        )))

        if not in_df:
            return _passthrough(context, "Switch / Case")

        if not switch_field or not cases:
            lines.append("# WARNING: Switch / Case missing field or case mappings")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        def _case_lit(val: str) -> str:
            return _literal_expr(value_type, val)

        # Build when/otherwise route expression
        first = cases[0]
        first_val = first.get("value", "") if isinstance(first, dict) else getattr(first, "value", "")
        first_tgt = first.get("target_step", "") if isinstance(first, dict) else getattr(first, "target_step", "")
        first_lit = _case_lit(first_val)
        if use_contains:
            expr = (
                f'when(col("{switch_field}").cast("string").contains('
                f'{first_lit}.cast("string")), lit({first_tgt!r}))'
            )
        else:
            expr = (
                f'when(col("{switch_field}") == {first_lit}, lit({first_tgt!r}))'
            )
        for case in cases[1:]:
            val = case.get("value", "") if isinstance(case, dict) else getattr(case, "value", "")
            tgt = case.get("target_step", "") if isinstance(case, dict) else getattr(case, "target_step", "")
            case_lit = _case_lit(val)
            if use_contains:
                expr = (
                    f'{expr}.when(col("{switch_field}").cast("string").contains('
                    f'{case_lit}.cast("string")), lit({tgt!r}))'
                )
            else:
                expr = (
                    f'{expr}.when(col("{switch_field}") == {case_lit}, lit({tgt!r}))'
                )
        if default_target:
            expr = f"{expr}.otherwise(lit({default_target!r}))"
        else:
            expr = f"{expr}.otherwise(lit(''))"

        route_col = f"_route_{context.step.name.replace(' ', '_').replace('-', '_')}"
        lines.append(f"_routed_{out_var} = {in_df}.withColumn('{route_col}', {expr})")

        # Emit per-target branch DataFrames (Filter Rows style)
        successors = set(context.dag.successors(context.step.name))
        targets: list[str] = []
        for case in cases:
            tgt = case.get("target_step", "") if isinstance(case, dict) else getattr(case, "target_step", "")
            if tgt and tgt not in targets:
                targets.append(tgt)
        if default_target and default_target not in targets:
            targets.append(default_target)

        emitted = False
        for tgt in targets:
            if successors and tgt not in successors:
                continue
            branch_var = _branch_stream_name(tgt)
            lines.append(
                f"{branch_var} = _routed_{out_var}.filter(col('{route_col}') == lit({tgt!r}))"
                f".drop('{route_col}')"
            )
            emitted = True

        if not emitted:
            lines.append(
                f"# No connected case targets found; primary output keeps route column"
            )
            lines.append(f"{out_var} = _routed_{out_var}")
            return lines, "partial"

        # Primary output = first connected case (or default) without route column pollution
        primary = targets[0] if not successors else next(
            (t for t in targets if t in successors), targets[0]
        )
        primary_var = _branch_stream_name(primary)
        if out_var != primary_var:
            lines.append(f"{out_var} = {primary_var}")
        return lines, "converted"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Dummy + Filter Rows remain in advanced_handlers / transform_handlers.
FLOW_HANDLERS: list[BaseStepHandler] = [
    AbortHandler(),
    AppendStreamsHandler(),
    BlockUntilStepsFinishHandler(),
    BlockingStepHandler(),
    DetectEmptyStreamHandler(),
    MetaInjectHandler(),
    IdentifyLastRowHandler(),
    JavaFilterHandler(),
    JobExecutorHandler(),
    PrioritizeStreamsHandler(),
    SingleThreaderHandler(),
    SwitchCaseHandler(),
    TransExecutorHandler(),
]
