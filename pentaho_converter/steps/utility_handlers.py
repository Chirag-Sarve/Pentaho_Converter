"""Handlers for Pentaho Utility transformation steps.

Supports:
- Clone Row, Null If, If Field Value Is Null (via advanced IfNullHandler enhancement)
- Delay Row, Metadata Structure of Stream, Write to Log, Table Compare
- Change File Encoding, Zip File, Process Files
- Execute a Process, Run SSH Commands, Mail, Syslog, EDI to XML
"""

from __future__ import annotations

import logging
import re

from ..lineage import substitute_pentaho_variables
from ..metadata_propagation import get_converter_metadata
from ..path_utils import spark_load_path_expr, spark_save_path_expr
from ..step_xml import (
    get_step_element,
    parse_change_file_encoding_config,
    parse_clone_row_config,
    parse_delay_row_config,
    parse_edi_to_xml_config,
    parse_exec_process_config,
    parse_mail_config,
    parse_meta_structure_config,
    parse_null_if_config,
    parse_process_files_config,
    parse_ssh_config,
    parse_syslog_config,
    parse_table_compare_config,
    parse_write_to_log_config,
    parse_zip_file_config,
)
from .base import BaseStepHandler, StepContext
from .field_transform_handlers import _lit_for_value

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return step_type.strip().lower().replace(" ", "").replace("(", "").replace(")", "")


def _meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


def _params(context: StepContext) -> dict:
    return context.transformation.parameters or {}


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
    "_propagated_keys", "_propagation_trace",
})
_REDACT_KEYS = frozenset({
    "password", "auth_password", "passphrase", "proxy_password",
})


def _preserve(meta: dict, keys: tuple[str, ...] = ()) -> list[str]:
    """Preserve curated keys first, then any residual parser metadata."""
    lines: list[str] = []
    seen: set[str] = set()

    def _emit(key: str, val: object) -> None:
        if key in _REDACT_KEYS or "password" in key.lower() or "passphrase" in key.lower():
            lines.append(f"# preserved.{key}=<redacted>")
        else:
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
    logger.warning("Utility step '%s': %s", step_name, message)


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _resolve_str(context: StepContext, value: str) -> str:
    return substitute_pentaho_variables(value or "", _params(context))


# ---------------------------------------------------------------------------
# Clone Row
# ---------------------------------------------------------------------------


class CloneRowHandler(BaseStepHandler):
    """Duplicate each input row N times (plus optional clone flag/number)."""

    _TYPES = {"clonerow", "clonerows"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_clone_row_config(step_el) if step_el is not None else {}
        for key, val in cfg.items():
            meta.setdefault(key, val)

        nr = int(meta.get("nr_clones") or 0)
        nr_in_field = bool(meta.get("nr_clone_in_field"))
        nr_field = meta.get("nr_clone_field") or ""
        add_flag = bool(meta.get("add_clone_flag"))
        flag_field = meta.get("clone_flag_field") or "cloneflag"
        add_num = bool(meta.get("add_clone_num"))
        num_field = meta.get("clone_num_field") or "clonenum"

        lines = [f"# Clone Row: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "nr_clones", "nr_clone_in_field", "nr_clone_field",
            "add_clone_flag", "clone_flag_field", "add_clone_num", "clone_num_field",
        )))

        if not in_df:
            return _passthrough(context, "Clone Row")

        if nr_in_field and nr_field:
            # Each row duplicated (1 + field value) times; index 0 = original.
            lines.append(
                f"{out_var} = {in_df}.withColumn('_clone_n', "
                f"greatest(coalesce(col({nr_field!r}).cast('int'), lit(0)), lit(0)))"
            )
            lines.append(
                f"{out_var} = {out_var}.withColumn('_clone_i', "
                f"explode(sequence(lit(0), col('_clone_n'))))"
            )
            if add_flag:
                lines.append(
                    f"{out_var} = {out_var}.withColumn({flag_field!r}, col('_clone_i') > lit(0))"
                )
            if add_num:
                lines.append(
                    f"{out_var} = {out_var}.withColumn({num_field!r}, col('_clone_i'))"
                )
            lines.append(f"{out_var} = {out_var}.drop('_clone_n', '_clone_i')")
            return lines, "converted"

        if nr <= 0:
            _warn(context.step.name, f"nr_clones={nr} — no duplicates; passthrough")
            lines.append(f"# WARNING: Clone Row nr_clones={nr} — no duplicates; passthrough")
            lines.append(f"{out_var} = {in_df}")
            if add_flag:
                lines.append(f"{out_var} = {out_var}.withColumn({flag_field!r}, lit(False))")
            if add_num:
                lines.append(f"{out_var} = {out_var}.withColumn({num_field!r}, lit(0))")
            return lines, "partial"

        # Original + nr additional clones
        lines.append(f"_clone_parts_{out_var} = []")
        lines.append(f"_base_{out_var} = {in_df}")
        if add_flag:
            lines.append(
                f"_orig_{out_var} = _base_{out_var}.withColumn({flag_field!r}, lit(False))"
            )
        else:
            lines.append(f"_orig_{out_var} = _base_{out_var}")
        if add_num:
            lines.append(f"_orig_{out_var} = _orig_{out_var}.withColumn({num_field!r}, lit(0))")
        lines.append(f"_clone_parts_{out_var}.append(_orig_{out_var})")
        lines.append(f"for _ci in range(1, {nr} + 1):")
        lines.append(f"    _c = _base_{out_var}")
        if add_flag:
            lines.append(f"    _c = _c.withColumn({flag_field!r}, lit(True))")
        if add_num:
            lines.append(f"    _c = _c.withColumn({num_field!r}, lit(_ci))")
        lines.append(f"    _clone_parts_{out_var}.append(_c)")
        lines.append(
            f"{out_var} = _clone_parts_{out_var}[0]"
        )
        lines.append(f"for _part in _clone_parts_{out_var}[1:]:")
        lines.append(f"    {out_var} = {out_var}.unionByName(_part, allowMissingColumns=True)")
        return lines, "converted"


# ---------------------------------------------------------------------------
# Null If
# ---------------------------------------------------------------------------


class NullIfHandler(BaseStepHandler):
    """Set field values to null when they match configured comparison values."""

    _TYPES = {"nullif"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if not meta.get("fields") and step_el is not None:
            meta.update(parse_null_if_config(step_el))

        fields = meta.get("fields") or []
        lines = [f"# Null If: {context.step.name}"]
        lines.extend(_preserve(meta, ("fields",)))

        if not in_df:
            return _passthrough(context, "Null If")

        if not fields:
            _warn(context.step.name, "Null If has no fields configured")
            lines.append("# WARNING: Null If has no fields configured")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        lines.append(f"{out_var} = {in_df}")
        for item in fields:
            name = item.get("name") or ""
            if not name:
                continue
            value = item.get("value", "")
            pdi_type = item.get("type") or ""
            if pdi_type:
                lines.append(f"# preserved.field_type.{name}={pdi_type!r}")
            # Empty comparison value → treat empty string / null as sentinel
            if value == "":
                cmp_expr = f"(col({name!r}).isNull() | (col({name!r}).cast('string') == lit('')))"
            else:
                lit_v = _lit_for_value(str(value))
                cmp_expr = f"(col({name!r}) == {lit_v})"
            # otherwise(col) preserves the original Spark dtype; lit(None) alone would widen to NullType
            lines.append(
                f"{out_var} = {out_var}.withColumn({name!r}, "
                f"when({cmp_expr}, lit(None)).otherwise(col({name!r})))"
            )
        return lines, "converted"


# ---------------------------------------------------------------------------
# Delay Row
# ---------------------------------------------------------------------------


class DelayRowHandler(BaseStepHandler):
    """Document per-row delay (unsupported in distributed Spark) and passthrough."""

    _TYPES = {"delay", "delayrow"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_delay_row_config(step_el).items():
                meta.setdefault(k, v)

        timeout = meta.get("timeout", 0)
        scale = meta.get("scale_time") or "milliseconds"
        lines = [f"# Delay Row: {context.step.name}"]
        lines.extend(_preserve(meta, ("timeout", "scale_time")))
        lines.append(
            f"# WARNING: per-row delay ({timeout} {scale}) is not supported in distributed "
            "Spark/Databricks — sleeping would block executors and break parallelism"
        )
        _warn(
            context.step.name,
            f"Delay Row ({timeout} {scale}) cannot run per-row on distributed executors",
        )
        lines.append(
            "# NOTE: For throttling, consider rate-limited Structured Streaming or "
            "external orchestration delays between jobs"
        )
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Metadata Structure of Stream
# ---------------------------------------------------------------------------


class MetaStructureHandler(BaseStepHandler):
    """Emit input schema metadata as a DataFrame (Position/Fieldname/Type/...)."""

    _TYPES = {"metastructure", "stepmetastructure", "metadatastructureofstream"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_meta_structure_config(step_el).items():
                meta.setdefault(k, v)

        pos_f = meta.get("position_field") or "Position"
        name_f = meta.get("fieldname_field") or "Fieldname"
        comments_f = meta.get("comments_field") or "Comments"
        type_f = meta.get("type_field") or "Type"
        length_f = meta.get("length_field") or "Length"
        precision_f = meta.get("precision_field") or "Precision"
        origin_f = meta.get("origin_field") or "Origin"
        output_rowcount = bool(meta.get("output_rowcount"))
        rowcount_f = meta.get("rowcount_field") or "rowcount"

        lines = [f"# Metadata Structure of Stream: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "position_field", "fieldname_field", "comments_field", "type_field",
            "length_field", "precision_field", "origin_field",
            "output_rowcount", "rowcount_field",
        )))

        if not in_df:
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"[], '{pos_f} INT, {name_f} STRING, {comments_f} STRING, "
                f"{type_f} STRING, {length_f} INT, {precision_f} INT, {origin_f} STRING')"
            )
            return lines, "partial"

        lines.append(f"_ms_schema_{out_var} = {in_df}.schema")
        lines.append(f"_ms_rows_{out_var} = []")
        lines.append(f"for _ms_i, _ms_f in enumerate(_ms_schema_{out_var}.fields, start=1):")
        lines.append(
            f"    _ms_len = getattr(_ms_f.dataType, 'length', None)"
        )
        lines.append(
            f"    if _ms_len is None:"
        )
        lines.append(
            f"        _ms_len = getattr(_ms_f.dataType, 'precision', None)"
        )
        lines.append(
            f"    _ms_prec = getattr(_ms_f.dataType, 'scale', None)"
        )
        lines.append(
            f"    _ms_rows_{out_var}.append(("
            f"_ms_i, _ms_f.name, "
            f"str((_ms_f.metadata or {{}}).get('comment', '')), "
            f"_ms_f.dataType.simpleString(), _ms_len, _ms_prec, {context.step.name!r}))"
        )
        lines.append(
            f"{out_var} = spark.createDataFrame("
            f"_ms_rows_{out_var}, "
            f"'{pos_f} INT, {name_f} STRING, {comments_f} STRING, "
            f"{type_f} STRING, {length_f} INT, {precision_f} INT, {origin_f} STRING')"
        )
        if output_rowcount:
            lines.append(
                f"{out_var} = {out_var}.withColumn({rowcount_f!r}, lit(len(_ms_rows_{out_var})))"
            )
        lines.append(
            f"# Schema metadata propagated from input DataFrame '{in_df}'"
        )
        return lines, "converted"


# ---------------------------------------------------------------------------
# Write to Log
# ---------------------------------------------------------------------------


class WriteToLogHandler(BaseStepHandler):
    """Structured logging of selected columns (passthrough stream)."""

    _TYPES = {"writetolog"}

    _LEVEL_MAP = {
        "nothing": "DEBUG",
        "error": "ERROR",
        "minimal": "WARNING",
        "basic": "INFO",
        "detailed": "INFO",
        "debug": "DEBUG",
        "rowlevel": "DEBUG",
    }

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_write_to_log_config(step_el).items():
                meta.setdefault(k, v)

        raw_level = str(meta.get("log_level") or "Basic")
        py_level = self._LEVEL_MAP.get(raw_level.strip().lower(), "INFO")
        fields = meta.get("fields") or []
        limit_rows = bool(meta.get("limit_rows"))
        limit_n = int(meta.get("limit_rows_number") or 10)
        subject = meta.get("log_subject") or context.step.name
        message = meta.get("log_message") or ""
        display_header = bool(meta.get("display_header"))

        lines = [f"# Write to Log: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "log_level", "log_subject", "log_message", "display_header",
            "limit_rows", "limit_rows_number", "fields",
        )))
        lines.append("import logging")
        lines.append(
            f"_log_{_safe_ident(out_var)} = logging.getLogger("
            f"'pentaho.writetolog.{_safe_ident(context.step.name)}')"
        )
        lines.append(f"_log_{_safe_ident(out_var)}.setLevel(logging.{py_level})")

        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "partial"

        sample_n = limit_n if limit_rows else 20
        lines.append(
            f"# NOTE: sampling up to {sample_n} rows for logging "
            "(avoid collect() on full partitions)"
        )
        if fields:
            cols_list = ", ".join(repr(f) for f in fields if f)
            lines.append(f"_log_df_{out_var} = {in_df}.select({cols_list})")
        else:
            lines.append(f"_log_df_{out_var} = {in_df}")
        lines.append(f"_log_rows_{out_var} = _log_df_{out_var}.take({sample_n})")
        if display_header:
            lines.append(
                f"_log_{_safe_ident(out_var)}.{py_level.lower()}("
                f"{subject!r} + ' | columns=' + str(_log_df_{out_var}.columns))"
            )
        if message:
            lines.append(
                f"_log_{_safe_ident(out_var)}.{py_level.lower()}({message!r})"
            )
        lines.append(f"for _lr in _log_rows_{out_var}:")
        lines.append(
            f"    _log_{_safe_ident(out_var)}.{py_level.lower()}("
            f"{subject!r} + ' | ' + str(_lr.asDict()))"
        )
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


# ---------------------------------------------------------------------------
# Table Compare
# ---------------------------------------------------------------------------


class TableCompareHandler(BaseStepHandler):
    """Compare two tables on keys and emit difference classification."""

    _TYPES = {"tablecompare"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_table_compare_config(step_el).items():
                meta.setdefault(k, v)

        ref_schema = meta.get("reference_schema") or ""
        cmp_schema = meta.get("compare_schema") or ""
        ref_table = meta.get("reference_table") or ""
        cmp_table = meta.get("compare_table") or ""
        keys = list(meta.get("key_fields") or [])
        exclude = set(meta.get("exclude_fields") or [])

        def _fq(schema: str, table: str) -> str:
            if schema and table:
                return f"{schema}.{table}"
            return table or schema

        ref_fq = _fq(ref_schema, ref_table)
        cmp_fq = _fq(cmp_schema, cmp_table)

        lines = [f"# Table Compare: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "reference_connection", "compare_connection",
            "reference_schema", "compare_schema",
            "reference_table", "compare_table",
            "key_fields", "exclude_fields",
            "nr_errors_field", "nr_records_reference_field", "nr_records_compare_field",
            "key_desc_field", "value_reference_field", "value_compare_field",
        )))

        if not ref_fq or not cmp_fq:
            lines.append("# WARNING: Table Compare missing reference/compare table names")
            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "partial"

        lines.append(f"_tc_ref_{out_var} = spark.table({ref_fq!r})")
        lines.append(f"_tc_cmp_{out_var} = spark.table({cmp_fq!r})")
        lines.append(f"_tc_ref_n_{out_var} = _tc_ref_{out_var}.count()")
        lines.append(f"_tc_cmp_n_{out_var} = _tc_cmp_{out_var}.count()")

        if keys:
            join_cond = " & ".join(
                f'(col("r.{k}") == col("c.{k}"))' for k in keys
            )
            lines.append(f'_tc_r_{out_var} = _tc_ref_{out_var}.alias("r")')
            lines.append(f'_tc_c_{out_var} = _tc_cmp_{out_var}.alias("c")')
            lines.append(
                f"{out_var} = _tc_r_{out_var}.join(_tc_c_{out_var}, {join_cond}, 'full_outer')"
            )
            key0 = keys[0]
            lines.append(
                f"{out_var} = {out_var}.withColumn('_tc_diff', "
                f'when(col("c.{key0}").isNull(), lit("missing_in_compare"))'
                f'.when(col("r.{key0}").isNull(), lit("missing_in_reference"))'
                f'.otherwise(lit("matched")))'
            )
            # Value diffs on non-key, non-excluded columns present on both sides
            lines.append(
                f"_tc_compare_cols_{out_var} = ["
                f"c for c in _tc_ref_{out_var}.columns "
                f"if c in _tc_cmp_{out_var}.columns "
                f"and c not in {keys!r} and c not in {sorted(exclude)!r}]"
            )
            lines.append(f"_tc_val_diff_{out_var} = lit(False)")
            lines.append(f"for _col in _tc_compare_cols_{out_var}:")
            lines.append(
                f"    _tc_val_diff_{out_var} = _tc_val_diff_{out_var} | "
                f'(coalesce(col(f"r.{{_col}}").cast("string"), lit("")) != '
                f'coalesce(col(f"c.{{_col}}").cast("string"), lit("")))'
            )
            lines.append(
                f"{out_var} = {out_var}.withColumn('_tc_diff', "
                f'when(col("_tc_diff") == lit("matched"), '
                f'when(_tc_val_diff_{out_var}, lit("value_diff")).otherwise(lit("identical")))'
                f'.otherwise(col("_tc_diff")))'
            )
        else:
            lines.append("# WARNING: no key_fields — using exceptAll for set difference")
            lines.append(
                f"_tc_only_ref_{out_var} = _tc_ref_{out_var}.exceptAll(_tc_cmp_{out_var})"
                f'.withColumn("_tc_diff", lit("only_in_reference"))'
            )
            lines.append(
                f"_tc_only_cmp_{out_var} = _tc_cmp_{out_var}.exceptAll(_tc_ref_{out_var})"
                f'.withColumn("_tc_diff", lit("only_in_compare"))'
            )
            lines.append(
                f"{out_var} = _tc_only_ref_{out_var}.unionByName("
                f"_tc_only_cmp_{out_var}, allowMissingColumns=True)"
            )

        nr_err = meta.get("nr_errors_field") or "nrErrors"
        nr_ref = meta.get("nr_records_reference_field") or "nrRecordsReference"
        nr_cmp = meta.get("nr_records_compare_field") or "nrRecordsCompare"
        lines.append(
            f"{out_var} = {out_var}"
            f".withColumn({nr_ref!r}, lit(_tc_ref_n_{out_var}))"
            f".withColumn({nr_cmp!r}, lit(_tc_cmp_n_{out_var}))"
            f'.withColumn({nr_err!r}, '
            f'lit({out_var}.filter(col("_tc_diff") != lit("identical")).count()))'
        )
        if in_df:
            lines.append(
                f"# NOTE: hop input '{in_df}' ignored — Table Compare reads configured tables"
            )
        return lines, "converted"


# ---------------------------------------------------------------------------
# Change File Encoding
# ---------------------------------------------------------------------------


class ChangeFileEncodingHandler(BaseStepHandler):
    """Convert a text file from source encoding to target encoding."""

    _TYPES = {"changefileencoding", "fileencoding"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_change_file_encoding_config(step_el).items():
                meta.setdefault(k, v)

        src = _resolve_str(context, str(meta.get("source_file") or ""))
        dst = _resolve_str(context, str(meta.get("target_file") or ""))
        src_enc = meta.get("source_encoding") or "UTF-8"
        dst_enc = meta.get("target_encoding") or "UTF-8"
        src_field = meta.get("source_file_field") or ""
        dst_field = meta.get("target_file_field") or ""

        lines = [f"# Change File Encoding: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "source_file", "target_file", "source_encoding", "target_encoding",
            "source_file_field", "target_file_field", "create_parent_folder",
        )))

        if not src and not src_field:
            lines.append("# WARNING: missing source file / field — cannot convert encoding")
            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "partial"

        lines.append("import pathlib")
        if src_field and in_df:
            lines.append(
                f"# Dynamic paths from stream fields source={src_field!r} target={dst_field!r}"
            )
            lines.append(f"for _row in {in_df}.select("
                         f"{src_field!r}"
                         + (f", {dst_field!r}" if dst_field else "")
                         + f").distinct().collect():")
            lines.append(f"    _src = str(_row[{src_field!r}])")
            if dst_field:
                lines.append(f"    _dst = str(_row[{dst_field!r}])")
            else:
                lines.append(f"    _dst = _src + '.encoded'")
            lines.append(f"    try:")
            lines.append(
                f"        _text = pathlib.Path(_src).read_text(encoding={src_enc!r})"
            )
            lines.append(
                f"        pathlib.Path(_dst).write_text(_text, encoding={dst_enc!r})"
            )
            lines.append(f"    except (OSError, LookupError, UnicodeError) as _exc:")
            lines.append(
                f"        print(f'Change File Encoding failed for {{_src}}: {{_exc}}')"
            )
        else:
            src_expr = spark_load_path_expr(src) if src else "''"
            dst_expr = spark_save_path_expr(dst) if dst else "''"
            lines.append(f"_cfe_src = {src_expr}")
            lines.append(f"_cfe_dst = {dst_expr}")
            lines.append("try:")
            lines.append(
                f"    _cfe_text = pathlib.Path(_cfe_src).read_text(encoding={src_enc!r})"
            )
            lines.append(
                f"    pathlib.Path(_cfe_dst).write_text(_cfe_text, encoding={dst_enc!r})"
            )
            lines.append("except (OSError, LookupError, UnicodeError) as _cfe_exc:")
            lines.append(
                "    print(f'Change File Encoding failed: {_cfe_exc}')  # invalid encoding/path"
            )
            lines.append(
                f"# preserved.source_encoding={src_enc!r} target_encoding={dst_enc!r}"
            )

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Zip File
# ---------------------------------------------------------------------------


class ZipFileHandler(BaseStepHandler):
    """Create a ZIP archive from stream filename fields."""

    _TYPES = {"zipfile"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_zip_file_config(step_el).items():
                meta.setdefault(k, v)

        zip_name = _resolve_str(context, str(meta.get("zip_filename") or ""))
        zip_field = meta.get("zip_filename_field") or ""
        src_field = meta.get("source_filename_field") or ""
        compression = str(meta.get("compression") or "DEFLATED").upper()
        keep_folder = bool(meta.get("keep_source_folder"))
        overwrite = bool(meta.get("overwrite_zip_entry"))

        lines = [f"# Zip File: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "zip_filename", "zip_filename_field", "source_filename_field",
            "target_filename_field", "compression", "overwrite_zip_entry",
            "create_parent_folder", "keep_source_folder", "after_zip",
            "move_to_folder", "include_subfolders",
        )))

        if not src_field:
            lines.append("# WARNING: Zip File missing source_filename_field")
            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "partial"

        zip_mode = "a" if overwrite else "w"
        comp_const = "zipfile.ZIP_DEFLATED" if "DEFLAT" in compression or compression == "DEFAULT" else (
            "zipfile.ZIP_STORED" if "STORE" in compression else "zipfile.ZIP_DEFLATED"
        )

        lines.append("import zipfile, pathlib, os")
        if zip_field:
            lines.append(f"# Zip path taken from field {zip_field!r}")
        elif zip_name:
            lines.append(f"_zip_path = {spark_save_path_expr(zip_name)}")
        else:
            lines.append(f"_zip_path = f'{{PENTAHO_DATA_DIR}}/archive.zip'")
            lines.append("# WARNING: no zip_filename configured — using default archive.zip")

        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "partial"

        select_cols = [src_field] + ([zip_field] if zip_field else [])
        cols_repr = ", ".join(repr(c) for c in select_cols)
        lines.append(f"_zip_rows = {in_df}.select({cols_repr}).distinct().collect()")
        lines.append("_zip_groups = {}")
        lines.append("for _zr in _zip_rows:")
        if zip_field:
            lines.append(f"    _zp = str(_zr[{zip_field!r}])")
        else:
            lines.append("    _zp = _zip_path")
        lines.append(f"    _zip_groups.setdefault(_zp, []).append(str(_zr[{src_field!r}]))")
        lines.append("for _zp, _files in _zip_groups.items():")
        lines.append("    pathlib.Path(_zp).parent.mkdir(parents=True, exist_ok=True)")
        lines.append(f"    with zipfile.ZipFile(_zp, {zip_mode!r}, compression={comp_const}) as _zf:")
        lines.append("        for _src in _files:")
        lines.append("            if not _src or not os.path.exists(_src):")
        lines.append("                print(f'Zip File: missing source {_src!r}')")
        lines.append("                continue")
        if keep_folder:
            lines.append("            _arc = _src.replace('\\\\', '/')" )
        else:
            lines.append("            _arc = os.path.basename(_src)")
        lines.append("            try:")
        lines.append("                _zf.write(_src, arcname=_arc)")
        lines.append("            except OSError as _zexc:")
        lines.append("                print(f'Zip File write failed for {_src}: {_zexc}')")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Process Files
# ---------------------------------------------------------------------------


class ProcessFilesHandler(BaseStepHandler):
    """Copy / move / delete files named in the stream."""

    _TYPES = {"processfiles"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_process_files_config(step_el).items():
                meta.setdefault(k, v)

        operation = str(meta.get("operation") or "copy").lower()
        src_field = meta.get("source_filename_field") or ""
        tgt_field = meta.get("target_filename_field") or ""
        overwrite = bool(meta.get("overwrite_target"))
        create_parent = bool(meta.get("create_parent_folder"))
        simulate = bool(meta.get("simulate"))

        lines = [f"# Process Files: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "operation", "source_filename_field", "target_filename_field",
            "overwrite_target", "create_parent_folder", "add_result_filenames", "simulate",
        )))

        if not src_field:
            lines.append("# WARNING: Process Files missing source_filename_field")
            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "partial"

        lines.append("import shutil, os, pathlib")
        lines.append(
            "# WARNING: local filesystem ops may require driver-local or mounted Volumes paths "
            "on Databricks"
        )
        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "partial"

        cols = [src_field] + ([tgt_field] if tgt_field and operation != "delete" else [])
        lines.append(
            f"for _pf in {in_df}.select({', '.join(repr(c) for c in cols)}).distinct().collect():"
        )
        lines.append(f"    _src = str(_pf[{src_field!r}])")
        if operation != "delete" and tgt_field:
            lines.append(f"    _dst = str(_pf[{tgt_field!r}])")
        lines.append(f"    if {simulate!r}:")
        lines.append(f"        print(f'Process Files simulate {operation}: {{_src}}')")
        lines.append("        continue")
        lines.append("    try:")
        if operation == "delete":
            lines.append("        if os.path.exists(_src):")
            lines.append("            os.remove(_src)")
            lines.append("        else:")
            lines.append("            print(f'Process Files: missing file {_src!r}')")
        elif operation == "move":
            if create_parent:
                lines.append("        pathlib.Path(_dst).parent.mkdir(parents=True, exist_ok=True)")
            lines.append(f"        if os.path.exists(_dst) and not {overwrite!r}:")
            lines.append("            print(f'Process Files: target exists {_dst!r}')")
            lines.append("        else:")
            lines.append("            shutil.move(_src, _dst)")
        else:  # copy
            if create_parent:
                lines.append("        pathlib.Path(_dst).parent.mkdir(parents=True, exist_ok=True)")
            lines.append(f"        if os.path.exists(_dst) and not {overwrite!r}:")
            lines.append("            print(f'Process Files: target exists {_dst!r}')")
            lines.append("        else:")
            lines.append("            shutil.copy2(_src, _dst)")
        lines.append("    except OSError as _pf_exc:")
        lines.append("        print(f'Process Files failed: {_pf_exc}')")
        lines.append(f"{out_var} = {in_df}")
        return lines, "partial"


# ---------------------------------------------------------------------------
# Execute a Process / SSH / Mail / Syslog / EDI — mostly metadata + warnings
# ---------------------------------------------------------------------------


class ExecProcessHandler(BaseStepHandler):
    """Preserve Execute a Process config; warn that OS exec is unsupported on Databricks."""

    _TYPES = {"execprocess", "executeaprocess"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_exec_process_config(step_el).items():
                meta.setdefault(k, v)

        process_field = meta.get("process_field") or ""
        executable = meta.get("executable") or ""
        out_f = meta.get("output_field") or "outputLine"
        err_f = meta.get("error_field") or "errorLine"
        exit_f = meta.get("exit_value_field") or "exitValue"

        lines = [f"# Execute a Process: {context.step.name}"]
        lines.append(
            "# UNSUPPORTED: arbitrary OS process execution is not supported on Databricks "
            "shared clusters (use Jobs/init scripts or external orchestration instead)"
        )
        lines.append("# WARNING: Execute a Process cannot run arbitrary executables on Databricks")
        _warn(context.step.name, "Execute a Process is unsupported on Databricks clusters")
        lines.extend(_preserve(meta, (
            "process_field", "executable", "arguments", "argument_fields",
            "output_field", "error_field", "exit_value_field", "fail_when_nonzero",
            "output_delimited", "output_delimiter",
        )))
        if not executable and not process_field:
            lines.append("# WARNING: missing executable / process field")
        if in_df:
            lines.append(f"{out_var} = {in_df}")
            lines.append(f"{out_var} = {out_var}.withColumn({out_f!r}, lit(None).cast('string'))")
            lines.append(f"{out_var} = {out_var}.withColumn({err_f!r}, lit(None).cast('string'))")
            lines.append(f"{out_var} = {out_var}.withColumn({exit_f!r}, lit(None).cast('int'))")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"


class SSHHandler(BaseStepHandler):
    """Preserve Run SSH Commands config; warn that remote exec is unsupported."""

    _TYPES = {"ssh", "runsshcommands"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_ssh_config(step_el).items():
                meta.setdefault(k, v)

        server = meta.get("server") or ""
        username = meta.get("username") or ""
        command = meta.get("command") or ""
        key_file = meta.get("key_file") or ""
        password = meta.get("password") or ""
        stdout_f = meta.get("stdout_field") or "stdOut"
        stderr_f = meta.get("stderr_field") or "stdErr"

        lines = [f"# Run SSH Commands: {context.step.name}"]
        lines.append(
            "# UNSUPPORTED: direct SSH execution from Spark workers is not supported on "
            "Databricks — preserve config and use Jobs/API/orchestration instead"
        )
        lines.append("# WARNING: Run SSH Commands cannot execute remote shells from Databricks jobs")
        _warn(context.step.name, "SSH execution is unsupported on Databricks")
        lines.extend(_preserve(meta, (
            "server", "port", "username", "password", "passphrase",
            "use_private_key", "key_file",
            "command", "command_field", "use_command_field",
            "stdout_field", "stderr_field", "timeout",
            "proxy_host", "proxy_port", "proxy_username",
        )))
        if not server:
            lines.append("# WARNING: missing SSH host")
        if not username:
            lines.append("# WARNING: missing SSH username")
        if not password and not key_file and not meta.get("use_private_key"):
            lines.append("# WARNING: missing SSH credentials (password / private key)")
        if not command and not meta.get("command_field"):
            lines.append("# WARNING: missing SSH command")

        if in_df:
            lines.append(f"{out_var} = {in_df}")
            lines.append(f"{out_var} = {out_var}.withColumn({stdout_f!r}, lit(None).cast('string'))")
            lines.append(f"{out_var} = {out_var}.withColumn({stderr_f!r}, lit(None).cast('string'))")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"


class MailHandler(BaseStepHandler):
    """Preserve SMTP Mail configuration and emit a documented stub."""

    _TYPES = {"mail", "sendmail"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_mail_config(step_el).items():
                meta.setdefault(k, v)

        lines = [f"# Mail: {context.step.name}"]
        lines.append(
            "# UNSUPPORTED: SMTP send from Spark executors is not native on Databricks — "
            "use a notebook/Job task with smtplib or an external notification service"
        )
        lines.append("# WARNING: Mail/SMTP send is not executed automatically in Databricks Spark jobs")
        _warn(context.step.name, "Mail SMTP send is unsupported as an in-pipeline Spark step")
        lines.extend(_preserve(meta, (
            "server", "port", "destination", "destination_cc", "destination_bcc",
            "reply_to", "reply_address", "subject", "comment",
            "include_date", "only_comment", "use_html", "encoding", "priority",
            "include_files", "zip_files", "zip_filename",
            "use_authentication", "auth_user", "auth_password", "use_secure_auth",
            "secure_connection_type", "contact_person", "contact_phone",
            "attached_filenames", "attach_content_field", "attach_filename_field",
        )))
        if not meta.get("server"):
            lines.append("# WARNING: missing SMTP server")
        if not meta.get("destination"):
            lines.append("# WARNING: missing mail recipients")
        if meta.get("use_authentication") and not meta.get("auth_user"):
            lines.append("# WARNING: SMTP auth enabled but auth_user missing")

        # Documented stub (driver-side sketch) — not executed automatically in clusters
        lines.append("# --- optional smtplib stub (enable manually outside Spark jobs) ---")
        lines.append("# import smtplib")
        lines.append("# from email.message import EmailMessage")
        lines.append("# _msg = EmailMessage()")
        lines.append(f"# _msg['Subject'] = {str(meta.get('subject') or '')!r}")
        lines.append(f"# _msg['From'] = {str(meta.get('reply_address') or meta.get('auth_user') or '')!r}")
        lines.append(f"# _msg['To'] = {str(meta.get('destination') or '')!r}")
        lines.append(f"# _msg.set_content({str(meta.get('comment') or '')!r})")
        lines.append(
            f"# with smtplib.SMTP({str(meta.get('server') or '')!r}, "
            f"{str(meta.get('port') or '25')}) as _smtp:"
        )
        if meta.get("use_secure_auth"):
            lines.append("#     _smtp.starttls()")
        if meta.get("use_authentication"):
            lines.append("#     _smtp.login('<auth_user>', '<auth_password>')")
        lines.append("#     _smtp.send_message(_msg)")

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"


class SyslogHandler(BaseStepHandler):
    """Preserve Syslog configuration; warn when unsupported."""

    _TYPES = {"syslogmessage", "writetosyslog", "sendmessagetosyslog"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_syslog_config(step_el).items():
                meta.setdefault(k, v)

        lines = [f"# Send Message to Syslog: {context.step.name}"]
        lines.append(
            "# UNSUPPORTED: UDP/TCP Syslog clients are not a Databricks/Spark-native sink — "
            "route logs via the Databricks logging / monitoring stack instead"
        )
        lines.append("# WARNING: Syslog send is not supported as a Spark/Databricks sink")
        _warn(context.step.name, "Syslog send is unsupported on Databricks")
        lines.extend(_preserve(meta, (
            "server", "port", "facility", "priority", "message_field",
            "add_timestamp", "date_pattern", "add_hostname",
        )))
        if not meta.get("server"):
            lines.append("# WARNING: missing Syslog server")
        if not meta.get("message_field"):
            lines.append("# WARNING: missing Syslog message field")

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "partial"


class EdiToXmlHandler(BaseStepHandler):
    """Preserve EDI→XML configuration; emit best-effort placeholder conversion."""

    _TYPES = {"edi2xml", "editoxml"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        if step_el is not None:
            for k, v in parse_edi_to_xml_config(step_el).items():
                meta.setdefault(k, v)

        input_field = meta.get("input_field") or ""
        output_field = meta.get("output_field") or "xml"

        lines = [f"# EDI to XML: {context.step.name}"]
        lines.extend(_preserve(meta, ("input_field", "output_field")))
        lines.append(
            "# WARNING: EDIFACT→XML requires a dedicated parser library; "
            "emitting placeholder XML wrapper around the EDI payload"
        )

        if not in_df:
            return _passthrough(context, "EDI to XML")

        if not input_field:
            lines.append("# WARNING: missing EDI input field")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        # Practical stub: wrap EDI text in a simple XML element for downstream parsing
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"{output_field!r}, "
            f"concat(lit('<edi><![CDATA['), "
            f"coalesce(col({input_field!r}).cast('string'), lit('')), "
            f"lit(']]></edi>')))"
        )
        return lines, "partial"


# ---------------------------------------------------------------------------
# Registry list
# ---------------------------------------------------------------------------

UTILITY_HANDLERS: list[BaseStepHandler] = [
    CloneRowHandler(),
    NullIfHandler(),
    DelayRowHandler(),
    MetaStructureHandler(),
    WriteToLogHandler(),
    TableCompareHandler(),
    ChangeFileEncodingHandler(),
    ZipFileHandler(),
    ProcessFilesHandler(),
    ExecProcessHandler(),
    SSHHandler(),
    MailHandler(),
    SyslogHandler(),
    EdiToXmlHandler(),
]
