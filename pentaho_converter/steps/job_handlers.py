"""Handlers for Pentaho Job-category transformation steps.

Supports:
- Copy Rows to Result (RowsToResult)
- Get Rows from Result (RowsFromResult)
- Get Files from Result (FilesFromResult)
- Set Files in Result (FilesToResult)
- Set Variables (SetVariable)
- Get Variables (GetVariable)

Pentaho job-result buffers and variable scopes have no exact Spark equivalent.
Migrations use notebook-scoped result buffers, Spark conf, environment variables,
and Databricks widgets where available, with LIMITATION comments for unsupported
job/parent/root scopes and cross-job orchestration.
"""

from __future__ import annotations

import logging
import re

from ..metadata_propagation import get_converter_metadata
from ..schema_utils import fields_to_schema_ddl, spark_cast_type
from ..step_xml import (
    get_step_element,
    parse_files_from_result_config,
    parse_files_to_result_config,
    parse_get_variable_config,
    parse_rows_from_result_config,
    parse_rows_to_result_config,
    parse_set_variable_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)

# Job scopes beyond JVM cannot be mirrored exactly in a Spark notebook.
_UNSUPPORTED_SCOPES = frozenset({
    "PARENT_JOB", "GP_JOB", "ROOT_JOB",
    "PARENTJOB", "GRANDPARENTJOB", "GRAND_PARENT_JOB",
    "PARENT", "GRANDPARENT", "ROOT",
})

_FILE_RESULT_DDL = (
    "type STRING, filename STRING, path STRING, parentorigin STRING, "
    "origin STRING, comment STRING, timestamp TIMESTAMP"
)


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


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _meta(context: StepContext) -> dict:
    return dict(get_converter_metadata(context))


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

    extras = meta.get("extras")
    if isinstance(extras, dict):
        for key, val in extras.items():
            tag = f"extras.{key}"
            if tag in seen or val in (None, "", [], {}):
                continue
            seen.add(tag)
            _emit(tag, val)

    for key, val in meta.items():
        if key in seen or key in _SKIP_PRESERVE:
            continue
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    return lines


def _warn(step_name: str, message: str) -> None:
    logger.warning("Job step '%s': %s", step_name, message)


def _merge_cfg(meta: dict, cfg: dict) -> dict:
    """Prefer dedicated Job XML parser output over any prior metadata merge."""
    for key, val in cfg.items():
        meta[key] = val
    return meta


def _ensure_result_buffers(lines: list[str]) -> None:
    """Initialize notebook-scoped result buffers once per generated block."""
    lines.append(
        "_pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})"
    )
    lines.append(
        "_pentaho_result_files = globals().setdefault('_pentaho_result_files', [])"
    )


def _normalize_scope(raw: str) -> str:
    text = (raw or "JVM").strip().upper().replace(" ", "_").replace("-", "_")
    aliases = {
        "JAVA_VIRTUAL_MACHINE": "JVM",
        "VALID_IN_THE_JAVA_VIRTUAL_MACHINE": "JVM",
        "VALID_IN_THE_PARENT_JOB": "PARENT_JOB",
        "PARENT": "PARENT_JOB",
        "PARENTJOB": "PARENT_JOB",
        "VALID_IN_THE_GRAND_PARENT_JOB": "GP_JOB",
        "GRAND_PARENT_JOB": "GP_JOB",
        "GRANDPARENT": "GP_JOB",
        "GRANDPARENTJOB": "GP_JOB",
        "VALID_IN_THE_ROOT_JOB": "ROOT_JOB",
        "ROOT": "ROOT_JOB",
        "ROOTJOB": "ROOT_JOB",
    }
    return aliases.get(text, text if text else "JVM")


def _extract_var_keys(variable_string: str) -> list[str]:
    """Extract bare variable names from ${VAR} / %%VAR%% or bare identifiers."""
    text = variable_string or ""
    keys = re.findall(r"\$\{([^}]+)\}", text)
    keys.extend(re.findall(r"%%([^%]+)%%", text))
    if not keys and text.strip() and "${" not in text and "%%" not in text:
        keys.append(text.strip())
    return [k.strip() for k in keys if k.strip()]


def _simple_resolve_lines(
    assign_to: str,
    var_key: str,
    default: str = "",
) -> list[str]:
    """Generate readable multi-line variable resolution (prefer clarity over density)."""
    conf_key = f"pentaho.var.{var_key}"
    return [
        f"{assign_to} = None",
        f"_dbu_{assign_to} = globals().get('dbutils')",
        f"if _dbu_{assign_to} is not None and hasattr(_dbu_{assign_to}, 'widgets'):",
        f"    try:",
        f"        {assign_to} = _dbu_{assign_to}.widgets.get({var_key!r})",
        f"    except Exception:",
        f"        {assign_to} = None",
        f"if {assign_to} in (None, ''):",
        f"    import os as _os_{assign_to}",
        f"    {assign_to} = _os_{assign_to}.environ.get({var_key!r})",
        f"if {assign_to} in (None, ''):",
        f"    try:",
        f"        {assign_to} = spark.conf.get({conf_key!r})",
        f"    except Exception:",
        f"        {assign_to} = None",
        f"if {assign_to} in (None, ''):",
        f"    {assign_to} = {default!r}",
    ]


def _cast_expr(col_expr: str, type_name: str, format_mask: str = "") -> str:
    spark_t = spark_cast_type(type_name)
    t = (type_name or "String").strip().lower()
    if spark_t == "string":
        return col_expr
    if t in ("date",) and format_mask:
        return f"to_date({col_expr}, {format_mask!r})"
    if t in ("timestamp", "datetime") and format_mask:
        return f"to_timestamp({col_expr}, {format_mask!r})"
    return f"{col_expr}.cast({spark_t!r})"


# ---------------------------------------------------------------------------
# Copy Rows to Result
# ---------------------------------------------------------------------------


class RowsToResultHandler(BaseStepHandler):
    """Copy Rows to Result → notebook-scoped result row buffer + pass-through."""

    _TYPES = {"rowstoresult", "copyrowstoresult"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        try:
            cfg = parse_rows_to_result_config(step_el) if step_el is not None else {}
            _merge_cfg(meta, cfg)

            lines = [f"# Copy Rows to Result: {context.step.name}"]
            lines.extend(_preserve(meta, ("result_buffer", "preserve_order", "extras")))
            lines.append(
                "# LIMITATION: Pentaho Result rows are job-level; Databricks uses a "
                "notebook-scoped buffer (_pentaho_result_rows) for downstream hops / "
                "orchestration. Cross-job Result transfer needs Databricks Jobs "
                "task values or persisted Delta tables."
            )
            _ensure_result_buffers(lines)

            if not in_df:
                _warn(context.step.name, "no input stream; storing empty result rows")
                lines.append(
                    f"_pentaho_result_rows[{context.step.name!r}] = "
                    "spark.createDataFrame([], '_result_rows STRING').limit(0)"
                )
                lines.append(
                    f"{out_var} = spark.createDataFrame([], '_result_rows STRING').limit(0)"
                )
                return lines, "partial"

            # Preserve schema + ordering via local checkpoint / coalesce hint
            lines.append(
                f"# Preserve schema and relative ordering for '{context.step.name}'"
            )
            lines.append(f"_result_rows_{out_var} = {in_df}")
            lines.append(
                f"_pentaho_result_rows[{context.step.name!r}] = _result_rows_{out_var}"
            )
            lines.append(
                f"_pentaho_result_rows['__latest__'] = _result_rows_{out_var}"
            )
            lines.append(f"{out_var} = {in_df}")
            logger.info(
                "RowsToResult %s buffered as _pentaho_result_rows[%r]",
                context.step.name,
                context.step.name,
            )
            return lines, "converted"
        except Exception as exc:
            logger.exception("RowsToResult failed for %s", context.step.name)
            return [
                f"# Copy Rows to Result: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = {in_df}" if in_df else (
                    f"{out_var} = spark.createDataFrame([], '_result_rows STRING')"
                ),
            ], "partial"


# ---------------------------------------------------------------------------
# Get Rows from Result
# ---------------------------------------------------------------------------


class RowsFromResultHandler(BaseStepHandler):
    """Get Rows from Result → read notebook-scoped buffer / empty schema frame."""

    _TYPES = {"rowsfromresult", "getrowsfromresult"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        in_df = context.input_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        try:
            cfg = parse_rows_from_result_config(step_el) if step_el is not None else {}
            _merge_cfg(meta, cfg)
            fields = meta.get("fields") or []
            if not isinstance(fields, list):
                fields = []

            lines = [f"# Get Rows from Result: {context.step.name}"]
            lines.extend(_preserve(meta, ("fields", "output_columns", "result_buffer")))
            lines.append(
                "# LIMITATION: reads from notebook-scoped _pentaho_result_rows "
                "(populated by Copy Rows to Result / job orchestration)."
            )
            _ensure_result_buffers(lines)

            ddl = fields_to_schema_ddl(fields) or "_result_rows STRING"
            lines.append(
                f"_src_rows_{out_var} = _pentaho_result_rows.get('__latest__')"
            )
            lines.append(f"if _src_rows_{out_var} is None:")
            lines.append(
                f"    _src_rows_{out_var} = next("
                f"iter(_pentaho_result_rows.values()), None) "
                f"if _pentaho_result_rows else None"
            )
            lines.append(f"if _src_rows_{out_var} is None:")
            lines.append(
                f"    # Empty result / missing prior Copy Rows to Result"
            )
            lines.append(
                f"    {out_var} = spark.createDataFrame([], '{ddl}')"
            )
            lines.append("else:")
            if fields:
                cols = ", ".join(
                    repr(f["name"]) for f in fields if isinstance(f, dict) and f.get("name")
                )
                if cols:
                    lines.append(f"    try:")
                    lines.append(
                        f"        {out_var} = _src_rows_{out_var}.select({cols})"
                    )
                    lines.append(f"    except Exception:")
                    lines.append(
                        f"        # Schema mismatch — fall back to full buffer"
                    )
                    lines.append(f"        {out_var} = _src_rows_{out_var}")
                else:
                    lines.append(f"    {out_var} = _src_rows_{out_var}")
            else:
                lines.append(f"    {out_var} = _src_rows_{out_var}")

            if in_df:
                lines.append(
                    f"# WARNING: Get Rows from Result ignores upstream hop {in_df!r} "
                    "(PDI expects this as a source step)"
                )

            logger.info(
                "RowsFromResult %s fields=%d",
                context.step.name,
                len(fields),
            )
            return lines, "converted"
        except Exception as exc:
            logger.exception("RowsFromResult failed for %s", context.step.name)
            return [
                f"# Get Rows from Result: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.createDataFrame([], '_result_rows STRING')",
            ], "partial"


# ---------------------------------------------------------------------------
# Get Files from Result
# ---------------------------------------------------------------------------


class FilesFromResultHandler(BaseStepHandler):
    """Get Files from Result → DataFrame of ResultFile metadata."""

    _TYPES = {"filesfromresult", "getfilesfromresult"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        in_df = context.input_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        try:
            cfg = parse_files_from_result_config(step_el) if step_el is not None else {}
            _merge_cfg(meta, cfg)

            lines = [f"# Get Files from Result: {context.step.name}"]
            lines.extend(_preserve(meta, (
                "fields", "output_columns", "result_buffer", "preserve_order", "extras",
            )))
            lines.append(
                "# LIMITATION: reads notebook-scoped _pentaho_result_files "
                "(populated by Set Files in Result / job orchestration)."
            )
            _ensure_result_buffers(lines)

            lines.append(f"_raw_files_{out_var} = list(_pentaho_result_files or [])")
            lines.append(
                f"# Deduplicate duplicate path entries while preserving first-seen order"
            )
            lines.append(f"_seen_{out_var} = set()")
            lines.append(f"_files_{out_var} = []")
            lines.append(f"for _f in _raw_files_{out_var}:")
            lines.append(
                f"    _path = ((_f.get('path') or _f.get('filename')) "
                f"if isinstance(_f, dict) else str(_f))"
            )
            lines.append(f"    if _path in _seen_{out_var}:")
            lines.append(f"        continue")
            lines.append(f"    _seen_{out_var}.add(_path)")
            lines.append(
                f"    if isinstance(_f, dict):"
            )
            lines.append(f"        _files_{out_var}.append(_f)")
            lines.append(f"    else:")
            lines.append(
                f"        _files_{out_var}.append({{"
                f"'type': 'GENERAL', 'filename': str(_f), 'path': str(_f), "
                f"'parentorigin': '', 'origin': '', 'comment': '', 'timestamp': None}})"
            )
            lines.append(f"if not _files_{out_var}:")
            lines.append(
                f"    {out_var} = spark.createDataFrame([], '{_FILE_RESULT_DDL}')"
            )
            lines.append("else:")
            lines.append(
                f"    {out_var} = spark.createDataFrame(_files_{out_var})"
            )

            if in_df:
                lines.append(
                    f"# WARNING: Get Files from Result ignores upstream hop {in_df!r}"
                )

            logger.info("FilesFromResult %s", context.step.name)
            return lines, "converted"
        except Exception as exc:
            logger.exception("FilesFromResult failed for %s", context.step.name)
            return [
                f"# Get Files from Result: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = spark.createDataFrame([], '{_FILE_RESULT_DDL}')",
            ], "partial"


# ---------------------------------------------------------------------------
# Set Files in Result
# ---------------------------------------------------------------------------


class FilesToResultHandler(BaseStepHandler):
    """Set Files in Result → append filename field values into result file buffer."""

    _TYPES = {"filestoresult", "setfilesinresult", "setfilestoresult"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        try:
            cfg = parse_files_to_result_config(step_el) if step_el is not None else {}
            _merge_cfg(meta, cfg)

            filename_field = (meta.get("filename_field") or "").strip()
            file_type = (meta.get("file_type") or "GENERAL").strip() or "GENERAL"

            lines = [f"# Set Files in Result: {context.step.name}"]
            lines.extend(_preserve(meta, (
                "filename_field", "file_type", "result_buffer", "preserve_order", "extras",
            )))
            lines.append(
                "# LIMITATION: writes notebook-scoped _pentaho_result_files; "
                "cross-job ResultFile lists need Databricks Jobs task values / DBFS manifests."
            )
            _ensure_result_buffers(lines)

            if not in_df:
                _warn(context.step.name, "no input stream for Set Files in Result")
                lines.append(
                    f"{out_var} = spark.createDataFrame([], '_files_to_result STRING')"
                )
                return lines, "partial"

            if not filename_field:
                lines.append(
                    "# WARNING: filename_field missing — no files appended to result"
                )
                lines.append(f"{out_var} = {in_df}")
                return lines, "partial"

            lines.append(
                f"_file_rows_{out_var} = {in_df}.select({filename_field!r}).collect()"
            )
            lines.append(f"for _row in _file_rows_{out_var}:")
            lines.append(f"    _path = _row[{filename_field!r}]")
            lines.append(f"    if _path is None or str(_path).strip() == '':")
            lines.append(f"        continue  # skip null / empty file references")
            lines.append(f"    _path = str(_path)")
            lines.append(
                f"    _pentaho_result_files.append({{"
                f"'type': {file_type!r}, 'filename': _path.split('/')[-1], "
                f"'path': _path, 'parentorigin': {context.step.name!r}, "
                f"'origin': {context.step.name!r}, 'comment': '', 'timestamp': None}})"
            )
            lines.append(f"{out_var} = {in_df}")
            logger.info(
                "FilesToResult %s field=%s type=%s",
                context.step.name,
                filename_field,
                file_type,
            )
            return lines, "converted"
        except Exception as exc:
            logger.exception("FilesToResult failed for %s", context.step.name)
            return [
                f"# Set Files in Result: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = {in_df}" if in_df else (
                    f"{out_var} = spark.createDataFrame([], '_files_to_result STRING')"
                ),
            ], "partial"


# ---------------------------------------------------------------------------
# Set Variables
# ---------------------------------------------------------------------------


class SetVariablesHandler(BaseStepHandler):
    """Set Variables → Spark conf / env / widgets; warn on job scopes."""

    _TYPES = {"setvariables", "setvariable"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        status = "converted"
        try:
            cfg = parse_set_variable_config(step_el) if step_el is not None else {}
            _merge_cfg(meta, cfg)

            fields = meta.get("fields") or []
            if not isinstance(fields, list):
                fields = []
            use_formatting = bool(meta.get("use_formatting"))

            lines = [f"# Set Variables: {context.step.name}"]
            lines.extend(_preserve(meta, ("fields", "use_formatting", "variable_names")))
            lines.append("import os")
            if use_formatting:
                lines.append(
                    "# LIMITATION: use_formatting=True — Pentaho ValueMeta masks are not "
                    "applied; values are coerced with str() before spark.conf/os.environ"
                )
                status = "partial"

            # Capture first-row values for field-driven variables
            if in_df and fields:
                field_names = sorted({
                    f.get("field_name") for f in fields
                    if isinstance(f, dict) and f.get("field_name")
                })
                if field_names:
                    cols = ", ".join(repr(n) for n in field_names)
                    lines.append(
                        f"_var_row_{out_var} = {in_df}.limit(1).select({cols}).collect()"
                    )
                    lines.append(
                        f"_var_vals_{out_var} = _var_row_{out_var}[0].asDict() "
                        f"if _var_row_{out_var} else {{}}"
                    )
                else:
                    lines.append(f"_var_vals_{out_var} = {{}}")
            else:
                lines.append(f"_var_vals_{out_var} = {{}}")
                if not in_df and fields:
                    lines.append(
                        "# WARNING: no input — using default_value for each variable"
                    )

            seen_vars: set[str] = set()
            for field in fields:
                if not isinstance(field, dict):
                    continue
                field_name = (field.get("field_name") or "").strip()
                var_name = (field.get("variable_name") or field_name).strip()
                if not var_name:
                    continue
                if var_name in seen_vars:
                    lines.append(
                        f"# WARNING: duplicate variable {var_name!r} — later mapping wins"
                    )
                    status = "partial"
                seen_vars.add(var_name)

                scope = _normalize_scope(field.get("variable_type") or "JVM")
                default = field.get("default_value") or ""
                conf_key = f"pentaho.var.{var_name}"
                safe = _safe_ident(var_name)

                lines.append(f"# variable {var_name!r} scope={scope!r}")
                if scope in _UNSUPPORTED_SCOPES or scope not in ("JVM",):
                    if scope != "JVM":
                        lines.append(
                            f"# LIMITATION: scope {scope!r} has no Databricks equivalent "
                            f"(parent/grand-parent/root job). Value still written to "
                            f"spark.conf / os.environ for notebook reuse."
                        )
                        status = "partial"
                        _warn(
                            context.step.name,
                            f"unsupported scope {scope} for variable {var_name}",
                        )

                if field_name:
                    lines.append(
                        f"_{safe}_val = _var_vals_{out_var}.get({field_name!r})"
                    )
                    lines.append(
                        f"if _{safe}_val is None:"
                    )
                    lines.append(f"    _{safe}_val = {default!r}")
                else:
                    lines.append(f"_{safe}_val = {default!r}")

                lines.append(f"_{safe}_str = '' if _{safe}_val is None else str(_{safe}_val)")
                lines.append(f"spark.conf.set({conf_key!r}, _{safe}_str)")
                lines.append(f"os.environ[{var_name!r}] = _{safe}_str")
                # Optional widget update for Databricks runtime parameters
                lines.append(f"_dbu_{safe} = globals().get('dbutils')")
                lines.append(
                    f"if _dbu_{safe} is not None and hasattr(_dbu_{safe}, 'widgets'):"
                )
                lines.append(f"    try:")
                lines.append(
                    f"        _dbu_{safe}.widgets.text({var_name!r}, _{safe}_str)"
                )
                lines.append(f"    except Exception as _widget_err_{safe}:")
                lines.append(
                    f"        _ = _widget_err_{safe}  # widget may already exist / read-only"
                )

            if not fields:
                lines.append("# WARNING: no variables configured")
                status = "partial"

            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(
                    f"{out_var} = spark.createDataFrame([], '_set_variables STRING')"
                )

            logger.info(
                "SetVariables %s vars=%d",
                context.step.name,
                len(seen_vars),
            )
            return lines, status
        except Exception as exc:
            logger.exception("SetVariables failed for %s", context.step.name)
            return [
                f"# Set Variables: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = {in_df}" if in_df else (
                    f"{out_var} = spark.createDataFrame([], '_set_variables STRING')"
                ),
            ], "partial"


# ---------------------------------------------------------------------------
# Get Variables
# ---------------------------------------------------------------------------


class GetVariablesHandler(BaseStepHandler):
    """Get Variables → columns from widgets / env / spark.conf / defaults."""

    _TYPES = {"getvariables", "getvariable"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        params = dict(context.transformation.parameters or {})
        status = "converted"
        try:
            cfg = parse_get_variable_config(step_el) if step_el is not None else {}
            _merge_cfg(meta, cfg)

            fields = meta.get("fields") or []
            if not isinstance(fields, list):
                fields = []

            lines = [f"# Get Variables: {context.step.name}"]
            lines.extend(_preserve(meta, ("fields", "output_columns")))
            lines.append("import os")
            lines.append("import re as _re_var")
            lines.append(
                "# Lookup order: Databricks widgets → os.environ → spark.conf "
                "(pentaho.var.*) → transformation parameters → empty string"
            )

            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(
                    f"{out_var} = spark.range(1).select(lit(1).alias('_row'))"
                )

            if not fields:
                lines.append("# WARNING: no variables configured")
                status = "partial"
                return lines, status

            for field in fields:
                if not isinstance(field, dict):
                    continue
                field_name = (field.get("name") or "").strip()
                variable_str = field.get("variable") or ""
                if not field_name:
                    continue
                type_name = field.get("type") or field.get("type_name") or "String"
                format_mask = field.get("format") or ""
                trim_type = field.get("trim_type") or "none"
                safe = _safe_ident(field_name)
                keys = _extract_var_keys(variable_str)

                lines.append(
                    f"# field {field_name!r} from variable string {variable_str!r}"
                )
                for meta_key in (
                    "format", "currency", "decimal", "group",
                    "length", "precision", "trim_type", "type",
                ):
                    meta_val = field.get(meta_key)
                    if meta_val not in (None, "", -1, "-1"):
                        lines.append(
                            f"# preserved.field.{field_name}.{meta_key}={meta_val!r}"
                        )

                if not keys:
                    lines.append(
                        f"# WARNING: missing variable for field {field_name!r}"
                    )
                    lines.append(f"_{safe}_resolved = ''")
                    status = "partial"
                elif len(keys) == 1 and (
                    variable_str.strip() in (f"${{{keys[0]}}}", f"%%{keys[0]}%%", keys[0])
                ):
                    # Single pure variable — prefer param override then runtime lookup
                    key = keys[0]
                    default = str(params.get(key, ""))
                    lines.extend(_simple_resolve_lines(f"_{safe}_resolved", key, default))
                else:
                    # Template with embedded variables
                    lines.append(f"_{safe}_template = {variable_str!r}")

                    def _replacer_body() -> list[str]:
                        body = [
                            f"def _{safe}_expand(_tmpl):",
                            f"    def _{safe}_lookup(_m):",
                            f"        _k = _m.group(1)",
                        ]
                        # Prefer transformation parameters as compile-time defaults
                        body.append(f"        _defaults = {params!r}")
                        body.append(f"        _v = None")
                        body.append(f"        _dbu = globals().get('dbutils')")
                        body.append(
                            f"        if _dbu is not None and hasattr(_dbu, 'widgets'):"
                        )
                        body.append(f"            try:")
                        body.append(f"                _v = _dbu.widgets.get(_k)")
                        body.append(f"            except Exception:")
                        body.append(f"                _v = None")
                        body.append(f"        if _v in (None, ''):")
                        body.append(f"            _v = os.environ.get(_k)")
                        body.append(f"        if _v in (None, ''):")
                        body.append(f"            try:")
                        body.append(
                            f"                _v = spark.conf.get('pentaho.var.' + _k)"
                        )
                        body.append(f"            except Exception:")
                        body.append(f"                _v = None")
                        body.append(f"        if _v in (None, ''):")
                        body.append(f"            _v = _defaults.get(_k, '')")
                        body.append(f"        return '' if _v is None else str(_v)")
                        body.append(
                            f"    _out = _re_var.sub("
                            f"r'\\$\\{{([^}}]+)\\}}', _{safe}_lookup, _tmpl)"
                        )
                        body.append(
                            f"    _out = _re_var.sub("
                            f"r'%%([^%]+)%%', _{safe}_lookup, _out)"
                        )
                        body.append(f"    return _out")
                        return body

                    lines.extend(_replacer_body())
                    lines.append(f"_{safe}_resolved = _{safe}_expand(_{safe}_template)")

                # Null / empty handling + trim at Python layer before lit()
                lines.append(
                    f"if _{safe}_resolved is None:"
                )
                lines.append(f"    _{safe}_resolved = ''")
                tt = trim_type.strip().lower()
                if tt in ("both", "left_right", "lr"):
                    lines.append(f"_{safe}_resolved = str(_{safe}_resolved).strip()")
                elif tt in ("left", "l"):
                    lines.append(f"_{safe}_resolved = str(_{safe}_resolved).lstrip()")
                elif tt in ("right", "r"):
                    lines.append(f"_{safe}_resolved = str(_{safe}_resolved).rstrip()")

                lit_expr = f"lit(_{safe}_resolved)"
                casted = _cast_expr(lit_expr, type_name, format_mask)
                # Prefer Spark trim for both when we want DataFrame-native trim;
                # already applied in Python for constants.
                lines.append(
                    f"{out_var} = {out_var}.withColumn({field_name!r}, {casted})"
                )

            logger.info(
                "GetVariables %s fields=%d",
                context.step.name,
                len(fields),
            )
            return lines, status
        except Exception as exc:
            logger.exception("GetVariables failed for %s", context.step.name)
            return [
                f"# Get Variables: {context.step.name}",
                f"# ERROR: {exc}",
                f"{out_var} = {in_df}" if in_df else (
                    f"{out_var} = spark.createDataFrame([], '_get_variables STRING')"
                ),
            ], "partial"


JOB_HANDLERS: list[BaseStepHandler] = [
    RowsToResultHandler(),
    RowsFromResultHandler(),
    FilesFromResultHandler(),
    FilesToResultHandler(),
    SetVariablesHandler(),
    GetVariablesHandler(),
]
