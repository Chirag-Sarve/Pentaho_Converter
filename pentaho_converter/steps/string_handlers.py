"""Handlers for Pentaho string manipulation steps."""

from __future__ import annotations

from ..step_xml import _child_text, get_step_element, parse_string_operation_fields
from .base import BaseStepHandler, StepContext


def _apply_trim(expr: str, trim_type: str) -> str:
    t = (trim_type or "none").lower()
    if t in ("left", "ltrim"):
        return f"ltrim({expr})"
    if t in ("right", "rtrim"):
        return f"rtrim({expr})"
    if t in ("both", "all"):
        return f"trim({expr})"
    return expr


def _apply_case(expr: str, lower_upper: str) -> str:
    lu = (lower_upper or "none").lower()
    if lu in ("upper", "uppercase"):
        return f"upper({expr})"
    if lu in ("lower", "lowercase"):
        return f"lower({expr})"
    return expr


def _build_string_expression(field) -> str:
    """Build a chained PySpark expression for one string operation field."""
    expr = f'col("{field.in_stream_name}")'

    expr = _apply_trim(expr, field.trim_type)
    expr = _apply_case(expr, field.lower_upper)

    if field.init_cap:
        expr = f"initcap({expr})"

    if field.cut_from or field.cut_to:
        start = field.cut_from if field.cut_from else "1"
        if field.cut_to:
            length = f"int({field.cut_to}) - int({start}) + 1"
            expr = f"substring({expr}, int({start}), {length})"
        else:
            expr = f"substring({expr}, int({start}))"

    if field.replace_string:
        if field.use_regex:
            expr = (
                f'regexp_replace({expr}, {field.replace_string!r}, '
                f'{field.replace_by_string!r})'
            )
        else:
            expr = (
                f'regexp_replace({expr}, '
                f'regexp_replace(lit({field.replace_string!r}), '
                f'lit("([\\\\.\\\\^\\\\$\\\\|?\\\\*\\\\+\\\\(\\\\)\\\\[\\\\]{{}}])"), '
                f'lit("\\\\\\\\$1")), '
                f'{field.replace_by_string!r})'
            )

    return expr


class StringOperationsHandler(BaseStepHandler):
    """Converts Pentaho String Operations, String Cut, and Replace in String steps."""

    _TYPES = {"stringoperations", "stringcut", "replaceinstring"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(step)
        lines = [f"# String Operations: {step.name}"]

        if not in_df or step_el is None:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        op_fields = parse_string_operation_fields(step_el)

        # StringCut: single cut_from/cut_to at step level
        if not op_fields and step.step_type.lower() == "stringcut":
            cut_from = _child_text(step_el, "cut_from", step.attributes.get("cut_from", "1"))
            in_field = _child_text(step_el, "in_stream_name", step.attributes.get("in_stream_name", ""))
            out_field = _child_text(step_el, "out_stream_name", step.attributes.get("out_stream_name", in_field))
            cut_to = _child_text(step_el, "cut_to", step.attributes.get("cut_to", ""))
            if in_field:
                expr = f'col("{in_field}")'
                if cut_to:
                    expr = f"substring({expr}, int({cut_from}), int({cut_to}) - int({cut_from}) + 1)"
                else:
                    expr = f"substring({expr}, int({cut_from}))"
                lines.append(f'{out_var} = {in_df}.withColumn("{out_field}", {expr})')
                return lines, "converted"

        # ReplaceInString: step-level replace config
        if not op_fields and step.step_type.lower() == "replaceinstring":
            in_field = _child_text(step_el, "in_stream_name", step.attributes.get("in_stream_name", ""))
            out_field = _child_text(step_el, "out_stream_name", step.attributes.get("out_stream_name", in_field))
            search = (
                _child_text(step_el, "search")
                or _child_text(step_el, "replace_string")
                or step.attributes.get("search", "")
            )
            replace = (
                _child_text(step_el, "replace")
                or _child_text(step_el, "replace_by_string")
                or step.attributes.get("replace", "")
            )
            use_regex = (
                _child_text(step_el, "use_regex", step.attributes.get("use_regex", "N")).upper() == "Y"
            )
            if in_field and search:
                expr = f'col("{in_field}")'
                if use_regex:
                    expr = f"regexp_replace({expr}, {search!r}, {replace!r})"
                else:
                    expr = f"regexp_replace({expr}, {search!r}, {replace!r})"
                lines.append(f'{out_var} = {in_df}.withColumn("{out_field}", {expr})')
                return lines, "converted"

        if not op_fields:
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"

        lines.append(f"{out_var} = {in_df}")
        for field in op_fields:
            expr = _build_string_expression(field)
            out_name = field.out_stream_name or field.in_stream_name
            lines.append(f'{out_var} = {out_var}.withColumn("{out_name}", {expr})')

        return lines, "converted"
