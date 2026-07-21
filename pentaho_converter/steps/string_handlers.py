"""Handlers for Pentaho string manipulation steps."""

from __future__ import annotations

import logging
import re

from ..metadata_propagation import get_converter_metadata
from ..step_xml import _child_text, _bool_from_yn, get_step_element, parse_string_operation_fields
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


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


def _escape_regex_literal(pattern: str) -> str:
    """Escape a literal search string for use inside regexp_replace."""
    return re.escape(pattern or "")


def _build_replace_expression(field) -> str:
    """Build a regexp_replace expression honouring Replace in String options."""
    expr = f'col("{field.in_stream_name}").cast("string")'
    search = field.replace_string or ""
    replace_by_field = getattr(field, "replace_field_by_string", "") or ""
    set_empty = bool(getattr(field, "set_empty_string", False))
    use_regex = bool(field.use_regex)
    whole_word = bool(getattr(field, "whole_word", False))
    case_sensitive = bool(getattr(field, "case_sensitive", True))

    if set_empty:
        replacement = ""
    elif replace_by_field:
        replacement = None
    else:
        replacement = field.replace_by_string or ""

    if not search and not set_empty and not replace_by_field:
        return f'col("{field.in_stream_name}")'

    # Column-sourced replacement: SQL replace() via expr
    if replace_by_field:
        if use_regex or whole_word or not case_sensitive:
            logger.warning(
                "ReplaceInString field '%s': replace_field_by_string with regex/flags "
                "is approximated with SQL replace()",
                field.in_stream_name,
            )
        return (
            f'expr("replace(cast(`{field.in_stream_name}` as string), '
            f"'{search.replace(chr(39), chr(39)+chr(39))}', "
            f'cast(`{replace_by_field}` as string))")'
        )

    if use_regex:
        pattern = search
    else:
        pattern = _escape_regex_literal(search)

    if whole_word and pattern:
        pattern = rf"\b(?:{pattern})\b"
    if not case_sensitive and pattern:
        pattern = f"(?i){pattern}"

    return f"regexp_replace({expr}, {pattern!r}, {replacement!r})"


def _build_string_cut_expression(expr: str, cut_from: str, cut_to: str) -> str:
    """Map Pentaho StringCut (Java substring, 0-based exclusive end) to Spark substring.

    Positive from/to: rcode.substring(cutFrom, cutTo) → Spark substring(s, from+1, to-from).
    Empty/zero cut_to with cut_from==0 may mean take suffix / whole; negatives get a warning path.
    """
    from_s = (cut_from or "0").strip() or "0"
    to_s = (cut_to or "0").strip() or "0"
    try:
        from_i = int(from_s)
        to_i = int(to_s)
    except ValueError:
        return (
            f"substring({expr}, int({from_s}) + 1, "
            f"greatest(int({to_s}) - int({from_s}), lit(0)))"
        )

    if from_i >= 0 and to_i > 0 and to_i >= from_i:
        length = to_i - from_i
        if length <= 0:
            return "lit(None)"
        return f"substring({expr}, {from_i + 1}, {length})"
    if from_i >= 0 and to_i == 0:
        # Empty cut_to → remaining string from cut_from
        return f"substring({expr}, {from_i + 1}, length({expr}))"
    if from_i < 0 or to_i < 0:
        # Approximate negative indices via length arithmetic
        return (
            f"substring({expr}, "
            f"length({expr}) + lit({from_i}) + lit(1), "
            f"greatest(lit({to_i - from_i}), lit(0)))"
        )
    return "lit(None)"


def _apply_padding(expr: str, padding_type: str, pad_char: str, pad_len: str) -> str:
    pt = (padding_type or "none").lower()
    if pt in ("none", "", "n"):
        return expr
    if not pad_len:
        return expr
    ch = (pad_char or " ")[:1]
    if pt in ("left", "lpad"):
        return f"lpad({expr}.cast('string'), int({pad_len}), {ch!r})"
    if pt in ("right", "rpad"):
        return f"rpad({expr}.cast('string'), int({pad_len}), {ch!r})"
    return expr


def _apply_digits(expr: str, digits_type: str) -> str:
    dt = (digits_type or "none").lower()
    if dt in ("only", "digits", "getonlydigits"):
        return f"regexp_replace({expr}, '[^0-9]', '')"
    if dt in ("remove", "removedigits"):
        return f"regexp_replace({expr}, '[0-9]', '')"
    return expr


def _apply_mask_xml(expr: str, mask_xml: str) -> str:
    m = (mask_xml or "none").lower().replace(" ", "").replace("_", "")
    if m in ("none", "", "n"):
        return expr
    if m in ("escapexml", "maskxml"):
        return (
            f"regexp_replace(regexp_replace(regexp_replace(regexp_replace("
            f"regexp_replace({expr}, '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), "
            f"\"'\", '&apos;'), '\"', '&quot;')"
        )
    if m in ("unescapexml",):
        return (
            f"regexp_replace(regexp_replace(regexp_replace(regexp_replace("
            f"regexp_replace({expr}, '&quot;', '\"'), '&apos;', \"'\"), '&gt;', '>'), "
            f"'&lt;', '<'), '&amp;', '&')"
        )
    if m in ("cdata", "usecdata"):
        return f"concat(lit('<![CDATA['), {expr}, lit(']]>'))"
    if m in ("escapehtml",):
        return (
            f"regexp_replace(regexp_replace(regexp_replace({expr}, '&', '&amp;'), "
            f"'<', '&lt;'), '>', '&gt;')"
        )
    if m in ("unescapehtml",):
        return (
            f"regexp_replace(regexp_replace(regexp_replace({expr}, '&gt;', '>'), "
            f"'&lt;', '<'), '&amp;', '&')"
        )
    if m in ("escapesql",):
        return f"regexp_replace({expr}, \"'\", \"''\")"
    return expr


def _apply_remove_special(expr: str, mode: str) -> str:
    m = (mode or "none").lower().replace(" ", "")
    if m in ("none", "", "n"):
        return expr
    if m in ("cr", "carriage"):
        return f"regexp_replace({expr}, '\\r', '')"
    if m in ("lf", "newline"):
        return f"regexp_replace({expr}, '\\n', '')"
    if m in ("crlf",):
        return f"regexp_replace({expr}, '\\r|\\n', '')"
    if m in ("tab",):
        return f"regexp_replace({expr}, '\\t', '')"
    if m in ("espace", "space", "spaces"):
        return f"regexp_replace({expr}, ' ', '')"
    return expr


def _build_string_expression(field) -> str:
    """Build a chained PySpark expression for one string operation field."""
    # Replace-in-string style fields take priority when a search pattern is present
    # ONLY when no other String Operations transforms are configured
    if field.replace_string or getattr(field, "replace_field_by_string", "") or getattr(
        field, "set_empty_string", False
    ):
        if not field.trim_type or field.trim_type == "none":
            if (not field.lower_upper or field.lower_upper == "none") and not field.init_cap:
                if not field.cut_from and not field.cut_to:
                    if (getattr(field, "padding_type", "none") or "none") == "none":
                        if (getattr(field, "digits_type", "none") or "none") == "none":
                            if (getattr(field, "mask_xml", "none") or "none") == "none":
                                if (
                                    getattr(field, "remove_special_characters", "none")
                                    or "none"
                                ) == "none":
                                    return _build_replace_expression(field)

    expr = f'col("{field.in_stream_name}").cast("string")'
    expr = _apply_trim(expr, field.trim_type)
    expr = _apply_case(expr, field.lower_upper)

    if field.init_cap:
        expr = f"initcap({expr})"

    expr = _apply_padding(
        expr,
        getattr(field, "padding_type", "none"),
        getattr(field, "pad_char", " "),
        getattr(field, "pad_len", ""),
    )
    expr = _apply_digits(expr, getattr(field, "digits_type", "none"))
    expr = _apply_mask_xml(expr, getattr(field, "mask_xml", "none"))
    expr = _apply_remove_special(expr, getattr(field, "remove_special_characters", "none"))

    if field.cut_from or field.cut_to:
        # String Operations may carry cut_* ; StringCut uses dedicated path
        expr = _build_string_cut_expression(expr, field.cut_from, field.cut_to)

    if field.replace_string or getattr(field, "replace_field_by_string", ""):
        search = field.replace_string or ""
        if field.use_regex:
            pattern = search
        else:
            pattern = _escape_regex_literal(search)
        if getattr(field, "whole_word", False) and pattern:
            pattern = rf"\b(?:{pattern})\b"
        if not getattr(field, "case_sensitive", True) and pattern:
            pattern = f"(?i){pattern}"
        replace_by = field.replace_by_string or ""
        if getattr(field, "set_empty_string", False):
            replace_by = ""
        expr = f"regexp_replace({expr}, {pattern!r}, {replace_by!r})"

    return expr


class StringOperationsHandler(BaseStepHandler):
    """Converts Pentaho String Operations, String Cut, and Replace in String steps."""

    _TYPES = {
        "stringoperations",
        "stringcut",
        "stringscut",
        "replaceinstring",
        "replacestring",  # Pentaho KTR type id for Replace in String
        "regexreplace",  # alias of Replace in String with regex semantics
    }

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(step)
        st = step.step_type.lower().replace(" ", "")
        label = {
            "replaceinstring": "Replace in String",
            "replacestring": "Replace in String",
            "regexreplace": "Regex Replace",
            "stringcut": "String Cut",
            "stringscut": "String Cut",
        }.get(st, "String Operations")
        lines = [f"# {label}: {step.name}"]

        if not in_df or step_el is None:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        metadata = get_converter_metadata(context)
        # Normalize Pentaho type id ReplaceString → replace-in-string semantics
        if st == "replacestring":
            st = "replaceinstring"

        # Dedicated String Cut path (0-based exclusive end)
        if st in ("stringcut", "stringscut"):
            from ..step_xml import parse_string_cut_config

            cut_cfg = metadata if metadata.get("fields") else parse_string_cut_config(step_el)
            cut_fields = cut_cfg.get("fields") or []
            if not cut_fields:
                # Fallback to operation fields parser
                cut_fields = [
                    {
                        "in_stream_name": f.in_stream_name,
                        "out_stream_name": f.out_stream_name,
                        "cut_from": f.cut_from,
                        "cut_to": f.cut_to,
                    }
                    for f in parse_string_operation_fields(step_el)
                ]
            if not cut_fields:
                lines.append(f"# WARNING: StringCut '{step.name}': no fields configured")
                lines.append(f"{out_var} = {in_df}")
                return lines, "partial"
            lines.append(f"{out_var} = {in_df}")
            lines.append(
                "# StringCut uses Java substring semantics (0-based, end exclusive)"
            )
            for item in cut_fields:
                in_name = item.get("in_stream_name") or ""
                out_name = item.get("out_stream_name") or in_name
                if not in_name:
                    continue
                expr = _build_string_cut_expression(
                    f'col("{in_name}").cast("string")',
                    item.get("cut_from") or "0",
                    item.get("cut_to") or "0",
                )
                # UTF-8 / multibyte: Spark substring operates on Unicode code points
                lines.append(
                    f'# preserved.is_unicode=Y implied — Spark substring is code-point based'
                )
                lines.append(f'{out_var} = {out_var}.withColumn("{out_name}", {expr})')
            return lines, "converted"

        op_fields = parse_string_operation_fields(step_el)
        if not op_fields and metadata.get("fields"):
            # Rebuild from propagated metadata when available
            from ..step_xml import StringOpField

            for item in metadata["fields"]:
                if not isinstance(item, dict):
                    continue
                in_name = item.get("in_stream_name") or item.get("in") or item.get("name")
                if not in_name:
                    continue
                op_fields.append(
                    StringOpField(
                        in_stream_name=in_name,
                        out_stream_name=item.get("out_stream_name")
                        or item.get("out")
                        or in_name,
                        trim_type=(item.get("trim_type") or "none").lower(),
                        lower_upper=(item.get("lower_upper") or "none").lower(),
                        init_cap=bool(item.get("init_cap")),
                        cut_from=item.get("cut_from") or "",
                        cut_to=item.get("cut_to") or "",
                        replace_string=item.get("replace_string") or "",
                        replace_by_string=item.get("replace_by_string") or "",
                        use_regex=bool(item.get("use_regex")),
                        replace_field_by_string=item.get("replace_field_by_string") or "",
                        set_empty_string=bool(item.get("set_empty_string")),
                        whole_word=bool(item.get("whole_word")),
                        case_sensitive=bool(item.get("case_sensitive", True)),
                        is_unicode=bool(item.get("is_unicode")),
                        padding_type=(item.get("padding_type") or "none").lower(),
                        pad_char=item.get("pad_char") or " ",
                        pad_len=item.get("pad_len") or "",
                        digits_type=(item.get("digits_type") or "none").lower(),
                        mask_xml=(item.get("mask_xml") or "none").lower(),
                        remove_special_characters=(
                            item.get("remove_special_characters") or "none"
                        ).lower(),
                    )
                )

        # ReplaceInString / RegexReplace: step-level replace config
        if not op_fields and st in ("replaceinstring", "regexreplace"):
            in_field = _child_text(
                step_el, "in_stream_name", step.attributes.get("in_stream_name", "")
            )
            out_field = _child_text(
                step_el,
                "out_stream_name",
                step.attributes.get("out_stream_name", in_field),
            )
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
            use_regex = st == "regexreplace" or _bool_from_yn(
                _child_text(step_el, "use_regex", step.attributes.get("use_regex", "N"))
            )
            whole_word = _bool_from_yn(_child_text(step_el, "whole_word", "N"))
            case_sensitive = _bool_from_yn(
                _child_text(step_el, "case_sensitive", "Y"), default=True
            )
            if in_field and search:
                from ..step_xml import StringOpField

                tmp = StringOpField(
                    in_stream_name=in_field,
                    out_stream_name=out_field or in_field,
                    replace_string=search,
                    replace_by_string=replace,
                    use_regex=use_regex,
                    whole_word=whole_word,
                    case_sensitive=case_sensitive,
                )
                expr = _build_replace_expression(tmp)
                lines.append(f'{out_var} = {in_df}.withColumn("{tmp.out_stream_name}", {expr})')
                return lines, "converted"

        if not op_fields:
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"

        lines.append(f"{out_var} = {in_df}")
        for field in op_fields:
            if st in ("replaceinstring", "regexreplace"):
                if st == "regexreplace":
                    field.use_regex = True
                expr = _build_replace_expression(field)
            else:
                expr = _build_string_expression(field)
            out_name = field.out_stream_name or field.in_stream_name
            lines.append(f'{out_var} = {out_var}.withColumn("{out_name}", {expr})')
            if getattr(field, "is_unicode", False):
                lines.append(f"# preserved.is_unicode=Y for {out_name}")
            pad = getattr(field, "padding_type", "none")
            if pad and pad != "none":
                lines.append(
                    f"# preserved.padding={pad!r} pad_char={getattr(field, 'pad_char', '')!r} "
                    f"pad_len={getattr(field, 'pad_len', '')!r}"
                )

        return lines, "converted"
