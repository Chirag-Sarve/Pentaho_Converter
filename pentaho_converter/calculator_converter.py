"""Convert Pentaho Calculator step definitions to PySpark column expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .step_xml import CalculationSpec

_UNSUPPORTED_MARKER = "__UNSUPPORTED__:"

# Pentaho CalculatorMetaFunction.calc_desc index → type name (IDs match array index).
_CALC_DESC_BY_ID: tuple[str, ...] = (
    "-",
    "CONSTANT",
    "COPY_OF_FIELD",
    "ADD",
    "SUBTRACT",
    "MULTIPLY",
    "DIVIDE",
    "SQUARE",
    "SQUARE_ROOT",
    "PERCENT_1",
    "PERCENT_2",
    "PERCENT_3",
    "COMBINATION_1",
    "COMBINATION_2",
    "ROUND_1",
    "ROUND_2",
    "ROUND_STD_1",
    "ROUND_STD_2",
    "CEIL",
    "FLOOR",
    "NVL",
    "ADD_DAYS",
    "YEAR_OF_DATE",
    "MONTH_OF_DATE",
    "DAY_OF_YEAR",
    "DAY_OF_MONTH",
    "DAY_OF_WEEK",
    "WEEK_OF_YEAR",
    "WEEK_OF_YEAR_ISO8601",
    "YEAR_OF_DATE_ISO8601",
    "BYTE_TO_HEX_ENCODE",
    "HEX_TO_BYTE_DECODE",
    "CHAR_TO_HEX_ENCODE",
    "HEX_TO_CHAR_DECODE",
    "CRC32",
    "ADLER32",
    "MD5",
    "SHA1",
    "LEVENSHTEIN_DISTANCE",
    "METAPHONE",
    "DOUBLE_METAPHONE",
    "ABS",
    "REMOVE_TIME_FROM_DATE",
    "DATE_DIFF",
    "ADD3",
    "INIT_CAP",
    "UPPER_CASE",
    "LOWER_CASE",
    "MASK_XML",
    "USE_CDATA",
    "REMOVE_CR",
    "REMOVE_LF",
    "REMOVE_CRLF",
    "REMOVE_TAB",
    "GET_ONLY_DIGITS",
    "REMOVE_DIGITS",
    "STRING_LEN",
    "LOAD_FILE_CONTENT_BINARY",
    "ADD_TIME_TO_DATE",
    "QUARTER_OF_DATE",
    "SUBSTITUTE_VARIABLE",
    "UNESCAPE_XML",
    "ESCAPE_HTML",
    "UNESCAPE_HTML",
    "ESCAPE_SQL",
    "DATE_WORKING_DIFF",
    "ADD_MONTHS",
    "CHECK_XML_FILE_WELL_FORMED",
    "CHECK_XML_WELL_FORMED",
    "GET_FILE_ENCODING",
    "DAMERAU_LEVENSHTEIN",
    "NEEDLEMAN_WUNSH",
    "JARO",
    "JARO_WINKLER",
    "SOUNDEX",
    "REFINED_SOUNDEX",
    "ADD_HOURS",
    "ADD_MINUTES",
    "DATE_DIFF_MSEC",
    "DATE_DIFF_SEC",
    "DATE_DIFF_MN",
    "DATE_DIFF_HR",
    "HOUR_OF_DAY",
    "MINUTE_OF_HOUR",
    "SECOND_OF_MINUTE",
    "ROUND_CUSTOM_1",
    "ROUND_CUSTOM_2",
    "ADD_SECONDS",
    "REMAINDER",
)

_CALC_TYPE_ALIASES: dict[str, str] = {
    "PLUS": "ADD",
    "MINUS": "SUBTRACT",
    "MOD": "REMAINDER",
    "SQRT": "SQUARE_ROOT",
    "ROUND": "ROUND_1",
    "UPPER": "UPPER_CASE",
    "LOWER": "LOWER_CASE",
    "INITCAP": "INIT_CAP",
    "LENGTH": "STRING_LEN",
    "COPY_FIELD": "COPY_OF_FIELD",
    "TRIM": "TRIM",
    "BOOLEAN": "BOOLEAN",
    "GT": "GREATER_THAN",
    "GTE": "GREATER_EQUAL",
    "GE": "GREATER_EQUAL",
    "LT": "LESS_THAN",
    "LTE": "LESS_EQUAL",
    "LE": "LESS_EQUAL",
    "EQ": "EQUAL",
    "NE": "NOT_EQUAL",
    "COMPARISON": "EQUAL",
}


def _normalize_calc_type(raw: str) -> str:
    text = (raw or "").strip().upper().replace(" ", "_")
    if text.isdigit():
        idx = int(text)
        if 0 <= idx < len(_CALC_DESC_BY_ID):
            return _CALC_DESC_BY_ID[idx]
    return _CALC_TYPE_ALIASES.get(text, text)


def _col(field_name: str) -> str:
    if not field_name:
        return "lit(None)"
    return f'col("{field_name}")'


def _operand(field_name: str, value_type: str = "") -> str:
    """Resolve a calculator operand as a column reference or literal."""
    if not field_name:
        return "lit(None)"
    if value_type:
        lit_expr = _lit_or_col(field_name, value_type)
        if lit_expr != f"lit({field_name!r})":
            return lit_expr
    try:
        if "." in field_name:
            float(field_name)
            return f"lit({float(field_name)})"
        int(field_name)
        return f"lit({int(field_name)})"
    except ValueError:
        pass
    return _col(field_name)


def _lit_or_col(value: str, value_type: str = "") -> str:
    if not value:
        return "lit(None)"
    if value_type.lower() in ("number", "integer", "bignumber"):
        try:
            if "." in value:
                return f"lit({float(value)})"
            return f"lit({int(value)})"
        except ValueError:
            pass
    if value_type.lower() in ("boolean", "bool"):
        return "lit(True)" if value.strip().upper() in ("Y", "YES", "TRUE", "1", "T") else "lit(False)"
    return f"lit({value!r})"


def _is_string_type(value_type: str) -> bool:
    return (value_type or "").strip().lower() == "string"


def _int_operand(field_expr: str) -> str:
    return f"{field_expr}.cast('int')"


def _add_to_timestamp(date_expr: str, amount_expr: str, seconds_per_unit: int) -> str:
    return (
        f"from_unixtime(unix_timestamp({date_expr}) + "
        f"({amount_expr}).cast('long') * {seconds_per_unit})"
    )


def _date_diff_seconds(start_expr: str, end_expr: str) -> str:
    return f"(unix_timestamp({start_expr}) - unix_timestamp({end_expr}))"


def _apply_output_cast(expr: str, calc: CalculationSpec) -> str:
    vt = (calc.value_type or "").strip().lower()
    if not vt:
        return expr
    if vt in ("integer", "int"):
        return f"({expr}).cast('int')"
    if vt in ("number", "bignumber", "float", "double"):
        prec = (calc.value_precision or "").strip()
        length = (calc.value_length or "").strip()
        if prec.isdigit() and length.isdigit():
            return f"({expr}).cast('decimal({length},{prec})')"
        return f"({expr}).cast('double')"
    if vt == "string":
        return f"({expr}).cast('string')"
    if vt in ("date", "timestamp", "datetime"):
        return f"({expr}).cast('timestamp')"
    if vt in ("boolean", "bool"):
        return f"({expr}).cast('boolean')"
    return expr


def _unknown_calc_type(calc_type: str) -> str:
    return f"{_UNSUPPORTED_MARKER}{calc_type}"


@dataclass
class CalculationConvertResult:
    """Result of converting one Calculator entry to PySpark."""

    expr: str
    warning: str = ""
    supported: bool = True


def _unsupported_calc_fallback(calc: CalculationSpec, calc_type: str) -> tuple[str, str]:
    """Return a non-null fallback expression and a conversion warning."""
    if calc.field_a:
        expr = _col(calc.field_a)
        return (
            expr,
            (
                f"Calculator function '{calc_type}' is not supported for output column "
                f"'{calc.field_name}'; using field_a '{calc.field_a}' as placeholder"
            ),
        )
    if calc.field_name:
        expr = _col(calc.field_name)
        return (
            expr,
            (
                f"Calculator function '{calc_type}' is not supported for output column "
                f"'{calc.field_name}'; preserving existing column value as placeholder"
            ),
        )
    return (
        "lit(None)",
        f"Calculator function '{calc_type}' is not supported and no source field is available",
    )


def _calculation_incomplete_warning(calc: CalculationSpec) -> str:
    if not (calc.field_name or "").strip():
        return "Calculator entry is missing field_name"
    if not (calc.calc_type or "").strip():
        return f"Calculator entry '{calc.field_name}' is missing calc_type"
    return ""


def calculations_from_metadata(metadata: dict[str, Any]) -> list[CalculationSpec]:
    """Build CalculationSpec list from propagated converter metadata."""
    specs: list[CalculationSpec] = []
    for item in metadata.get("calculations") or []:
        if not isinstance(item, dict):
            continue
        field_name = (item.get("field_name") or "").strip()
        if not field_name:
            continue
        specs.append(
            CalculationSpec(
                field_name=field_name,
                calc_type=item.get("calc_type", ""),
                field_a=item.get("field_a", ""),
                field_b=item.get("field_b", ""),
                field_c=item.get("field_c", ""),
                value_type=item.get("value_type", ""),
                conversion_mask=item.get("conversion_mask", ""),
                value=item.get("value", ""),
                remove=bool(item.get("remove")),
                decimal_symbol=item.get("decimal_symbol", ""),
                grouping_symbol=item.get("grouping_symbol", ""),
                currency_symbol=item.get("currency_symbol", ""),
                value_length=item.get("value_length", ""),
                value_precision=item.get("value_precision", ""),
            )
        )
    return specs


def convert_calculation_result(calc: CalculationSpec) -> CalculationConvertResult:
    """Convert a single Calculator entry to a PySpark column expression with warnings."""
    incomplete = _calculation_incomplete_warning(calc)
    if incomplete:
        fallback, warning = _unsupported_calc_fallback(calc, calc.calc_type or "UNKNOWN")
        return CalculationConvertResult(
            expr=_apply_output_cast(fallback, calc),
            warning=incomplete if not warning else f"{incomplete}; {warning}",
            supported=False,
        )

    calc_type = _normalize_calc_type(calc.calc_type)
    a = _operand(calc.field_a, calc.value_type)
    b = _operand(calc.field_b, calc.value_type)
    c = _operand(calc.field_c, calc.value_type)
    expr = _build_calculation_expr(calc, calc_type, a, b, c)
    if expr.startswith(_UNSUPPORTED_MARKER):
        unsupported_type = expr[len(_UNSUPPORTED_MARKER):]
        fallback, warning = _unsupported_calc_fallback(calc, unsupported_type)
        return CalculationConvertResult(
            expr=_apply_output_cast(fallback, calc),
            warning=warning,
            supported=False,
        )
    return CalculationConvertResult(
        expr=_apply_output_cast(expr, calc),
        supported=True,
    )


def convert_calculation(calc: CalculationSpec) -> str:
    """Convert a single Calculator entry to a PySpark column expression."""
    return convert_calculation_result(calc).expr


def _build_calculation_expr(
    calc: CalculationSpec,
    calc_type: str,
    a: str,
    b: str,
    c: str,
) -> str:
    # Arithmetic
    if calc_type in ("ADD",):
        if _is_string_type(calc.value_type):
            return f"concat({a}, {b})"
        return f"({a} + {b})"
    if calc_type in ("SUBTRACT",):
        return f"({a} - {b})"
    if calc_type in ("MULTIPLY",):
        if _is_string_type(calc.value_type):
            return f"repeat({a}, {_int_operand(b)})"
        return f"({a} * {b})"
    if calc_type in ("DIVIDE",):
        return f"({a} / {b})"
    if calc_type in ("REMAINDER",):
        return f"({a} % {b})"
    if calc_type in ("SQUARE",):
        return f"({a} * {a})"
    if calc_type in ("SQUARE_ROOT",):
        return f"sqrt({a})"
    if calc_type in ("ABS",):
        return f"abs({a})"
    if calc_type in ("ROUND_1", "ROUND_STD_1"):
        return f"round({a})"
    if calc_type in ("ROUND_2", "ROUND_STD_2"):
        return f"round({a}, {_int_operand(b)})"
    if calc_type in ("ROUND_CUSTOM_1",):
        return f"round({a}, {_int_operand(b)})"
    if calc_type in ("ROUND_CUSTOM_2",):
        return f"round({a}, {_int_operand(b)})"
    if calc_type in ("CEIL",):
        return f"ceil({a})"
    if calc_type in ("FLOOR",):
        return f"floor({a})"
    if calc_type in ("NVL",):
        return f"coalesce({a}, {b})"
    if calc_type in ("COMBINATION_1",):
        return f"({a} + {b} * {c})"
    if calc_type in ("ADD3",):
        if _is_string_type(calc.value_type):
            return f"concat({a}, {b}, {c})"
        return f"({a} + {b} + {c})"
    if calc_type in ("COMBINATION_2",):
        return f"sqrt(({a} * {a}) + ({b} * {b}))"
    if calc_type in ("POWER",):
        return f"pow({a}, {b})"

    # Percent variants
    if calc_type == "PERCENT_1":
        return f"(lit(100) * {a} / {b})"
    if calc_type == "PERCENT_2":
        return f"({a} - ({a} * {b} / lit(100)))"
    if calc_type == "PERCENT_3":
        return f"({a} + ({a} * {b} / lit(100)))"

    # String
    if calc_type in ("UPPER_CASE",):
        return f"upper({a})"
    if calc_type in ("LOWER_CASE",):
        return f"lower({a})"
    if calc_type in ("INIT_CAP",):
        return f"initcap({a})"
    if calc_type in ("STRING_LEN",):
        return f"length({a})"
    if calc_type in ("CONCAT",):
        parts = [p for p in (a, b, c) if p != "lit(None)"]
        if len(parts) < 2:
            parts = [a, b]
        return f"concat({', '.join(parts)})"
    if calc_type in ("TRIM",):
        return f"trim({a})"
    if calc_type in ("REMOVE_CR",):
        return f"regexp_replace({a}, '\\r', '')"
    if calc_type in ("REMOVE_LF",):
        return f"regexp_replace({a}, '\\n', '')"
    if calc_type in ("REMOVE_CRLF",):
        return f"regexp_replace({a}, '\\r\\n', '')"
    if calc_type in ("REMOVE_TAB",):
        return f"regexp_replace({a}, '\\t', '')"
    if calc_type in ("GET_ONLY_DIGITS",):
        return f"regexp_replace({a}, '[^0-9]', '')"
    if calc_type in ("REMOVE_DIGITS",):
        return f"regexp_replace({a}, '[0-9]', '')"
    if calc_type in ("MASK_XML",):
        return f"regexp_replace({a}, '[&<>\"]', '')"
    if calc_type in ("USE_CDATA",):
        return f"concat(lit('<![CDATA['), {a}, lit(']]>'))"
    if calc_type in ("MD5",):
        return f"md5({a})"
    if calc_type in ("SHA1",):
        return f"sha1({a})"
    if calc_type in ("SOUNDEX",):
        return f"soundex({a})"
    if calc_type in ("METAPHONE",):
        return f"soundex({a})"

    # Date / time
    if calc_type == "ADD_DAYS":
        return f"date_add({a}, {_int_operand(b)})"
    if calc_type == "ADD_MONTHS":
        return f"add_months({a}, {_int_operand(b)})"
    if calc_type == "ADD_HOURS":
        return _add_to_timestamp(a, b, 3600)
    if calc_type == "ADD_MINUTES":
        return _add_to_timestamp(a, b, 60)
    if calc_type == "ADD_SECONDS":
        return _add_to_timestamp(a, b, 1)
    if calc_type == "DATE_DIFF":
        return f"datediff({a}, {b})"
    if calc_type == "DATE_DIFF_SEC":
        return _date_diff_seconds(a, b)
    if calc_type == "DATE_DIFF_MN":
        return f"({_date_diff_seconds(a, b)} / 60)"
    if calc_type == "DATE_DIFF_HR":
        return f"({_date_diff_seconds(a, b)} / 3600)"
    if calc_type == "DATE_DIFF_MSEC":
        return f"({_date_diff_seconds(a, b)} * 1000)"
    if calc_type == "YEAR_OF_DATE":
        return f"year({a})"
    if calc_type == "MONTH_OF_DATE":
        return f"month({a})"
    if calc_type == "DAY_OF_MONTH":
        return f"dayofmonth({a})"
    if calc_type == "DAY_OF_YEAR":
        return f"dayofyear({a})"
    if calc_type == "DAY_OF_WEEK":
        return f"dayofweek({a})"
    if calc_type == "WEEK_OF_YEAR":
        return f"weekofyear({a})"
    if calc_type == "WEEK_OF_YEAR_ISO8601":
        return f"weekofyear({a})"
    if calc_type == "YEAR_OF_DATE_ISO8601":
        return f"year({a})"
    if calc_type == "QUARTER_OF_DATE":
        return f"quarter({a})"
    if calc_type == "HOUR_OF_DAY":
        return f"hour({a})"
    if calc_type == "MINUTE_OF_HOUR":
        return f"minute({a})"
    if calc_type == "SECOND_OF_MINUTE":
        return f"second({a})"
    if calc_type == "REMOVE_TIME_FROM_DATE":
        return f"to_date({a})"
    if calc_type == "ADD_TIME_TO_DATE":
        return f"to_timestamp(concat(date_format({a}, 'yyyy-MM-dd'), lit(' '), date_format({b}, 'HH:mm:ss')))"

    # Boolean / comparison
    if calc_type in ("BOOLEAN",):
        return f"{a}.cast('boolean')"
    if calc_type in ("GREATER_THAN",):
        return f"({a} > {b})"
    if calc_type in ("LESS_THAN",):
        return f"({a} < {b})"
    if calc_type in ("EQUAL",):
        return f"({a} == {b})"
    if calc_type in ("NOT_EQUAL",):
        return f"({a} != {b})"
    if calc_type in ("GREATER_EQUAL",):
        return f"({a} >= {b})"
    if calc_type in ("LESS_EQUAL",):
        return f"({a} <= {b})"

    # Constants / copy
    if calc_type == "CONSTANT":
        return _lit_or_col(calc.value or calc.field_a, calc.value_type)
    if calc_type in ("COPY_OF_FIELD",):
        return a

    # Hex / encoding helpers (best-effort Spark equivalents)
    if calc_type in ("CHAR_TO_HEX_ENCODE", "BYTE_TO_HEX_ENCODE"):
        return f"hex({a})"
    if calc_type in ("HEX_TO_BYTE_DECODE", "HEX_TO_CHAR_DECODE"):
        return f"unhex({a})"

    if calc_type in (
        "CRC32",
        "ADLER32",
        "DOUBLE_METAPHONE",
        "LEVENSHTEIN_DISTANCE",
        "LOAD_FILE_CONTENT_BINARY",
        "SUBSTITUTE_VARIABLE",
        "UNESCAPE_XML",
        "ESCAPE_HTML",
        "UNESCAPE_HTML",
        "ESCAPE_SQL",
        "DATE_WORKING_DIFF",
        "CHECK_XML_FILE_WELL_FORMED",
        "CHECK_XML_WELL_FORMED",
        "GET_FILE_ENCODING",
        "DAMERAU_LEVENSHTEIN",
        "NEEDLEMAN_WUNSH",
        "JARO",
        "JARO_WINKLER",
        "REFINED_SOUNDEX",
        "-",
        "NONE",
        "CALC_NONE",
    ):
        return _unknown_calc_type(calc_type)

    return _unknown_calc_type(calc_type)
