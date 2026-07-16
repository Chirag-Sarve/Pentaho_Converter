"""Convert Pentaho Calculator step definitions to PySpark column expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .step_xml import (
    CalculationSpec,
    _CALC_LONG_DESC_BY_TYPE,
    _normalize_calc_type as _normalize_calc_type_xml,
)

_UNSUPPORTED_MARKER = "__UNSUPPORTED__:"

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
    "COPY_OF_FIELD": "COPY_OF_FIELD",
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
    """Normalize to converter-canonical names (COPY_FIELD → COPY_OF_FIELD)."""
    text = _normalize_calc_type_xml(raw or "")
    if not text:
        return ""
    if text == "COPY_FIELD":
        return "COPY_OF_FIELD"
    upper = text.upper().replace(" ", "_")
    if upper in _CALC_TYPE_ALIASES:
        return _CALC_TYPE_ALIASES[upper]
    # Long desc already mapped by parser; also handle converter-local aliases
    collapsed = " ".join((raw or "").split())
    if collapsed in _CALC_LONG_DESC_BY_TYPE:
        mapped = _CALC_LONG_DESC_BY_TYPE[collapsed]
        return "COPY_OF_FIELD" if mapped == "COPY_FIELD" else mapped
    return upper


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


def _decimal_cast(calc: CalculationSpec, *, default_precision: str = "38", default_scale: str = "10") -> str:
    """Build a Spark decimal cast, preferring Pentaho length/precision when present."""
    prec = (calc.value_precision or "").strip()
    length = (calc.value_length or "").strip()
    if prec.isdigit() and length.isdigit():
        return f"decimal({length},{prec})"
    if prec.isdigit():
        return f"decimal({default_precision},{prec})"
    if length.isdigit():
        return f"decimal({length},{default_scale})"
    return f"decimal({default_precision},{default_scale})"


def _operand_type_is_decimal(type_name: str) -> bool:
    return (type_name or "").strip().lower() in (
        "bignumber",
        "bigdecimal",
        "decimal",
    )


def _source_operands_are_decimal(
    calc: CalculationSpec,
    operand_types: dict[str, str] | None,
) -> bool:
    if not operand_types:
        return False
    for name in (calc.field_a, calc.field_b, calc.field_c):
        if name and _operand_type_is_decimal(operand_types.get(name, "")):
            return True
    return False


def _apply_output_cast(
    expr: str,
    calc: CalculationSpec,
    operand_types: dict[str, str] | None = None,
) -> str:
    """Apply Calculator value_type cast; preserve DECIMAL over double when possible."""
    vt = (calc.value_type or "").strip().lower()
    source_decimal = _source_operands_are_decimal(calc, operand_types)

    if not vt:
        if source_decimal:
            return f"({expr}).cast('{_decimal_cast(calc)}')"
        return expr
    if vt in ("integer", "int"):
        return f"({expr}).cast('int')"
    if vt in ("bignumber", "bigdecimal", "decimal"):
        return f"({expr}).cast('{_decimal_cast(calc)}')"
    if vt in ("number", "float", "double"):
        prec = (calc.value_precision or "").strip()
        length = (calc.value_length or "").strip()
        if prec.isdigit() and length.isdigit():
            return f"({expr}).cast('decimal({length},{prec})')"
        # Preserve DECIMAL when operands are decimal/BigNumber rather than widening to double.
        if source_decimal or (vt == "number" and (prec.isdigit() or length.isdigit())):
            return f"({expr}).cast('{_decimal_cast(calc)}')"
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
                calc_type=_normalize_calc_type(item.get("calc_type", "")),
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


def convert_calculation_result(
    calc: CalculationSpec,
    operand_types: dict[str, str] | None = None,
) -> CalculationConvertResult:
    """Convert a single Calculator entry to a PySpark column expression with warnings."""
    incomplete = _calculation_incomplete_warning(calc)
    if incomplete:
        fallback, warning = _unsupported_calc_fallback(calc, calc.calc_type or "UNKNOWN")
        return CalculationConvertResult(
            expr=_apply_output_cast(fallback, calc, operand_types),
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
            expr=_apply_output_cast(fallback, calc, operand_types),
            warning=warning,
            supported=False,
        )
    warning = ""
    if calc.conversion_mask or calc.decimal_symbol or calc.grouping_symbol or calc.currency_symbol:
        warning = (
            f"preserved.conversion_mask={calc.conversion_mask!r} "
            f"decimal={calc.decimal_symbol!r} grouping={calc.grouping_symbol!r} "
            f"currency={calc.currency_symbol!r} — Spark cast does not apply locale masks"
        )
    if calc_type == "ADLER32":
        extra = "ADLER32 approximated with crc32()"
        warning = f"{warning}; {extra}" if warning else extra
    if calc_type == "METAPHONE":
        extra = "METAPHONE approximated with soundex() — not a true Metaphone"
        warning = f"{warning}; {extra}" if warning else extra
    return CalculationConvertResult(
        expr=_apply_output_cast(expr, calc, operand_types),
        warning=warning,
        supported=True,
    )


def convert_calculation(
    calc: CalculationSpec,
    operand_types: dict[str, str] | None = None,
) -> str:
    """Convert a single Calculator entry to a PySpark column expression."""
    return convert_calculation_result(calc, operand_types).expr


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
        return f"regexp_replace({a}, '\\r|\\n', '')"
    if calc_type in ("REMOVE_TAB",):
        return f"regexp_replace({a}, '\\t', '')"
    if calc_type in ("GET_ONLY_DIGITS",):
        return f"regexp_replace({a}, '[^0-9]', '')"
    if calc_type in ("REMOVE_DIGITS",):
        return f"regexp_replace({a}, '[0-9]', '')"
    if calc_type in ("MASK_XML",):
        # Escape XML special characters (Pentaho MASK_XML), not strip them
        return (
            f"regexp_replace(regexp_replace(regexp_replace(regexp_replace("
            f"regexp_replace({a}, '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), "
            f"\"'\", '&apos;'), '\"', '&quot;')"
        )
    if calc_type in ("USE_CDATA",):
        return f"concat(lit('<![CDATA['), {a}, lit(']]>'))"
    if calc_type in ("MD5",):
        return f"md5({a})"
    if calc_type in ("SHA1",):
        return f"sha1({a})"
    if calc_type in ("CRC32",):
        return f"crc32({a})"
    if calc_type in ("ADLER32",):
        # Spark has no Adler32 — approximate with crc32 and rely on warning path via alias
        return f"crc32({a})"
    if calc_type in ("LEVENSHTEIN_DISTANCE",):
        return f"levenshtein({a}.cast('string'), {b}.cast('string'))"
    if calc_type in ("SOUNDEX",):
        return f"soundex({a})"
    if calc_type in ("METAPHONE",):
        # Approximation — Spark has soundex only; warning added in convert_calculation_result
        return f"soundex({a})"
    if calc_type in ("UNESCAPE_XML",):
        return (
            f"regexp_replace(regexp_replace(regexp_replace(regexp_replace("
            f"regexp_replace({a}, '&quot;', '\"'), '&apos;', \"'\"), '&gt;', '>'), "
            f"'&lt;', '<'), '&amp;', '&')"
        )
    if calc_type in ("ESCAPE_HTML",):
        return (
            f"regexp_replace(regexp_replace(regexp_replace({a}, '&', '&amp;'), "
            f"'<', '&lt;'), '>', '&gt;')"
        )
    if calc_type in ("UNESCAPE_HTML",):
        return (
            f"regexp_replace(regexp_replace(regexp_replace({a}, '&gt;', '>'), "
            f"'&lt;', '<'), '&amp;', '&')"
        )
    if calc_type in ("ESCAPE_SQL",):
        return f"regexp_replace({a}, \"'\", \"''\")"

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

    if calc_type in ("DAMERAU_LEVENSHTEIN",):
        return f"levenshtein({a}.cast('string'), {b}.cast('string'))"
    if calc_type in ("JARO", "JARO_WINKLER"):
        # Emit a Python UDF hook — generated modules define `_jaro_similarity`.
        return f"_jaro_udf({a}.cast('string'), {b}.cast('string'))"

    if calc_type in (
        "DOUBLE_METAPHONE",
        "LOAD_FILE_CONTENT_BINARY",
        "SUBSTITUTE_VARIABLE",
        "DATE_WORKING_DIFF",
        "CHECK_XML_FILE_WELL_FORMED",
        "CHECK_XML_WELL_FORMED",
        "GET_FILE_ENCODING",
        "NEEDLEMAN_WUNSH",
        "REFINED_SOUNDEX",
        "-",
        "NONE",
        "CALC_NONE",
    ):
        return _unknown_calc_type(calc_type)

    return _unknown_calc_type(calc_type)
