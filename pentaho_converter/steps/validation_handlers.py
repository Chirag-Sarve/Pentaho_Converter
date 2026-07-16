"""Handlers for Pentaho Validation transformation steps.

Supports:
- Credit Card Validator (Luhn + card type detection)
- Data Validator (field rules → PySpark predicates)
- Mail Validator (regex / Python email checks; SMTP documented as unsupported)
- XSD Validator (lxml / xmlschema UDF with migration warnings)
"""

from __future__ import annotations

import logging
import re

from ..filter_converter import _branch_stream_name, _connected_branch_targets
from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_credit_card_validator_config,
    parse_data_validator_config,
    parse_mail_validator_config,
    parse_xsd_validator_config,
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
    logger.warning("Validation step '%s': %s", step_name, message)


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


def _merge_cfg(meta: dict, cfg: dict) -> None:
    for key, val in cfg.items():
        if key not in meta or meta.get(key) in (None, "", [], {}):
            meta[key] = val


def _resolve_accept_reject(
    meta: dict,
    context: StepContext,
) -> tuple[str | None, str | None]:
    """Map accepted/rejected destinations from Filter-style or error-hop metadata."""
    true_target, false_target = _connected_branch_targets(meta, context, context.step.name)
    err = (meta.get("error_target_step") or "").strip()
    if err and not false_target:
        successors = set(context.dag.successors(context.step.name))
        if not successors or err in successors:
            false_target = err
            if not true_target and successors:
                remaining = sorted(successors - {err})
                if remaining:
                    true_target = remaining[0]
    return true_target, false_target


def _emit_accept_reject(
    lines: list[str],
    *,
    in_df: str,
    out_var: str,
    result_col: str,
    true_target: str | None,
    false_target: str | None,
    bool_result: bool = True,
) -> None:
    """Split accepted/rejected streams using the validation result column."""
    if bool_result:
        ok_pred = f'col({result_col!r}) == lit(True)'
        bad_pred = f'col({result_col!r}) == lit(False)'
    else:
        # String-mode result: treat configured "valid" message / True / Y / valid as accepted
        ok_pred = (
            f'(col({result_col!r}).cast("string").isin("True", "true", "Y", "1", "valid") '
            f'| col({result_col!r}).cast("boolean") == lit(True))'
        )
        bad_pred = f'~({ok_pred})'

    if true_target and false_target:
        true_var = _branch_stream_name(true_target)
        false_var = _branch_stream_name(false_target)
        lines.append(f"{true_var} = {in_df}.filter({ok_pred})")
        lines.append(f"{false_var} = {in_df}.filter({bad_pred})")
        if out_var not in (true_var, false_var):
            lines.append(f"{out_var} = {true_var}")
        return

    if false_target and not true_target:
        false_var = _branch_stream_name(false_target)
        lines.append(f"{false_var} = {in_df}.filter({bad_pred})")
        lines.append(f"{out_var} = {in_df}.filter({ok_pred})")
        return

    if true_target and not false_target:
        true_var = _branch_stream_name(true_target)
        lines.append(f"{true_var} = {in_df}.filter({ok_pred})")
        if out_var != true_var:
            lines.append(f"{out_var} = {true_var}")
        return

    lines.append(f"{out_var} = {in_df}")


def _spark_type_for_pentaho(data_type: str) -> str | None:
    t = (data_type or "").strip().lower()
    mapping = {
        "string": "string",
        "number": "double",
        "integer": "long",
        "bignumber": "decimal(38,18)",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "binary": "binary",
    }
    return mapping.get(t)


def _numeric_literal_expr(raw: str) -> str | None:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        if "." in text or "e" in text.lower():
            return f"lit({float(text)})"
        return f"lit({int(text)})"
    except ValueError:
        return f"lit({text!r})"


def _normalized_numeric_expr(field: str, rule: dict) -> str:
    """Build a Spark string expression with grouping/decimal symbols normalized."""
    s = f'col({field!r}).cast("string")'
    grouping = (rule.get("grouping_symbol") or "").strip()
    decimal = (rule.get("decimal_symbol") or "").strip()
    expr = s
    if grouping:
        expr = f'regexp_replace({expr}, {re.escape(grouping)!r}, "")'
    if decimal and decimal != ".":
        expr = f'regexp_replace({expr}, {re.escape(decimal)!r}, ".")'
    return expr


def _rule_ok_expr(rule: dict) -> tuple[str, list[str]]:
    """Build a Spark boolean expression that is True when the rule passes."""
    field = (rule.get("field_name") or "").strip()
    warnings: list[str] = []
    if not field:
        return "lit(True)", ["Data Validator rule missing field_name"]

    c = f'col({field!r})'
    s = f'{c}.cast("string")'
    is_null = f'({c}.isNull() | ({s} == lit("")))'
    checks: list[str] = []
    num_expr = _normalized_numeric_expr(field, rule)

    if rule.get("only_null_allowed"):
        return is_null, warnings

    if not rule.get("null_allowed", True):
        checks.append(f"~({is_null})")

    if rule.get("only_numeric_allowed"):
        checks.append(
            f'({is_null}) | {num_expr}.rlike(r"^[+-]?\\d+(\\.\\d+)?([eE][+-]?\\d+)?$")'
        )

    dt = rule.get("data_type") or ""
    if rule.get("data_type_verified") and dt:
        spark_t = _spark_type_for_pentaho(dt)
        if spark_t in ("date", "timestamp"):
            mask = rule.get("conversion_mask") or ""
            if spark_t == "date":
                if mask:
                    checks.append(
                        f'({is_null}) | to_date({s}, {mask!r}).isNotNull()'
                    )
                else:
                    checks.append(f'({is_null}) | to_date({s}).isNotNull()')
            else:
                if mask:
                    checks.append(
                        f'({is_null}) | to_timestamp({s}, {mask!r}).isNotNull()'
                    )
                else:
                    checks.append(f'({is_null}) | to_timestamp({s}).isNotNull()')
        elif spark_t == "boolean":
            checks.append(
                f'({is_null}) | lower({s}).isin("true", "false", "y", "n", "1", "0")'
            )
        elif spark_t in ("double", "long", "decimal(38,18)"):
            checks.append(
                f'({is_null}) | {num_expr}.rlike(r"^[+-]?\\d+(\\.\\d+)?([eE][+-]?\\d+)?$")'
            )
        elif spark_t == "binary":
            warnings.append(
                f"data_type Binary verification for {field!r} approximated as non-null string"
            )
        # String needs no cast verification beyond presence

    min_len = (rule.get("minimum_length") or "").strip()
    max_len = (rule.get("maximum_length") or "").strip()
    if min_len:
        try:
            checks.append(
                f'({is_null}) | (length({s}) >= lit({int(min_len)}))'
            )
        except ValueError:
            warnings.append(f"invalid minimum_length={min_len!r} for {field}")
    if max_len:
        try:
            checks.append(
                f'({is_null}) | (length({s}) <= lit({int(max_len)}))'
            )
        except ValueError:
            warnings.append(f"invalid maximum_length={max_len!r} for {field}")

    min_val = (rule.get("minimum_value") or "").strip()
    max_val = (rule.get("maximum_value") or "").strip()
    if min_val:
        lit_v = _numeric_literal_expr(min_val)
        if lit_v:
            checks.append(f'({is_null}) | ({num_expr}.cast("double") >= {lit_v})')
    if max_val:
        lit_v = _numeric_literal_expr(max_val)
        if lit_v:
            checks.append(f'({is_null}) | ({num_expr}.cast("double") <= {lit_v})')

    start = rule.get("start_string") or ""
    end = rule.get("end_string") or ""
    start_na = rule.get("start_string_not_allowed") or ""
    end_na = rule.get("end_string_not_allowed") or ""
    if start:
        checks.append(f'({is_null}) | {s}.startswith({start!r})')
    if end:
        checks.append(f'({is_null}) | {s}.endswith({end!r})')
    if start_na:
        checks.append(f'({is_null}) | ~{s}.startswith({start_na!r})')
    if end_na:
        checks.append(f'({is_null}) | ~{s}.endswith({end_na!r})')

    regex = rule.get("regular_expression") or ""
    regex_na = rule.get("regular_expression_not_allowed") or ""
    if regex:
        checks.append(f'({is_null}) | {s}.rlike({regex!r})')
    if regex_na:
        checks.append(f'({is_null}) | ~{s}.rlike({regex_na!r})')

    allowed = rule.get("allowed_values") or []
    if allowed and not rule.get("is_sourcing_values"):
        vals = ", ".join(repr(str(v)) for v in allowed)
        checks.append(f'({is_null}) | {s}.isin({vals})')

    if rule.get("is_sourcing_values"):
        src_step = rule.get("sourcing_step") or ""
        src_field = rule.get("sourcing_field") or ""
        warnings.append(
            f"allowed values sourced from step={src_step!r} field={src_field!r} "
            f"— join against source DF at runtime if schema evolves"
        )
        if src_step and src_field:
            src_df = f"df_{src_step.replace(' ', '_').replace('-', '_')}"
            checks.append(
                f'({is_null}) | {s}.isin(['
                f'r[0] for r in {src_df}.select({src_field!r}).distinct().collect()'
                f'])'
            )

    if not checks:
        if rule.get("null_allowed", True):
            return "lit(True)", warnings
        return f"~({is_null})", warnings

    # Null-allowed: null rows pass unless only_null / not-null already encoded
    if rule.get("null_allowed", True) and not rule.get("only_null_allowed"):
        body = " & ".join(f"({p})" for p in checks)
        return f"(({is_null}) | ({body}))", warnings

    return " & ".join(f"({p})" for p in checks), warnings


# ---------------------------------------------------------------------------
# Credit Card Validator
# ---------------------------------------------------------------------------


class CreditCardValidatorHandler(BaseStepHandler):
    """Validate credit-card numbers via Luhn and detect vendor prefixes."""

    _TYPES = {"creditcardvalidator", "creditcard"}

    def can_handle(self, step_type: str) -> bool:
        t = _norm(step_type)
        if t in self._TYPES:
            return True
        # Catch display/legacy ids without colliding with Random CC generator
        return "creditcardvalid" in t and "generator" not in t and "random" not in t

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_credit_card_validator_config(step_el) if step_el is not None else {}
        _merge_cfg(meta, cfg)

        field = meta.get("fieldname") or ""
        result_field = (meta.get("resultfieldname") or "result").strip() or "result"
        card_type_field = (meta.get("cardtype") or "").strip()
        only_digits = bool(meta.get("onlydigits"))
        not_valid_msg = (meta.get("notvalidmsg") or "").strip()

        lines = [f"# Credit Card Validator: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "fieldname", "resultfieldname", "cardtype", "onlydigits", "notvalidmsg",
            "send_true_to", "send_false_to", "error_target_step",
        )))

        if not in_df:
            return _passthrough(context, "Credit Card Validator")
        if not field:
            lines.append("# WARNING: missing credit card fieldname")
            _warn(context.step.name, "missing credit card fieldname")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        fn = _safe_ident(out_var)
        lines.append("from pyspark.sql.functions import udf")
        lines.append("from pyspark.sql.types import BooleanType, StringType, StructType, StructField")
        lines.append(f"def _cc_luhn_check_{fn}(raw, only_digits={only_digits!r}):")
        lines.append("    if raw is None:")
        lines.append("        return (False, '', 'Credit card number is null')")
        lines.append("    text = str(raw).strip()")
        lines.append("    if not text:")
        lines.append("        return (False, '', 'Credit card number is empty')")
        lines.append("    if only_digits:")
        lines.append("        import re as _re")
        lines.append("        text = _re.sub(r'[^0-9]', '', text)")
        lines.append("    else:")
        lines.append("        text = text.replace(' ', '').replace('-', '')")
        lines.append("    if not text or not text.isdigit():")
        lines.append("        return (False, '', 'Credit card number has invalid format')")
        lines.append("    digits = [int(ch) for ch in text]")
        lines.append("    checksum = 0")
        lines.append("    parity = len(digits) % 2")
        lines.append("    for i, d in enumerate(digits):")
        lines.append("        if i % 2 == parity:")
        lines.append("            d *= 2")
        lines.append("            if d > 9:")
        lines.append("                d -= 9")
        lines.append("        checksum += d")
        lines.append("    if checksum % 10 != 0:")
        lines.append("        return (False, '', 'Failed Luhn (MOD-10) check')")
        lines.append("    # Vendor / card-type detection via BIN prefixes (Pentaho-compatible subset)")
        lines.append("    vendor = 'Unknown'")
        lines.append("    if text.startswith('4') and len(text) in (13, 16, 19):")
        lines.append("        vendor = 'Visa'")
        lines.append("    elif text[:2] in ('51', '52', '53', '54', '55') or (")
        lines.append("            len(text) >= 4 and 2221 <= int(text[:4]) <= 2720):")
        lines.append("        vendor = 'MasterCard'")
        lines.append("    elif text[:2] in ('34', '37') and len(text) == 15:")
        lines.append("        vendor = 'American Express'")
        lines.append("    elif text.startswith('6011') or text.startswith('65') or text[:3] in (")
        lines.append("            '644', '645', '646', '647', '648', '649'):")
        lines.append("        vendor = 'Discover'")
        lines.append("    elif text[:2] in ('36', '38') or text.startswith('30'):")
        lines.append("        vendor = 'Diners Club'")
        lines.append("    elif text.startswith('2014') or text.startswith('2149'):")
        lines.append("        vendor = 'enRoute'")
        lines.append("    elif text.startswith('35'):")
        lines.append("        vendor = 'JCB'")
        lines.append("    return (True, vendor, '')")
        lines.append(
            f"_cc_schema_{fn} = StructType(["
            f"StructField('ok', BooleanType(), False), "
            f"StructField('vendor', StringType(), True), "
            f"StructField('msg', StringType(), True)])"
        )
        lines.append(f"_cc_udf_{fn} = udf(_cc_luhn_check_{fn}, _cc_schema_{fn})")
        lines.append(
            f"_cc_tmp_{fn} = {in_df}.withColumn('_cc_check', _cc_udf_{fn}(col({field!r})))"
        )
        lines.append(
            f"{out_var} = _cc_tmp_{fn}.withColumn({result_field!r}, col('_cc_check.ok'))"
        )
        if card_type_field:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{card_type_field!r}, col('_cc_check.vendor'))"
            )
        else:
            lines.append("# cardtype field empty — vendor detection computed but not emitted")
        if not_valid_msg:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{not_valid_msg!r}, col('_cc_check.msg'))"
            )
        lines.append(f"{out_var} = {out_var}.drop('_cc_check')")
        lines.append(
            "# NOTE: card-type column holds detected vendor; "
            "Pentaho Credit Card Validator has no allowed-vendor restriction list"
        )

        true_t, false_t = _resolve_accept_reject(meta, context)
        if true_t or false_t:
            _emit_accept_reject(
                lines,
                in_df=out_var,
                out_var=out_var,
                result_col=result_field,
                true_target=true_t,
                false_target=false_t,
            )
        return lines, "converted"


# ---------------------------------------------------------------------------
# Data Validator
# ---------------------------------------------------------------------------


class DataValidatorHandler(BaseStepHandler):
    """Apply Pentaho Data Validator field rules and accept/reject branches."""

    _TYPES = {"validator", "datavalidator"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_data_validator_config(step_el) if step_el is not None else {}
        _merge_cfg(meta, cfg)

        validations = meta.get("validations") or []
        validate_all = bool(meta.get("validate_all"))
        concat_errors = bool(meta.get("concat_errors"))
        sep = meta.get("concat_separator") or "|"

        lines = [f"# Data Validator: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "validate_all", "concat_errors", "concat_separator", "validations",
            "send_true_to", "send_false_to", "error_target_step",
        )))
        if validations:
            lines.append(f"# preserved.validations_count={len(validations)}")
            for i, rule in enumerate(validations):
                if isinstance(rule, dict):
                    # Emit complete rule so every Pentaho property is retained in code
                    lines.append(f"# preserved.validation[{i}]={rule!r}")

        if not in_df:
            return _passthrough(context, "Data Validator")

        if not validations:
            lines.append("# WARNING: Data Validator has no validator_field rules")
            _warn(context.step.name, "no validator_field rules — accepting all rows")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        status = "converted"
        ok_exprs: list[str] = []
        err_whens: list[str] = []
        all_warnings: list[str] = []

        for rule in validations:
            if not isinstance(rule, dict):
                continue
            expr, warns = _rule_ok_expr(rule)
            all_warnings.extend(warns)
            ok_exprs.append(expr)
            code = (
                rule.get("error_code")
                or rule.get("validation_name")
                or rule.get("field_name")
                or "ERR"
            )
            desc = (
                rule.get("error_description")
                or f"Validation failed for {rule.get('field_name')}"
            )
            err_whens.append(f'when(~({expr}), lit({f"{code}:{desc}"!r}))')
            if rule.get("conversion_mask") and not rule.get("data_type_verified"):
                lines.append(
                    f"# preserved.conversion_mask for {rule.get('field_name')!r}="
                    f"{rule.get('conversion_mask')!r} (used when data_type_verified)"
                )

        for w in all_warnings:
            lines.append(f"# WARNING: {w}")
            _warn(context.step.name, w)
            status = "partial"

        if len(ok_exprs) == 1:
            all_ok = ok_exprs[0]
        else:
            all_ok = " & ".join(f"({e})" for e in ok_exprs)
            if not validate_all:
                lines.append(
                    "# NOTE: validate_all=N — Pentaho stops at first failure; "
                    "Spark evaluates all rules for accept/reject routing"
                )

        annotated = f"_validated_{_safe_ident(out_var)}"
        lines.append(f"{annotated} = {in_df}.withColumn('_row_valid', {all_ok})")
        has_errors_col = bool(err_whens)

        if concat_errors and err_whens:
            msg_cols: list[str] = []
            for i, wexpr in enumerate(err_whens):
                col_name = f"_verr_{i}"
                lines.append(
                    f"{annotated} = {annotated}.withColumn("
                    f"{col_name!r}, {wexpr}.otherwise(lit(None)))"
                )
                msg_cols.append(col_name)
            concat_args = ", ".join(f"col({c!r})" for c in msg_cols)
            lines.append(
                f"{annotated} = {annotated}.withColumn("
                f"'_validation_errors', concat_ws({sep!r}, {concat_args}))"
            )
            for c in msg_cols:
                lines.append(f"{annotated} = {annotated}.drop({c!r})")
            lines.append(f"# concat_errors separator={sep!r}")
        elif err_whens:
            lines.append(
                f"{annotated} = {annotated}.withColumn("
                f"'_validation_errors', {err_whens[0]}.otherwise(lit(None)))"
            )

        drop_ok = (
            ".drop('_row_valid', '_validation_errors')"
            if has_errors_col
            else ".drop('_row_valid')"
        )
        true_t, false_t = _resolve_accept_reject(meta, context)
        if true_t or false_t:
            true_var = _branch_stream_name(true_t) if true_t else None
            false_var = _branch_stream_name(false_t) if false_t else None
            if true_var and false_var:
                lines.append(
                    f"{true_var} = {annotated}.filter(col('_row_valid') == lit(True))"
                    f"{drop_ok}"
                )
                lines.append(
                    f"{false_var} = {annotated}.filter(col('_row_valid') == lit(False))"
                )
                if out_var not in (true_var, false_var):
                    lines.append(f"{out_var} = {true_var}")
            elif false_var:
                lines.append(
                    f"{false_var} = {annotated}.filter(col('_row_valid') == lit(False))"
                )
                lines.append(
                    f"{out_var} = {annotated}.filter(col('_row_valid') == lit(True))"
                    f"{drop_ok}"
                )
            else:
                lines.append(
                    f"{true_var} = {annotated}.filter(col('_row_valid') == lit(True))"
                    f"{drop_ok}"
                )
                if out_var != true_var:
                    lines.append(f"{out_var} = {true_var}")
        else:
            lines.append(
                f"{out_var} = {annotated}.filter(col('_row_valid') == lit(True))"
                f"{drop_ok}"
            )
            lines.append(
                "# rejected rows filtered out; configure send_false_to / error hop "
                "to retain invalid stream"
            )

        return lines, status


# ---------------------------------------------------------------------------
# Mail Validator
# ---------------------------------------------------------------------------


class MailValidatorHandler(BaseStepHandler):
    """Validate email addresses (regex / email.utils); SMTP check documented."""

    _TYPES = {"mailvalidator", "emailvalidator"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_mail_validator_config(step_el) if step_el is not None else {}
        _merge_cfg(meta, cfg)

        email_field = meta.get("emailfield") or ""
        result_field = meta.get("resultfieldname") or "result"
        as_string = bool(meta.get("result_as_string"))
        smtp_check = bool(meta.get("smtp_check"))
        valid_msg = meta.get("email_valid_msg") or "Email is valid"
        invalid_msg = meta.get("email_not_valid_msg") or "Email is not valid"
        errors_field = meta.get("errors_field_name") or ""
        timeout = meta.get("timeout") or "0"

        lines = [f"# Mail Validator: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "emailfield", "resultfieldname", "result_as_string", "smtp_check",
            "email_valid_msg", "email_not_valid_msg", "errors_field_name",
            "timeout", "default_smtp", "email_sender", "default_smtp_field",
            "dynamic_default_smtp", "send_true_to", "send_false_to",
            "error_target_step",
        )))

        if not in_df:
            return _passthrough(context, "Mail Validator")
        if not email_field:
            lines.append("# WARNING: missing emailfield")
            _warn(context.step.name, "missing emailfield")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        status = "converted"
        # smtpCheck=Y ⇒ Pentaho "strict" (SMTP); otherwise relaxed regex structure check
        if smtp_check:
            lines.append(
                f"# WARNING: smtpCheck=Y (timeout={timeout!r}, "
                f"defaultSMTP={meta.get('default_smtp')!r}, "
                f"sender={meta.get('email_sender')!r}) is unsupported on Databricks — "
                f"falling back to structural email validation"
            )
            _warn(context.step.name, "SMTP email verification unsupported; using regex/email.utils")
            status = "partial"
        else:
            lines.append("# email validation mode=relaxed (structure / regex; smtpCheck=N)")

        fn = _safe_ident(out_var)
        lines.append("from pyspark.sql.functions import udf")
        lines.append("from pyspark.sql.types import BooleanType, StringType, StructType, StructField")
        lines.append(f"def _mail_validate_{fn}(addr, strict={smtp_check!r}):")
        lines.append("    if addr is None:")
        lines.append("        return (False, 'Email address is null')")
        lines.append("    text = str(addr).strip()")
        lines.append("    if not text:")
        lines.append("        return (False, 'Email address is empty')")
        lines.append("    import re as _re")
        lines.append("    from email.utils import parseaddr")
        lines.append("    # Strict: tighter RFC-ish pattern; relaxed: basic local@domain")
        lines.append(
            "    _strict_re = _re.compile("
            "r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')"
        )
        lines.append(
            "    _relax_re = _re.compile(r'^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')"
        )
        lines.append("    _name, _email = parseaddr(text)")
        lines.append("    candidate = _email or text")
        lines.append("    pattern = _strict_re if strict else _relax_re")
        lines.append("    if pattern.match(candidate):")
        lines.append("        return (True, '')")
        lines.append("    return (False, 'Invalid email address format')")
        lines.append(
            f"_mail_schema_{fn} = StructType(["
            f"StructField('ok', BooleanType(), False), "
            f"StructField('err', StringType(), True)])"
        )
        lines.append(f"_mail_udf_{fn} = udf(_mail_validate_{fn}, _mail_schema_{fn})")
        lines.append(
            f"_mail_tmp_{fn} = {in_df}.withColumn("
            f"'_mail_check', _mail_udf_{fn}(col({email_field!r})))"
        )

        if as_string:
            lines.append(
                f"{out_var} = _mail_tmp_{fn}.withColumn("
                f"{result_field!r}, when(col('_mail_check.ok'), lit({valid_msg!r}))"
                f".otherwise(lit({invalid_msg!r})))"
            )
        else:
            lines.append(
                f"{out_var} = _mail_tmp_{fn}.withColumn("
                f"{result_field!r}, col('_mail_check.ok'))"
            )

        if errors_field:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{errors_field!r}, col('_mail_check.err'))"
            )
        lines.append(f"{out_var} = {out_var}.drop('_mail_check')")

        true_t, false_t = _resolve_accept_reject(meta, context)
        if true_t or false_t:
            if as_string:
                ok_pred = f"col({result_field!r}) == lit({valid_msg!r})"
            else:
                ok_pred = f"col({result_field!r}) == lit(True)"
            if true_t and false_t:
                tv, fv = _branch_stream_name(true_t), _branch_stream_name(false_t)
                lines.append(f"{tv} = {out_var}.filter({ok_pred})")
                lines.append(f"{fv} = {out_var}.filter(~({ok_pred}))")
                if out_var not in (tv, fv):
                    lines.append(f"{out_var} = {tv}")
            elif false_t:
                fv = _branch_stream_name(false_t)
                lines.append(f"{fv} = {out_var}.filter(~({ok_pred}))")
            elif true_t:
                tv = _branch_stream_name(true_t)
                lines.append(f"{tv} = {out_var}.filter({ok_pred})")
                if out_var != tv:
                    lines.append(f"{out_var} = {tv}")

        return lines, status


# ---------------------------------------------------------------------------
# XSD Validator
# ---------------------------------------------------------------------------


class XSDValidatorHandler(BaseStepHandler):
    """Validate XML against an XSD via Python XML libraries (lxml preferred)."""

    _TYPES = {"xsdvalidator", "xmlschemavalidator"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = _meta(context)
        step_el = get_step_element(context.step)
        cfg = parse_xsd_validator_config(step_el) if step_el is not None else {}
        _merge_cfg(meta, cfg)

        xml_field = meta.get("xmlstream") or ""
        xml_is_file = bool(meta.get("xmlsourcefile"))
        xsd_path = meta.get("xsdfilename") or ""
        xsd_source = (meta.get("xsdsource") or "filename").strip().lower()
        xsd_field = meta.get("xsddefinedfield") or ""
        result_field = meta.get("resultfieldname") or "result"
        as_string = bool(meta.get("outputstringfield"))
        if_valid = meta.get("ifxmlvalid") or ""
        if_invalid = meta.get("ifxmlinvalid") or ""
        add_msg = bool(meta.get("addvalidationmsg"))
        msg_field = meta.get("validationmsgfield") or "ValidationMsgField"
        allow_ext = bool(meta.get("allow_external_entities", True))

        lines = [f"# XSD Validator: {context.step.name}"]
        lines.extend(_preserve(meta, (
            "xmlstream", "xmlsourcefile", "xsdfilename", "xsdsource",
            "xsddefinedfield", "resultfieldname", "outputstringfield",
            "ifxmlvalid", "ifxmlinvalid", "addvalidationmsg", "validationmsgfield",
            "allow_external_entities", "send_true_to", "send_false_to",
            "error_target_step",
        )))

        if not in_df:
            return _passthrough(context, "XSD Validator")
        if not xml_field:
            lines.append("# WARNING: missing xmlstream field")
            _warn(context.step.name, "missing xmlstream")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        status = "converted"
        source_is_file = xsd_source in ("filename", "specify_filename", "")
        source_is_field = xsd_source in ("fieldname", "specify_fieldname")
        source_in_xml = xsd_source in (
            "noneed", "no_need", "isincludedinxml", "specify_file_in_xml",
        )

        if source_is_file and not xsd_path:
            lines.append("# WARNING: XSD filename missing — validation will fail at runtime")
            status = "partial"
        if source_is_field and not xsd_field:
            lines.append("# WARNING: XSD field name missing")
            status = "partial"
        if not allow_ext:
            lines.append(
                "# WARNING: allowExternalEntities=N — lxml still resolves local includes; "
                "remote entity resolution is disabled best-effort"
            )
            status = "partial"
        if source_in_xml:
            lines.append(
                "# WARNING: XSD embedded in XML (xsdsource=noneed) — "
                "schemaLocation / external schema fetch is not fully equivalent to Pentaho"
            )
            status = "partial"
        lines.append(
            "# NAMESPACE: lxml XMLSchema validates qualified names using "
            "targetNamespace / xmlns from the XSD and instance; "
            "prefix remapping must still match schema URIs"
        )
        lines.append(
            "# LIMITATION: unsupported / divergent vs Pentaho Xerces — "
            "xs:key/keyref/unique, xs:redefine, some identity constraints, "
            "and entity expansion policy may differ; review validation failures"
        )

        fn = _safe_ident(out_var)
        lines.append("from pyspark.sql.functions import udf")
        lines.append("from pyspark.sql.types import BooleanType, StringType, StructType, StructField")
        lines.append(
            f"def _xsd_validate_{fn}(xml_val, xsd_val=None, "
            f"xml_is_file={xml_is_file!r}, allow_ext={allow_ext!r}):"
        )
        lines.append("    if xml_val is None or str(xml_val).strip() == '':")
        lines.append("        return (False, 'XML is null or empty')")
        lines.append("    try:")
        lines.append("        from lxml import etree")
        lines.append("    except ImportError as exc:")
        lines.append(
            "        raise ImportError('lxml is required for XSD Validator migration') from exc"
        )
        lines.append("    try:")
        lines.append("        import os")
        lines.append("        if xml_is_file:")
        lines.append("            if not os.path.exists(str(xml_val)):")
        lines.append("                return (False, f'XML file not found: {xml_val}')")
        lines.append("            xml_doc = etree.parse(str(xml_val))")
        lines.append("        else:")
        lines.append(
            "            xml_bytes = xml_val.encode('utf-8') "
            "if isinstance(xml_val, str) else xml_val"
        )
        lines.append("            xml_doc = etree.fromstring(xml_bytes)")

        if source_is_field:
            lines.append("        if not xsd_val:")
            lines.append("            return (False, 'XSD path/content is missing')")
            lines.append("        if os.path.exists(str(xsd_val)):")
            lines.append("            schema_doc = etree.parse(str(xsd_val))")
            lines.append("        else:")
            lines.append(
                "            schema_doc = etree.fromstring("
                "xsd_val.encode('utf-8') if isinstance(xsd_val, str) else xsd_val)"
            )
            lines.append("        schema = etree.XMLSchema(schema_doc)")
            lines.append("        ok = schema.validate(xml_doc)")
            lines.append("        if ok:")
            lines.append("            return (True, '')")
            lines.append(
                "        err = '; '.join(str(e) for e in schema.error_log) "
                "if schema.error_log else 'XSD validation failed'"
            )
            lines.append("        return (False, err)")
        elif source_in_xml:
            lines.append("        # Well-formedness + optional xsi:schemaLocation (best-effort)")
            lines.append("        _ = allow_ext  # preserved for network policy documentation")
            lines.append(
                "        return (True, "
                "'Embedded XSD/schemaLocation not fully applied — well-formed XML only')"
            )
        else:
            lines.append(f"        xsd_path = {xsd_path!r}")
            lines.append("        if not xsd_path or not os.path.exists(xsd_path):")
            lines.append("            return (False, f'XSD file missing: {xsd_path!r}')")
            lines.append("        schema = etree.XMLSchema(etree.parse(xsd_path))")
            lines.append("        ok = schema.validate(xml_doc)")
            lines.append("        if ok:")
            lines.append("            return (True, '')")
            lines.append(
                "        err = '; '.join(str(e) for e in schema.error_log) "
                "if schema.error_log else 'XSD validation failed'"
            )
            lines.append("        return (False, err)")

        lines.append("    except etree.XMLSyntaxError as exc:")
        lines.append("        return (False, f'Invalid XML: {exc}')")
        lines.append("    except Exception as exc:")
        lines.append("        return (False, f'XSD validation error: {exc}')")

        lines.append(
            f"_xsd_schema_{fn} = StructType(["
            f"StructField('ok', BooleanType(), False), "
            f"StructField('msg', StringType(), True)])"
        )
        lines.append(f"_xsd_udf_{fn} = udf(_xsd_validate_{fn}, _xsd_schema_{fn})")

        if source_is_field:
            lines.append(
                f"_xsd_tmp_{fn} = {in_df}.withColumn("
                f"'_xsd_check', _xsd_udf_{fn}(col({xml_field!r}), col({xsd_field!r})))"
            )
        else:
            lines.append(
                f"_xsd_tmp_{fn} = {in_df}.withColumn("
                f"'_xsd_check', _xsd_udf_{fn}(col({xml_field!r})))"
            )

        if as_string:
            vmsg = if_valid or "Y"
            imsg = if_invalid or "N"
            lines.append(
                f"{out_var} = _xsd_tmp_{fn}.withColumn("
                f"{result_field!r}, when(col('_xsd_check.ok'), lit({vmsg!r}))"
                f".otherwise(lit({imsg!r})))"
            )
        else:
            lines.append(
                f"{out_var} = _xsd_tmp_{fn}.withColumn("
                f"{result_field!r}, col('_xsd_check.ok'))"
            )
        if add_msg:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"{msg_field!r}, col('_xsd_check.msg'))"
            )
        lines.append(f"{out_var} = {out_var}.drop('_xsd_check')")

        true_t, false_t = _resolve_accept_reject(meta, context)
        if true_t or false_t:
            if as_string:
                vmsg = if_valid or "Y"
                ok_pred = f"col({result_field!r}) == lit({vmsg!r})"
            else:
                ok_pred = f"col({result_field!r}) == lit(True)"
            if true_t and false_t:
                tv, fv = _branch_stream_name(true_t), _branch_stream_name(false_t)
                lines.append(f"{tv} = {out_var}.filter({ok_pred})")
                lines.append(f"{fv} = {out_var}.filter(~({ok_pred}))")
                if out_var not in (tv, fv):
                    lines.append(f"{out_var} = {tv}")
            elif false_t:
                lines.append(
                    f"{_branch_stream_name(false_t)} = {out_var}.filter(~({ok_pred}))"
                )
            elif true_t:
                tv = _branch_stream_name(true_t)
                lines.append(f"{tv} = {out_var}.filter({ok_pred})")
                if out_var != tv:
                    lines.append(f"{out_var} = {tv}")

        return lines, status


VALIDATION_HANDLERS: list[BaseStepHandler] = [
    CreditCardValidatorHandler(),
    DataValidatorHandler(),
    MailValidatorHandler(),
    XSDValidatorHandler(),
]
