"""Handlers for Checksum, Number Range, and Fields Change Sequence transforms."""

from __future__ import annotations

import logging

from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_checksum_config,
    parse_fields_change_sequence_config,
    parse_number_range_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _passthrough(context: StepContext, label: str) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [f"# {label}: {context.step.name}"]
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
    return lines, "converted"


class ChecksumHandler(BaseStepHandler):
    """Add a Checksum — CRC32 / MD5 / SHA-1 / SHA-256 over selected fields."""

    _TYPES = {"checksum", "addachecksum"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("fields") and get_step_element(context.step) is not None:
            metadata = parse_checksum_config(get_step_element(context.step))

        algo = (metadata.get("checksum_type") or "CRC32").upper().replace("_", "-")
        result_field = metadata.get("result_field") or "checksum"
        result_type = (metadata.get("result_type") or "string").lower()
        fields = metadata.get("fields") or []
        lines = [f"# Add a Checksum: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Add a Checksum")
        if not fields:
            lines.append(
                f"# WARNING: Checksum '{context.step.name}': no input fields configured"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        if metadata.get("compatibility_mode"):
            lines.append("# preserved.compatibilityMode=Y — Spark checksum may differ byte-for-byte")
        if metadata.get("old_checksum_behaviour"):
            lines.append(
                "# preserved.oldChecksumBehaviourMode=Y — "
                "legacy byte packing not reproduced exactly in Spark"
            )

        # Pentaho concatenates field string values with NO separator
        concat_parts = ", ".join(
            f'coalesce(col("{f}").cast("string"), lit(""))' for f in fields
        )
        payload = (
            f"concat({concat_parts})" if len(fields) > 1 else concat_parts
        )
        lines.append(f"{out_var} = {in_df}")

        is_digest_hex = False
        if algo in ("CRC32",):
            digest = f"crc32({payload})"
        elif algo in ("MD5",):
            digest = f"md5({payload})"
            is_digest_hex = True
        elif algo in ("SHA-1", "SHA1"):
            digest = f"sha1({payload})"
            is_digest_hex = True
        elif algo in ("SHA-256", "SHA256"):
            digest = f"sha2({payload}, 256)"
            is_digest_hex = True
        elif algo in ("ADLER32",):
            lines.append(
                "# WARNING: ADLER32 not available in Spark SQL — approximating with crc32()"
            )
            digest = f"crc32({payload})"
        else:
            lines.append(f"# WARNING: unsupported checksum algorithm {algo!r}; using md5()")
            digest = f"md5({payload})"
            is_digest_hex = True

        # Pentaho resultType: hex = hex encoding; string = interpretive string of digest;
        # binary = raw bytes. Spark md5/sha already return hex strings.
        if result_type in ("hex", "hexadecimal"):
            if not is_digest_hex:
                digest = f"lower(conv({digest}.cast('bigint'), 10, 16))"
            # else already hex
        elif result_type in ("binary",):
            if is_digest_hex:
                digest = f"unhex({digest})"
            else:
                lines.append(
                    f"# WARNING: binary result_type for {algo} — casting numeric digest to binary via hex"
                )
                digest = f"unhex(lower(conv({digest}.cast('bigint'), 10, 16)))"
        else:
            # string
            if is_digest_hex:
                lines.append(
                    "# preserved.resultType=string — Spark MD5/SHA return hex; "
                    "hex string kept (closest Databricks equivalent)"
                )
            digest = f"({digest}).cast('string')"

        # Null handling: coalesce empty strings so null fields don't drop the row hash
        lines.append(
            f'{out_var} = {out_var}.withColumn("{result_field}", {digest})'
        )
        lines.append(
            f"# preserved.checksumtype={algo!r} resultType={result_type!r} fields={fields!r}"
        )
        logger.debug("Checksum %s algo=%s fields=%s", context.step.name, algo, fields)
        return lines, "converted"


class NumberRangeHandler(BaseStepHandler):
    """Number Range — map numeric values into labeled bands (lower <= x < upper)."""

    _TYPES = {"numberrange"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("rules") and get_step_element(context.step) is not None:
            metadata = parse_number_range_config(get_step_element(context.step))

        input_field = metadata.get("input_field") or ""
        output_field = metadata.get("output_field") or "range"
        fallback = metadata.get("fallback_value")
        rules = metadata.get("rules") or []
        lines = [f"# Number Range: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Number Range")
        if not input_field or not rules:
            lines.append(
                f"# WARNING: NumberRange '{context.step.name}': inputField/rules incomplete"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        lines.append(
            "# Number Range semantics: lower_bound <= value < upper_bound "
            "(Pentaho NumberRangeRule)"
        )
        # Build nested when chain; first matching rule wins (Pentaho order)
        expr = f"lit({fallback!r})" if fallback is not None and fallback != "" else "lit(None)"
        # Apply in reverse so first rule is outermost otherwise?
        # when(c1, v1).when(c2, v2).otherwise(default) — first match wins
        parts: list[str] = []
        for rule in rules:
            lower = rule.get("lower_bound")
            upper = rule.get("upper_bound")
            value = rule.get("value")
            if value is None:
                continue
            conds: list[str] = []
            if lower not in (None, ""):
                try:
                    conds.append(
                        f'col("{input_field}").cast("double") >= lit({float(lower)})'
                    )
                except ValueError:
                    lines.append(f"# WARNING: invalid lower_bound={lower!r} skipped")
                    continue
            if upper not in (None, ""):
                try:
                    conds.append(
                        f'col("{input_field}").cast("double") < lit({float(upper)})'
                    )
                except ValueError:
                    lines.append(f"# WARNING: invalid upper_bound={upper!r} skipped")
                    continue
            # Empty lower AND upper → match all non-null values (open range)
            if not conds:
                cond = f'col("{input_field}").isNotNull()'
            else:
                cond = " & ".join(f"({c})" for c in conds)
            parts.append((cond, value))

        if not parts:
            lines.append(f"{out_var} = {in_df}.withColumn({output_field!r}, {expr})")
            return lines, "partial"

        chain = f"when({parts[0][0]}, lit({parts[0][1]!r}))"
        for cond, value in parts[1:]:
            chain += f".when({cond}, lit({value!r}))"
        chain += f".otherwise({expr})"

        # Null input → fallback
        final = (
            f'when(col("{input_field}").isNull(), {expr}).otherwise({chain})'
        )
        lines.append(f"{out_var} = {in_df}.withColumn({output_field!r}, {final})")
        lines.append(
            f"# preserved.fallback={fallback!r} rules={len(parts)} "
            f"lower_inclusive=True upper_inclusive=False"
        )
        return lines, "converted"


class FieldsChangeSequenceHandler(BaseStepHandler):
    """Add value fields changing sequence — reset counter when group fields change."""

    _TYPES = {
        "fieldschangesequence",
        "addvaluefieldschangingsequence",
    }

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("result_field") and get_step_element(context.step) is not None:
            metadata = parse_fields_change_sequence_config(get_step_element(context.step))

        result_field = metadata.get("result_field") or "change_seq"
        start_at = int(metadata.get("start_at") or 1)
        increment = int(metadata.get("increment_by") or 1)
        group_fields = list(metadata.get("group_fields") or [])
        lines = [f"# Fields Change Sequence: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Fields Change Sequence")

        # Pentaho: empty field list → watch ALL input columns for change
        if not group_fields:
            group_fields = [
                c for c in context.input_column_names if c and not c.startswith("_")
            ]
            lines.append(
                "# No group fields configured — using all upstream columns "
                "(Pentaho FieldsChangeSequence semantics)"
            )
            if not group_fields:
                lines.append(
                    "# WARNING: upstream schema unknown — cannot detect field changes; "
                    "emitting global sequence"
                )

        lines.append(
            "# Sequence resets when any configured field changes vs previous row "
            "(requires deterministic order)"
        )
        lines.append(
            f"_w_chg_{out_var} = Window.orderBy(monotonically_increasing_id())"
        )
        lines.append(f"_chg_{out_var} = {in_df}")

        if group_fields:
            change_parts: list[str] = []
            for name in group_fields:
                change_parts.append(
                    f'(~col("{name}").eqNullSafe(lag(col("{name}"), 1).over(_w_chg_{out_var})))'
                )
            change_expr = " | ".join(change_parts)
            lines.append(
                f"_chg_{out_var} = _chg_{out_var}.withColumn("
                f"'_chg_flag', "
                f"when(row_number().over(_w_chg_{out_var}) == 1, lit(1))"
                f".when({change_expr}, lit(1)).otherwise(lit(0))"
                f")"
            )
        else:
            # Schema unknown and no fields → treat every row as start of new group is wrong;
            # emit never-reset after first (global). Flag already documented above.
            lines.append(
                f"_chg_{out_var} = _chg_{out_var}.withColumn('_chg_flag', lit(0))"
            )
            lines.append(
                f"_chg_{out_var} = _chg_{out_var}.withColumn("
                f"'_chg_flag', "
                f"when(row_number().over(_w_chg_{out_var}) == 1, lit(1))"
                f".otherwise(col('_chg_flag')))"
            )

        lines.append(
            f"_chg_{out_var} = _chg_{out_var}.withColumn("
            f"'_chg_grp', _sum(col('_chg_flag')).over(_w_chg_{out_var}))"
        )
        lines.append(
            f"_w_seq_{out_var} = Window.partitionBy('_chg_grp')"
            f".orderBy(monotonically_increasing_id())"
        )
        lines.append(
            f"{out_var} = _chg_{out_var}.withColumn("
            f'"{result_field}", '
            f"lit({start_at}) + (row_number().over(_w_seq_{out_var}) - lit(1)) * lit({increment})"
            f")"
        )
        lines.append(f"{out_var} = {out_var}.drop('_chg_flag', '_chg_grp')")
        lines.append(
            f"# preserved.start={start_at} increment={increment} group_fields={group_fields!r}"
        )
        lines.append(
            "# WARNING: distributed execution uses monotonically_increasing_id for order; "
            "sort upstream for stable reset semantics"
        )
        return lines, "converted"


CALC_EXTENDED_HANDLERS = [
    ChecksumHandler(),
    NumberRangeHandler(),
    FieldsChangeSequenceHandler(),
]
