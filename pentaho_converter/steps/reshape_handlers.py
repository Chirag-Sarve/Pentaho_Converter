"""Handlers for reshape Transform steps (Normaliser, Denormaliser, Flattener, Split*)."""

from __future__ import annotations

import logging
from collections import OrderedDict

from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_denormaliser_config,
    parse_flattener_config,
    parse_normaliser_config,
    parse_split_field_to_rows_config,
    parse_split_fields_config,
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


def _spark_cast(type_name: str) -> str | None:
    t = (type_name or "").strip().lower().replace(" ", "")
    mapping = {
        "string": "string",
        "integer": "bigint",
        "int": "bigint",
        "number": "double",
        "bignumber": "decimal(38,10)",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "binary": "binary",
    }
    return mapping.get(t)


def _agg_expr(agg_type: str, value_expr: str, alias: str) -> str:
    a = (agg_type or "NONE").upper().replace(" ", "_")
    if a in ("SUM",):
        return f"sum({value_expr}).alias({alias!r})"
    if a in ("COUNT", "COUNT_ALL"):
        return f"count({value_expr}).alias({alias!r})"
    if a in ("MIN",):
        return f"min({value_expr}).alias({alias!r})"
    if a in ("MAX",):
        return f"max({value_expr}).alias({alias!r})"
    if a in ("AVERAGE", "AVG"):
        return f"avg({value_expr}).alias({alias!r})"
    if a in ("CONCAT_COMMA", "CONCATENATE"):
        return f"concat_ws(',', collect_list({value_expr})).alias({alias!r})"
    if a in ("LAST",):
        return f"last({value_expr}, ignorenulls=True).alias({alias!r})"
    # NONE / FIRST
    return f"first({value_expr}, ignorenulls=True).alias({alias!r})"


def _escape_split_delim(delimiter: str) -> str:
    return (
        delimiter.replace("\\", "\\\\")
        .replace(".", "\\.")
        .replace("|", "\\|")
        .replace("+", "\\+")
        .replace("*", "\\*")
        .replace("?", "\\?")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("{", "\\{")
        .replace("}", "\\}")
        .replace("^", "\\^")
        .replace("$", "\\$")
    )


def _apply_split_field_typing(
    expr: str,
    field: dict,
    lines: list[str],
) -> str:
    """Apply trim / nullif / ifnull / cast / format for a Split Fields token."""
    name = field.get("name") or "field"
    trimtype = (field.get("trimtype") or "none").lower()
    if trimtype in ("left", "ltrim"):
        expr = f"ltrim({expr})"
    elif trimtype in ("right", "rtrim"):
        expr = f"rtrim({expr})"
    elif trimtype in ("both", "all"):
        expr = f"trim({expr})"

    nullif = field.get("nullif") or ""
    ifnull = field.get("ifnull") or ""
    if nullif:
        expr = f"when({expr} == lit({nullif!r}), lit(None)).otherwise({expr})"
    if ifnull:
        expr = f"coalesce({expr}, lit({ifnull!r}))"

    cast_type = _spark_cast(field.get("type", "String"))
    fmt = field.get("format") or ""
    if cast_type == "date" and fmt:
        expr = f"to_date(({expr}).cast('string'), {fmt!r})"
    elif cast_type == "timestamp" and fmt:
        expr = f"to_timestamp(({expr}).cast('string'), {fmt!r})"
    elif cast_type and cast_type != "string":
        expr = f'({expr}).cast("{cast_type}")'

    lines.append(
        f"# preserved.field {name!r} id={field.get('id')!r} idrem={field.get('idrem')} "
        f"type={field.get('type')!r} format={fmt!r} "
        f"group={field.get('group')!r} decimal={field.get('decimal')!r} "
        f"currency={field.get('currency')!r} "
        f"length={field.get('length')!r} precision={field.get('precision')!r} "
        f"nullif={nullif!r} ifnull={ifnull!r} trimtype={trimtype!r}"
    )
    if field.get("group") or field.get("decimal") or field.get("currency"):
        lines.append(
            f"# WARNING: group/decimal/currency for {name!r} not applied in Spark cast; "
            "use locale-aware parsing if required"
        )
    return expr


class RowNormaliserHandler(BaseStepHandler):
    """Row Normaliser — unpivot type/value groups into long-form rows."""

    _TYPES = {"rownormaliser", "rownormalizer", "normaliser"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("fields") and get_step_element(context.step) is not None:
            metadata = parse_normaliser_config(get_step_element(context.step))

        type_field = metadata.get("type_field") or "typefield"
        fields = metadata.get("fields") or []
        lines = [f"# Row Normaliser: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Row Normaliser")

        if not fields:
            lines.append(
                f"# WARNING: RowNormaliser '{context.step.name}': no fields configured"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        # Group by type value → list of (source_name, norm_name)
        groups: OrderedDict[str, list[tuple[str, str]]] = OrderedDict()
        source_names: set[str] = set()
        for item in fields:
            src = item.get("name") or ""
            value = item.get("value") or ""
            norm = item.get("norm") or src
            if not src:
                continue
            groups.setdefault(value, []).append((src, norm))
            source_names.add(src)

        # Keep columns = all inputs not being normalized
        keep_cols = [c for c in context.input_column_names if c and c not in source_names]
        parts: list[str] = []
        for idx, (value_key, mappings) in enumerate(groups.items()):
            part_var = f"_norm_{out_var}_{idx}"
            if keep_cols or context.input_column_names:
                select_parts: list[str] = [f'col("{c}")' for c in keep_cols]
                select_parts.append(f'lit({value_key!r}).alias("{type_field}")')
                for src, norm in mappings:
                    select_parts.append(f'col("{src}").alias("{norm}")')
                lines.append(
                    f"{part_var} = {in_df}.select({', '.join(select_parts)})"
                )
            else:
                # Upstream schema unknown — retain all columns then drop sources
                lines.append(
                    f"# WARNING: RowNormaliser '{context.step.name}': upstream schema "
                    "unknown; preserving all non-source columns via withColumn/drop"
                )
                lines.append(f"{part_var} = {in_df}")
                lines.append(
                    f'{part_var} = {part_var}.withColumn("{type_field}", lit({value_key!r}))'
                )
                for src, norm in mappings:
                    if src != norm:
                        lines.append(
                            f'{part_var} = {part_var}.withColumn("{norm}", col("{src}"))'
                        )
                drop_list = ", ".join(f'"{s}"' for s in sorted(source_names))
                if drop_list:
                    lines.append(f"{part_var} = {part_var}.drop({drop_list})")
            parts.append(part_var)

        if len(parts) == 1:
            lines.append(f"{out_var} = {parts[0]}")
        else:
            lines.append(f"{out_var} = {parts[0]}")
            for part in parts[1:]:
                lines.append(f"{out_var} = {out_var}.unionByName({part}, allowMissingColumns=True)")

        logger.debug("RowNormaliser %s → %d type values", context.step.name, len(groups))
        return lines, "converted"


class RowDenormaliserHandler(BaseStepHandler):
    """Row Denormaliser — pivot key/value pairs into wide columns."""

    _TYPES = {"rowdenormaliser", "rowdenormalizer", "denormaliser"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("target_fields") and get_step_element(context.step) is not None:
            metadata = parse_denormaliser_config(get_step_element(context.step))

        group_fields = metadata.get("group_fields") or []
        key_field = metadata.get("key_field") or ""
        targets = metadata.get("target_fields") or []
        lines = [f"# Row Denormaliser: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Row Denormaliser")

        if not key_field or not targets:
            lines.append(
                f"# WARNING: RowDenormaliser '{context.step.name}': "
                "key_field/target fields incomplete"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        agg_exprs: list[str] = []
        for target in targets:
            field_name = target.get("field_name") or ""
            key_value = target.get("key_value") or ""
            target_name = target.get("target_name") or field_name
            if not field_name or not target_name:
                continue
            value_expr = (
                f'when(col("{key_field}") == lit({key_value!r}), col("{field_name}"))'
            )
            null_string = target.get("target_null_string") or ""
            if null_string:
                value_expr = (
                    f'when(col("{key_field}") == lit({key_value!r}), '
                    f'when(col("{field_name}").cast("string") == lit({null_string!r}), '
                    f'lit(None)).otherwise(col("{field_name}")))'
                )
            agg = (target.get("target_aggregation_type") or "NONE").upper()
            if agg in ("LAST",):
                pass  # handled via last() in _agg_expr
            agg_exprs.append(_agg_expr(agg, value_expr, target_name))
            lines.append(
                f"# preserved.target {target_name!r} type={target.get('target_type')!r} "
                f"format={target.get('target_format')!r} "
                f"length={target.get('target_length')!r} "
                f"precision={target.get('target_precision')!r} "
                f"decimal={target.get('target_decimal_symbol')!r} "
                f"grouping={target.get('target_grouping_symbol')!r} "
                f"currency={target.get('target_currency_symbol')!r} "
                f"null_string={null_string!r} aggregation={agg}"
            )

        if not agg_exprs:
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        if group_fields:
            group_cols = ", ".join(f'"{g}"' for g in group_fields)
            lines.append(
                f"{out_var} = {in_df}.groupBy({group_cols}).agg({', '.join(agg_exprs)})"
            )
        else:
            lines.append(f"{out_var} = {in_df}.agg({', '.join(agg_exprs)})")

        # Post-cast target types where requested
        for target in targets:
            target_name = target.get("target_name") or ""
            cast_type = _spark_cast(target.get("target_type", ""))
            fmt = target.get("target_format") or ""
            if target_name and cast_type == "date" and fmt:
                lines.append(
                    f'{out_var} = {out_var}.withColumn("{target_name}", '
                    f'to_date(col("{target_name}").cast("string"), {fmt!r}))'
                )
            elif target_name and cast_type == "timestamp" and fmt:
                lines.append(
                    f'{out_var} = {out_var}.withColumn("{target_name}", '
                    f'to_timestamp(col("{target_name}").cast("string"), {fmt!r}))'
                )
            elif target_name and cast_type:
                lines.append(
                    f'{out_var} = {out_var}.withColumn("{target_name}", '
                    f'col("{target_name}").cast("{cast_type}"))'
                )

        return lines, "converted"


class RowFlattenerHandler(BaseStepHandler):
    """Row Flattener — pack every N consecutive values of a field into N columns."""

    _TYPES = {"flattener", "rowflattener"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("field_name") and get_step_element(context.step) is not None:
            metadata = parse_flattener_config(get_step_element(context.step))

        field_name = metadata.get("field_name") or ""
        targets = metadata.get("target_fields") or []
        lines = [f"# Row Flattener: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Row Flattener")

        if not field_name or not targets:
            lines.append(
                f"# WARNING: Flattener '{context.step.name}': field_name/targets missing"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        n = len(targets)
        lines.append(
            f"# Flattener packs consecutive values of {field_name!r} into {n} target columns"
        )
        lines.append(
            f"_flat_{out_var} = {in_df}.withColumn("
            f"'_flat_rn', row_number().over(Window.orderBy(monotonically_increasing_id())))"
        )
        lines.append(
            f"_flat_{out_var} = _flat_{out_var}.withColumn("
            f"'_flat_grp', ((col('_flat_rn') - lit(1)) / lit({n})).cast('int'))"
        )
        lines.append(
            f"_flat_{out_var} = _flat_{out_var}.withColumn("
            f"'_flat_pos', ((col('_flat_rn') - lit(1)) % lit({n})).cast('int'))"
        )

        # Preserve other columns via first() per group (approximation of stream flatten)
        other_cols = [
            c for c in context.input_column_names if c and c != field_name
        ]
        agg_parts: list[str] = []
        for col_name in other_cols:
            agg_parts.append(
                f'first(col("{col_name}"), ignorenulls=True).alias("{col_name}")'
            )
        for idx, target in enumerate(targets):
            agg_parts.append(
                f'first(when(col("_flat_pos") == lit({idx}), col("{field_name}")), '
                f'ignorenulls=True).alias("{target}")'
            )
        lines.append(
            f"{out_var} = _flat_{out_var}.groupBy('_flat_grp').agg({', '.join(agg_parts)})"
        )
        lines.append(f"{out_var} = {out_var}.drop('_flat_grp')")
        lines.append(
            "# WARNING: Flattener relies on row order via monotonically_increasing_id(); "
            "ensure upstream ordering matches Pentaho stream order"
        )
        return lines, "converted"


class SplitFieldToRowsHandler(BaseStepHandler):
    """Split Field to Rows — explode a delimited string into one row per token."""

    _TYPES = {"splitfieldtorows"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("split_field") and get_step_element(context.step) is not None:
            metadata = parse_split_field_to_rows_config(get_step_element(context.step))

        split_field = metadata.get("split_field") or ""
        delimiter = metadata.get("delimiter") or ";"
        new_field = metadata.get("new_field") or split_field or "split"
        include_rn = bool(metadata.get("include_row_number"))
        rn_field = metadata.get("row_number_field") or "rownr"
        reset_rn = bool(metadata.get("reset_row_number", True))
        is_regex = bool(metadata.get("delimiter_is_regex"))

        lines = [f"# Split Field to Rows: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Split Field to Rows")
        if not split_field:
            lines.append(
                f"# WARNING: SplitFieldToRows '{context.step.name}': splitfield missing"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        if is_regex:
            lines.append(
                f"# preserved.delimiter_is_regex=Y delimiter={delimiter!r}"
            )
            lines.append(
                f"_split_{out_var} = split(col(\"{split_field}\").cast('string'), {delimiter!r})"
            )
        else:
            escaped = _escape_split_delim(delimiter)
            lines.append(
                f"_split_{out_var} = split(col(\"{split_field}\").cast('string'), {escaped!r})"
            )

        lines.append(
            f"{out_var} = {in_df}.withColumn({new_field!r}, explode_outer(_split_{out_var}))"
        )
        # Null exploded tokens → empty string (avoids dropping parent row context)
        lines.append(
            f'{out_var} = {out_var}.withColumn({new_field!r}, '
            f'coalesce(col({new_field!r}), lit("")))'
        )
        lines.append(
            f"# preserved.reset_row_number={reset_rn} "
            f"include_row_number={include_rn} rownum_field={rn_field!r}"
        )

        if include_rn:
            other = [c for c in context.input_column_names if c and c != split_field]
            if reset_rn and other:
                partition = ", ".join(f'"{c}"' for c in other)
                lines.append(
                    f"_w_split_{out_var} = Window.partitionBy({partition})"
                    f".orderBy(monotonically_increasing_id())"
                )
            else:
                lines.append(
                    f"_w_split_{out_var} = Window.orderBy(monotonically_increasing_id())"
                )
            lines.append(
                f'{out_var} = {out_var}.withColumn({rn_field!r}, '
                f"row_number().over(_w_split_{out_var}))"
            )

        return lines, "converted"


class SplitFieldsHandler(BaseStepHandler):
    """Split Fields — split one string into multiple typed columns."""

    _TYPES = {"fieldsplitter", "splitfields"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        if not metadata.get("split_field") and get_step_element(context.step) is not None:
            metadata = parse_split_fields_config(get_step_element(context.step))

        split_field = metadata.get("split_field") or ""
        delimiter = metadata.get("delimiter") or ","
        enclosure = metadata.get("enclosure") or ""
        fields = metadata.get("fields") or []

        lines = [f"# Split Fields: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Split Fields")
        if not split_field or not fields:
            lines.append(
                f"# WARNING: SplitFields '{context.step.name}': splitfield/fields missing"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        # Pentaho: ID mode when the first field ID is non-empty
        select_by_id = bool((fields[0].get("id") or "").strip())
        use_from_csv = bool(enclosure) and not select_by_id

        lines.append(f"{out_var} = {in_df}")

        if use_from_csv:
            schema = ", ".join(
                f"`{(f.get('name') or f'field_{i}')}` STRING" for i, f in enumerate(fields)
            )
            lines.append(
                f"# Enclosure-aware split via from_csv "
                f"(delimiter={delimiter!r}, enclosure={enclosure!r})"
            )
            lines.append(
                f'{out_var} = {out_var}.withColumn("_csv_{out_var}", '
                f'from_csv(col("{split_field}").cast("string"), {schema!r}, '
                f'{{"sep": {delimiter!r}, "quote": {enclosure!r}, "mode": "PERMISSIVE"}}))'
            )
            for idx, field in enumerate(fields):
                name = field.get("name") or f"field_{idx}"
                expr = f'col("_csv_{out_var}.{name}")'
                expr = _apply_split_field_typing(expr, field, lines)
                lines.append(f'{out_var} = {out_var}.withColumn("{name}", {expr})')
            lines.append(f'{out_var} = {out_var}.drop("_csv_{out_var}", "{split_field}")')
            return lines, "converted"

        if enclosure and select_by_id:
            lines.append(
                f"# WARNING: enclosure={enclosure!r} with ID-based fields — "
                "Spark split() is not enclosure-aware; results may differ from Pentaho"
            )

        escaped = _escape_split_delim(delimiter)
        lines.append(
            f'{out_var} = {out_var}.withColumn("_parts_{out_var}", '
            f'split(col("{split_field}").cast("string"), {escaped!r}))'
        )

        if select_by_id:
            lines.append("# Split Fields using ID lookup (part.startswith(id))")
            for idx, field in enumerate(fields):
                name = field.get("name") or f"field_{idx}"
                field_id = field.get("id") or ""
                if field_id:
                    # SQL expr avoids Python lambda (and keeps ID match as Pentaho startswith)
                    raw = (
                        f'element_at(filter(`_parts_{out_var}`, '
                        f'x -> startswith(x, {field_id!r})), 1)'
                    )
                    expr = f"expr({raw!r})"
                    if field.get("idrem"):
                        expr = (
                            f"when(({expr}).startswith({field_id!r}), "
                            f"substring({expr}, {len(field_id) + 1}, 2147483647))"
                            f".otherwise({expr})"
                        )
                else:
                    expr = "lit(None)"
                    lines.append(
                        f"# WARNING: empty id for field {name!r} in ID mode → null"
                    )
                expr = _apply_split_field_typing(expr, field, lines)
                lines.append(f'{out_var} = {out_var}.withColumn("{name}", {expr})')
        else:
            for idx, field in enumerate(fields):
                name = field.get("name") or f"field_{idx}"
                expr = f'element_at(col("_parts_{out_var}"), {idx + 1})'
                field_id = field.get("id") or ""
                if field_id and field.get("idrem"):
                    expr = (
                        f"when({expr}.startswith({field_id!r}), "
                        f"substring({expr}, {len(field_id) + 1}, 2147483647))"
                        f".otherwise({expr})"
                    )
                elif field_id:
                    lines.append(f"# preserved.id={field_id!r} for {name} (idrem=N)")
                expr = _apply_split_field_typing(expr, field, lines)
                lines.append(f'{out_var} = {out_var}.withColumn("{name}", {expr})')

        lines.append(
            f'{out_var} = {out_var}.drop("_parts_{out_var}", "{split_field}")'
        )
        return lines, "converted"


RESHAPE_HANDLERS = [
    RowNormaliserHandler(),
    RowDenormaliserHandler(),
    RowFlattenerHandler(),
    SplitFieldToRowsHandler(),
    SplitFieldsHandler(),
]
