"""Handlers for Pentaho transformation steps."""

from __future__ import annotations

from ..calculator_converter import (
    CalculationConvertResult,
    calculations_from_metadata,
    convert_calculation_result,
)
from ..filter_converter import convert_filter_rows_step
from ..group_by_converter import convert_group_by_step
from ..database_lookup_converter import convert_database_lookup_step
from ..merge_join_converter import convert_merge_join_step
from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    _child_text,
    format_spark_join_on,
    get_step_element,
    parse_calculations,
    parse_join_keys,
    parse_sort_fields,
)
from .base import BaseStepHandler, StepContext


class FilterRowsHandler(BaseStepHandler):
    _TYPES = {"filterrows"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        return convert_filter_rows_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


def _select_spark_cast(type_name: str, conversion_mask: str = "") -> str | None:
    """Return a Spark cast type string for Select Values meta changes, or None."""
    t = (type_name or "").strip().lower().replace(" ", "")
    if not t:
        return None
    if t in ("integer", "int"):
        return "bigint"
    if t in ("number", "float", "double"):
        return "double"
    if t in ("bignumber",):
        return "decimal(38,10)"
    if t in ("boolean", "bool"):
        return "boolean"
    if t == "string":
        return "string"
    if t == "date":
        return "date"
    if t == "timestamp":
        return "timestamp"
    if t == "binary":
        return "binary"
    if conversion_mask and ("yy" in conversion_mask.lower() or "dd" in conversion_mask.lower()):
        return "timestamp"
    return "string" if t else None


def _select_meta_expr(src: str, change: dict) -> tuple[str, str | None, list[str]]:
    """Build a Spark column expression for a Select Values meta change.

    Returns ``(expr, cast_type, warning_comments)``.
    """
    warnings: list[str] = []
    cast_type = _select_spark_cast(
        change.get("type_name", ""), change.get("conversion_mask", "")
    )
    mask = change.get("conversion_mask") or ""
    expr = f'col("{src}")'
    if mask and cast_type in ("date", "timestamp"):
        fn = "to_date" if cast_type == "date" else "to_timestamp"
        expr = f'{fn}(col("{src}").cast("string"), {mask!r})'
    elif cast_type:
        expr = f'col("{src}").cast("{cast_type}")'

    length = (change.get("length") or "").strip()
    precision = (change.get("precision") or "").strip()
    # String length truncates; numeric length/precision is metadata-only in Spark.
    if length and length not in ("-1", "-2") and (cast_type == "string" or not cast_type):
        try:
            n = int(length)
            if n > 0 and cast_type == "string":
                base = expr if ".cast(" in expr else f'{expr}.cast("string")'
                expr = f"substring({base}, 1, {n})"
            elif n > 0 and not cast_type:
                warnings.append(
                    f"# preserved.meta length={length!r} precision={precision!r} for {src}"
                )
        except ValueError:
            warnings.append(
                f"# preserved.meta length={length!r} precision={precision!r} for {src}"
            )
    elif (length and length not in ("-1", "-2")) or (precision and precision not in ("-1", "-2")):
        warnings.append(
            f"# preserved.meta length={length!r} precision={precision!r} for {src}"
        )

    for label, key in (
        ("conversion_mask", "conversion_mask"),
        ("decimal_symbol", "decimal_symbol"),
        ("grouping_symbol", "grouping_symbol"),
        ("currency_symbol", "currency_symbol"),
        ("encoding", "encoding"),
        ("storage_type", "storage_type"),
        ("date_format_locale", "date_format_locale"),
        ("date_format_timezone", "date_format_timezone"),
    ):
        val = change.get(key) or ""
        if not val:
            continue
        # Mask already applied for date/timestamp casts.
        if key == "conversion_mask" and cast_type in ("date", "timestamp"):
            continue
        warnings.append(f"# preserved.meta {label}={val!r} for {src}")
    if change.get("date_format_lenient"):
        warnings.append(f"# preserved.meta date_format_lenient=Y for {src}")
    if change.get("lenient_string_to_number"):
        warnings.append(f"# preserved.meta lenient_string_to_number=Y for {src}")
    return expr, cast_type, warnings


class SelectValuesHandler(BaseStepHandler):
    """Select Values: select/rename columns, remove columns, and metadata casts."""

    _TYPES = {"selectvalues"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def _parse_config(self, context: StepContext) -> dict:
        metadata = get_converter_metadata(context)
        # Re-parse when structured select config is absent or empty but XML remains.
        has_select = bool(metadata.get("select_fields") or metadata.get("select_columns"))
        has_meta = bool(metadata.get("meta_changes"))
        has_remove = bool(metadata.get("remove_names"))
        if has_select or has_meta or has_remove:
            return metadata
        step_el = get_step_element(context.step)
        if step_el is None:
            return metadata
        from ..step_xml import parse_select_values_config

        return parse_select_values_config(step_el)

    def _resolve_select_fields(
        self, context: StepContext, config: dict
    ) -> list[tuple[str, str]]:
        """Return (source_name, output_name) pairs for the select projection."""
        fields: list[tuple[str, str]] = []
        seen: set[str] = set()

        def _add(name: str, rename: str = "") -> None:
            name = (name or "").strip()
            if not name:
                return
            rename = (rename or "").strip()
            if name in seen:
                if rename:
                    for idx, (existing_name, _) in enumerate(fields):
                        if existing_name == name:
                            fields[idx] = (name, rename)
                            break
                return
            seen.add(name)
            fields.append((name, rename))

        for item in config.get("select_fields") or config.get("fields") or []:
            if isinstance(item, dict) and item.get("name"):
                _add(item["name"], item.get("rename", ""))

        if not fields:
            for f in self._fields(context):
                if f.name:
                    _add(f.name, f.rename)

        if not fields:
            for col_name in config.get("select_columns") or []:
                _add(col_name)

        # Apply meta renames onto the select list (does not invent a select by itself)
        for change in config.get("meta_changes") or []:
            src = (change.get("name") or "").strip()
            rename = (change.get("rename") or "").strip()
            if src and rename and src in seen:
                _add(src, rename)

        remove_names = {
            (n or "").strip()
            for n in (config.get("remove_names") or [])
            if n
        }
        # Also collect remove tags from XML if config lacked them
        step_el = get_step_element(context.step)
        if step_el is not None:
            from ..step_xml import _iter_step_or_fields

            for rem_el in _iter_step_or_fields(step_el, "remove"):
                name = _child_text(rem_el, "name")
                if name:
                    remove_names.add(name)

        # Remove-only / meta-only: start from all input columns
        if remove_names and not fields:
            for col_name in context.input_column_names:
                _add(col_name)

        if remove_names:
            fields = [(n, r) for n, r in fields if n not in remove_names]

        select_unspecified = bool(config.get("select_unspecified"))
        if select_unspecified and fields:
            selected = {n for n, _ in fields}
            extras = sorted(
                c for c in context.input_column_names if c and c not in selected and c not in remove_names
            )
            for col_name in extras:
                _add(col_name)

        return fields

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        config = self._parse_config(context)
        select_fields = self._resolve_select_fields(context, config)
        meta_changes = config.get("meta_changes") or []
        status = "converted"

        lines = [f"# Select Values: {step.name}"]
        if not in_df:
            lines.append(
                f"# WARNING: Select Values '{step.name}': no upstream DataFrame; "
                "emitting empty placeholder"
            )
            lines.append(f"{out_var} = spark.range(0).limit(0)")
            return lines, "partial"

        # Meta-only (no select / remove): keep all columns, apply casts/renames
        remove_names = {
            (n or "").strip() for n in (config.get("remove_names") or []) if n
        }
        if not select_fields and meta_changes and not remove_names:
            lines.append(f"{out_var} = {in_df}")
            for change in meta_changes:
                src = change.get("name") or ""
                if not src:
                    continue
                dst = change.get("rename") or src
                expr, cast_type, meta_notes = _select_meta_expr(src, change)
                if dst != src:
                    lines.append(f'{out_var} = {out_var}.withColumn("{dst}", {expr}).drop("{src}")')
                else:
                    lines.append(f'{out_var} = {out_var}.withColumn("{dst}", {expr})')
                lines.extend(meta_notes)
            return lines, status

        # Remove-only (no select list / no upstream column inventory): emit drop()
        if not select_fields and remove_names:
            drop_list = ", ".join(f'"{n}"' for n in sorted(remove_names))
            lines.append(f"{out_var} = {in_df}.drop({drop_list})")
            for change in meta_changes:
                src = change.get("name") or ""
                if not src or src in remove_names:
                    continue
                dst = change.get("rename") or src
                expr, cast_type, meta_notes = _select_meta_expr(src, change)
                if dst != src or cast_type:
                    if dst != src:
                        lines.append(
                            f'{out_var} = {out_var}.withColumn("{dst}", {expr}).drop("{src}")'
                        )
                    else:
                        lines.append(f'{out_var} = {out_var}.withColumn("{dst}", {expr})')
                lines.extend(meta_notes)
            return lines, status

        if not select_fields:
            # Truly empty Select Values — pass through upstream (do not invent an empty DF).
            if meta_changes or remove_names:
                lines.append(
                    f"# WARNING: Select Values '{step.name}': configuration could not be "
                    "fully resolved; preserving upstream DataFrame"
                )
                status = "partial"
            lines.append(f"{out_var} = {in_df}")
            return lines, status

        # Build cast lookup from meta changes keyed by source / rename
        meta_by_src = {
            (c.get("name") or ""): c for c in meta_changes if c.get("name")
        }
        # Select-tab length/precision from structured select_fields
        select_meta_by_name: dict[str, dict] = {}
        for item in config.get("select_fields") or []:
            if isinstance(item, dict) and item.get("name"):
                select_meta_by_name[item["name"]] = item

        col_exprs: list[str] = []
        for name, rename in select_fields:
            out_name = rename or name
            change = dict(meta_by_src.get(name) or {})
            # Meta tab can rename using the Select-tab rename as the source name
            if rename and rename in meta_by_src and name not in meta_by_src:
                change = dict(meta_by_src[rename])
            sel_opts = select_meta_by_name.get(name) or {}
            if not change.get("length") and sel_opts.get("length"):
                change["length"] = sel_opts["length"]
            if not change.get("precision") and sel_opts.get("precision"):
                change["precision"] = sel_opts["precision"]
            # Unofficial type on select-tab field (only when explicitly present)
            if not change.get("type_name"):
                for f in config.get("fields") or []:
                    if (
                        isinstance(f, dict)
                        and f.get("name") == name
                        and (f.get("type") or "").strip()
                    ):
                        change["type_name"] = f["type"]
                        break
            if change.get("rename"):
                out_name = change["rename"]
            expr, cast_type, meta_notes = _select_meta_expr(name, change)
            if out_name != name or cast_type or change.get("length") or change.get("precision"):
                col_exprs.append(f'{expr}.alias("{out_name}")')
            else:
                col_exprs.append(expr)
            change["_notes"] = meta_notes
            meta_by_src[name] = change

        lines.append(f"{out_var} = {in_df}.select({', '.join(col_exprs)})")
        for name, _rename in select_fields:
            for note in (meta_by_src.get(name) or {}).get("_notes") or []:
                lines.append(note)
        if config.get("select_unspecified"):
            if context.input_column_names:
                lines.append(
                    "# preserved.select_unspecified=Y — unspecified columns appended"
                )
            else:
                lines.append(
                    "# WARNING: select_unspecified=Y but upstream column inventory "
                    "was empty; only explicitly selected columns were projected"
                )
                status = "partial"
        return lines, status


class SortRowsHandler(BaseStepHandler):
    _TYPES = {"sortrows"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..step_xml import parse_sort_rows_config

        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        step_el = get_step_element(step)
        if not metadata.get("sort_fields") and step_el is not None:
            metadata = parse_sort_rows_config(step_el)

        sort_fields = metadata.get("sort_fields") or []
        if not sort_fields:
            # Fallback to legacy (name, ascending) tuples
            legacy = parse_sort_fields(step_el) if step_el is not None else []
            sort_fields = [
                {"name": name, "ascending": asc, "case_sensitive": True}
                for name, asc in legacy
            ]

        lines = [f"# Sort Rows: {step.name}"]
        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        # Spill / memory options have no Spark equivalent — preserve for operators
        lines.append(
            f"# preserved.directory={metadata.get('directory')!r} "
            f"prefix={metadata.get('prefix')!r} "
            f"sort_size={metadata.get('sort_size')!r} "
            f"free_memory={metadata.get('free_memory')!r} "
            f"compress={metadata.get('compress')} "
            f"compress_variable={metadata.get('compress_variable')!r}"
        )

        sort_key_names: list[str] = []
        if sort_fields:
            work = in_df
            sort_cols: list[str] = []
            ci_tmps: list[str] = []
            for item in sort_fields:
                name = item.get("name") or ""
                if not name:
                    continue
                sort_key_names.append(name)
                ascending = item.get("ascending", True)
                case_sensitive = item.get("case_sensitive", True)
                expr_col = f'col("{name}")'
                if not case_sensitive:
                    tmp = f"_sort_ci_{name}"
                    if work == in_df:
                        lines.append(f"_sort_{out_var} = {in_df}")
                        work = f"_sort_{out_var}"
                    lines.append(
                        f'{work} = {work}.withColumn("{tmp}", '
                        f'lower(col("{name}").cast("string")))'
                    )
                    expr_col = f'col("{tmp}")'
                    ci_tmps.append(tmp)
                if item.get("collator_enabled"):
                    lines.append(
                        f"# WARNING: collator_enabled for {name!r} not supported; "
                        f"strength={item.get('collator_strength')!r}"
                    )
                if item.get("presorted"):
                    lines.append(f"# preserved.presorted=Y for {name}")
                # Nulls last matches typical Pentaho numeric/string sort behaviour
                direction = "asc_nulls_last()" if ascending else "desc_nulls_last()"
                sort_cols.append(f"{expr_col}.{direction}")
            lines.append(f"{out_var} = {work}.orderBy({', '.join(sort_cols)})")
            if ci_tmps:
                drop_list = ", ".join(f'"{t}"' for t in ci_tmps)
                lines.append(f"{out_var} = {out_var}.drop({drop_list})")
        else:
            fields = self._fields(context)
            if fields:
                sort_cols = []
                for f in fields:
                    sort_key_names.append(f.name)
                    direction = (
                        "desc_nulls_last()"
                        if self._attr(context, "ascending", "Y").upper() == "N"
                        else "asc_nulls_last()"
                    )
                    sort_cols.append(f'col("{f.name}").{direction}')
                lines.append(f"{out_var} = {in_df}.orderBy({', '.join(sort_cols)})")
            else:
                lines.append(
                    f"{out_var} = {in_df}.orderBy(monotonically_increasing_id())"
                )

        if metadata.get("unique_rows"):
            # Pentaho "Only pass unique rows" verifies sort keys only
            if sort_key_names:
                subset = "[" + ", ".join(f'"{n}"' for n in sort_key_names) + "]"
                lines.append(f"{out_var} = {out_var}.dropDuplicates({subset})")
            else:
                lines.append(f"{out_var} = {out_var}.dropDuplicates()")
            lines.append(
                "# preserved.unique_rows=Y — dropDuplicates on sort keys only"
            )

        return lines, "converted"


class GroupByHandler(BaseStepHandler):
    _TYPES = {"groupby"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        fallback_keys = [f.name for f in self._fields(context) if f.name]
        lines = convert_group_by_step(
            metadata, in_df, out_var, step.name, fallback_group_keys=fallback_keys
        )
        return lines, "converted"


class CalculatorHandler(BaseStepHandler):
    _TYPES = {"calculator"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)

        lines = [f"# Calculator: {step.name}"]
        if not in_df:
            lines.append(
                f"# WARNING: Calculator '{step.name}': no upstream DataFrame; "
                "cannot evaluate calculations"
            )
            lines.append(f"{out_var} = spark.range(0).limit(0)")
            return lines, "partial"

        calculations = calculations_from_metadata(metadata)
        if not calculations:
            step_el = get_step_element(step)
            if step_el is not None:
                calculations = parse_calculations(step_el)
                # Ensure parsed calculations also land in converter metadata for validators.
                if calculations and not metadata.get("calculations"):
                    metadata["calculations"] = [
                        {
                            "field_name": c.field_name,
                            "calc_type": c.calc_type,
                            "field_a": c.field_a,
                            "field_b": c.field_b,
                            "field_c": c.field_c,
                            "value_type": c.value_type,
                            "remove": c.remove,
                        }
                        for c in calculations
                    ]
                    context.extra["converter_metadata"] = metadata
        if not calculations:
            calc_type = self._attr(context, "calc_type", "")
            field_a = self._attr(context, "field_a", "")
            if calc_type and field_a:
                from ..step_xml import CalculationSpec

                lines.append(
                    f"# WARNING: Calculator '{step.name}': using legacy flat attributes; "
                    "parsed calculations metadata is missing"
                )
                calculations = [
                    CalculationSpec(
                        field_name=self._attr(context, "field_name", "calc_result"),
                        calc_type=calc_type,
                        field_a=field_a,
                        field_b=self._attr(context, "field_b"),
                        value_type=self._attr(context, "value_type"),
                    )
                ]

        if not calculations:
            # Preserve upstream DataFrame — never emit an empty unresolved calculator DF.
            lines.append(
                f"# WARNING: Calculator '{step.name}': no calculation metadata found; "
                "preserving upstream DataFrame"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        # Upstream column types (BigNumber/Decimal) drive decimal-preserving casts.
        operand_types: dict[str, str] = {}
        for field in getattr(step, "fields", None) or []:
            name = getattr(field, "name", "") or ""
            type_name = getattr(field, "type_name", "") or ""
            if name and type_name:
                operand_types[name] = type_name
        lineage = context.extra.get("lineage") or context.extra.get("column_lineage")
        input_cols = getattr(lineage, "input_columns", None) if lineage is not None else None
        if isinstance(input_cols, dict):
            for name, schema in input_cols.items():
                type_name = getattr(schema, "type_name", None) or (
                    schema.get("type_name") if isinstance(schema, dict) else ""
                )
                if name and type_name:
                    operand_types[str(name)] = str(type_name)
        meta_types = metadata.get("field_types") or metadata.get("input_field_types") or {}
        if isinstance(meta_types, dict):
            for name, type_name in meta_types.items():
                if name and type_name:
                    operand_types[str(name)] = str(type_name)

        lines.append(f"{out_var} = {in_df}")
        status = "converted"
        for calc in calculations:
            result: CalculationConvertResult = convert_calculation_result(calc, operand_types)
            if result.warning:
                # Locale/format masks are informational — do not emit score-reducing WARNING.
                if result.warning.startswith("preserved.conversion_mask"):
                    lines.append(f"# INFO: {result.warning}")
                else:
                    lines.append(f"# WARNING: {result.warning}")
            if not result.supported:
                status = "partial"
            lines.append(
                f'{out_var} = {out_var}.withColumn("{calc.field_name}", {result.expr})'
            )
            if calc.remove:
                drop_cols = [c for c in (calc.field_a, calc.field_b, calc.field_c) if c]
                if drop_cols:
                    quoted = ", ".join(f'"{c}"' for c in drop_cols)
                    lines.append(f"{out_var} = {out_var}.drop({quoted})")

        return lines, status


class FormulaHandler(BaseStepHandler):
    """Formula → PySpark (delegates to scripting_converter.convert_formula_step).

    Not registered in ``registry_list`` — the canonical registration is
    ``SCRIPTING_HANDLERS.ScriptingFormulaHandler``. Kept for direct/backward-compatible use.
    """

    _TYPES = {"formula"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..scripting_converter import convert_formula_step
        from ..step_xml import get_step_element, parse_formula_config

        metadata = dict(get_converter_metadata(context))
        step_el = get_step_element(context.step)
        if step_el is not None:
            for key, val in parse_formula_config(step_el).items():
                if key not in metadata or metadata[key] in (None, "", [], {}):
                    metadata[key] = val
        return convert_formula_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
        )


class ReplaceNullHandler(BaseStepHandler):
    _TYPES = {"replacenull"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        fields = self._fields(context)
        replace_value = self._attr(context, "replace_value", "''")

        lines = [f"# Replace Null Values: {step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}")
            for f in fields:
                if f.name:
                    lines.append(
                        f'{out_var} = {out_var}.withColumn("{f.name}", '
                        f'when(col("{f.name}").isNull(), {replace_value}).otherwise(col("{f.name}")))'
                    )
            return lines, "converted" if fields else "converted"
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"


class MergeJoinHandler(BaseStepHandler):
    _TYPES = {"mergejoin"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        return convert_merge_join_step(
            metadata,
            inputs,
            out_var,
            context.step.name,
            context=context,
        )


class StreamLookupHandler(BaseStepHandler):
    _TYPES = {"streamlookup"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..metadata_propagation import get_converter_metadata
        from ..step_xml import parse_database_lookup_config

        step = context.step
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        step_el = get_step_element(step)
        keys = parse_join_keys(step_el) if step_el is not None else []
        meta = dict(get_converter_metadata(context))
        if step_el is not None:
            for k, v in parse_database_lookup_config(step_el).items():
                meta.setdefault(k, v)
        if not keys and meta.get("keys"):
            from ..step_xml import JoinKeyPair
            keys = [
                JoinKeyPair(
                    left=k.get("stream_field") or k.get("left") or "",
                    right=k.get("table_field") or k.get("right") or "",
                )
                for k in meta["keys"]
                if isinstance(k, dict) and (k.get("stream_field") or k.get("left"))
            ]
        return_fields = meta.get("return_fields") or []
        cached = bool(meta.get("cached"))
        try:
            cache_size = int(meta.get("cache_size") or 0)
        except (TypeError, ValueError):
            cache_size = 0

        lines = [f"# Stream Lookup: {step.name}"]
        for key, val in (
            ("cached", cached),
            ("cache_size", cache_size),
            ("connection", meta.get("connection")),
            ("return_fields", return_fields),
            ("keys", meta.get("keys")),
        ):
            if val not in (None, "", [], {}, 0, False):
                lines.append(f"# preserved.{key}={val!r}")
        if cached or cache_size > 0:
            lines.append(f"# preserved.cache={{'cached': {cached!r}, 'cache_size': {cache_size}}}")
        if return_fields and "return_fields" not in "\n".join(lines):
            lines.append(f"# preserved.return_fields={return_fields!r}")

        if len(inputs) >= 2:
            main_df, lookup_df = inputs[0], inputs[1]
            if keys:
                # Null lookup keys: filter nulls from lookup side to avoid spurious matches
                key_right = [k.right for k in keys if getattr(k, "right", None)]
                if key_right:
                    null_preds = " & ".join(
                        f"col({r!r}).isNotNull()" for r in key_right
                    )
                    lines.append(
                        f"_lkp_src_{out_var} = {lookup_df}.filter({null_preds})"
                    )
                else:
                    lines.append(f"_lkp_src_{out_var} = {lookup_df}")
                # Cache / broadcast
                if cached or cache_size > 0:
                    lines.append(f"_lkp_{out_var} = broadcast(_lkp_src_{out_var})")
                else:
                    lines.append(f"_lkp_{out_var} = broadcast(_lkp_src_{out_var})")
                    lines.append(
                        "# NOTE: Stream Lookup always broadcasts lookup side "
                        "(Pentaho in-memory cache equivalent)"
                    )
                on_arg, use_on = format_spark_join_on(main_df, f"_lkp_{out_var}", keys)
                if use_on:
                    lines.append(
                        f"{out_var} = {main_df}.join(_lkp_{out_var}, on={on_arg}, how='left')"
                    )
                else:
                    lines.append(
                        f"{out_var} = {main_df}.join(_lkp_{out_var}, {on_arg}, 'left')"
                    )
                # Select return fields (rename + defaults)
                for rf in return_fields:
                    if not isinstance(rf, dict):
                        continue
                    name = rf.get("name") or ""
                    rename = rf.get("rename") or name
                    default = rf.get("default")
                    if not name:
                        continue
                    if rename and rename != name:
                        if default not in (None, ""):
                            lines.append(
                                f"{out_var} = {out_var}.withColumn("
                                f"{rename!r}, coalesce(col({name!r}), lit({default!r}))).drop({name!r})"
                            )
                        else:
                            lines.append(
                                f"{out_var} = {out_var}.withColumnRenamed({name!r}, {rename!r})"
                            )
                    elif default not in (None, ""):
                        lines.append(
                            f"{out_var} = {out_var}.withColumn("
                            f"{name!r}, coalesce(col({name!r}), lit({default!r})))"
                        )
                lines.append(
                    "# NOTE: duplicate matches keep all joined rows (left join); "
                    "dedupe lookup keys upstream if Pentaho ate extras"
                )
            else:
                lines.append(
                    f"# StreamLookup '{step.name}': no join keys — lookup join not generated"
                )
                lines.append(f"{out_var} = {main_df}")
            return lines, "converted"
        elif len(inputs) == 1:
            lines.append(f"{out_var} = {inputs[0]}")
            return lines, "converted"
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"


class DatabaseLookupHandler(BaseStepHandler):
    _TYPES = {"databaselookup", "dblookup"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = get_converter_metadata(context)
        return convert_database_lookup_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )
