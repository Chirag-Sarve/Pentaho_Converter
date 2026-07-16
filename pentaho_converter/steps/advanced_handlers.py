"""Handlers for additional Pentaho step types (production coverage)."""

from __future__ import annotations

from ..metadata_propagation import get_converter_metadata
from ..value_mapper_converter import convert_value_mapper_step, mappings_from_step_element
from ..step_xml import (
    RankConfig,
    get_step_element,
    parse_rank_config,
    parse_sequence_config,
    parse_sort_fields,
    parse_system_info_fields,
    system_info_expr,
    SequenceConfig,
)
from .base import BaseStepHandler, StepContext


def _passthrough(context: StepContext, comment: str) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [f"# {comment}: {context.step.name}"]
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_init STRING').limit(0)")
    return lines, "converted"


def _build_join_on(keys: list, left_alias: str = "", right_alias: str = "") -> str:
    if not keys:
        return "lit(True)"
    parts = []
    for k in keys:
        left = k.left
        right = k.right
        if left_alias:
            left = f"{left_alias}.{left}" if "." not in left else left
        if right_alias:
            right = f"{right_alias}.{right}" if "." not in right else right
        parts.append(f'col("{k.left}") == col("{k.right}")')
    if len(parts) == 1:
        return parts[0]
    return "(" + " & ".join(parts) + ")"


class UniqueHandler(BaseStepHandler):
    _TYPES = {
        "unique", "uniquerows", "uniquerowsbyhashset",
        "uniquerowshashset", "uniquehashset",
    }

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in {
            t.replace(" ", "") for t in self._TYPES
        }

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        st = step.step_type.strip().lower().replace(" ", "")
        is_hashset = st in {
            "uniquerowsbyhashset", "uniquerowshashset", "uniquehashset",
        }
        label = "Unique Rows (HashSet)" if is_hashset else "Unique Rows"
        metadata = get_converter_metadata(context)
        lines = [f"# {label}: {step.name}"]
        if not in_df:
            return _passthrough(context, label)

        compare = metadata.get("compare_fields") or []
        if not compare:
            compare = [
                {"name": f.name, "case_insensitive": False}
                for f in self._fields(context)
                if f.name
            ]
        compare_names = [c["name"] for c in compare if c.get("name")]
        count_rows = bool(metadata.get("count_rows")) or (
            self._attr(context, "count_rows", "N").upper() == "Y"
        )
        count_field = (
            str(metadata.get("count_field") or "").strip()
            or self._attr(context, "count_field", "count")
            or "count"
        )
        if metadata.get("reject_duplicate_row"):
            lines.append(
                f"# WARNING: reject_duplicate_row not routed to error hops; "
                f"description={metadata.get('error_description')!r}"
            )
        else:
            lines.append(
                f"# preserved.reject_duplicate_row=N "
                f"error_description={metadata.get('error_description')!r}"
            )
        if is_hashset:
            lines.append(f"# preserved.store_values={metadata.get('store_values', True)}")
        else:
            lines.append(
                "# Unique Rows expects sorted input in Pentaho; "
                "Spark dropDuplicates is order-independent"
            )
        lines.append(
            f"# preserved.count_rows={count_rows} count_field={count_field!r} "
            f"compare_fields={[c.get('name') for c in compare]}"
        )

        ci_fields = [c for c in compare if c.get("case_insensitive") and c.get("name")]
        work = in_df
        dedupe_cols: list[str] = []
        if ci_fields:
            lines.append(f"_uniq_{out_var} = {in_df}")
            work = f"_uniq_{out_var}"
            for c in compare:
                name = c["name"]
                if c.get("case_insensitive"):
                    tmp = f"_ci_{name}"
                    lines.append(
                        f'{work} = {work}.withColumn("{tmp}", '
                        f'lower(col("{name}").cast("string")))'
                    )
                    dedupe_cols.append(tmp)
                else:
                    dedupe_cols.append(name)
        else:
            dedupe_cols = list(compare_names)

        part_cols = (
            ", ".join(f'col("{c}")' for c in dedupe_cols) if dedupe_cols else "lit(1)"
        )
        subset = (
            "[" + ", ".join(f'"{c}"' for c in dedupe_cols) + "]" if dedupe_cols else ""
        )

        if count_rows:
            lines.append(f"{out_var} = {work}")
            lines.append(f"_w_cnt_{out_var} = Window.partitionBy({part_cols})")
            lines.append(
                f'{out_var} = {out_var}.withColumn("{count_field}", '
                f"count(lit(1)).over(_w_cnt_{out_var}))"
            )
            lines.append(
                f"_w_rn_{out_var} = Window.partitionBy({part_cols})"
                f".orderBy(monotonically_increasing_id())"
            )
            lines.append(
                f"{out_var} = {out_var}.withColumn('_uniq_rn', "
                f"row_number().over(_w_rn_{out_var}))"
            )
            lines.append(
                f"{out_var} = {out_var}.filter(col('_uniq_rn') == 1).drop('_uniq_rn')"
            )
        elif subset:
            lines.append(f"{out_var} = {work}.dropDuplicates({subset})")
        else:
            lines.append(f"{out_var} = {in_df}.dropDuplicates()")

        if ci_fields:
            drop_ci = ", ".join(f'"_ci_{c["name"]}"' for c in ci_fields)
            lines.append(f"{out_var} = {out_var}.drop({drop_ci})")

        return lines, "converted"


class ValueMapperHandler(BaseStepHandler):
    _TYPES = {"valuemapper"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..step_xml import parse_value_mapper_config

        metadata = dict(get_converter_metadata(context))
        step_el = get_step_element(context.step)
        # Prefer complete XML mapping rows so defaults never replace declared mappings.
        if step_el is not None:
            cfg = parse_value_mapper_config(step_el)
            xml_maps = cfg.get("mappings") or mappings_from_step_element(step_el) or []
            meta_maps = metadata.get("mappings") or metadata.get("value_mappings") or []
            if xml_maps and len(xml_maps) >= len(meta_maps):
                metadata["mappings"] = xml_maps
            elif not meta_maps and xml_maps:
                metadata["mappings"] = xml_maps
            for key in (
                "field_to_use",
                "target_field",
                "non_match_default",
                "case_sensitive",
                "non_empty",
            ):
                if key in cfg and cfg[key] is not None:
                    metadata[key] = cfg[key]
        elif not metadata.get("mappings") and not metadata.get("value_mappings"):
            extra = mappings_from_step_element(step_el)
            if extra:
                metadata["mappings"] = extra
        return convert_value_mapper_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
        )


class SequenceHandler(BaseStepHandler):
    _TYPES = {"sequence", "addsequence"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        metadata = get_converter_metadata(context)
        cfg = parse_sequence_config(step_el) if step_el is not None else SequenceConfig()
        # Prefer enriched metadata when present
        if metadata.get("field_name"):
            cfg = SequenceConfig(
                field_name=metadata.get("field_name") or cfg.field_name,
                start_at=int(metadata.get("start_at") or cfg.start_at),
                increment_by=int(metadata.get("increment_by") or cfg.increment_by),
                max_value=metadata.get("max_value")
                if metadata.get("max_value") is not None
                else cfg.max_value,
                use_counter=bool(metadata.get("use_counter", cfg.use_counter)),
                use_database=bool(metadata.get("use_database", cfg.use_database)),
                connection=metadata.get("connection") or cfg.connection,
                schema_name=metadata.get("schema_name") or cfg.schema_name,
                sequence_name=metadata.get("sequence_name") or cfg.sequence_name,
                counter_name=metadata.get("counter_name") or cfg.counter_name,
            )
        lines = [f"# Add Sequence: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Add Sequence")

        if cfg.use_database:
            lines.append(
                f"# WARNING: database sequence not executed remotely; "
                f"connection={cfg.connection!r} schema={cfg.schema_name!r} "
                f"sequence={cfg.sequence_name!r} — generating Spark counter instead"
            )

        lines.append(
            f"# preserved.use_counter={cfg.use_counter} counter_name={cfg.counter_name!r}"
        )
        # Stable order for distributed runs
        lines.append(
            f"_w_seq_{out_var} = Window.orderBy(monotonically_increasing_id())"
        )
        base = (
            f"lit({cfg.start_at}) + (row_number().over(_w_seq_{out_var}) - lit(1)) "
            f"* lit({cfg.increment_by})"
        )
        if cfg.max_value is not None and cfg.increment_by:
            # Pentaho wraps to start after exceeding max
            # period = number of values in [start, max] stepping by incr
            period_expr = (
                f"((lit({cfg.max_value}) - lit({cfg.start_at})) // lit({cfg.increment_by})) + lit(1)"
            )
            seq_expr = (
                f"lit({cfg.start_at}) + "
                f"((row_number().over(_w_seq_{out_var}) - lit(1)) % greatest({period_expr}, lit(1))) "
                f"* lit({cfg.increment_by})"
            )
            lines.append(
                f"# preserved.max_value={cfg.max_value} — wrap to start (Pentaho counter)"
            )
        else:
            seq_expr = base

        lines.append(
            f'{out_var} = {in_df}.withColumn("{cfg.field_name}", {seq_expr})'
        )
        lines.append(
            "# WARNING: Spark row_number over monotonically_increasing_id is order-based; "
            "sort upstream if deterministic sequencing across partitions is required"
        )
        return lines, "converted"


class SystemInfoHandler(BaseStepHandler):
    _TYPES = {"systeminfo"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        lines = [f"# System Info: {context.step.name}"]
        fields = parse_system_info_fields(step_el) if step_el is not None else []

        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.range(1).select(lit(1).alias('_row'))")

        if not fields:
            lines.append(f'{out_var} = {out_var}.withColumn("_system_ts", current_timestamp())')
            return lines, "converted"

        for name, sys_type in fields:
            expr = system_info_expr(sys_type)
            lines.append(f'{out_var} = {out_var}.withColumn("{name}", {expr})')
        return lines, "converted"


def _is_dummy_step_type(step_type: str) -> bool:
    compact = (
        (step_type or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
    )
    return compact in {"dummy", "dummytrans", "dummydonothing"}


def dummy_df_name(step_name: str) -> str:
    """Output DataFrame variable for a Dummy step (always df_Dummy_<Name>)."""
    safe = step_name.replace(" ", "_").replace("-", "_")
    lower = safe.lower()
    if lower == "dummy" or lower.startswith("dummy_"):
        return f"df_{safe}"
    return f"df_Dummy_{safe}"


def _safe_step_df_name(step_name: str) -> str:
    return f"df_{step_name.replace(' ', '_').replace('-', '_')}"


def _filter_branch_stream_for_dummy(
    context: StepContext, pred_name: str
) -> str | None:
    """If Dummy is a Filter/JavaFilter/SwitchCase branch target, return that DF name."""
    pred = context.dag.steps.get(pred_name)
    if pred is None:
        return None
    st = (pred.step_type or "").strip().lower().replace(" ", "")
    dummy_name = context.step.name

    if st in {"filterrows", "javafilter"}:
        meta: dict = {}
        if pred.parsed_config:
            meta = dict(pred.parsed_config)
        else:
            step_el = get_step_element(pred)
            if step_el is not None:
                if st == "filterrows":
                    from ..step_xml import parse_filter_rows_config

                    meta = parse_filter_rows_config(step_el)
                else:
                    from ..step_xml import parse_java_filter_config

                    meta = parse_java_filter_config(step_el)

        from ..filter_converter import _connected_branch_targets

        true_target, false_target = _connected_branch_targets(meta, context, pred_name)
        if dummy_name in {true_target, false_target}:
            return _safe_step_df_name(dummy_name)
        return None

    if st == "switchcase":
        meta = dict(pred.parsed_config) if pred.parsed_config else {}
        if not meta:
            step_el = get_step_element(pred)
            if step_el is not None:
                from ..step_xml import parse_switch_case_config

                meta = parse_switch_case_config(step_el)
        targets = {
            (c.get("target_step") if isinstance(c, dict) else "")
            for c in (meta.get("cases") or [])
        }
        default = (meta.get("default_target_step") or "").strip()
        if default:
            targets.add(default)
        if dummy_name in targets:
            return _safe_step_df_name(dummy_name)
        return None

    return None


def _dummy_hop_input_df(context: StepContext) -> str | None:
    """Resolve Dummy input from the incoming hop (not code-generation order)."""
    preds = context.dag.predecessors(context.step.name)
    if not preds:
        return None
    # Dummy is a single-input pass-through; use the hop source step.
    pred_name = preds[0]
    branch_df = _filter_branch_stream_for_dummy(context, pred_name)
    if branch_df:
        return branch_df
    return context.df_variable_map.get(pred_name, _safe_step_df_name(pred_name))


class DummyHandler(BaseStepHandler):
    """Pass-through step — forwards the hop input DataFrame unchanged."""

    _TYPES = {"dummy", "dummytrans", "dummydonothing"}

    def can_handle(self, step_type: str) -> bool:
        return _is_dummy_step_type(step_type)

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..metadata_propagation import get_converter_metadata
        from ..step_xml import parse_dummy_config

        out_var = context.df_variable_map.get(
            context.step.name, dummy_df_name(context.step.name)
        )
        in_df = _dummy_hop_input_df(context)
        lines = [
            f"# Dummy: {context.step.name}",
            "# Pass-through step - DataFrame unchanged",
        ]
        meta = dict(get_converter_metadata(context))
        step_el = get_step_element(context.step)
        if step_el is not None:
            cfg = parse_dummy_config(step_el)
            for key, val in cfg.items():
                meta.setdefault(key, val)
        extras = meta.get("extras") or {}
        for key, val in extras.items():
            if val not in (None, "", [], {}):
                lines.append(f"# preserved.{key}={val!r}")
        if in_df:
            # Never overwrite an existing branch stream with a different hop source.
            if in_df != out_var:
                lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(
                f"{out_var} = spark.createDataFrame([], '_init STRING').limit(0)"
            )
        return lines, "converted"


# SetVariablesHandler / GetVariablesHandler moved to job_handlers.py (Job category).


class DimensionLookupHandler(BaseStepHandler):
    """Dimension Lookup/Update → SCD Type 1/2 Delta MERGE + stream TK join."""

    _TYPES = {"dimensionlookup", "dimensionlookupupdate"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..data_warehouse_converter import convert_dimension_lookup_step

        metadata = get_converter_metadata(context)
        return convert_dimension_lookup_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


class CombinationLookupHandler(BaseStepHandler):
    """Combination Lookup/Update → junk-dimension Delta MERGE + surrogate key."""

    _TYPES = {"combinationlookup"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..data_warehouse_converter import convert_combination_lookup_step

        metadata = get_converter_metadata(context)
        return convert_combination_lookup_step(
            metadata,
            context.input_df_name(),
            context.output_df_name(),
            context.step.name,
            context=context,
        )


class SynchronizeAfterMergeHandler(BaseStepHandler):
    """Synchronize After Merge → Delta MERGE driven by operation-order flags."""

    _TYPES = {"synchronizeaftermerge", "synchronizemerge"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in {
            "synchronizeaftermerge", "synchronizemerge"
        }

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from ..generation_config import GenerationConfig
        from ..metadata_propagation import get_converter_metadata
        from ..table_names import qualify_table_name

        in_df = context.input_df_name()
        out_var = context.output_df_name()
        meta = get_converter_metadata(context)
        schema = self._attr(context, "schema", "") or str(meta.get("schema") or "")
        table = (
            self._attr(context, "table", "")
            or self._attr(context, "tablename", "")
            or str(meta.get("table") or "target")
        )
        cfg = context.extra.get("generation_config")
        if not isinstance(cfg, GenerationConfig):
            cfg = GenerationConfig.defaults()
        full = qualify_table_name(table, schema, config=cfg)

        key_pairs: list[tuple[str, str]] = []
        for item in meta.get("keys") or []:
            if isinstance(item, dict):
                stream = (item.get("stream_field") or "").strip()
                table_f = (item.get("table_field") or stream).strip()
                if stream or table_f:
                    key_pairs.append((stream or table_f, table_f or stream))
        if not key_pairs:
            key_pairs = [(f.name, f.name) for f in context.step.fields if f.name]

        op_field = (
            self._attr(context, "operation_order_field", "")
            or str(meta.get("operation_order_field") or "flag")
        )
        order_insert = self._attr(context, "order_insert", "") or str(meta.get("order_insert") or "new")
        order_update = self._attr(context, "order_update", "") or str(meta.get("order_update") or "changed")
        order_delete = self._attr(context, "order_delete", "") or str(meta.get("order_delete") or "deleted")

        lines = [f"# Synchronize After Merge: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Synchronize After Merge")

        lines.append(f"_sync_src = {in_df}")
        lines.append("_sync_src.createOrReplaceTempView('_sync_src')")
        if key_pairs:
            merge_cond = " AND ".join(
                f"t.`{table_f}` = s.`{stream_f}`" for stream_f, table_f in key_pairs
            )
            lines.append(
                f"spark.sql('''MERGE INTO {full} t USING _sync_src s "
                f"ON {merge_cond} "
                f"WHEN MATCHED AND s.`{op_field}` = '{order_delete}' THEN DELETE "
                f"WHEN MATCHED AND s.`{op_field}` = '{order_update}' THEN UPDATE SET * "
                f"WHEN NOT MATCHED AND s.`{op_field}` = '{order_insert}' THEN INSERT *''')"
            )
        else:
            lines.append(
                f"# WARNING: No lookup keys — appending all rows into {full}"
            )
            lines.append(
                f"_sync_src.write.format('delta').mode('append').saveAsTable({full!r})"
            )
        lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class RankHandler(BaseStepHandler):
    _TYPES = {"rank"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        cfg = parse_rank_config(step_el) if step_el is not None else RankConfig()
        sort_fields = parse_sort_fields(step_el) if step_el is not None else []
        lines = [f"# Rank: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Rank")

        partition = self._attr(context, "partition_field", "")
        order_cols = []
        for name, asc in sort_fields or [(cfg.field_name, True)]:
            if name:
                order_cols.append(f'col("{name}").{"asc" if asc else "desc"}()')
        if not order_cols and cfg.field_name:
            order_cols = [f'col("{cfg.field_name}").asc()']

        if partition:
            lines.append(f"_w_rank = Window.partitionBy('{partition}').orderBy({', '.join(order_cols)})")
        else:
            lines.append(f"_w_rank = Window.orderBy({', '.join(order_cols) if order_cols else 'monotonically_increasing_id()'})")

        rank_fn = "rank()" if cfg.rank else "dense_rank()"
        lines.append(
            f"{out_var} = {in_df}.withColumn('{cfg.rank_field}', {rank_fn}.over(_w_rank))"
        )
        if cfg.sort_size:
            lines.append(f"{out_var} = {out_var}.filter(col('{cfg.rank_field}') <= {cfg.sort_size})")
        return lines, "converted"


class TopNHandler(BaseStepHandler):
    _TYPES = {"top", "rowsfilter", "limit"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        n = int(self._attr(context, "nr_lines", self._attr(context, "limit", "10")) or "10")
        lines = [f"# Top N: {context.step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}.limit({n})")
            return lines, "converted"
        return _passthrough(context, "Top N")


class IfNullHandler(BaseStepHandler):
    _TYPES = {"ifnull", "iffieldvaluenull", "iffieldvalueisnull"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in self._TYPES

    def _replacement_value(self, raw: str, *, set_empty: bool = False) -> str:
        if set_empty:
            return 'lit("")'
        value = (raw or "").strip()
        if not value:
            return "lit(None)"
        try:
            if "." in value:
                return f"lit({float(value)})"
            return f"lit({int(value)})"
        except ValueError:
            pass
        if value.upper() in ("TRUE", "FALSE"):
            return f"lit({value.upper() == 'TRUE'})"
        return f"lit({value!r})"

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        replacements = metadata.get("replacements") or []
        value_types = metadata.get("value_types") or []
        select_fields = bool(metadata.get("select_fields"))
        select_values_type = bool(metadata.get("select_values_type"))
        replace_all = metadata.get("replace_all") or ""
        set_empty_all = bool(metadata.get("set_empty_string_all"))

        lines = [f"# If Field Value Is Null: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "If Field Value Is Null")

        # Pentaho precedence: selectFields → selectValuesType → replaceAllByValue
        if select_fields and replacements:
            lines.append(f"{out_var} = {in_df}")
            for item in replacements:
                field = item.get("name", "")
                if not field:
                    continue
                replace = self._replacement_value(
                    item.get("value", ""),
                    set_empty=bool(item.get("set_empty_string")),
                )
                lines.append(
                    f"{out_var} = {out_var}.withColumn({field!r}, "
                    f"when(col({field!r}).isNull(), {replace}).otherwise(col({field!r})))"
                )
            return lines, "converted"

        if select_values_type and value_types:
            lines.append(f"{out_var} = {in_df}")
            lines.append(
                f"# Type-based null replacements: {[vt.get('type') for vt in value_types]!r}"
            )
            for vt in value_types:
                pdi_type = (vt.get("type") or "").strip().lower()
                replace = self._replacement_value(
                    vt.get("value", ""),
                    set_empty=bool(vt.get("set_empty_string")),
                )
                spark_type = {
                    "string": "string", "integer": "int", "number": "double",
                    "bignumber": "decimal", "date": "date", "timestamp": "timestamp",
                    "boolean": "boolean", "binary": "binary",
                }.get(pdi_type, pdi_type or "string")
                lines.append(f"for _col, _dtype in {out_var}.dtypes:")
                lines.append(
                    f"    if _dtype.startswith({spark_type!r}) or _dtype == {spark_type!r}:"
                )
                lines.append(
                    f"        {out_var} = {out_var}.withColumn("
                    f"_col, when(col(_col).isNull(), {replace}).otherwise(col(_col)))"
                )
            return lines, "converted"

        if replace_all or set_empty_all:
            replace = self._replacement_value(str(replace_all), set_empty=set_empty_all)
            input_cols = context.extra.get("input_columns") or [
                f.name for f in (context.step.fields or []) if f.name
            ]
            lines.append(f"{out_var} = {in_df}")
            if input_cols:
                for field in input_cols:
                    lines.append(
                        f"{out_var} = {out_var}.withColumn({field!r}, "
                        f"when(col({field!r}).isNull(), {replace}).otherwise(col({field!r})))"
                    )
            else:
                lines.append(f"for _col in {out_var}.columns:")
                lines.append(
                    f"    {out_var} = {out_var}.withColumn("
                    f"_col, when(col(_col).isNull(), {replace}).otherwise(col(_col)))"
                )
            return lines, "converted"

        # Fallback: field list present without explicit selectFields flag
        if replacements:
            lines.append(f"{out_var} = {in_df}")
            for item in replacements:
                field = item.get("name", "")
                if not field:
                    continue
                replace = self._replacement_value(
                    item.get("value", ""),
                    set_empty=bool(item.get("set_empty_string")),
                )
                lines.append(
                    f"{out_var} = {out_var}.withColumn({field!r}, "
                    f"when(col({field!r}).isNull(), {replace}).otherwise(col({field!r})))"
                )
            return lines, "converted"

        field = self._attr(context, "field", self._attr(context, "fieldname", ""))
        replace_raw = self._attr(context, "replace", self._attr(context, "value", ""))
        if field:
            replace = self._replacement_value(replace_raw)
            lines.append(
                f"{out_var} = {in_df}.withColumn({field!r}, "
                f"when(col({field!r}).isNull(), {replace}).otherwise(col({field!r})))"
            )
            return lines, "converted"
        return _passthrough(context, "If Field Value Is Null")


class JsonInputHandler(BaseStepHandler):
    _TYPES = {"jsoninput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "file", ""))
        lines = [f"# JSON Input: {context.step.name}"]
        lines.append(
            f"{out_var} = spark.read.format('json').option('multiline', 'true').load({filename!r})"
        )
        return lines, "converted" if filename else "converted"


class XmlInputHandler(BaseStepHandler):
    _TYPES = {"getxmldata", "xmlinput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "xml_field", ""))
        lines = [f"# XML Input: {context.step.name}"]
        lines.append(
            f"{out_var} = spark.read.format('xml').option('rowTag', 'row').load({filename!r})"
        )
        return lines, "converted" if filename else "converted"


# All advanced handlers for auto-registration
# Flow steps (Abort, Blocking, DetectEmpty, SwitchCase, Append, …) live in flow_handlers.py
ADVANCED_HANDLERS: list[BaseStepHandler] = [
    UniqueHandler(),
    ValueMapperHandler(),
    SequenceHandler(),
    SystemInfoHandler(),
    DummyHandler(),
    DimensionLookupHandler(),
    CombinationLookupHandler(),
    SynchronizeAfterMergeHandler(),
    RankHandler(),
    TopNHandler(),
    # RegexReplace → StringOperationsHandler; MergeRows → join_handlers
    IfNullHandler(),
    JsonInputHandler(),
    XmlInputHandler(),
]
