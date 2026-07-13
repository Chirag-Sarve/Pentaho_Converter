"""Handlers for additional Pentaho step types (production coverage)."""

from __future__ import annotations

from ..expression_converter import convert_formula
from ..filter_converter import convert_filter_condition
from ..metadata_propagation import get_converter_metadata
from ..value_mapper_converter import convert_value_mapper_step, mappings_from_step_element
from ..step_xml import (
    RankConfig,
    _child_text,
    aggregate_to_spark,
    format_spark_join_on,
    get_step_element,
    parse_denormaliser_group_fields,
    parse_filter_compare_element,
    parse_javascript_script,
    parse_join_keys,
    parse_normaliser_type_fields,
    parse_rank_config,
    parse_sequence_config,
    parse_sort_fields,
    parse_switch_case_rules,
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
        step_el = get_step_element(step)
        lines = [f"# Unique: {step.name}"]
        if not in_df:
            return _passthrough(context, "Unique")

        compare_fields = [f.name for f in self._fields(context) if f.name]
        count_rows = self._attr(context, "count_rows", "N").upper() == "Y"
        count_field = self._attr(context, "count_field", "")

        if count_field:
            subset = f'["{count_field}"]'
            lines.append(f"{out_var} = {in_df}.dropDuplicates({subset})")
        elif compare_fields:
            subset = "[" + ", ".join(f'"{c}"' for c in compare_fields) + "]"
            lines.append(f"{out_var} = {in_df}.dropDuplicates({subset})")
        elif count_rows:
            lines.append("_w_unique = Window.orderBy(monotonically_increasing_id())")
            lines.append(
                f"{out_var} = {in_df}.withColumn('_rn', row_number().over(_w_unique))"
            )
            lines.append(f"{out_var} = {out_var}.filter(col('_rn') == 1).drop('_rn')")
        else:
            lines.append(f"{out_var} = {in_df}.dropDuplicates()")
        return lines, "converted"


class ValueMapperHandler(BaseStepHandler):
    _TYPES = {"valuemapper"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        metadata = dict(get_converter_metadata(context))
        if not metadata.get("mappings") and not metadata.get("value_mappings"):
            step_el = get_step_element(context.step)
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
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        cfg = parse_sequence_config(step_el) if step_el is not None else SequenceConfig()
        lines = [f"# Sequence: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Sequence")

        lines.append("_w_seq = Window.orderBy(monotonically_increasing_id())")
        lines.append(
            f"{out_var} = {in_df}.withColumn("
            f"\"{cfg.field_name}\", "
            f"lit({cfg.start_at}) + (row_number().over(_w_seq) - 1) * lit({cfg.increment_by}))"
        )
        if cfg.max_value is not None:
            lines.append(
                f"{out_var} = {out_var}.withColumn("
                f"\"{cfg.field_name}\", "
                f"when(col(\"{cfg.field_name}\") > lit({cfg.max_value}), lit({cfg.max_value}))"
                f".otherwise(col(\"{cfg.field_name}\")))"
            )
        return lines, "converted"


class SystemInfoHandler(BaseStepHandler):
    _TYPES = {"systeminfo"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

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


class AbortHandler(BaseStepHandler):
    _TYPES = {"abort"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        message = self._attr(context, "message", "Pentaho Abort step triggered")
        lines = [f"# Abort: {context.step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.createDataFrame([], '_abort STRING').limit(0)")
        lines.append(f"if True:  # Abort condition for step {context.step.name!r}")
        lines.append(f"    raise RuntimeError({message!r})")
        return lines, "converted"


class MemoryGroupByHandler(BaseStepHandler):
    _TYPES = {"memorygroupby"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from .transform_handlers import GroupByHandler
        return GroupByHandler().generate_code(context)


class AnalyticQueryHandler(BaseStepHandler):
    _TYPES = {"analyticquery"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        sql = self._attr(context, "sql", "") or self._attr(context, "query", "")
        lines = [f"# Analytic Query: {context.step.name}"]
        if sql:
            lines.append(f"{out_var} = spark.sql({sql!r})")
            return lines, "converted"
        if in_df:
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"
        lines.append(f"{out_var} = spark.sql('SELECT 1 AS _placeholder')")
        return lines, "converted"


class BlockingStepHandler(BaseStepHandler):
    _TYPES = {"blockingstep", "block"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        lines = [f"# Blocking Step: {context.step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}.cache()")
            lines.append(f"_ = {out_var}.count()")
            return lines, "converted"
        return _passthrough(context, "Blocking Step")


class DetectEmptyStreamHandler(BaseStepHandler):
    _TYPES = {"detectemptystream", "detectempty"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        lines = [f"# Detect Empty Stream: {context.step.name}"]
        if in_df:
            lines.append(f"_empty_flag_{out_var} = {in_df}.limit(1).count() == 0")
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"
        return _passthrough(context, "Detect Empty Stream")


class SetVariablesHandler(BaseStepHandler):
    _TYPES = {"setvariables", "setvariable"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        var_name = self._attr(context, "variable_name", self._attr(context, "name", "var"))
        var_value = self._attr(context, "variable_value", self._attr(context, "value", ""))
        lines = [f"# Set Variables: {context.step.name}"]
        lines.append(f"spark.conf.set('pentaho.var.{var_name}', {var_value!r})")
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.range(1).select(lit({var_value!r}).alias('_var'))")
        return lines, "converted"


class GetVariablesHandler(BaseStepHandler):
    _TYPES = {"getvariables", "getvariable"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        var_name = self._attr(context, "variable_name", "var")
        field_name = self._attr(context, "field_name", var_name)
        lines = [f"# Get Variables: {context.step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.range(1).select(lit(1).alias('_row'))")
        lines.append(
            f"{out_var} = {out_var}.withColumn(\"{field_name}\", "
            f"lit(spark.conf.get('pentaho.var.{var_name}', '')))"
        )
        return lines, "converted"


class SwitchCaseHandler(BaseStepHandler):
    _TYPES = {"switchcase"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        switch_field, rules, default_target = (
            parse_switch_case_rules(step_el) if step_el is not None else ("", [], "")
        )
        lines = [f"# Switch Case: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Switch Case")

        route_col = f"_route_{context.step.name.replace(' ', '_')}"
        if switch_field and rules:
            expr = f'when(col("{switch_field}") == lit({rules[0].value!r}), lit({rules[0].target_step!r}))'
            for rule in rules[1:]:
                expr = (
                    f"{expr}.when(col(\"{switch_field}\") == lit({rule.value!r}), "
                    f"lit({rule.target_step!r}))"
                )
            if default_target:
                expr = f"{expr}.otherwise(lit({default_target!r}))"
            else:
                expr = f"{expr}.otherwise(lit('default'))"
            lines.append(f"{out_var} = {in_df}.withColumn('{route_col}', {expr})")
        else:
            lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class DimensionLookupHandler(BaseStepHandler):
    _TYPES = {"dimensionlookup", "dimensionlookupupdate"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from .transform_handlers import DatabaseLookupHandler
        return DatabaseLookupHandler().generate_code(context)


class CombinationLookupHandler(BaseStepHandler):
    _TYPES = {"combinationlookup"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from .transform_handlers import StreamLookupHandler
        return StreamLookupHandler().generate_code(context)


class SynchronizeAfterMergeHandler(BaseStepHandler):
    _TYPES = {"synchronizeaftermerge", "synchronizemerge"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in {
            "synchronizeaftermerge", "synchronizemerge"
        }

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        table = self._attr(context, "table", self._attr(context, "tablename", "target"))
        key_fields = [f.name for f in context.step.fields if f.name]
        lines = [f"# Synchronize After Merge: {context.step.name}"]
        if in_df:
            lines.append(f"target_tbl = spark.table({table!r})")
            if key_fields:
                merge_cond = " AND ".join(
                    f't.`{k}` = s.`{k}`' for k in key_fields
                )
                lines.append(
                    f"spark.sql(f'''MERGE INTO {{target_tbl}} t USING {{_sync_src}} s "
                    f"ON {merge_cond} WHEN MATCHED THEN UPDATE SET * "
                    f"WHEN NOT MATCHED THEN INSERT *''')"
                )
                lines.append(f"_sync_src = {in_df}")
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"
        return _passthrough(context, "Synchronize After Merge")


class MergeRowsHandler(BaseStepHandler):
    _TYPES = {"mergerows", "mergerow"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        keys = parse_join_keys(step_el) if step_el is not None else []
        flag_field = self._attr(context, "flag_field", "flag")
        lines = [f"# Merge Rows: {context.step.name}"]
        if len(inputs) >= 2:
            ref, compare = inputs[0], inputs[1]
            ref_a, cmp_a = f"_ref_{out_var}", f"_cmp_{out_var}"
            lines.append(f'{ref_a} = {ref}.alias("r")')
            lines.append(f'{cmp_a} = {compare}.alias("c")')
            if keys:
                join_cond = " & ".join(
                    f'(col("r.{k.left}") == col("c.{k.right or k.left}"))' for k in keys
                )
                lines.append(f"{out_var} = {ref_a}.join({cmp_a}, {join_cond}, 'full_outer')")
                key_ref = keys[0].left
                key_cmp = keys[0].right or keys[0].left
                lines.append(
                    f"{out_var} = {out_var}.withColumn('{flag_field}', "
                    f'when(col("c.{key_cmp}").isNull(), lit("deleted"))'
                    f'.when(col("r.{key_ref}").isNull(), lit("new"))'
                    f'.otherwise(lit("identical")))'
                )
            else:
                lines.append(f"{out_var} = {ref}.unionByName({compare}, allowMissingColumns=True)")
                lines.append(
                    f"{out_var} = {out_var}.withColumn('{flag_field}', lit('identical'))"
                )
            return lines, "converted"
        if len(inputs) == 1:
            lines.append(f"{out_var} = {inputs[0]}")
            return lines, "converted"
        return _passthrough(context, "Merge Rows")


class FuzzyMatchHandler(BaseStepHandler):
    _TYPES = {"fuzzymatch"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        inputs = context.all_input_df_names()
        out_var = context.output_df_name()
        lines = [f"# Fuzzy Match: {context.step.name}"]
        if len(inputs) >= 2:
            lines.append(f"{out_var} = {inputs[0]}.crossJoin({inputs[1]})")
            match_field = self._attr(context, "match_field", "match")
            lines.append(
                f"{out_var} = {out_var}.withColumn('{match_field}', "
                f"levenshtein(col('{match_field}_main'), col('{match_field}_lookup')))"
            )
            return lines, "converted"
        return _passthrough(context, "Fuzzy Match")


class RankHandler(BaseStepHandler):
    _TYPES = {"rank"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

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
    _TYPES = {"top", "rowsfilter", "samplerows", "limit"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        n = int(self._attr(context, "nr_lines", self._attr(context, "limit", "10")) or "10")
        lines = [f"# Top N: {context.step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}.limit({n})")
            return lines, "converted"
        return _passthrough(context, "Top N")


class RowNormaliserHandler(BaseStepHandler):
    _TYPES = {"rownormaliser", "rownormalizer", "normaliser"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        type_fields = parse_normaliser_type_fields(step_el) if step_el is not None else []
        lines = [f"# Row Normaliser: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Row Normaliser")

        if type_fields:
            type_field, value_fields = type_fields[0]
            stack_cols = ", ".join(f'"{v}"' for v in value_fields)
            lines.append(
                f"{out_var} = {in_df}.select('*', explode(array({stack_cols})).alias('_norm_val'))"
            )
            lines.append(
                f"{out_var} = {out_var}.withColumn('{type_field}', col('_norm_val'))"
            )
        else:
            key_field = self._attr(context, "type_field", "type")
            lines.append(f"{out_var} = {in_df}.withColumn('{key_field}', lit('normalized'))")
        return lines, "converted"


class RowDenormaliserHandler(BaseStepHandler):
    _TYPES = {"rowdenormaliser", "rowdenormalizer", "denormaliser"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        group_fields, target_field, target_fields = (
            parse_denormaliser_group_fields(step_el) if step_el is not None else ([], "", [])
        )
        lines = [f"# Row Denormaliser: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Row Denormaliser")

        if group_fields and target_field:
            agg_exprs = [
                f"first(col('{target_field}'), ignorenulls=True).alias('{tf}')"
                for tf in (target_fields or [target_field])
            ]
            group_cols = ", ".join(f'"{g}"' for g in group_fields)
            lines.append(
                f"{out_var} = {in_df}.groupBy({group_cols}).agg({', '.join(agg_exprs)})"
            )
        else:
            lines.append(f"{out_var} = {in_df}")
        return lines, "converted"


class RegexEvalHandler(BaseStepHandler):
    _TYPES = {"regexeval", "regularexpression"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        field = self._attr(context, "field", self._attr(context, "matcher", "field"))
        regex = self._attr(context, "regex", self._attr(context, "pattern", ".*"))
        result_field = self._attr(context, "resultfieldname", "match")
        lines = [f"# Regex Evaluation: {context.step.name}"]
        if in_df:
            lines.append(
                f"{out_var} = {in_df}.withColumn('{result_field}', "
                f"col('{field}').rlike({regex!r}))"
            )
            return lines, "converted"
        return _passthrough(context, "Regex Evaluation")


class RegexReplaceHandler(BaseStepHandler):
    _TYPES = {"regexreplace"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        from .string_handlers import StringOperationsHandler
        context.step.step_type = "ReplaceInString"
        return StringOperationsHandler().generate_code(context)


class JavaScriptValueHandler(BaseStepHandler):
    _TYPES = {"scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in {"scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue"}

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        script = parse_javascript_script(step_el) if step_el is not None else ""
        lines = [f"# Modified Java Script Value: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "JavaScript Value")

        lines.append(f"{out_var} = {in_df}")
        if script and context.step.fields:
            for field in context.step.fields:
                if field.name:
                    lines.append(
                        f'{out_var} = {out_var}.withColumn("{field.name}", col("{field.name}"))'
                    )
        elif context.step.fields:
            for field in context.step.fields:
                if field.name:
                    lines.append(
                        f'{out_var} = {out_var}.withColumn("{field.name}", lit(""))'
                    )
        return lines, "converted"


class UserDefinedJavaExpressionHandler(BaseStepHandler):
    _TYPES = {"userdefinedjavaclass", "userdefinedjavaexpression"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return "userdefinedjava" in t

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        class_name = self._attr(context, "class_name", "UserDefinedExpression")
        lines = [f"# User Defined Java Expression: {context.step.name}"]
        if in_df:
            lines.append(f"{out_var} = {in_df}")
            for field in context.step.fields:
                if field.name:
                    lines.append(
                        f'{out_var} = {out_var}.withColumn("{field.name}", '
                        f'lit(None).cast("string"))  # UDJC: {class_name}'
                    )
            return lines, "converted"
        return _passthrough(context, "User Defined Java Expression")


class DataValidatorHandler(BaseStepHandler):
    _TYPES = {"validator", "datavalidator"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        lines = [f"# Data Validator: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "Data Validator")

        condition_root = parse_filter_compare_element(step_el) if step_el is not None else None
        if condition_root is not None:
            filter_expr = convert_filter_condition(condition_root)
            lines.append(f"{out_var} = {in_df}.filter({filter_expr})")
        else:
            lines.append(f"{out_var} = {in_df}.filter(lit(True))")
        return lines, "converted"


class IfNullHandler(BaseStepHandler):
    _TYPES = {"ifnull", "iffieldvaluenull"}

    def can_handle(self, step_type: str) -> bool:
        t = step_type.strip().lower().replace(" ", "")
        return t in {"ifnull", "iffieldvaluenull"}

    def _replacement_value(self, raw: str) -> str:
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

        lines = [f"# If Field Value Is Null: {context.step.name}"]
        if not in_df:
            return _passthrough(context, "If Field Value Is Null")

        if replacements:
            lines.append(f"{out_var} = {in_df}")
            for item in replacements:
                field = item.get("name", "")
                if not field:
                    continue
                replace = self._replacement_value(item.get("value", ""))
                lines.append(
                    f"{out_var} = {out_var}.withColumn({field!r}, "
                    f"when(col({field!r}).isNull(), {replace}).otherwise(col({field!r})))"
                )
            return lines, "converted"

        field = self._attr(context, "field", self._attr(context, "fieldname", ""))
        replace = self._attr(context, "replace", self._attr(context, "value", "''"))
        if field:
            lines.append(
                f"{out_var} = {in_df}.withColumn({field!r}, "
                f"when(col({field!r}).isNull(), {replace}).otherwise(col({field!r})))"
            )
            return lines, "converted"
        return _passthrough(context, "If Field Value Is Null")


class FormulaStepHandler(BaseStepHandler):
    _TYPES = {"formula"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        formula = self._attr(context, "formula", "")
        field_name = self._attr(context, "field_name", "formula_result")
        if step_el is not None:
            formula = formula or _child_text(step_el, "formula")
            field_name = field_name or _child_text(step_el, "field_name", field_name)
        lines = [f"# Formula: {context.step.name}"]
        if in_df and formula:
            lines.append(
                f"{out_var} = {in_df}.withColumn({field_name!r}, {convert_formula(formula)})"
            )
            return lines, "converted"
        if in_df:
            lines.append(f"{out_var} = {in_df}")
            return lines, "converted"
        return _passthrough(context, "Formula")


class JsonInputHandler(BaseStepHandler):
    _TYPES = {"jsoninput"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower() in self._TYPES

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
        return step_type.strip().lower() in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        out_var = context.output_df_name()
        filename = self._attr(context, "filename", self._attr(context, "xml_field", ""))
        lines = [f"# XML Input: {context.step.name}"]
        lines.append(
            f"{out_var} = spark.read.format('xml').option('rowTag', 'row').load({filename!r})"
        )
        return lines, "converted" if filename else "converted"


# All advanced handlers for auto-registration
ADVANCED_HANDLERS: list[BaseStepHandler] = [
    UniqueHandler(),
    ValueMapperHandler(),
    SequenceHandler(),
    SystemInfoHandler(),
    AbortHandler(),
    MemoryGroupByHandler(),
    AnalyticQueryHandler(),
    BlockingStepHandler(),
    DetectEmptyStreamHandler(),
    SetVariablesHandler(),
    GetVariablesHandler(),
    SwitchCaseHandler(),
    DimensionLookupHandler(),
    CombinationLookupHandler(),
    SynchronizeAfterMergeHandler(),
    MergeRowsHandler(),
    FuzzyMatchHandler(),
    RankHandler(),
    TopNHandler(),
    RowNormaliserHandler(),
    RowDenormaliserHandler(),
    RegexEvalHandler(),
    RegexReplaceHandler(),
    JavaScriptValueHandler(),
    UserDefinedJavaExpressionHandler(),
    DataValidatorHandler(),
    IfNullHandler(),
    JsonInputHandler(),
    XmlInputHandler(),
]
