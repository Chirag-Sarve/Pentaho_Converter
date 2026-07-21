"""Per-step semantic validators for core Pentaho transformations."""

from __future__ import annotations

from typing import Any

from ..step_xml import (
    get_step_element,
    parse_calculations,
    parse_constant_fields,
    parse_data_grid_rows,
    parse_filter_compare_element,
    parse_group_by_fields,
    parse_join_keys,
    parse_row_generator_fields,
    parse_sort_fields,
    parse_string_operation_fields,
)
from ..step_context import StepContext
from ..validation.base import SemanticValidationResult, StepValidator
from ..validation.code_checks import columns_referenced, columns_written, validate_python_fragment
from ..validation.registry import register_validator
from ..filter_converter import convert_filter_condition


class RowGeneratorValidator(StepValidator):
    step_types = frozenset({"rowgenerator", "datagrid"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        code = "\n".join(code_lines)
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)

        fields = parsed.get("fields", [])
        rows = parsed.get("rows", [])
        limit = parsed.get("limit", 1)

        if "createDataFrame" not in code:
            result.errors.append("RowGenerator must use spark.createDataFrame.")
            result.score = 0.2
            return result

        converted_props = ["createDataFrame"]
        for f in fields:
            if f.get("value") and repr(f["value"]) in code or f["value"] in code:
                converted_props.append(f"field:{f['name']}")
            else:
                result.warnings.append(f"Default value for field '{f['name']}' not found in generated code.")

        if rows:
            converted_props.append("data_grid_rows")
            if str(len(rows)) not in code and "data = [" not in code:
                result.warnings.append("Data grid row count may not match XML definition.")

        if limit > 1 and f"* {limit}" not in code and f"* {limit}" not in code.replace(" ", ""):
            if f"* {limit}" not in code:
                result.warnings.append(f"Row limit {limit} may not be applied.")

        result.properties_converted = converted_props
        result.output_columns = [f["name"] for f in fields]
        result.score = _score(result)
        return result


class ConstantValidator(StepValidator):
    step_types = frozenset({"constant", "addconstants", "addconstant"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)
        constants = parsed.get("constants", [])

        for c in constants:
            col_name = c["name"]
            if f'withColumn("{col_name}"' in code or f"withColumn('{col_name}'" in code:
                result.properties_converted.append(f"constant:{col_name}")
                result.output_columns.append(col_name)
            else:
                result.errors.append(f"Constant column '{col_name}' not created in generated code.")

        if not constants:
            result.warnings.append("No constant fields defined in XML.")

        result.score = _score(result)
        return result


class CalculatorValidator(StepValidator):
    step_types = frozenset({"calculator"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)
        calcs = list(parsed.get("calculations") or [])

        # Prefer raw XML when converter metadata lost calculations.
        if not calcs:
            step_el = get_step_element(context.step)
            if step_el is not None:
                from ..step_xml import parse_calculations

                calcs = [
                    {
                        "field_name": c.field_name,
                        "calc_type": c.calc_type,
                        "remove": c.remove,
                    }
                    for c in parse_calculations(step_el)
                ]

        if "_calculator_unresolved" in code or (
            "createDataFrame([]," in code and "calculator" in code.lower()
        ):
            result.errors.append(
                "Calculator generated an empty unresolved DataFrame; "
                "upstream DataFrame must be preserved instead."
            )

        if not code_lines:
            result.errors.append("Calculator produced no generated DataFrame assignment.")

        for calc in calcs:
            fname = calc["field_name"]
            if f'withColumn("{fname}"' in code or f"withColumn('{fname}'" in code:
                result.properties_converted.append(f"calc:{fname}:{calc['calc_type']}")
                result.output_columns.append(fname)
            else:
                result.errors.append(f"Calculator output column '{fname}' not created.")
            if calc.get("remove") and ".drop(" not in code:
                result.warnings.append(
                    f"Calculator entry '{fname}' has remove=Y but no drop() in generated code."
                )
            elif calc.get("remove") and ".drop(" in code:
                result.properties_converted.append(f"remove:{fname}")

        if not calcs:
            result.errors.append("Missing Calculator metadata: no calculations found in XML.")
            if context.input_df_name() and (
                f"{context.output_df_name()} = {context.input_df_name()}" in code
                or f"{context.output_df_name()}={context.input_df_name()}" in code.replace(" ", "")
            ):
                result.warnings.append(
                    "Calculator preserved upstream DataFrame after missing metadata."
                )
            result.score = _score(result)
            return result

        # Broken downstream-style: withColumn without a prior assignment to out_var.
        out_var = context.output_df_name()
        if out_var and "withColumn(" in code and f"{out_var} =" not in code and f"{out_var}=" not in code.replace(" ", ""):
            result.errors.append(
                f"Broken DataFrame reference: {out_var} is used without an assignment."
            )

        result.score = _score(result)
        return result


class FilterRowsValidator(StepValidator):
    step_types = frozenset({"filterrows"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)

        if ".filter(" not in code:
            result.errors.append("FilterRows must generate a .filter() call.")
            result.score = 0.2
            return result

        result.properties_converted.append("filter_expression")
        expected_expr = parsed.get("filter_expression", "")
        if expected_expr and expected_expr not in code and "col(" not in code:
            result.warnings.append("Filter expression may not match XML condition tree.")

        refs = columns_referenced(code)
        for col in parsed.get("referenced_columns", []):
            if col not in refs:
                result.warnings.append(f"Filter references column '{col}' but it is not in generated filter.")

        result.score = _score(result)
        return result


class SelectValuesValidator(StepValidator):
    step_types = frozenset({"selectvalues"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)

        if not code_lines:
            result.errors.append(
                "SelectValues produced no generated DataFrame; conversion was aborted "
                "or parsing failed."
            )
            result.score = 0.1
            return result

        if "createDataFrame([]," in code and "_placeholder" in code:
            result.errors.append(
                "SelectValues generated an empty placeholder DataFrame; "
                "upstream DataFrame must be preserved instead."
            )

        select_columns = list(parsed.get("select_columns") or [])
        remove_names = list(parsed.get("remove_names") or [])
        meta_changes = list(parsed.get("meta_changes") or [])

        # Detect failed parsing: XML has field/remove/meta but parsed config is empty.
        step_el = get_step_element(context.step)
        if step_el is not None:
            from ..step_xml import parse_select_values_config

            cfg = parse_select_values_config(step_el)
            xml_select = cfg.get("select_columns") or []
            xml_remove = cfg.get("remove_names") or []
            xml_meta = cfg.get("meta_changes") or []
            if (xml_select or xml_remove or xml_meta) and not (
                select_columns or remove_names or meta_changes
            ):
                result.errors.append(
                    "SelectValues XML parsing failed: configured fields/remove/meta "
                    "were not propagated into converter metadata."
                )
            # Prefer freshly parsed values when metadata was incomplete
            if not select_columns and xml_select:
                select_columns = list(xml_select)
            if not remove_names and xml_remove:
                remove_names = list(xml_remove)
            if not meta_changes and xml_meta:
                meta_changes = list(xml_meta)

        has_select = ".select(" in code or ".selectExpr(" in code
        has_meta = ".withColumn(" in code and (
            ".cast(" in code or "to_date(" in code or "to_timestamp(" in code or ".drop(" in code
        )
        has_drop = ".drop(" in code
        out_var = context.output_df_name()
        in_df = context.input_df_name()
        has_passthrough = bool(
            out_var and in_df and f"{out_var} = {in_df}" in code
        )

        if not has_select and not has_meta and not has_drop and not has_passthrough:
            result.errors.append("SelectValues must generate a select, drop, or metadata update.")
            result.score = 0.3
            return result

        if has_select:
            result.properties_converted.append("select")
        if has_drop and remove_names:
            result.properties_converted.append("remove")
        if meta_changes:
            if has_meta or has_select:
                result.properties_converted.append("meta_changes")
            else:
                result.warnings.append("Meta changes configured but no cast/rename found.")
        if remove_names and not has_drop and not has_select:
            result.warnings.append("Remove names configured but no drop()/select generated.")

        removed = {(n or "").strip() for n in remove_names if n}
        for col in select_columns:
            if col in removed:
                # Intentionally dropped via Remove tab — not expected in generated select.
                result.properties_converted.append(f"removed:{col}")
                continue
            if col in code:
                result.properties_converted.append(f"column:{col}")
                result.output_columns.append(col)
            else:
                result.warnings.append(f"Selected column '{col}' not found in generated select.")

        # Broken downstream-style: assignment missing for output DF.
        if out_var and f"{out_var} =" not in code and f"{out_var}=" not in code.replace(" ", ""):
            result.errors.append(
                f"Broken DataFrame reference: missing assignment for {out_var}."
            )

        result.score = _score(result)
        return result


class SetValueConstantValidator(StepValidator):
    step_types = frozenset({
        "setvalueconstant",
        "setfieldvaluetoaconstant",
        "setfieldvalueconstant",
    })

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)
        fields = parsed.get("set_fields") or parsed.get("fields") or []
        if not fields:
            result.warnings.append("No SetValueConstant fields defined.")
        for item in fields:
            name = item.get("name", "")
            if name and f'withColumn("{name}"' in code:
                result.properties_converted.append(f"set_constant:{name}")
            elif name:
                result.errors.append(f"SetValueConstant field '{name}' not written.")
        result.score = _score(result)
        return result


class SetValueFieldValidator(StepValidator):
    step_types = frozenset({"setvaluefield", "setfieldvalue"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)
        fields = parsed.get("set_fields") or parsed.get("fields") or []
        if ".withColumn(" not in code and fields:
            result.errors.append("SetValueField must generate withColumn assignments.")
        for item in fields:
            name = item.get("name", "")
            replace_by = item.get("replace_by", "")
            if name and f'withColumn("{name}"' in code:
                result.properties_converted.append(f"set_field:{name}")
            if replace_by and f'col("{replace_by}")' in code:
                result.properties_converted.append(f"from:{replace_by}")
        result.score = _score(result)
        return result


class ConcatFieldsValidator(StepValidator):
    step_types = frozenset({"concatfields"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)
        target = parsed.get("target_field_name") or ""
        if "concat_ws(" not in code and "concat(" not in code:
            result.errors.append("ConcatFields must generate concat/concat_ws.")
        else:
            result.properties_converted.append("concat")
        if target and target in code:
            result.output_columns.append(target)
            result.properties_converted.append(f"target:{target}")
        if parsed.get("remove_selected_fields") and ".drop(" not in code:
            result.warnings.append("removeSelectedFields=Y but no drop() generated.")
        result.score = _score(result)
        return result


class AddXmlValidator(StepValidator):
    step_types = frozenset({"addxml"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)
        value_name = parsed.get("value_name") or ""
        if "concat(" not in code or "withColumn(" not in code:
            result.errors.append("AddXML must build an XML string column via concat/withColumn.")
        else:
            result.properties_converted.append("add_xml")
        if value_name and value_name in code:
            result.output_columns.append(value_name)
        root = parsed.get("root_node") or ""
        if root and root in code:
            result.properties_converted.append(f"root:{root}")
        result.score = _score(result)
        return result


class TextFileOutputValidator(StepValidator):
    step_types = frozenset({"textfileoutput", "textfileoutputlegacy"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)
        infos, comment_warnings = _collect_comment_levels(code)
        result.infos.extend(infos)

        has_write = (
            ".write." in code
            or ".text(" in code
            or ".csv(" in code
            or 'format("csv")' in code
            or "format('csv')" in code
            or ".save(" in code
        )
        if not has_write:
            result.errors.append("TextFileOutput must generate a write operation.")
            result.score = 0.2
            return result

        if ".text(" in code:
            result.properties_converted.append("text_write")
        if ".csv(" in code or "format('csv')" in code or 'format("csv")' in code:
            result.properties_converted.append("csv_write")
        if ".mode(" in code or "writer.mode(" in code:
            result.properties_converted.append("mode")
        if ".save(" in code or ".text(" in code or ".csv(" in code:
            result.properties_converted.append("save")

        # Spark CSV writes a directory; matching save/load paths are valid.
        if "Spark CSV" in code or "create a directory" in code.lower():
            result.infos.append(
                "Spark CSV outputs are directories — matching save/load paths are valid."
            )

        code_lower = code.lower()
        if "filename missing" in code_lower or "<output_name>" in code:
            result.warnings.append("Output filename missing or placeholder used.")
            result.properties_missing.append("filename")
        if "delimiter missing" in code_lower:
            result.warnings.append("Output delimiter missing from Pentaho metadata.")
            result.properties_missing.append("delimiter")
        if "encoding missing" in code_lower:
            result.infos.append("Output encoding missing — Spark uses the platform/default encoding.")
        if "fixed-width" in code_lower:
            result.warnings.append("Fixed-width output requires manual implementation.")
        if "is_command" in code_lower or "run as command" in code_lower:
            result.warnings.append("TextFileOutput 'Run as command' is not supported on Databricks.")
        if "preserved.field_format" in code or "split_every" in code_lower:
            result.infos.append("Legacy Text File Output formatting metadata preserved as comments.")

        # Comment warnings that are informational (split_every, formatting) stay as infos.
        for msg in comment_warnings:
            lowered = msg.lower()
            if any(
                token in lowered
                for token in (
                    "split_every",
                    "create_parent",
                    "field_format",
                    "endedline",
                    "enclosure_forced",
                    "precision",
                    "formatting",
                )
            ):
                result.infos.append(msg)
            elif "filename missing" in lowered or "delimiter missing" in lowered:
                continue  # already recorded as warnings
            elif "encoding missing" in lowered:
                continue
            else:
                result.warnings.append(msg)

        checks = {
            "filename": parsed.get("filename", ""),
            "delimiter": parsed.get("separator", ""),
            "header": parsed.get("header"),
            "encoding": parsed.get("encoding"),
        }
        for prop, val in checks.items():
            if val is None or val == "":
                if prop in ("filename", "delimiter") and prop not in result.properties_missing:
                    # Parsed empty is not automatically a score penalty when code emitted correctly.
                    if prop == "delimiter" and ('option("sep"' in code or "option('sep'" in code):
                        result.properties_converted.append(prop)
                    continue
                continue
            if prop == "filename" and (str(val) in code or "PENTAHO_DATA_DIR" in code):
                result.properties_converted.append(prop)
            elif prop == "delimiter" and (str(val) in code or repr(val) in code):
                result.properties_converted.append(prop)
            elif prop == "header" and "header" in code:
                result.properties_converted.append(prop)
            elif prop == "encoding" and str(val) in code:
                result.properties_converted.append(prop)
            elif prop in ("header", "encoding"):
                result.infos.append(f"Output property '{prop}' noted from Pentaho XML.")
            else:
                result.warnings.append(f"Output property '{prop}' may differ from Pentaho XML.")

        result.score = _score(result)
        # Cap only when executable pieces are actually missing (filename/delimiter/fixed-width).
        executable_gap = (
            "filename" in result.properties_missing
            or "delimiter" in result.properties_missing
            or any("fixed-width" in w.lower() for w in result.warnings)
            or any("is_command" in w.lower() or "run as command" in w.lower() for w in result.warnings)
        )
        if executable_gap:
            result.score = min(result.score, 0.85)
        return result


def _syntax_result(code_lines: list[str]) -> SemanticValidationResult:
    result = SemanticValidationResult()
    syntax_ok, errors = validate_python_fragment(code_lines)
    result.syntax_valid = syntax_ok
    result.errors.extend(errors)
    return result


class TableInputValidator(StepValidator):
    step_types = frozenset({"tableinput"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "spark.sql(" in code or "spark.table(" in code:
            result.properties_converted.append("read_source")
        elif "jdbc" in code:
            result.warnings.append("TableInput uses JDBC placeholder URL.")
        else:
            result.errors.append("TableInput must read from SQL or a table.")
        sql = parsed.get("sql", "")
        if sql and sql not in code:
            result.warnings.append("Generated SQL may not match Pentaho XML.")
        result.score = _score(result)
        return result


class CsvInputValidator(StepValidator):
    step_types = frozenset({"csvinput"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "format('csv')" not in code and 'format("csv")' not in code:
            result.errors.append("CSVInput must use spark.read.format('csv').")
        else:
            result.properties_converted.append("csv_reader")
        for prop in ("filename", "separator", "header"):
            val = parsed.get(prop)
            if val is not None and str(val) not in code and prop != "header":
                result.warnings.append(f"CSV property '{prop}' may differ from XML.")
            elif prop == "header" and "header" in code:
                result.properties_converted.append("header")
        result.score = _score(result)
        return result


class SortRowsValidator(StepValidator):
    step_types = frozenset({"sortrows"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if ".orderBy(" not in code:
            result.errors.append("SortRows must generate orderBy().")
        else:
            result.properties_converted.append("order_by")
        for item in parsed.get("sort_fields", []):
            if isinstance(item, (list, tuple)):
                name = item[0] if item else ""
            elif isinstance(item, dict):
                name = item.get("name") or ""
            else:
                name = str(item)
            if name and name not in code:
                result.warnings.append(f"Sort column '{name}' not found in generated orderBy.")
        if parsed.get("unique_rows") and "dropDuplicates" not in code:
            result.warnings.append("unique_rows enabled but dropDuplicates not found.")
        result.score = _score(result)
        return result


class GroupByValidator(StepValidator):
    step_types = frozenset({"groupby"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        has_group_by = ".groupBy(" in code
        has_group_only_distinct = ".distinct()" in code and parsed.get("group_keys") and not parsed.get("aggregates")
        if not has_group_by and not has_group_only_distinct:
            result.errors.append("GroupBy must generate groupBy().")
        elif has_group_by:
            result.properties_converted.append("group_by")
        else:
            result.properties_converted.append("group_keys_distinct")
        for key in parsed.get("group_keys", []):
            if key in code:
                result.properties_converted.append(f"key:{key}")
            else:
                result.warnings.append(f"Group key '{key}' not found in generated code.")
        for agg in parsed.get("aggregates", []):
            subject = agg.get("subject") or agg.get("name")
            if subject and subject in code:
                result.properties_converted.append(f"agg:{agg.get('name')}:{agg.get('aggregate')}")
            elif agg.get("aggregate", "").upper() == "COUNT_ALL" and "count(" in code:
                result.properties_converted.append(f"agg:{agg.get('name')}:COUNT_ALL")
            else:
                result.warnings.append(
                    f"Aggregate '{agg.get('name')}' may not use subject column '{subject}'."
                )
        result.score = _score(result)
        return result


class TableOutputValidator(StepValidator):
    step_types = frozenset({"tableoutput"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if ".write." not in code and "saveAsTable" not in code:
            result.errors.append("TableOutput must generate a write/saveAsTable call.")
        else:
            result.properties_converted.append("write_target")
        table = parsed.get("table", "")
        if table and table not in code:
            result.warnings.append("Target table name may differ from Pentaho XML.")
        result.score = _score(result)
        return result


class UniqueValidator(StepValidator):
    step_types = frozenset({
        "unique", "uniquerows", "uniquerowsbyhashset",
        "uniquerowshashset", "uniquehashset",
    })

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "dropDuplicates" not in code and "row_number" not in code:
            result.errors.append("Unique must generate dropDuplicates() or row_number dedup.")
        else:
            result.properties_converted.append("deduplicate")
        result.score = _score(result)
        return result


class SequenceValidator(StepValidator):
    step_types = frozenset({"sequence", "addsequence", "addsequencefields"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "row_number" not in code and "monotonically_increasing_id" not in code:
            result.errors.append("Sequence must add a row_number or monotonic id column.")
        else:
            result.properties_converted.append("sequence_column")
        result.score = _score(result)
        return result


class RankValidator(StepValidator):
    step_types = frozenset({"rank"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "rank(" not in code and "dense_rank" not in code and "row_number" not in code:
            result.errors.append("Rank must use a window rank function.")
        else:
            result.properties_converted.append("rank")
        result.score = _score(result)
        return result


class ValueMapperValidator(StepValidator):
    step_types = frozenset({"valuemapper"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "when(" not in code:
            result.errors.append("ValueMapper must generate when() mapping expressions.")
        else:
            result.properties_converted.append("value_map")
        result.score = _score(result)
        return result


class SystemInfoValidator(StepValidator):
    step_types = frozenset({"systeminfo"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "current_date" not in code and "current_timestamp" not in code:
            result.errors.append("SystemInfo must add current_date/current_timestamp columns.")
        else:
            result.properties_converted.append("system_fields")
        result.score = _score(result)
        return result


class AbortValidator(StepValidator):
    step_types = frozenset({"abort"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "raise RuntimeError" in code:
            result.properties_converted.append("abort_raise")
            if "if True:" in code:
                result.errors.append(
                    "Abort must raise RuntimeError only when the abort condition is met "
                    "(not via unconditional if True)."
                )
            elif "_abort_count_" in code:
                result.properties_converted.append("conditional_abort")
        elif context.input_df_name():
            result.errors.append(
                "Abort must raise RuntimeError when an upstream DataFrame is present."
            )
        else:
            result.warnings.append(
                "Abort has no upstream DataFrame; RuntimeError skipped because "
                "the abort condition is not satisfied."
            )
        if "row_threshold" in str(parsed) or "_abort_count_" in code or "threshold" in code.lower():
            result.properties_converted.append("row_threshold")
        if "preserved.message" in code or "RuntimeError" in code:
            result.properties_converted.append("message")
        result.score = _score(result)
        return result


class GenericStepValidator(StepValidator):
    """Fallback validator for steps without a dedicated validator."""

    step_types = frozenset()

    def handles(self, step_type: str) -> bool:
        return True

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)

        code = "\n".join(code_lines)
        in_df = context.input_df_name()
        out_var = context.output_df_name()

        if in_df and out_var and f"{out_var} = {in_df}" == code.strip():
            result.warnings.append("Step appears to be a passthrough without transformation logic.")
            result.score = 0.4
        elif errors:
            result.score = 0.2
        elif syntax_ok:
            result.score = 0.75
            result.warnings.append("No dedicated semantic validator for this step type.")
        else:
            result.score = 0.3

        result.output_columns = list(columns_written(code))
        return result


# Properties whose absence means executable Spark I/O is incomplete.
_CRITICAL_MISSING_PROPS = frozenset({
    "filename",
    "delimiter",
    "schema",
})


def _score(result: SemanticValidationResult) -> float:
    """Score from ERROR/WARNING only — ``infos`` never reduce the percentage."""
    if result.errors:
        base = max(0.0, 0.5 - 0.1 * len(result.errors))
    else:
        base = 1.0
    base -= 0.05 * len(result.warnings)
    critical_missing = [
        p for p in result.properties_missing if p in _CRITICAL_MISSING_PROPS
    ]
    base -= 0.05 * len(critical_missing)
    if not result.syntax_valid:
        base = min(base, 0.3)
    return max(0.0, min(1.0, base))


def _collect_comment_levels(code: str) -> tuple[list[str], list[str]]:
    """Split generated comment markers into (infos, warnings)."""
    infos: list[str] = []
    warnings: list[str] = []
    for raw in code.splitlines():
        line = raw.strip()
        if not line.startswith("#"):
            continue
        body = line.lstrip("#").strip()
        upper = body.upper()
        if upper.startswith("INFO:") or body.lower().startswith("preserved.field_format"):
            infos.append(body)
        elif upper.startswith("WARNING:") or upper.startswith("TODO:"):
            warnings.append(body)
    return infos, warnings


def parse_step_config(context: StepContext) -> dict[str, Any]:
    """Parse step XML into a dict used by validators and converters."""
    step = context.step
    step_el = get_step_element(step)
    parsed: dict[str, Any] = {
        "step_type": step.step_type,
        "step_name": step.name,
        "attributes": dict(step.attributes),
        "fields": [{"name": f.name, "type": f.type_name} for f in step.fields],
    }
    if step_el is None:
        return parsed

    # Compact form matches registry/parser keys (display names often include spaces).
    st = (
        (step.step_type or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
    )
    if st in ("rowgenerator", "datagrid"):
        cols, rows = parse_data_grid_rows(step_el)
        fields = parse_row_generator_fields(step_el)
        parsed["fields"] = [{"name": f.name, "type": f.type_name, "value": f.value} for f in fields]
        parsed["rows"] = rows
        parsed["columns"] = cols
        try:
            parsed["limit"] = int(step_el.findtext("limit", "1") or "1")
        except ValueError:
            parsed["limit"] = 1

    elif st == "constant":
        parsed["constants"] = [
            {"name": c.name, "type": c.type_name, "value": c.value}
            for c in parse_constant_fields(step_el)
        ]

    elif st == "calculator":
        parsed["calculations"] = [
            {
                "field_name": c.field_name,
                "calc_type": c.calc_type,
                "field_a": c.field_a,
                "field_b": c.field_b,
                "remove": c.remove,
            }
            for c in parse_calculations(step_el)
        ]

    elif st == "filterrows":
        root = parse_filter_compare_element(step_el)
        if root is not None:
            parsed["filter_expression"] = convert_filter_condition(root)
        parsed["referenced_columns"] = _extract_condition_columns(root)

    elif st == "selectvalues":
        from ..step_xml import parse_select_values_config
        cfg = parse_select_values_config(step_el)
        parsed.update(cfg)
        parsed["select_columns"] = cfg.get("select_columns") or [
            f.name for f in step.fields if f.name
        ]
        parsed["output_columns"] = cfg.get("output_columns") or [
            (f.rename or f.name) for f in step.fields if f.name
        ]

    elif st in ("setvalueconstant", "setvaluefield"):
        from ..step_xml import parse_set_value_constant_config, parse_set_value_field_config
        cfg = (
            parse_set_value_constant_config(step_el)
            if st == "setvalueconstant"
            else parse_set_value_field_config(step_el)
        )
        parsed.update(cfg)

    elif st == "concatfields":
        from ..step_xml import parse_concat_fields_config
        parsed.update(parse_concat_fields_config(step_el))

    elif st == "addxml":
        from ..step_xml import parse_add_xml_config
        parsed.update(parse_add_xml_config(step_el))

    elif st in ("replaceinstring", "replacestring"):
        from ..step_xml import parse_replace_in_string_config
        parsed.update(parse_replace_in_string_config(step_el))

    elif st == "stringoperations":
        from ..step_xml import parse_string_operations_config
        parsed.update(parse_string_operations_config(step_el))

    elif st == "stringcut":
        from ..step_xml import parse_string_cut_config
        parsed.update(parse_string_cut_config(step_el))

    elif st in ("checksum", "addachecksum"):
        from ..step_xml import parse_checksum_config
        parsed.update(parse_checksum_config(step_el))

    elif st == "numberrange":
        from ..step_xml import parse_number_range_config
        parsed.update(parse_number_range_config(step_el))

    elif st in ("fieldschangesequence", "addvaluefieldschangingsequence"):
        from ..step_xml import parse_fields_change_sequence_config
        parsed.update(parse_fields_change_sequence_config(step_el))

    elif st in ("closuregenerator", "closure"):
        from ..step_xml import parse_closure_generator_config
        parsed.update(parse_closure_generator_config(step_el))

    elif st in ("getslavesequence", "getidfromslaveserver", "getidfromslave"):
        from ..step_xml import parse_get_slave_sequence_config
        parsed.update(parse_get_slave_sequence_config(step_el))

    elif st in ("xslt", "xsltransformation", "xsltransform"):
        from ..step_xml import parse_xslt_config
        parsed.update(parse_xslt_config(step_el))

    elif st in ("sequence", "addsequence"):
        from ..step_xml import parse_sequence_config_dict
        parsed.update(parse_sequence_config_dict(step_el))

    elif st in ("textfileoutput", "textfileoutputlegacy"):
        parsed["filename"] = (
            step_el.findtext("file", "")
            or step_el.findtext("filename", "")
            or step.attributes.get("filename", "")
            or step.attributes.get("file", "")
        )
        # Do not invent a default delimiter — empty means missing.
        parsed["separator"] = (
            (step_el.findtext("separator") or "").strip()
            or step.attributes.get("separator", "")
        )
        parsed["header"] = step_el.findtext("header", "") or step.attributes.get("header", "")
        parsed["encoding"] = step_el.findtext("encoding", "") or step.attributes.get("encoding", "")
        parsed["compression"] = (
            step_el.findtext("compression", "") or step.attributes.get("compression", "")
        )

    elif st == "tableinput":
        parsed["sql"] = step_el.findtext("sql", "") or step.attributes.get("sql", "")
        parsed["connection"] = step_el.findtext("connection", "") or step.attributes.get("connection", "")
        parsed["schema"] = step_el.findtext("schema", "") or step.attributes.get("schema", "")
        parsed["table"] = step_el.findtext("table", "") or step.attributes.get("table", "")

    elif st == "csvinput":
        parsed["filename"] = step_el.findtext("filename", "") or step.attributes.get("filename", "")
        parsed["separator"] = step_el.findtext("separator", ",") or step.attributes.get("separator", ",")
        parsed["header"] = step_el.findtext("header", "Y")

    elif st == "sortrows":
        from ..step_xml import parse_sort_rows_config
        parsed.update(parse_sort_rows_config(step_el))

    elif st in (
        "unique", "uniquerows", "uniquerowsbyhashset",
        "uniquerowshashset", "uniquehashset",
    ):
        from ..step_xml import parse_unique_rows_config
        parsed.update(parse_unique_rows_config(step_el))

    elif st in ("rownormaliser", "rownormalizer", "normaliser"):
        from ..step_xml import parse_normaliser_config
        parsed.update(parse_normaliser_config(step_el))

    elif st in ("rowdenormaliser", "rowdenormalizer", "denormaliser"):
        from ..step_xml import parse_denormaliser_config
        parsed.update(parse_denormaliser_config(step_el))

    elif st in ("flattener", "rowflattener"):
        from ..step_xml import parse_flattener_config
        parsed.update(parse_flattener_config(step_el))

    elif st == "splitfieldtorows":
        from ..step_xml import parse_split_field_to_rows_config
        parsed.update(parse_split_field_to_rows_config(step_el))

    elif st in ("fieldsplitter", "splitfields"):
        from ..step_xml import parse_split_fields_config
        parsed.update(parse_split_fields_config(step_el))

    elif st in ("groupby", "memorygroupby"):
        group_keys, aggregates = parse_group_by_fields(step_el)
        parsed["group_keys"] = group_keys
        parsed["aggregates"] = [
            {"name": a.name, "aggregate": a.aggregate, "subject": a.subject or a.name}
            for a in aggregates
        ]

    elif st == "analyticquery":
        from ..step_xml import parse_analytic_query_config
        parsed.update(parse_analytic_query_config(step_el))

    elif st == "samplerows":
        from ..step_xml import parse_sample_rows_config
        parsed.update(parse_sample_rows_config(step_el))

    elif st == "reservoirsampling":
        from ..step_xml import parse_reservoir_sampling_config
        parsed.update(parse_reservoir_sampling_config(step_el))

    elif st in ("univariatestats", "univariatestatistics"):
        from ..step_xml import parse_univariate_stats_config
        parsed.update(parse_univariate_stats_config(step_el))

    elif st in ("stepsmetrics", "outputstepsmetrics"):
        from ..step_xml import parse_steps_metrics_config
        parsed.update(parse_steps_metrics_config(step_el))

    elif st == "tableoutput":
        parsed["schema"] = step_el.findtext("schema", "") or step.attributes.get("schema", "")
        parsed["table"] = step_el.findtext("table", "") or step.attributes.get("table", "")

    elif st == "stringoperations":
        parsed["operations"] = [
            {"in": o.in_stream_name, "out": o.out_stream_name}
            for o in parse_string_operation_fields(step_el)
        ]

    elif st == "formula":
        parsed["formula"] = step_el.findtext("formula", "") or step.attributes.get("formula", "")
        parsed["field_name"] = step_el.findtext("field_name", "") or step.attributes.get("field_name", "")

    elif st in ("mergejoin", "joinrows", "joiner", "streamlookup", "databaselookup", "mergerows", "mergerow"):
        parsed["join_keys"] = [{"left": k.left, "right": k.right} for k in parse_join_keys(step_el)]
        parsed["join_type"] = step_el.findtext("join_type", "") or step.attributes.get("join_type", "")

    elif st in ("csvoutput", "csvfileoutput"):
        parsed["filename"] = step_el.findtext("filename", "") or step.attributes.get("filename", "")
        parsed["separator"] = step_el.findtext("separator", ",") or step.attributes.get("separator", ",")
        parsed["header"] = step_el.findtext("header", "Y")

    elif st in ("insertupdate", "update", "delete"):
        parsed["schema"] = step_el.findtext("schema", "") or step.attributes.get("schema", "")
        parsed["table"] = step_el.findtext("table", "") or step.attributes.get("table", "")
        parsed["key_fields"] = [f.name for f in step.fields if f.name]

    elif st in ("execsql", "executesql", "sql"):
        parsed["sql"] = step_el.findtext("sql", "") or step.attributes.get("sql", "")

    elif st in ("jsoninput", "jsonoutput", "xmlinput", "getxmldata", "xmloutput", "xmlpad", "xmlwriter"):
        parsed["filename"] = (
            step_el.findtext("filename", "")
            or step_el.findtext("file", "")
            or step.attributes.get("filename", "")
        )

    elif st in ("parquetinput", "parquetfileinput", "parquetoutput", "parquetfileoutput",
                "orcinput", "orcfileinput", "orcoutput", "orcfileoutput"):
        parsed["filename"] = step_el.findtext("filename", "") or step_el.findtext("file", "") or step.attributes.get("filename", "")
        parsed["compression"] = step_el.findtext("compression", "") or step.attributes.get("compression", "")

    elif st in ("avroinput", "avrofileinput", "avrooutput", "avrofileoutput"):
        from ..step_xml import parse_avro_input_config, parse_avro_output_config

        if st in ("avroinput", "avrofileinput"):
            cfg = parse_avro_input_config(step_el) if step_el is not None else {}
        else:
            cfg = parse_avro_output_config(step_el) if step_el is not None else {}
        parsed.update(cfg)
        parsed["filename"] = cfg.get("filename") or step.attributes.get("filename", "")
        parsed["compression"] = cfg.get("compression") or step.attributes.get("compression", "")

    elif st in ("mongodbinput", "mongoinput", "mongodboutput", "mongooutput"):
        from ..step_xml import parse_mongodb_input_config, parse_mongodb_output_config

        if st in ("mongodboutput", "mongooutput"):
            cfg = parse_mongodb_output_config(step_el) if step_el is not None else {}
        else:
            cfg = parse_mongodb_input_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st in (
        "kafkaconsumer",
        "kafkaconsumerinput",
        "kafkastreaminput",
        "kafka",
        "kafkaproducer",
        "kafkaproduceroutput",
    ):
        from ..step_xml import parse_kafka_consumer_config, parse_kafka_producer_config

        if st in ("kafkaproducer", "kafkaproduceroutput"):
            cfg = parse_kafka_producer_config(step_el) if step_el is not None else {}
        else:
            cfg = parse_kafka_consumer_config(step_el) if step_el is not None else {}
        parsed.update(cfg)
        parsed["topic"] = cfg.get("topic") or step_el.findtext("topic", "") or step.attributes.get("topic", "")
        parsed["bootstrap_servers"] = (
            cfg.get("bootstrap_servers")
            or step_el.findtext("bootstrap_servers", "")
            or step.attributes.get("bootstrap_servers", "")
        )

    elif st in ("recordsfromstream", "getrecordsfromstream"):
        from ..step_xml import parse_records_from_stream_config

        cfg = parse_records_from_stream_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st in ("jmsconsumer", "jmsconsumerinput", "activemqconsumer"):
        from ..step_xml import parse_jms_consumer_config

        cfg = parse_jms_consumer_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st in ("jmsproducer", "jmsproduceroutput", "activemqproducer"):
        from ..step_xml import parse_jms_producer_config

        cfg = parse_jms_producer_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st in ("mqttconsumer", "mqttconsumerinput", "mqttclient"):
        from ..step_xml import parse_mqtt_consumer_config

        cfg = parse_mqtt_consumer_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st in ("mqttproducer", "mqttproduceroutput"):
        from ..step_xml import parse_mqtt_producer_config

        cfg = parse_mqtt_producer_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st == "injector":
        from ..step_xml import parse_injector_config

        cfg = parse_injector_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st == "socketreader":
        from ..step_xml import parse_socket_reader_config

        cfg = parse_socket_reader_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st == "socketwriter":
        from ..step_xml import parse_socket_writer_config

        cfg = parse_socket_writer_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st in (
        "gpbulkloader",
        "greenplumbulkloader",
        "greenplumload",
        "greenplumloader",
        "gpload",
        "infobrightloader",
        "infobrightbulkloader",
        "infobright",
        "vectorwisebulkloader",
        "ingresvectorwisebulkloader",
        "vwbulkloader",
        "ingresbulkloader",
        "vectorwiseloader",
        "monetdbbulkloader",
        "monetdbloader",
        "monetdbbulk",
        "mysqlbulkloader",
        "mysqlloader",
        "loaddatainfile",
        "orabulkloader",
        "oraclebulkloader",
        "oracleloader",
        "sqlldr",
        "pgbulkloader",
        "postgresqlbulkloader",
        "postgresbulkloader",
        "psqlbulkloader",
        "terafast",
        "teradatafastloadbulkloader",
        "terafastbulkloader",
        "teradatafastload",
        "fastload",
        "teradatabulkloader",
        "teradatatptbulkloader",
        "tptbulkloader",
        "teratpt",
        "teradatatpt",
        "verticabulkloader",
        "verticaloader",
        "verticacopy",
    ):
        from ..step_xml import parse_bulk_loader_config

        cfg = parse_bulk_loader_config(step_el) if step_el is not None else {}
        parsed.update(cfg)

    elif st in ("rest", "restclient", "http", "httppost", "httpget"):
        parsed["url"] = step_el.findtext("url", "") or step.attributes.get("url", "")

    return parsed


def _extract_condition_columns(root) -> list[str]:
    if root is None:
        return []
    cols: list[str] = []
    for el in root.iter():
        tag = el.tag.lower() if hasattr(el, "tag") else ""
        if tag == "leftvalue" and el.text:
            cols.append(el.text.strip())
        if tag == "rightvalue" and el.text:
            cols.append(el.text.strip())
    return cols


def register_builtin_validators() -> None:
    register_validator(RowGeneratorValidator())
    register_validator(ConstantValidator())
    register_validator(CalculatorValidator())
    register_validator(FilterRowsValidator())
    register_validator(SelectValuesValidator())
    register_validator(SetValueConstantValidator())
    register_validator(SetValueFieldValidator())
    register_validator(ConcatFieldsValidator())
    register_validator(AddXmlValidator())
    register_validator(TextFileOutputValidator())
    register_validator(TableInputValidator())
    register_validator(CsvInputValidator())
    register_validator(SortRowsValidator())
    register_validator(GroupByValidator())
    register_validator(TableOutputValidator())
    register_validator(UniqueValidator())
    register_validator(SequenceValidator())
    register_validator(RankValidator())
    register_validator(ValueMapperValidator())
    register_validator(SystemInfoValidator())
    register_validator(AbortValidator())
    from .validators_extended import register_extended_validators
    register_extended_validators()
    register_validator(GenericStepValidator())
