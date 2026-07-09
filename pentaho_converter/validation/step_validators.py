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
    step_types = frozenset({"constant"})

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
        calcs = parsed.get("calculations", [])

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
            result.errors.append("No calculations found in XML.")
            result.score = 0.1
            return result

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

        if ".select(" not in code and ".selectExpr(" not in code:
            result.errors.append("SelectValues must generate a select operation.")
            result.score = 0.3
            return result

        for col in parsed.get("select_columns", []):
            if col in code:
                result.properties_converted.append(f"column:{col}")
                result.output_columns.append(col)
            else:
                result.warnings.append(f"Selected column '{col}' not found in generated select.")

        result.score = _score(result)
        return result


class TextFileOutputValidator(StepValidator):
    step_types = frozenset({"textfileoutput"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = SemanticValidationResult()
        syntax_ok, errors = validate_python_fragment(code_lines)
        result.syntax_valid = syntax_ok
        result.errors.extend(errors)
        code = "\n".join(code_lines)

        if ".write." not in code:
            result.errors.append("TextFileOutput must generate a write operation.")
            result.score = 0.2
            return result

        checks = {
            "filename": parsed.get("filename", ""),
            "delimiter": parsed.get("separator", ","),
            "header": parsed.get("header"),
            "encoding": parsed.get("encoding"),
            "compression": parsed.get("compression"),
        }
        for prop, val in checks.items():
            if val is None or val == "":
                result.properties_missing.append(prop)
                continue
            if prop == "filename" and str(val) in code:
                result.properties_converted.append(prop)
            elif prop == "delimiter" and str(val) in code:
                result.properties_converted.append(prop)
            elif prop == "header" and "header" in code:
                result.properties_converted.append(prop)
            elif prop in ("encoding", "compression") and str(val) in code:
                result.properties_converted.append(prop)
            else:
                result.warnings.append(f"Output property '{prop}' may differ from Pentaho XML.")

        result.score = _score(result)
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
        for name, _asc in parsed.get("sort_fields", []):
            if name not in code:
                result.warnings.append(f"Sort column '{name}' not found in generated orderBy.")
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
        if "raise RuntimeError" not in code:
            result.errors.append("Abort must raise RuntimeError to stop the pipeline.")
        else:
            result.properties_converted.append("abort_raise")
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


def _score(result: SemanticValidationResult) -> float:
    if result.errors:
        base = max(0.0, 0.5 - 0.1 * len(result.errors))
    else:
        base = 1.0
    base -= 0.05 * len(result.warnings)
    base -= 0.05 * len(result.properties_missing)
    if not result.syntax_valid:
        base = min(base, 0.3)
    return max(0.0, min(1.0, base))


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

    st = step.step_type.lower()
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
        from ..transformation_parser import _parse_fields
        flds = _parse_fields(step_el) if step_el is not None else list(step.fields)
        parsed["select_columns"] = [f.name for f in flds if f.name]
        parsed["output_columns"] = [(f.rename or f.name) for f in flds if f.name]

    elif st == "textfileoutput":
        parsed["filename"] = (
            step_el.findtext("file", "")
            or step_el.findtext("filename", "")
            or step.attributes.get("filename", "")
            or step.attributes.get("file", "")
        )
        parsed["separator"] = step_el.findtext("separator", ",") or step.attributes.get("separator", ",")
        parsed["header"] = step_el.findtext("header", "Y")
        parsed["encoding"] = step_el.findtext("encoding", "")
        parsed["compression"] = step_el.findtext("compression", "")

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
        parsed["sort_fields"] = parse_sort_fields(step_el)

    elif st == "groupby":
        group_keys, aggregates = parse_group_by_fields(step_el)
        parsed["group_keys"] = group_keys
        parsed["aggregates"] = [
            {"name": a.name, "aggregate": a.aggregate, "subject": a.subject or a.name}
            for a in aggregates
        ]

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
                "orcinput", "orcfileinput", "orcoutput", "orcfileoutput",
                "avroinput", "avrofileinput", "avrooutput", "avrofileoutput"):
        parsed["filename"] = step_el.findtext("filename", "") or step_el.findtext("file", "") or step.attributes.get("filename", "")
        parsed["compression"] = step_el.findtext("compression", "") or step.attributes.get("compression", "")

    elif st in ("kafkaconsumerinput", "kafkastreaminput", "kafka", "kafkaproduceroutput"):
        parsed["topic"] = step_el.findtext("topic", "") or step.attributes.get("topic", "")
        parsed["bootstrap_servers"] = step_el.findtext("bootstrap_servers", "") or step.attributes.get("bootstrap_servers", "")

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
