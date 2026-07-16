"""Extended semantic validators for additional Pentaho step types."""

from __future__ import annotations

from ..validation.base import SemanticValidationResult, StepValidator
from ..validation.registry import register_validator
from .step_validators import _score, _syntax_result


class _PatternValidator(StepValidator):
    """Validator that checks generated code contains expected PySpark patterns."""

    def __init__(
        self,
        step_types: frozenset[str],
        *,
        requires_any: tuple[str, ...] = (),
        requires_all: tuple[str, ...] = (),
        error_message: str = "Generated code missing expected transformation logic.",
        converted_label: str = "logic",
        warn_passthrough: bool = True,
    ) -> None:
        self.step_types = step_types
        self.requires_any = requires_any
        self.requires_all = requires_all
        self.error_message = error_message
        self.converted_label = converted_label
        self.warn_passthrough = warn_passthrough

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        in_df = context.input_df_name()
        out_var = context.output_df_name()

        if self.warn_passthrough and in_df and f"{out_var} = {in_df}" in code and len(code_lines) <= 3:
            result.warnings.append("Step may be a passthrough without full transformation logic.")

        if self.requires_all and not all(p in code for p in self.requires_all):
            result.errors.append(self.error_message)
        elif self.requires_any and not any(p in code for p in self.requires_any):
            result.errors.append(self.error_message)
        else:
            result.properties_converted.append(self.converted_label)

        for col in parsed.get("output_columns", []):
            if col and col in code:
                result.output_columns.append(col)

        result.score = _score(result)
        return result


class FormulaValidator(StepValidator):
    step_types = frozenset({"formula"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        field_name = parsed.get("field_name", "")
        formula = parsed.get("formula", "")

        if "withColumn" not in code:
            result.errors.append("Formula must generate withColumn() with converted expression.")
        else:
            result.properties_converted.append("formula_expression")
            if field_name and field_name not in code:
                result.warnings.append(f"Formula output field '{field_name}' not found in generated code.")
            if formula and formula not in code and "expr(" not in code and "col(" not in code:
                result.warnings.append("Formula expression may not match Pentaho XML.")

        result.score = _score(result)
        return result


class MergeJoinValidator(StepValidator):
    step_types = frozenset({"mergejoin"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        keys = parsed.get("join_keys", [])
        if keys:
            if ".join(" not in code and "crossJoin" not in code:
                result.errors.append("MergeJoin must generate a .join() or crossJoin() call.")
            else:
                result.properties_converted.append("join")
        elif "no join keys" in code.lower():
            result.warnings.append("MergeJoin has no join keys defined in XML — join not generated.")
        elif ".join(" in code:
            result.errors.append("MergeJoin must not use keyless join() without cross join type.")
        else:
            result.warnings.append("MergeJoin has no join keys and no join was generated.")
        result.score = _score(result)
        return result


class JoinRowsValidator(StepValidator):
    step_types = frozenset({"joinrows", "joiner"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "crossJoin" not in code and ".join(" not in code:
            result.errors.append(
                "JoinRows must generate a crossJoin() (Cartesian product) call."
            )
        else:
            result.properties_converted.append("cartesian_join")
        if "Cartesian" in code or "crossJoin" in code or "expensive" in code.lower():
            result.warnings.append(
                "JoinRows Cartesian products can cause severe performance issues."
            )
        if parsed.get("directory") or parsed.get("cache_size"):
            result.properties_converted.append("cache_options")
        result.score = _score(result)
        return result


class MergeRowsValidator(StepValidator):
    step_types = frozenset({"mergerows", "mergerow"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if ".join(" not in code and "unionByName" not in code:
            result.errors.append("MergeRows must generate join or union logic.")
        else:
            result.properties_converted.append("merge_rows")
        flag = parsed.get("flag_field") or "flagfield"
        if flag and flag not in code and "withColumn" not in code:
            result.warnings.append("MergeRows flag field may be missing from generated code.")
        for token in ("deleted", "new", "identical"):
            if token not in code:
                result.warnings.append(f"MergeRows should emit '{token}' flag indicator.")
                break
        if parsed.get("value_fields") and "changed" not in code:
            result.warnings.append(
                "MergeRows has compare/value fields but no 'changed' flag logic."
            )
        result.score = _score(result)
        return result


class MultiwayMergeJoinValidator(StepValidator):
    step_types = frozenset({"multimergejoin", "multiwaymergejoin", "multimerge"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if ".join(" not in code and "crossJoin" not in code:
            result.errors.append("MultiwayMergeJoin must generate chained join() calls.")
        else:
            result.properties_converted.append("multiway_join")
        if not (parsed.get("key_fields") or parsed.get("keys")):
            result.warnings.append("MultiwayMergeJoin has no key_fields in metadata.")
        result.score = _score(result)
        return result


class SortedMergeValidator(StepValidator):
    step_types = frozenset({"sortedmerge"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "unionByName" not in code and "orderBy" not in code and "union(" not in code:
            result.errors.append(
                "SortedMerge must generate union/merge logic with ordering."
            )
        else:
            result.properties_converted.append("sorted_merge")
        if parsed.get("sort_fields") and "orderBy" not in code:
            result.warnings.append("SortedMerge has sort fields but no orderBy().")
        result.score = _score(result)
        return result


class XMLJoinValidator(StepValidator):
    step_types = frozenset({"xmljoin"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "collect_list" not in code and "concat" not in code:
            result.errors.append(
                "XMLJoin must aggregate/concat source XML fragments into the target."
            )
        else:
            result.properties_converted.append("xml_join")
        if parsed.get("complex_join") or parsed.get("target_xpath"):
            result.warnings.append(
                "XMLJoin XPath/complex join is only approximated in Spark."
            )
        result.score = _score(result)
        return result


class StreamLookupValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"streamlookup"}),
            requires_any=(".join(", "broadcast("),
            error_message="StreamLookup must generate a broadcast join.",
            converted_label="lookup_join",
        )


class DatabaseLookupValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"databaselookup", "dblookup"}),
            requires_any=("spark.table(", ".join(", "broadcast("),
            error_message="DatabaseLookup must read lookup table and join.",
            converted_label="db_lookup",
        )


class DimensionLookupValidator(StepValidator):
    """Validate Dimension Lookup/Update codegen (MERGE/join or intentional partial)."""

    step_types = frozenset({"dimensionlookup", "dimensionlookupupdate"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        keys = parsed.get("keys") or parsed.get("join_keys") or []
        if not keys:
            if "missing business keys" in code.lower() or "lit(None)" in code:
                result.warnings.append(
                    "DimensionLookup missing business keys — emitted partial null-TK path."
                )
                result.properties_converted.append("dim_lookup_partial")
            else:
                result.errors.append(
                    "DimensionLookup without keys must emit partial null technical-key path."
                )
        else:
            ok = any(
                p in code
                for p in (
                    "MERGE INTO",
                    "DeltaTable",
                    ".merge(",
                    "whenMatchedUpdate",
                    "whenNotMatchedInsert",
                    ".join(",
                    "spark.table(",
                )
            )
            if not ok:
                result.errors.append(
                    "DimensionLookup must join dimension data or use Delta MERGE."
                )
            else:
                result.properties_converted.append("dim_lookup")
            if parsed.get("update") is not False and "update" in str(parsed.get("update", True)).lower():
                # Update mode should emit MERGE when possible
                has_merge = any(
                    p in code
                    for p in ("MERGE INTO", "DeltaTable", ".merge(", "whenMatchedUpdate")
                )
                if not has_merge and "lookup-only" not in code.lower():
                    # may still be lookup path when update=N
                    pass
            if "PunchThrough" in code or "PunchThrough" in str(parsed.get("fields")):
                if any(
                    (isinstance(f, dict) and f.get("update_type") == "PunchThrough")
                    for f in (parsed.get("fields") or [])
                ):
                    has_merge = any(
                        p in code for p in ("MERGE INTO", "DeltaTable", ".merge(", "whenMatchedUpdate")
                    )
                    if "PunchThrough" in code and has_merge:
                        result.properties_converted.append("punch_through")
        for col in parsed.get("output_columns", []):
            if col and col in code:
                result.output_columns.append(col)
        tk = parsed.get("technical_key_rename") or parsed.get("technical_key")
        if tk and tk in code:
            result.output_columns.append(tk)
        result.score = _score(result)
        return result


class CombinationLookupValidator(StepValidator):
    """Validate Combination Lookup/Update codegen (MERGE/SK or intentional partial)."""

    step_types = frozenset({"combinationlookup"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        keys = parsed.get("keys") or parsed.get("join_keys") or []
        if not keys:
            if "missing business keys" in code.lower() or "lit(None)" in code:
                result.warnings.append(
                    "CombinationLookup missing business keys — emitted partial null-TK path."
                )
                result.properties_converted.append("combo_lookup_partial")
            else:
                result.errors.append(
                    "CombinationLookup without keys must emit partial null technical-key path."
                )
        else:
            ok = any(
                p in code
                for p in (
                    "MERGE INTO",
                    "DeltaTable",
                    ".merge(",
                    "whenNotMatchedInsert",
                    "row_number",
                    "monotonically_increasing_id",
                    ".join(",
                )
            )
            if not ok:
                result.errors.append(
                    "CombinationLookup must MERGE or generate surrogate keys."
                )
            else:
                result.properties_converted.append("combo_lookup")
        tk = parsed.get("technical_key")
        if tk and tk in code:
            result.output_columns.append(tk)
        result.score = _score(result)
        return result


class ReplaceNullValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"replacenull"}),
            requires_any=("when(", "isNull(", "coalesce("),
            error_message="ReplaceNull must generate null-replacement logic.",
            converted_label="replace_null",
        )


class IfNullValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"ifnull", "iffieldvaluenull", "iffieldvalueisnull"}),
            requires_any=("when(", "isNull(", "coalesce("),
            error_message="IfNull must generate null-handling logic.",
            converted_label="if_null",
        )


class StringOperationsValidator(StepValidator):
    step_types = frozenset({
        "stringoperations",
        "stringcut",
        "stringscut",
        "replaceinstring",
        "regexreplace",
    })

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        ops = parsed.get("operations") or parsed.get("fields") or []

        string_fns = (
            "upper(", "lower(", "trim(", "ltrim(", "rtrim(", "initcap(",
            "substring(", "regexp_replace(", "replace(", "concat(",
            "lpad(", "rpad(",
        )
        if not any(fn in code for fn in string_fns) and "withColumn" not in code:
            result.errors.append("StringOperations must apply string functions via withColumn().")
        else:
            result.properties_converted.append("string_ops")
        for op in ops:
            if not isinstance(op, dict):
                continue
            out_name = (
                op.get("out")
                or op.get("out_stream_name")
                or op.get("in")
                or op.get("in_stream_name")
            )
            if out_name and out_name in code:
                result.output_columns.append(out_name)
            elif out_name:
                result.warnings.append(f"Output column '{out_name}' not found in generated code.")
        result.score = _score(result)
        return result


class CsvOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"csvoutput", "csvfileoutput"}),
            requires_any=("format('csv')", 'format("csv")'),
            requires_all=(".save(",),
            error_message="CSVOutput must write CSV with format('csv').",
            converted_label="csv_write",
        )


class ExcelInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"excelinput", "microsoftexcelinput"}),
            requires_any=("format('com.crealytics.spark.excel')", ".load("),
            error_message="ExcelInput must read Excel format.",
            converted_label="excel_read",
        )


class ExcelOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"exceloutput", "microsoftexceloutput"}),
            requires_any=(
                "format('com.crealytics.spark.excel')",
                'format("com.crealytics.spark.excel")',
                "TODO:",
            ),
            error_message="Excel Output must emit spark-excel write or TODO guidance.",
            converted_label="excel_output",
            warn_passthrough=False,
        )

    def validate(self, context, parsed, code_lines):
        from ..validation.step_validators import _score

        result = super().validate(context, parsed, code_lines)
        code = "\n".join(code_lines)
        if "WARNING" in code or "PARTIAL" in code:
            if "filename missing" in code.lower() or "workbook" in code.lower():
                result.warnings.append("Excel workbook/filename missing or incomplete.")
                result.properties_missing.append("filename")
            if "encoding missing" in code.lower():
                result.warnings.append("Excel encoding missing from Pentaho metadata.")
                result.properties_missing.append("encoding")
            if "formatting" in code.lower() or "template" in code.lower():
                result.warnings.append("Excel formatting/template options preserved as comments.")
                result.properties_missing.append("formatting")
            result.score = min(_score(result), 0.85)
        else:
            result.score = _score(result)
        return result


class ExcelWriterValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"excelwriter", "typeexcelwriter", "microsoftexcelwriter"}),
            requires_any=(
                "format('com.crealytics.spark.excel')",
                'format("com.crealytics.spark.excel")',
                "TODO:",
            ),
            error_message="Excel Writer must emit spark-excel write (shared with Excel Output) or TODO.",
            converted_label="excel_writer",
            warn_passthrough=False,
        )


class TextFileInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"textfileinput", "oldtextfileinput"}),
            requires_any=(".read.text(", ".load(", ".csv(", 'format("csv")', "format('csv')"),
            error_message="TextFileInput must read a text/CSV file.",
            converted_label="text_read",
        )

    def validate(self, context, parsed, code_lines):
        from ..validation.step_validators import _collect_comment_levels, _score

        result = super().validate(context, parsed, code_lines)
        code = "\n".join(code_lines)
        infos, comment_warnings = _collect_comment_levels(code)
        result.infos.extend(infos)

        # Spark CSV outputs are directories — matching write/read paths are valid.
        if "Spark CSV outputs are directories" in code or "Spark CSV" in code:
            result.infos.append(
                "Spark CSV outputs are directories — matching save/load paths are valid."
            )

        code_lower = code.lower()
        # Incomplete executable metadata → WARNING; locale/formatting → INFO only.
        if "filename missing" in code_lower or (
            "<input_file>" in code and "filename missing" in code_lower
        ):
            result.warnings.append("Input filename missing or placeholder used.")
            result.properties_missing.append("filename")
        if "delimiter missing" in code_lower:
            result.warnings.append("Input delimiter missing from Pentaho metadata.")
            result.properties_missing.append("delimiter")
        if "encoding missing" in code_lower:
            result.infos.append("Input encoding missing — Spark uses the platform/default encoding.")
        if "schema unavailable" in code_lower:
            result.infos.append("Schema unavailable — inferSchema used; review recommended.")
        if "fixed-width" in code_lower:
            result.warnings.append("Fixed-width input requires manual review.")

        for msg in comment_warnings:
            lowered = msg.lower()
            if any(
                token in lowered
                for token in (
                    "date_format_locale",
                    "date_format_lenient",
                    "field_format",
                    "currency",
                    "grouping",
                    "decimal",
                    "repeat",
                    "shortfilefieldname",
                    "pathfieldname",
                    "hiddenfieldname",
                    "sizefieldname",
                    "urinamefieldname",
                    "extensionfieldname",
                    "legacy text file input option",
                    "file metadata field",
                )
            ):
                result.infos.append(msg)
            elif "filename missing" in lowered or "delimiter missing" in lowered:
                continue
            elif "encoding missing" in lowered or "schema unavailable" in lowered:
                continue
            else:
                result.warnings.append(msg)

        if "preserved.field_format" in code or "date_format_locale" in code_lower:
            result.infos.append("Legacy Text File Input locale/formatting metadata preserved.")

        if result.errors and (".csv(" in code or ".load(" in code or ".text(" in code):
            result.errors = [e for e in result.errors if "must read" not in e.lower()]

        result.score = _score(result)
        executable_gap = (
            "filename" in result.properties_missing
            or "delimiter" in result.properties_missing
            or any("fixed-width" in w.lower() for w in result.warnings)
        )
        # Only cap when executable Spark behavior is actually incomplete.
        if executable_gap:
            result.score = min(result.score, 0.85)
        return result


class InsertUpdateValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"insertupdate"}),
            requires_any=("MERGE INTO", "saveAsTable(", "append"),
            error_message="InsertUpdate must generate MERGE or append write.",
            converted_label="upsert",
        )


class UpdateValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"update"}),
            requires_any=("MERGE INTO", "saveAsTable(", "overwrite"),
            error_message="Update must generate MERGE or overwrite write.",
            converted_label="update",
        )


class DeleteValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"delete"}),
            requires_any=("MERGE INTO", "DELETE"),
            error_message="Delete must generate MERGE DELETE logic.",
            converted_label="delete",
            warn_passthrough=False,
        )


class ExecuteSQLValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"execsql", "executesql", "sql"}),
            requires_any=("spark.sql(", "preserved.sql="),
            error_message="ExecuteSQL must call spark.sql() or preserve SQL metadata.",
            converted_label="execute_sql",
            warn_passthrough=False,
        )


class ExecSQLRowValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"execsqlrow", "executerowsqlscript", "executerowsql"}),
            requires_any=("spark.sql(", "toLocalIterator", "WARNING"),
            error_message="ExecSQLRow must emit per-row SQL with scale warnings.",
            converted_label="execute_sql_row",
            warn_passthrough=False,
        )


class JsonInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"jsoninput"}),
            requires_any=("format('json')", ".load("),
            error_message="JSONInput must read JSON format.",
            converted_label="json_read",
        )


class JsonOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"jsonoutput"}),
            requires_any=(".json(", "format('json')", 'format("json")'),
            error_message="JSONOutput must write JSON format.",
            converted_label="json_write",
        )


class XmlInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"getxmldata", "xmlinput"}),
            requires_any=("format('xml')", ".load("),
            error_message="XMLInput must read XML format.",
            converted_label="xml_read",
        )


class XmlOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"xmloutput", "xmlpad", "xmlwriter"}),
            requires_any=("format('xml')", ".save("),
            error_message="XMLOutput must write XML format.",
            converted_label="xml_write",
        )


class ParquetInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"parquetinput", "parquetfileinput"}),
            requires_any=("parquet(", "format('parquet')"),
            error_message="ParquetInput must read Parquet format.",
            converted_label="parquet_read",
        )


class ParquetOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"parquetoutput", "parquetfileoutput"}),
            requires_any=(".parquet(", "format('parquet')", 'format("parquet")'),
            error_message="ParquetOutput must write Parquet format.",
            converted_label="parquet_write",
        )


class DeltaFileOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"deltaoutput", "deltafileoutput", "writeoutdelta"}),
            requires_any=('format("delta")', "format('delta')"),
            error_message="Delta File Output must write Delta format with .save().",
            converted_label="delta_file_write",
        )


class OrcInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"orcinput", "orcfileinput"}),
            requires_any=("format('orc')", ".load("),
            error_message="ORCInput must read ORC format.",
            converted_label="orc_read",
        )


class OrcOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"orcoutput", "orcfileoutput"}),
            requires_any=("format('orc')", ".save("),
            error_message="ORCOutput must write ORC format.",
            converted_label="orc_write",
        )


class AvroInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"avroinput", "avrofileinput"}),
            requires_any=("format('avro')", ".load("),
            error_message="AvroInput must read Avro format.",
            converted_label="avro_read",
        )


class AvroOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"avrooutput", "avrofileoutput"}),
            requires_any=("format('avro')", ".save("),
            error_message="AvroOutput must write Avro format.",
            converted_label="avro_write",
        )


class MongoDBInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"mongodbinput", "mongoinput"}),
            requires_any=("format('mongodb')", 'format("mongodb")'),
            error_message="MongoDB Input must use the Spark MongoDB connector format.",
            converted_label="mongodb_read",
            warn_passthrough=False,
        )


class MongoDBOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"mongodboutput", "mongooutput"}),
            requires_any=("format('mongodb')", 'format("mongodb")'),
            error_message="MongoDB Output must use the Spark MongoDB connector format.",
            converted_label="mongodb_write",
            warn_passthrough=False,
        )


class KafkaValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({
                "kafkaconsumer",
                "kafkaconsumerinput",
                "kafkastreaminput",
                "kafka",
                "kafkaproducer",
                "kafkaproduceroutput",
            }),
            requires_any=("format('kafka')", 'format("kafka")'),
            error_message="Kafka step must use spark Kafka format (readStream/writeStream).",
            converted_label="kafka",
        )


class RecordsFromStreamValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"recordsfromstream", "getrecordsfromstream"}),
            requires_any=("Get Records from Stream",),
            error_message="Get Records from Stream must emit labeled pass-through or schema code.",
            converted_label="records_from_stream",
            warn_passthrough=False,
        )


class JmsStreamingValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({
                "jmsconsumer",
                "jmsconsumerinput",
                "activemqconsumer",
                "jmsproducer",
                "jmsproduceroutput",
                "activemqproducer",
            }),
            requires_any=("# UNSUPPORTED", "preserved."),
            error_message="JMS step must preserve metadata and document unsupported Databricks mapping.",
            converted_label="jms_streaming",
            warn_passthrough=False,
        )


class MqttStreamingValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({
                "mqttconsumer",
                "mqttconsumerinput",
                "mqttclient",
                "mqttproducer",
                "mqttproduceroutput",
            }),
            requires_any=("# UNSUPPORTED", "# WARNING", "preserved."),
            error_message="MQTT step must preserve metadata and document migration constraints.",
            converted_label="mqtt_streaming",
            warn_passthrough=False,
        )


class InjectorValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"injector"}),
            requires_any=("createDataFrame(", "Injector"),
            error_message="Injector must generate createDataFrame() with preserved schema.",
            converted_label="injector",
            warn_passthrough=False,
        )


class SocketReaderValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"socketreader"}),
            requires_any=("format('socket')", 'format("socket")', "# WARNING", "# UNSUPPORTED"),
            error_message="Socket Reader must emit Structured Streaming socket source or migration warnings.",
            converted_label="socket_reader",
            warn_passthrough=False,
        )


class SocketWriterValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"socketwriter"}),
            requires_any=("# UNSUPPORTED", "preserved.", "# WARNING"),
            error_message="Socket Writer must preserve metadata and document unsupported Databricks mapping.",
            converted_label="socket_writer",
            warn_passthrough=False,
        )


class MemoryGroupByValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"memorygroupby"}),
            requires_any=(".groupBy(", "Window", ".agg("),
            error_message="MemoryGroupBy must generate groupBy()/agg() or window aggregation.",
            converted_label="group_by",
        )


class AnalyticQueryValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"analyticquery"}),
            requires_any=("Window", "lag(", "lead(", "over("),
            error_message="AnalyticQuery must use Spark Window lead/lag (or ranking) functions.",
            converted_label="analytic",
        )


class SampleRowsValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"samplerows"}),
            requires_any=(".filter(", ".sample(", ".limit(", "row_number"),
            error_message="SampleRows must filter line ranges or use sample()/limit().",
            converted_label="sample_rows",
        )


class ReservoirSamplingValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"reservoirsampling"}),
            requires_any=("rand(", ".sample(", ".limit("),
            error_message="ReservoirSampling must use rand()/sample()/limit().",
            converted_label="reservoir",
        )


class UnivariateStatsValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"univariatestats", "univariatestatistics"}),
            requires_any=(".agg(", "stddev", "percentile", "approxQuantile", "avg("),
            error_message="UnivariateStats must emit statistical aggregates.",
            converted_label="univariate",
        )


class StepsMetricsValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"stepsmetrics", "outputstepsmetrics"}),
            requires_any=("count(", "createDataFrame", "preserved."),
            error_message="StepsMetrics must collect counts and preserve metric metadata.",
            converted_label="steps_metrics",
            warn_passthrough=False,
        )


class SwitchCaseValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"switchcase"}),
            requires_any=("when(",),
            error_message="SwitchCase must generate when() expressions.",
            converted_label="switch_case",
        )


class AppendStreamsValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"append", "appendstreams"}),
            requires_any=("unionByName(", "union(", "# Append Streams"),
            error_message="Append Streams must generate unionByName/union or preserve stream metadata.",
            converted_label="append_union",
            warn_passthrough=False,
        )


class IdentifyLastRowValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"identifylastrow", "identifylastrowinastream"}),
            requires_any=("row_number(", "Window"),
            error_message="Identify Last Row must use window row_number.",
            converted_label="last_row_flag",
        )


class JavaFilterValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"javafilter"}),
            requires_any=(".filter(", "UNSUPPORTED Java", "migration required"),
            error_message="Java Filter must generate a .filter() call or preserve unsupported Java.",
            converted_label="java_filter",
            warn_passthrough=False,
        )


class PrioritizeStreamsValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"prioritystream", "prioritizestreams"}),
            requires_any=("unionByName(", "union(", "# Prioritize Streams"),
            error_message="Prioritize Streams must merge inputs in priority order.",
            converted_label="prioritize_merge",
            warn_passthrough=False,
        )


class JavaScriptValueValidator(StepValidator):
    step_types = frozenset({"scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "WARNING" in code or "original JavaScript" in code or "--- JS" in code:
            result.properties_converted.append("javascript_preserved")
        if "withColumn" in code or "expr(" in code:
            result.properties_converted.append("javascript")
            result.warnings.append("JavaScript logic converted approximately — verify semantics.")
            result.score = 0.7
        else:
            result.errors.append("JavaScript value step requires manual review or expr() conversion.")
            result.score = 0.3
        result.score = _score(result)
        return result


class UserDefinedJavaClassValidator(StepValidator):
    step_types = frozenset({"userdefinedjavaclass"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        result.warnings.append("User-defined Java Class requires manual intervention.")
        if "WARNING" in code and ("class_source" in code or "Java class" in code or "UDJC" in code):
            result.properties_converted.append("udjc_preserved")
            result.score = 0.45
        else:
            result.score = 0.3
        result.score = _score(result)
        return result


class UserDefinedJavaExpressionValidator(StepValidator):
    step_types = frozenset({"userdefinedjavaexpression"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "withColumn" in code and "WARNING" not in code:
            result.properties_converted.append("udje")
            result.score = 0.9
        elif "withColumn" in code:
            result.properties_converted.append("udje")
            result.warnings.append("Some Java expressions preserved with warnings.")
            result.score = 0.65
        else:
            result.warnings.append("User-defined Java Expression requires review.")
            result.score = 0.3
        result.score = _score(result)
        return result


class UserDefinedJavaValidator(StepValidator):
    """Backward-compatible combined validator (class + expression)."""

    step_types = frozenset({"userdefinedjavaclass", "userdefinedjavaexpression"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        st = (getattr(context.step, "step_type", "") or "").lower().replace(" ", "")
        if st == "userdefinedjavaexpression":
            return UserDefinedJavaExpressionValidator().validate(context, parsed, code_lines)
        return UserDefinedJavaClassValidator().validate(context, parsed, code_lines)


class RestHttpValidator(StepValidator):
    step_types = frozenset({
        "rest", "restclient", "http", "httppost", "httpget", "httpclient",
    })

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "requests." in code or "requests.request" in code:
            result.properties_converted.append("http_requests")
            if "WARNING" in code or "toLocalIterator" in code:
                result.warnings.append(
                    "REST/HTTP migration uses driver-side requests; review auth secrets and scale."
                )
                result.score = 0.75
            else:
                result.score = 0.85
        elif "createDataFrame" in code:
            result.warnings.append("REST/HTTP steps require external API integration — partial support.")
            result.properties_converted.append("http_stub")
            result.score = 0.55
        else:
            result.warnings.append("REST/HTTP steps require external API integration — partial support.")
            result.score = 0.35
        result.score = _score(result)
        return result


class UnsupportedInputValidator(StepValidator):
    """Validator for Input steps with no Databricks equivalent."""

    def __init__(self, step_types: frozenset[str]) -> None:
        self.step_types = step_types

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "UNSUPPORTED" in code:
            result.warnings.append(
                f"Input step '{context.step.step_type}' has no Databricks equivalent; "
                "metadata preserved and empty frame emitted."
            )
            result.properties_converted.append("metadata_preserved")
            result.score = 0.5
        else:
            result.errors.append("Unsupported input must document UNSUPPORTED and preserve metadata.")
            result.score = 0.3
        result.score = _score(result)
        return result


class UnsupportedOutputValidator(StepValidator):
    """Validator for Output steps with no Databricks equivalent."""

    def __init__(self, step_types: frozenset[str]) -> None:
        self.step_types = step_types

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "UNSUPPORTED" in code:
            result.warnings.append(
                f"Output step '{context.step.step_type}' has no Databricks equivalent; "
                "metadata preserved and write skipped."
            )
            result.properties_converted.append("metadata_preserved")
            result.score = 0.5
        else:
            result.errors.append("Unsupported output must document UNSUPPORTED and preserve metadata.")
            result.score = 0.3
        result.score = _score(result)
        return result


class GetSlaveSequenceValidator(StepValidator):
    """Carte slave sequences are not portable — require UNSUPPORTED documentation."""

    step_types = frozenset({"getslavesequence", "getidfromslaveserver", "getidfromslave"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "UNSUPPORTED" not in code:
            result.errors.append(
                "Get ID from Slave Server must document UNSUPPORTED Carte behavior."
            )
            result.score = 0.3
            return result

        result.warnings.append(
            "Get ID from Slave Server has no Carte equivalent on Databricks; "
            "configuration preserved; local IDs are an approximation only."
        )
        result.properties_converted.append("metadata_preserved")
        if "withColumn" in code or "row_number" in code:
            result.properties_converted.append("local_sequence_approx")
        # Cap below 0.95 so derive_status reports partial (not fully converted).
        result.score = 0.55
        return result


class XsltTransformValidator(StepValidator):
    """XSL Transformation — lxml UDF path is partial relative to JAXP/SAXON."""

    step_types = frozenset({"xslt", "xsltransformation", "xsltransform"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if (
            "preserved.xslfilename" not in code
            and "lxml" not in code
            and "etree.XSLT" not in code
        ):
            result.errors.append(
                "XSLT must emit transform logic or preserve stylesheet metadata."
            )
            result.score = 0.3
            return result

        result.properties_converted.append("xslt_metadata")
        if "lxml" in code or "etree.XSLT" in code:
            result.properties_converted.append("lxml_udf")
        if "WARNING" in code or "SAXON" in code or "invalid/missing XSL" in code:
            result.warnings.append(
                "XSLT migration is partial (lxml/libxslt; SAXON/output properties limited)."
            )
            result.score = 0.65
        else:
            result.score = 0.85
        if "WARNING" in code or "SAXON" in code:
            result.score = min(result.score, 0.7)
        return result


def register_extended_validators() -> None:
    """Register validators for the extended step batch."""
    validators: list[StepValidator] = [
        FormulaValidator(),
        MergeJoinValidator(),
        JoinRowsValidator(),
        MergeRowsValidator(),
        MultiwayMergeJoinValidator(),
        SortedMergeValidator(),
        XMLJoinValidator(),
        StreamLookupValidator(),
        DatabaseLookupValidator(),
        _PatternValidator(
            frozenset({"dbjoin", "databasejoin"}),
            requires_any=("spark.sql(", ".join("),
            error_message="DatabaseJoin must execute lookup SQL and join.",
            converted_label="db_join",
        ),
        _PatternValidator(
            frozenset({"dbproc", "calldbproc", "calldbprocedure"}),
            requires_any=("CALL", "callproc", "preserved.procedure"),
            error_message="Call DB Procedure must preserve procedure metadata.",
            converted_label="db_proc",
        ),
        _PatternValidator(
            frozenset({"dynamicsqlrow"}),
            requires_any=("spark.sql(", "toLocalIterator"),
            error_message="DynamicSQLRow must execute runtime SQL.",
            converted_label="dynamic_sql",
        ),
        _PatternValidator(
            frozenset({"fileexists"}),
            requires_any=("_file_exists", "dbutils.fs", "os.path.exists"),
            error_message="FileExists must check filesystem paths.",
            converted_label="file_exists",
        ),
        _PatternValidator(
            frozenset({"tableexists"}),
            requires_any=("tableExists", "spark.catalog", "_table_exists"),
            error_message="TableExists must inspect catalog/tables.",
            converted_label="table_exists",
        ),
        _PatternValidator(
            frozenset({"columnexists"}),
            requires_any=("_column_exists", "spark.table", ".schema"),
            error_message="ColumnExists must inspect table schema.",
            converted_label="column_exists",
        ),
        _PatternValidator(
            frozenset({"checkfilelocked", "fileslocked", "lockedfiles"}),
            requires_any=("_file_is_locked", "WARNING"),
            error_message="CheckFileLocked must emit lock probe with limitations.",
            converted_label="file_locked",
        ),
        _PatternValidator(
            frozenset({"webserviceavailable", "checkwebserviceavailable"}),
            requires_any=("requests.get", "_ws_available"),
            error_message="WebServiceAvailable must probe HTTP availability.",
            converted_label="ws_available",
        ),
        _PatternValidator(
            frozenset({"webservice", "webservicelookup"}),
            requires_any=("UNSUPPORTED", "preserved.wsdl", "zeep"),
            error_message="WebServicesLookup must document SOAP limits and preserve WSDL.",
            converted_label="ws_lookup",
        ),
        ReplaceNullValidator(),
        IfNullValidator(),
        StringOperationsValidator(),
        CsvOutputValidator(),
        ExcelInputValidator(),
        ExcelOutputValidator(),
        ExcelWriterValidator(),
        TextFileInputValidator(),
        InsertUpdateValidator(),
        UpdateValidator(),
        DeleteValidator(),
        ExecuteSQLValidator(),
        ExecSQLRowValidator(),
        JsonInputValidator(),
        JsonOutputValidator(),
        XmlInputValidator(),
        XmlOutputValidator(),
        ParquetInputValidator(),
        ParquetOutputValidator(),
        DeltaFileOutputValidator(),
        OrcInputValidator(),
        OrcOutputValidator(),
        AvroInputValidator(),
        AvroOutputValidator(),
        MongoDBInputValidator(),
        MongoDBOutputValidator(),
        KafkaValidator(),
        RecordsFromStreamValidator(),
        JmsStreamingValidator(),
        MqttStreamingValidator(),
        InjectorValidator(),
        SocketReaderValidator(),
        SocketWriterValidator(),
        MemoryGroupByValidator(),
        AnalyticQueryValidator(),
        SampleRowsValidator(),
        ReservoirSamplingValidator(),
        UnivariateStatsValidator(),
        StepsMetricsValidator(),
        SwitchCaseValidator(),
        AppendStreamsValidator(),
        IdentifyLastRowValidator(),
        JavaFilterValidator(),
        PrioritizeStreamsValidator(),
        JavaScriptValueValidator(),
        UserDefinedJavaClassValidator(),
        UserDefinedJavaExpressionValidator(),
        UserDefinedJavaValidator(),
        RestHttpValidator(),
        # ---- Additional Input transformation validators ----
        _PatternValidator(
            frozenset({"fixedinput", "fixedfileinput", "fixedwidthinput"}),
            requires_any=("substring(", "read.text", "format('text')", ".load("),
            error_message="FixedFileInput must parse fixed-width text.",
            converted_label="fixed_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"gzipcsvinput", "gzipcsvfileinput", "gzipcsv"}),
            requires_all=("format('csv')", "gzip"),
            error_message="GZIP CSV Input must read CSV with gzip compression.",
            converted_label="gzip_csv_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"s3csvinput", "s3csvfileinput", "s3fileinput"}),
            requires_any=("format('csv')", ".load("),
            error_message="S3 CSV Input must load CSV from an object-store path.",
            converted_label="s3_csv_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"yamlinput"}),
            requires_any=("read.text(", "yaml"),
            error_message="YAML Input must load YAML content for DataFrame conversion.",
            converted_label="yaml_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"propertyinput", "propertiesinput"}),
            requires_any=("read.text(", "split("),
            error_message="Property Input must parse key=value properties.",
            converted_label="property_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"xmlinputstream", "staxxmlinput", "xmlinputstreamstax"}),
            requires_any=("format('xml')", ".load("),
            error_message="XML Input Stream must use spark-xml.",
            converted_label="xml_stream_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"loadfileinput", "loadfilecontentinmemory", "loadfile"}),
            requires_any=("wholeTextFiles(", "binaryFiles("),
            error_message="Load File Content must use wholeTextFiles/binaryFiles.",
            converted_label="load_file_content",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"accessinput", "microsoftaccessinput"}),
            requires_any=("ucanaccess", "format('jdbc')"),
            error_message="Access Input must use UCanAccess JDBC.",
            converted_label="access_jdbc",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"sasinput"}),
            requires_any=("sas", ".load("),
            error_message="SAS Input must use a SAS Spark format.",
            converted_label="sas_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"xbaseinput", "dbfinput"}),
            requires_any=("dbf", ".load("),
            error_message="XBase Input must read DBF format.",
            converted_label="xbase_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"shapefilereader", "esrishapefile", "esrishapefilereader", "gisfileinput"}),
            requires_any=("shapefile", "Sedona", "GeometryRDD"),
            error_message="Shapefile Input must use Sedona/shapefile reader.",
            converted_label="shapefile_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"getfilenames"}),
            requires_any=("dbutils.fs.ls", "listStatus", "createDataFrame"),
            error_message="Get File Names must list filesystem entries.",
            converted_label="get_file_names",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"getsubfolders", "getsubfoldernames"}),
            requires_any=("dbutils.fs.ls", "isDirectory", "isDir"),
            error_message="Get Subfolder Names must list directories.",
            converted_label="get_subfolders",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"getfilesrowscount", "filesrowscount"}),
            requires_any=(".count(",),
            error_message="Get Files Rows Count must call count().",
            converted_label="file_row_count",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"gettablenames"}),
            requires_any=("catalog.listTables", "listTables"),
            error_message="Get Table Names must use spark.catalog.",
            converted_label="get_table_names",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"randomvalue", "generaterandomvalue"}),
            requires_any=("rand(", "randn(", "uuid()"),
            error_message="Generate Random Value must use Spark random functions.",
            converted_label="random_value",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"randomccnumbergenerator", "generaterandomcreditcardnumbers", "creditcardgenerator"}),
            requires_any=("concat(", "rand(", "lpad("),
            error_message="Random Credit Card generator must emit card-number columns.",
            converted_label="random_cc",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"salesforceinput"}),
            requires_any=("salesforce", "soql", "sfObject"),
            error_message="Salesforce Input must use a Salesforce connector format.",
            converted_label="salesforce_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"ldapinput"}),
            requires_any=("LDAP", "ldap", "createDataFrame"),
            error_message="LDAP Input must preserve connection metadata.",
            converted_label="ldap_partial",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"ldifinput"}),
            requires_any=("read.text(", "ldif"),
            error_message="LDIF Input must parse LDIF text.",
            converted_label="ldif_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"rssinput"}),
            requires_any=("format('xml')", ".load("),
            error_message="RSS Input must parse feed XML.",
            converted_label="rss_read",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"hl7input"}),
            requires_any=("hl7", "read.text("),
            error_message="HL7 Input must load HL7 message text.",
            converted_label="hl7_partial",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"systeminfo"}),
            requires_any=("current_timestamp(", "current_date(", "withColumn", "sparkUser"),
            error_message="Get System Info must add system columns.",
            converted_label="system_info",
            warn_passthrough=False,
        ),
        UnsupportedInputValidator(
            frozenset({
                "getrepositorynames",
                "mailinput",
                "emailmessagesinput",
                "emailinput",
                "cubeinput",
                "deserializefromfile",
                "deserialisefromfile",
                "mondrianinput",
                "olapinput",
                "xmla",
                "xmlainput",
                "sapinput",
                "saperpinput",
            })
        ),
        # ---- Output transformation validators ----
        _PatternValidator(
            frozenset({"accessoutput", "microsoftaccessoutput"}),
            requires_any=("ucanaccess", "format('jdbc')"),
            error_message="Access Output must use UCanAccess JDBC write.",
            converted_label="access_jdbc_write",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"s3fileoutput", "s3output"}),
            requires_any=("s3a://", "s3://", ".save("),
            error_message="S3 File Output must write to an object-store path.",
            converted_label="s3_write",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"sqlfileoutput"}),
            requires_any=("INSERT INTO", ".text(", "write.mode"),
            error_message="SQL File Output must generate SQL text files.",
            converted_label="sql_file_write",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"propertyoutput", "propertiesoutput"}),
            requires_any=(".text(", "lit('=')", "='"),
            error_message="Properties Output must write key=value text.",
            converted_label="properties_write",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"rssoutput"}),
            requires_any=("format('xml')", ".save("),
            error_message="RSS Output must write XML feed content.",
            converted_label="rss_write",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"ldapoutput"}),
            requires_any=("LDAP", "ldap", "TODO"),
            error_message="LDAP Output must preserve connection metadata.",
            converted_label="ldap_output_partial",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"salesforceinsert", "salesforceupdate", "salesforceupsert", "salesforcedelete"}),
            requires_any=("salesforce", "sfObject"),
            error_message="Salesforce Output must use a Salesforce connector format.",
            converted_label="salesforce_write",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"autodoc", "automaticdocumentationoutput", "autodocoutput"}),
            requires_any=(".text(", "createDataFrame", "documentation"),
            error_message="Automatic Documentation Output must emit a documentation artifact.",
            converted_label="autodoc_write",
            warn_passthrough=False,
        ),
        UnsupportedOutputValidator(
            frozenset({
                "cubeoutput",
                "serializetofile",
                "serialisetofile",
                "pentahoreportingoutput",
                "reportexport",
                "prptoutput",
                "pentahoreporting",
            })
        ),
        # Advanced step pattern validators
        _PatternValidator(
            frozenset({"dummy", "dummytrans", "dummydonothing"}),
            requires_any=(" = ", "Pass-through step"),
            error_message="Dummy must assign the hop input DataFrame unchanged.",
            converted_label="passthrough",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"blockingstep", "block"}),
            requires_any=(".cache(", ".persist(", "count("),
            error_message="BlockingStep should cache or materialize data.",
            converted_label="block",
        ),
        _PatternValidator(
            frozenset({"detectemptystream", "detectempty"}),
            requires_any=("count(", "isEmpty", "limit("),
            error_message="DetectEmptyStream must check row count.",
            converted_label="empty_check",
        ),
        _PatternValidator(
            frozenset({"blockuntilstepsfinish", "blockthisstepuntilstepsfinish"}),
            requires_any=("count(", "LIMITATION", "wait"),
            error_message="Block Until Steps Finish must document wait dependencies.",
            converted_label="block_until",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"metainject", "etlmetadatainjection"}),
            requires_any=("_meta_inject_", "LIMITATION", "mappings"),
            error_message="ETL Metadata Injection must preserve injection metadata.",
            converted_label="meta_inject",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"jobexecutor"}),
            requires_any=("_exec_meta_", "LIMITATION", "Job Executor"),
            error_message="Job Executor must preserve child job metadata.",
            converted_label="job_executor",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"transexecutor", "transformationexecutor"}),
            requires_any=("_exec_meta_", "LIMITATION", "Transformation Executor"),
            error_message="Transformation Executor must preserve child transformation metadata.",
            converted_label="trans_executor",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"mapping", "mappingsubtransformation"}),
            requires_any=("_invoke_mapping_", "createOrReplaceTempView", "LIMITATION"),
            error_message="Mapping must invoke a reusable child helper and preserve metadata.",
            converted_label="mapping",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"simplemapping", "simplemappingsubtransformation"}),
            requires_any=("_invoke_mapping_", "createOrReplaceTempView", "LIMITATION"),
            error_message="Simple Mapping must invoke a reusable child helper and preserve metadata.",
            converted_label="simple_mapping",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"mappinginput", "mappinginputspecification"}),
            requires_any=("_pentaho_mapping_input", "missing required", "select("),
            error_message="Mapping Input must validate schema and project fields.",
            converted_label="mapping_input",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"mappingoutput", "mappingoutputspecification"}),
            requires_any=("_pentaho_mapping_output", "Mapping Output"),
            error_message="Mapping Output must project/publish the output schema.",
            converted_label="mapping_output",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"singlethreader"}),
            requires_any=("_single_threader_", "LIMITATION", "Single Threader"),
            error_message="Single Threader must preserve sub-transformation metadata.",
            converted_label="single_threader",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"setvariables", "setvariable"}),
            requires_any=("spark.conf.set", "os.environ", "LIMITATION"),
            error_message="Set Variables must assign spark.conf / env values.",
            converted_label="set_var",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"getvariables", "getvariable"}),
            requires_any=("withColumn", "lit(", "spark.conf.get", "widgets"),
            error_message="Get Variables must read variables into columns.",
            converted_label="get_var",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"rowstoresult", "copyrowstoresult"}),
            requires_any=("_pentaho_result_rows", "Copy Rows to Result"),
            error_message="Copy Rows to Result must buffer rows for job result.",
            converted_label="rows_to_result",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"rowsfromresult", "getrowsfromresult"}),
            requires_any=("_pentaho_result_rows", "Get Rows from Result", "createDataFrame"),
            error_message="Get Rows from Result must read the result row buffer.",
            converted_label="rows_from_result",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"filesfromresult", "getfilesfromresult"}),
            requires_any=("_pentaho_result_files", "Get Files from Result", "createDataFrame"),
            error_message="Get Files from Result must read the result file list.",
            converted_label="files_from_result",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"filestoresult", "setfilesinresult", "setfilestoresult"}),
            requires_any=("_pentaho_result_files", "Set Files in Result"),
            error_message="Set Files in Result must append file metadata to the result list.",
            converted_label="files_to_result",
            warn_passthrough=False,
        ),
        DimensionLookupValidator(),
        CombinationLookupValidator(),
        _PatternValidator(
            frozenset({"synchronizeaftermerge", "synchronizemerge"}),
            requires_any=("MERGE INTO",),
            error_message="SynchronizeAfterMerge must use MERGE.",
            converted_label="sync_merge",
        ),
        _PatternValidator(
            frozenset({"fuzzymatch"}),
            requires_any=("levenshtein(", "crossJoin", "soundex("),
            error_message="FuzzyMatch must compute similarity join.",
            converted_label="fuzzy",
        ),
        _PatternValidator(
            frozenset({"top", "rowsfilter", "limit"}),
            requires_any=(".limit(", "row_number", "filter("),
            error_message="TopN/Limit must filter or limit rows.",
            converted_label="top_n",
        ),
        _PatternValidator(
            frozenset({"checksum", "addachecksum"}),
            requires_any=("crc32(", "md5(", "sha1(", "sha2(", "withColumn"),
            error_message="Checksum must compute a hash/crc column.",
            converted_label="checksum",
        ),
        _PatternValidator(
            frozenset({"numberrange"}),
            requires_any=("when(", "withColumn"),
            error_message="NumberRange must emit conditional range mapping.",
            converted_label="number_range",
        ),
        _PatternValidator(
            frozenset({"fieldschangesequence", "addvaluefieldschangingsequence"}),
            requires_any=("row_number", "lag(", "Window"),
            error_message="FieldsChangeSequence must generate a resetting sequence.",
            converted_label="fields_change_sequence",
        ),
        _PatternValidator(
            frozenset({"closuregenerator", "closure"}),
            requires_any=("unionByName", "_cg_edges_", "dropDuplicates", "createDataFrame"),
            error_message="ClosureGenerator must expand parent-child hierarchy.",
            converted_label="closure",
        ),
        GetSlaveSequenceValidator(),
        XsltTransformValidator(),
        _PatternValidator(
            frozenset({"splunkinput", "splunkoutput", "splunk"}),
            requires_any=("UNSUPPORTED", "preserved."),
            error_message="Splunk steps must document UNSUPPORTED and preserve metadata.",
            converted_label="splunk_metadata",
        ),
        _PatternValidator(
            frozenset({"rownormaliser", "rownormalizer", "normaliser"}),
            requires_any=("unionByName", ".select(", "explode(", "stack("),
            error_message="RowNormaliser must unpivot/normalise rows.",
            converted_label="normalise",
        ),
        _PatternValidator(
            frozenset({"rowdenormaliser", "rowdenormalizer", "denormaliser"}),
            requires_any=("groupBy(", "pivot(", "agg("),
            error_message="RowDenormaliser must pivot/aggregate.",
            converted_label="denormalise",
        ),
        _PatternValidator(
            frozenset({"flattener", "rowflattener"}),
            requires_any=("groupBy(", "row_number", "when("),
            error_message="RowFlattener must pack consecutive values via window/groupBy.",
            converted_label="flatten",
        ),
        _PatternValidator(
            frozenset({"splitfieldtorows"}),
            requires_any=("explode(", "explode_outer(", "split("),
            error_message="SplitFieldToRows must split and explode tokens.",
            converted_label="split_to_rows",
        ),
        _PatternValidator(
            frozenset({"fieldsplitter", "splitfields"}),
            requires_any=("split(", "element_at(", "withColumn"),
            error_message="SplitFields must split into typed columns.",
            converted_label="split_fields",
        ),
        _PatternValidator(
            frozenset({"regexeval", "regularexpression"}),
            requires_any=("regexp_extract(", "rlike("),
            error_message="RegexEval must apply regex functions.",
            converted_label="regex_eval",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"ruleaccumulator", "rulesaccumulator"}),
            requires_any=("WARNING", "preserved.rule", "Rules Accumulator"),
            error_message="Rules Accumulator must preserve Drools metadata with warnings.",
            converted_label="rules_accumulator",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"ruleexecutor", "rulesexecutor"}),
            requires_any=("WARNING", "preserved.rule", "Rules Executor", "when("),
            error_message="Rules Executor must preserve Drools metadata with warnings.",
            converted_label="rules_executor",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"regexreplace"}),
            requires_any=("regexp_replace(",),
            error_message="RegexReplace must use regexp_replace().",
            converted_label="regex_replace",
        ),
        _PatternValidator(
            frozenset({"validator", "datavalidator"}),
            requires_any=(".filter(", "when(", "_row_valid", "WARNING"),
            error_message="DataValidator must apply field rules / filter invalid rows.",
            converted_label="validate",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"creditcardvalidator", "creditcard"}),
            requires_any=("udf(", "Luhn", "_cc_luhn", "WARNING"),
            error_message="CreditCardValidator must emit Luhn validation logic.",
            converted_label="credit_card_validate",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"mailvalidator", "emailvalidator"}),
            requires_any=("udf(", "email", "rlike(", "WARNING"),
            error_message="MailValidator must emit email validation logic.",
            converted_label="mail_validate",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"xsdvalidator", "xmlschemavalidator"}),
            requires_any=("udf(", "XMLSchema", "lxml", "WARNING", "LIMITATION"),
            error_message="XSDValidator must emit XML schema validation logic.",
            converted_label="xsd_validate",
            warn_passthrough=False,
        ),
        # ---- Cryptography transformation validators ----
        _PatternValidator(
            frozenset({"pgpencryptstream", "pgpencrypt"}),
            requires_any=("gnupg", "pgp_encrypt", "WARNING", "LIMITATION"),
            error_message="PGP Encrypt Stream must emit OpenPGP/python-gnupg logic.",
            converted_label="pgp_encrypt",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"pgpdecryptstream", "pgpdecrypt"}),
            requires_any=("gnupg", "pgp_decrypt", "dbutils.secrets", "WARNING"),
            error_message="PGP Decrypt Stream must emit OpenPGP decrypt with secret refs.",
            converted_label="pgp_decrypt",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"secretkeygenerator", "secretkeygen"}),
            requires_any=("token_bytes", "secrets", "createDataFrame"),
            error_message="Secret Key Generator must emit secure key generation.",
            converted_label="secret_key_gen",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"symmetriccryptotrans", "symmetriccrypto", "symmetriccryptography"}),
            requires_any=("cryptography", "Cipher", "symmetric", "dbutils.secrets", "WARNING"),
            error_message="Symmetric Cryptography must emit cryptography encrypt/decrypt logic.",
            converted_label="symmetric_crypto",
            warn_passthrough=False,
        ),
        # ---- Experimental transformation validators ----
        _PatternValidator(
            frozenset({"sftpput", "sftpputfile", "putafilewithsftp", "putsftp"}),
            requires_any=("paramiko", "SFTP", "dbutils.secrets", "WARNING"),
            error_message="SFTP Put must emit Paramiko upload logic with secret refs.",
            converted_label="sftp_put",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"script", "scriptvalues", "experimentalscript"}),
            requires_any=(
                "Experimental Script",
                "preserved.script_language",
                "WARNING",
                "UNSUPPORTED",
                "withColumn",
            ),
            error_message="Experimental Script must preserve scripts and emit migration stubs.",
            converted_label="experimental_script",
            warn_passthrough=False,
        ),
        # ---- Pentaho Server transformation validators ----
        _PatternValidator(
            frozenset({"callendpoint", "callendpointstep"}),
            requires_any=("requests", "Call Endpoint", "WARNING", "LIMITATION"),
            error_message="Call Endpoint must emit requests-based HTTP logic.",
            converted_label="call_endpoint",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({
                "getsessionvariable",
                "getsessionvariables",
                "getsessionvariablestep",
            }),
            requires_any=(
                "_pentaho_session_vars",
                "spark.conf.get",
                "widgets",
                "Get Session Variables",
            ),
            error_message="Get Session Variables must resolve session values into columns.",
            converted_label="get_session_var",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({
                "setsessionvariable",
                "setsessionvariables",
                "setsessionvariablestep",
            }),
            requires_any=(
                "_pentaho_session_vars",
                "spark.conf.set",
                "os.environ",
                "Set Session Variables",
            ),
            error_message="Set Session Variables must write session values to runtime stores.",
            converted_label="set_session_var",
            warn_passthrough=False,
        ),
        # ---- Bulk Loading transformation validators ----
        _PatternValidator(
            frozenset({
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
            }),
            requires_any=("saveAsTable", "format('jdbc')", "UNSUPPORTED", "Bulk Loader"),
            error_message=(
                "Bulk Loader must emit Delta saveAsTable and/or JDBC fallback, "
                "or document unsupported native loader."
            ),
            converted_label="bulk_loader",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"hadoopfileinput", "hadoopfileinputplugin", "hadoopfileoutputplugin"}),
            requires_any=(".load(", ".read."),
            error_message="Hadoop file step must read/write Hadoop paths.",
            converted_label="hadoop_io",
        ),
        # ---- Utility transformation validators ----
        _PatternValidator(
            frozenset({"clonerow", "clonerows"}),
            requires_any=("unionByName", "explode(", "sequence(", "WARNING"),
            error_message="Clone Row must duplicate rows via union or explode.",
            converted_label="clone_row",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"nullif"}),
            requires_any=("when(", "lit(None)"),
            error_message="Null If must convert matching values to null.",
            converted_label="null_if",
        ),
        _PatternValidator(
            frozenset({"delay", "delayrow"}),
            requires_any=("WARNING", "preserved.timeout"),
            error_message="Delay Row must document distributed-execution limitations.",
            converted_label="delay_row",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"metastructure", "stepmetastructure", "metadatastructureofstream"}),
            requires_any=("createDataFrame", ".schema"),
            error_message="Metadata Structure must emit schema metadata rows.",
            converted_label="meta_structure",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"writetolog"}),
            requires_any=("logging.getLogger", "getLogger"),
            error_message="Write to Log must use structured logging.",
            converted_label="write_to_log",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"tablecompare"}),
            requires_any=(".join(", "exceptAll", "spark.table"),
            error_message="Table Compare must compare tables and detect differences.",
            converted_label="table_compare",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"changefileencoding", "fileencoding"}),
            requires_any=("encoding=", "read_text", "write_text", "WARNING"),
            error_message="Change File Encoding must convert or warn about encodings.",
            converted_label="change_encoding",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"zipfile"}),
            requires_any=("zipfile.ZipFile", "ZIP_DEFLATED", "ZIP_STORED", "WARNING"),
            error_message="Zip File must use Python zipfile utilities.",
            converted_label="zip_file",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"processfiles"}),
            requires_any=("shutil.", "os.remove", "WARNING"),
            error_message="Process Files must emit file operation equivalents.",
            converted_label="process_files",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"execprocess", "executeaprocess"}),
            requires_any=("UNSUPPORTED", "preserved."),
            error_message="Execute a Process must warn when unsupported and preserve config.",
            converted_label="exec_process",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"ssh", "runsshcommands"}),
            requires_any=("UNSUPPORTED", "preserved."),
            error_message="SSH must warn when unsupported and preserve config.",
            converted_label="ssh",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"mail", "sendmail"}),
            requires_any=("UNSUPPORTED", "preserved.", "smtplib"),
            error_message="Mail must preserve SMTP metadata and emit a stub/warning.",
            converted_label="mail",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"syslogmessage", "writetosyslog", "sendmessagetosyslog"}),
            requires_any=("UNSUPPORTED", "preserved."),
            error_message="Syslog must warn when unsupported and preserve config.",
            converted_label="syslog",
            warn_passthrough=False,
        ),
        _PatternValidator(
            frozenset({"edi2xml", "editoxml"}),
            requires_any=("withColumn", "preserved.", "CDATA"),
            error_message="EDI to XML must preserve config and emit conversion stub.",
            converted_label="edi_xml",
            warn_passthrough=False,
        ),
    ]
    for v in validators:
        register_validator(v)
