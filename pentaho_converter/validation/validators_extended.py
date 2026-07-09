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


class JoinRowsValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"joinrows", "joiner"}),
            requires_any=(".join(",),
            error_message="JoinRows must generate a .join() call.",
            converted_label="join",
        )


class MergeRowsValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"mergerows", "mergerow"}),
            requires_any=(".join(", "unionByName"),
            error_message="MergeRows must generate join or union logic.",
            converted_label="merge_rows",
        )


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
            frozenset({"databaselookup"}),
            requires_any=("spark.table(", ".join(", "broadcast("),
            error_message="DatabaseLookup must read lookup table and join.",
            converted_label="db_lookup",
        )


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
            frozenset({"ifnull", "iffieldvaluenull"}),
            requires_any=("when(", "isNull(", "coalesce("),
            error_message="IfNull must generate null-handling logic.",
            converted_label="if_null",
        )


class StringOperationsValidator(StepValidator):
    step_types = frozenset({"stringoperations", "stringcut", "replaceinstring"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        ops = parsed.get("operations", [])

        string_fns = (
            "upper(", "lower(", "trim(", "ltrim(", "rtrim(", "initcap(",
            "substring(", "regexp_replace(",
        )
        if not any(fn in code for fn in string_fns) and "withColumn" not in code:
            result.errors.append("StringOperations must apply string functions via withColumn().")
        else:
            result.properties_converted.append("string_ops")
        for op in ops:
            out_name = op.get("out") or op.get("in")
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
            requires_all=("format('csv')", ".save("),
            error_message="CSVOutput must write CSV with format('csv').",
            converted_label="csv_write",
        )


class ExcelInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"excelinput"}),
            requires_any=("format('com.crealytics.spark.excel')", ".load("),
            error_message="ExcelInput must read Excel format.",
            converted_label="excel_read",
        )


class ExcelOutputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"exceloutput"}),
            requires_any=("format('com.crealytics.spark.excel')", ".save("),
            error_message="ExcelOutput must write Excel format.",
            converted_label="excel_write",
        )


class TextFileInputValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"textfileinput"}),
            requires_any=(".read.text(", ".load("),
            error_message="TextFileInput must read text file.",
            converted_label="text_read",
        )


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
            requires_any=("spark.sql(",),
            error_message="ExecuteSQL must call spark.sql().",
            converted_label="execute_sql",
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
            requires_any=("format('json')", ".save("),
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
            requires_any=("format('parquet')", ".save("),
            error_message="ParquetOutput must write Parquet format.",
            converted_label="parquet_write",
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


class KafkaValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"kafkaconsumerinput", "kafkastreaminput", "kafka", "kafkaproduceroutput"}),
            requires_any=("format('kafka')",),
            error_message="Kafka step must use spark Kafka format.",
            converted_label="kafka",
        )


class MemoryGroupByValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"memorygroupby"}),
            requires_any=(".groupBy(",),
            error_message="MemoryGroupBy must generate groupBy().",
            converted_label="group_by",
        )


class SwitchCaseValidator(_PatternValidator):
    def __init__(self) -> None:
        super().__init__(
            frozenset({"switchcase"}),
            requires_any=("when(",),
            error_message="SwitchCase must generate when() expressions.",
            converted_label="switch_case",
        )


class JavaScriptValueValidator(StepValidator):
    step_types = frozenset({"scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        code = "\n".join(code_lines)
        if "expr(" not in code and "withColumn" not in code:
            result.errors.append("JavaScript value step requires manual review or expr() conversion.")
            result.score = 0.3
        else:
            result.warnings.append("JavaScript logic converted approximately — verify semantics.")
            result.properties_converted.append("javascript")
            result.score = 0.7
        result.score = _score(result)
        return result


class UserDefinedJavaValidator(StepValidator):
    step_types = frozenset({"userdefinedjavaclass", "userdefinedjavaexpression"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        result.warnings.append("User-defined Java requires manual intervention.")
        if "expr(" in "\n".join(code_lines) or "withColumn" in "\n".join(code_lines):
            result.properties_converted.append("udje")
            result.score = 0.5
        else:
            result.score = 0.3
        result.score = _score(result)
        return result


class RestHttpValidator(StepValidator):
    step_types = frozenset({"rest", "restclient", "http", "httppost", "httpget"})

    def validate(self, context, parsed, code_lines) -> SemanticValidationResult:
        result = _syntax_result(code_lines)
        result.warnings.append("REST/HTTP steps require external API integration — partial support.")
        if "createDataFrame" in "\n".join(code_lines):
            result.properties_converted.append("http_stub")
            result.score = 0.55
        else:
            result.score = 0.35
        result.score = _score(result)
        return result


def register_extended_validators() -> None:
    """Register validators for the extended step batch."""
    validators: list[StepValidator] = [
        FormulaValidator(),
        MergeJoinValidator(),
        JoinRowsValidator(),
        MergeRowsValidator(),
        StreamLookupValidator(),
        DatabaseLookupValidator(),
        ReplaceNullValidator(),
        IfNullValidator(),
        StringOperationsValidator(),
        CsvOutputValidator(),
        ExcelInputValidator(),
        ExcelOutputValidator(),
        TextFileInputValidator(),
        InsertUpdateValidator(),
        UpdateValidator(),
        DeleteValidator(),
        ExecuteSQLValidator(),
        JsonInputValidator(),
        JsonOutputValidator(),
        XmlInputValidator(),
        XmlOutputValidator(),
        ParquetInputValidator(),
        ParquetOutputValidator(),
        OrcInputValidator(),
        OrcOutputValidator(),
        AvroInputValidator(),
        AvroOutputValidator(),
        KafkaValidator(),
        MemoryGroupByValidator(),
        SwitchCaseValidator(),
        JavaScriptValueValidator(),
        UserDefinedJavaValidator(),
        RestHttpValidator(),
        # Advanced step pattern validators
        _PatternValidator(
            frozenset({"analyticquery"}),
            requires_any=("Window", "over("),
            error_message="AnalyticQuery must use window functions.",
            converted_label="analytic",
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
            frozenset({"setvariables", "setvariable"}),
            requires_any=("lit(", "withColumn", "spark.conf"),
            error_message="SetVariables must assign values.",
            converted_label="set_var",
        ),
        _PatternValidator(
            frozenset({"getvariables", "getvariable"}),
            requires_any=("lit(", "withColumn"),
            error_message="GetVariables must read variables into columns.",
            converted_label="get_var",
        ),
        _PatternValidator(
            frozenset({"dimensionlookup", "dimensionlookupupdate"}),
            requires_any=(".join(", "broadcast("),
            error_message="DimensionLookup must join lookup data.",
            converted_label="dim_lookup",
        ),
        _PatternValidator(
            frozenset({"combinationlookup"}),
            requires_any=(".join(", "monotonically_increasing_id"),
            error_message="CombinationLookup must join or generate keys.",
            converted_label="combo_lookup",
        ),
        _PatternValidator(
            frozenset({"synchronizeaftermerge", "synchronizemerge"}),
            requires_any=("MERGE INTO",),
            error_message="SynchronizeAfterMerge must use MERGE.",
            converted_label="sync_merge",
        ),
        _PatternValidator(
            frozenset({"fuzzymatch"}),
            requires_any=("levenshtein(", "crossJoin"),
            error_message="FuzzyMatch must compute similarity join.",
            converted_label="fuzzy",
        ),
        _PatternValidator(
            frozenset({"top", "rowsfilter", "samplerows", "limit"}),
            requires_any=(".limit(", "row_number", "filter("),
            error_message="TopN/Limit must filter or limit rows.",
            converted_label="top_n",
        ),
        _PatternValidator(
            frozenset({"rownormaliser", "rownormalizer", "normaliser"}),
            requires_any=("stack(", "explode(", "withColumn"),
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
            frozenset({"regexeval", "regularexpression"}),
            requires_any=("regexp_extract(", "rlike(", "regexp_replace("),
            error_message="RegexEval must apply regex functions.",
            converted_label="regex_eval",
        ),
        _PatternValidator(
            frozenset({"regexreplace"}),
            requires_any=("regexp_replace(",),
            error_message="RegexReplace must use regexp_replace().",
            converted_label="regex_replace",
        ),
        _PatternValidator(
            frozenset({"validator", "datavalidator"}),
            requires_any=(".filter(", "when("),
            error_message="DataValidator must filter invalid rows.",
            converted_label="validate",
        ),
        _PatternValidator(
            frozenset({"hadoopfileinput", "hadoopfileinputplugin", "hadoopfileoutputplugin"}),
            requires_any=(".load(", ".read."),
            error_message="Hadoop file step must read/write Hadoop paths.",
            converted_label="hadoop_io",
        ),
    ]
    for v in validators:
        register_validator(v)
