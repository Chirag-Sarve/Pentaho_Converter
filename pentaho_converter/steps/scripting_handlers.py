"""Handlers for Pentaho Scripting transformations."""

from __future__ import annotations

import logging

from ..metadata_propagation import get_converter_metadata
from ..scripting_converter import (
    convert_exec_sql_row_step,
    convert_exec_sql_step,
    convert_formula_step,
    convert_javascript_value_step,
    convert_regex_eval_step,
    convert_rules_accumulator_step,
    convert_rules_executor_step,
    convert_user_defined_java_class_step,
    convert_user_defined_java_expression_step,
)
from ..step_xml import (
    get_step_element,
    parse_exec_sql_config,
    parse_exec_sql_row_config,
    parse_formula_config,
    parse_javascript_value_config,
    parse_regex_eval_config,
    parse_rules_accumulator_config,
    parse_rules_executor_config,
    parse_user_defined_java_class_config,
    parse_user_defined_java_expression_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return (step_type or "").strip().lower().replace(" ", "")


def _merge_parsed(context: StepContext, parser) -> dict:
    metadata = dict(get_converter_metadata(context))
    step_el = get_step_element(context.step)
    if step_el is not None:
        parsed = parser(step_el)
        for key, val in parsed.items():
            if key not in metadata or metadata[key] in (None, "", [], {}):
                metadata[key] = val
            elif isinstance(val, list) and not metadata.get(key):
                metadata[key] = val
    return metadata


def _passthrough_error(context: StepContext, label: str, exc: Exception) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [
        f"# {label}: {context.step.name}",
        f"# ERROR: {exc}",
    ]
    logger.exception("%s '%s' failed: %s", label, context.step.name, exc)
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
    return lines, "partial"


class ExecSQLHandler(BaseStepHandler):
    """Execute SQL Script → spark.sql with JDBC/param preservation."""

    _TYPES = {"execsql", "executesql", "sql"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_exec_sql_config)
            lines, status = convert_exec_sql_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "ExecSQL '%s': connection=%s args=%s status=%s",
                context.step.name,
                metadata.get("connection"),
                len(metadata.get("arguments") or []),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Execute SQL Script", exc)


class ExecSQLRowHandler(BaseStepHandler):
    """Execute Row SQL Script → per-row spark.sql with scale warnings."""

    _TYPES = {"execsqlrow", "executerowsqlscript", "executerowsql"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_exec_sql_row_config)
            lines, status = convert_exec_sql_row_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "ExecSQLRow '%s': sql_field=%s status=%s",
                context.step.name,
                metadata.get("sql_field"),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Execute Row SQL Script", exc)


class ScriptingFormulaHandler(BaseStepHandler):
    """Formula → convert_formula / withColumn (Scripting category entry).

    Registered ahead of the classic FormulaHandler; same conversion path via
    ``convert_formula_step`` so nested + flat formulas stay covered.
    """

    _TYPES = {"formula"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_formula_config)
            lines, status = convert_formula_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "Formula '%s': %s expression(s) status=%s",
                context.step.name,
                len(metadata.get("formulas") or []) or (1 if metadata.get("formula") else 0),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Formula", exc)


class ScriptValueModHandler(BaseStepHandler):
    """Modified Java Script Value → approximate PySpark + preserve JS."""

    _TYPES = {"scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_javascript_value_config)
            lines, status = convert_javascript_value_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "ScriptValueMod '%s': fields=%s status=%s",
                context.step.name,
                len(metadata.get("fields") or []),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Modified Java Script Value", exc)


class RegexEvalHandler(BaseStepHandler):
    """Regex Evaluation → rlike / regexp_extract."""

    _TYPES = {"regexeval", "regularexpression"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_regex_eval_config)
            lines, status = convert_regex_eval_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "RegexEval '%s': captures=%s status=%s",
                context.step.name,
                len(metadata.get("fields") or []),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Regex Evaluation", exc)


class RulesAccumulatorHandler(BaseStepHandler):
    """Rules Accumulator → preserve Drools + optional when()/agg stubs."""

    _TYPES = {"ruleaccumulator", "rulesaccumulator"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_rules_accumulator_config)
            lines, status = convert_rules_accumulator_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info("RulesAccumulator '%s' status=%s", context.step.name, status)
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Rules Accumulator", exc)


class RulesExecutorHandler(BaseStepHandler):
    """Rules Executor → preserve Drools + conditional when() when feasible."""

    _TYPES = {"ruleexecutor", "rulesexecutor"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_rules_executor_config)
            lines, status = convert_rules_executor_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info("RulesExecutor '%s' status=%s", context.step.name, status)
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "Rules Executor", exc)


class UserDefinedJavaClassHandler(BaseStepHandler):
    """User Defined Java Class → preserve source; manual migration warning."""

    _TYPES = {"userdefinedjavaclass"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_user_defined_java_class_config)
            lines, status = convert_user_defined_java_class_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "UDJC '%s': class=%s status=%s",
                context.step.name,
                metadata.get("class_name"),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "User Defined Java Class", exc)


class UserDefinedJavaExpressionHandler(BaseStepHandler):
    """User Defined Java Expression → simple Janino expr → PySpark."""

    _TYPES = {"userdefinedjavaexpression"}

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context, parse_user_defined_java_expression_config)
            lines, status = convert_user_defined_java_expression_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
            )
            logger.info(
                "UDJE '%s': %s expression(s) status=%s",
                context.step.name,
                len(metadata.get("fields") or []),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, "User Defined Java Expression", exc)


SCRIPTING_HANDLERS: list[BaseStepHandler] = [
    ExecSQLHandler(),
    ExecSQLRowHandler(),
    ScriptingFormulaHandler(),
    ScriptValueModHandler(),
    RegexEvalHandler(),
    RulesAccumulatorHandler(),
    RulesExecutorHandler(),
    UserDefinedJavaClassHandler(),
    UserDefinedJavaExpressionHandler(),
]
