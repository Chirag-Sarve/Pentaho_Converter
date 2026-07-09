"""Dedicated Pentaho step converters — one converter per registered handler."""

from __future__ import annotations

from ..steps.advanced_handlers import ADVANCED_HANDLERS
from ..steps.extended_handlers import EXTENDED_HANDLERS
from ..steps.fallback import FallbackHandler
from ..steps.generate_handlers import ConstantHandler, RowGeneratorHandler
from ..steps.input_handlers import (
    CsvInputHandler,
    ExcelInputHandler,
    TableInputHandler,
    TextFileInputHandler,
)
from ..steps.output_handlers import ExcelOutputHandler, TableOutputHandler, TextFileOutputHandler
from ..steps.string_handlers import StringOperationsHandler
from ..steps.transform_handlers import (
    CalculatorHandler,
    DatabaseLookupHandler,
    FilterRowsHandler,
    FormulaHandler,
    GroupByHandler,
    MergeJoinHandler,
    ReplaceNullHandler,
    SelectValuesHandler,
    SortRowsHandler,
    StreamLookupHandler,
)
from .base import BaseStepConverter
from .factory import make_converter


def _all_handlers():
    """All Pentaho step handlers in registration order (first match wins)."""
    return [
        RowGeneratorHandler(),
        ConstantHandler(),
        CalculatorHandler(),
        FilterRowsHandler(),
        SelectValuesHandler(),
        TextFileOutputHandler(),
        TableInputHandler(),
        CsvInputHandler(),
        ExcelInputHandler(),
        TextFileInputHandler(),
        StringOperationsHandler(),
        SortRowsHandler(),
        GroupByHandler(),
        FormulaHandler(),
        ReplaceNullHandler(),
        MergeJoinHandler(),
        StreamLookupHandler(),
        DatabaseLookupHandler(),
        TableOutputHandler(),
        ExcelOutputHandler(),
        *EXTENDED_HANDLERS,
        *ADVANCED_HANDLERS,
    ]


def build_all_converters() -> list[BaseStepConverter]:
    """Build one dedicated BaseStepConverter per handler."""
    seen_types: set[str] = set()
    converters: list[BaseStepConverter] = []
    for handler in _all_handlers():
        types = getattr(handler, "_TYPES", set())
        if types and types.issubset(seen_types):
            continue
        conv = make_converter(handler)
        if conv.step_types.intersection(seen_types):
            # Skip duplicate step type registrations (e.g. FormulaHandler vs FormulaStepHandler)
            overlap = conv.step_types & seen_types
            if overlap == conv.step_types:
                continue
        seen_types.update(conv.step_types)
        converters.append(conv)
    return converters


DEDICATED_CONVERTERS: list[BaseStepConverter] = build_all_converters()


def build_fallback_converter() -> BaseStepConverter:
    return make_converter(FallbackHandler(), name="Fallback")
