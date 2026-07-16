"""Dedicated Pentaho step converters — one converter per registered handler."""

from __future__ import annotations

from ..steps.advanced_handlers import ADVANCED_HANDLERS
from ..steps.extended_handlers import EXTENDED_HANDLERS
from ..steps.extra_input_handlers import EXTRA_INPUT_HANDLERS
from ..steps.extra_output_handlers import EXTRA_OUTPUT_HANDLERS
from ..steps.field_transform_handlers import FIELD_TRANSFORM_HANDLERS
from ..steps.flow_handlers import FLOW_HANDLERS
from ..steps.job_handlers import JOB_HANDLERS
from ..steps.mapping_handlers import MAPPING_HANDLERS
from ..steps.reshape_handlers import RESHAPE_HANDLERS
from ..steps.calc_extended_handlers import CALC_EXTENDED_HANDLERS
from ..steps.special_transform_handlers import SPECIAL_TRANSFORM_HANDLERS
from ..steps.streaming_handlers import STREAMING_HANDLERS
from ..steps.statistics_handlers import STATISTICS_HANDLERS
from ..steps.scripting_handlers import SCRIPTING_HANDLERS
from ..steps.utility_handlers import UTILITY_HANDLERS
from ..steps.inline_handlers import INLINE_HANDLERS
from ..steps.lookup_handlers import LOOKUP_HANDLERS
from ..steps.join_handlers import JOIN_HANDLERS
from ..steps.validation_handlers import VALIDATION_HANDLERS
from ..steps.cryptography_handlers import CRYPTOGRAPHY_HANDLERS
from ..steps.bulk_loading_handlers import BULK_LOADING_HANDLERS
from ..steps.experimental_handlers import EXPERIMENTAL_HANDLERS
from ..steps.pentaho_server_handlers import PENTAHO_SERVER_HANDLERS
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
        *FIELD_TRANSFORM_HANDLERS,
        *RESHAPE_HANDLERS,
        *CALC_EXTENDED_HANDLERS,
        *SPECIAL_TRANSFORM_HANDLERS,
        TextFileOutputHandler(),
        TableInputHandler(),
        CsvInputHandler(),
        ExcelInputHandler(),
        TextFileInputHandler(),
        *EXTRA_INPUT_HANDLERS,
        StringOperationsHandler(),
        SortRowsHandler(),
        GroupByHandler(),
        *STATISTICS_HANDLERS,
        *SCRIPTING_HANDLERS,  # includes ScriptingFormulaHandler (canonical Formula)
        ReplaceNullHandler(),
        MergeJoinHandler(),
        *JOIN_HANDLERS,
        StreamLookupHandler(),
        DatabaseLookupHandler(),
        *LOOKUP_HANDLERS,
        TableOutputHandler(),
        ExcelOutputHandler(),
        *EXTRA_OUTPUT_HANDLERS,
        *STREAMING_HANDLERS,
        *INLINE_HANDLERS,
        *UTILITY_HANDLERS,
        *FLOW_HANDLERS,
        *MAPPING_HANDLERS,
        *JOB_HANDLERS,
        *VALIDATION_HANDLERS,
        *CRYPTOGRAPHY_HANDLERS,
        *BULK_LOADING_HANDLERS,
        *EXPERIMENTAL_HANDLERS,
        *PENTAHO_SERVER_HANDLERS,
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
            # Skip duplicate step type registrations when aliases partially overlap
            overlap = conv.step_types & seen_types
            if overlap == conv.step_types:
                continue
        seen_types.update(conv.step_types)
        converters.append(conv)
    return converters


DEDICATED_CONVERTERS: list[BaseStepConverter] = build_all_converters()


def build_fallback_converter() -> BaseStepConverter:
    return make_converter(FallbackHandler(), name="Fallback")
