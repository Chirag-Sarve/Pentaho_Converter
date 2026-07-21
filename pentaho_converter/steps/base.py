"""Abstract base and registry for Pentaho step handlers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from ..conversion_outcome import (
    STATUS_FAILED,
    STATUS_UNSUPPORTED,
    StepConversionOutcome,
    derive_status,
)
from ..metadata_propagation import (
    get_converter_metadata,
    merge_input_lineage,
    propagate_step_metadata,
    update_lineage_map,
    validate_lineage_before_convert,
)
from ..step_context import StepContext
from ..step_xml import _child_text, extract_step_property, get_step_element
from ..validation.step_validators import GenericStepValidator, parse_step_config, register_builtin_validators

if TYPE_CHECKING:
    from ..converters.base import BaseStepConverter
    from ..converters.bridge import LegacyHandlerBridge

# Re-export for backward compatibility
__all__ = ["StepContext", "BaseStepHandler", "StepRegistry", "build_default_registry"]


class BaseStepHandler(ABC):
    """Legacy handler interface — prefer BaseStepConverter for new steps."""

    @abstractmethod
    def can_handle(self, step_type: str) -> bool:
        ...

    @abstractmethod
    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        """Return (code_lines, legacy_status)."""

    def _attr(self, context: StepContext, key: str, default: str = "") -> str:
        val = context.step.attributes.get(key, "")
        if val and val.strip().startswith("<"):
            # Legacy: nested XML was serialized into attributes before parser fix.
            try:
                from xml.etree import ElementTree as ET
                frag = ET.fromstring(val)
                if frag.tag == "file":
                    name = _child_text(frag, "name")
                    if name:
                        return name
            except ET.ParseError:
                pass
        if val:
            return val
        step_el = get_step_element(context.step)
        if step_el is not None:
            return extract_step_property(step_el, key, _child_text(step_el, key, default))
        return default

    def _fields(self, context: StepContext):
        if context.step.fields:
            return context.step.fields
        step_el = get_step_element(context.step)
        if step_el is None:
            return []
        from ..transformation_parser import _parse_fields
        return _parse_fields(step_el)


ConverterLike = Any


class StepRegistry:
    """Dispatches step types to registered converters with semantic validation."""

    def __init__(self) -> None:
        self._converters: list[ConverterLike] = []
        self._fallback: ConverterLike | None = None
        self._dedicated_types: set[str] = set()

    def register(self, converter: ConverterLike, *, dedicated: bool = False) -> None:
        self._converters.append(converter)
        if dedicated and hasattr(converter, "step_types"):
            self._dedicated_types.update(converter.step_types)

    def set_fallback(self, converter: ConverterLike) -> None:
        self._fallback = converter

    def get_converter(self, step_type: str) -> ConverterLike | None:
        # Try exact lowercased key, then compact (spaces/parens stripped) so
        # both KTR type IDs and UI display names match dedicated handlers.
        raw = (step_type or "").strip().lower()
        compact = raw.replace(" ", "").replace("(", "").replace(")", "")
        keys = [raw] if raw == compact else [raw, compact]
        for key in keys:
            for c in self._converters:
                if getattr(c, "handles", lambda _t: False)(key) or getattr(
                    c, "can_handle", lambda _t: False
                )(key):
                    return c
        return self._fallback

    def convert_step(self, step_type: str, context: StepContext) -> StepConversionOutcome:
        """Parse, generate, validate, and return an honest conversion outcome."""
        from ..converters.base import BaseStepConverter
        from ..converters.bridge import LegacyHandlerBridge

        register_builtin_validators()
        converter = self.get_converter(step_type)
        if converter is None:
            return self._unsupported_outcome(context)

        return self._convert_with_propagated_metadata(converter, context)

    def _convert_with_propagated_metadata(
        self, converter: ConverterLike, context: StepContext
    ) -> StepConversionOutcome:
        """Propagate parser metadata, validate lineage, then invoke converter."""
        from ..converters.base import BaseStepConverter
        from ..converters.bridge import LegacyHandlerBridge

        bundle = propagate_step_metadata(context)
        input_schemas = merge_input_lineage(context)
        lineage_check = validate_lineage_before_convert(
            context, bundle.converter_metadata, input_schemas
        )

        if lineage_check.errors:
            # Lineage assists validation only — never abort generation or drop
            # downstream steps (including Text File Output writes).
            for err in lineage_check.errors:
                if err not in lineage_check.warnings:
                    lineage_check.warnings.append(err)
            lineage_check.errors = []

        if isinstance(converter, (BaseStepConverter, LegacyHandlerBridge)):
            original_parse = converter.parse_xml

            def _patched_parse(ctx: StepContext) -> dict:
                return get_converter_metadata(ctx)

            converter.parse_xml = _patched_parse  # type: ignore[method-assign]
            try:
                outcome = converter.convert(context)
            finally:
                converter.parse_xml = original_parse  # type: ignore[method-assign]

            for w in lineage_check.warnings:
                if w not in outcome.warnings:
                    outcome.warnings.append(w)

            predicted = context.extra.get("predicted_lineage")
            if predicted is not None:
                update_lineage_map(context, predicted)
            return outcome

        # Raw legacy BaseStepHandler wrapped on the fly
        bridge = LegacyHandlerBridge(converter, dedicated=context.step.step_type.lower() in self._dedicated_types)
        return self._convert_with_propagated_metadata(bridge, context)

    def generate_code(self, step_type: str, context: StepContext) -> tuple[list[str], str]:
        """Backward-compatible API returning (lines, status)."""
        outcome = self.convert_step(step_type, context)
        return outcome.code_lines, outcome.status

    def _unsupported_outcome(self, context: StepContext) -> StepConversionOutcome:
        propagate_step_metadata(context)
        parsed = get_converter_metadata(context)
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        lines: list[str] = []
        if in_df:
            lines.append(f"{out_var} = {in_df}")
        else:
            lines.append(f"{out_var} = spark.range(0).select(lit(1).alias('_empty')).limit(0)")

        validation = GenericStepValidator().validate(context, parsed, lines)
        validation.errors.append(
            f"No dedicated converter registered for step type '{context.step.step_type}'."
        )
        validation.score = min(validation.score, 0.2)

        status = derive_status(
            validation.score,
            validation.errors,
            validation.warnings,
            has_dedicated_handler=False,
            step_type=context.step.step_type,
        )
        return StepConversionOutcome(
            code_lines=lines,
            status=status if status != STATUS_FAILED else STATUS_UNSUPPORTED,
            semantic_score=validation.score,
            detail=context.step.name,
            warnings=validation.warnings,
            errors=validation.errors,
            syntax_valid=validation.syntax_valid,
            handler_name=context.step.step_type,
        )


def build_default_registry() -> StepRegistry:
    """Create a registry with one dedicated converter per Pentaho step type."""
    from ..converters.registry_list import DEDICATED_CONVERTERS, build_fallback_converter

    registry = StepRegistry()
    for conv in DEDICATED_CONVERTERS:
        registry.register(conv, dedicated=True)
    registry.set_fallback(build_fallback_converter())
    return registry
