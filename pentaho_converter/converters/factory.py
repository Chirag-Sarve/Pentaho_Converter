"""Factory for wrapping legacy handlers as dedicated BaseStepConverter instances."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..step_context import StepContext
from .base import BaseStepConverter

if TYPE_CHECKING:
    from ..steps.base import BaseStepHandler


def make_converter(handler: BaseStepHandler, *, name: str | None = None) -> BaseStepConverter:
    """Wrap a BaseStepHandler as a dedicated BaseStepConverter."""
    step_types: frozenset[str] = frozenset(getattr(handler, "_TYPES", set()))
    converter_name = name or handler.__class__.__name__.replace("Handler", "")

    class HandlerStepConverter(BaseStepConverter):
        def generate(self, context: StepContext, parsed: dict) -> list[str]:
            lines, _ = handler.generate_code(context)
            return lines

    HandlerStepConverter.step_types = step_types
    HandlerStepConverter.converter_name = converter_name
    return HandlerStepConverter()
