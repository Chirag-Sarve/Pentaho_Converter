"""Validator registry."""

from __future__ import annotations

from .base import StepValidator

_VALIDATORS: list[StepValidator] = []


def register_validator(validator: StepValidator) -> None:
    _VALIDATORS.append(validator)


def get_validator(step_type: str) -> StepValidator | None:
    for v in _VALIDATORS:
        if v.handles(step_type):
            return v
    return None
