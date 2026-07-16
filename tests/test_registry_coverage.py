"""Registry completeness: every registered type has handler, parser, validator."""

from __future__ import annotations

import unittest

from pentaho_converter.converters.registry_list import _all_handlers, build_all_converters
from pentaho_converter.step_xml import _STRUCTURED_STEP_TYPES, is_structured_step_type
from pentaho_converter.steps.base import build_default_registry
from pentaho_converter.validation.registry import get_validator
from pentaho_converter.validation.step_validators import register_builtin_validators


def _norm(t: str) -> str:
    return (t or "").strip().lower().replace(" ", "").replace("(", "").replace(")", "")


class TestRegistryCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_builtin_validators()
        cls.converters = build_all_converters()
        cls.types: set[str] = set()
        for conv in cls.converters:
            for t in conv.step_types:
                cls.types.add(_norm(t))

    def test_no_empty_type_stubs_in_handler_list(self):
        stubs = [
            f"{type(h).__module__}.{type(h).__name__}"
            for h in _all_handlers()
            if not (getattr(h, "_TYPES", set()) or set())
        ]
        self.assertEqual(stubs, [], f"Empty _TYPES stubs still registered: {stubs}")

    def test_no_duplicate_formula_registration(self):
        formula_handlers = [
            type(h).__name__
            for h in _all_handlers()
            if "formula" in { _norm(t) for t in (getattr(h, "_TYPES", set()) or set()) }
        ]
        self.assertEqual(
            formula_handlers,
            ["ScriptingFormulaHandler"],
            "Formula must be registered once via ScriptingFormulaHandler",
        )

    def test_every_registered_type_has_non_generic_or_generic_validator(self):
        missing = [t for t in sorted(self.types) if get_validator(t) is None]
        self.assertEqual(missing, [], f"Types with no validator: {missing}")

    def test_all_registered_types_are_structured(self):
        missing = sorted(t for t in self.types if t not in _STRUCTURED_STEP_TYPES)
        self.assertEqual(
            missing,
            [],
            f"Registered types missing from _STRUCTURED_STEP_TYPES: {missing}",
        )
        for t in ("parquetinput", "orcoutput", "splunkinput", "regexreplace", "top"):
            self.assertTrue(is_structured_step_type(t), t)

    def test_default_registry_converts_canonical_gaps(self):
        registry = build_default_registry()
        for step_type in (
            "ParquetInput", "OrcOutput", "HadoopFileInput", "Top",
            "RegexReplace", "SplunkInput", "SystemInfo", "ReplaceNull",
        ):
            converter = registry.get_converter(step_type)
            self.assertIsNotNone(converter, step_type)
            self.assertIsNot(
                converter,
                registry._fallback,
                f"{step_type} should not fall through to Fallback",
            )


if __name__ == "__main__":
    unittest.main()
