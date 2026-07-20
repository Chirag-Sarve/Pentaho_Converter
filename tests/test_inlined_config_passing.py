"""Config is loaded once in run_*; only referenced keys are passed to steps."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import MagicMock

from pentaho_converter.code_generator import (
    PySparkCodeGenerator,
    _config_keys_referenced,
    _needs_config_mapping,
    _transformation_config_keys,
)
from pentaho_converter.conversion_outcome import StepConversionOutcome
from pentaho_converter.models import (
    ConversionStats,
    PentahoHop,
    PentahoStep,
    PentahoTransformation,
)


class TestConfigKeyHelpers(unittest.TestCase):
    def test_referenced_keys_are_detected(self):
        keys = ["PROJECT_HOME", "OUTPUT_PATH", "LOG_PATH"]
        self.assertEqual(
            _config_keys_referenced(
                ["df.write.csv(f'{OUTPUT_PATH}/out')"],
                keys,
            ),
            ["OUTPUT_PATH"],
        )
        self.assertEqual(
            _config_keys_referenced(["df = df.filter(col('x') > 1)"], keys),
            [],
        )

    def test_needs_config_mapping(self):
        self.assertTrue(_needs_config_mapping(["n = config.get('partition_by')"]))
        self.assertTrue(_needs_config_mapping(["v = config['OUTPUT_PATH']"]))
        self.assertFalse(_needs_config_mapping(["df = df.filter(col('a') == 1)"]))

    def test_transformation_config_keys_preserve_order(self):
        trans = PentahoTransformation(
            name="T",
            file_path=Path("T.ktr"),
            parameters={"PROJECT_HOME": "/p", "OUTPUT_PATH": "/o"},
            variables={"LOG_PATH": "/l", "PROJECT_HOME": "/ignored"},
        )
        self.assertEqual(
            _transformation_config_keys(trans),
            ["PROJECT_HOME", "OUTPUT_PATH", "LOG_PATH"],
        )


class TestInlinedConfigPassing(unittest.TestCase):
    def test_only_used_keys_passed_to_steps(self):
        trans = PentahoTransformation(
            name="Demo",
            file_path=Path("Demo.ktr"),
            steps=[
                PentahoStep(name="Read", step_type="TextFileInput"),
                PentahoStep(name="Filter", step_type="FilterRows"),
                PentahoStep(name="Write", step_type="TextFileOutput"),
            ],
            hops=[
                PentahoHop(from_name="Read", to_name="Filter"),
                PentahoHop(from_name="Filter", to_name="Write"),
            ],
            parameters={
                "PROJECT_HOME": "/data",
                "OUTPUT_PATH": "/out",
                "LOG_PATH": "/logs",
            },
        )

        code_by_step = {
            "Read": ["read_df = spark.read.csv(f'{PROJECT_HOME}/in.csv')"],
            "Filter": ["filter_df = read_df.filter(col('id').isNotNull())"],
            "Write": [
                "write_df = filter_df",
                "write_df.write.mode('overwrite').csv(f'{OUTPUT_PATH}/out')",
            ],
        }

        def fake_convert(_step_type, context):
            step = context.step
            return StepConversionOutcome(
                code_lines=list(code_by_step[step.name]),
                status="converted",
                semantic_score=1.0,
                detail=step.name,
            )

        registry = MagicMock()
        registry.convert_step.side_effect = fake_convert
        gen = PySparkCodeGenerator(registry=registry)
        lines, _next, run_name = gen.generate_inlined_transformation_block(
            trans, ConversionStats(), []
        )
        code = "\n".join(lines)

        self.assertEqual(run_name, "run_Demo")
        self.assertIn("PROJECT_HOME = config.get('PROJECT_HOME', '/data')", code)
        self.assertIn("OUTPUT_PATH = config.get('OUTPUT_PATH', '/out')", code)
        self.assertIn("LOG_PATH = config.get('LOG_PATH', '/logs')", code)

        self.assertIn("def step_01_Read(spark, PROJECT_HOME):", code)
        self.assertIn("def step_02_Filter(spark, read_df):", code)
        self.assertIn("def step_03_Write(spark, filter_df, OUTPUT_PATH):", code)

        self.assertIn("read_df = step_01_Read(spark, PROJECT_HOME)", code)
        self.assertIn("filter_df = step_02_Filter(spark, read_df)", code)
        self.assertIn("write_df = step_03_Write(spark, filter_df, OUTPUT_PATH)", code)

        step_region = code.split("def run_Demo")[0]
        self.assertNotIn("config.get(", step_region)


if __name__ == "__main__":
    unittest.main()
