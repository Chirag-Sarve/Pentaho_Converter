"""Regression harness: convert every .ktr/.kjb in the repository."""
from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pentaho_converter.models import ConversionStats
from pentaho_converter.pipeline import convert_pentaho_project
from pentaho_converter.transformation_parser import parse_transformation
from pentaho_converter.models import ConversionStats as CS
from pentaho_converter.code_generator import PySparkCodeGenerator
from pentaho_converter.steps.base import build_default_registry


def convert_ktr(path: Path) -> dict:
    trans = parse_transformation(path)
    stats = CS()
    logs: list[str] = []
    code = PySparkCodeGenerator(build_default_registry()).generate_transformation(trans, stats, logs)
    issues = []
    for s in stats.step_results:
        if s.status != "converted" or s.semantic_score < 0.95:
            issues.append(f"{s.step_name}({s.step_type}): {s.status} {s.semantic_score:.0%} {s.errors}")
    if "_placeholder" in code:
        issues.append("contains _placeholder")
    if "return df_" in code:
        ret_line = [l for l in code.splitlines() if l.strip().startswith("return ")][-1]
        if "df_" not in ret_line:
            issues.append(f"bad return: {ret_line.strip()}")
    return {
        "file": str(path.relative_to(ROOT)),
        "steps": len(stats.step_results),
        "converted": sum(1 for s in stats.step_results if s.status == "converted"),
        "avg_score": sum(s.semantic_score for s in stats.step_results) / max(1, len(stats.step_results)),
        "issues": issues,
        "code_snippet": "\n".join(code.splitlines()[-8:]),
    }


def convert_zip_project() -> dict:
    buf = io.BytesIO()
    samples = ROOT / "samples"
    with zipfile.ZipFile(buf, "w") as zf:
        for p in samples.rglob("*"):
            if p.is_file():
                zf.write(p, p.relative_to(samples).as_posix())
    result = convert_pentaho_project(buf.getvalue(), "Master")
    issues = []
    for s in result.stats.step_results:
        if s.status != "converted" or s.semantic_score < 0.95:
            issues.append(f"{s.step_name}: {s.status}")
    return {
        "file": "samples/Master (zip)",
        "steps": len(result.stats.step_results),
        "converted": sum(1 for s in result.stats.step_results if s.status == "converted"),
        "avg_score": result.stats.semantic_accuracy_percent / 100,
        "issues": issues,
    }


def main() -> None:
    results = []
    for ktr in sorted(ROOT.rglob("*.ktr")):
        results.append(convert_ktr(ktr))
    results.append(convert_zip_project())
    print("=== REGRESSION REPORT ===")
    for r in results:
        print(f"\n{r['file']}: {r['converted']}/{r['steps']} converted, score={r['avg_score']:.0%}")
        for i in r["issues"]:
            print(f"  ISSUE: {i}")
        if not r["issues"]:
            print("  OK")
    failed = sum(1 for r in results if r["issues"])
    print(f"\nTotal files: {len(results)}, with issues: {failed}")


if __name__ == "__main__":
    main()
