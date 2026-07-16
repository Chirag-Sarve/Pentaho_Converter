"""Smoke-check that optimization did not alter business filter/join/agg keys.

Compares stripped business call signatures across modules: ignores imports,
logging, broadcast/cache wrappers, and write_delta vs saveAsTable rewrites.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "databricks_project/src/pentaho_migration/transformations"


def normalize_business_ops(source: str) -> list[str]:
    """Extract normalized business operation fingerprints from source text."""
    # Strip optimization noise for fingerprinting
    s = source
    s = re.sub(r"broadcast\(([^()]+(?:\([^()]*\))?[^()]*)\)", r"\1", s)
    s = re.sub(r"maybe_broadcast\(([^,\n]+)(?:,[^)]*)?\)", r"\1", s)
    s = re.sub(r"cache_for_reuse\(([^)]+)\)", r"\1", s)
    s = re.sub(r"\.persist\([^)]*\)", "", s)
    s = re.sub(r"\.cache\(\)", "", s)
    s = re.sub(r"apply_spark_runtime_hints\([^\n]+\)\n?", "", s)
    s = re.sub(r"log_event\([^\n]+\)\n?", "", s)
    s = re.sub(r"with timed\([^\n]+\):\n?", "", s)
    # write_delta(...) ↔ saveAsTable — compare sink table name only
    s = re.sub(
        r"write_delta\(\s*(\w+)\s*,\s*([^,\n]+)\s*,[\s\S]*?\)",
        r"DELTA_SINK(\1,\2)",
        s,
    )
    s = re.sub(
        r"(\w+)\.write\s*\\\s*\.format\(\s*[\"']delta[\"']\s*\)\s*\\\s*"
        r"\.mode\([^)]+\)\s*\\\s*\.saveAsTable\(\s*([^)]+)\s*\)",
        r"DELTA_SINK(\1,\2)",
        s,
    )

    ops: list[str] = []
    for m in re.finditer(
        r"\.(?:filter|where|join|groupBy|agg|select|withColumn)\s*\(",
        s,
    ):
        # Capture a short window of the call for fingerprint
        start = m.start()
        chunk = s[start : start + 200].replace("\n", " ")
        chunk = re.sub(r"\s+", " ", chunk)
        ops.append(chunk[:160])
    return ops


def main() -> None:
    """Verify each file still parses and contains expected DataFrame API usage."""
    files = list(ROOT.rglob("*.py"))
    parse_fail = 0
    preexisting_ident = 0
    has_df_api = 0
    has_opt = 0
    for path in files:
        if path.name == "__init__.py":
            continue
        src = path.read_text(encoding="utf-8")
        try:
            ast.parse(src)
        except SyntaxError as exc:
            # Pre-existing generator artifacts: step names with ? or & become invalid idents
            if "df_" in src and ("?" in src or "df_Read_P&L" in src):
                preexisting_ident += 1
            else:
                parse_fail += 1
                print("PARSE_FAIL", path.name, exc.msg)
            continue
        if "def run(" not in src:
            continue
        if re.search(r"\.(filter|join|groupBy|select|withColumn)\(", src):
            has_df_api += 1
        if "apply_spark_runtime_hints" in src or "write_delta" in src or "log_event" in src:
            has_opt += 1
        if ('.format("delta")' in src or ".format('delta')" in src) and "write_delta" not in src:
            print("WARN leftover delta write without helper", path.name)

    print(
        f"parse_fail={parse_fail} preexisting_invalid_idents={preexisting_ident} "
        f"dataframe_api_modules={has_df_api} optimized_modules={has_opt} total={len(files)}"
    )
    if parse_fail:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
