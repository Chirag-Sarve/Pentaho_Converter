"""Add Databricks structured logging / AQE hints to child jobs (no hop changes)."""
from __future__ import annotations

import re
from pathlib import Path

CHILDREN = (
    Path(__file__).resolve().parents[1]
    / "databricks_project/src/pentaho_migration/jobs/children"
)

IMP = """from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    get_logger,
    log_event,
    timed,
)
"""


def optimize(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    if "def run(" not in text or "apply_spark_runtime_hints" in text:
        return False
    original = text
    if "pentaho_migration.common.databricks_opt" not in text:
        text = text.replace(
            "from pentaho_migration.job_engine import (",
            IMP + "from pentaho_migration.job_engine import (",
            1,
        )
    m = re.search(r'logger = logging\.getLogger\("([^"]+)"\)', text)
    log_name = m.group(1) if m else f"Master_ETL.children.{path.stem}"
    if "_LOG = get_logger" not in text:
        if m:
            text = text.replace(
                m.group(0),
                m.group(0) + f'\n_LOG = get_logger("{log_name}")',
                1,
            )
        else:
            text = text.replace(
                "EXPANDED = True\n",
                f'EXPANDED = True\n\n_LOG = get_logger("{log_name}")\n',
                1,
            )
    if "apply_spark_runtime_hints(spark, config)" not in text:
        text = text.replace(
            "config = dict(config or {})\n",
            "config = dict(config or {})\n"
            "    if spark is not None:\n"
            "        apply_spark_runtime_hints(spark, config)\n"
            '    log_event(_LOG, "job_start", job=JOB_NAME)\n',
            1,
        )
    if 'log_event(_LOG, "job_end"' not in text:
        text = re.sub(
            r"(final = runtime\.run\(\)\n)",
            r"\1"
            '    log_event(_LOG, "job_end", success=final.success, last=final.name,\n'
            "              steps=len(runtime.executed))\n",
            text,
            count=1,
        )
    if text == original:
        return False
    compile(text, str(path), "exec")
    path.write_text(text, encoding="utf-8", newline="\n")
    return True


def main() -> None:
    n = 0
    for path in sorted(CHILDREN.glob("*.py")):
        if path.name.startswith("_"):
            continue
        if optimize(path):
            print("optimized", path.name)
            n += 1
    print(f"Done — {n} child jobs")


if __name__ == "__main__":
    main()
