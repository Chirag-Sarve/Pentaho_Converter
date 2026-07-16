"""Apply Databricks optimizations to generated transformation modules.

Preserves business expressions — only injects logging, Delta helpers, broadcast
on known-small lookup/dim join sides, cache helpers, and AQE session hints.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRANS = ROOT / "databricks_project/src/pentaho_migration/transformations"
JOBS = ROOT / "databricks_project/src/pentaho_migration/jobs"

IMPORT_BLOCK = """from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    cache_for_reuse,
    get_logger,
    log_event,
    timed,
    unpersist_quiet,
    write_delta,
)
"""

SKIP_FILES = {
    "customer_load.py",
    "sales_load.py",
    "complex_business_logic.py",
    "__init__.py",
}

# Right-hand DataFrame names that are dimension/lookup-sized → broadcast-safe.
BROADCAST_RHS_NAMES = (
    "df_Sort_Products_By_Product",
    "df_Sort_Categories",
    "df_Read_Category",
    "df_Region_reference",
)


def module_logger_name(path: Path) -> str:
    rel = path.as_posix().replace("\\", "/")
    if "pentaho_migration/" in rel:
        rel = rel.split("pentaho_migration/")[-1]
    return "pentaho_migration." + rel.replace("/", ".").removesuffix(".py")


def ensure_imports(text: str, mod_log: str) -> str:
    if "pentaho_migration.common.databricks_opt" in text:
        needed = [
            "apply_spark_runtime_hints",
            "cache_for_reuse",
            "get_logger",
            "log_event",
            "write_delta",
        ]
        for name in needed:
            if name not in text:
                text = text.replace(
                    "from pentaho_migration.common.databricks_opt import (",
                    f"from pentaho_migration.common.databricks_opt import (\n    {name},",
                    1,
                )
        if "_LOG = get_logger" not in text:
            # After import block
            text = re.sub(
                r"(from pentaho_migration\.common\.databricks_opt import \([^)]+\))\n",
                rf'\1\n\n_LOG = get_logger("{mod_log}")\n',
                text,
                count=1,
                flags=re.DOTALL,
            )
        return text

    # Prefer insert after pyspark.sql.functions import (multiline) or single spark import
    patterns = [
        r"(from pyspark\.sql\.functions import \([\s\S]*?\)\n)",
        r"(from pyspark\.sql import [^\n]+\n)",
        r"(from typing import Any, Mapping\n)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            insert = IMPORT_BLOCK + f'\n_LOG = get_logger("{mod_log}")\n\n'
            return text[: m.end()] + insert + text[m.end() :]
    return IMPORT_BLOCK + f'\n_LOG = get_logger("{mod_log}")\n\n' + text


def inject_runtime_hints(text: str) -> str:
    if "apply_spark_runtime_hints(spark, config)" in text:
        return text
    pattern = r"(config = dict\(config or \{\}\)\n)"
    repl = (
        r"\1"
        "    apply_spark_runtime_hints(spark, config)\n"
        '    log_event(_LOG, "transformation_start")\n'
    )
    text2, n = re.subn(pattern, repl, text, count=1)
    return text2 if n else text


def rewrite_delta_writes(text: str) -> str:
    """Replace inline delta saveAsTable chains with write_delta helper."""

    pattern = re.compile(
        r"(?P<indent>[ \t]*)(?:spark\.sql\(\s*f?['\"]CREATE SCHEMA IF NOT EXISTS \{catalog\}\.\{schema\}['\"]\s*\)\s*\n)?"
        r"(?P=indent)(?P<df>df_\w+)\.write\s*\\\s*\n"
        r"[ \t]*\.format\(\s*[\"']delta[\"']\s*\)\s*\\\s*\n"
        r"[ \t]*\.mode\(\s*(?P<mode>[\"']\w+[\"'])\s*\)\s*\\\s*\n"
        r"[ \t]*\.saveAsTable\(\s*\n"
        r"[ \t]*(?P<table>f?[\"'][^\"']+[\"'])\s*\n"
        r"[ \t]*\)",
        re.MULTILINE,
    )

    def _sub(m: re.Match[str]) -> str:
        indent = m.group("indent")
        df = m.group("df")
        mode = m.group("mode")
        table = m.group("table")
        return (
            f"{indent}write_delta(\n"
            f"{indent}    {df},\n"
            f"{indent}    {table},\n"
            f"{indent}    mode={mode},\n"
            f"{indent}    partition_by=config.get('partition_by') or [],\n"
            f"{indent}    target_files=config.get('target_files'),\n"
            f"{indent}    spark=spark,\n"
            f"{indent})\n"
            f'{indent}log_event(_LOG, "delta_write", table={table}, mode={mode})'
        )

    text, n1 = pattern.subn(_sub, text)

    pattern2 = re.compile(
        r"(?P<indent>[ \t]*)(?:spark\.sql\(\s*f?['\"]CREATE SCHEMA IF NOT EXISTS \{catalog\}\.\{schema\}['\"]\s*\)\s*\n)?"
        r"(?P=indent)(?P<df>df_\w+)\.write\.format\(\s*[\"']delta[\"']\s*\)"
        r"\.mode\(\s*(?P<mode>[\"']\w+[\"'])\s*\)\.saveAsTable\(\s*(?P<table>[^)]+)\)"
    )

    def _sub2(m: re.Match[str]) -> str:
        indent = m.group("indent")
        return (
            f"{indent}write_delta(\n"
            f"{indent}    {m.group('df')},\n"
            f"{indent}    {m.group('table')},\n"
            f"{indent}    mode={m.group('mode')},\n"
            f"{indent}    partition_by=config.get('partition_by') or [],\n"
            f"{indent}    target_files=config.get('target_files'),\n"
            f"{indent}    spark=spark,\n"
            f"{indent})\n"
            f'{indent}log_event(_LOG, "delta_write", table={m.group("table")}, mode={m.group("mode")})'
        )

    text, n2 = pattern2.subn(_sub2, text)
    return text


def replace_cache_calls_safe(text: str) -> str:
    """Prefer cache_for_reuse helper; same fan-out semantics as .cache()."""
    return re.sub(
        r"(\bdf_\w+\s*=\s*)(df_\w+)\.cache\(\)",
        r"\1cache_for_reuse(\2)",
        text,
    )


def add_broadcast_on_small_rhs(text: str) -> str:
    """Wrap known-small RHSs in broadcast() when not already wrapped."""
    if "broadcast" not in text and any(n in text for n in BROADCAST_RHS_NAMES):
        # ensure broadcast import from pyspark
        if "from pyspark.sql.functions import (" in text:
            if "\nbroadcast," not in text and "broadcast," not in text.split(
                "from pyspark.sql.functions import"
            )[1][:400]:
                text = text.replace(
                    "from pyspark.sql.functions import (",
                    "from pyspark.sql.functions import (\n    broadcast,",
                    1,
                )
        elif "from pyspark.sql.functions import" in text:
            text = text.replace(
                "from pyspark.sql.functions import ",
                "from pyspark.sql.functions import broadcast, ",
                1,
            )

    for name in BROADCAST_RHS_NAMES:
        # .join(name, → .join(broadcast(name),
        text = re.sub(
            rf"\.join\(\s*{re.escape(name)}\s*,",
            f".join(broadcast({name}),",
            text,
        )
        # avoid double-wrap
        text = text.replace(
            f"broadcast(broadcast({name}))",
            f"broadcast({name})",
        )
    return text


def inject_end_log(text: str) -> str:
    if 'log_event(_LOG, "transformation_end"' in text:
        return text
    lines = text.splitlines(keepends=True)
    for i in range(len(lines) - 1, -1, -1):
        m = re.match(r"^([ \t]+)return (.+)\s*$", lines[i])
        if m and "def " not in "".join(lines[max(0, i - 5) : i]):
            indent = m.group(1)
            # Ensure we're inside run() — last return in file is fine for generated modules
            lines.insert(i, f'{indent}log_event(_LOG, "transformation_end")\n')
            return "".join(lines)
    return text


def optimize_file(path: Path) -> bool:
    if path.name in SKIP_FILES or path.name.startswith("_"):
        return False
    text = path.read_text(encoding="utf-8")
    if "def run(" not in text:
        return False
    original = text
    mod_log = module_logger_name(path)
    text = ensure_imports(text, mod_log)
    text = inject_runtime_hints(text)
    text = rewrite_delta_writes(text)
    text = replace_cache_calls_safe(text)
    text = add_broadcast_on_small_rhs(text)
    text = inject_end_log(text)
    if text != original:
        path.write_text(text, encoding="utf-8", newline="\n")
        return True
    return False


def optimize_job(path: Path, job_name: str) -> None:
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8")
    if "apply_spark_runtime_hints" in text and "log_event(_LOG" in text:
        return
    if "pentaho_migration.common.databricks_opt" not in text:
        # Insert before job_engine import
        if "from pentaho_migration.job_engine import" in text:
            text = text.replace(
                "from pentaho_migration.job_engine import",
                "from pentaho_migration.common.databricks_opt import (\n"
                "    apply_spark_runtime_hints,\n"
                "    get_logger,\n"
                "    log_event,\n"
                "    timed,\n"
                ")\n"
                "from pentaho_migration.job_engine import",
                1,
            )
        else:
            text = (
                "from pentaho_migration.common.databricks_opt import (\n"
                "    apply_spark_runtime_hints,\n"
                "    get_logger,\n"
                "    log_event,\n"
                "    timed,\n"
                ")\n"
                + text
            )
    if f'_LOG = get_logger("{job_name}")' not in text:
        # After module-level logger if any
        if f'logging.getLogger("{job_name}")' in text:
            text = text.replace(
                f'logger = logging.getLogger("{job_name}")',
                f'logger = logging.getLogger("{job_name}")\n'
                f'_LOG = get_logger("{job_name}")',
                1,
            )
        else:
            # after imports / JOB_NAME
            text = re.sub(
                rf'(JOB_NAME\s*=\s*"{re.escape(job_name)}"\n)',
                rf'\1\n_LOG = get_logger("{job_name}")\n',
                text,
                count=1,
            )
            if f'_LOG = get_logger("{job_name}")' not in text:
                text = text.replace(
                    "from __future__ import annotations\n",
                    "from __future__ import annotations\n\n"
                    f'_LOG = get_logger("{job_name}")  # noqa: E402 — set after imports\n',
                    1,
                )
                # Move _LOG after imports properly
                text = text.replace(
                    f'_LOG = get_logger("{job_name}")  # noqa: E402 — set after imports\n',
                    "",
                    1,
                )
                # place after databricks_opt import
                text = text.replace(
                    "    timed,\n)\n",
                    f'    timed,\n)\n\n_LOG = get_logger("{job_name}")\n',
                    1,
                )

    # In run(): after config =
    if "apply_spark_runtime_hints(spark, config)" not in text:
        text = text.replace(
            "config = dict(config or {})\n",
            "config = dict(config or {})\n"
            "    if spark is not None:\n"
            "        apply_spark_runtime_hints(spark, config)\n"
            f'    log_event(_LOG, "job_start", job="{job_name}")\n',
            1,
        )
    path.write_text(text, encoding="utf-8", newline="\n")
    print(f"Optimized job {path.name}")


def main() -> None:
    changed = 0
    for path in sorted(TRANS.rglob("*.py")):
        if optimize_file(path):
            changed += 1
            print("optimized", path.relative_to(ROOT).as_posix())
    optimize_job(JOBS / "Master_ETL.py", "Master_ETL")
    optimize_job(JOBS / "master.py", "Master")
    print(f"Done — {changed} transformation modules updated")


if __name__ == "__main__":
    main()
