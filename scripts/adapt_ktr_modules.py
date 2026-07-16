"""Adapt converter output into independent run(spark, config) modules."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "databricks_project/src/pentaho_migration/transformations/_raw"
OUT = ROOT / "databricks_project/src/pentaho_migration/transformations"
PKG = ROOT / "databricks_project/src/pentaho_migration"

SAMPLE_NAMES = {"Customer_Load", "Complex_Business_Logic", "Sales_Load"}

FUNC_RE = re.compile(r"^def (run_\w+)\(spark\):\n(.*)\Z", re.M | re.S)
SINGLE_TUPLE_RE = re.compile(
    r"\((None|'[^']*'|\"[^\"]*\"|\d+(?:\.\d+)?)\)(?=\s*,|\s*\])"
)
PARAM_RE = re.compile(r"^([A-Z][A-Z0-9_]*) = (.+)$")

IMPORT_CANDIDATES = [
    "broadcast",
    "col",
    "count",
    "current_date",
    "current_timestamp",
    "date_add",
    "length",
    "lit",
    "lower",
    "regexp_replace",
    "substring",
    "to_date",
    "trim",
    "upper",
    "when",
    "coalesce",
    "expr",
]


def to_snake(name: str) -> str:
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return s.replace("-", "_").replace(" ", "_").lower()


def fix_single_col_tuples(body: str) -> str:
    def _repl_data(match: re.Match[str]) -> str:
        return SINGLE_TUPLE_RE.sub(r"(\1,)", match.group(0))

    return re.sub(r"data = \[.*?\]", _repl_data, body, flags=re.S)


def dedent_body(body: str) -> list[str]:
    lines = []
    for line in body.splitlines():
        lines.append(line[4:] if line.startswith("    ") else line)
    return lines


def build_imports(body: str) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for name in IMPORT_CANDIDATES:
        if re.search(rf"\b{name}\b", body) and name not in seen:
            seen.add(name)
            ordered.append(name)
    if re.search(r"\b_sum\b", body) and "sum as _sum" not in seen:
        ordered.append("sum as _sum")

    lines = [
        "from pyspark.sql import DataFrame, SparkSession",
        "from pyspark.sql.functions import (",
    ]
    for name in ordered:
        lines.append(f"    {name},")
    lines.append(")")
    return "\n".join(lines)


def rewrite_body_lines(lines: list[str]) -> str:
    rewritten: list[str] = []
    for line in lines:
        if line.strip().startswith('"""Execute transformation:'):
            continue
        line = (
            line.replace("TARGET_CATALOG", "catalog")
            .replace("TARGET_SCHEMA", "schema")
            .replace("PENTAHO_DATA_DIR", "data_dir")
        )
        stripped = line.strip()
        m = PARAM_RE.match(stripped)
        if m and m.group(1) in {"BATCH_DATE", "MIN_ORDER_AMOUNT", "DISCOUNT_RATE"}:
            key, val = m.group(1), m.group(2)
            rewritten.append(f"    {key} = config.get({key!r}, {val})")
            continue
        rewritten.append(f"    {line}" if line.strip() else "")

    text = "\n".join(rewritten)
    text = text.replace(
        ".load('/data/sales.csv')",
        ".load(config.get('sales_csv', '/data/sales.csv'))",
    )
    text = text.replace(
        ".load('/data/inbound/product_prices.csv')",
        ".load(config.get('product_prices_csv', '/data/inbound/product_prices.csv'))",
    )
    return text


def adapt(path: Path) -> Path:
    code = path.read_text(encoding="utf-8")
    source_name = path.stem
    match = FUNC_RE.search(code)
    if not match:
        raise ValueError(f"No run_* function in {path}")

    body = fix_single_col_tuples(match.group(2))
    body_text = rewrite_body_lines(dedent_body(body))
    schema_default = "analytics" if source_name in SAMPLE_NAMES else "default"

    module = f'''"""PySpark module migrated from Pentaho transformation: {source_name}.

Source: {source_name}.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

{build_imports(body_text)}


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``{source_name}`` step-for-step.

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Runtime configuration. Recognized keys include ``catalog``, ``schema``,
        ``data_dir``, transformation parameters, and path overrides.

    Returns
    -------
    DataFrame
        Downstream DataFrame after the final hop(s).
    """
    config = dict(config or {{}})
    catalog = config.get("catalog", "main")
    schema = config.get("schema", {schema_default!r})
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")

{body_text.rstrip()}
'''
    out_path = OUT / f"{to_snake(source_name)}.py"
    out_path.write_text(module, encoding="utf-8")
    return out_path


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    PKG.mkdir(parents=True, exist_ok=True)
    written = [adapt(p) for p in sorted(RAW.glob("*.py"))]
    (PKG / "__init__.py").write_text(
        '"""Pentaho → PySpark migration package (independent transformation modules)."""\n',
        encoding="utf-8",
    )
    (OUT / "__init__.py").write_text(
        '"""One PySpark module per Pentaho .ktr transformation."""\n',
        encoding="utf-8",
    )
    print(f"Wrote {len(written)} modules:")
    for path in written:
        print(f"  {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
