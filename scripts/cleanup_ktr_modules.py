"""Post-clean migrated transformation modules."""
from __future__ import annotations

import re
import shutil
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / (
    "databricks_project/src/pentaho_migration/transformations"
)


def main() -> None:
    for path in OUT.glob("*.py"):
        if path.name == "__init__.py":
            continue
        text = path.read_text(encoding="utf-8")
        text2 = re.sub(
            r'\n    """Execute transformation: [^"]+"""\n',
            "\n",
            text,
        )
        if text2 != text:
            path.write_text(text2, encoding="utf-8")
            print(f"cleaned docstring: {path.name}")

    complex_path = OUT / "complex_business_logic.py"
    text = complex_path.read_text(encoding="utf-8")
    old = (
        "writer.mode('overwrite').save(f'{data_dir}/order_summary_audit.txt')\n\n"
        "    return df_Text_file_audit_log"
    )
    new = (
        "audit_path = config.get(\n"
        "        'order_summary_audit',\n"
        "        f'{data_dir}/order_summary_audit.txt',\n"
        "    )\n"
        "    writer.mode('overwrite').save(audit_path)\n\n"
        "    return df_Aggregate_by_region"
    )
    if old not in text:
        raise SystemExit("complex_business_logic expected snippet not found")
    complex_path.write_text(text.replace(old, new), encoding="utf-8")
    print("updated complex_business_logic return/audit path")

    raw = OUT / "_raw"
    if raw.exists():
        shutil.rmtree(raw)
        print("removed _raw")


if __name__ == "__main__":
    main()
