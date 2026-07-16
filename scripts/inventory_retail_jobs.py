"""Inventory Retail child jobs for conversion."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

RETAIL = Path(
    r"C:\Users\Prateek.Kotian\Desktop\Pentaho\Retail & E-commerce\Retail_ETL_Project"
)


def main() -> None:
    for kjb in sorted((RETAIL / "jobs").rglob("*.kjb")):
        if kjb.name == "Master_ETL.kjb":
            continue
        root = ET.parse(kjb).getroot()
        print(f"=== {kjb.relative_to(RETAIL)} ===")
        for e in root.findall("./entries/entry"):
            t = (e.findtext("type") or "").strip()
            n = (e.findtext("name") or "").strip()
            fn = (e.findtext("filename") or "").strip()
            tn = (e.findtext("transname") or "").strip()
            if t in {"TRANS", "JOB"}:
                print(f"  {t}: {n} -> {tn or fn}")
            elif t in {"SPECIAL", "SUCCESS", "ABORT"}:
                print(f"  {t}: {n}")
        hops = root.findall("./hops/hop")
        print(f"  hops={len(hops)}")


if __name__ == "__main__":
    main()
