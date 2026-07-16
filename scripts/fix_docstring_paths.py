"""Fix Windows path backslashes in docstrings even when later identifiers are invalid."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "databricks_project/src/pentaho_migration"


def main() -> None:
    fixed = 0
    for path in ROOT.rglob("*.py"):
        src = path.read_text(encoding="utf-8")
        if "\\Users" not in src and "Source: C:\\" not in src:
            # also plain backslash Users after Source
            if "Source: C:" not in src or "\\" not in src.split("Source:", 1)[-1][:200]:
                continue
        lines = src.splitlines(keepends=True)
        out: list[str] = []
        changed = False
        for line in lines:
            if "Source:" in line and "\\" in line:
                new_line = line.replace("\\", "/")
                changed = changed or new_line != line
                out.append(new_line)
            else:
                out.append(line)
        if not changed:
            continue
        path.write_text("".join(out), encoding="utf-8", newline="\n")
        fixed += 1
        print("fixed", path.relative_to(ROOT).as_posix())
    print(f"fixed={fixed}")


if __name__ == "__main__":
    main()
