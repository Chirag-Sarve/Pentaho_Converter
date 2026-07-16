"""Diagnose and fix remaining \\U unicodeescape SyntaxErrors in retail modules."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "databricks_project/src/pentaho_migration"


def find_escapes(text: str) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for i, line in enumerate(text.splitlines(), 1):
        if "\\U" in line or "\\Users" in line:
            hits.append((i, line[:160]))
    return hits


def main() -> None:
    bad: list[Path] = []
    for path in ROOT.rglob("*.py"):
        src = path.read_text(encoding="utf-8")
        try:
            compile(src, str(path), "exec")
        except SyntaxError:
            bad.append(path)

    print(f"bad_count={len(bad)}")
    for path in bad[:3]:
        src = path.read_text(encoding="utf-8")
        print("---", path.name)
        for ln, preview in find_escapes(src)[:8]:
            print(f"  L{ln}: {preview!r}")

    # Aggressive fix: replace backslash in any Source: comment/docstring line
    # and any string/comment containing C:\Users
    fixed = 0
    for path in bad:
        src = path.read_text(encoding="utf-8")
        lines = src.splitlines(keepends=True)
        out: list[str] = []
        changed = False
        for line in lines:
            if "\\Users" in line or (line.lstrip().startswith("Source:") and "\\" in line):
                new_line = line.replace("\\", "/")
                if new_line != line:
                    changed = True
                out.append(new_line)
            else:
                out.append(line)
        new_src = "".join(out)
        try:
            compile(new_src, str(path), "exec")
        except SyntaxError as exc:
            print("STILL_FAIL", path.name, exc.msg, "lineno", exc.lineno)
            # Fall back: escape all backslashes in the top docstring only by
            # converting the whole leading docstring to use forward slashes.
            continue
        if changed:
            path.write_text(new_src, encoding="utf-8", newline="\n")
            fixed += 1
            print("fixed", path.relative_to(ROOT))
    print(f"fixed={fixed}")


if __name__ == "__main__":
    main()
