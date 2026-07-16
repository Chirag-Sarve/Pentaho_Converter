"""Formatting rules for concise, developer-facing generated PySpark."""

from __future__ import annotations


def remove_generator_comments(code_lines: list[str]) -> list[str]:
    """Retain only standardized ETL-flow comments in generated transformations."""
    clean_lines: list[str] = []
    for line in code_lines:
        stripped = line.lstrip()
        if stripped.startswith("#") and not stripped.startswith("# Step "):
            continue
        clean_lines.append(line)
    return clean_lines
