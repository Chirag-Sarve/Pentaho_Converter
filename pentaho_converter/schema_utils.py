"""Map Pentaho field types to Spark schema DDL for file readers."""

from __future__ import annotations

from typing import Any


def spark_cast_type(type_name: str) -> str:
    """Map a Pentaho type name to a Spark SQL type (lowercase)."""
    t = (type_name or "String").strip().lower()
    mapping = {
        "integer": "int",
        "int": "int",
        "long": "bigint",
        "number": "double",
        "bignumber": "decimal(38,18)",
        "float": "float",
        "double": "double",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "datetime": "timestamp",
        "binary": "binary",
    }
    return mapping.get(t, "string")


def fields_to_schema_ddl(fields: list[dict[str, Any]]) -> str | None:
    """Build a Spark struct DDL string from Pentaho field metadata."""
    named = [field for field in fields if field.get("name")]
    if not named:
        return None

    ddl_parts: list[str] = []
    for field in named:
        type_name = field.get("type") or field.get("type_name") or "String"
        ddl_parts.append(f"{field['name']} {spark_cast_type(type_name).upper()}")
    return ", ".join(ddl_parts)
