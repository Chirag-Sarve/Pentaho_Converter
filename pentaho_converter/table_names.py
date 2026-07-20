"""Resolve Pentaho JDBC/H2 schema names to Databricks Unity Catalog table identifiers."""

from __future__ import annotations

from .generation_config import GenerationConfig

# Source DB default schemas that should not be used as Databricks schema names.
_GENERIC_SOURCE_SCHEMAS = frozenset({
    "PUBLIC",
    "DBO",
    "SA",
    "DEFAULT",
    "INFORMATION_SCHEMA",
    "SYS",
    "SYSTEM",
    "ROOT",
    "MAIN",
})


def resolve_target_schema(source_schema: str, default_schema: str = "default") -> str:
    """Map a Pentaho/JDBC schema to a Databricks schema name."""
    name = (source_schema or "").strip()
    if not name or name.upper() in _GENERIC_SOURCE_SCHEMAS:
        return default_schema
    return name.lower()


def qualify_table_name(
    table: str,
    source_schema: str = "",
    *,
    config: GenerationConfig | None = None,
    use_target_vars: bool = False,
) -> str:
    """Build a Unity Catalog table name: catalog.schema.table."""
    cfg = config or GenerationConfig.defaults()
    table_name = (table or "target_table").strip()
    if use_target_vars:
        return f'{{TARGET_CATALOG}}.{{TARGET_SCHEMA}}.{table_name}'

    catalog = (cfg.catalog or "workspace").strip()
    schema = resolve_target_schema(source_schema, cfg.schema or "default")
    return f"{catalog}.{schema}.{table_name}"


def table_write_lines(
    *,
    out_var: str,
    in_df: str,
    table: str,
    source_schema: str,
    step_name: str,
    config: GenerationConfig | None = None,
    mode: str = "overwrite",
    step_type: str = "TableOutput",
) -> list[str]:
    """Generate PySpark lines for Delta ``saveAsTable`` (Unity Catalog)."""
    table_name = (table or "").strip()
    pentaho_schema = (source_schema or "").strip()
    schema_note = f" (Pentaho schema: {pentaho_schema})" if pentaho_schema else ""

    lines = [f"# Pentaho step: {step_name} (type: {step_type}){schema_note}"]
    lines.append(f"{out_var} = {in_df}")
    if not table_name:
        lines.append("# WARNING: Target table name missing from Pentaho metadata.")
        lines.append(
            f"{out_var}.write \\\n"
            f"    .format(\"delta\") \\\n"
            f"    .mode({mode!r}) \\\n"
            f"    .saveAsTable(\n"
            f"        f\"{{TARGET_CATALOG}}.{{TARGET_SCHEMA}}.<table_name>\"\n"
            f"    )"
        )
        return lines

    lines.append("spark.sql(f'CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}')")
    lines.append(
        f"{out_var}.write \\\n"
        f"    .format(\"delta\") \\\n"
        f"    .mode({mode!r}) \\\n"
        f"    .saveAsTable(\n"
        f"        f\"{{TARGET_CATALOG}}.{{TARGET_SCHEMA}}.{table_name}\"\n"
        f"    )"
    )
    return lines
