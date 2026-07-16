"""Convert Pentaho Mapping (sub-transformation) steps to modular PySpark.

Generates reusable child-function invocation instead of flattening the
referenced transformation. Mapping Input/Output Specification steps implement
the sub-transformation contract (schema validation and projection).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def safe_func_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", (name or "").strip())
    if cleaned and cleaned[0].isdigit():
        cleaned = f"trans_{cleaned}"
    return cleaned or "transformation"


def child_transformation_key(meta: dict[str, Any]) -> str:
    """Stable identifier for circular-reference detection and helper metadata."""
    return (
        (meta.get("filename") or "").strip()
        or (meta.get("trans_name") or "").strip()
        or (meta.get("directory_path") or "").strip()
        or (meta.get("trans_object_id") or "").strip()
        or ""
    )


def child_run_function_name(meta: dict[str, Any]) -> str:
    """Derive ``run_<name>`` for the child transformation when available."""
    raw = child_transformation_key(meta)
    if not raw:
        return "run_mapping_child"
    stem = Path(raw.replace("\\", "/")).stem or raw
    # Strip common Pentaho internal directory prefixes left after variable removal
    stem = re.sub(r"^\$\{[^}]+\}/?", "", stem)
    stem = Path(stem.replace("\\", "/")).stem or stem
    return f"run_{safe_func_name(stem)}"


def _main_io_definition(mappings: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not mappings:
        return None
    for item in mappings:
        if item.get("main_path") or not (
            item.get("input_step") or item.get("output_step")
        ):
            return item
    return mappings[0]


def collect_connectors(mappings: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    connectors: list[dict[str, str]] = []
    for mapping in mappings or []:
        for conn in mapping.get("connectors") or []:
            if isinstance(conn, dict) and (conn.get("parent") or conn.get("child")):
                connectors.append({
                    "parent": conn.get("parent") or "",
                    "child": conn.get("child") or conn.get("parent") or "",
                })
    return connectors


def _rename_lines(df_var: str, connectors: list[dict[str, str]], *, direction: str) -> list[str]:
    """Emit withColumnRenamed for parent↔child field connectors."""
    lines: list[str] = []
    for conn in connectors:
        parent = (conn.get("parent") or "").strip()
        child = (conn.get("child") or parent).strip()
        if not parent or not child or parent == child:
            continue
        if direction == "parent_to_child":
            src, dst = parent, child
        else:
            src, dst = child, parent
        lines.append(f'{df_var} = {df_var}.withColumnRenamed({src!r}, {dst!r})')
    return lines


def _parameter_dict_literal(parameters: list[dict[str, Any]] | None) -> str:
    pairs: list[str] = []
    for item in parameters or []:
        if not isinstance(item, dict):
            continue
        name = (item.get("variable") or item.get("name") or "").strip()
        if not name:
            continue
        value = item.get("input") or item.get("value") or item.get("field") or ""
        pairs.append(f"{name!r}: {value!r}")
    return "{" + ", ".join(pairs) + "}"


_SKIP_PRESERVE = frozenset({
    "step_type", "step_name", "attributes", "fields", "transformation_parameters",
    "_propagated_keys", "_propagation_trace", "extras",
})


def _preserve_all(meta: dict[str, Any], keys: tuple[str, ...] = ()) -> list[str]:
    """Emit # preserved.* for preferred keys first, then remaining metadata/extras."""
    lines: list[str] = []
    seen: set[str] = set()

    def _emit(key: str, val: object) -> None:
        lines.append(f"# preserved.{key}={val!r}")

    for key in keys:
        if key in seen:
            continue
        val = meta.get(key)
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)

    extras = meta.get("extras")
    if isinstance(extras, dict):
        for key, val in extras.items():
            tag = f"extras.{key}"
            if tag in seen or val in (None, "", [], {}):
                continue
            seen.add(tag)
            _emit(tag, val)

    for key, val in meta.items():
        if key in seen or key in _SKIP_PRESERVE:
            continue
        if val in (None, "", [], {}):
            continue
        seen.add(key)
        _emit(key, val)
    return lines


def _view_suffix(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "secondary")
    return cleaned or "secondary"


def convert_mapping_step(
    *,
    step_name: str,
    in_df: str | None,
    out_var: str,
    meta: dict[str, Any],
    label: str = "Mapping (Sub-transformation)",
    simple: bool = False,
    input_df_by_step: dict[str, str] | None = None,
) -> tuple[list[str], str, list[str]]:
    """Generate modular Mapping / Simple Mapping invocation code.

    Returns ``(code_lines, status, warnings)``.
    """
    warnings: list[str] = []
    lines: list[str] = [f"# {label}: {step_name}"]
    input_df_by_step = dict(input_df_by_step or {})

    child_key = child_transformation_key(meta)
    child_func = child_run_function_name(meta)
    input_mappings = list(meta.get("input_mappings") or [])
    output_mappings = list(meta.get("output_mappings") or [])
    if simple:
        if meta.get("input_mapping") and not input_mappings:
            input_mappings = [meta["input_mapping"]]
        if meta.get("output_mapping") and not output_mappings:
            output_mappings = [meta["output_mapping"]]

    input_connectors = collect_connectors(input_mappings)
    output_connectors = collect_connectors(output_mappings)
    main_in = _main_io_definition(input_mappings) or {}
    main_out = _main_io_definition(output_mappings) or {}
    inherit_all = bool(meta.get("inherit_all_variables", True))
    parameters = meta.get("parameters") or []

    preserve_keys = (
        "specification_method", "filename", "trans_name", "directory_path",
        "trans_object_id", "input_mappings", "output_mappings",
        "input_mapping", "output_mapping", "parameters", "inherit_all_variables",
        "allow_multiple_input", "allow_multiple_output",
    )
    lines.extend(_preserve_all(meta, preserve_keys))

    if not child_key:
        warnings.append(f"{label} '{step_name}' missing sub-transformation path")
        lines.append("# WARNING: missing sub-transformation filename/trans_name")

    empty_params = [
        p for p in parameters
        if isinstance(p, dict) and not (p.get("input") or p.get("value") or p.get("field"))
    ]
    if empty_params:
        names = [p.get("variable") or p.get("name") for p in empty_params]
        warnings.append(f"{label} '{step_name}' has parameters without values: {names}")
        lines.append(f"# WARNING: missing parameter values for {names!r}")

    multi_in = bool(meta.get("allow_multiple_input") and len(input_mappings) > 1)
    multi_out = bool(meta.get("allow_multiple_output") and len(output_mappings) > 1)
    if multi_in:
        lines.append(
            "# Multi-input Mapping: main path drives the DataFrame; secondary paths "
            "are published as named temp views for the child Mapping Input steps."
        )
    if multi_out:
        lines.append(
            "# LIMITATION: Multiple Mapping Output paths are preserved in metadata; "
            "generated code returns the main output path only."
        )
        warnings.append(f"{label} '{step_name}' multi-output paths not fully inlined")

    if main_in.get("input_step") or main_in.get("output_step"):
        lines.append(
            f"# execution_order.input: parent_step={main_in.get('input_step')!r} → "
            f"mapping_input={main_in.get('output_step')!r}"
        )
    if main_out.get("input_step") or main_out.get("output_step"):
        lines.append(
            f"# execution_order.output: mapping_output={main_out.get('input_step')!r} → "
            f"parent_step={main_out.get('output_step')!r}"
        )
    if main_in.get("rename_on_output"):
        lines.append(
            "# preserved.rename_on_output=True — reverse input renames after child returns"
        )

    helper = f"_invoke_mapping_{out_var}"
    mapped_in = f"_mapping_in_{out_var}"
    params_var = f"_mapping_params_{out_var}"

    # Resolve main input DataFrame (prefer explicit input_step when present)
    main_src_step = (main_in.get("input_step") or "").strip()
    main_df = input_df_by_step.get(main_src_step) if main_src_step else None
    if not main_df:
        main_df = in_df

    if main_df:
        lines.append(f"{mapped_in} = {main_df}")
    else:
        warnings.append(f"{label} '{step_name}' has no input DataFrame")
        lines.append(f"{mapped_in} = spark.createDataFrame([], '_mapping_empty STRING').limit(0)")
        lines.append("# WARNING: Mapping received empty/missing input stream")

    main_connectors = list(main_in.get("connectors") or []) if main_in else input_connectors
    if not main_connectors and input_connectors:
        main_connectors = input_connectors

    if main_connectors:
        lines.append("# Apply parent→child field mappings before sub-transformation")
        lines.extend(_rename_lines(mapped_in, main_connectors, direction="parent_to_child"))
        parent_fields = [c["parent"] for c in main_connectors if c.get("parent")]
        if parent_fields:
            lines.append(f"_missing_in_{out_var} = {parent_fields!r}")
            lines.append(
                f"_missing_in_{out_var} = "
                f"[c for c in _missing_in_{out_var} if c not in {mapped_in}.columns]"
            )
            lines.append(f"if _missing_in_{out_var}:")
            lines.append(
                f"    raise ValueError("
                f"'Mapping ' + {step_name!r} + ' missing input fields: ' + str(_missing_in_{out_var}))"
            )
    else:
        lines.append("# No explicit input field connectors — pass columns unchanged")

    # Secondary input mappings → named temp views (child Mapping Input can read them)
    for idx, mapping in enumerate(input_mappings):
        if mapping is main_in or mapping == main_in:
            continue
        if main_in and mapping.get("main_path"):
            continue
        src_step = (mapping.get("input_step") or "").strip()
        tgt_step = (mapping.get("output_step") or f"secondary_{idx}").strip()
        src_df = input_df_by_step.get(src_step) if src_step else None
        view = f"_pentaho_mapping_input_{_view_suffix(tgt_step)}"
        lines.append(
            f"# secondary_input[{idx}]: parent_step={src_step!r} → "
            f"mapping_input={tgt_step!r} view={view!r}"
        )
        if src_df:
            sec_var = f"_mapping_sec_{out_var}_{idx}"
            lines.append(f"{sec_var} = {src_df}")
            sec_connectors = list(mapping.get("connectors") or [])
            if sec_connectors:
                lines.extend(_rename_lines(sec_var, sec_connectors, direction="parent_to_child"))
            lines.append(f"{sec_var}.createOrReplaceTempView({view!r})")
        else:
            warnings.append(
                f"{label} '{step_name}' secondary input {src_step!r} not available in parent hops"
            )
            lines.append(
                f"# WARNING: secondary input DataFrame for {src_step!r} was not resolved"
            )

    lines.append(f"{params_var} = {_parameter_dict_literal(parameters)}")
    lines.append(f"_inherit_all_{out_var} = {inherit_all!r}")
    if inherit_all:
        lines.append(
            "# Variable inheritance: parent Spark conf / widgets cascade into child "
            "(mapped parameter keys still override on conflict)."
        )
    else:
        lines.append(
            "# Variable inheritance disabled — only mapped parameters are applied"
        )
        lines.append(
            "# LIMITATION: Spark cannot clear ambient parent conf isolation like PDI; "
            "treat unmapped parent variables as out-of-band."
        )

    # Reusable helper: invoke generated child run_* without flattening
    lines.append(f"def {helper}(spark, input_df, parameters=None, inherit_all=True):")
    lines.append(
        '    """Invoke child mapping transformation without flattening its steps."""'
    )
    lines.append("    parameters = dict(parameters or {})")
    lines.append("    stack = globals().setdefault('_pentaho_mapping_stack', [])")
    lines.append(f"    child_key = {child_key!r}")
    lines.append("    if not child_key:")
    lines.append(
        "        raise ValueError('Mapping child transformation path is missing')"
    )
    lines.append("    if child_key in stack:")
    lines.append(
        "        raise RuntimeError("
        "f'Circular mapping reference detected for {child_key!r}: {stack}')"
    )
    lines.append("    stack.append(child_key)")
    lines.append("    try:")
    lines.append("        if not inherit_all:")
    lines.append(
        "            _isolation = 'mapped-params-only'  # ambient parent vars may remain"
    )
    lines.append("        for _k, _v in parameters.items():")
    lines.append("            if _v in (None, ''):")
    lines.append(
                "                raise ValueError("
                "f'Mapping parameter {_k!r} has no value')"
    )
    lines.append("            spark.conf.set(f'pentaho.param.{_k}', str(_v))")
    lines.append("            try:")
    lines.append("                dbutils.widgets.text(_k, str(_v))  # type: ignore[name-defined]")
    lines.append("            except Exception:")
    lines.append("                _ = None  # widgets unavailable outside Databricks notebooks")
    lines.append(
        "        # Contract: Mapping Input Specification reads this temp view"
    )
    lines.append(
        "        input_df.createOrReplaceTempView('_pentaho_mapping_input')"
    )
    lines.append(f"        _child_fn = globals().get({child_func!r})")
    lines.append("        if callable(_child_fn):")
    lines.append("            result_df = _child_fn(spark)")
    lines.append("        else:")
    lines.append(
        "            # Child module not in this notebook — keep input with warning"
    )
    lines.append(
        "            # LIMITATION: Load/import generated child module, then call "
        f"{child_func}(spark)"
    )
    lines.append("            result_df = input_df")
    lines.append("        try:")
    lines.append(
        "            return spark.table('_pentaho_mapping_output')"
    )
    lines.append("        except Exception:")
    lines.append("            return result_df")
    lines.append("    finally:")
    lines.append("        if stack and stack[-1] == child_key:")
    lines.append("            stack.pop()")

    lines.append(
        f"{out_var} = {helper}(spark, {mapped_in}, {params_var}, _inherit_all_{out_var})"
    )

    # Output renames (child→parent). If rename_on_output, also reverse input renames.
    out_renames = list(output_connectors)
    if main_in.get("rename_on_output") and main_connectors:
        for conn in main_connectors:
            parent = (conn.get("parent") or "").strip()
            child = (conn.get("child") or parent).strip()
            if parent and child and parent != child:
                out_renames.append({"parent": parent, "child": child})

    if out_renames:
        lines.append("# Apply child→parent output field mappings")
        lines.extend(_rename_lines(out_var, out_renames, direction="child_to_parent"))
        child_fields = [c["child"] for c in output_connectors if c.get("child")]
        if child_fields:
            lines.append(
                f"_missing_out_{out_var} = "
                f"[c for c in {child_fields!r} if c not in {out_var}.columns]"
            )
            lines.append(f"if _missing_out_{out_var}:")
            lines.append(
                "    # WARNING: schema mismatch — output fields missing after mapping"
            )
            lines.append(
                f"    raise ValueError("
                f"'Mapping ' + {step_name!r} + ' missing output fields: ' + str(_missing_out_{out_var}))"
            )

    if multi_out:
        for idx, mapping in enumerate(output_mappings):
            if mapping is main_out or mapping == main_out:
                continue
            lines.append(
                f"# preserved.secondary_output[{idx}]="
                f"{{'mapping_output': {mapping.get('input_step')!r}, "
                f"'parent_target': {mapping.get('output_step')!r}, "
                f"'connectors': {mapping.get('connectors')!r}}}"
            )

    lines.append(
        f"# LIMITATION: Nested Mapping executes via reusable {child_func}; "
        "ensure the child transformation is generated as a separate function/module."
    )

    status = "converted"
    if warnings or not child_key or multi_out:
        status = "partial"
    return lines, status, warnings


def convert_mapping_input_step(
    *,
    step_name: str,
    in_df: str | None,
    out_var: str,
    meta: dict[str, Any],
) -> tuple[list[str], str, list[str]]:
    """Generate Mapping Input Specification schema validation + ordering."""
    from .schema_utils import fields_to_schema_ddl, spark_cast_type

    warnings: list[str] = []
    lines = [f"# Mapping Input Specification: {step_name}"]
    fields: list[dict[str, Any]] = list(meta.get("fields") or [])
    select_unspecified = bool(
        meta.get("select_unspecified") or meta.get("include_unspecified_fields")
    )

    lines.extend(_preserve_all(meta, (
        "fields", "field_names", "select_unspecified", "include_unspecified_fields",
    )))

    step_view = f"_pentaho_mapping_input_{_view_suffix(step_name)}"
    # Prefer rows injected by parent Mapping helper (primary, then step-named view)
    lines.append("try:")
    lines.append(f"    {out_var} = spark.table('_pentaho_mapping_input')")
    lines.append("except Exception:")
    lines.append("    try:")
    lines.append(f"        {out_var} = spark.table({step_view!r})")
    lines.append("    except Exception:")
    if in_df:
        lines.append(f"        {out_var} = {in_df}")
    else:
        ddl = fields_to_schema_ddl(fields)
        if ddl:
            lines.append(f"        {out_var} = spark.createDataFrame([], '{ddl}')")
        else:
            lines.append(
                f"        {out_var} = spark.createDataFrame([], '_mapping_input STRING').limit(0)"
            )
            warnings.append(
                f"Mapping Input '{step_name}' has no input stream and no field schema"
            )
            lines.append("        # WARNING: empty Mapping Input with no declared fields")

    required = [f["name"] for f in fields if f.get("required", True) and f.get("name")]
    optional = [f["name"] for f in fields if f.get("optional") and f.get("name")]
    ordered = [f["name"] for f in fields if f.get("name")]

    if required:
        lines.append(f"_required_{out_var} = {required!r}")
        lines.append(
            f"_missing_req_{out_var} = "
            f"[c for c in _required_{out_var} if c not in {out_var}.columns]"
        )
        lines.append(f"if _missing_req_{out_var}:")
        lines.append(
            f"    raise ValueError("
            f"'Mapping Input ' + {step_name!r} + ' missing required fields: ' "
            f"+ str(_missing_req_{out_var}))"
        )

    for field in fields:
        name = field.get("name")
        if not name:
            continue
        spark_type = spark_cast_type(field.get("type") or "String")
        lines.append(f"if {name!r} in {out_var}.columns:")
        lines.append(
            f"    {out_var} = {out_var}.withColumn("
            f"{name!r}, col({name!r}).cast({spark_type!r}))"
        )
        default = field.get("default_value")
        if default not in (None, ""):
            lines.append(
                f"    {out_var} = {out_var}.withColumn("
                f"{name!r}, when(col({name!r}).isNull(), lit({default!r}))"
                f".otherwise(col({name!r})))"
            )
        elif field.get("required", True):
            lines.append(
                f"    # Required field {name!r}: nulls allowed at row level; "
                "filter downstream if needed"
            )

    if ordered:
        if select_unspecified:
            lines.append(
                "# Include unspecified fields (ordered by name) after specified fields"
            )
            lines.append(f"_specified_{out_var} = {[c for c in ordered]!r}")
            lines.append(
                f"_extra_{out_var} = sorted("
                f"[c for c in {out_var}.columns if c not in _specified_{out_var}])"
            )
            lines.append(
                f"{out_var} = {out_var}.select("
                f"*[c for c in _specified_{out_var} if c in {out_var}.columns] "
                f"+ _extra_{out_var})"
            )
        else:
            lines.append("# Project to Mapping Input field order (drop unspecified)")
            lines.append(
                f"{out_var} = {out_var}.select("
                f"*[c for c in {ordered!r} if c in {out_var}.columns])"
            )
    elif select_unspecified:
        lines.append(
            f"{out_var} = {out_var}.select(*sorted({out_var}.columns))"
        )

    if optional:
        lines.append(f"# optional_fields={optional!r}")

    lines.append(
        "# Null/empty input streams: schema is still validated; zero rows are valid"
    )

    status = "converted" if fields or in_df else "partial"
    if warnings:
        status = "partial"
    return lines, status, warnings


def convert_mapping_output_step(
    *,
    step_name: str,
    in_df: str | None,
    out_var: str,
    meta: dict[str, Any],
) -> tuple[list[str], str, list[str]]:
    """Generate Mapping Output Specification projection / rename + publish view."""
    warnings: list[str] = []
    lines = [f"# Mapping Output Specification: {step_name}"]
    fields: list[dict[str, Any]] = list(meta.get("fields") or [])
    renames: list[dict[str, str]] = list(meta.get("renames") or [])
    field_names = list(meta.get("field_names") or meta.get("output_columns") or [])

    lines.extend(_preserve_all(meta, (
        "fields", "field_names", "output_columns", "renames",
    )))

    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        warnings.append(f"Mapping Output '{step_name}' has no input DataFrame")
        lines.append(
            f"{out_var} = spark.createDataFrame([], '_mapping_output STRING').limit(0)"
        )
        lines.append("# WARNING: Mapping Output received empty input")

    for field in fields:
        name = field.get("name")
        rename = field.get("rename")
        if name and rename and name != rename:
            lines.append(f"if {name!r} in {out_var}.columns:")
            lines.append(
                f"    {out_var} = {out_var}.withColumnRenamed({name!r}, {rename!r})"
            )
    if renames:
        lines.extend(_rename_lines(out_var, renames, direction="child_to_parent"))

    ordered = field_names or [f["name"] for f in fields if f.get("name")]
    if ordered:
        lines.append("# Project Mapping Output schema in declared field order")
        lines.append(
            f"_out_cols_{out_var} = [c for c in {ordered!r} if c in {out_var}.columns]"
        )
        lines.append(f"if _out_cols_{out_var}:")
        lines.append(
            f"    _extra_out_{out_var} = "
            f"[c for c in {out_var}.columns if c not in _out_cols_{out_var}]"
        )
        lines.append(
            f"    {out_var} = {out_var}.select(*_out_cols_{out_var} + _extra_out_{out_var})"
        )
        lines.append(
            f"_missing_schema_{out_var} = "
            f"[c for c in {ordered!r} if c not in {out_var}.columns]"
        )
        lines.append(f"if _missing_schema_{out_var}:")
        lines.append(
            f"    raise ValueError("
            f"'Mapping Output ' + {step_name!r} + ' schema mismatch, missing: ' "
            f"+ str(_missing_schema_{out_var}))"
        )
    else:
        lines.append(
            "# No static output field list — pass stream through "
            "(parent Mapping may apply renames)"
        )

    lines.append(
        f"{out_var}.createOrReplaceTempView('_pentaho_mapping_output')"
    )
    step_view = f"_pentaho_mapping_output_{_view_suffix(step_name)}"
    lines.append(f"{out_var}.createOrReplaceTempView({step_view!r})")
    lines.append(
        "# Parent Mapping helper prefers '_pentaho_mapping_output' when present"
    )

    status = "converted" if in_df else "partial"
    if warnings:
        status = "partial"
    return lines, status, warnings
