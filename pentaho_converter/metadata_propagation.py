"""Propagate parsed XML metadata to converters and validate column lineage."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .metadata_models import (
    ColumnLineage,
    ColumnSchema,
    LineageValidationResult,
    StepMetadataBundle,
)
from .models import PentahoField, PentahoStep, PentahoTransformation
from .step_context import StepContext

# Keys from ``parsed_config`` that must reach converter ``parsed`` dicts.
_STRUCTURED_PROPAGATION_KEYS = frozenset({
    "calculations",
    "join_type",
    "step1",
    "step2",
    "keys",
    "keys_1",
    "keys_2",
    "group_keys",
    "aggregates",
    "fields",
    "field_to_use",
    "target_field",
    "mappings",
    "non_match_default",
    "compare_value",
    "condition",
    "send_true_to",
    "send_false_to",
    "limit",
    "columns",
    "rows",
    "field_name",
    "start_at",
    "increment_by",
    "max_value",
    "filename",
    "extension",
    "separator",
    "header",
    "footer",
    "encoding",
    "compression",
    "enclosure",
    "append",
    "file",
    "output_fields",
    "connection",
    "sql",
    "schema",
    "table",
    "parameters",
    "variables_active",
    "execute_each_row",
    "limit",
    "keys",
    "return_fields",
    "cached",
    "cache_size",
    "orderby",
    "fail_on_multiple",
    "eat_row_on_failure",
})


def _normalize_step_type(step_type: str) -> str:
    return (step_type or "").strip().lower().replace(" ", "")


def _fields_to_dicts(fields: list[PentahoField]) -> list[dict[str, str]]:
    return [
        {
            "name": f.name,
            "type": f.type_name,
            "length": f.length,
            "precision": f.precision,
            "format": f.format,
            "rename": f.rename,
            "default": f.default,
        }
        for f in fields
        if f.name
    ]


def _schemas_from_names(
    names: set[str],
    type_map: dict[str, str] | None = None,
    *,
    source_step: str = "",
) -> dict[str, ColumnSchema]:
    type_map = type_map or {}
    return {
        name: ColumnSchema(
            name=name,
            type_name=type_map.get(name, "String"),
            source_step=source_step,
        )
        for name in names
        if name
    }


def _resolve_parsed_config(step: PentahoStep) -> dict[str, Any]:
    """Return structured parser output, falling back to on-demand step_xml parse."""
    if step.parsed_config:
        return deepcopy(step.parsed_config)
    if step.raw_element is not None:
        from .step_xml import parse_step_metadata

        parsed = parse_step_metadata(step.raw_element, step.step_type)
        if parsed:
            return deepcopy(parsed)
    return {}


def build_step_metadata_bundle(step: PentahoStep) -> StepMetadataBundle:
    """Capture parser output before registry propagation."""
    return StepMetadataBundle(
        step_name=step.name,
        step_type=step.step_type,
        attributes=dict(step.attributes),
        parsed_config=_resolve_parsed_config(step),
    )


def _enrich_converter_metadata(
    step: PentahoStep,
    transformation: PentahoTransformation,
    trace: list[str],
) -> dict[str, Any]:
    """Merge parsed_config into the dict converters receive via parse_xml."""
    meta: dict[str, Any] = {
        "step_type": step.step_type,
        "step_name": step.name,
        "attributes": dict(step.attributes),
        "fields": _fields_to_dicts(step.fields),
        "transformation_parameters": dict(transformation.parameters),
    }

    parsed_config = _resolve_parsed_config(step)

    if parsed_config:
        for key, value in parsed_config.items():
            meta[key] = deepcopy(value)
            trace.append(f"parsed_config.{key}")
    else:
        trace.append("parsed_config:empty")

    st = _normalize_step_type(step.step_type)

    # Aliases expected by existing converter/validator contracts.
    if st in ("mergejoin", "joinrows", "joiner", "streamlookup", "databaselookup", "mergerows"):
        keys = meta.get("keys") or []
        if keys and "join_keys" not in meta:
            normalized = []
            for pair in keys:
                if isinstance(pair, dict):
                    if pair.get("left") or pair.get("right"):
                        normalized.append({
                            "left": pair.get("left", ""),
                            "right": pair.get("right", pair.get("left", "")),
                        })
                    elif pair.get("stream_field") or pair.get("table_field"):
                        normalized.append({
                            "left": pair.get("stream_field", ""),
                            "right": pair.get("table_field", pair.get("stream_field", "")),
                        })
            meta["join_keys"] = normalized or keys
            trace.append("alias.join_keys")

    if st == "groupby":
        aggregates = meta.get("aggregates") or []
        if aggregates and "aggregate_fields" not in meta:
            meta["aggregate_fields"] = aggregates
            trace.append("alias.aggregate_fields")

    if st == "valuemapper":
        mappings = meta.get("mappings") or []
        if mappings and "value_mappings" not in meta:
            meta["value_mappings"] = mappings
            trace.append("alias.value_mappings")

    if st == "filterrows":
        if meta.get("condition") and "filter_condition" not in meta:
            meta["filter_condition"] = meta["condition"]
            trace.append("alias.filter_condition")

    if st == "selectvalues":
        field_dicts = meta.get("fields") or _fields_to_dicts(step.fields)
        select_cols = [f["name"] for f in field_dicts if f.get("name")]
        output_cols = [f.get("rename") or f["name"] for f in field_dicts if f.get("name")]
        if select_cols:
            meta["select_columns"] = select_cols
            trace.append("derived.select_columns")
        if output_cols:
            meta["output_columns"] = output_cols
            trace.append("derived.output_columns")

    if st in ("rowgenerator", "datagrid"):
        field_dicts = meta.get("fields") or []
        if field_dicts and "row_field_types" not in meta:
            meta["row_field_types"] = {
                f["name"]: f.get("type") or f.get("type_name", "String")
                for f in field_dicts
                if f.get("name")
            }
            trace.append("derived.row_field_types")

    if st == "textfileoutput":
        if meta.get("filename") and "file_path" not in meta:
            meta["file_path"] = meta["filename"]
            trace.append("alias.file_path")

    if st == "tableinput":
        if meta.get("sql") and transformation.parameters:
            from .lineage import substitute_pentaho_variables

            resolved = substitute_pentaho_variables(meta["sql"], transformation.parameters)
            if resolved != meta["sql"]:
                meta["sql_resolved"] = resolved
                trace.append("derived.sql_resolved")

    propagated = sorted(k for k in meta if k in _STRUCTURED_PROPAGATION_KEYS or k.startswith(("alias.", "derived.")))
    meta["_propagated_keys"] = sorted(
        k for k in meta if k in _STRUCTURED_PROPAGATION_KEYS or k in (
            "join_keys", "aggregate_fields", "value_mappings", "filter_condition",
            "select_columns", "output_columns", "row_field_types", "file_path", "sql_resolved",
        )
    )
    meta["_propagation_trace"] = trace
    return meta


def propagate_step_metadata(context: StepContext) -> StepMetadataBundle:
    """Populate context.extra with complete metadata for converters."""
    step = context.step
    trace: list[str] = ["parser.parsed_config", "parser.attributes", "parser.fields"]

    bundle = build_step_metadata_bundle(step)
    converter_metadata = _enrich_converter_metadata(step, context.transformation, trace)

    bundle.converter_metadata = converter_metadata
    bundle.propagation_trace = trace

    context.extra["metadata_bundle"] = bundle
    context.extra["converter_metadata"] = converter_metadata
    context.extra["parsed_config"] = _resolve_parsed_config(step)
    return bundle


def get_converter_metadata(context: StepContext) -> dict[str, Any]:
    """Return metadata dict for converter parse_xml (registry-injected)."""
    if "converter_metadata" in context.extra:
        return deepcopy(context.extra["converter_metadata"])

    bundle = propagate_step_metadata(context)
    return deepcopy(bundle.converter_metadata)


def _collect_referenced_columns(step_type: str, metadata: dict[str, Any]) -> set[str]:
    """Extract column names referenced by step metadata (pre-codegen)."""
    st = _normalize_step_type(step_type)
    refs: set[str] = set()

    if st == "calculator":
        for calc in metadata.get("calculations") or []:
            for key in ("field_a", "field_b", "field_c"):
                if calc.get(key):
                    refs.add(calc[key])

    elif st == "filterrows":
        def _walk_condition(node: dict | None) -> None:
            if not node:
                return
            if node.get("leftvalue"):
                refs.add(node["leftvalue"])
            if node.get("rightvalue"):
                refs.add(node["rightvalue"])
            for child in node.get("conditions") or []:
                _walk_condition(child)

        _walk_condition(metadata.get("condition"))
        _walk_condition(metadata.get("filter_condition"))

    elif st == "valuemapper":
        if metadata.get("field_to_use"):
            refs.add(metadata["field_to_use"])

    elif st == "groupby":
        for key in metadata.get("group_keys") or []:
            refs.add(key)
        for agg in metadata.get("aggregates") or []:
            subject = agg.get("subject") or agg.get("name")
            if subject:
                refs.add(subject)

    elif st in ("mergejoin", "joinrows", "streamlookup", "databaselookup"):
        for pair in metadata.get("keys") or metadata.get("join_keys") or []:
            left = pair.get("left") if isinstance(pair, dict) else None
            if left:
                refs.add(left)

    elif st == "selectvalues":
        for col in metadata.get("select_columns") or []:
            refs.add(col)

    elif st == "formula":
        if metadata.get("field_name"):
            refs.add(metadata["field_name"])

    elif st == "sortrows":
        for item in metadata.get("sort_fields") or []:
            if isinstance(item, (list, tuple)) and item:
                refs.add(item[0])
            elif isinstance(item, dict) and item.get("name"):
                refs.add(item["name"])

    return refs


def infer_lineage_from_metadata(
    context: StepContext,
    metadata: dict[str, Any],
    input_schemas: dict[str, ColumnSchema],
) -> ColumnLineage:
    """Infer output column lineage from propagated metadata (no generated code)."""
    step = context.step
    st = _normalize_step_type(step.step_type)
    input_names = set(input_schemas)
    output_names = set(input_names)
    type_map = {name: schema.type_name for name, schema in input_schemas.items()}
    added: set[str] = set()
    removed: set[str] = set()
    modified: set[str] = set()
    renamed: dict[str, str] = {}

    if st in ("rowgenerator", "datagrid"):
        output_names = {
            f.get("name") for f in (metadata.get("fields") or []) if f.get("name")
        }
        type_map.update({
            f["name"]: f.get("type", "String")
            for f in (metadata.get("fields") or [])
            if f.get("name")
        })
        added = output_names.copy()
        removed = input_names - output_names

    elif st == "constant":
        for const in metadata.get("constants") or []:
            name = const.get("name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = const.get("type", "String")

    elif st == "calculator":
        for calc in metadata.get("calculations") or []:
            fname = calc.get("field_name")
            if fname:
                output_names.add(fname)
                added.add(fname)
                type_map[fname] = calc.get("value_type") or "String"
            if calc.get("remove"):
                for key in ("field_a", "field_b", "field_c"):
                    col = calc.get(key)
                    if col and col in output_names:
                        output_names.discard(col)
                        removed.add(col)

    elif st == "selectvalues":
        field_dicts = metadata.get("fields") or []
        selected = metadata.get("select_columns") or [f.get("name") for f in field_dicts]
        output_names = set()
        for f in field_dicts:
            src = f.get("name")
            if not src or (selected and src not in selected):
                continue
            dst = f.get("rename") or src
            output_names.add(dst)
            type_map[dst] = f.get("type", type_map.get(src, "String"))
            if f.get("rename") and f["rename"] != src:
                renamed[src] = f["rename"]
        removed = input_names - set(selected or input_names)

    elif st == "groupby":
        keys = set(metadata.get("group_keys") or [])
        agg_names = {a.get("name") for a in (metadata.get("aggregates") or []) if a.get("name")}
        output_names = keys | agg_names
        added = agg_names
        removed = input_names - output_names
        for agg in metadata.get("aggregates") or []:
            subject = agg.get("subject")
            if subject:
                modified.add(subject)

    elif st == "valuemapper":
        target = metadata.get("target_field") or metadata.get("field_to_use")
        if target:
            output_names.add(target)
            added.add(target)
            type_map[target] = "String"

    elif st == "sequence":
        field_name = metadata.get("field_name", "seq")
        output_names.add(field_name)
        added.add(field_name)
        type_map[field_name] = "Integer"

    elif st in ("mergejoin", "joinrows", "streamlookup", "databaselookup"):
        for ret in metadata.get("return_fields") or []:
            name = ret.get("rename") or ret.get("name")
            if name:
                output_names.add(name)
                added.add(name)
        for pair in metadata.get("keys") or metadata.get("join_keys") or []:
            right = pair.get("right") if isinstance(pair, dict) else None
            if right:
                output_names.add(right)
                added.add(right)

    elif st == "filterrows":
        # Schema preserved; routing hops are metadata-only.
        pass

    elif st == "textfileoutput":
        # Output step — schema unchanged on stream.
        pass

    elif st == "tableinput":
        # Unknown until SQL executed; keep empty or infer from SQL tokens lightly.
        output_names = set(input_names)

    output_schemas = _schemas_from_names(output_names, type_map, source_step=step.name)
    return ColumnLineage(
        step_name=step.name,
        step_type=step.step_type,
        input_df=context.input_df_name(),
        output_df=context.output_df_name(),
        input_columns=input_schemas,
        output_columns=output_schemas,
        added_columns=added,
        removed_columns=removed,
        modified_columns=modified,
        renamed_columns=renamed,
    )


def merge_input_lineage(context: StepContext) -> dict[str, ColumnSchema]:
    """Merge upstream column schemas from lineage_map in context.extra."""
    lineage_map: dict[str, dict[str, ColumnSchema]] = context.extra.get("lineage_map", {})
    schemas: dict[str, ColumnSchema] = {}

    for pred in context.dag.predecessors(context.step.name):
        for name, schema in lineage_map.get(pred, {}).items():
            schemas[name] = schema

    # Fallback: plain column name set from code_generator compatibility.
    for col in context.extra.get("input_columns") or []:
        if col not in schemas:
            schemas[col] = ColumnSchema(name=col, type_name="String", source_step="upstream")

    return schemas


def validate_lineage_before_convert(
    context: StepContext,
    metadata: dict[str, Any],
    input_schemas: dict[str, ColumnSchema],
) -> LineageValidationResult:
    """Validate column references and output schema before converter runs."""
    result = LineageValidationResult()
    st = _normalize_step_type(context.step.step_type)
    input_names = set(input_schemas)

    if st in (
        "tableinput", "csvinput", "rowgenerator", "datagrid",
        "jsoninput", "textfileinput", "excelinput", "xmlinput",
    ):
        return result

    refs = _collect_referenced_columns(st, metadata)
    missing = sorted(refs - input_names)
    if missing and input_names:
        if st == "selectvalues":
            result.errors.append(
                f"SelectValues references missing columns: {', '.join(missing)}"
            )
        else:
            result.warnings.append(
                f"Metadata references columns not in upstream lineage: {', '.join(missing)}"
            )

    predicted = infer_lineage_from_metadata(context, metadata, input_schemas)
    output_names = list(predicted.output_column_names)
    seen: set[str] = set()
    duplicates: list[str] = []
    for name in output_names:
        if name in seen:
            duplicates.append(name)
        seen.add(name)
    if duplicates:
        result.errors.append(f"Duplicate output column names: {', '.join(sorted(set(duplicates)))}")

    rename_values = set(predicted.renamed_columns.values())
    collisions = sorted(rename_values & (input_names - set(predicted.renamed_columns)))
    if collisions:
        result.errors.append(
            f"Rename target collides with existing column: {', '.join(collisions)}"
        )

    for old, new in predicted.renamed_columns.items():
        if old not in input_names:
            result.errors.append(f"Rename source column '{old}' not in upstream lineage")

    context.extra["predicted_lineage"] = predicted
    context.extra["lineage_validation"] = result
    return result


def update_lineage_map(context: StepContext, lineage: ColumnLineage) -> None:
    """Store output schemas for downstream steps."""
    lineage_map: dict[str, dict[str, ColumnSchema]] = context.extra.setdefault("lineage_map", {})
    lineage_map[context.step.name] = dict(lineage.output_columns)
    context.extra["output_columns"] = sorted(lineage.output_column_names)
    context.extra["column_lineage"] = lineage
