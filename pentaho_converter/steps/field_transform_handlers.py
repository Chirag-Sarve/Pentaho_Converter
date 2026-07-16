"""Handlers for field-value Transform steps (Set Value, Concat, Add XML)."""

from __future__ import annotations

import logging
import re

from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_add_xml_config,
    parse_concat_fields_config,
    parse_set_value_constant_config,
    parse_set_value_field_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _lit_for_value(value: str, *, empty: bool = False, mask: str = "") -> str:
    if empty:
        return 'lit("")'
    if value is None or value == "":
        return "lit(None)"
    if re.fullmatch(r"-?\d+", value or ""):
        return f"lit({int(value)})"
    if re.fullmatch(r"-?\d+\.\d+", value or ""):
        return f"lit({float(value)})"
    if (value or "").upper() in ("TRUE", "FALSE"):
        return f"lit({value.upper() == 'TRUE'})"
    if mask and ("yy" in mask.lower() or "dd" in mask.lower() or "MM" in mask):
        return f'to_timestamp(lit({value!r}), {mask!r})'
    return f"lit({value!r})"


def _xml_escape_expr(col_ref: str) -> str:
    """Best-effort XML entity escaping for element/attribute text."""
    return (
        f"regexp_replace(regexp_replace(regexp_replace("
        f"coalesce({col_ref}.cast('string'), lit('')), "
        f"'&', '&amp;'), '<', '&lt;'), '>', '&gt;')"
    )


class SetValueConstantHandler(BaseStepHandler):
    """Set Field Value to a Constant → overwrite columns with literal values."""

    _TYPES = {
        "setvalueconstant",
        "setfieldvaluetoaconstant",
        "setfieldvalueconstant",
    }

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        fields = metadata.get("set_fields") or metadata.get("fields") or []
        usevar = bool(metadata.get("usevar"))

        lines = [f"# Set Field Value to a Constant: {step.name}"]
        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        if not fields:
            step_el = get_step_element(step)
            if step_el is not None:
                fields = parse_set_value_constant_config(step_el).get("fields") or []

        if not fields:
            lines.append(f"# WARNING: SetValueConstant '{step.name}': no fields configured")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        if usevar:
            from ..lineage import substitute_pentaho_variables

            params = getattr(context.transformation, "parameters", None) or {}
            lines.append(
                "# preserved.usevar=True — substituting from transformation parameters when available"
            )
            for item in fields:
                raw = item.get("value", "")
                if raw and "${" in raw:
                    item["value"] = substitute_pentaho_variables(raw, params)

        lines.append(f"{out_var} = {in_df}")
        for item in fields:
            name = item.get("name", "")
            if not name:
                continue
            empty = bool(item.get("set_empty_string"))
            value = item.get("value", "")
            mask = item.get("mask", "")
            expr = _lit_for_value(value, empty=empty, mask=mask)
            lines.append(f'{out_var} = {out_var}.withColumn("{name}", {expr})')
            if mask:
                lines.append(f"# preserved.mask={mask!r} for {name}")
            logger.debug("SetValueConstant %s -> %s", name, expr)

        return lines, "converted"


class SetValueFieldHandler(BaseStepHandler):
    """Set Field Value → copy value from replaceby column into target column."""

    _TYPES = {"setvaluefield", "setfieldvalue"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)
        fields = metadata.get("set_fields") or metadata.get("fields") or []

        lines = [f"# Set Field Value: {step.name}"]
        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        if not fields:
            step_el = get_step_element(step)
            if step_el is not None:
                fields = parse_set_value_field_config(step_el).get("fields") or []

        if not fields:
            lines.append(f"# WARNING: SetValueField '{step.name}': no fields configured")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        lines.append(f"{out_var} = {in_df}")
        status = "converted"
        for item in fields:
            name = item.get("name", "")
            replace_by = item.get("replace_by", "")
            if not name:
                continue
            if not replace_by:
                lines.append(
                    f"# WARNING: SetValueField '{name}' has empty replaceby — column left unchanged"
                )
                status = "partial"
                continue
            lines.append(
                f'{out_var} = {out_var}.withColumn("{name}", col("{replace_by}"))'
            )

        return lines, status


class ConcatFieldsHandler(BaseStepHandler):
    """Concat Fields → concat_ws / nested concat into a target string column."""

    _TYPES = {"concatfields"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)

        if not metadata.get("target_field_name") and get_step_element(step) is not None:
            metadata = parse_concat_fields_config(get_step_element(step))

        target = metadata.get("target_field_name") or "concat_result"
        separator = metadata.get("separator") or ""
        enclosure = metadata.get("enclosure") or ""
        enclosure_forced = bool(metadata.get("enclosure_forced"))
        remove_selected = bool(metadata.get("remove_selected_fields"))
        field_items = metadata.get("fields") or []
        field_names = [f.get("name") for f in field_items if f.get("name")]

        lines = [f"# Concat Fields: {step.name}"]
        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        if not field_names:
            # Concat all input columns when no field list is given
            field_names = list(context.input_column_names)

        if not field_names:
            lines.append(
                f"# WARNING: ConcatFields '{step.name}': no fields available to concatenate"
            )
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        parts: list[str] = []
        for item, name in zip(
            field_items or [{"name": n} for n in field_names],
            field_names,
        ):
            nullif = (item.get("nullif") if isinstance(item, dict) else "") or ""
            if nullif:
                casted = (
                    f'coalesce(col("{name}").cast("string"), lit({nullif!r}))'
                )
            else:
                casted = f'coalesce(col("{name}").cast("string"), lit(""))'
            if enclosure:
                parts.append(f'concat(lit({enclosure!r}), {casted}, lit({enclosure!r}))')
            else:
                parts.append(casted)

        if not enclosure:
            # Prefer concat_ws; fold nullif via coalesce when configured
            ws_args: list[str] = []
            for item, name in zip(
                field_items or [{"name": n} for n in field_names],
                field_names,
            ):
                nullif = (item.get("nullif") if isinstance(item, dict) else "") or ""
                if nullif:
                    ws_args.append(
                        f'coalesce(col("{name}").cast("string"), lit({nullif!r}))'
                    )
                else:
                    ws_args.append(f'col("{name}").cast("string")')
            expr = f"concat_ws({separator!r}, {', '.join(ws_args)})"
        elif separator:
            concat_args: list[str] = []
            for i, p in enumerate(parts):
                if i > 0:
                    concat_args.append(f"lit({separator!r})")
                concat_args.append(p)
            expr = f"concat({', '.join(concat_args)})"
        elif len(parts) == 1:
            expr = parts[0]
        else:
            expr = f"concat({', '.join(parts)})"

        if enclosure_forced:
            lines.append("# preserved.enclosure_forced=Y")

        lines.append(f"{out_var} = {in_df}")
        lines.append(f'{out_var} = {out_var}.withColumn("{target}", {expr})')

        target_len = int(metadata.get("target_field_length") or 0)
        if target_len > 0:
            lines.append(
                f'{out_var} = {out_var}.withColumn("{target}", '
                f'substring(col("{target}"), 1, {target_len}))'
            )
            lines.append(f"# preserved.targetFieldLength={target_len}")

        if remove_selected and field_names:
            drop_list = ", ".join(f'"{n}"' for n in field_names)
            lines.append(f"{out_var} = {out_var}.drop({drop_list})")

        if metadata.get("encoding"):
            lines.append(f"# preserved.encoding={metadata.get('encoding')!r}")

        return lines, "converted"


class AddXmlHandler(BaseStepHandler):
    """Add XML → build an XML fragment string column from selected fields."""

    _TYPES = {"addxml"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        step = context.step
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        metadata = get_converter_metadata(context)

        if not metadata.get("value_name") and get_step_element(step) is not None:
            metadata = parse_add_xml_config(get_step_element(step))

        value_name = metadata.get("value_name") or "xmlvaluename"
        root = metadata.get("root_node") or "Row"
        omit_null = bool(metadata.get("omit_null_values"))
        omit_header = bool(metadata.get("omit_xml_header", True))
        fields = metadata.get("fields") or []

        lines = [f"# Add XML: {step.name}"]
        if not in_df:
            lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
            return lines, "converted"

        if not fields:
            lines.append(f"# WARNING: AddXML '{step.name}': no output fields configured")
            lines.append(f"{out_var} = {in_df}")
            return lines, "partial"

        # Partition attributes vs elements
        attrs = [f for f in fields if f.get("attribute")]
        elements = [f for f in fields if not f.get("attribute")]

        # Root open tag with attributes (attributes without parent)
        attr_pieces: list[str] = []
        for attr in attrs:
            parent = (attr.get("attribute_parent_name") or "").strip()
            if parent:
                continue  # attached under a specific element below
            name = attr.get("name", "")
            el_name = attr.get("element") or name
            nullif = attr.get("nullif") or ""
            raw = f'coalesce(col("{name}").cast("string"), lit({nullif!r}))'
            escaped = _xml_escape_expr(f'col("{name}")') if not nullif else (
                f"regexp_replace(regexp_replace(regexp_replace("
                f"{raw}, '&', '&amp;'), '<', '&lt;'), '>', '&gt;')"
            )
            if omit_null:
                attr_pieces.append(
                    f"when(col({name!r}).isNull(), lit('')).otherwise("
                    f"concat(lit(' {el_name}=\"'), {escaped}, lit('\"')))"
                )
            else:
                attr_pieces.append(
                    f"concat(lit(' {el_name}=\"'), {escaped}, lit('\"'))"
                )

        if attr_pieces:
            open_tag = (
                f"concat(lit('<{root}'), "
                + ", ".join(attr_pieces)
                + f", lit('>'))"
            )
        else:
            open_tag = f"lit('<{root}>')"

        body_parts: list[str] = []
        for field in elements:
            name = field.get("name", "")
            el_name = field.get("element") or name
            nullif = field.get("nullif") or ""
            # Nested attributes that belong to this element
            nested_attrs = [
                a for a in attrs
                if (a.get("attribute_parent_name") or "").strip() == el_name
            ]
            nested_attr_exprs: list[str] = []
            for a in nested_attrs:
                a_name = a.get("name", "")
                a_el = a.get("element") or a_name
                a_esc = _xml_escape_expr(f'col("{a_name}")')
                nested_attr_exprs.append(
                    f"concat(lit(' {a_el}=\"'), {a_esc}, lit('\"'))"
                )

            content = _xml_escape_expr(f'col("{name}")')
            if nullif:
                content = (
                    f"when(col({name!r}).isNull(), lit({nullif!r})).otherwise({content})"
                )

            if nested_attr_exprs:
                open_el = (
                    f"concat(lit('<{el_name}'), "
                    + ", ".join(nested_attr_exprs)
                    + ", lit('>'))"
                )
                fragment = (
                    f"concat({open_el}, {content}, lit('</{el_name}>'))"
                )
            else:
                fragment = (
                    f"concat(lit('<{el_name}>'), {content}, lit('</{el_name}>'))"
                )

            if omit_null:
                fragment = (
                    f"when(col({name!r}).isNull(), lit('')).otherwise({fragment})"
                )
            body_parts.append(fragment)

        xml_parts = [open_tag, *body_parts, f"lit('</{root}>')"]
        xml_expr = f"concat({', '.join(xml_parts)})"

        if not omit_header:
            encoding = metadata.get("encoding") or "UTF-8"
            header = f"lit('<?xml version=\"1.0\" encoding=\"{encoding}\"?>')"
            xml_expr = f"concat({header}, {xml_expr})"

        lines.append(f"{out_var} = {in_df}")
        lines.append(f'{out_var} = {out_var}.withColumn("{value_name}", {xml_expr})')
        lines.append(f"# preserved.encoding={metadata.get('encoding')!r}")
        lines.append(f"# preserved.omitXMLheader={omit_header}")
        lines.append(f"# preserved.omitNullValues={omit_null}")

        return lines, "converted"


FIELD_TRANSFORM_HANDLERS = [
    SetValueConstantHandler(),
    SetValueFieldHandler(),
    ConcatFieldsHandler(),
    AddXmlHandler(),
]
