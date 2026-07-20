"""Convert Pentaho Filter Rows condition metadata to PySpark filter expressions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from xml.etree import ElementTree as ET

from .step_context import StepContext
from .step_xml import parse_filter_condition_tree


@dataclass
class FilterExpressionResult:
    """Result of converting one filter condition tree to a Spark expression."""

    expr: str
    warnings: list[str] = field(default_factory=list)
    ok: bool = True


def _col_ref(field_name: str) -> str:
    if not field_name:
        return "lit(None)"
    return f'col("{field_name}")'


def _is_null_value(value: dict[str, Any] | None) -> bool:
    if not value:
        return False
    if (value.get("isnull") or "").upper() == "Y":
        return True
    text = value.get("text")
    return text in ("", None) and "type" in value


def _literal_expr(type_name: str, value: str) -> str:
    if value == "" or value is None:
        return "lit(None)"
    t = (type_name or "String").lower()
    if "bool" in t:
        return "lit(True)" if value.upper() in ("Y", "TRUE", "1", "T") else "lit(False)"
    if t in ("integer", "int"):
        try:
            return f"lit({int(value)})"
        except ValueError:
            return f"lit({value!r})"
    if t in ("long",):
        try:
            return f"lit({int(value)})"
        except ValueError:
            return f"lit({value!r})"
    if t in ("number", "bignumber", "float", "double"):
        try:
            return f"lit({float(value)})"
        except ValueError:
            return f"lit({value!r})"
    if t in ("decimal", "bigdecimal"):
        return f'lit("{value}").cast("decimal")'
    if t == "date":
        return f"to_date(lit({value!r}))"
    if t in ("timestamp", "datetime"):
        return f"to_timestamp(lit({value!r}))"
    return f"lit({value!r})"


def _value_dict_literal(value: dict[str, Any] | None) -> str:
    if not value:
        return "lit(None)"
    return _literal_expr(value.get("type", "String"), value.get("text", ""))


def _looks_like_literal(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    if text.replace(".", "", 1).isdigit():
        return True
    return text.startswith("-") and text[1:].replace(".", "", 1).isdigit()


def _right_operand(node: dict[str, Any]) -> str:
    value = node.get("value")
    if isinstance(value, dict) and (
        value.get("text") not in (None, "")
        or (value.get("isnull") or "").upper() == "Y"
    ):
        return _value_dict_literal(value)

    right_field = (node.get("rightvalue") or "").strip()
    if right_field:
        if _looks_like_literal(right_field):
            type_name = (value or {}).get("type", "Number") if isinstance(value, dict) else "Number"
            return _literal_expr(type_name, right_field)
        return _col_ref(right_field)
    if isinstance(value, dict):
        return _value_dict_literal(value)
    return "lit(None)"


def _left_operand(node: dict[str, Any]) -> str:
    return _col_ref((node.get("leftvalue") or "").strip())


def _parse_in_list_values(node: dict[str, Any]) -> tuple[list[str], str]:
    value = node.get("value") or {}
    text_val = (value.get("text") or "").strip()
    type_name = value.get("type", "String")
    if not text_val:
        return [], type_name
    return [v.strip() for v in text_val.split(";") if v.strip()], type_name


def _between_expr(left: str, value: dict[str, Any] | None) -> FilterExpressionResult:
    if not value:
        return FilterExpressionResult(
            expr="lit(False)",
            warnings=["BETWEEN comparison is missing low/high values in metadata"],
            ok=False,
        )
    type_name = value.get("type", "Number")
    text_val = (value.get("text") or "").strip()
    parts = [p.strip() for p in text_val.split(";")]
    if len(parts) < 2:
        return FilterExpressionResult(
            expr="lit(False)",
            warnings=[f"BETWEEN comparison requires two values, found: {text_val!r}"],
            ok=False,
        )
    low = _literal_expr(type_name, parts[0])
    high = _literal_expr(type_name, parts[1])
    return FilterExpressionResult(expr=f"(({left} >= {low}) & ({left} <= {high}))")


def _equality_expr(left: str, right: str, value: dict[str, Any] | None, negate: bool = False) -> str:
    if _is_null_value(value) or right == "lit(None)":
        return f"{left}.isNotNull()" if negate else f"{left}.isNull()"
    return f"({left} != {right})" if negate else f"({left} == {right})"


def _parse_leaf_condition(node: dict[str, Any]) -> FilterExpressionResult:
    """Convert a single leaf Pentaho condition metadata node to a Spark boolean expression."""
    func = (node.get("function") or "").strip().upper()
    left = _left_operand(node)
    right = _right_operand(node)
    value = node.get("value")

    if func in ("=", "==", "EQ"):
        return FilterExpressionResult(_equality_expr(left, right, value))
    if func in ("<>", "!=", "NE"):
        return FilterExpressionResult(_equality_expr(left, right, value, negate=True))
    if func == "<":
        return FilterExpressionResult(f"({left} < {right})")
    if func == ">":
        return FilterExpressionResult(f"({left} > {right})")
    if func == "<=":
        return FilterExpressionResult(f"({left} <= {right})")
    if func == ">=":
        return FilterExpressionResult(f"({left} >= {right})")
    if func == "IS NULL":
        return FilterExpressionResult(f"{left}.isNull()")
    if func == "IS NOT NULL":
        return FilterExpressionResult(f"{left}.isNotNull()")
    if func == "TRUE":
        if not (node.get("leftvalue") or "").strip():
            return FilterExpressionResult("lit(True)")
        return FilterExpressionResult(left)
    if func == "FALSE":
        if not (node.get("leftvalue") or "").strip():
            return FilterExpressionResult("lit(False)")
        return FilterExpressionResult(f"(~({left}))")
    if func == "LIKE":
        pattern = (value or {}).get("text", "")
        return FilterExpressionResult(f'{left}.like({pattern!r})')
    if func == "NOT LIKE":
        pattern = (value or {}).get("text", "")
        return FilterExpressionResult(f'~({left}.like({pattern!r}))')
    if func == "CONTAINS":
        return FilterExpressionResult(f"{left}.contains({right})")
    if func == "NOT CONTAINS":
        return FilterExpressionResult(f"~({left}.contains({right}))")
    if func == "STARTS WITH":
        return FilterExpressionResult(f"{left}.startswith({right})")
    if func == "NOT STARTS WITH":
        return FilterExpressionResult(f"~({left}.startswith({right}))")
    if func == "ENDS WITH":
        return FilterExpressionResult(f"{left}.endswith({right})")
    if func == "NOT ENDS WITH":
        return FilterExpressionResult(f"~({left}.endswith({right}))")
    if func in ("REGEXP", "REGEX", "REGEXP MATCHES"):
        return FilterExpressionResult(f"{left}.rlike({right})")
    if func in ("NOT REGEXP", "NOT REGEX", "REGEXP NOT MATCHES"):
        return FilterExpressionResult(f"~({left}.rlike({right}))")
    if func in ("IN LIST", "IN"):
        values, type_name = _parse_in_list_values(node)
        if not values:
            return FilterExpressionResult(
                "lit(False)",
                warnings=["IN comparison has an empty value list in metadata"],
            )
        items = ", ".join(_literal_expr(type_name, v) for v in values)
        return FilterExpressionResult(f"{left}.isin([{items}])")
    if func in ("NOT IN LIST", "NOT IN"):
        values, type_name = _parse_in_list_values(node)
        if not values:
            return FilterExpressionResult(
                "lit(True)",
                warnings=["NOT IN comparison has an empty value list in metadata"],
            )
        items = ", ".join(_literal_expr(type_name, v) for v in values)
        return FilterExpressionResult(f"~({left}.isin([{items}]))")
    if func == "BETWEEN":
        return _between_expr(left, value)
    if func == "NOT BETWEEN":
        between = _between_expr(left, value)
        if not between.ok:
            return between
        return FilterExpressionResult(f"(~({between.expr}))", warnings=between.warnings)

    if "=" in func:
        return FilterExpressionResult(_equality_expr(left, right, value))

    return FilterExpressionResult(
        expr="lit(False)",
        warnings=[f"Unsupported FilterRows comparison operator: {func!r}"],
        ok=False,
    )


def _parse_condition_node(node: dict[str, Any] | None) -> FilterExpressionResult:
    """Recursively parse a Pentaho condition metadata tree."""
    if not node:
        return FilterExpressionResult(
            expr="lit(False)",
            warnings=["Filter condition metadata is empty"],
            ok=False,
        )

    warnings: list[str] = []
    negated = (node.get("negated") or "N").upper() == "Y"
    children = list(node.get("conditions") or [])

    if children:
        if not children:
            return FilterExpressionResult(
                expr="lit(False)",
                warnings=["Filter condition group has no child conditions"],
                ok=False,
            )
        parts: list[tuple[str, str]] = []
        for child in children:
            child_result = (
                _parse_condition_node(child)
                if child.get("conditions") or (child.get("leftvalue") and child.get("function"))
                else _parse_leaf_condition(child)
            )
            warnings.extend(child_result.warnings)
            if not child_result.ok:
                return FilterExpressionResult(
                    expr=child_result.expr,
                    warnings=warnings,
                    ok=False,
                )
            op = (child.get("operator") or "AND").upper()
            if not parts:
                parts.append(("AND", child_result.expr))
            else:
                parts.append((op if op in ("AND", "OR") else "AND", child_result.expr))

        combined = parts[0][1]
        for op, expr in parts[1:]:
            joiner = " & " if op == "AND" else " | "
            combined = f"({combined}{joiner}{expr})"
    elif node.get("leftvalue") and node.get("function"):
        leaf = _parse_leaf_condition(node)
        warnings.extend(leaf.warnings)
        if not leaf.ok:
            return leaf
        combined = leaf.expr
    else:
        return FilterExpressionResult(
            expr="lit(False)",
            warnings=["Filter condition metadata is missing leftvalue/function or child conditions"],
            ok=False,
        )

    if negated:
        return FilterExpressionResult(f"(~({combined}))", warnings=warnings)
    return FilterExpressionResult(combined, warnings=warnings)


def convert_filter_condition_from_metadata(
    condition: dict[str, Any] | None,
) -> FilterExpressionResult:
    """Convert propagated Filter Rows condition metadata to a PySpark filter expression."""
    return _parse_condition_node(condition)


def convert_filter_condition(root: Any | None) -> str:
    """Backward-compatible adapter for legacy XML condition roots."""
    if root is None:
        return "lit(False)"
    if isinstance(root, dict):
        return convert_filter_condition_from_metadata(root).expr
    if isinstance(root, ET.Element):
        tree = parse_filter_condition_tree(root)
        return convert_filter_condition_from_metadata(tree).expr
    return "lit(False)"


def _parenthesize_bool_clauses(expr: str) -> str:
    """Wrap comparison clauses so & / | bind correctly in generated Python."""
    import re

    if not re.search(r"[&|]", expr):
        return expr
    parts = re.split(r"(\s[&|]\s)", expr.strip())
    rebuilt: list[str] = []
    for part in parts:
        stripped = part.strip()
        if stripped in ("&", "|"):
            rebuilt.append(f" {stripped} ")
        elif stripped:
            rebuilt.append(f"({stripped})")
    return "".join(rebuilt)


def _wrap_compare_literals(expr: str) -> str:
    """Wrap raw literal operands in lit() for valid Spark Column expressions."""
    import re

    expr = re.sub(
        r"(==|!=|<=|>=|<|>)\s*('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")",
        r"\1 lit(\2)",
        expr,
    )
    expr = re.sub(
        r"(==|!=|<=|>=|<|>)\s*((?:\d+\.\d+|\d+)(?:[eE][+-]?\d+)?)\b",
        r"\1 lit(\2)",
        expr,
    )
    expr = re.sub(
        r"(==|!=)\s*\b(true|false)\b",
        lambda m: f"{m.group(1)} lit({m.group(2).capitalize()})",
        expr,
        flags=re.IGNORECASE,
    )
    expr = re.sub(
        r"(==|!=)\s*\bnull\b",
        r"\1 lit(None)",
        expr,
        flags=re.IGNORECASE,
    )
    return expr


def convert_simple_condition(condition: str) -> str:
    """Best-effort conversion of a simple text condition (legacy/fallback)."""
    if not condition or not condition.strip():
        return "lit(False)"

    import re

    expr = condition.strip()

    null_match = re.match(
        r"^([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+NOT\s+NULL$",
        expr,
        flags=re.IGNORECASE,
    )
    if null_match:
        return f'col("{null_match.group(1)}").isNotNull()'

    null_match = re.match(
        r"^([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+NULL$",
        expr,
        flags=re.IGNORECASE,
    )
    if null_match:
        return f'col("{null_match.group(1)}").isNull()'

    replacements = [
        ("&&", "&"),
        ("||", "|"),
        (" and ", " & "),
        (" AND ", " & "),
        (" or ", " | "),
        (" OR ", " | "),
        (" not ", " ~"),
        (" NOT ", " ~"),
    ]
    for old, new in replacements:
        expr = expr.replace(old, new)

    expr = re.sub(r"(?<![!<>=])=(?!=)", "==", expr)

    def _field_to_col(match: re.Match) -> str:
        name = match.group(1)
        if name.lower() in ("true", "false", "null", "none", "lit", "col"):
            return name
        return f'col("{name}")'

    segments = re.split(r"('(?:[^'\\]|\\.)*')", expr)
    for i in range(0, len(segments), 2):
        segments[i] = re.sub(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", _field_to_col, segments[i])
    expr = _wrap_compare_literals("".join(segments))
    return _parenthesize_bool_clauses(expr)


def _df_name_for_step(context: StepContext, step_name: str) -> str:
    safe = step_name.replace(" ", "_").replace("-", "_")
    return context.df_variable_map.get(step_name, f"df_{safe}")


def _branch_stream_name(step_name: str) -> str:
    """Branch stream DF name from the target step name (not Dummy-prefixed map names)."""
    safe = step_name.replace(" ", "_").replace("-", "_")
    return f"df_{safe}"


def resolve_incoming_branch_df(context: StepContext, step_name: str | None = None) -> str | None:
    """If ``step_name`` is a Filter/JavaFilter/SwitchCase branch target, return that stream DF.

    Filter/JavaFilter write ``df_<Target> = …filter…`` before the target step runs. Callers
    must use this stream (not the Filter primary output) so success rows never overwrite
    the failure/true branch DataFrame.
    """
    target = step_name or context.step.name
    preds = context.dag.predecessors(target)
    if not preds:
        return None
    pred_name = preds[0]
    pred = context.dag.steps.get(pred_name)
    if pred is None:
        return None
    st = (pred.step_type or "").strip().lower().replace(" ", "")

    if st in {"filterrows", "javafilter"}:
        meta: dict[str, Any] = {}
        if pred.parsed_config:
            meta = dict(pred.parsed_config)
        else:
            step_el = None
            try:
                from .step_xml import get_step_element, parse_filter_rows_config, parse_java_filter_config

                step_el = get_step_element(pred)
                if step_el is not None:
                    meta = (
                        parse_filter_rows_config(step_el)
                        if st == "filterrows"
                        else parse_java_filter_config(step_el)
                    )
            except Exception:
                meta = {}
        true_target, false_target = _connected_branch_targets(meta, context, pred_name)
        if target in {true_target, false_target}:
            return _branch_stream_name(target)
        return None

    if st == "switchcase":
        meta = dict(pred.parsed_config) if pred.parsed_config else {}
        if not meta:
            try:
                from .step_xml import get_step_element, parse_switch_case_config

                step_el = get_step_element(pred)
                if step_el is not None:
                    meta = parse_switch_case_config(step_el)
            except Exception:
                meta = {}
        targets = {
            (c.get("target_step") if isinstance(c, dict) else "")
            for c in (meta.get("cases") or [])
        }
        default = (meta.get("default_target_step") or "").strip()
        if default:
            targets.add(default)
        if target in targets:
            return _branch_stream_name(target)
        return None

    return None


def _collect_condition_fields(node: dict[str, Any] | None) -> list[str]:
    """Collect stream field names referenced by a FilterRows condition tree."""
    if not isinstance(node, dict):
        return []
    fields: list[str] = []
    left = (node.get("leftvalue") or "").strip()
    right = (node.get("rightvalue") or "").strip()
    if left:
        fields.append(left)
    if right and not _looks_like_literal(right):
        fields.append(right)
    for child in node.get("conditions") or []:
        fields.extend(_collect_condition_fields(child))
    # Preserve order, drop empties/duplicates
    seen: set[str] = set()
    ordered: list[str] = []
    for name in fields:
        if name and name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def _condition_from_metadata(metadata: dict[str, Any]) -> FilterExpressionResult:
    condition = metadata.get("filter_condition") or metadata.get("condition")
    compare_value = (metadata.get("compare_value") or "").strip()
    warnings: list[str] = []
    parts: list[str] = []

    if condition:
        tree_result = convert_filter_condition_from_metadata(condition)
        warnings.extend(tree_result.warnings)
        if not tree_result.ok:
            return tree_result
        if tree_result.expr and tree_result.expr != "lit(False)":
            parts.append(tree_result.expr)

    if compare_value:
        compare_expr = convert_simple_condition(compare_value)
        if compare_expr and compare_expr != "lit(False)":
            parts.append(compare_expr)
        elif not parts:
            return FilterExpressionResult(
                expr="lit(False)",
                warnings=[f"Could not convert compare_value text condition: {compare_value!r}"],
                ok=False,
            )
        else:
            warnings.append(
                f"compare_value text condition could not be converted and was ignored: {compare_value!r}"
            )

    if not parts:
        return FilterExpressionResult(
            expr="lit(False)",
            warnings=["FilterRows metadata is missing condition and compare_value"],
            ok=False,
        )

    combined = parts[0] if len(parts) == 1 else " & ".join(f"({p})" for p in parts)
    return FilterExpressionResult(combined, warnings=warnings)


def _unresolved_lines(step_name: str, out_var: str, message: str) -> list[str]:
    return [
        f"# WARNING: FilterRows '{step_name}': {message}",
        f"{out_var} = spark.createDataFrame([], '_filter_unresolved STRING')",
        f"{out_var} = {out_var}.filter(lit(False))  # unresolved condition → no rows",
    ]


def _connected_branch_targets(
    metadata: dict[str, Any],
    context: StepContext | None,
    step_name: str,
) -> tuple[str | None, str | None]:
    send_true = (metadata.get("send_true_to") or "").strip()
    send_false = (metadata.get("send_false_to") or "").strip()
    successors: set[str] = set()
    if context is not None:
        successors = set(context.dag.successors(step_name))

    def _is_connected(target: str) -> bool:
        if not target:
            return False
        if not successors:
            return True
        return target in successors

    true_target = send_true if _is_connected(send_true) else None
    false_target = send_false if _is_connected(send_false) else None

    if successors:
        if not true_target and not false_target:
            ordered = sorted(successors)
            if len(ordered) == 1:
                sole = ordered[0]
                if send_false and sole == send_false:
                    false_target = send_false
                elif send_true and sole == send_true:
                    true_target = send_true
                else:
                    true_target = sole
            elif len(ordered) >= 2:
                if send_true in successors:
                    true_target = send_true
                else:
                    true_target = ordered[0]
                if send_false in successors:
                    false_target = send_false
                else:
                    remaining = [s for s in ordered if s != true_target]
                    false_target = remaining[0] if remaining else None
        elif len(successors) >= 2:
            if true_target and not false_target:
                remaining = sorted(successors - {true_target})
                if send_false in remaining:
                    false_target = send_false
                elif remaining:
                    false_target = remaining[0]
            elif false_target and not true_target:
                remaining = sorted(successors - {false_target})
                if send_true in remaining:
                    true_target = send_true
                elif remaining:
                    true_target = remaining[0]

        if true_target and true_target not in successors:
            true_target = None
        if false_target and false_target not in successors:
            false_target = None

    return true_target, false_target


def convert_filter_rows_step(
    metadata: dict[str, Any],
    in_df: str | None,
    out_var: str,
    step_name: str,
    context: StepContext | None = None,
) -> tuple[list[str], str]:
    """Generate PySpark lines for a Filter Rows step from propagated metadata."""
    lines = [f"# Filter Rows: {step_name}"]

    if not in_df:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
        return lines, "converted"

    condition_result = _condition_from_metadata(metadata)
    for warning in condition_result.warnings:
        lines.append(f"# WARNING: {warning}")

    if not condition_result.ok or not condition_result.expr or condition_result.expr == "lit(False)":
        lines.extend(
            _unresolved_lines(
                step_name,
                out_var,
                "filter logic is missing or unsupported in metadata",
            )
        )
        return lines, "partial"

    filter_expr = condition_result.expr
    true_target, false_target = _connected_branch_targets(metadata, context, step_name)

    condition = metadata.get("filter_condition") or metadata.get("condition")
    required_fields = _collect_condition_fields(condition if isinstance(condition, dict) else None)
    if required_fields:
        req_list = ", ".join(repr(c) for c in required_fields)
        lines.append(f"_filter_required = [{req_list}]")
        lines.append(
            f"_filter_missing = [c for c in _filter_required if c not in {in_df}.columns]"
        )
        lines.append("if _filter_missing:")
        lines.append(
            f'    raise ValueError('
            f'f"Column {{_filter_missing[0]}} missing before {step_name} step '
            f'(missing={{_filter_missing}}, available={{list({in_df}.columns)}})")'
        )

    if true_target and false_target:
        # Use raw df_<TargetStep> names so Dummy (df_Dummy_*) can pass the stream through
        # without overwriting the branch DataFrame with the Filter primary output.
        true_var = _branch_stream_name(true_target)
        false_var = _branch_stream_name(false_target)
        lines.append(f"{true_var} = {in_df}.filter({filter_expr})")
        lines.append(f"{false_var} = {in_df}.filter(~({filter_expr}))")
        if out_var != true_var:
            lines.append(f"{out_var} = {true_var}")
        return lines, "converted"

    if false_target and not true_target:
        false_var = _branch_stream_name(false_target)
        lines.append(f"{false_var} = {in_df}.filter(~({filter_expr}))")
        if out_var != false_var:
            lines.append(f"{out_var} = {false_var}")
        return lines, "converted"

    lines.append(f"{out_var} = {in_df}.filter({filter_expr})")
    return lines, "converted"
