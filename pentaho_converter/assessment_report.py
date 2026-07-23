"""Conversion Assessment Report enrichment for the Compare Code UI.

Reporting / navigation metadata only — does not alter conversion, generation,
or execution behavior.
"""

from __future__ import annotations

from typing import Any

# Canonical assessment buckets shown in the Compare Code report
ASSESSMENT_FULL = "fully_converted"
ASSESSMENT_PARTIAL = "partially_converted"
ASSESSMENT_MANUAL = "manual_required"

_PARTIAL_STATUSES = frozenset({"partial", "partially_supported", "approximated"})
_MANUAL_STATUSES = frozenset({
    "manual_required",
    "failed",
    "unsupported",
    "skipped",
})

_STEP_IMPL_HINTS: dict[str, str] = {
    "javascript": "Port the JavaScript step to PySpark DataFrame / SQL expressions, or use a Pandas UDF only where row logic cannot be expressed declaratively.",
    "scriptvalueemod": "Rewrite Script Value Mod logic as Spark column expressions or a controlled UDF; avoid one-to-one Rhino JS execution.",
    "userdefinedjavaclass": "Reimplement UDJC in PySpark (DataFrame API / Spark SQL). Preserve input/output schema and side-effect ordering.",
    "userdefinedjavaexpression": "Convert the Java expression to an equivalent Spark SQL / Column expression.",
    "rules": "Encode business rules as Spark filters, when/otherwise chains, or an external rules service invoked from PySpark.",
    "sapinput": "Use an SAP connector (JCo/ODP/Open Hub) or intermediate extract files, then load into Spark as a DataFrame.",
    "databaselookup": "Replace with a broadcast/hash join against the lookup table; verify join keys, nulls, and multi-match behaviour.",
    "databaselookupstream": "Implement as a left join (or map-side lookup) and confirm cache / first-match semantics.",
    "httpclient": "Use requests/httpx outside Spark or mapPartitions for HTTP calls; prefer batching and idempotent retries.",
    "rest": "Call the REST API from a driver-side or mapPartitions client and return a structured DataFrame.",
    "ldapinput": "Extract via an LDAP client, materialize rows, then create a Spark DataFrame with the expected schema.",
    "mail": "Send mail from the job orchestrator (driver) after the Spark action completes; do not rely on per-row SMTP inside executors.",
    "jobexecutor": "Invoke the converted child job module from the orchestrator with the same parameter contract.",
    "transexecutor": "Call the converted transformation `run_*` entry point and wire returned DataFrames explicitly.",
    "mapping": "Inline or call the mapped sub-transformation; align stream names and field renames.",
    "simplemapping": "Call the mapped transformation function and project columns to match the mapping specification.",
    "filterrows": "Use DataFrame.filter / where with the equivalent boolean expression; verify TRUE/FALSE stream routing.",
    "calculator": "Use withColumn / selectExpr for each calculator function; confirm null and type behaviour.",
    "formula": "Translate LibreOffice-style formulas to Spark SQL expressions.",
    "groupby": "Use groupBy().agg(...); verify aggregate types and null grouping keys.",
    "memorygroupby": "Same as Group By — prefer Spark aggregations over in-memory row loops.",
    "mergejoin": "Use DataFrame.join with the correct join type and key columns.",
    "streamlookup": "Left-join the lookup stream (broadcast if small); confirm first-match vs multi-match.",
    "dimensionlookup": "Implement SCD Type 1/2 logic with joins + window functions or MERGE INTO on Delta.",
    "combinationlookup": "Upsert technical keys with a deterministic hash/join; persist the combo table.",
    "textfileinput": "spark.read.csv / text with matching delimiter, header, encoding, and schema.",
    "textfileoutput": "df.write.csv (or format) with matching delimiter, header, compression, and path.",
    "tableinput": "spark.read.jdbc / table with the same SQL and connection options.",
    "tableoutput": "df.write.jdbc / saveAsTable with truncate/append semantics matching Pentaho.",
    "insertupdate": "Use MERGE INTO (Delta) or JDBC upsert; preserve key columns and update field list.",
    "update": "Targeted MERGE/UPDATE by key columns; verify where-clause equivalence.",
    "delete": "DELETE/MERGE by keys, or filter-and-rewrite for non-Delta sinks.",
    "abort": "Raise a controlled exception when the abort condition is met.",
    "dummy": "Pass-through — return the input DataFrame unchanged.",
    "selectvalues": "df.select / withColumnRenamed / cast to match metadata changes.",
    "valuemapper": "Use when/otherwise or a broadcast map join for value substitutions.",
    "nullif": "Replace matching values with null via when/otherwise.",
    "ifnull": "Use coalesce / when(col.isNull(), ...).",
    "addxml": "Build XML strings with concat/concat_ws or to_json depending on target format.",
    "getxmldata": "Parse XML with from_xml / xpath / explode into relational columns.",
    "jsoninput": "spark.read.json or from_json with the configured schema paths.",
    "jsonoutput": "to_json / write.json with the required structure.",
}


def map_assessment_status(status: str) -> str:
    """Map converter status to exactly one assessment bucket."""
    st = (status or "").lower()
    if st == "converted":
        return ASSESSMENT_FULL
    if st in _PARTIAL_STATUSES:
        return ASSESSMENT_PARTIAL
    if st in _MANUAL_STATUSES:
        return ASSESSMENT_MANUAL
    # Unknown statuses require developer attention
    return ASSESSMENT_MANUAL


def assessment_label(bucket: str) -> str:
    if bucket == ASSESSMENT_FULL:
        return "Fully Converted"
    if bucket == ASSESSMENT_PARTIAL:
        return "Partially Converted"
    return "Manual Implementation Required"


def assessment_icon(bucket: str) -> str:
    if bucket == ASSESSMENT_FULL:
        return "🟢"
    if bucket == ASSESSMENT_PARTIAL:
        return "🟡"
    return "🔴"


def _hint_for_step_type(step_type: str) -> str:
    key = (step_type or "").strip().lower()
    if key in _STEP_IMPL_HINTS:
        return _STEP_IMPL_HINTS[key]
    # Fuzzy contains for compound type names
    for token, hint in _STEP_IMPL_HINTS.items():
        if token in key:
            return hint
    return (
        f"Implement equivalent PySpark DataFrame logic for Pentaho step type '{step_type or 'unknown'}', "
        "preserving schema, filters, and side effects."
    )


def _join_messages(*groups: list[str], limit: int = 8) -> str:
    parts: list[str] = []
    for group in groups:
        for item in group or []:
            text = str(item).strip()
            if text and text not in parts:
                parts.append(text)
            if len(parts) >= limit:
                return "; ".join(parts)
    return "; ".join(parts)


def _what_was_converted(step: Any, bucket: str) -> str:
    detail = (getattr(step, "detail", "") or "").strip()
    infos = list(getattr(step, "infos", []) or [])
    if bucket == ASSESSMENT_FULL:
        if detail:
            return detail
        return "Step logic was converted to executable PySpark."
    if bucket == ASSESSMENT_PARTIAL:
        base = detail or "Core step structure was generated."
        if infos:
            return f"{base} Additional notes: {_join_messages(infos, limit=4)}"
        return base
    return detail or "No automatic conversion was produced for this step."


def _what_is_missing(step: Any, bucket: str) -> str:
    warnings = list(getattr(step, "warnings", []) or [])
    errors = list(getattr(step, "errors", []) or [])
    detail = (getattr(step, "detail", "") or "").strip()
    if bucket == ASSESSMENT_FULL:
        if warnings:
            return f"No blocking gaps. Review warnings: {_join_messages(warnings)}"
        return "None — no manual implementation is required."
    if bucket == ASSESSMENT_PARTIAL:
        missing = _join_messages(errors, warnings)
        return missing or (
            "Some behaviour may differ from Pentaho (edge cases, options, or exact semantics)."
        )
    missing = _join_messages(errors, warnings)
    return missing or detail or "Automatic conversion is not available for this step type/configuration."


def _why_not_automatic(step: Any, bucket: str) -> str:
    status = (getattr(step, "status", "") or "").lower()
    step_type = getattr(step, "step_type", "") or "unknown"
    if bucket == ASSESSMENT_FULL:
        return "N/A — conversion completed automatically."
    if bucket == ASSESSMENT_PARTIAL:
        return (
            f"Step type '{step_type}' was only partially mapped "
            f"(converter status: {status or 'partial'}). "
            "Exact Pentaho semantics could not be guaranteed for every option."
        )
    if status == "manual_required":
        return (
            f"Step type '{step_type}' requires developer-authored logic "
            "(scripting, proprietary connector, or unsupported construct)."
        )
    if status in ("unsupported", "failed", "skipped"):
        return (
            f"Automatic conversion failed or is unsupported for '{step_type}' "
            f"(status: {status})."
        )
    return f"Automatic conversion could not complete for '{step_type}'."


def _verify_checklist(step: Any, bucket: str) -> list[str]:
    if bucket == ASSESSMENT_FULL:
        warns = list(getattr(step, "warnings", []) or [])
        return [f"Confirm warning: {w}" for w in warns[:5]] if warns else [
            "Spot-check output schema and row counts against Pentaho.",
        ]
    if bucket == ASSESSMENT_PARTIAL:
        items = [
            "Compare output columns and types with the Pentaho step.",
            "Validate null handling and default values.",
            "Run a sample data diff against Pentaho results.",
        ]
        for w in list(getattr(step, "warnings", []) or [])[:3]:
            items.append(f"Review: {w}")
        return items
    return [
        "Implement the missing behaviour in the generated function.",
        "Preserve input/output schema expected by downstream steps.",
        "Add tests with representative Pentaho sample data.",
        "Update job/transformation wiring if the function signature changes.",
    ]


def _developer_notes(step: Any, bucket: str) -> str:
    parts = [
        _what_was_converted(step, bucket),
        f"Missing: {_what_is_missing(step, bucket)}",
        f"Why: {_why_not_automatic(step, bucket)}",
    ]
    verify = _verify_checklist(step, bucket)
    if verify:
        parts.append("Verify: " + " | ".join(verify[:4]))
    return "\n".join(parts)


def _recommended_implementation(step: Any, bucket: str) -> str:
    step_type = getattr(step, "step_type", "") or ""
    hint = _hint_for_step_type(step_type)
    if bucket == ASSESSMENT_FULL:
        return "No change required. Keep the generated PySpark function as-is."
    if bucket == ASSESSMENT_PARTIAL:
        return (
            f"{hint} Review the generated function and replace any approximated "
            "sections with exact business logic where needed."
        )
    return (
        f"{hint} Replace the generated placeholder/stub with production-ready "
        "PySpark that returns the expected DataFrame."
    )


def build_placeholder_comment(step: Any, bucket: str) -> str:
    """Readable comment block for developers (report/copy — not injected into Spark)."""
    name = getattr(step, "step_name", "") or "Unknown Step"
    step_type = getattr(step, "step_type", "") or "Unknown"
    reason = _why_not_automatic(step, bucket)
    expected = _what_was_converted(step, bucket)
    suggested = _recommended_implementation(step, bucket)
    missing = _what_is_missing(step, bucket)

    if bucket == ASSESSMENT_FULL:
        return ""

    if bucket == ASSESSMENT_PARTIAL:
        verify_lines = "\n".join(f"# - {item}" for item in _verify_checklist(step, bucket)[:5])
        return (
            "# ===================================================================\n"
            "# MANUAL IMPLEMENTATION REQUIRED\n"
            "#\n"
            f"# Pentaho Step : {name}\n"
            f"# Step Type    : {step_type}\n"
            "#\n"
            "# Automatic conversion completed partially.\n"
            "#\n"
            f"# What was converted:\n#     {expected}\n"
            "#\n"
            f"# What is missing:\n#     {missing}\n"
            "#\n"
            "# Verify:\n"
            f"{verify_lines}\n"
            "#\n"
            "# Suggested implementation:\n"
            f"#     {suggested}\n"
            "# ==================================================================="
        )

    return (
        "############################################################\n"
        "# MANUAL IMPLEMENTATION REQUIRED\n"
        "#\n"
        "# Pentaho Step:\n"
        f"#     {name}\n"
        "#\n"
        "# Step Type:\n"
        f"#     {step_type}\n"
        "#\n"
        "# Reason:\n"
        f"#     {reason}\n"
        "#\n"
        "# Expected Behaviour:\n"
        f"#     {expected}\n"
        "#\n"
        "# Functionality Missing:\n"
        f"#     {missing}\n"
        "#\n"
        "# Suggested PySpark Implementation:\n"
        f"#     {suggested}\n"
        "#\n"
        "# TODO:\n"
        f"#     Implement {name} and return the expected Spark DataFrame.\n"
        "############################################################"
    )


def _lookup_nav_entry(
    code_navigation: dict[str, Any] | None,
    step: Any,
) -> dict[str, Any]:
    steps = (code_navigation or {}).get("steps_by_name") or {}
    name = getattr(step, "step_name", "") or ""
    step_type = getattr(step, "step_type", "") or ""
    trans = getattr(step, "transformation_name", "") or ""

    # Prefer composite keys used by the indexer when present
    for key in (
        f"{trans}\x00{name}\x00{step_type}",
        f"{trans}\x00{name}",
        name,
    ):
        entry = steps.get(key)
        if entry:
            return entry

    # Fallback: scan values
    for entry in steps.values():
        if not isinstance(entry, dict):
            continue
        if (entry.get("step_name") or "") != name:
            continue
        if trans and (entry.get("transformation_name") or "") not in ("", trans):
            continue
        return entry
    return {}


def enrich_step_assessment(
    step: Any,
    *,
    code_navigation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one assessment row from a StepConversionResult (+ optional nav)."""
    bucket = map_assessment_status(getattr(step, "status", "") or "")
    nav = _lookup_nav_entry(code_navigation, step)

    generated_file = (
        getattr(step, "generated_file", "") or nav.get("file") or ""
    )
    function_name = (
        getattr(step, "function_name", "") or nav.get("function_name") or ""
    )
    start_line = getattr(step, "start_line", None)
    end_line = getattr(step, "end_line", None)
    if start_line is None:
        start_line = nav.get("code_start_line") or nav.get("start_line") or nav.get("line")
    if end_line is None:
        end_line = nav.get("code_end_line") or nav.get("end_line") or start_line

    transformation_name = (
        getattr(step, "transformation_name", "")
        or nav.get("transformation_name")
        or ""
    )

    confidence = int(round(float(getattr(step, "semantic_score", 0) or 0) * 100))
    if "score" in nav and nav.get("score") is not None:
        try:
            confidence = int(nav["score"])
        except (TypeError, ValueError):
            pass

    manual_work = bucket != ASSESSMENT_FULL
    issues = list(nav.get("issues") or [])
    expected_hints = [
        i.get("expected") for i in issues if isinstance(i, dict) and i.get("expected")
    ]

    recommended = _recommended_implementation(step, bucket)
    if expected_hints:
        recommended = f"{recommended} Example: {expected_hints[0]}"

    return {
        "name": getattr(step, "step_name", "") or "",
        "type": getattr(step, "step_type", "") or "",
        "status": getattr(step, "status", "") or "",
        "display_status": getattr(step, "display_status", "") or "",
        "assessment_status": bucket,
        "assessment_label": assessment_label(bucket),
        "assessment_icon": assessment_icon(bucket),
        "semantic_score": confidence,
        "confidence": confidence,
        "has_logic": (getattr(step, "status", "") or "") == "converted",
        "detail": getattr(step, "detail", "") or "",
        "warnings": list(getattr(step, "warnings", []) or []),
        "errors": list(getattr(step, "errors", []) or []),
        "infos": list(getattr(step, "infos", []) or []),
        "transformation_name": transformation_name,
        "generated_file": generated_file,
        "function_name": function_name,
        "start_line": start_line,
        "end_line": end_line,
        "manual_work_required": manual_work,
        "what_converted": _what_was_converted(step, bucket),
        "what_missing": _what_is_missing(step, bucket),
        "why_not_converted": _why_not_automatic(step, bucket),
        "verify_checklist": _verify_checklist(step, bucket),
        "developer_notes": _developer_notes(step, bucket),
        "recommended_implementation": recommended,
        "placeholder_comment": build_placeholder_comment(step, bucket),
        "highlight_level": nav.get("highlight_level") or (
            "success" if bucket == ASSESSMENT_FULL
            else ("warning" if bucket == ASSESSMENT_PARTIAL else "error")
        ),
        "has_placeholder": bool(nav.get("has_placeholder")),
        "issues": issues,
    }


def estimate_manual_work(partial: int, manual: int, total: int) -> str:
    if total <= 0:
        return "None"
    remaining = partial + manual
    ratio = remaining / total
    if remaining == 0:
        return "None"
    if ratio <= 0.05 or remaining <= 3:
        return "Low"
    if ratio <= 0.15 or remaining <= 12:
        return "Medium"
    return "High"


def build_assessment_summary(steps: list[dict[str, Any]], *, accuracy_score: int = 0) -> dict[str, Any]:
    total = len(steps)
    full = sum(1 for s in steps if s.get("assessment_status") == ASSESSMENT_FULL)
    partial = sum(1 for s in steps if s.get("assessment_status") == ASSESSMENT_PARTIAL)
    manual = sum(1 for s in steps if s.get("assessment_status") == ASSESSMENT_MANUAL)
    if total and not accuracy_score:
        accuracy_score = int(round(sum(int(s.get("confidence") or 0) for s in steps) / total))
    return {
        "overall_conversion_score": accuracy_score,
        "total_steps": total,
        "fully_converted": full,
        "partially_converted": partial,
        "manual_required": manual,
        "estimated_manual_work": estimate_manual_work(partial, manual, total),
    }


def build_assessment_payload(
    step_results: list[Any],
    *,
    code_navigation: dict[str, Any] | None = None,
    accuracy_score: int = 0,
) -> dict[str, Any]:
    """Full assessment payload for the Compare Code / Assessment Report UI."""
    transformations = [
        enrich_step_assessment(step, code_navigation=code_navigation)
        for step in (step_results or [])
    ]
    summary = build_assessment_summary(transformations, accuracy_score=accuracy_score)
    return {
        "assessment_summary": summary,
        "transformations": transformations,
    }
