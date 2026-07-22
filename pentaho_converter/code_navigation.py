"""Build navigation metadata linking Pentaho files to generated PySpark sections."""

from __future__ import annotations

import re
from typing import Any

_TRANS_SECTION = re.compile(r"^# Transformation:\s*(.+)\s*$")
_JOB_SECTION = re.compile(r"^# Job:\s*(.+)\s*$")
_STEP_MARKER = re.compile(r"^\s*# Step: (.+?) \((.+?)\) \[(.+?)\]\s*$")


def build_step_to_file(transformations: dict) -> dict[str, str]:
    """Map Pentaho step names to their source .ktr file."""
    step_to_file: dict[str, str] = {}
    for trans in transformations.values():
        file_name = trans.file_path.name
        for step in trans.steps:
            step_to_file[step.name] = file_name
    return step_to_file


def _index_transformation_sections(code: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    lines = code.splitlines()
    sections: list[dict[str, Any]] = []
    by_name: dict[str, int] = {}
    for index, line in enumerate(lines):
        match = _TRANS_SECTION.match(line.strip())
        if not match:
            continue
        name = match.group(1).strip()
        sections.append({"name": name, "line": index + 1})
        by_name[name] = index + 1

    for index, section in enumerate(sections):
        if index + 1 < len(sections):
            section["end_line"] = sections[index + 1]["line"] - 1
        else:
            section["end_line"] = len(lines)
    return sections, by_name


def _index_job_sections(code: str) -> dict[str, int]:
    by_name: dict[str, int] = {}
    for index, line in enumerate(code.splitlines()):
        match = _JOB_SECTION.match(line.strip())
        if match:
            by_name[match.group(1).strip()] = index + 1
    return by_name


def _index_step_blocks(code: str) -> list[dict[str, Any]]:
    """Index generated code blocks delimited by ``# Step:`` markers."""
    lines = code.splitlines()
    blocks: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        match = _STEP_MARKER.match(line)
        if not match:
            continue
        blocks.append({
            "step_name": match.group(1).strip(),
            "step_type": match.group(2).strip(),
            "generation_status": match.group(3).strip(),
            "marker_line": index + 1,
        })

    for index, block in enumerate(blocks):
        start = block["marker_line"]
        end = blocks[index + 1]["marker_line"] - 1 if index + 1 < len(blocks) else len(lines)
        block["start_line"] = start
        block["end_line"] = end
        block_lines = lines[start - 1 : end]

        header_line = start
        code_start = start
        code_end = start
        for offset, block_line in enumerate(block_lines):
            absolute = start + offset
            stripped = block_line.strip()
            if stripped.startswith("# ") and not stripped.startswith("# Step:"):
                header_line = absolute
            if stripped and not stripped.startswith("#"):
                if code_start == start:
                    code_start = absolute
                code_end = absolute
        block["header_line"] = header_line
        block["code_start_line"] = code_start if code_end >= code_start else header_line
        block["code_end_line"] = code_end if code_end >= code_start else header_line
    return blocks


def _severity_for_status(status: str) -> str:
    if status in ("failed", "unsupported", "skipped"):
        return "error"
    if status in ("partial", "partially_supported", "approximated", "manual_required"):
        return "warning"
    return "info"


def _expected_hint(step_type: str, message: str) -> str | None:
    lower = message.lower()
    step_lower = (step_type or "").lower()
    if ".filter()" in message or step_lower == "filterrows":
        return "customers.filter(...)"
    if "calculator" in lower or step_lower == "calculator":
        return "df.withColumn(...)"
    if "groupby" in lower or step_lower in ("groupby", "memorygroupby"):
        return "df.groupBy(...).agg(...)"
    if step_lower == "analyticquery":
        return "df.withColumn(..., lag/lead(...).over(Window...))"
    if step_lower in ("samplerows", "reservoirsampling"):
        return "df.sample(...) / filter(row_number) / limit(...)"
    if step_lower in ("univariatestats", "univariatestatistics"):
        return "df.agg(count/avg/stddev/...)"
    if step_lower in ("stepsmetrics", "outputstepsmetrics"):
        return "spark.createDataFrame([{step metrics}])"
    if "selectvalues" in lower or step_lower == "selectvalues":
        return "df.select(...)"
    if step_lower in ("setvalueconstant", "setvaluefield", "constant"):
        return "df.withColumn(...)"
    if step_lower == "concatfields":
        return "df.withColumn(..., concat_ws(...))"
    if step_lower == "addxml":
        return "df.withColumn(..., concat(...))"
    if step_lower in ("replaceinstring", "replacestring", "stringoperations", "stringcut"):
        return "df.withColumn(..., regexp_replace(...))"
    if "placeholder" in lower:
        return "Real PySpark logic instead of placeholder DataFrame"
    return None


def _resolve_issue_lines(
    message: str,
    block_lines: list[str],
    start_line: int,
    step_type: str,
    *,
    header_line: int,
    code_start_line: int,
    code_end_line: int,
) -> tuple[int, int]:
    """Map a validation message to precise generated-code line numbers."""
    lower = message.lower()

    if "placeholder" in lower:
        for offset, block_line in enumerate(block_lines):
            if (
                "placeholder STRING" in block_line
                or "'_placeholder" in block_line
                or '"_placeholder' in block_line
            ):
                line = start_line + offset
                return line, line

    if ".filter(" in message or "filterrows" in lower:
        for offset, block_line in enumerate(block_lines):
            if ".filter(" in block_line:
                line = start_line + offset
                return line, line
        if code_start_line >= start_line:
            return code_start_line, max(code_start_line, code_end_line)
        return header_line, header_line

    if "calculation" in lower or (step_type or "").lower() == "calculator":
        for offset, block_line in enumerate(block_lines):
            if "Calculator:" in block_line:
                line = start_line + offset
                return line, line
        if code_start_line >= start_line:
            return code_start_line, max(code_start_line, code_end_line)
        return header_line, header_line

    if "withcolumn" in lower:
        for offset, block_line in enumerate(block_lines):
            if ".withColumn(" in block_line:
                line = start_line + offset
                return line, line

    for offset, block_line in enumerate(block_lines):
        if "# WARNING:" in block_line and any(
            token in block_line for token in message.split()[:4] if len(token) > 3
        ):
            line = start_line + offset
            return line, line

    for offset, block_line in enumerate(block_lines):
        if "# WARNING:" in block_line:
            line = start_line + offset
            return line, line

    if code_start_line >= start_line:
        return code_start_line, max(code_start_line, code_end_line)
    return header_line, header_line


def _compute_highlight_level(
    status: str,
    score: int,
    errors_count: int,
    warnings_count: int,
    todo_count: int,
    has_placeholder: bool,
) -> str:
    """Derive viewer highlight level from step validation metadata."""
    status_lower = (status or "").lower()
    if (
        status_lower == "converted"
        and score == 100
        and warnings_count == 0
        and errors_count == 0
        and todo_count == 0
        and not has_placeholder
    ):
        return "success"
    if (
        status_lower in ("failed", "unsupported", "skipped")
        or errors_count > 0
        or has_placeholder
        or todo_count > 0
        or score < 70
    ):
        return "error"
    if (
        warnings_count > 0
        or status_lower in ("partial", "partially_supported", "approximated", "manual_required")
        or 70 <= score <= 99
    ):
        return "warning"
    return "error"


def _step_has_placeholder(block_lines: list[str], errors: list[str]) -> bool:
    if any(
        "placeholder STRING" in err
        or "createDataFrame([], '_placeholder" in err
        or 'createDataFrame([], "_placeholder' in err
        for err in errors
    ):
        return True
    # Match empty-DF sentinel only — not identifiers like null_placeholder_df
    return any(
        "placeholder STRING" in line
        or "'_placeholder" in line
        or '"_placeholder' in line
        for line in block_lines
    )


def _step_todo_count(status: str, errors: list[str]) -> int:
    if (status or "").lower() == "manual_required":
        return 1
    if any("todo" in err.lower() for err in errors):
        return 1
    return 0


def _build_step_issues(
    step_result: Any,
    block: dict[str, Any],
    code_lines: list[str],
) -> list[dict[str, Any]]:
    start = block["start_line"]
    end = block["end_line"]
    block_lines = code_lines[start - 1 : end]
    step_type = getattr(step_result, "step_type", block.get("step_type", ""))
    status = getattr(step_result, "status", block.get("generation_status", ""))

    messages: list[tuple[str, str]] = []
    for err in getattr(step_result, "errors", []) or []:
        if err:
            messages.append(("error", err))
    for warn in getattr(step_result, "warnings", []) or []:
        if warn:
            messages.append(("warning", warn))

    if not messages:
        detail = getattr(step_result, "detail", "") or ""
        if detail and status not in ("converted",):
            messages.append((_severity_for_status(status), detail))

    issues: list[dict[str, Any]] = []
    for severity, message in messages:
        line, end_line = _resolve_issue_lines(
            message,
            block_lines,
            start,
            step_type,
            header_line=block["header_line"],
            code_start_line=block["code_start_line"],
            code_end_line=block["code_end_line"],
        )
        issues.append({
            "message": message,
            "severity": severity,
            "line": line,
            "end_line": end_line,
            "expected": _expected_hint(step_type, message),
        })

    if not issues:
        if (
            status == "converted"
            and not getattr(step_result, "errors", [])
            and not getattr(step_result, "warnings", [])
        ):
            return []
        issues.append({
            "message": getattr(step_result, "detail", "") or f"Step: {block['step_name']}",
            "severity": _severity_for_status(status),
            "line": block["code_start_line"],
            "end_line": block["code_end_line"],
            "expected": None,
        })
    return issues


def _build_step_entry(
    step_result: Any,
    block: dict[str, Any],
    code_lines: list[str],
    *,
    generated_file: str,
    ktr_file: str,
) -> dict[str, Any]:
    start = block["start_line"]
    end = block["end_line"]
    block_lines = code_lines[start - 1 : end]
    status = getattr(step_result, "status", block.get("generation_status", ""))
    errors_list = list(getattr(step_result, "errors", []) or [])
    warnings_list = list(getattr(step_result, "warnings", []) or [])
    score = int(round(float(getattr(step_result, "semantic_score", 0) or 0) * 100))
    has_placeholder = _step_has_placeholder(block_lines, errors_list)
    todo_count = _step_todo_count(status, errors_list)
    issues = _build_step_issues(step_result, block, code_lines)
    highlight_level = _compute_highlight_level(
        status,
        score,
        len(errors_list),
        len(warnings_list),
        todo_count,
        has_placeholder,
    )
    return {
        "step_name": block["step_name"],
        "step_type": block["step_type"],
        "file": generated_file,
        "ktr_file": ktr_file,
        "start_line": start,
        "end_line": end,
        "header_line": block["header_line"],
        "code_start_line": block["code_start_line"],
        "code_end_line": block["code_end_line"],
        "line": issues[0]["line"] if issues else block["code_start_line"],
        "status": status,
        "score": score,
        "warnings_count": len(warnings_list),
        "errors_count": len(errors_list),
        "todo_count": todo_count,
        "has_placeholder": has_placeholder,
        "highlight_level": highlight_level,
        "issues": issues,
    }


def _build_step_navigation(
    code: str,
    *,
    step_results: list[Any] | None,
    step_to_file: dict[str, str] | None,
    generated_file: str,
) -> dict[str, dict[str, Any]]:
    """Build precise per-step navigation metadata from generated code markers."""
    code_lines = code.splitlines()
    blocks = _index_step_blocks(code)
    blocks_by_name: dict[str, list[dict[str, Any]]] = {}
    for block in blocks:
        blocks_by_name.setdefault(block["step_name"], []).append(block)

    results_by_name: dict[str, list[Any]] = {}
    for result in step_results or []:
        results_by_name.setdefault(result.step_name, []).append(result)

    steps_by_name: dict[str, dict[str, Any]] = {}
    seen_pairs: set[tuple[str, str]] = set()

    for step_name, block_list in blocks_by_name.items():
        result_list = results_by_name.get(step_name, [])
        for index, block in enumerate(block_list):
            step_result = result_list[index] if index < len(result_list) else result_list[0] if result_list else None
            if step_result is None:
                continue

            pair = (step_name, block["step_type"])
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            ktr_file = (step_to_file or {}).get(step_name, "")
            entry = _build_step_entry(
                step_result,
                block,
                code_lines,
                generated_file=generated_file,
                ktr_file=ktr_file,
            )
            steps_by_name[step_name] = entry
            steps_by_name[f"{step_name}\x00{block['step_type']}"] = entry
    return steps_by_name


def _inventory_by_file(inventory: list[dict]) -> dict[str, dict]:
    return {item["file"]: item for item in inventory}


def _nodes_by_file(lineage: dict) -> dict[str, dict]:
    return {node["file"]: node for node in lineage.get("nodes", [])}


def _edges_from(lineage: dict, from_file: str) -> list[dict]:
    edges = [edge for edge in lineage.get("edges", []) if edge.get("from") == from_file]
    return sorted(edges, key=lambda edge: edge.get("sequence", 0))


def _resolve_trans_name_for_file(file_name: str, inventory_by_file: dict[str, dict]) -> str:
    item = inventory_by_file.get(file_name, {})
    return item.get("name") or file_name.rsplit(".", 1)[0]


def _find_section_for_transformation(
    trans_name: str,
    sections_by_name: dict[str, int],
    transformation_sections: list[dict[str, Any]],
) -> dict[str, Any] | None:
    candidates = [trans_name]
    if trans_name not in sections_by_name:
        stem = trans_name.rsplit(".", 1)[0]
        candidates.append(stem)

    for candidate in candidates:
        if candidate in sections_by_name:
            line = sections_by_name[candidate]
            for section in transformation_sections:
                if section["name"] == candidate:
                    return {
                        "line": line,
                        "end_line": section["end_line"],
                        "anchor": f"# Transformation: {candidate}",
                        "name": candidate,
                        "section_type": "transformation",
                    }

    lowered = trans_name.lower()
    for section in transformation_sections:
        if section["name"].lower() == lowered:
            return {
                "line": section["line"],
                "end_line": section["end_line"],
                "anchor": f"# Transformation: {section['name']}",
                "name": section["name"],
                "section_type": "transformation",
            }
    return None


def _find_section_for_job(
    job_name: str,
    job_sections_by_name: dict[str, int],
    transformation_sections: list[dict[str, Any]],
) -> dict[str, Any] | None:
    candidates = [job_name]
    stem = job_name.rsplit(".", 1)[0]
    if stem not in candidates:
        candidates.append(stem)

    for candidate in candidates:
        if candidate not in job_sections_by_name:
            continue
        line = job_sections_by_name[candidate]
        end_line = len(transformation_sections) and transformation_sections[-1]["end_line"] or line
        for section in transformation_sections:
            if section["line"] <= line <= section.get("end_line", line):
                end_line = section.get("end_line", line)
                break
        return {
            "line": line,
            "end_line": end_line,
            "anchor": f"# Job: {candidate}",
            "name": candidate,
            "section_type": "job",
        }

    lowered = job_name.lower()
    for name, line in job_sections_by_name.items():
        if name.lower() == lowered:
            return {
                "line": line,
                "end_line": line,
                "anchor": f"# Job: {name}",
                "name": name,
                "section_type": "job",
            }
    return None


def _walk_job_for_first_section(
    job_file: str,
    lineage: dict,
    inventory_by_file: dict[str, dict],
    sections_by_name: dict[str, int],
    transformation_sections: list[dict[str, Any]],
    visited: set[str] | None = None,
) -> dict[str, Any] | None:
    visited = visited or set()
    if job_file in visited:
        return None
    visited.add(job_file)

    for edge in _edges_from(lineage, job_file):
        target = edge["to"]
        item = inventory_by_file.get(target, {})
        file_type = item.get("type", "")
        if file_type == ".ktr":
            trans_name = _resolve_trans_name_for_file(target, inventory_by_file)
            section = _find_section_for_transformation(
                trans_name, sections_by_name, transformation_sections
            )
            if section:
                return section
        elif file_type == ".kjb":
            nested = _walk_job_for_first_section(
                target,
                lineage,
                inventory_by_file,
                sections_by_name,
                transformation_sections,
                visited,
            )
            if nested:
                return nested
    return None


def _build_outline_node(
    file_name: str,
    lineage: dict,
    inventory_by_file: dict[str, dict],
    nodes_by_file: dict[str, dict],
    sections_by_file: dict[str, dict],
) -> dict[str, Any] | None:
    node = nodes_by_file.get(file_name)
    if not node:
        return None
    item = inventory_by_file.get(file_name, {})
    file_type = item.get("type", ".kjb")
    children: list[dict[str, Any]] = []
    for child_file in node.get("children", []):
        child = _build_outline_node(
            child_file, lineage, inventory_by_file, nodes_by_file, sections_by_file
        )
        if child:
            children.append(child)
    section = sections_by_file.get(file_name) or {}
    return {
        "id": file_name,
        "name": node.get("name") or file_name,
        "type": "job" if file_type == ".kjb" else "transformation",
        "line": section.get("line"),
        "children": children,
    }


def build_code_navigation(
    generated_code: str,
    lineage: dict,
    inventory: list[dict],
    *,
    step_to_file: dict[str, str] | None = None,
    step_results: list[Any] | None = None,
    generated_file: str = "",
) -> dict[str, Any]:
    """Map Pentaho inventory files and steps to generated PySpark line numbers."""
    code = generated_code or ""
    transformation_sections, sections_by_name = _index_transformation_sections(code)
    job_sections_by_name = _index_job_sections(code)
    inventory_by_file = _inventory_by_file(inventory)
    nodes_by_file = _nodes_by_file(lineage)
    sections_by_file: dict[str, dict[str, Any]] = {}

    for file_name, item in inventory_by_file.items():
        if item.get("type") == ".ktr":
            trans_name = _resolve_trans_name_for_file(file_name, inventory_by_file)
            section = _find_section_for_transformation(
                trans_name, sections_by_name, transformation_sections
            )
            if section:
                sections_by_file[file_name] = section
        elif item.get("type") == ".kjb":
            job_name = item.get("name") or file_name.rsplit(".", 1)[0]
            section = _find_section_for_job(
                job_name, job_sections_by_name, transformation_sections
            )
            if not section:
                section = _walk_job_for_first_section(
                    file_name,
                    lineage,
                    inventory_by_file,
                    sections_by_name,
                    transformation_sections,
                )
                if section:
                    section = {
                        **section,
                        "anchor": f"# Job: {job_name}",
                        "name": job_name,
                        "section_type": "job",
                        "via": "first_transformation",
                    }
            if section:
                sections_by_file[file_name] = section

    outline: list[dict[str, Any]] = []
    for root in lineage.get("root_jobs") or []:
        node = _build_outline_node(
            root, lineage, inventory_by_file, nodes_by_file, sections_by_file
        )
        if node:
            outline.append(node)

    outlined_ids: set[str] = set()

    def _collect_ids(node: dict[str, Any]) -> None:
        outlined_ids.add(node["id"])
        for child in node.get("children", []):
            _collect_ids(child)

    for node in outline:
        _collect_ids(node)

    for file_name, item in inventory_by_file.items():
        if file_name in outlined_ids:
            continue
        section = sections_by_file.get(file_name) or {}
        outline.append({
            "id": file_name,
            "name": item.get("name") or file_name,
            "type": "job" if item.get("type") == ".kjb" else "transformation",
            "line": section.get("line"),
            "children": [],
        })

    steps_by_name = _build_step_navigation(
        code,
        step_results=step_results,
        step_to_file=step_to_file,
        generated_file=generated_file,
    )

    sections_by_step: dict[str, dict[str, Any]] = {}
    for step_name, entry in steps_by_name.items():
        if "\x00" in step_name:
            continue
        sections_by_step[step_name] = entry

    return {
        "sections_by_file": sections_by_file,
        "sections_by_step": sections_by_step,
        "steps_by_name": steps_by_name,
        "transformation_sections": transformation_sections,
        "outline": outline,
    }
