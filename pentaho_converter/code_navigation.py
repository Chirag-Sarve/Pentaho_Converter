"""Build navigation metadata linking Pentaho files to generated PySpark sections."""

from __future__ import annotations

import re
from typing import Any

_TRANS_SECTION = re.compile(r"^# Transformation:\s*(.+)\s*$")
_JOB_SECTION = re.compile(r"^# Job:\s*(.+)\s*$")
# Legacy marker: # Step: Name (Type) [status]
_STEP_MARKER_LEGACY = re.compile(r"^\s*# Step: (.+?) \((.+?)\) \[(.+?)\]\s*$")
# Current generator: # Step 09 : Lookup Provider
_STEP_MARKER_NUMBERED = re.compile(r"^\s*# Step\s+(\d+)\s*:\s*(.+?)\s*$")
_STEP_DEF = re.compile(r"^(\s*)def (step_\d+_[A-Za-z0-9_]+)\s*\(")
_TRANS_IN_CALL = re.compile(r"transformation\s*=\s*['\"]([^'\"]+)['\"]")


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


def _scan_block_extents(block_lines: list[str], start: int) -> dict[str, int]:
    """Derive header / code extents within a step block."""
    header_line = start
    code_start = start
    code_end = start
    for offset, block_line in enumerate(block_lines):
        absolute = start + offset
        stripped = block_line.strip()
        if stripped.startswith("# ") and not (
            stripped.startswith("# Step:") or _STEP_MARKER_NUMBERED.match(stripped)
        ):
            header_line = absolute
        if stripped and not stripped.startswith("#"):
            if code_start == start:
                code_start = absolute
            code_end = absolute
    return {
        "header_line": header_line,
        "code_start_line": code_start if code_end >= code_start else header_line,
        "code_end_line": code_end if code_end >= code_start else header_line,
    }


def _guess_transformation_name(block_lines: list[str]) -> str:
    for line in block_lines:
        match = _TRANS_IN_CALL.search(line)
        if match:
            return match.group(1).strip()
    return ""


def _index_step_blocks(code: str) -> list[dict[str, Any]]:
    """Index generated step blocks (legacy markers or numbered ``# Step N :`` + def)."""
    lines = code.splitlines()
    raw: list[dict[str, Any]] = []

    for index, line in enumerate(lines):
        legacy = _STEP_MARKER_LEGACY.match(line)
        if legacy:
            raw.append({
                "step_name": legacy.group(1).strip(),
                "step_type": legacy.group(2).strip(),
                "generation_status": legacy.group(3).strip(),
                "marker_line": index + 1,
                "function_name": "",
                "step_number": None,
            })
            continue
        numbered = _STEP_MARKER_NUMBERED.match(line)
        if numbered:
            raw.append({
                "step_name": numbered.group(2).strip(),
                "step_type": "",
                "generation_status": "",
                "marker_line": index + 1,
                "function_name": "",
                "step_number": int(numbered.group(1)),
            })

    # Attach ``def step_NN_...`` to the nearest preceding marker when possible.
    for index, line in enumerate(lines):
        def_match = _STEP_DEF.match(line)
        if not def_match:
            continue
        func_name = def_match.group(2)
        attached = False
        for block in reversed(raw):
            if block["marker_line"] > index + 1 or block.get("function_name"):
                continue
            gap = lines[block["marker_line"] : index]
            if any(_STEP_DEF.match(g) for g in gap):
                continue
            if any(
                _STEP_MARKER_LEGACY.match(g) or _STEP_MARKER_NUMBERED.match(g)
                for g in gap
            ):
                continue
            block["function_name"] = func_name
            attached = True
            break
        if not attached:
            parts = func_name.split("_", 2)
            approx_name = parts[2].replace("_", " ") if len(parts) >= 3 else func_name
            raw.append({
                "step_name": approx_name,
                "step_type": "",
                "generation_status": "",
                "marker_line": index + 1,
                "function_name": func_name,
                "step_number": int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None,
            })

    raw.sort(key=lambda b: b["marker_line"])
    blocks: list[dict[str, Any]] = []
    for index, block in enumerate(raw):
        start = block["marker_line"]
        end = raw[index + 1]["marker_line"] - 1 if index + 1 < len(raw) else len(lines)
        for line_no in range(start, end + 1):
            stripped = lines[line_no - 1].strip()
            if line_no > start and stripped.startswith("def run_"):
                end = line_no - 1
                break
        block_lines = lines[start - 1 : end]
        extents = _scan_block_extents(block_lines, start)
        function_name = block.get("function_name") or ""
        if not function_name:
            for bl in block_lines:
                dm = _STEP_DEF.match(bl)
                if dm:
                    function_name = dm.group(2)
                    break
        def_line = start
        for offset, bl in enumerate(block_lines):
            if _STEP_DEF.match(bl):
                def_line = start + offset
                break
        blocks.append({
            **block,
            "function_name": function_name,
            "transformation_name": _guess_transformation_name(block_lines),
            "start_line": start,
            "end_line": end,
            "def_line": def_line,
            **extents,
        })
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
    step_type = getattr(step_result, "step_type", None) or block.get("step_type", "")
    status = getattr(step_result, "status", None) or block.get("generation_status", "")

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
            "line": block.get("def_line") or block["code_start_line"],
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
    status = (
        getattr(step_result, "status", None)
        or block.get("generation_status", "")
        or "converted"
    )
    errors_list = list(getattr(step_result, "errors", []) or [])
    warnings_list = list(getattr(step_result, "warnings", []) or [])
    score = int(round(float(getattr(step_result, "semantic_score", 0) or 0) * 100))
    has_placeholder = _step_has_placeholder(block_lines, errors_list)
    todo_count = _step_todo_count(status, errors_list)
    issues = _build_step_issues(step_result, block, code_lines) if step_result is not None else []
    highlight_level = _compute_highlight_level(
        status,
        score if step_result is not None else 100,
        len(errors_list),
        len(warnings_list),
        todo_count,
        has_placeholder,
    )
    step_type = (
        getattr(step_result, "step_type", None)
        or block.get("step_type", "")
        or ""
    )
    transformation_name = (
        getattr(step_result, "transformation_name", None)
        or block.get("transformation_name", "")
        or ""
    )
    function_name = (
        getattr(step_result, "function_name", None)
        or block.get("function_name", "")
        or ""
    )
    def_line = block.get("def_line") or block["code_start_line"]
    return {
        "step_name": block["step_name"],
        "step_type": step_type,
        "transformation_name": transformation_name,
        "function_name": function_name,
        "file": generated_file,
        "ktr_file": ktr_file,
        "start_line": start,
        "end_line": end,
        "header_line": block["header_line"],
        "code_start_line": def_line,
        "code_end_line": block["code_end_line"],
        "line": (issues[0]["line"] if issues else def_line),
        "status": status,
        "score": score if step_result is not None else 100,
        "warnings_count": len(warnings_list),
        "errors_count": len(errors_list),
        "todo_count": todo_count,
        "has_placeholder": has_placeholder,
        "highlight_level": highlight_level,
        "issues": issues,
    }


def _match_step_result(
    block: dict[str, Any],
    results_by_name: dict[str, list[Any]],
    results_by_func: dict[str, Any],
) -> Any | None:
    func = block.get("function_name") or ""
    if func and func in results_by_func:
        return results_by_func[func]
    candidates = results_by_name.get(block["step_name"], [])
    if not candidates:
        return None
    trans = block.get("transformation_name") or ""
    if trans:
        for result in candidates:
            if getattr(result, "transformation_name", "") == trans:
                return result
            detail = getattr(result, "detail", "") or ""
            if f"Transformation: {trans}" in detail:
                return result
    for result in candidates:
        rf = getattr(result, "function_name", "") or ""
        if rf and func and rf == func:
            return result
    return candidates[0]


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

    results_by_name: dict[str, list[Any]] = {}
    results_by_func: dict[str, Any] = {}
    for result in step_results or []:
        results_by_name.setdefault(result.step_name, []).append(result)
        func = getattr(result, "function_name", "") or ""
        if func:
            results_by_func[func] = result

    steps_by_name: dict[str, dict[str, Any]] = {}
    seen_pairs: set[tuple[str, str]] = set()

    for block in blocks:
        step_result = _match_step_result(block, results_by_name, results_by_func)
        step_name = block["step_name"]
        step_type = (
            getattr(step_result, "step_type", None) if step_result is not None else None
        ) or block.get("step_type") or ""

        if step_result is None and not block.get("function_name"):
            continue

        pair = (step_name, step_type)
        if pair in seen_pairs and step_name in steps_by_name:
            entry_existing = steps_by_name[step_name]
            func = block.get("function_name") or ""
            if func:
                steps_by_name[f"fn\x00{func}"] = entry_existing
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
        if step_type:
            steps_by_name[f"{step_name}\x00{step_type}"] = entry
        if entry.get("function_name"):
            steps_by_name[f"fn\x00{entry['function_name']}"] = entry
        if entry.get("transformation_name"):
            steps_by_name[f"{entry['transformation_name']}\x00{step_name}"] = entry
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


def _iter_python_sources(files: dict[str, str]) -> list[tuple[str, str]]:
    """Yield (path, content) for generated Python modules that may contain steps."""
    preferred: list[tuple[str, str]] = []
    others: list[tuple[str, str]] = []
    for path, content in files.items():
        if not path.endswith(".py"):
            continue
        norm = path.replace("\\", "/")
        if norm.endswith("__init__.py"):
            continue
        if "/engine/" in norm or norm.endswith("/config.py"):
            continue
        if "/jobs/" in norm:
            preferred.append((path, content))
        else:
            others.append((path, content))
    # Prefer transformation-like modules (tr_*) ahead of job orchestrators (jb_*).
    preferred.sort(key=lambda item: _navigation_file_rank(item[0], ""))
    others.sort(key=lambda item: _navigation_file_rank(item[0], ""))
    return preferred + others


def _basename_stem(path: str) -> str:
    name = path.replace("\\", "/").rsplit("/", 1)[-1]
    if name.lower().endswith(".py"):
        name = name[:-3]
    return name


def _is_orchestrator_module(path: str) -> bool:
    """True for master / prep-style job shells that should not own transformation steps."""
    stem = _basename_stem(path).lower()
    if stem in {"master_etl", "jb_master", "jb_prep", "main", "workflow"}:
        return True
    if stem.startswith("master_") or stem.endswith("_master"):
        return True
    return False


def _navigation_file_rank(path: str, transformation_name: str = "") -> int:
    """Lower is better when choosing which generated file owns a transformation step."""
    norm = (path or "").replace("\\", "/")
    stem = _basename_stem(norm).lower()
    trans_stem = ""
    if transformation_name:
        trans_stem = re.sub(r"[^a-z0-9_]+", "_", transformation_name.strip().lower()).strip("_")

    rank = 50
    if "/jobs/" in norm:
        rank = 20
    if trans_stem and (stem == trans_stem or stem.endswith("_" + trans_stem) or trans_stem in stem):
        rank = 0
    elif stem.startswith("tr_") or stem.startswith("transformation_"):
        rank = 5
    elif _is_orchestrator_module(norm):
        rank = 90
    elif stem.startswith("jb_"):
        rank = 40
    return rank


def _file_contains_function(content: str, function_name: str) -> bool:
    if not content or not function_name:
        return False
    pattern = re.compile(rf"^def {re.escape(function_name)}\s*\(", re.MULTILINE)
    return bool(pattern.search(content))


def _prefer_step_entry(
    existing: dict[str, Any] | None,
    candidate: dict[str, Any],
    *,
    files: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Choose the better navigation entry when the same step key appears in multiple modules."""
    if existing is None:
        return candidate

    exist_file = (existing.get("file") or "").replace("\\", "/")
    cand_file = (candidate.get("file") or "").replace("\\", "/")
    trans = (
        candidate.get("transformation_name")
        or existing.get("transformation_name")
        or ""
    )
    exist_fn = existing.get("function_name") or ""
    cand_fn = candidate.get("function_name") or ""

    exist_has_fn = bool(exist_fn)
    cand_has_fn = bool(cand_fn)
    if cand_has_fn and not exist_has_fn:
        return candidate
    if exist_has_fn and not cand_has_fn:
        return existing

    if files:
        exist_content = files.get(exist_file) or files.get(existing.get("file") or "") or ""
        cand_content = files.get(cand_file) or files.get(candidate.get("file") or "") or ""
        exist_live = _file_contains_function(exist_content, exist_fn) if exist_fn else False
        cand_live = _file_contains_function(cand_content, cand_fn) if cand_fn else False
        if cand_live and not exist_live:
            return candidate
        if exist_live and not cand_live:
            return existing

    exist_rank = _navigation_file_rank(exist_file, trans)
    cand_rank = _navigation_file_rank(cand_file, trans)
    if cand_rank < exist_rank:
        return candidate
    if exist_rank < cand_rank:
        return existing

    # Prefer the entry that already has precise line extents.
    exist_lines = bool(existing.get("code_start_line") or existing.get("start_line"))
    cand_lines = bool(candidate.get("code_start_line") or candidate.get("start_line"))
    if cand_lines and not exist_lines:
        return candidate
    return existing


def _search_files_for_step(
    files: dict[str, str],
    *,
    step_name: str,
    step_type: str = "",
    transformation_name: str = "",
    function_name: str = "",
) -> dict[str, Any] | None:
    """Locate ``def step_<...>`` for a validation row when index metadata is missing."""
    if not files or not (step_name or function_name):
        return None

    safe = re.sub(r"[^A-Za-z0-9_]+", "_", (step_name or "").strip()).strip("_") or "step"
    sources = _iter_python_sources(files)
    sources = sorted(
        sources,
        key=lambda item: _navigation_file_rank(item[0], transformation_name),
    )

    marker_re = re.compile(
        rf"^\s*# Step(?:\s+\d+\s*:|:)\s*{re.escape(step_name)}\b"
    ) if step_name else None
    legacy_re = re.compile(
        rf"^\s*# Step:\s*{re.escape(step_name)}\s*\("
        + (re.escape(step_type) if step_type else r"[^)]+")
        + r"\)"
    ) if step_name else None
    exact_def = (
        re.compile(rf"^def {re.escape(function_name)}\s*\(")
        if function_name
        else None
    )
    name_def = re.compile(rf"^def (step_\d+_{re.escape(safe)})\s*\(") if step_name else None

    def _scan(path: str, content: str) -> dict[str, Any] | None:
        lines = content.splitlines()
        for index, line in enumerate(lines):
            found_fn = ""
            start = 0
            if exact_def and exact_def.match(line):
                found_fn = function_name
                start = index + 1
            elif name_def:
                match = name_def.match(line)
                if match:
                    found_fn = match.group(1)
                    start = index + 1
            if not start and marker_re and marker_re.match(line):
                start = index + 1
                for j in range(index, min(index + 8, len(lines))):
                    dm = _STEP_DEF.match(lines[j])
                    if dm:
                        found_fn = dm.group(2)
                        start = j + 1
                        break
            if not start and legacy_re and legacy_re.match(line):
                start = index + 1
            if not start:
                continue
            end = len(lines)
            for k in range(start, len(lines)):
                stripped = lines[k]
                if (
                    _STEP_MARKER_LEGACY.match(stripped)
                    or _STEP_MARKER_NUMBERED.match(stripped)
                    or re.match(r"^def (?:step_\d+|run_)", stripped)
                ):
                    if k + 1 != start:
                        end = k
                        break
            while end > start and not lines[end - 1].strip():
                end -= 1
            return {
                "step_name": step_name,
                "step_type": step_type,
                "transformation_name": transformation_name,
                "function_name": found_fn or function_name,
                "file": path,
                "ktr_file": "",
                "start_line": start,
                "end_line": end,
                "header_line": start,
                "code_start_line": start,
                "code_end_line": end,
                "line": start,
                "status": "converted",
                "score": 100,
                "warnings_count": 0,
                "errors_count": 0,
                "todo_count": 0,
                "has_placeholder": False,
                "highlight_level": "success",
                "issues": [],
            }
        return None

    # Prefer non-orchestrator modules that actually own transformation step functions.
    for path, content in sources:
        if transformation_name and _is_orchestrator_module(path):
            continue
        found = _scan(path, content)
        if found:
            return found

    # Last resort only: orchestrators (jb_master / jb_prep / Master_ETL).
    for path, content in sources:
        if not _is_orchestrator_module(path):
            continue
        found = _scan(path, content)
        if found:
            return found
    return None


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


def build_project_code_navigation(
    files: dict[str, str],
    lineage: dict,
    inventory: list[dict],
    *,
    step_to_file: dict[str, str] | None = None,
    step_results: list[Any] | None = None,
    main_workflow: str = "",
) -> dict[str, Any]:
    """Index step navigation across all generated project Python modules.

    Scans each job module once (not on every UI click) and merges step metadata.
    Transformation step ownership prefers the module that actually defines
    ``def step_<...>`` (never Master_ETL / jb_master merely because they are primary).
    """
    sources = _iter_python_sources(files)
    if not sources and files:
        first = next(iter(files.items()))
        sources = [first]

    # Prefer a non-orchestrator jobs module as the outline/primary scan target.
    primary_path = ""
    for path, _content in sources:
        if not _is_orchestrator_module(path):
            primary_path = path
            break
    if not primary_path:
        primary_path = main_workflow or (sources[0][0] if sources else "")
    primary_code = files.get(primary_path, "") if primary_path else ""
    if not primary_code and sources:
        primary_path, primary_code = sources[0]

    nav = build_code_navigation(
        primary_code,
        lineage,
        inventory,
        step_to_file=step_to_file,
        step_results=step_results,
        generated_file=primary_path,
    )

    merged_steps: dict[str, dict[str, Any]] = {}
    # Re-index every source; do not seed from primary alone (avoids orchestrator wins).
    for path, content in sources:
        file_steps = _build_step_navigation(
            content,
            step_results=step_results,
            step_to_file=step_to_file,
            generated_file=path,
        )
        for key, entry in file_steps.items():
            merged_steps[key] = _prefer_step_entry(
                merged_steps.get(key),
                entry,
                files=files,
            )

    # Ensure every StepConversionResult can resolve via transformation\0step / fn keys.
    for result in step_results or []:
        entry = _lookup_step_entry(merged_steps, result)
        if entry is None:
            found = _search_files_for_step(
                files,
                step_name=getattr(result, "step_name", "") or "",
                step_type=getattr(result, "step_type", "") or "",
                transformation_name=getattr(result, "transformation_name", "") or "",
                function_name=getattr(result, "function_name", "") or "",
            )
            if found:
                entry = found
                _index_step_entry(merged_steps, entry)
        elif entry:
            # Keep compound keys fresh even when plain step_name pointed at a worse file.
            _index_step_entry(merged_steps, entry)

    sections_by_step: dict[str, dict[str, Any]] = {}
    for step_name, entry in merged_steps.items():
        if "\x00" in step_name:
            continue
        sections_by_step[step_name] = entry

    nav["steps_by_name"] = merged_steps
    nav["sections_by_step"] = sections_by_step
    nav["indexed_files"] = [path for path, _ in sources]
    nav["step_locations"] = navigation_report_payload(
        {"steps_by_name": merged_steps}
    )
    return nav


def _index_step_entry(steps_by_name: dict[str, dict[str, Any]], entry: dict[str, Any]) -> None:
    """Register an entry under plain, typed, function, and transformation keys."""
    step_name = entry.get("step_name") or ""
    step_type = entry.get("step_type") or ""
    function_name = entry.get("function_name") or ""
    transformation_name = entry.get("transformation_name") or ""

    if step_name:
        steps_by_name[step_name] = _prefer_step_entry(steps_by_name.get(step_name), entry)
        if step_type:
            typed = f"{step_name}\x00{step_type}"
            steps_by_name[typed] = _prefer_step_entry(steps_by_name.get(typed), entry)
    if function_name:
        fn_key = f"fn\x00{function_name}"
        steps_by_name[fn_key] = _prefer_step_entry(steps_by_name.get(fn_key), entry)
    if transformation_name and step_name:
        trans_key = f"{transformation_name}\x00{step_name}"
        steps_by_name[trans_key] = _prefer_step_entry(steps_by_name.get(trans_key), entry)


def _lookup_step_entry(
    steps: dict[str, dict[str, Any]],
    result: Any,
) -> dict[str, Any] | None:
    """Resolve navigation for a validation row — transformation scope first."""
    func = getattr(result, "function_name", "") or ""
    step_name = getattr(result, "step_name", "") or ""
    step_type = getattr(result, "step_type", "") or ""
    trans = getattr(result, "transformation_name", "") or ""

    candidates: list[dict[str, Any]] = []
    if func:
        entry = steps.get(f"fn\x00{func}")
        if entry:
            candidates.append(entry)
    if trans and step_name:
        entry = steps.get(f"{trans}\x00{step_name}")
        if entry:
            candidates.append(entry)
    if step_name and step_type:
        entry = steps.get(f"{step_name}\x00{step_type}")
        if entry:
            candidates.append(entry)
    if step_name:
        entry = steps.get(step_name)
        if entry:
            candidates.append(entry)

    best: dict[str, Any] | None = None
    for entry in candidates:
        best = _prefer_step_entry(best, entry)
    return best


def enrich_step_results_with_navigation(
    step_results: list[Any],
    code_navigation: dict[str, Any],
    *,
    files: dict[str, str] | None = None,
) -> None:
    """Copy resolved file/line/function metadata onto ``StepConversionResult`` objects.

    Every validation row should end with non-empty ``generated_file``,
    ``function_name``, ``start_line``, and ``end_line`` when the step exists in
    generated code. Never leave transformation steps pointed at jb_master/jb_prep
    when a better owning module is available.
    """
    steps = (code_navigation or {}).get("steps_by_name") or {}
    file_map = files or {}

    for result in step_results or []:
        entry = _lookup_step_entry(steps, result)
        if entry is None and file_map:
            entry = _search_files_for_step(
                file_map,
                step_name=result.step_name,
                step_type=result.step_type,
                transformation_name=getattr(result, "transformation_name", "") or "",
                function_name=getattr(result, "function_name", "") or "",
            )
            if entry:
                _index_step_entry(steps, entry)

        if not entry:
            continue

        # If metadata points at an orchestrator that does not define the function, re-search.
        func = entry.get("function_name") or getattr(result, "function_name", "") or ""
        entry_file = entry.get("file") or ""
        content = file_map.get(entry_file, "") if file_map else ""
        if (
            file_map
            and func
            and (
                _is_orchestrator_module(entry_file)
                or (content and not _file_contains_function(content, func))
            )
        ):
            repaired = _search_files_for_step(
                file_map,
                step_name=result.step_name,
                step_type=result.step_type,
                transformation_name=getattr(result, "transformation_name", "") or "",
                function_name=func,
            )
            if repaired:
                entry = repaired
                _index_step_entry(steps, entry)

        if not getattr(result, "function_name", ""):
            result.function_name = entry.get("function_name") or ""
        if not getattr(result, "transformation_name", ""):
            result.transformation_name = entry.get("transformation_name") or ""
        result.generated_file = entry.get("file") or result.generated_file or ""
        result.start_line = entry.get("code_start_line") or entry.get("start_line")
        result.end_line = entry.get("code_end_line") or entry.get("end_line")

    if code_navigation is not None:
        code_navigation["steps_by_name"] = steps
        code_navigation["step_locations"] = navigation_report_payload(
            {"steps_by_name": steps}
        )


def navigation_report_payload(code_navigation: dict[str, Any]) -> list[dict[str, Any]]:
    """Compact step-location list suitable for project report / JSON persistence."""
    steps = (code_navigation or {}).get("steps_by_name") or {}
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for key, entry in steps.items():
        if "\x00" in key:
            continue
        sig = (
            entry.get("transformation_name") or "",
            entry.get("step_name") or "",
            entry.get("function_name") or "",
        )
        if sig in seen:
            continue
        seen.add(sig)
        rows.append({
            "transformation_name": entry.get("transformation_name") or "",
            "step_name": entry.get("step_name") or "",
            "step_type": entry.get("step_type") or "",
            "generated_file": entry.get("file") or "",
            "function_name": entry.get("function_name") or "",
            "start_line": entry.get("code_start_line") or entry.get("start_line"),
            "end_line": entry.get("code_end_line") or entry.get("end_line"),
            "status": entry.get("status") or "",
        })
    rows.sort(key=lambda r: (r["generated_file"], r["start_line"] or 0, r["step_name"]))
    return rows

