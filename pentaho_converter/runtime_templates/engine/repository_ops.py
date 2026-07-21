"""Repository-category job entry helpers.

Pentaho repositories do not exist on Databricks. These helpers preserve
configuration, emit clear warnings/TODOs, and approximate success/failure
semantics when local metadata or explicit overrides are available.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from xml.dom import minidom

logger = logging.getLogger(__name__)


def yn_true(raw: Any, default: bool = False) -> bool:
    if raw is None or raw == "":
        return default
    return str(raw).strip().upper() in {"Y", "YES", "TRUE", "1", "T"}


def attr(attrs: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return str(attrs[key])
    return default


def attr_yn(attrs: Mapping[str, Any], *keys: str, default: bool = False) -> bool:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return yn_true(attrs[key], default)
    return default


def iter_warning_logs(prefix: str, warnings: Iterable[str]) -> None:
    for warning in warnings:
        logger.warning("%s | %s", prefix, warning)


@dataclass
class RepoOutcome:
    success: bool
    message: str = ""
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    paths: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


def _yn_flag(raw: Any) -> bool:
    return yn_true(raw, default=False)


def check_connected_to_repository(
    *,
    isspecificrep: bool = False,
    repname: str = "",
    isspecificuser: bool = False,
    username: str = "",
    repository_meta: Mapping[str, Any] | None = None,
    connected_override: str = "",
) -> RepoOutcome:
    """Approximate PDI ``CONNECTED_TO_REPOSITORY``.

    PDI fails when no repository session exists. On Databricks there is no
    Pentaho repository API — fail by default unless:

    * ``connected_override`` is Y/true (explicit dry-run assertion), or
    * ``repository_meta`` provides a synthetic connected session
      (``connected=Y``, optional ``name`` / ``username``).
    """
    warnings: list[str] = [
        "CONNECTED_TO_REPOSITORY: Pentaho repository sessions do not exist on "
        "Databricks — configuration preserved; no Repository API is called"
    ]
    config = {
        "isspecificrep": isspecificrep,
        "repname": repname,
        "isspecificuser": isspecificuser,
        "username": username,
    }
    logger.info(
        "CONNECTED_TO_REPOSITORY | preserved config: isspecificrep=%s repname=%r "
        "isspecificuser=%s username=%r",
        isspecificrep,
        repname,
        isspecificuser,
        username,
    )

    override = str(connected_override or "").strip().upper()
    if override in {"Y", "YES", "TRUE", "1"}:
        warnings.append(
            "REPOSITORY_CONNECTED override is set — treating as connected "
            "(dry-run / migration escape hatch)"
        )
        # Still validate specific name/user against override meta if provided
        meta = dict(repository_meta or {})
        if isspecificrep:
            if not repname:
                err = ValueError("isspecificrep=Y but repname is empty")
                return RepoOutcome(False, str(err), warnings, error=err, extra=config)
            expected = str(meta.get("name") or repname)
            if meta.get("name") and str(meta["name"]) != repname:
                err = ValueError(
                    f"Connected repository {meta['name']!r} != required {repname!r}"
                )
                return RepoOutcome(False, str(err), warnings, error=err, extra=config)
            _ = expected
        if isspecificuser:
            if not username:
                err = ValueError("isspecificuser=Y but username is empty")
                return RepoOutcome(False, str(err), warnings, error=err, extra=config)
            if meta.get("username") and str(meta["username"]) != username:
                err = ValueError(
                    f"Connected user {meta['username']!r} != required {username!r}"
                )
                return RepoOutcome(False, str(err), warnings, error=err, extra=config)
        return RepoOutcome(
            True,
            "Repository connected (override)",
            warnings,
            extra={**config, "mode": "override"},
        )

    meta = dict(repository_meta or {})
    connected = _yn_flag(meta.get("connected")) or bool(meta.get("name"))
    if not connected:
        err = RuntimeError(
            "Not connected to a Pentaho repository (unsupported on Databricks). "
            "Export jobs from Spoon beforehand, or set REPOSITORY_CONNECTED=Y "
            "to assert connectivity for migration dry-runs."
        )
        return RepoOutcome(False, str(err), warnings, error=err, extra=config)

    warnings.append(
        "Using runtime.repository metadata as a synthetic repository session"
    )
    if isspecificrep:
        if not repname:
            err = ValueError("isspecificrep=Y but repname is empty")
            return RepoOutcome(False, str(err), warnings, error=err, extra=config)
        actual = str(meta.get("name") or "")
        if actual != repname:
            err = ValueError(
                f"Connected repository {actual!r} != required {repname!r}"
            )
            return RepoOutcome(False, str(err), warnings, error=err, extra=config)
    if isspecificuser:
        if not username:
            err = ValueError("isspecificuser=Y but username is empty")
            return RepoOutcome(False, str(err), warnings, error=err, extra=config)
        actual_user = str(meta.get("username") or meta.get("login") or "")
        if actual_user != username:
            err = ValueError(
                f"Connected user {actual_user!r} != required {username!r}"
            )
            return RepoOutcome(False, str(err), warnings, error=err, extra=config)

    return RepoOutcome(
        True,
        f"Synthetic repository connected: {meta.get('name', '')}",
        warnings,
        extra={**config, "mode": "synthetic", "repository": dict(meta)},
    )


def _append_datetime(
    path: Path,
    *,
    add_date: bool,
    add_time: bool,
    specify_format: bool,
    date_time_format: str,
) -> Path:
    if not (add_date or add_time or specify_format):
        return path
    now = datetime.now()
    if specify_format and date_time_format:
        try:
            stamp = now.strftime(date_time_format)
        except ValueError:
            stamp = now.strftime("%Y%m%d_%H%M%S")
    else:
        parts: list[str] = []
        if add_date:
            parts.append(now.strftime("%Y%m%d"))
        if add_time:
            parts.append(now.strftime("%H%M%S"))
        stamp = "_".join(parts) if parts else now.strftime("%Y%m%d_%H%M%S")
    return path.with_name(f"{path.stem}_{stamp}{path.suffix}")


def _resolve_target(
    path: Path,
    iffileexists: str,
) -> tuple[Path | None, list[str]]:
    warnings: list[str] = []
    mode = str(iffileexists).strip().lower()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        return path, warnings
    # PDI: 0=overwrite, 1=unique, 2=fail, 3=do nothing (common patterns)
    if mode in {"2", "fail"}:
        raise FileExistsError(f"Export target already exists: {path}")
    if mode in {"3", "do_nothing", "donothing"}:
        warnings.append(f"Target exists — skipped (iffileexists={mode}): {path}")
        return None, warnings
    if mode in {"1", "unique", "create_new"}:
        stem, suffix = path.stem, path.suffix
        n = 1
        while True:
            candidate = path.with_name(f"{stem}_{n}{suffix}")
            if not candidate.exists():
                return candidate, warnings
            n += 1
    # overwrite / 0 / default
    return path, warnings


def _collect_local_objects(root: Path, export_type: str) -> list[Path]:
    if not root.exists():
        return []
    etype = (export_type or "Export_All").strip()
    patterns: list[str]
    if etype in {"Export_Jobs", "jobs"}:
        patterns = ["*.kjb", "*.KJB"]
    elif etype in {"Export_Trans", "transformations", "trans"}:
        patterns = ["*.ktr", "*.KTR"]
    else:
        # Export_All / Export_By_Folder / Export_One_Folder
        patterns = ["*.kjb", "*.KJB", "*.ktr", "*.KTR"]

    found: list[Path] = []
    if root.is_file():
        return [root]
    for pat in patterns:
        found.extend(root.rglob(pat))
    # Deduplicate
    uniq: list[Path] = []
    seen: set[str] = set()
    for fp in found:
        key = str(fp.resolve()) if fp.exists() else str(fp)
        if key not in seen:
            seen.add(key)
            uniq.append(fp)
    return uniq


def _write_export_xml(
    target: Path,
    *,
    objects: Sequence[Path],
    config: Mapping[str, Any],
    stub: bool = False,
) -> None:
    root = ET.Element("repository_export")
    root.set("generator", "pentaho_converter")
    root.set("note", "Approximation of Pentaho Repository XML export for Databricks")

    cfg_el = ET.SubElement(root, "preserved_configuration")
    for key, value in config.items():
        child = ET.SubElement(cfg_el, str(key))
        child.text = "" if value is None else str(value)

    todo = ET.SubElement(root, "todo")
    todo.text = (
        "Pentaho Repository export is not available at Databricks runtime. "
        "Prefer exporting from Spoon (Export repository to XML) and stage the "
        "file, or provide a local directoryPath of .kjb/.ktr files."
    )

    objs = ET.SubElement(root, "objects")
    if stub and not objects:
        ET.SubElement(objs, "object", {"type": "stub", "path": ""})
    for fp in objects:
        kind = "job" if fp.suffix.lower() == ".kjb" else (
            "transformation" if fp.suffix.lower() == ".ktr" else "file"
        )
        el = ET.SubElement(objs, "object", {"type": kind, "path": str(fp)})
        try:
            # Embed filename only — full file copy would duplicate large trees
            el.set("name", fp.name)
            el.set("size", str(fp.stat().st_size))
        except OSError:
            pass

    rough = ET.tostring(root, encoding="utf-8")
    try:
        pretty = minidom.parseString(rough).toprettyxml(indent="  ", encoding="utf-8")
        target.write_bytes(pretty)
    except Exception:  # noqa: BLE001
        target.write_bytes(b'<?xml version="1.0" encoding="UTF-8"?>\n' + rough)


def export_repository_to_xml(
    *,
    repositoryname: str = "",
    username: str = "",
    password: str = "",
    targetfilename: str = "",
    iffileexists: str = "0",
    export_type: str = "Export_All",
    directory_path: str = "",
    add_date: bool = False,
    add_time: bool = False,
    specify_format: bool = False,
    date_time_format: str = "",
    createfolder: bool = True,
    newfolder: bool = False,
    add_result_filesname: bool = False,
    nr_errors_less_than: str = "10",
    success_condition: str = "",
    allow_stub: bool = False,
    local_object_roots: Sequence[str] | None = None,
) -> RepoOutcome:
    """Approximate PDI ``EXPORT_REPOSITORY`` without a live repository."""
    warnings: list[str] = [
        "EXPORT_REPOSITORY: Pentaho Repository API is unavailable on Databricks — "
        "TODO: export from Spoon and stage XML, or provide local .kjb/.ktr paths"
    ]
    if password and not str(password).startswith("${"):
        warnings.append(
            "repository password present in job XML — prefer ${VAR} secrets; "
            "value is not used at Databricks runtime"
        )
    if newfolder:
        warnings.append("newfolder=Y has no repository folder semantics here — ignored")

    config = {
        "repositoryname": repositoryname,
        "username": username,
        "password": "***" if password else "",
        "targetfilename": targetfilename,
        "iffileexists": iffileexists,
        "export_type": export_type,
        "directoryPath": directory_path,
        "add_date": "Y" if add_date else "N",
        "add_time": "Y" if add_time else "N",
        "SpecifyFormat": "Y" if specify_format else "N",
        "date_time_format": date_time_format,
        "createfolder": "Y" if createfolder else "N",
        "newfolder": "Y" if newfolder else "N",
        "add_result_filesname": "Y" if add_result_filesname else "N",
        "nr_errors_less_than": nr_errors_less_than,
        "success_condition": success_condition,
    }
    logger.info("EXPORT_REPOSITORY | preserved config: %s", config)

    if not targetfilename:
        err = ValueError("EXPORT_REPOSITORY targetfilename is empty")
        return RepoOutcome(False, str(err), warnings, error=err, extra=config)

    target = Path(targetfilename)
    target = _append_datetime(
        target,
        add_date=add_date,
        add_time=add_time,
        specify_format=specify_format,
        date_time_format=date_time_format,
    )
    if createfolder:
        target.parent.mkdir(parents=True, exist_ok=True)

    try:
        out_path, w = _resolve_target(target, iffileexists)
        warnings.extend(w)
    except FileExistsError as exc:
        return RepoOutcome(False, str(exc), warnings, error=exc, extra=config)
    if out_path is None:
        return RepoOutcome(
            True, "Skipped existing export target", warnings, extra=config
        )

    roots: list[Path] = []
    if directory_path:
        roots.append(Path(directory_path))
    for raw in local_object_roots or []:
        if raw:
            roots.append(Path(raw))

    objects: list[Path] = []
    for root in roots:
        objects.extend(_collect_local_objects(root, export_type))

    if not objects and not allow_stub:
        err = RuntimeError(
            "No local repository objects found to serialize. "
            "Set directoryPath to a folder of .kjb/.ktr files, or set "
            "EXPORT_REPOSITORY_ALLOW_STUB=Y to write a TODO stub XML, "
            "or manually export from Pentaho Repository before the job runs."
        )
        return RepoOutcome(False, str(err), warnings, error=err, extra=config)

    if not objects and allow_stub:
        warnings.append(
            "EXPORT_REPOSITORY_ALLOW_STUB=Y — writing stub XML with preserved config"
        )

    try:
        _write_export_xml(
            out_path, objects=objects, config=config, stub=allow_stub and not objects
        )
    except OSError as exc:
        return RepoOutcome(False, str(exc), warnings, error=exc, extra=config)

    # success_condition / nr_errors_less_than are PDI export-error thresholds;
    # with local serialization we treat write success as overall success.
    if success_condition:
        warnings.append(
            f"success_condition={success_condition!r} / nr_errors_less_than="
            f"{nr_errors_less_than!r} not applied to local serialization"
        )

    return RepoOutcome(
        True,
        f"Wrote repository export XML ({len(objects)} object(s)) → {out_path}",
        warnings,
        paths=[str(out_path)],
        extra={
            **config,
            "object_count": len(objects),
            "objects": [str(p) for p in objects],
            "target": str(out_path),
        },
    )
