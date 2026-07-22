"""XML-category job entry helpers (well-formed, DTD/XSD validate, XSLT).

Uses ``xml.etree.ElementTree`` for well-formed checks and prefers ``lxml``
for DTD / XSD / XSL (documented dependency on Databricks when required).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from xml.etree import ElementTree as ET

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
class XmlOutcome:
    success: bool
    message: str = ""
    paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _import_lxml() -> Any:
    try:
        from lxml import etree  # type: ignore

        return etree
    except ImportError as exc:
        raise ImportError(
            "lxml is required for DTD/XSD/XSL job entries on Databricks — "
            "install cluster library 'lxml'"
        ) from exc


def compile_wildcard(pattern: str | None) -> re.Pattern[str]:
    text = (pattern or "").strip()
    if not text:
        return re.compile(r".*")
    try:
        return re.compile(text)
    except re.error:
        return re.compile(re.escape(text).replace(r"\*", ".*").replace(r"\?", "."))


def iter_xml_files(
    roots: Sequence[Mapping[str, str]],
    *,
    recursive: bool = False,
) -> list[Path]:
    found: list[Path] = []
    for row in roots:
        raw = str(row.get("source") or row.get("source_filefolder") or "").strip()
        if not raw:
            continue
        root = Path(raw)
        pattern = compile_wildcard(str(row.get("wildcard") or ""))
        if not root.exists():
            continue
        if root.is_file():
            if pattern.search(root.name):
                found.append(root)
            continue
        it = root.rglob("*") if recursive else root.iterdir()
        for fp in it:
            try:
                if fp.is_file() and pattern.search(fp.name):
                    found.append(fp)
            except OSError:
                continue
    return found


def check_well_formed(path: Path) -> tuple[bool, str]:
    try:
        ET.parse(path)
        return True, ""
    except ET.ParseError as exc:
        return False, str(exc)
    except OSError as exc:
        return False, str(exc)


def xml_well_formed(
    pairs: Sequence[Mapping[str, str]],
    *,
    recursive: bool = False,
    success_condition: str = "success_if_no_errors",
    nr_errors_less_than: str = "10",
    resultfilenames: str = "all_filenames",
    arg_from_previous_paths: Sequence[str] | None = None,
) -> XmlOutcome:
    warnings: list[str] = []
    files = list(iter_xml_files(pairs, recursive=recursive))
    if arg_from_previous_paths:
        for p in arg_from_previous_paths:
            fp = Path(p)
            if fp.exists() and fp.is_file():
                files.append(fp)

    if not files:
        err = FileNotFoundError("XML_WELL_FORMED: no input files matched")
        return XmlOutcome(False, str(err), error=err)

    well: list[str] = []
    bad: list[str] = []
    errors: list[str] = []
    for fp in files:
        ok, msg = check_well_formed(fp)
        if ok:
            well.append(str(fp))
        else:
            bad.append(str(fp))
            errors.append(f"{fp}: {msg}")

    cond = (success_condition or "success_if_no_errors").strip().lower()
    try:
        threshold = int(float(nr_errors_less_than or "10"))
    except ValueError:
        threshold = 10
        warnings.append(f"Invalid nr_errors_less_than — using {threshold}")

    if cond in {
        "success_if_at_least_x_files_well_formed",
        "success_when_at_least",
    }:
        ok = len(well) >= threshold
    elif cond in {
        "success_if_bad_formed_files_less",
        "success_when_errors_less",
    }:
        ok = len(bad) < threshold
    else:
        # success_if_no_errors (default)
        ok = not bad

    rf = (resultfilenames or "all_filenames").strip().lower()
    if rf in {"add_well_formed_files_only", "well_formed_files_only"}:
        result_paths = well
    elif rf in {"add_bad_formed_files_only", "bad_formed_files_only"}:
        result_paths = bad
    else:
        result_paths = well + bad

    return XmlOutcome(
        ok,
        f"well={len(well)} bad={len(bad)} condition={cond}",
        result_paths,
        errors,
        warnings,
        error=None if ok else ValueError("; ".join(errors) or "XML well-formed check failed"),
        extra={"well": well, "bad": bad},
    )


def dtd_validate(
    xml_filename: str,
    dtd_filename: str = "",
    *,
    dtd_intern: bool = False,
) -> XmlOutcome:
    warnings: list[str] = []
    xml_path = Path(xml_filename)
    if not xml_filename or not xml_path.exists():
        err = FileNotFoundError(f"XML file not found: {xml_filename}")
        return XmlOutcome(False, str(err), error=err)

    try:
        etree = _import_lxml()
    except ImportError as exc:
        return XmlOutcome(False, str(exc), error=exc, warnings=[str(exc)])

    try:
        parser = etree.XMLParser(dtd_validation=False, load_dtd=True, no_network=True)
        with xml_path.open("rb") as fh:
            doc = etree.parse(fh, parser)

        if dtd_intern:
            warnings.append("dtdintern=Y — validating using DTD declared in the XML document")
            dtd = doc.docinfo.internalDTD or doc.docinfo.externalDTD
            if dtd is None:
                err = ValueError("No internal/external DTD found in XML document")
                return XmlOutcome(False, str(err), [str(xml_path)], error=err, warnings=warnings)
            ok = dtd.validate(doc)
            errors = [str(e) for e in dtd.error_log] if not ok else []
        else:
            dtd_path = Path(dtd_filename)
            if not dtd_filename or not dtd_path.exists():
                err = FileNotFoundError(f"DTD file not found: {dtd_filename}")
                return XmlOutcome(False, str(err), error=err, warnings=warnings)
            with dtd_path.open("rb") as dfh:
                dtd = etree.DTD(dfh)
            ok = dtd.validate(doc)
            errors = [str(e) for e in dtd.error_log] if not ok else []
    except Exception as exc:  # noqa: BLE001
        return XmlOutcome(False, str(exc), [str(xml_path)], error=exc, warnings=warnings)

    return XmlOutcome(
        ok,
        "DTD validation passed" if ok else "DTD validation failed",
        [str(xml_path)],
        errors,
        warnings,
        error=None if ok else ValueError("; ".join(errors) or "DTD validation failed"),
    )


def xsd_validate(
    xml_filename: str,
    xsd_filename: str,
    *,
    allow_external_entities: bool = False,
) -> XmlOutcome:
    warnings: list[str] = []
    xml_path = Path(xml_filename)
    xsd_path = Path(xsd_filename)
    if not xml_filename or not xml_path.exists():
        err = FileNotFoundError(f"XML file not found: {xml_filename}")
        return XmlOutcome(False, str(err), error=err)
    if not xsd_filename or not xsd_path.exists():
        err = FileNotFoundError(f"XSD file not found: {xsd_filename}")
        return XmlOutcome(False, str(err), error=err)

    if allow_external_entities:
        warnings.append(
            "allowExternalEntities=Y — external entity loading remains restricted "
            "for security on Databricks"
        )

    try:
        etree = _import_lxml()
    except ImportError as exc:
        return XmlOutcome(False, str(exc), error=exc, warnings=[str(exc)])

    try:
        with xsd_path.open("rb") as sfh:
            schema_doc = etree.parse(sfh)
        schema = etree.XMLSchema(schema_doc)
        parser = etree.XMLParser(
            schema=None,
            resolve_entities=False,
            no_network=True,
        )
        with xml_path.open("rb") as xfh:
            doc = etree.parse(xfh, parser)
        ok = schema.validate(doc)
        errors = [str(e) for e in schema.error_log] if not ok else []
    except Exception as exc:  # noqa: BLE001
        return XmlOutcome(False, str(exc), [str(xml_path)], error=exc, warnings=warnings)

    return XmlOutcome(
        ok,
        "XSD validation passed" if ok else "XSD validation failed",
        [str(xml_path)],
        errors,
        warnings,
        error=None if ok else ValueError("; ".join(errors) or "XSD validation failed"),
    )


def _resolve_output_path(path: Path, iffileexists: str | int) -> tuple[Path | None, list[str]]:
    warnings: list[str] = []
    mode = str(iffileexists).strip()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        return path, warnings
    if mode in {"1", "do_nothing"}:
        warnings.append(f"Output exists — skipped (iffileexists={mode}): {path}")
        return None, warnings
    if mode in {"2", "fail"}:
        raise FileExistsError(f"Output file already exists: {path}")
    if mode in {"0", "unique", "create_new"}:
        stem, suffix = path.stem, path.suffix
        n = 1
        while True:
            candidate = path.with_name(f"{stem}_{n}{suffix}")
            if not candidate.exists():
                return candidate, warnings
            n += 1
    # overwrite / default
    return path, warnings


def xsl_transform(
    xml_filename: str,
    xsl_filename: str,
    output_filename: str,
    *,
    parameters: Sequence[Mapping[str, str]] | None = None,
    output_properties: Sequence[Mapping[str, str]] | None = None,
    iffileexists: str | int = 1,
    xsltfactory: str = "JAXP",
) -> XmlOutcome:
    warnings: list[str] = []
    factory = (xsltfactory or "JAXP").upper()
    if factory not in {"JAXP", ""}:
        warnings.append(
            f"xsltfactory={xsltfactory!r} is not available in Python — using lxml/libxslt"
        )

    xml_path = Path(xml_filename)
    xsl_path = Path(xsl_filename)
    if not xml_filename or not xml_path.exists():
        err = FileNotFoundError(f"XML file not found: {xml_filename}")
        return XmlOutcome(False, str(err), error=err)
    if not xsl_filename or not xsl_path.exists():
        err = FileNotFoundError(f"XSL file not found: {xsl_filename}")
        return XmlOutcome(False, str(err), error=err)
    if not output_filename:
        err = ValueError("XSLT outputfilename is empty")
        return XmlOutcome(False, str(err), error=err)

    try:
        out_path, w = _resolve_output_path(Path(output_filename), iffileexists)
        warnings.extend(w)
    except FileExistsError as exc:
        return XmlOutcome(False, str(exc), error=exc)
    if out_path is None:
        return XmlOutcome(True, "Skipped existing output", [], warnings=warnings)

    try:
        etree = _import_lxml()
    except ImportError as exc:
        return XmlOutcome(False, str(exc), error=exc, warnings=[str(exc)])

    try:
        with xml_path.open("rb") as xfh:
            xml_doc = etree.parse(xfh)
        with xsl_path.open("rb") as sfh:
            xsl_doc = etree.parse(sfh)
        transform = etree.XSLT(xsl_doc)

        kwargs: dict[str, Any] = {}
        for row in parameters or []:
            name = str(row.get("name") or "").strip()
            value = str(row.get("value") or row.get("field") or "")
            if name:
                kwargs[name] = etree.XSLT.strparam(value)

        result = transform(xml_doc, **kwargs) if kwargs else transform(xml_doc)

        # Output properties (subset)
        method = "xml"
        encoding = "UTF-8"
        pretty = False
        for prop in output_properties or []:
            pname = str(prop.get("name") or "").strip().lower()
            pval = str(prop.get("value") or "")
            if pname in {"method"}:
                method = pval or method
            elif pname in {"encoding"}:
                encoding = pval or encoding
            elif pname in {"indent"} and pval.lower() in {"yes", "true", "1"}:
                pretty = True
            else:
                warnings.append(f"outputproperty {pname!r}={pval!r} not fully applied")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        if method.lower() == "text":
            text = str(result)
            out_path.write_text(text, encoding=encoding)
        else:
            result.write(
                str(out_path),
                pretty_print=pretty,
                encoding=encoding,
                xml_declaration=True,
            )
    except Exception as exc:  # noqa: BLE001
        return XmlOutcome(False, str(exc), error=exc, warnings=warnings)

    return XmlOutcome(
        True,
        f"XSLT wrote {out_path}",
        [str(out_path)],
        warnings=warnings,
        extra={"output": str(out_path), "factory": factory},
    )
