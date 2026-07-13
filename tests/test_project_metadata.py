"""Tests for execution-faithful project lineage metadata."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

from pentaho_converter.models import ConversionStats
from pentaho_converter.job_parser import parse_job
from pentaho_converter.pipeline import convert_pentaho_project
from pentaho_converter.project_metadata import build_project_metadata
from pentaho_converter.scanner import scan_project
from pentaho_converter.transformation_parser import parse_transformation


def _sample_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for p in Path("samples").rglob("*"):
            if p.is_file() and p.suffix.lower() in (".kjb", ".ktr"):
                zf.write(p, p.relative_to("samples").as_posix())
    return buf.getvalue()


def test_lineage_follows_job_hop_execution_order(tmp_path):
  result = convert_pentaho_project(_sample_zip(), "samples")
  edges = result.lineage["edges"]

  assert result.lineage["root_jobs"] == ["Master.kjb"]
  assert edges[0] == {
      "from": "Master.kjb",
      "to": "Customer_Load.ktr",
      "via_entry": "Customer Load",
      "sequence": 0,
  }
  assert edges[1] == {
      "from": "Customer_Load.ktr",
      "to": "Sales_Load.ktr",
      "via_entry": "Sales Load",
      "sequence": 1,
  }


def test_root_job_is_not_nested_child():
    samples = Path("samples")
    scan = scan_project(samples, [])
    jobs = {}
    for kjb in scan.job_files:
        jobs[str(kjb)] = parse_job(kjb, [])
    trans = {}
    for ktr in scan.transformation_files:
        trans[str(ktr)] = parse_transformation(ktr, [])

    _, lineage = build_project_metadata(
        scan,
        jobs,
        trans,
        ConversionStats(),
        primary_job_name="Master",
    )
    assert lineage["root_jobs"] == ["Master.kjb"]
