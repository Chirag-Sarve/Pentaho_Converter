#!/usr/bin/env python3
"""CLI entry point -- convert Pentaho project ZIP to PySpark code.

Usage:
    python converter.py project.zip output_dir/ [--log-level DEBUG|INFO|WARNING|ERROR]
    python converter.py serve [--host HOST] [--port PORT]   # web UI (Flask)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pentaho_converter.pipeline import convert_pentaho_project, package_files_as_zip


def _setup_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _run_serve(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="converter.py serve",
        description="Start the web UI for Pentaho project ZIP upload and conversion.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1).")
    parser.add_argument("--port", type=int, default=5000, help="Port (default: 5000).")
    parser.add_argument("--no-debug", action="store_true", help="Disable Flask debug mode.")
    args = parser.parse_args(argv)

    from app import app as flask_app

    flask_app.run(debug=not args.no_debug, host=args.host, port=args.port)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert Pentaho PDI project ZIP archives into PySpark code.",
    )
    parser.add_argument("input_zip", type=str, help="Path to the Pentaho project ZIP file.")
    parser.add_argument("output_dir", type=str, help="Directory for generated PySpark files.")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    argv_list = list(sys.argv[1:] if argv is None else argv)
    if argv_list and argv_list[0] == "serve":
        return _run_serve(argv_list[1:])

    args = _build_parser().parse_args(argv_list)
    _setup_logging(args.log_level)
    logger = logging.getLogger("converter")

    input_path = Path(args.input_zip)
    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        return 1

    if input_path.suffix.lower() != ".zip":
        logger.warning("Input file does not have .zip extension: %s", input_path)

    try:
        result = convert_pentaho_project(input_path.read_bytes(), input_path.stem)
    except Exception as exc:
        logger.error("Conversion failed: %s", exc)
        return 2

    for line in result.logs:
        logger.info(line)

    if not result.files:
        logger.error("No files generated.")
        return 2

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for rel_path, content in result.files.items():
        target = out_dir / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    zip_path = out_dir / f"{input_path.stem}_pyspark.zip"
    zip_path.write_bytes(package_files_as_zip(result.files))

    _print_summary(
        result.stats,
        out_dir,
        zip_path,
        files_generated=len(result.files),
    )
    return 0


def _print_summary(stats, out_dir: Path, zip_path: Path, *, files_generated: int = 0) -> None:
    print("\n" + "=" * 60)
    print("  Pentaho -> Databricks Project Conversion Summary")
    print("=" * 60)
    print(f"  Jobs found             : {stats.jobs_found}")
    print(f"  Transformations found  : {stats.transformations_found}")
    print(f"  Steps converted        : {stats.steps_converted}")
    print(f"  Steps approximated     : {stats.steps_approximated}")
    print(f"  Steps skipped          : {stats.steps_skipped}")
    print(f"  Files generated        : {files_generated}")
    if stats.warnings:
        print(f"  Warnings               : {len(stats.warnings)}")
        for w in stats.warnings:
            print(f"    - {w}")
    print(f"  Output directory       : {out_dir}")
    print(f"  ZIP package            : {zip_path}")
    print("  Run on Databricks      : Master_ETL.py")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    sys.exit(main())
