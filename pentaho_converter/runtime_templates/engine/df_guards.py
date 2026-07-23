"""DataFrame validation and step diagnostics for generated transformations."""

from __future__ import annotations

import logging
import traceback
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)


def dataframe_schema_text(df: Any) -> str:
    """Return a compact schema description for logging."""
    if df is None:
        return "<None>"
    try:
        schema = getattr(df, "schema", None)
        if schema is not None and hasattr(schema, "simpleString"):
            return schema.simpleString()
    except Exception as exc:  # noqa: BLE001
        return f"<schema unavailable: {exc}>"
    try:
        columns = list(getattr(df, "columns", []) or [])
        return ", ".join(columns) if columns else "<no columns>"
    except Exception as exc:  # noqa: BLE001
        return f"<columns unavailable: {exc}>"


def dataframe_row_count(df: Any) -> int | str | None:
    """Best-effort row count; returns a string reason when counting fails."""
    if df is None:
        return None
    try:
        return int(df.count())
    except Exception as exc:  # noqa: BLE001
        return f"<unavailable: {exc}>"


def describe_dataframe(df: Any) -> dict[str, Any]:
    """Collect schema / columns / row count for diagnostics."""
    if df is None:
        return {"df": None, "columns": [], "schema": "<None>", "row_count": None}
    try:
        columns = list(getattr(df, "columns", []) or [])
    except Exception:  # noqa: BLE001
        columns = []
    return {
        "df": type(df).__name__,
        "columns": columns,
        "schema": dataframe_schema_text(df),
        "row_count": dataframe_row_count(df),
    }


def log_step_dataframe(
    df: Any,
    *,
    step_name: str,
    phase: str,
    transformation: str | None = None,
    func_name: str | None = None,
) -> None:
    """Log step name, schema, and row count before/after a generated step.

    Emitted at DEBUG so production (LOG_LEVEL=INFO) stays quiet. Skips the
    expensive ``df.count()`` when DEBUG is disabled.
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return
    info = describe_dataframe(df)
    logger.debug(
        "STEP %s | transformation=%s | step=%s | func=%s | rows=%s | schema=%s | columns=%s",
        phase.upper(),
        transformation or "?",
        step_name,
        func_name or "?",
        info.get("row_count"),
        info.get("schema"),
        info.get("columns"),
    )


def require_dataframe(
    df: Any,
    *,
    transformation: str,
    step_name: str,
    func_name: str,
    required_columns: Sequence[str] | None = None,
) -> Any:
    """Validate a DataFrame before a step runs; raise ValueError with context."""
    if df is None:
        raise ValueError(
            f"DataFrame is None before {step_name} step "
            f"(transformation={transformation!r}, function={func_name})"
        )
    try:
        columns = set(getattr(df, "columns", []) or [])
    except Exception as exc:  # noqa: BLE001
        raise ValueError(
            f"Cannot read columns before {step_name} step "
            f"(transformation={transformation!r}, function={func_name}): {exc}"
        ) from exc

    missing = [c for c in (required_columns or ()) if c and c not in columns]
    if missing:
        raise ValueError(
            f"Column {missing[0]} missing before {step_name} step "
            f"(missing={missing}, available={sorted(columns)}, "
            f"transformation={transformation!r}, function={func_name})"
        )
    return df


def _iter_traceback_frames(exc: BaseException):
    tb = exc.__traceback__
    while tb is not None:
        yield tb.tb_frame
        tb = tb.tb_next


def _looks_like_dataframe(value: Any) -> bool:
    return value is not None and hasattr(value, "schema") and hasattr(value, "columns")


def collect_failure_context(exc: BaseException) -> dict[str, Any]:
    """Extract transformation/step/function/DataFrame context from an exception."""
    context: dict[str, Any] = {
        "transformation": None,
        "step_name": None,
        "func_name": None,
        "dataframes": [],
    }
    for frame in _iter_traceback_frames(exc):
        fname = frame.f_code.co_name
        locals_map = frame.f_locals
        if fname.startswith("run_tr_") or fname.startswith("run_"):
            context["transformation"] = context["transformation"] or fname
            for key in ("TRANSFORMATION_NAME", "transformation", "trans_name"):
                if key in locals_map and locals_map[key]:
                    context["transformation"] = str(locals_map[key])
        if fname.startswith("step_"):
            context["func_name"] = fname
            # step_04_Write_Active → Write Active
            parts = fname.split("_", 2)
            if len(parts) >= 3:
                context["step_name"] = parts[2].replace("_", " ")
            for key in ("step_name", "STEP_NAME"):
                if key in locals_map and locals_map[key]:
                    context["step_name"] = str(locals_map[key])
        for name, value in locals_map.items():
            if not _looks_like_dataframe(value):
                continue
            if any(d.get("name") == name for d in context["dataframes"]):
                continue
            context["dataframes"].append(
                {"name": name, **describe_dataframe(value)}
            )
    return context


def log_exception_diagnostics(
    *,
    entry_name: str,
    exc: BaseException,
    kind: str = "TRANS",
) -> None:
    """Print full traceback plus transformation/step/DataFrame diagnostics."""
    context = collect_failure_context(exc)
    logger.error(
        "%s FAIL | entry=%s | transformation=%s | step=%s | function=%s | error=%s: %s",
        kind,
        entry_name,
        context.get("transformation") or "?",
        context.get("step_name") or "?",
        context.get("func_name") or "?",
        type(exc).__name__,
        exc,
    )
    logger.error("Full traceback:\n%s", "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
    for df_info in context.get("dataframes") or []:
        logger.error(
            "Input/local DataFrame | name=%s | rows=%s | schema=%s | columns=%s",
            df_info.get("name"),
            df_info.get("row_count"),
            df_info.get("schema"),
            df_info.get("columns"),
        )
    if not context.get("dataframes"):
        logger.error(
            "No DataFrame locals found in traceback frames for entry=%s",
            entry_name,
        )


def required_columns_expr(columns: Iterable[str]) -> str:
    """Render a Python list literal for generated code."""
    cols = [c for c in columns if c]
    return "[" + ", ".join(repr(c) for c in cols) + "]"
