"""Handlers for Pentaho Bulk Loading transformation steps.

Supports:
- Greenplum Load
- Infobright Loader
- Ingres VectorWise Bulk Loader
- MonetDB Bulk Loader
- MySQL Bulk Loader
- Oracle Bulk Loader
- PostgreSQL Bulk Loader
- Teradata FastLoad Bulk Loader
- Teradata TPT Bulk Loader
- Vertica Bulk Loader

All map to Delta ``saveAsTable`` with vendor-native options preserved as warnings.
"""

from __future__ import annotations

import logging

from ..bulk_loading_converter import convert_bulk_loader_step
from ..generation_config import GenerationConfig
from ..metadata_propagation import get_converter_metadata
from ..step_xml import get_step_element, parse_bulk_loader_config
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _norm(step_type: str) -> str:
    return (
        (step_type or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
        .replace("_", "")
        .replace("-", "")
    )


def _generation_config(context: StepContext) -> GenerationConfig:
    cfg = context.extra.get("generation_config")
    if isinstance(cfg, GenerationConfig):
        return cfg
    return GenerationConfig.defaults()


def _merge_parsed(context: StepContext) -> dict:
    metadata = dict(get_converter_metadata(context))
    step_el = get_step_element(context.step)
    if step_el is not None:
        parsed = parse_bulk_loader_config(step_el)
        for key, val in parsed.items():
            if key not in metadata or metadata[key] in (None, "", [], {}):
                metadata[key] = val
            elif key == "extras" and isinstance(val, dict):
                existing = metadata.get("extras") if isinstance(metadata.get("extras"), dict) else {}
                metadata["extras"] = {**val, **existing}
            elif isinstance(val, list) and not metadata.get(key):
                metadata[key] = val
    return metadata


def _passthrough_error(context: StepContext, label: str, exc: Exception) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [
        f"# {label}: {context.step.name}",
        f"# ERROR: {exc}",
        "# WARNING: Bulk loader conversion failed; continuing pipeline without write.",
    ]
    logger.exception("%s '%s' failed: %s", label, context.step.name, exc)
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append("from pyspark.sql.types import StructType")
        lines.append(f"{out_var} = spark.createDataFrame([], StructType([]))")
    return lines, "partial"


class _BulkLoaderHandler(BaseStepHandler):
    """Shared base for vendor bulk-loader handlers."""

    _TYPES: set[str] = set()
    _VENDOR = "Bulk"
    _NATIVE_LOADER = "vendor bulk loader"
    _LABEL = "Bulk Loader"

    def can_handle(self, step_type: str) -> bool:
        return _norm(step_type) in {_norm(t) for t in self._TYPES}

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        try:
            metadata = _merge_parsed(context)
            lines, status = convert_bulk_loader_step(
                metadata,
                context.input_df_name(),
                context.output_df_name(),
                context.step.name,
                vendor=self._VENDOR,
                native_loader=self._NATIVE_LOADER,
                generation_config=_generation_config(context),
            )
            logger.info(
                "%s '%s' table=%s status=%s",
                self._LABEL,
                context.step.name,
                metadata.get("table"),
                status,
            )
            return lines, status
        except Exception as exc:
            return _passthrough_error(context, self._LABEL, exc)


class GreenplumLoadHandler(_BulkLoaderHandler):
    """Greenplum Load → Delta saveAsTable (gpload/COPY not available)."""

    _TYPES = {
        "gpbulkloader",
        "greenplumbulkloader",
        "greenplumload",
        "greenplumloader",
        "gpload",
    }
    _VENDOR = "Greenplum"
    _NATIVE_LOADER = "gpload / Greenplum COPY"
    _LABEL = "Greenplum Load"


class InfobrightLoaderHandler(_BulkLoaderHandler):
    """Infobright Loader → Delta saveAsTable."""

    _TYPES = {
        "infobrightloader",
        "infobrightbulkloader",
        "infobright",
    }
    _VENDOR = "Infobright"
    _NATIVE_LOADER = "Infobright Loader (bhload)"
    _LABEL = "Infobright Loader"


class IngresVectorWiseBulkLoaderHandler(_BulkLoaderHandler):
    """Ingres VectorWise Bulk Loader → Delta saveAsTable."""

    _TYPES = {
        "vectorwisebulkloader",
        "ingresvectorwisebulkloader",
        "vwbulkloader",
        "ingresbulkloader",
        "vectorwiseloader",
    }
    _VENDOR = "Ingres VectorWise"
    _NATIVE_LOADER = "vwload / VectorWise bulk load"
    _LABEL = "Ingres VectorWise Bulk Loader"


class MonetDBBulkLoaderHandler(_BulkLoaderHandler):
    """MonetDB Bulk Loader → Delta saveAsTable."""

    _TYPES = {
        "monetdbbulkloader",
        "monetdbloader",
        "monetdbbulk",
    }
    _VENDOR = "MonetDB"
    _NATIVE_LOADER = "mclient COPY INTO"
    _LABEL = "MonetDB Bulk Loader"


class MySQLBulkLoaderHandler(_BulkLoaderHandler):
    """MySQL Bulk Loader → Delta saveAsTable (LOAD DATA LOCAL not available)."""

    _TYPES = {
        "mysqlbulkloader",
        "mysqlloader",
        "loaddatainfile",
    }
    _VENDOR = "MySQL"
    _NATIVE_LOADER = "LOAD DATA [LOCAL] INFILE / named pipe"
    _LABEL = "MySQL Bulk Loader"


class OracleBulkLoaderHandler(_BulkLoaderHandler):
    """Oracle Bulk Loader → Delta saveAsTable (sqlldr not available)."""

    _TYPES = {
        "orabulkloader",
        "oraclebulkloader",
        "oracleloader",
        "sqlldr",
    }
    _VENDOR = "Oracle"
    _NATIVE_LOADER = "Oracle SQL*Loader (sqlldr)"
    _LABEL = "Oracle Bulk Loader"


class PostgreSQLBulkLoaderHandler(_BulkLoaderHandler):
    """PostgreSQL Bulk Loader → Delta saveAsTable (psql COPY not available)."""

    _TYPES = {
        "pgbulkloader",
        "postgresqlbulkloader",
        "postgresbulkloader",
        "psqlbulkloader",
    }
    _VENDOR = "PostgreSQL"
    _NATIVE_LOADER = "psql COPY FROM STDIN"
    _LABEL = "PostgreSQL Bulk Loader"


class TeradataFastLoadBulkLoaderHandler(_BulkLoaderHandler):
    """Teradata FastLoad Bulk Loader → Delta saveAsTable."""

    _TYPES = {
        "terafast",
        "teradatafastloadbulkloader",
        "terafastbulkloader",
        "teradatafastload",
        "fastload",
    }
    _VENDOR = "Teradata FastLoad"
    _NATIVE_LOADER = "Teradata FastLoad"
    _LABEL = "Teradata FastLoad Bulk Loader"


class TeradataTPTBulkLoaderHandler(_BulkLoaderHandler):
    """Teradata TPT Bulk Loader → Delta saveAsTable."""

    _TYPES = {
        "teradatabulkloader",
        "teradatatptbulkloader",
        "tptbulkloader",
        "teratpt",
        "teradatatpt",
    }
    _VENDOR = "Teradata TPT"
    _NATIVE_LOADER = "Teradata Parallel Transporter (TPT)"
    _LABEL = "Teradata TPT Bulk Loader"


class VerticaBulkLoaderHandler(_BulkLoaderHandler):
    """Vertica Bulk Loader → Delta saveAsTable (VerticaCopyStream not available)."""

    _TYPES = {
        "verticabulkloader",
        "verticaloader",
        "verticacopy",
    }
    _VENDOR = "Vertica"
    _NATIVE_LOADER = "VerticaCopyStream / COPY"
    _LABEL = "Vertica Bulk Loader"


BULK_LOADING_HANDLERS: list[BaseStepHandler] = [
    GreenplumLoadHandler(),
    InfobrightLoaderHandler(),
    IngresVectorWiseBulkLoaderHandler(),
    MonetDBBulkLoaderHandler(),
    MySQLBulkLoaderHandler(),
    OracleBulkLoaderHandler(),
    PostgreSQLBulkLoaderHandler(),
    TeradataFastLoadBulkLoaderHandler(),
    TeradataTPTBulkLoaderHandler(),
    VerticaBulkLoaderHandler(),
]
