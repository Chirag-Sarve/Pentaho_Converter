"""Handlers for Closure Generator, Get ID from Slave Server, and XSL Transformation."""

from __future__ import annotations

import logging
import re

from ..metadata_propagation import get_converter_metadata
from ..step_xml import (
    get_step_element,
    parse_closure_generator_config,
    parse_get_slave_sequence_config,
    parse_xslt_config,
)
from .base import BaseStepHandler, StepContext

logger = logging.getLogger(__name__)


def _passthrough(context: StepContext, label: str) -> tuple[list[str], str]:
    in_df = context.input_df_name()
    out_var = context.output_df_name()
    lines = [f"# {label}: {context.step.name}"]
    if in_df:
        lines.append(f"{out_var} = {in_df}")
    else:
        lines.append(f"{out_var} = spark.createDataFrame([], '_placeholder STRING')")
    return lines, "converted"


def _safe_ident(name: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", name or "step")
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned or "step"


class ClosureGeneratorHandler(BaseStepHandler):
    """Reflexive transitive closure table (Mondrian) via iterative parent walk."""

    _TYPES = {"closuregenerator", "closure"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        metadata = get_converter_metadata(context)
        cfg = (
            parse_closure_generator_config(step_el)
            if step_el is not None
            else {
                "parent_id_field": "",
                "child_id_field": "",
                "distance_field": "distance",
                "is_root_zero": False,
                "max_depth": 50,
            }
        )
        parent_f = metadata.get("parent_id_field") or cfg.get("parent_id_field") or ""
        child_f = metadata.get("child_id_field") or cfg.get("child_id_field") or ""
        dist_f = metadata.get("distance_field") or cfg.get("distance_field") or "distance"
        root_zero = bool(
            metadata.get("is_root_zero")
            if "is_root_zero" in metadata
            else cfg.get("is_root_zero")
        )
        max_depth = int(metadata.get("max_depth") or cfg.get("max_depth") or 50)

        lines = [f"# Closure Generator: {context.step.name}"]
        lines.append(
            f"# preserved.parent_id_field={parent_f!r} child_id_field={child_f!r} "
            f"distance_field={dist_f!r} is_root_zero={'Y' if root_zero else 'N'}"
        )

        if not in_df:
            return _passthrough(context, "Closure Generator")

        if not parent_f or not child_f:
            lines.append(
                "# WARNING: missing parent_id_field/child_id_field — emitting empty closure schema"
            )
            lines.append(
                f"{out_var} = spark.createDataFrame("
                f"[], '{parent_f or 'parent_id'} STRING, "
                f"{child_f or 'child_id'} STRING, {dist_f} LONG')"
            )
            return lines, "partial"

        # Map child→parent edges; seed reflexive rows; iteratively walk ancestors.
        # Matches Pentaho: output is (parent, child, distance) only; stop at null/root-zero;
        # cycles abort past max_depth (Pentaho hard-codes 50).
        lines.append(
            f"_cg_edges_{out_var} = {in_df}.select("
            f'col("{child_f}").alias("_cg_child"), '
            f'col("{parent_f}").alias("_cg_parent"))'
        )
        lines.append(
            f"_cg_seed_{out_var} = _cg_edges_{out_var}.select("
            f'col("_cg_child").alias("{parent_f}"), '
            f'col("_cg_child").alias("{child_f}"), '
            f'lit(0).cast("long").alias("{dist_f}"))'
        )
        lines.append(f"_cg_acc_{out_var} = _cg_seed_{out_var}")
        lines.append(f"_cg_cur_{out_var} = _cg_seed_{out_var}")
        if root_zero:
            lines.append(
                "# Root is zero: ancestors equal to 0 are treated as top-level and omitted"
            )
        lines.append(
            f"# Iterative ancestor expansion up to depth {max_depth} "
            "(circular hierarchies beyond this are dropped with a warning)"
        )
        lines.append(f"for _cg_d_{out_var} in range(1, {max_depth + 1}):")
        lines.append(
            f"    _cg_nxt_{out_var} = ("
            f"_cg_cur_{out_var}.alias('c')"
            f".join(_cg_edges_{out_var}.alias('e'), "
            f'col("c.{parent_f}") == col("e._cg_child"), "inner")'
            + (
                '.filter(col("e._cg_parent").isNotNull() & '
                '(col("e._cg_parent") != lit(0)))'
                if root_zero
                else '.filter(col("e._cg_parent").isNotNull())'
            )
            + f'.select('
            f'col("e._cg_parent").alias("{parent_f}"), '
            f'col("c.{child_f}").alias("{child_f}"), '
            f'lit(_cg_d_{out_var}).cast("long").alias("{dist_f}")))'
        )
        lines.append(f"    if not _cg_nxt_{out_var}.take(1):")
        lines.append("        break")
        lines.append(
            f"    _cg_acc_{out_var} = _cg_acc_{out_var}.unionByName(_cg_nxt_{out_var})"
        )
        lines.append(f"    _cg_cur_{out_var} = _cg_nxt_{out_var}")
        lines.append(
            f"{out_var} = _cg_acc_{out_var}.dropDuplicates("
            f'["{parent_f}", "{child_f}"])'
        )
        lines.append(
            "# WARNING: empty input yields empty closure; missing parents stop the walk "
            f"(null{' / 0' if root_zero else ''} treated as root)"
        )
        lines.append(
            "# WARNING: deep/cyclic hierarchies stop at "
            f"max_depth={max_depth} without raising (unlike Pentaho which throws)"
        )
        logger.info(
            "ClosureGenerator '%s': parent=%s child=%s distance=%s root_zero=%s",
            context.step.name,
            parent_f,
            child_f,
            dist_f,
            root_zero,
        )
        return lines, "converted"


class GetSlaveSequenceHandler(BaseStepHandler):
    """Get ID from Slave Server — Carte sequence has no Databricks equivalent."""

    _TYPES = {
        "getslavesequence",
        "getidfromslaveserver",
        "getidfromslave",
    }

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        metadata = get_converter_metadata(context)
        cfg = (
            parse_get_slave_sequence_config(step_el)
            if step_el is not None
            else {
                "value_name": "id",
                "slave_server": "",
                "sequence_name": "",
                "increment": 10000,
                "increment_raw": "10000",
            }
        )
        value_name = metadata.get("value_name") or cfg.get("value_name") or "id"
        slave = metadata.get("slave_server") or cfg.get("slave_server") or ""
        seq_name = metadata.get("sequence_name") or cfg.get("sequence_name") or ""
        increment = metadata.get("increment")
        if increment is None:
            increment = cfg.get("increment", 10000)
        incr_raw = metadata.get("increment_raw") or cfg.get("increment_raw") or str(increment)

        lines = [f"# Get ID from Slave Server: {context.step.name}"]
        lines.append(
            "# UNSUPPORTED: Carte slave-server sequences have no Databricks equivalent"
        )
        lines.append(
            f"# preserved.valuename={value_name!r} slave={slave!r} "
            f"seqname={seq_name!r} increment={incr_raw!r}"
        )
        if not slave:
            lines.append(
                "# WARNING: missing slave server configuration — metadata preserved only"
            )
        if not seq_name:
            lines.append(
                "# WARNING: missing sequence name — metadata preserved only"
            )

        if not in_df:
            lines.append(
                f"{out_var} = spark.createDataFrame([], '{value_name} LONG')"
            )
            lines.append(
                "# WARNING: empty input stream — no IDs generated"
            )
            return lines, "partial"

        # Local spark counter approximates uniqueness within one job run only.
        lines.append(
            "# WARNING: emitting local Spark row_number IDs as an approximation; "
            "values are NOT reserved via /kettle/nextSequence and are not cluster-federated"
        )
        lines.append(
            f"_w_slave_{out_var} = Window.orderBy(monotonically_increasing_id())"
        )
        lines.append(
            f'{out_var} = {in_df}.withColumn('
            f'"{value_name}", '
            f"row_number().over(_w_slave_{out_var}).cast('long'))"
        )
        lines.append(
            f"# preserved.increment={increment} used only for documentation "
            "(local counter steps by 1, not by reserved range size)"
        )
        logger.warning(
            "GetSlaveSequence '%s': no Carte equivalent; local row_number for %s "
            "(slave=%r seq=%r)",
            context.step.name,
            value_name,
            slave,
            seq_name,
        )
        return lines, "partial"


class XsltHandler(BaseStepHandler):
    """XSL Transformation via Python lxml XSLT applied through a Spark UDF."""

    _TYPES = {"xslt", "xsltransformation", "xsltransform"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        metadata = get_converter_metadata(context)
        cfg = parse_xslt_config(step_el) if step_el is not None else {}
        xsl_filename = metadata.get("xsl_filename") or cfg.get("xsl_filename") or ""
        xml_field = metadata.get("field_name") or cfg.get("field_name") or ""
        result_field = metadata.get("result_field") or cfg.get("result_field") or "result"
        xsl_file_field = metadata.get("xsl_file_field") or cfg.get("xsl_file_field") or ""
        use_xsl_field = bool(
            metadata.get("xsl_file_field_use")
            if "xsl_file_field_use" in metadata
            else cfg.get("xsl_file_field_use")
        )
        xsl_is_file = bool(
            metadata.get("xsl_field_is_a_file")
            if "xsl_field_is_a_file" in metadata
            else cfg.get("xsl_field_is_a_file", True)
        )
        xsl_factory = metadata.get("xsl_factory") or cfg.get("xsl_factory") or "JAXP"
        parameters = metadata.get("parameters") or cfg.get("parameters") or []
        output_props = (
            metadata.get("output_properties") or cfg.get("output_properties") or []
        )

        lines = [f"# XSL Transformation: {context.step.name}"]
        lines.append(
            f"# preserved.xslfilename={xsl_filename!r} fieldname={xml_field!r} "
            f"resultfieldname={result_field!r} xslfactory={xsl_factory!r}"
        )
        lines.append(
            f"# preserved.xslfilefielduse={'Y' if use_xsl_field else 'N'} "
            f"xslfilefield={xsl_file_field!r} "
            f"xslfieldisafile={'Y' if xsl_is_file else 'N'}"
        )
        if parameters:
            lines.append(f"# preserved.parameters={parameters!r}")
        if output_props:
            lines.append(f"# preserved.outputproperties={output_props!r}")
            lines.append(
                "# WARNING: only indent/method output properties are applied via "
                "etree.tostring; other JAXP output keys remain documented only"
            )

        if not in_df:
            return _passthrough(context, "XSL Transformation")

        if not xml_field:
            lines.append(
                "# WARNING: missing XML fieldname — passthrough with null result column"
            )
            lines.append(
                f'{out_var} = {in_df}.withColumn("{result_field}", lit(None).cast("string"))'
            )
            return lines, "partial"

        if use_xsl_field and not xsl_file_field:
            lines.append(
                "# WARNING: xslfilefielduse=Y but xslfilefield is empty — "
                "passthrough with null result"
            )
            lines.append(
                f'{out_var} = {in_df}.withColumn("{result_field}", lit(None).cast("string"))'
            )
            return lines, "partial"

        if not use_xsl_field and not xsl_filename:
            lines.append(
                "# WARNING: invalid/missing XSL path — passthrough with null result column"
            )
            lines.append(
                f'{out_var} = {in_df}.withColumn("{result_field}", lit(None).cast("string"))'
            )
            return lines, "partial"

        if str(xsl_factory).upper() == "SAXON":
            lines.append(
                "# WARNING: SAXON factory is not available on Databricks; "
                "using libxslt/lxml (JAXP-equivalent) instead"
            )

        fn = _safe_ident(out_var)
        param_fields = [
            p.get("field") for p in parameters if isinstance(p, dict) and p.get("field")
        ]
        param_names = [
            p.get("name") for p in parameters if isinstance(p, dict) and p.get("name")
        ]

        lines.append("from pyspark.sql.functions import udf")
        lines.append("from pyspark.sql.types import StringType")
        lines.append(f"def _xslt_apply_{fn}(xml_text, xsl_src=None, *param_vals):")
        lines.append("    if xml_text is None:")
        lines.append("        return None")
        lines.append("    try:")
        lines.append("        from lxml import etree")
        lines.append("    except ImportError as exc:")
        lines.append(
            "        raise ImportError("
            "'lxml is required for XSL Transformation migration') from exc"
        )
        lines.append("    try:")
        lines.append("        xml_doc = etree.fromstring("
                      "xml_text.encode('utf-8') if isinstance(xml_text, str) "
                      "else xml_text)")
        if use_xsl_field:
            if xsl_is_file:
                lines.append("        if not xsl_src:")
                lines.append("            return None")
                lines.append("        xsl_doc = etree.parse(str(xsl_src))")
            else:
                lines.append("        if xsl_src is None:")
                lines.append("            return None")
                lines.append(
                    "        xsl_doc = etree.fromstring("
                    "xsl_src.encode('utf-8') if isinstance(xsl_src, str) else xsl_src)"
                )
        else:
            lines.append(f"        xsl_doc = etree.parse({xsl_filename!r})")
        lines.append("        transform = etree.XSLT(xsl_doc)")
        if param_names:
            lines.append(f"        _param_names = {param_names!r}")
            lines.append("        _kwargs = {}")
            lines.append("        for _i, _n in enumerate(_param_names):")
            lines.append("            if _i < len(param_vals) and param_vals[_i] is not None:")
            lines.append("                _kwargs[_n] = etree.XSLT.strparam(str(param_vals[_i]))")
            lines.append("        result_tree = transform(xml_doc, **_kwargs)")
        else:
            lines.append("        result_tree = transform(xml_doc)")
        if output_props:
            lines.append(f"        _out_props = {{(p.get('name') or '').lower(): p.get('value') for p in {output_props!r}}}")
            lines.append("        _pretty = str(_out_props.get('indent', '')).lower() in ('yes', 'true', '1')")
            lines.append("        _method = (_out_props.get('{http://xml.apache.org/xslt}method') or _out_props.get('method') or 'xml')")
            lines.append("        try:")
            lines.append("            return etree.tostring(result_tree, pretty_print=_pretty, method=str(_method), encoding='unicode')")
            lines.append("        except Exception:")
            lines.append("            return str(result_tree)")
        else:
            lines.append("        return str(result_tree)")
        lines.append("    except Exception as exc:")
        lines.append(
            "        # Null XML / bad XSL / transform errors → null result (do not fail job)"
        )
        lines.append("        return None")

        lines.append(f"_xslt_udf_{fn} = udf(_xslt_apply_{fn}, StringType())")

        if use_xsl_field:
            call_args = [f'col("{xml_field}")', f'col("{xsl_file_field}")']
        else:
            call_args = [f'col("{xml_field}")', "lit(None)"]
        for pf in param_fields:
            call_args.append(f'col("{pf}")')
        lines.append(
            f'{out_var} = {in_df}.withColumn('
            f'"{result_field}", _xslt_udf_{fn}({", ".join(call_args)}))'
        )
        lines.append(
            "# WARNING: requires cluster library 'lxml'; null XML / invalid XSL "
            "paths yield null results without failing the job"
        )
        if parameters and not param_fields:
            lines.append(
                "# WARNING: XSLT parameters listed without field bindings were ignored"
            )
        logger.info(
            "XSLT '%s': xml_field=%s result=%s use_field=%s factory=%s",
            context.step.name,
            xml_field,
            result_field,
            use_xsl_field,
            xsl_factory,
        )
        return lines, "partial" if (
            parameters or use_xsl_field or output_props or str(xsl_factory).upper() == "SAXON"
        ) else "converted"


class SplunkIOHandler(BaseStepHandler):
    """Splunk Input/Output — listed under Transform in PDI; no native Databricks connector."""

    _TYPES = {"splunkinput", "splunkoutput", "splunk"}

    def can_handle(self, step_type: str) -> bool:
        return step_type.strip().lower().replace(" ", "") in self._TYPES

    def generate_code(self, context: StepContext) -> tuple[list[str], str]:
        in_df = context.input_df_name()
        out_var = context.output_df_name()
        step_el = get_step_element(context.step)
        st = context.step.step_type.strip().lower().replace(" ", "")
        is_input = "input" in st or st == "splunk"
        label = "Splunk Input" if is_input else "Splunk Output"
        attrs: dict[str, str] = {}
        if step_el is not None:
            for tag in (
                "host", "port", "username", "password", "query", "index",
                "sourcetype", "source", "connection", "filename", "app",
            ):
                val = step_el.findtext(tag)
                if val:
                    attrs[tag] = val
        lines = [f"# {label}: {context.step.name}"]
        lines.append(
            "# UNSUPPORTED: Splunk has no built-in Databricks/Spark connector in this migrator"
        )
        for k, v in attrs.items():
            safe = "***" if k == "password" else v
            lines.append(f"# preserved.{k}={safe!r}")
        if not attrs:
            lines.append("# WARNING: no Splunk connection/query metadata found in step XML")
        if is_input:
            lines.append(
                f"{out_var} = spark.createDataFrame([], '_splunk_placeholder STRING')"
            )
            lines.append(
                "# WARNING: replace with Spark Splunk connector / REST extract as needed"
            )
        else:
            if in_df:
                lines.append(f"{out_var} = {in_df}")
            else:
                lines.append(
                    f"{out_var} = spark.createDataFrame([], '_splunk_placeholder STRING')"
                )
            lines.append(
                "# WARNING: Splunk write skipped — implement via REST/HEC or partner connector"
            )
        logger.warning("Splunk step '%s' (%s): metadata preserved only", context.step.name, st)
        return lines, "partial"


SPECIAL_TRANSFORM_HANDLERS = [
    ClosureGeneratorHandler(),
    GetSlaveSequenceHandler(),
    XsltHandler(),
    SplunkIOHandler(),
]
