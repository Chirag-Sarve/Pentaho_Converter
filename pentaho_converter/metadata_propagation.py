"""Propagate parsed XML metadata to converters and validate column lineage."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from .metadata_models import (
    ColumnLineage,
    ColumnSchema,
    LineageValidationResult,
    StepMetadataBundle,
)
from .models import PentahoField, PentahoStep, PentahoTransformation
from .step_context import StepContext

# Keys from ``parsed_config`` that must reach converter ``parsed`` dicts.
_STRUCTURED_PROPAGATION_KEYS = frozenset({
    "calculations",
    "join_type",
    "step1",
    "step2",
    "keys",
    "keys_1",
    "keys_2",
    "group_keys",
    "aggregates",
    "fields",
    "field_to_use",
    "target_field",
    "mappings",
    "non_match_default",
    "compare_value",
    "condition",
    "send_true_to",
    "send_false_to",
    "limit",
    "columns",
    "rows",
    "field_name",
    "start_at",
    "increment_by",
    "max_value",
    "filename",
    "extension",
    "separator",
    "header",
    "footer",
    "encoding",
    "compression",
    "enclosure",
    "append",
    "file",
    "output_fields",
    "connection",
    "sql",
    "schema",
    "table",
    "parameters",
    "variables_active",
    "execute_each_row",
    "limit",
    "keys",
    "return_fields",
    "cached",
    "cache_size",
    "orderby",
    "fail_on_multiple",
    "eat_row_on_failure",
    "constants",
    "select_fields",
    "remove_names",
    "meta_changes",
    "select_unspecified",
    "usevar",
    "set_fields",
    "target_field_name",
    "target_field_length",
    "remove_selected_fields",
    "enclosure_forced",
    "value_name",
    "root_node",
    "omit_xml_header",
    "omit_null_values",
    "operations",
    # Excel Output / Writer
    "sheetname",
    "nullisblank",
    "autosizecolums",
    "autosizecolumns",
    "protect_sheet",
    "password_set",
    "template_enabled",
    "template_append",
    "template_filename",
    "usetempfiles",
    "tempdirectory",
    "splitevery",
    "add_date",
    "add_time",
    "custom",
    "starting_cell",
    "streaming",
    "if_file_exists",
    "add_to_result_filenames",
    "do_not_open_newfile_init",
    # SAP Input (Deprecated)
    "sap_connection",
    "function",
    "function_name",
    "module",
    "client",
    "system",
    "language",
    "filters",
    "pagination",
    "batch_size",
    "page_size",
    "row_skips",
    # Reshape / sort / unique
    "sort_fields",
    "directory",
    "prefix",
    "sort_size",
    "free_memory",
    "compress",
    "compress_variable",
    "unique_rows",
    "compare_fields",
    "count_rows",
    "count_field",
    "reject_duplicate_row",
    "error_description",
    "store_values",
    "type_field",
    "key_field",
    "group_fields",
    "target_fields",
    "split_field",
    "delimiter",
    "new_field",
    "include_row_number",
    "row_number_field",
    "reset_row_number",
    "delimiter_is_regex",
    # Calc / checksum / range / sequence
    "calculations",
    "checksum_type",
    "result_field",
    "result_type",
    "compatibility_mode",
    "input_field",
    "output_field",
    "fallback_value",
    "rules",
    "start_at",
    "increment_by",
    "max_value",
    "use_counter",
    "use_database",
    "sequence_name",
    # Closure / slave sequence / XSLT
    "parent_id_field",
    "child_id_field",
    "distance_field",
    "is_root_zero",
    "max_depth",
    "value_name",
    "slave_server",
    "increment",
    "increment_raw",
    "xsl_filename",
    "result_field",
    "xsl_file_field",
    "xsl_file_field_use",
    "xsl_field_is_a_file",
    "xsl_factory",
    "output_properties",
    # Streaming
    "bootstrap_servers",
    "topics",
    "topic",
    "consumer_group",
    "starting_offsets",
    "starting_offsets_raw",
    "connection_type",
    "cluster_name",
    "batch_size",
    "batch_duration",
    "auto_commit",
    "sub_transformation",
    "checkpoint_location",
    "options",
    "security",
    "key_field",
    "message_field",
    "source_step",
    "destination",
    "destination_type",
    "connection_factory",
    "url",
    "username",
    "password",
    "receive_timeout",
    "message_selector",
    "client_id",
    "durable",
    "transacted",
    "acknowledge_mode",
    "delivery_mode",
    "priority",
    "time_to_live",
    "persistent",
    "broker_url",
    "qos",
    "clean_session",
    "keep_alive",
    "ssl",
    "retained",
    "acks",
    "compression",
    # Utility steps
    "nr_clones",
    "nr_clones_raw",
    "add_clone_flag",
    "clone_flag_field",
    "nr_clone_in_field",
    "nr_clone_field",
    "add_clone_num",
    "clone_num_field",
    "replacements",
    "value_types",
    "replace_all",
    "replace_all_mask",
    "set_empty_string_all",
    "select_values_type",
    "timeout",
    "timeout_raw",
    "scale_time",
    "source_file",
    "target_file",
    "source_encoding",
    "target_encoding",
    "source_file_field",
    "target_file_field",
    "create_parent_folder",
    "add_source_result",
    "add_target_result",
    "output_rowcount",
    "rowcount_field",
    "position_field",
    "fieldname_field",
    "comments_field",
    "length_field",
    "precision_field",
    "origin_field",
    "log_level",
    "display_header",
    "limit_rows",
    "limit_rows_number",
    "log_message",
    "log_subject",
    "reference_connection",
    "compare_connection",
    "reference_schema",
    "compare_schema",
    "reference_table",
    "compare_table",
    "key_fields",
    "exclude_fields",
    "nr_errors_field",
    "nr_records_reference_field",
    "nr_records_compare_field",
    "zip_filename",
    "zip_filename_field",
    "source_filename_field",
    "target_filename_field",
    "move_to_folder",
    "overwrite_zip_entry",
    "keep_source_folder",
    "add_filename_result",
    "include_subfolders",
    "after_zip",
    "operation",
    "overwrite_target",
    "add_result_filenames",
    "simulate",
    "process_field",
    "executable",
    "arguments",
    "argument_fields",
    "error_field",
    "exit_value_field",
    "fail_when_nonzero",
    "output_delimited",
    "output_delimiter",
    "server",
    "port",
    "use_private_key",
    "key_file",
    "passphrase",
    "command",
    "command_field",
    "use_command_field",
    "stdout_field",
    "stderr_field",
    "proxy_host",
    "proxy_port",
    "proxy_username",
    "destination_cc",
    "destination_bcc",
    "reply_to",
    "reply_address",
    "subject",
    "comment",
    "include_date",
    "only_comment",
    "use_html",
    "include_files",
    "zip_files",
    "zip_filename",
    "use_authentication",
    "auth_user",
    "auth_password",
    "use_secure_auth",
    "secure_connection_type",
    "contact_person",
    "contact_phone",
    "attached_filenames",
    "attach_content_field",
    "attach_filename_field",
    "facility",
    "message_field",
    "add_timestamp",
    "date_pattern",
    "add_hostname",
    # Flow steps
    "row_threshold",
    "row_threshold_raw",
    "message",
    "always_log_rows",
    "abort_option",
    "head_name",
    "tail_name",
    "stream_order",
    "wait_steps",
    "steps",
    "pass_all_rows",
    "compress_files",
    "switch_field",
    "fieldname",
    "default_target_step",
    "use_contains",
    "case_value_type",
    "case_value_format",
    "case_value_decimal",
    "case_value_group",
    "cases",
    "stream_priority",
    "streams",
    "specification_method",
    "trans_name",
    "job_name",
    "directory_path",
    "no_execution",
    "stream_source_step",
    "stream_target_step",
    "group_size",
    "group_field",
    "group_time",
    "result_rows_target_step",
    "result_files_target_step",
    "execution_result_target_step",
    "inherit_all_variables",
    "inject_step",
    "retrieve_step",
    "batch_time",
    "pass_parameters",
    # Mapping (sub-transformation) category
    "trans_object_id",
    "input_mappings",
    "output_mappings",
    "input_mapping",
    "output_mapping",
    "allow_multiple_input",
    "allow_multiple_output",
    "select_unspecified",
    "include_unspecified_fields",
    "field_names",
    "output_columns",
    "renames",
    # Validation steps
    "resultfieldname",
    "cardtype",
    "onlydigits",
    "notvalidmsg",
    "validations",
    "validate_all",
    "concat_errors",
    "concat_separator",
    "error_target_step",
    "emailfield",
    "result_as_string",
    "smtp_check",
    "email_valid_msg",
    "email_not_valid_msg",
    "errors_field_name",
    "timeout",
    "default_smtp",
    "email_sender",
    "default_smtp_field",
    "dynamic_default_smtp",
    "xmlstream",
    "xmlsourcefile",
    "xsdfilename",
    "xsdsource",
    "xsddefinedfield",
    "outputstringfield",
    "ifxmlvalid",
    "ifxmlinvalid",
    "addvalidationmsg",
    "validationmsgfield",
    "allow_external_entities",
    # Cryptography steps
    "gpg_location",
    "key_name",
    "keyname_in_field",
    "keyname_field",
    "stream_field",
    "result_field",
    "keyring_path",
    "public_key",
    "ascii_armor",
    "integrity_check",
    "passphrase_configured",
    "passphrase_secret_ref",
    "passphrase_from_field",
    "passphrase_field",
    "private_key",
    "keys",
    "secret_key_field",
    "secret_key_length_field",
    "algorithm_field",
    "output_key_in_binary",
    "encoding",
    "operation_type",
    "algorithm",
    "schema",
    "scheme",
    "cipher_mode",
    "padding",
    "message_field",
    "secret_key_configured",
    "secret_key_secret_ref",
    "secret_key_in_field",
    "read_key_as_binary",
    "output_result_as_binary",
    "iv_configured",
    "iv_secret_ref",
    "iv_field",
    "algorithm_normalized",
    # Experimental — SFTP Put
    "host",
    "authentication_method",
    "password_configured",
    "password_secret_ref",
    "private_key_ref",
    "passphrase_configured",
    "passphrase_secret_ref",
    "local_filename",
    "local_directory",
    "local_filename_field",
    "remote_filename",
    "remote_filename_field",
    "remote_directory",
    "remote_directory_field",
    "create_remote_directory",
    "overwrite",
    "append",
    "transfer_mode",
    "proxy_type",
    "proxy_password_configured",
    "proxy_password_secret_ref",
    "variable_substitution",
    "input_is_stream",
    "add_filename_to_result",
    "after_sftp_put",
    "destination_folder",
    "destination_folder_field",
    "create_destination_folder",
    "wildcard",
    "copy_previous",
    "copy_previous_files",
    "success_when_no_file",
    "use_old_ssh_algorithms",
    "logging_level",
    "private_key_secret_ref",
    # Pentaho Server
    "server_url",
    "module_name",
    "module_from_field",
    "endpoint_path",
    "http_method",
    "endpoint_from_field",
    "status_code_field",
    "response_time_field",
    "use_session_authentication",
    "is_bypassing_authentication",
    "request_parameters",
    "request_body",
    "content_type",
    "verify_ssl",
    "ssl_settings",
    "retries",
    "retry_delay",
    "session_variables",
    "variable_names",
    "variable_inheritance",
    "overwrite",
    "use_formatting",
    "scope",
    # Experimental — Script
    "script_language",
    "script_engine",
    "scripts",
    "optimization_level",
    "compatible",
    # Bulk Loading steps
    "database",
    "db_name_override",
    "truncate",
    "load_method",
    "load_action",
    "bulk_load_mode",
    "commit_size",
    "batch_size",
    "buffer_size",
    "bind_size",
    "read_size",
    "delimiter",
    "enclosure",
    "escape_char",
    "null_string",
    "charset",
    "file_format",
    "fifo_file",
    "data_file",
    "control_file",
    "log_file",
    "bad_file",
    "discard_file",
    "error_file",
    "load_file",
    "psql_path",
    "sqlldr_path",
    "gpload_path",
    "mclient_path",
    "vwload_path",
    "fastload_path",
    "tpt_path",
    "tbuild_path",
    "tpt_operator",
    "tpt_job_name",
    "agent_host",
    "agent_port",
    "stop_on_error",
    "max_errors",
    "reject_errors",
    "reject_limit",
    "ignore_errors",
    "erase_files",
    "direct",
    "parallel",
    "parallel_degree",
    "local_infile",
    "replace_data",
    "ignore_duplicates",
    "continue_on_error",
    "transaction_size",
    "stream_name",
    "copy_statement",
    "table_space",
    "error_table",
    "work_table",
    "log_table",
    "sessions",
    "max_sessions",
    "min_sessions",
    "pack_factor",
    "fill_record",
    "explicit_dates",
    "date_mask",
    "date_format",
    "time_format",
    "timestamp_format",
    "key_fields",
    "vendor",
    "native_loader",
    "extras",
})


def _normalize_step_type(step_type: str) -> str:
    return (
        (step_type or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("(", "")
        .replace(")", "")
    )


def _fields_to_dicts(fields: list[PentahoField]) -> list[dict[str, str]]:
    return [
        {
            "name": f.name,
            "type": f.type_name,
            "length": f.length,
            "precision": f.precision,
            "format": f.format,
            "rename": f.rename,
            "default": f.default,
        }
        for f in fields
        if f.name
    ]


def _schemas_from_names(
    names: set[str],
    type_map: dict[str, str] | None = None,
    *,
    source_step: str = "",
) -> dict[str, ColumnSchema]:
    type_map = type_map or {}
    return {
        name: ColumnSchema(
            name=name,
            type_name=type_map.get(name, "String"),
            source_step=source_step,
        )
        for name in names
        if name
    }


def _resolve_parsed_config(step: PentahoStep) -> dict[str, Any]:
    """Return structured parser output, falling back to on-demand step_xml parse."""
    if step.parsed_config:
        return deepcopy(step.parsed_config)
    if step.raw_element is not None:
        from .step_xml import parse_step_metadata

        parsed = parse_step_metadata(step.raw_element, step.step_type)
        if parsed:
            return deepcopy(parsed)
    return {}


def build_step_metadata_bundle(step: PentahoStep) -> StepMetadataBundle:
    """Capture parser output before registry propagation."""
    return StepMetadataBundle(
        step_name=step.name,
        step_type=step.step_type,
        attributes=dict(step.attributes),
        parsed_config=_resolve_parsed_config(step),
    )


def _enrich_converter_metadata(
    step: PentahoStep,
    transformation: PentahoTransformation,
    trace: list[str],
) -> dict[str, Any]:
    """Merge parsed_config into the dict converters receive via parse_xml."""
    meta: dict[str, Any] = {
        "step_type": step.step_type,
        "step_name": step.name,
        "attributes": dict(step.attributes),
        "fields": _fields_to_dicts(step.fields),
        "transformation_parameters": dict(transformation.parameters),
    }

    parsed_config = _resolve_parsed_config(step)

    if parsed_config:
        for key, value in parsed_config.items():
            meta[key] = deepcopy(value)
            trace.append(f"parsed_config.{key}")
    else:
        trace.append("parsed_config:empty")

    st = _normalize_step_type(step.step_type)

    # Aliases expected by existing converter/validator contracts.
    if st in (
        "mergejoin", "joinrows", "joiner", "streamlookup", "databaselookup", "dblookup",
        "dbjoin", "databasejoin", "mergerows", "mergerow", "fuzzymatch",
        "multimergejoin", "multiwaymergejoin", "multimerge",
        "dimensionlookup", "dimensionlookupupdate", "combinationlookup",
    ):
        keys = meta.get("keys") or []
        if keys and "join_keys" not in meta:
            normalized = []
            for pair in keys:
                if isinstance(pair, dict):
                    if pair.get("left") or pair.get("right"):
                        normalized.append({
                            "left": pair.get("left", ""),
                            "right": pair.get("right", pair.get("left", "")),
                        })
                    elif pair.get("stream_field") or pair.get("table_field"):
                        normalized.append({
                            "left": pair.get("stream_field", ""),
                            "right": pair.get("table_field", pair.get("stream_field", "")),
                        })
            meta["join_keys"] = normalized or keys
            trace.append("alias.join_keys")
        if st in ("mergerows", "mergerow") and meta.get("key_fields") and not meta.get("join_keys"):
            meta["join_keys"] = [
                {"left": k, "right": k} for k in meta["key_fields"] if k
            ]
            trace.append("alias.join_keys_from_key_fields")
        if st == "joinrows" and meta.get("condition") and "filter_condition" not in meta:
            meta["filter_condition"] = meta["condition"]
            trace.append("alias.filter_condition")

    if st in ("groupby", "memorygroupby"):
        aggregates = meta.get("aggregates") or []
        if aggregates and "aggregate_fields" not in meta:
            meta["aggregate_fields"] = aggregates
            trace.append("alias.aggregate_fields")

    if st == "analyticquery":
        if meta.get("group_fields") and "partition_fields" not in meta:
            meta["partition_fields"] = meta["group_fields"]
            trace.append("alias.partition_fields")
        if meta.get("analytic_fields") and "fields" not in meta:
            meta["fields"] = meta["analytic_fields"]
            trace.append("alias.analytic_fields")

    if st in ("samplerows", "reservoirsampling"):
        trace.append("derived.sampling_config")

    if st in ("univariatestats", "univariatestatistics"):
        if meta.get("stats"):
            trace.append("derived.univariate_stats")

    if st in ("stepsmetrics", "outputstepsmetrics"):
        if meta.get("metric_steps"):
            trace.append("derived.metric_steps")

    if st == "valuemapper":
        mappings = meta.get("mappings") or []
        if mappings and "value_mappings" not in meta:
            meta["value_mappings"] = mappings
            trace.append("alias.value_mappings")

    if st == "filterrows":
        if meta.get("condition") and "filter_condition" not in meta:
            meta["filter_condition"] = meta["condition"]
            trace.append("alias.filter_condition")

    if st == "selectvalues":
        field_dicts = meta.get("fields") or meta.get("select_fields") or _fields_to_dicts(step.fields)
        select_cols = [f["name"] for f in field_dicts if f.get("name")]
        output_cols = [f.get("rename") or f["name"] for f in field_dicts if f.get("name")]
        if select_cols:
            meta["select_columns"] = select_cols
            trace.append("derived.select_columns")
        if output_cols:
            meta["output_columns"] = output_cols
            trace.append("derived.output_columns")
        if meta.get("meta_changes"):
            trace.append("derived.meta_changes")
        if meta.get("remove_names"):
            trace.append("derived.remove_names")

    if st == "constant":
        if meta.get("constants"):
            trace.append("derived.constants")

    if st in ("setvalueconstant", "setvaluefield"):
        if meta.get("fields") and "set_fields" not in meta:
            meta["set_fields"] = meta["fields"]
            trace.append("alias.set_fields")

    if st == "concatfields":
        if meta.get("target_field_name"):
            trace.append("derived.target_field_name")

    if st == "addxml":
        if meta.get("value_name"):
            trace.append("derived.value_name")

    if st in ("replaceinstring", "stringoperations", "stringcut"):
        if meta.get("operations"):
            trace.append("derived.operations")

    if st in ("rowgenerator", "datagrid"):
        field_dicts = meta.get("fields") or []
        if field_dicts and "row_field_types" not in meta:
            meta["row_field_types"] = {
                f["name"]: f.get("type") or f.get("type_name", "String")
                for f in field_dicts
                if f.get("name")
            }
            trace.append("derived.row_field_types")

    if st in ("textfileoutput", "textfileoutputlegacy"):
        if meta.get("filename") and "file_path" not in meta:
            meta["file_path"] = meta["filename"]
            trace.append("alias.file_path")

    if st == "tableinput":
        if meta.get("sql") and transformation.parameters:
            from .lineage import substitute_pentaho_variables

            resolved = substitute_pentaho_variables(meta["sql"], transformation.parameters)
            if resolved != meta["sql"]:
                meta["sql_resolved"] = resolved
                trace.append("derived.sql_resolved")

    propagated = sorted(k for k in meta if k in _STRUCTURED_PROPAGATION_KEYS or k.startswith(("alias.", "derived.")))
    meta["_propagated_keys"] = sorted(
        k for k in meta if k in _STRUCTURED_PROPAGATION_KEYS or k in (
            "join_keys", "aggregate_fields", "value_mappings", "filter_condition",
            "select_columns", "output_columns", "row_field_types", "file_path", "sql_resolved",
        )
    )
    meta["_propagation_trace"] = trace
    return meta


def propagate_step_metadata(context: StepContext) -> StepMetadataBundle:
    """Populate context.extra with complete metadata for converters."""
    step = context.step
    trace: list[str] = ["parser.parsed_config", "parser.attributes", "parser.fields"]

    bundle = build_step_metadata_bundle(step)
    converter_metadata = _enrich_converter_metadata(step, context.transformation, trace)

    bundle.converter_metadata = converter_metadata
    bundle.propagation_trace = trace

    context.extra["metadata_bundle"] = bundle
    context.extra["converter_metadata"] = converter_metadata
    context.extra["parsed_config"] = _resolve_parsed_config(step)
    return bundle


def get_converter_metadata(context: StepContext) -> dict[str, Any]:
    """Return metadata dict for converter parse_xml (registry-injected)."""
    if "converter_metadata" in context.extra:
        return deepcopy(context.extra["converter_metadata"])

    bundle = propagate_step_metadata(context)
    return deepcopy(bundle.converter_metadata)


def _collect_referenced_columns(step_type: str, metadata: dict[str, Any]) -> set[str]:
    """Extract column names referenced by step metadata (pre-codegen)."""
    st = _normalize_step_type(step_type)
    refs: set[str] = set()

    if st == "calculator":
        for calc in metadata.get("calculations") or []:
            for key in ("field_a", "field_b", "field_c"):
                if calc.get(key):
                    refs.add(calc[key])

    elif st == "filterrows":
        def _walk_condition(node: dict | None) -> None:
            if not node:
                return
            if node.get("leftvalue"):
                refs.add(node["leftvalue"])
            if node.get("rightvalue"):
                refs.add(node["rightvalue"])
            for child in node.get("conditions") or []:
                _walk_condition(child)

        _walk_condition(metadata.get("condition"))
        _walk_condition(metadata.get("filter_condition"))

    elif st == "switchcase":
        field = metadata.get("switch_field") or metadata.get("fieldname")
        if field:
            refs.add(field)

    elif st == "javafilter":
        # Best-effort: extract simple identifiers from preserved condition text.
        cond = metadata.get("condition") or ""
        for token in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", cond):
            if token.lower() not in {
                "true", "false", "null", "and", "or", "not", "new", "return",
            }:
                refs.add(token)

    elif st == "valuemapper":
        if metadata.get("field_to_use"):
            refs.add(metadata["field_to_use"])

    elif st in ("groupby", "memorygroupby"):
        for key in metadata.get("group_keys") or []:
            refs.add(key)
        for agg in metadata.get("aggregates") or []:
            subject = agg.get("subject") or agg.get("name")
            if subject:
                refs.add(subject)

    elif st == "analyticquery":
        for key in metadata.get("group_fields") or metadata.get("partition_fields") or []:
            refs.add(key)
        for field in metadata.get("analytic_fields") or metadata.get("fields") or []:
            subject = field.get("subject") if isinstance(field, dict) else None
            if subject:
                refs.add(subject)
            of = field.get("order_field") if isinstance(field, dict) else None
            if of:
                refs.add(of)

    elif st in ("univariatestats", "univariatestatistics"):
        for item in metadata.get("stats") or metadata.get("fields") or []:
            source = item.get("source_field") if isinstance(item, dict) else None
            if source:
                refs.add(source)

    elif st in (
        "mergejoin", "joinrows", "mergerows", "mergerow",
        "multimergejoin", "multiwaymergejoin", "multimerge", "sortedmerge",
        "xmljoin", "streamlookup", "databaselookup", "dblookup",
        "dbjoin", "databasejoin", "fuzzymatch",
        "dimensionlookup", "dimensionlookupupdate", "combinationlookup",
    ):
        for pair in metadata.get("keys") or metadata.get("join_keys") or []:
            left = pair.get("left") if isinstance(pair, dict) else None
            if left:
                refs.add(left)
            stream = pair.get("stream_field") if isinstance(pair, dict) else None
            if stream:
                refs.add(stream)
        if st in ("dimensionlookup", "dimensionlookupupdate", "combinationlookup"):
            for item in metadata.get("fields") or []:
                if isinstance(item, dict) and item.get("stream_field"):
                    refs.add(item["stream_field"])
            for field_name in (
                metadata.get("stream_datefield"),
                metadata.get("start_date_field_name"),
                metadata.get("last_update_field"),
            ):
                if field_name:
                    refs.add(field_name)
        for key_name in metadata.get("key_fields") or []:
            if key_name:
                refs.add(key_name)
        for value_name in metadata.get("value_fields") or []:
            if value_name:
                refs.add(value_name)
        for item in metadata.get("sort_fields") or []:
            if isinstance(item, dict) and item.get("name"):
                refs.add(item["name"])
        for field_name in (
            metadata.get("target_xml_field"),
            metadata.get("source_xml_field"),
            metadata.get("join_compare_field"),
        ):
            if field_name:
                refs.add(field_name)
        for p in metadata.get("parameters") or []:
            if isinstance(p, dict) and p.get("name"):
                refs.add(p["name"])
        if metadata.get("main_stream_field"):
            refs.add(metadata["main_stream_field"])

    elif st == "selectvalues":
        for col in metadata.get("select_columns") or []:
            refs.add(col)
        for change in metadata.get("meta_changes") or []:
            if change.get("name"):
                refs.add(change["name"])
        for name in metadata.get("remove_names") or []:
            if name:
                refs.add(name)

    elif st in ("setvalueconstant", "setvaluefield"):
        for item in metadata.get("set_fields") or metadata.get("fields") or []:
            if item.get("name"):
                refs.add(item["name"])
            if item.get("replace_by"):
                refs.add(item["replace_by"])

    elif st == "concatfields":
        for item in metadata.get("fields") or []:
            if item.get("name"):
                refs.add(item["name"])

    elif st == "addxml":
        for item in metadata.get("fields") or []:
            if item.get("name"):
                refs.add(item["name"])

    elif st in ("replaceinstring", "stringoperations", "stringcut"):
        for op in metadata.get("operations") or metadata.get("fields") or []:
            if isinstance(op, dict):
                for key in ("in", "in_stream_name", "replace_field_by_string"):
                    if op.get(key):
                        refs.add(op[key])

    elif st == "formula":
        # Upstream refs are [field] tokens inside formulas, not the output field_name
        for entry in metadata.get("formulas") or []:
            formula = entry.get("formula") or ""
            for m in re.finditer(r"\[([^\]]+)\]", formula):
                refs.add(m.group(1).strip())
        flat = metadata.get("formula") or ""
        for m in re.finditer(r"\[([^\]]+)\]", flat):
            refs.add(m.group(1).strip())

    elif st in ("regexeval", "regularexpression"):
        if metadata.get("matcher"):
            refs.add(metadata["matcher"])

    elif st in ("scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue"):
        for field in metadata.get("fields") or []:
            if isinstance(field, dict) and field.get("name"):
                # Output declarations; upstream refs are inside JS (best-effort skip)
                pass
        script = metadata.get("script") or ""
        for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", script):
            tok = m.group(1)
            if tok not in {"var", "let", "const", "true", "false", "null", "undefined"}:
                refs.add(tok)

    elif st in ("execsql", "executesql", "sql", "execsqlrow", "executerowsqlscript", "executerowsql"):
        for arg in metadata.get("arguments") or []:
            if arg:
                refs.add(arg)
        if metadata.get("sql_field"):
            refs.add(metadata["sql_field"])

    elif st in ("userdefinedjavaexpression",):
        for field in metadata.get("fields") or []:
            expr = (field.get("expression") or "") if isinstance(field, dict) else ""
            for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", expr):
                tok = m.group(1)
                if tok not in {"true", "false", "null", "new"}:
                    refs.add(tok)

    elif st == "sortrows":
        for item in metadata.get("sort_fields") or []:
            if isinstance(item, (list, tuple)) and item:
                refs.add(item[0])
            elif isinstance(item, dict) and item.get("name"):
                refs.add(item["name"])

    elif st in (
        "unique", "uniquerows", "uniquerowsbyhashset",
        "uniquerowshashset", "uniquehashset",
    ):
        for item in metadata.get("compare_fields") or []:
            if isinstance(item, dict) and item.get("name"):
                refs.add(item["name"])
            elif isinstance(item, str):
                refs.add(item)

    elif st in ("rownormaliser", "rownormalizer", "normaliser"):
        for item in metadata.get("fields") or []:
            if item.get("name"):
                refs.add(item["name"])

    elif st in ("rowdenormaliser", "rowdenormalizer", "denormaliser"):
        for g in metadata.get("group_fields") or []:
            refs.add(g)
        if metadata.get("key_field"):
            refs.add(metadata["key_field"])
        for t in metadata.get("target_fields") or []:
            if t.get("field_name"):
                refs.add(t["field_name"])

    elif st in ("flattener", "rowflattener"):
        if metadata.get("field_name"):
            refs.add(metadata["field_name"])

    elif st == "splitfieldtorows":
        if metadata.get("split_field"):
            refs.add(metadata["split_field"])

    elif st in ("fieldsplitter", "splitfields"):
        if metadata.get("split_field"):
            refs.add(metadata["split_field"])

    elif st in ("closuregenerator", "closure"):
        if metadata.get("parent_id_field"):
            refs.add(metadata["parent_id_field"])
        if metadata.get("child_id_field"):
            refs.add(metadata["child_id_field"])

    elif st in ("getslavesequence", "getidfromslaveserver", "getidfromslave"):
        # Adds a new column; no upstream value references.
        pass

    elif st in ("xslt", "xsltransformation", "xsltransform"):
        if metadata.get("field_name"):
            refs.add(metadata["field_name"])
        if metadata.get("xsl_file_field_use") and metadata.get("xsl_file_field"):
            refs.add(metadata["xsl_file_field"])
        for param in metadata.get("parameters") or []:
            if isinstance(param, dict) and param.get("field"):
                refs.add(param["field"])

    elif st in ("pgpencryptstream", "pgpencrypt", "pgpdecryptstream", "pgpdecrypt"):
        for key in ("stream_field", "keyname_field", "passphrase_field"):
            field = metadata.get(key)
            if field:
                refs.add(field)

    elif st in ("symmetriccryptotrans", "symmetriccrypto", "symmetriccryptography"):
        for key in ("message_field", "secret_key_field", "iv_field"):
            field = metadata.get(key)
            if field:
                refs.add(field)

    elif st in ("sftpput", "sftpputfile", "putafilewithsftp", "putsftp"):
        for key in (
            "local_filename_field",
            "remote_directory_field",
            "remote_filename_field",
            "destination_folder_field",
        ):
            field = metadata.get(key)
            if field:
                refs.add(field)

    elif st in ("callendpoint", "callendpointstep"):
        if metadata.get("module_from_field") and metadata.get("module_name"):
            refs.add(metadata["module_name"])
        if metadata.get("endpoint_from_field"):
            if metadata.get("endpoint_path"):
                refs.add(metadata["endpoint_path"])
            if metadata.get("http_method"):
                refs.add(metadata["http_method"])
        for param in metadata.get("parameters") or metadata.get("request_parameters") or []:
            if isinstance(param, dict) and param.get("field_name"):
                refs.add(param["field_name"])
        for hdr in metadata.get("headers") or []:
            if isinstance(hdr, dict) and hdr.get("field"):
                refs.add(hdr["field"])

    elif st in (
        "setsessionvariable", "setsessionvariables", "setsessionvariablestep",
    ):
        for field in metadata.get("fields") or []:
            if isinstance(field, dict) and field.get("field_name"):
                refs.add(field["field_name"])

    elif st in ("script", "scriptvalues", "experimentalscript"):
        # Scripts may reference upstream fields by name; declared fields are outputs.
        pass

    return refs


def infer_lineage_from_metadata(
    context: StepContext,
    metadata: dict[str, Any],
    input_schemas: dict[str, ColumnSchema],
) -> ColumnLineage:
    """Infer output column lineage from propagated metadata (no generated code)."""
    step = context.step
    st = _normalize_step_type(step.step_type)
    input_names = set(input_schemas)
    output_names = set(input_names)
    type_map = {name: schema.type_name for name, schema in input_schemas.items()}
    added: set[str] = set()
    removed: set[str] = set()
    modified: set[str] = set()
    renamed: dict[str, str] = {}

    if st in ("rowgenerator", "datagrid"):
        output_names = {
            f.get("name") for f in (metadata.get("fields") or []) if f.get("name")
        }
        type_map.update({
            f["name"]: f.get("type", "String")
            for f in (metadata.get("fields") or [])
            if f.get("name")
        })
        added = output_names.copy()
        removed = input_names - output_names

    elif st == "constant":
        for const in metadata.get("constants") or []:
            name = const.get("name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = const.get("type", "String")

    elif st == "calculator":
        for calc in metadata.get("calculations") or []:
            fname = calc.get("field_name")
            if fname:
                output_names.add(fname)
                added.add(fname)
                type_map[fname] = calc.get("value_type") or "String"
            if calc.get("remove"):
                for key in ("field_a", "field_b", "field_c"):
                    col = calc.get(key)
                    if col and col in output_names:
                        output_names.discard(col)
                        removed.add(col)

    elif st == "selectvalues":
        field_dicts = metadata.get("fields") or metadata.get("select_fields") or []
        selected = metadata.get("select_columns") or [f.get("name") for f in field_dicts]
        remove_names = set(metadata.get("remove_names") or [])
        meta_changes = metadata.get("meta_changes") or []

        if selected:
            output_names = set()
            for f in field_dicts:
                src = f.get("name")
                if not src or src not in selected:
                    continue
                if src in remove_names:
                    continue
                dst = f.get("rename") or src
                output_names.add(dst)
                type_map[dst] = f.get("type", type_map.get(src, "String"))
                if f.get("rename") and f["rename"] != src:
                    renamed[src] = f["rename"]
            for change in meta_changes:
                src = change.get("name")
                if not src:
                    continue
                dst = change.get("rename") or src
                if src in output_names or not field_dicts:
                    if src in output_names and dst != src:
                        output_names.discard(src)
                        output_names.add(dst)
                        renamed[src] = dst
                    if change.get("type_name"):
                        type_map[dst] = change["type_name"]
            removed = (input_names - set(selected or input_names)) | (
                remove_names & input_names
            )
        elif remove_names:
            output_names = input_names - remove_names
            removed = remove_names & input_names
            for change in meta_changes:
                src = change.get("name")
                dst = (change.get("rename") or src) if src else ""
                if src and dst and dst != src and src in output_names:
                    output_names.discard(src)
                    output_names.add(dst)
                    renamed[src] = dst
        else:
            for change in meta_changes:
                src = change.get("name")
                if not src:
                    continue
                dst = change.get("rename") or src
                if dst != src and src in output_names:
                    output_names.discard(src)
                    output_names.add(dst)
                    renamed[src] = dst
                    modified.add(dst)
                elif change.get("type_name"):
                    modified.add(dst)
                    type_map[dst] = change["type_name"]

    elif st == "setvalueconstant":
        for item in metadata.get("set_fields") or metadata.get("fields") or []:
            name = item.get("name")
            if name:
                modified.add(name)

    elif st == "setvaluefield":
        for item in metadata.get("set_fields") or metadata.get("fields") or []:
            name = item.get("name")
            if name:
                modified.add(name)

    elif st == "concatfields":
        target = metadata.get("target_field_name")
        if target:
            output_names.add(target)
            added.add(target)
            type_map[target] = "String"
        if metadata.get("remove_selected_fields"):
            for item in metadata.get("fields") or []:
                name = item.get("name")
                if name and name in output_names:
                    output_names.discard(name)
                    removed.add(name)

    elif st == "addxml":
        value_name = metadata.get("value_name") or "xmlvaluename"
        output_names.add(value_name)
        added.add(value_name)
        type_map[value_name] = "String"

    elif st in ("replaceinstring", "stringoperations", "stringcut"):
        for op in metadata.get("operations") or metadata.get("fields") or []:
            if not isinstance(op, dict):
                continue
            out_name = op.get("out") or op.get("out_stream_name")
            in_name = op.get("in") or op.get("in_stream_name")
            if out_name and out_name != in_name:
                output_names.add(out_name)
                added.add(out_name)
            elif in_name:
                modified.add(in_name)

    elif st in ("groupby", "memorygroupby"):
        keys = set(metadata.get("group_keys") or [])
        agg_names = {a.get("name") for a in (metadata.get("aggregates") or []) if a.get("name")}
        output_names = keys | agg_names
        added = agg_names
        removed = input_names - output_names
        for agg in metadata.get("aggregates") or []:
            subject = agg.get("subject")
            if subject:
                modified.add(subject)

    elif st == "analyticquery":
        for field in metadata.get("analytic_fields") or metadata.get("fields") or []:
            name = field.get("name") if isinstance(field, dict) else None
            if name:
                output_names.add(name)
                added.add(name)

    elif st in ("univariatestats", "univariatestatistics"):
        output_names = set()
        for item in metadata.get("stats") or metadata.get("fields") or []:
            source = (item.get("source_field") or "") if isinstance(item, dict) else ""
            if not source:
                continue
            if item.get("calc_n", True):
                output_names.add(f"{source}(N)")
            if item.get("calc_mean", True):
                output_names.add(f"{source}(mean)")
            if item.get("calc_std_dev", True):
                output_names.add(f"{source}(stdDev)")
                if item.get("calc_variance") is None or item.get("calc_variance"):
                    output_names.add(f"{source}(variance)")
            if item.get("calc_min", True):
                output_names.add(f"{source}(min)")
            if item.get("calc_max", True):
                output_names.add(f"{source}(max)")
            if item.get("calc_median", True):
                output_names.add(f"{source}(median)")
        added = set(output_names)
        removed = input_names

    elif st in ("stepsmetrics", "outputstepsmetrics"):
        for key in (
            "step_name_field", "step_id_field", "lines_input_field",
            "lines_output_field", "lines_read_field", "lines_updated_field",
            "lines_written_field", "lines_errors_field", "seconds_field",
        ):
            fname = metadata.get(key)
            if fname:
                output_names.add(fname)
                added.add(fname)
        removed = input_names

    elif st == "valuemapper":
        target = metadata.get("target_field") or metadata.get("field_to_use")
        if target:
            output_names.add(target)
            added.add(target)
            type_map[target] = "String"

    elif st in ("sequence", "addsequence"):
        field_name = metadata.get("field_name", "seq")
        output_names.add(field_name)
        added.add(field_name)
        type_map[field_name] = "Integer"

    elif st in ("checksum", "addachecksum"):
        result_field = metadata.get("result_field") or "checksum"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "String"

    elif st == "numberrange":
        output_field = metadata.get("output_field") or "range"
        output_names.add(output_field)
        added.add(output_field)
        type_map[output_field] = "String"

    elif st in ("fieldschangesequence", "addvaluefieldschangingsequence"):
        result_field = metadata.get("result_field") or "change_seq"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "Integer"

    elif st in (
        "mergejoin", "joinrows", "mergerows", "mergerow",
        "multimergejoin", "multiwaymergejoin", "multimerge", "sortedmerge",
        "xmljoin", "streamlookup", "databaselookup", "dblookup",
        "dbjoin", "databasejoin", "fuzzymatch",
        "dimensionlookup", "dimensionlookupupdate", "combinationlookup",
    ):
        for ret in metadata.get("return_fields") or []:
            name = ret.get("rename") or ret.get("name")
            if name:
                output_names.add(name)
                added.add(name)
        for pair in metadata.get("keys") or metadata.get("join_keys") or []:
            right = pair.get("right") if isinstance(pair, dict) else None
            if right:
                output_names.add(right)
                added.add(right)
        tk = metadata.get("technical_key_rename") or metadata.get("technical_key")
        if tk:
            output_names.add(tk)
            added.add(tk)
            type_map[tk] = "Integer"
        out_match = metadata.get("output_match_field")
        if out_match:
            output_names.add(out_match)
            added.add(out_match)
        flag_field = metadata.get("flag_field")
        if flag_field:
            output_names.add(flag_field)
            added.add(flag_field)
        value_xml = metadata.get("value_xml_field")
        if value_xml:
            output_names.add(value_xml)
            added.add(value_xml)

    elif st == "formula":
        for entry in metadata.get("formulas") or []:
            name = entry.get("field_name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = entry.get("value_type") or "String"
        if metadata.get("field_name") and metadata.get("formula"):
            name = metadata["field_name"]
            output_names.add(name)
            added.add(name)
            type_map[name] = metadata.get("value_type") or "String"

    elif st in ("regexeval", "regularexpression"):
        result_field = metadata.get("result_field") or "result"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "String"  # Y/N
        for field in metadata.get("fields") or []:
            name = field.get("name") if isinstance(field, dict) else None
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = (field.get("type") if isinstance(field, dict) else None) or "String"

    elif st in ("scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue"):
        for field in metadata.get("fields") or []:
            if not isinstance(field, dict):
                continue
            name = field.get("rename") or field.get("name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = field.get("type") or "String"

    elif st in ("userdefinedjavaclass", "userdefinedjavaexpression"):
        for field in metadata.get("fields") or []:
            if not isinstance(field, dict):
                continue
            name = field.get("name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = field.get("type") or "String"

    elif st in (
        "ruleaccumulator", "rulesaccumulator", "ruleexecutor", "rulesexecutor",
    ):
        for field in metadata.get("result_columns") or metadata.get("fields") or []:
            if not isinstance(field, dict):
                continue
            name = field.get("name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = field.get("type") or "String"

    elif st in ("execsql", "executesql", "sql", "execsqlrow", "executerowsqlscript", "executerowsql"):
        for key in ("insert_field", "update_field", "delete_field", "read_field"):
            fname = metadata.get(key)
            if fname:
                output_names.add(fname)
                added.add(fname)
                type_map[fname] = "Integer"

    elif st == "filterrows":
        # Schema preserved; routing hops are metadata-only.
        pass

    elif st in (
        "fileexists", "tableexists", "columnexists",
        "checkfilelocked", "fileslocked", "lockedfiles",
        "webserviceavailable", "checkwebserviceavailable",
    ):
        result_field = metadata.get("result_field") or {
            "fileexists": "file_exists",
            "tableexists": "table_exists",
            "columnexists": "column_exists",
            "checkfilelocked": "file_locked",
            "fileslocked": "file_locked",
            "lockedfiles": "file_locked",
            "webserviceavailable": "webservice_available",
            "checkwebserviceavailable": "webservice_available",
        }.get(st, "result")
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "Boolean"
        if st == "fileexists" and metadata.get("include_file_type"):
            ft = metadata.get("file_type_field") or "filetype"
            output_names.add(ft)
            added.add(ft)
            type_map[ft] = "String"

    elif st in ("http", "httpclient", "httpget", "httppost", "rest", "restclient"):
        result_field = metadata.get("result_field") or "result"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "String"
        for key, default_type in (
            ("response_code_field", "Integer"),
            ("response_time_field", "Integer"),
            ("response_header_field", "String"),
        ):
            fname = metadata.get(key)
            if fname:
                output_names.add(fname)
                added.add(fname)
                type_map[fname] = default_type

    elif st in ("dbproc", "calldbproc", "calldbprocedure"):
        for res in metadata.get("results") or []:
            if isinstance(res, dict):
                name = res.get("rename") or res.get("name")
                if name:
                    output_names.add(name)
                    added.add(name)
        for p in metadata.get("parameters") or []:
            if isinstance(p, dict) and str(p.get("direction") or "").upper() in ("OUT", "INOUT", "RETURN"):
                name = p.get("rename") or p.get("name")
                if name:
                    output_names.add(name)
                    added.add(name)

    elif st in ("webservice", "webservicelookup"):
        for out in metadata.get("output_fields") or []:
            if isinstance(out, dict):
                name = out.get("rename") or out.get("name")
                if name:
                    output_names.add(name)
                    added.add(name)
                    type_map[name] = out.get("type") or "String"

    elif st in ("identifylastrow", "identifylastrowinastream"):
        result_field = metadata.get("result_field") or "result"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "Boolean"

    elif st in ("pgpencryptstream", "pgpencrypt", "pgpdecryptstream", "pgpdecrypt"):
        result_field = metadata.get("result_field") or "result"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "String"

    elif st in ("secretkeygenerator", "secretkeygen"):
        output_names = set()
        added = set()
        key_field = metadata.get("secret_key_field") or "secretKey"
        output_names.add(key_field)
        added.add(key_field)
        type_map[key_field] = (
            "Binary" if metadata.get("output_key_in_binary") else "String"
        )
        algo_field = metadata.get("algorithm_field")
        if algo_field:
            output_names.add(algo_field)
            added.add(algo_field)
            type_map[algo_field] = "String"
        len_field = metadata.get("secret_key_length_field")
        if len_field:
            output_names.add(len_field)
            added.add(len_field)
            type_map[len_field] = "Integer"
        removed = input_names - output_names

    elif st in ("symmetriccryptotrans", "symmetriccrypto", "symmetriccryptography"):
        result_field = metadata.get("result_field") or "result"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = (
            "Binary" if metadata.get("output_result_as_binary") else "String"
        )

    elif st in ("sftpput", "sftpputfile", "putafilewithsftp", "putsftp"):
        # Passthrough schema; upload side-effect only.
        pass

    elif st in ("callendpoint", "callendpointstep"):
        result_field = metadata.get("result_field") or "result"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "String"
        for key, default_type in (
            ("status_code_field", "Integer"),
            ("response_time_field", "Integer"),
        ):
            fname = metadata.get(key)
            if fname:
                output_names.add(fname)
                added.add(fname)
                type_map[fname] = default_type

    elif st in (
        "getsessionvariable", "getsessionvariables", "getsessionvariablestep",
    ):
        for field in metadata.get("fields") or []:
            if not isinstance(field, dict):
                continue
            name = field.get("name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = field.get("type") or field.get("type_name") or "String"

    elif st in (
        "setsessionvariable", "setsessionvariables", "setsessionvariablestep",
    ):
        # Passthrough schema; session writes are side-effects.
        pass

    elif st in ("script", "scriptvalues", "experimentalscript"):
        for field in metadata.get("fields") or []:
            if not isinstance(field, dict):
                continue
            name = field.get("rename") or field.get("name")
            if not name:
                continue
            output_names.add(name)
            if not field.get("replace"):
                added.add(name)
            else:
                modified.add(name)
            type_map[name] = field.get("type") or "String"

    elif st in ("javafilter", "switchcase"):
        # Schema preserved; routing hops are metadata-only.
        pass

    elif st in (
        "unique", "uniquerows", "uniquerowsbyhashset",
        "uniquerowshashset", "uniquehashset",
    ):
        if metadata.get("count_rows") and metadata.get("count_field"):
            output_names.add(metadata["count_field"])
            added.add(metadata["count_field"])
            type_map[metadata["count_field"]] = "Integer"

    elif st in ("rownormaliser", "rownormalizer", "normaliser"):
        type_field = metadata.get("type_field") or "typefield"
        source_names = {f.get("name") for f in (metadata.get("fields") or []) if f.get("name")}
        norm_names = {
            (f.get("norm") or f.get("name"))
            for f in (metadata.get("fields") or [])
            if f.get("name")
        }
        output_names = (input_names - source_names) | {type_field} | norm_names
        added = ({type_field} | norm_names) - input_names
        removed = source_names & input_names

    elif st in ("rowdenormaliser", "rowdenormalizer", "denormaliser"):
        group_fields = set(metadata.get("group_fields") or [])
        targets = {
            (t.get("target_name") or t.get("field_name"))
            for t in (metadata.get("target_fields") or [])
            if t.get("target_name") or t.get("field_name")
        }
        output_names = group_fields | targets
        added = targets
        removed = input_names - output_names

    elif st in ("flattener", "rowflattener"):
        field_name = metadata.get("field_name") or ""
        targets = set(metadata.get("target_fields") or [])
        output_names = (input_names - ({field_name} if field_name else set())) | targets
        added = targets
        if field_name:
            removed.add(field_name)

    elif st == "splitfieldtorows":
        new_field = metadata.get("new_field") or metadata.get("split_field")
        if new_field:
            output_names.add(new_field)
            added.add(new_field)
        if metadata.get("include_row_number"):
            rn = metadata.get("row_number_field") or "rownr"
            output_names.add(rn)
            added.add(rn)

    elif st in ("fieldsplitter", "splitfields"):
        split_field = metadata.get("split_field") or ""
        for f in metadata.get("fields") or []:
            name = f.get("name")
            if name:
                output_names.add(name)
                added.add(name)
                type_map[name] = f.get("type") or "String"
        if split_field and split_field in output_names:
            output_names.discard(split_field)
            removed.add(split_field)

    elif st in ("closuregenerator", "closure"):
        parent_f = metadata.get("parent_id_field") or "parent_id"
        child_f = metadata.get("child_id_field") or "child_id"
        dist_f = metadata.get("distance_field") or "distance"
        output_names = {parent_f, child_f, dist_f}
        added = output_names - input_names
        removed = input_names - output_names
        type_map[dist_f] = "Integer"

    elif st in ("getslavesequence", "getidfromslaveserver", "getidfromslave"):
        value_name = metadata.get("value_name") or "id"
        output_names.add(value_name)
        added.add(value_name)
        type_map[value_name] = "Integer"

    elif st in ("xslt", "xsltransformation", "xsltransform"):
        result_field = metadata.get("result_field") or "result"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "String"

    elif st in ("checksum", "addachecksum"):
        result_field = metadata.get("result_field") or "checksum"
        output_names.add(result_field)
        added.add(result_field)

    elif st == "numberrange":
        output_field = metadata.get("output_field") or "range"
        output_names.add(output_field)
        added.add(output_field)

    elif st in ("fieldschangesequence", "addvaluefieldschangingsequence"):
        result_field = metadata.get("result_field") or "change_seq"
        output_names.add(result_field)
        added.add(result_field)
        type_map[result_field] = "Integer"

    elif st in ("sequence", "addsequence"):
        field_name = metadata.get("field_name") or "sequenceno"
        output_names.add(field_name)
        added.add(field_name)
        type_map[field_name] = "Integer"

    elif st in ("textfileoutput", "textfileoutputlegacy"):
        # Output step — schema unchanged on stream.
        pass

    elif st == "tableinput":
        # Unknown until SQL executed; keep empty or infer from SQL tokens lightly.
        output_names = set(input_names)

    output_schemas = _schemas_from_names(output_names, type_map, source_step=step.name)
    return ColumnLineage(
        step_name=step.name,
        step_type=step.step_type,
        input_df=context.input_df_name(),
        output_df=context.output_df_name(),
        input_columns=input_schemas,
        output_columns=output_schemas,
        added_columns=added,
        removed_columns=removed,
        modified_columns=modified,
        renamed_columns=renamed,
    )


def merge_input_lineage(context: StepContext) -> dict[str, ColumnSchema]:
    """Merge upstream column schemas from lineage_map in context.extra."""
    lineage_map: dict[str, dict[str, ColumnSchema]] = context.extra.get("lineage_map", {})
    schemas: dict[str, ColumnSchema] = {}

    for pred in context.dag.predecessors(context.step.name):
        for name, schema in lineage_map.get(pred, {}).items():
            schemas[name] = schema

    # Fallback: plain column name set from code_generator compatibility.
    for col in context.extra.get("input_columns") or []:
        if col not in schemas:
            schemas[col] = ColumnSchema(name=col, type_name="String", source_step="upstream")

    return schemas


def validate_lineage_before_convert(
    context: StepContext,
    metadata: dict[str, Any],
    input_schemas: dict[str, ColumnSchema],
) -> LineageValidationResult:
    """Validate column references and output schema before converter runs."""
    result = LineageValidationResult()
    st = _normalize_step_type(context.step.step_type)
    input_names = set(input_schemas)

    if st in (
        "tableinput", "csvinput", "rowgenerator", "datagrid",
        "jsoninput", "textfileinput", "oldtextfileinput", "excelinput", "xmlinput", "getxmldata",
        "fixedinput", "fixedfileinput", "gzipcsvinput", "s3csvinput", "yamlinput",
        "propertyinput", "xmlinputstream", "loadfileinput", "accessinput", "sasinput",
        "xbaseinput", "shapefilereader", "getfilenames", "getsubfolders",
        "getfilesrowscount", "gettablenames", "randomvalue", "randomccnumbergenerator",
        "salesforceinput", "ldapinput", "ldifinput", "rssinput", "hl7input",
        "systeminfo", "cubeinput", "mondrianinput", "olapinput", "mailinput",
        "getrepositorynames", "parquetinput", "orcinput", "avroinput",
        "avrofileinput", "mongodbinput", "mongoinput",
        "sapinput", "saperpinput",
    ):
        return result

    refs = _collect_referenced_columns(st, metadata)
    missing = sorted(refs - input_names)
    if missing and input_names:
        # Incomplete upstream lineage must not abort Select Values (or other transforms);
        # emit a migration warning and let the converter preserve the DataFrame chain.
        if st == "selectvalues":
            result.warnings.append(
                f"SelectValues references columns not present in upstream lineage: "
                f"{', '.join(missing)}"
            )
        else:
            result.warnings.append(
                f"Metadata references columns not in upstream lineage: {', '.join(missing)}"
            )

    predicted = infer_lineage_from_metadata(context, metadata, input_schemas)
    output_names = list(predicted.output_column_names)
    seen: set[str] = set()
    duplicates: list[str] = []
    for name in output_names:
        if name in seen:
            duplicates.append(name)
        seen.add(name)
    if duplicates:
        result.errors.append(f"Duplicate output column names: {', '.join(sorted(set(duplicates)))}")

    rename_values = set(predicted.renamed_columns.values())
    collisions = sorted(rename_values & (input_names - set(predicted.renamed_columns)))
    if collisions:
        # Select Values with an explicit select list projects a new schema: renaming
        # onto an upstream name is valid when that upstream column is not kept.
        select_sources = {
            (c or "").strip()
            for c in (metadata.get("select_columns") or [])
            if c
        }
        if not select_sources:
            for item in metadata.get("select_fields") or metadata.get("fields") or []:
                if isinstance(item, dict) and item.get("name"):
                    select_sources.add(item["name"].strip())
        if st == "selectvalues" and select_sources:
            true_collisions = sorted(c for c in collisions if c in select_sources)
            if true_collisions:
                result.errors.append(
                    f"Rename target collides with selected column: "
                    f"{', '.join(true_collisions)}"
                )
            else:
                result.warnings.append(
                    f"Rename target reuses upstream column name via projection: "
                    f"{', '.join(collisions)}"
                )
        else:
            result.errors.append(
                f"Rename target collides with existing column: {', '.join(collisions)}"
            )

    for old, new in predicted.renamed_columns.items():
        if input_names and old not in input_names:
            result.warnings.append(
                f"Rename source column '{old}' not in upstream lineage"
            )
        elif not input_names and old:
            result.warnings.append(
                f"Rename source column '{old}' could not be verified (no upstream lineage)"
            )

    context.extra["predicted_lineage"] = predicted
    context.extra["lineage_validation"] = result
    return result


def update_lineage_map(context: StepContext, lineage: ColumnLineage) -> None:
    """Store output schemas for downstream steps."""
    lineage_map: dict[str, dict[str, ColumnSchema]] = context.extra.setdefault("lineage_map", {})
    lineage_map[context.step.name] = dict(lineage.output_columns)
    context.extra["output_columns"] = sorted(lineage.output_column_names)
    context.extra["column_lineage"] = lineage
