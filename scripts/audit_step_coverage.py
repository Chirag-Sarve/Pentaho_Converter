"""Audit registered Spoon steps vs parsers, validators, and tests."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pentaho_converter.converters.registry_list import (  # noqa: E402
    _all_handlers,
    build_all_converters,
)
from pentaho_converter.step_xml import _STRUCTURED_STEP_TYPES  # noqa: E402
from pentaho_converter.validation.registry import get_validator  # noqa: E402
from pentaho_converter.validation.step_validators import (  # noqa: E402
    register_builtin_validators,
)

# Intentionally unsupported (validators require UNSUPPORTED markers)
UNSUPPORTED_INPUT = frozenset({
    "getrepositorynames",
    "mailinput",
    "emailmessagesinput",
    "emailinput",
    "cubeinput",
    "deserializefromfile",
    "deserialisefromfile",
    "mondrianinput",
    "olapinput",
    "xmla",
    "xmlainput",
    "sapinput",
    "saperpinput",
})
UNSUPPORTED_OUTPUT = frozenset({
    "cubeoutput",
    "serializetofile",
    "serialisetofile",
    "pentahoreportingoutput",
    "reportexport",
    "prptoutput",
    "pentahoreporting",
})
# Other intentional partial / unsupported families (handlers emit warnings)
PARTIAL_FAMILIES = frozenset({
    # Streaming without Databricks equivalent
    "jmsconsumer", "jmsconsumerinput", "activemqconsumer",
    "jmsproducer", "jmsproduceroutput", "activemqproducer",
    "mqttconsumer", "mqttconsumerinput", "mqttclient",
    "mqttproducer", "mqttproduceroutput",
    # Carte / Splunk / sockets
    "getslavesequence", "getidfromslaveserver", "getidfromslave",
    "splunkinput", "splunkoutput", "splunk",
    "socketreader", "socketwriter",
    # Process / mail / syslog on Databricks
    "execprocess", "executeaprocess", "ssh", "runsshcommands",
    "mail", "sendmail", "syslogmessage", "writetosyslog", "sendmessagetosyslog",
    # Scripting that needs manual port
    "scriptvaluemod", "javascriptvalue", "modifiedjavascriptvalue",
    "ruleaccumulator", "rulesaccumulator", "ruleexecutor", "rulesexecutor",
    "userdefinedjavaclass", "userdefinedjavaexpression",
    "script", "scriptvalues", "experimentalscript",
    # Bulk loaders (approx via Delta)
    "gpbulkloader", "greenplumbulkloader", "greenplumload", "greenplumloader", "gpload",
    "infobrightloader", "infobrightbulkloader", "infobright",
    "vectorwisebulkloader", "ingresvectorwisebulkloader", "vwbulkloader",
    "ingresbulkloader", "vectorwiseloader",
    "monetdbbulkloader", "monetdbloader", "monetdbbulk",
    "mysqlbulkloader", "mysqlloader", "loaddatainfile",
    "orabulkloader", "oraclebulkloader", "oracleloader", "sqlldr",
    "pgbulkloader", "postgresqlbulkloader", "postgresbulkloader", "psqlbulkloader",
    "terafast", "teradatafastloadbulkloader", "terafastbulkloader",
    "teradatafastload", "fastload", "teradatabulkloader",
    "teradatatptbulkloader", "tptbulkloader", "teratpt", "teradatatpt",
    "verticabulkloader", "verticaloader", "verticacopy",
    # Flow partial
    "javafilter", "metainject", "etlmetadatainjection",
    "jobexecutor", "transexecutor", "transformationexecutor", "singlethreader",
    "blockuntilstepsfinish", "blockthisstepuntilstepsfinish",
    # Autodoc
    "autodoc", "automaticdocumentationoutput", "autodocoutput",
})


def _norm(t: str) -> str:
    return (t or "").strip().lower().replace(" ", "").replace("(", "").replace(")", "")


def _test_blob() -> str:
    parts: list[str] = []
    for path in (ROOT / "tests").glob("test_*.py"):
        parts.append(path.read_text(encoding="utf-8", errors="ignore").lower())
    return "\n".join(parts)


def main() -> int:
    register_builtin_validators()

    handlers = _all_handlers()
    registered: dict[str, str] = {}
    duplicate_claims: list[tuple[str, str, str]] = []
    empty_stubs: list[str] = []

    for handler in handlers:
        name = f"{type(handler).__module__}.{type(handler).__name__}"
        types = {_norm(t) for t in (getattr(handler, "_TYPES", set()) or set())}
        if not types:
            empty_stubs.append(name)
            continue
        for t in types:
            if t in registered:
                duplicate_claims.append((t, name, registered[t]))
            else:
                registered[t] = name

    converters = build_all_converters()
    conv_types: set[str] = set()
    handler_by_type: dict[str, str] = {}
    for conv in converters:
        hname = getattr(conv, "handler_name", None) or type(conv).__name__
        for t in conv.step_types:
            nt = _norm(t)
            conv_types.add(nt)
            handler_by_type[nt] = hname

    structured = {_norm(t) for t in _STRUCTURED_STEP_TYPES}
    blob = _test_blob()

    missing_parser: list[str] = []
    generic_only: list[str] = []
    untested: list[str] = []
    supported: list[str] = []
    partial: list[str] = []
    unsupported: list[str] = []

    for t in sorted(conv_types):
        if t not in structured:
            missing_parser.append(t)

        validator = get_validator(t)
        vname = type(validator).__name__ if validator else "None"
        if vname in ("GenericStepValidator", "None"):
            generic_only.append(t)

        mentioned = t in blob
        if not mentioned:
            untested.append(t)

        if t in UNSUPPORTED_INPUT or t in UNSUPPORTED_OUTPUT:
            unsupported.append(t)
        elif t in PARTIAL_FAMILIES:
            partial.append(t)
        else:
            supported.append(t)

    report = {
        "summary": {
            "handlers_in_list": len(handlers),
            "unique_registered_types": len(registered),
            "converters_built": len(converters),
            "converter_types": len(conv_types),
            "empty_type_stubs": empty_stubs,
            "duplicate_type_claims": [
                {"type": a, "later": b, "first": c} for a, b, c in duplicate_claims
            ],
            "missing_structured_parser_count": len(missing_parser),
            "generic_validator_only_count": len(generic_only),
            "untested_count": len(untested),
            "supported_count": len(supported),
            "partial_count": len(partial),
            "unsupported_count": len(unsupported),
        },
        "missing_structured_parser": missing_parser,
        "generic_validator_only": generic_only,
        "untested": untested,
        "supported": supported,
        "partial": partial,
        "unsupported": unsupported,
        "handler_by_type": handler_by_type,
    }

    out = ROOT / "docs" / "step_coverage_audit.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("=== STEP COVERAGE AUDIT ===")
    s = report["summary"]
    for k, v in s.items():
        if k in ("empty_type_stubs", "duplicate_type_claims"):
            print(f"{k}: {len(v) if isinstance(v, list) else v}")
        else:
            print(f"{k}: {v}")
    print("\nEmpty stubs:")
    for x in empty_stubs:
        print(f"  - {x}")
    print("\nDuplicate claims:")
    for d in duplicate_claims:
        print(f"  - {d[0]}: first={d[2]} later={d[1]}")
    print(f"\nMissing parsers ({len(missing_parser)}):")
    for t in missing_parser:
        print(f"  - {t}")
    print(f"\nGeneric-only validators ({len(generic_only)}):")
    for t in generic_only:
        print(f"  - {t}")
    print(f"\nUntested ({len(untested)}):")
    for t in untested:
        print(f"  - {t} -> {handler_by_type.get(t)}")
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
