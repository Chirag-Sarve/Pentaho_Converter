# Pentaho Transformation Coverage Report

Generated from a repository-wide audit of Spoon transformation handlers (parsers, registry, metadata, code generation, validators, tests, migration warnings).

## Architecture check

| Layer | Status |
|-------|--------|
| Registry (`registry_list` → `DEDICATED_CONVERTERS`) | Complete — 1 dedicated converter per handler; fallback last |
| XML parsers (`step_xml.parse_step_metadata`) | Synced for core + big-data + advanced gap types |
| Metadata propagation | Shared `get_converter_metadata` for all dedicated converters |
| Code generation | Handlers delegate to shared `*_converter.py` helpers where applicable |
| Validators | Dedicated / pattern validators for registered families; `GenericStepValidator` catch-all |
| Tests | Category suites + registry completeness + gap-step tests |
| Migration warnings | `# UNSUPPORTED` / `# WARNING` / `preserved.*` for intentional non-ports |

## Cleanup performed in this audit

- Removed empty `_TYPES` stub classes from `extended_handlers` / `advanced_handlers` (JoinRows, MergeRows, ExecSQL, REST, HTTP, FuzzyMatch, RegexEval, JS, UDJE).
- Removed duplicate `FormulaHandler` registration (canonical: `ScriptingFormulaHandler`).
- Reused `StringOperationsHandler` for `RegexReplace` (no context mutation).
- Synced `_STRUCTURED_STEP_TYPES` + `parse_step_metadata` for Avro/Mongo/Parquet/ORC/Delta/CSV/Hadoop, Rank/Top/Limit, SystemInfo, ReplaceNull, Splunk, Access/S3/SQL/Property outputs.
- Extended validator aliases (`addconstant*`, `setfieldvalue*`, `stringscut`, `microsoftexcelinput`, `regexreplace`).
- Added `tests/test_registry_coverage.py` and `tests/test_missing_gap_steps.py`.
- Added `scripts/audit_step_coverage.py` for re-running the inventory.

## Live inventory (post-audit)

| Metric | Count |
|--------|------:|
| Handlers in registry list | 209 |
| Unique registered type strings (incl. aliases) | 424 |
| Dedicated converters built | 209 |
| Empty `_TYPES` stubs | **0** |
| Duplicate type claims | **0** |
| Types missing structured parser | **0** |
| Types with only Generic validator | **0** |
| Classified supported | 312 |
| Classified partial | 92 |
| Classified intentionally unsupported | 20 |
| Alias strings not literally quoted in tests | 147 |

The 147 “untested” strings are nearly all Spoon aliases of families already covered by canonical tests (e.g. `postgresbulkloader` vs `PostgreSQLBulkLoader`, `microsoftexcelinput` vs `ExcelInput`). Canonical gap types (Parquet/ORC/Hadoop/Top/RegexReplace/Splunk/ReplaceNull) now have dedicated tests.

Re-run `python scripts/audit_step_coverage.py` for the live inventory (`docs/step_coverage_audit.json`).

### Fully supported (Spark/Databricks-native mapping)

Core ETL and Databricks-friendly formats, including (non-exhaustive):

- Generate: `RowGenerator`, `DataGrid`, `Constant` / `AddConstants`
- Field/string: `SelectValues`, `SetValueConstant`, `SetValueField`, `ConcatFields`, `AddXML`, `StringOperations`, `StringCut`, `ReplaceInString`, `RegexReplace`, `ReplaceNull`, `IfNull`
- Calc: `Calculator`, `Formula`, `Checksum`, `NumberRange`, `FieldsChangeSequence`
- Reshape: `RowNormaliser`/`Denormaliser`, `Flattener`, `SplitFieldToRows`, `SplitFields`
- Sort/agg: `SortRows`, `GroupBy`, `MemoryGroupBy`, `Unique` / hash-set variants, `Rank`, `Top`/`Limit`
- Joins: `MergeJoin`, `JoinRows`, `MergeRows`, `MultiwayMergeJoin`, `SortedMerge`, `XMLJoin`
- Lookups: `StreamLookup`, `DatabaseLookup`, `DBProc`, `DBJoin`, `DynamicSQLRow`, exists/locked/WS-available checks, HTTP/REST (approx)
- Core I/O: `TableInput`/`Output`, `CsvInput`, `TextFileInput`/`Output` (+ legacy), `ExcelInput`/`Output`/`Writer`, `JsonInput`/`Output`, `XmlInput`/`Output`
- Big data I/O: `Parquet`/`ORC`/`Avro`/`Delta` read-write, `MongoDB` I/O, `HadoopFileInput`
- Flow (portable): `Abort`, `Append`, `Dummy`, `DetectEmptyStream`, `IdentifyLastRow`, `SwitchCase`, `PrioritizeStreams`
- Mapping / job result / variables / validators / statistics / crypto (approx with docs)
- Bulk loaders → Delta `saveAsTable` + UNSUPPORTED native-loader notes

### Partially supported (honest approximation + warnings)

| Family | Behavior |
|--------|----------|
| Scripting JS / Rules / UDJC / UDJE / Experimental Script | Preserve script; `manual_required` / WARNING |
| JavaFilter, MetaInject, Job/Trans Executor, SingleThreader, BlockUntil | LIMITATION stubs + metadata |
| Get Slave Sequence | UNSUPPORTED Carte; local sequence approx |
| XSLT | `lxml` UDF + Saxon/field caveats |
| HTTP/REST/SOAP, FuzzyMatch | Approx clients; preserve config |
| Kafka / MQTT | Structured Streaming where possible; MQTT often WARNING |
| Socket Reader/Writer | WARNING / UNSUPPORTED |
| Utility process/mail/ssh/syslog/EDI | UNSUPPORTED on Databricks + preserved config |
| Salesforce / LDAP / S3 / Access / Autodoc | Connector-dependent PARTIAL |
| SFTP Put / Pentaho Server session/endpoint | Partial HTTP/API stubs |
| Bulk vendor loaders | Delta rewrite; native loader UNSUPPORTED |
| Dimension / Combination Lookup / SynchronizeAfterMerge | SCD/MERGE approximations |

### Intentionally unsupported (no Databricks equivalent)

Handlers emit empty/passthrough frames, `# UNSUPPORTED`, and `preserved.*` metadata. Validators require those markers.

**Inputs:** `GetRepositoryNames`, `MailInput` / Email Messages Input, `CubeInput`, `DeserializeFromFile`, `MondrianInput` / `OlapInput` / XMLA, `SapInput`

**Outputs:** `CubeOutput`, `SerializeToFile`, `PentahoReportingOutput` / Report Export

**Other:** `SplunkInput`/`SplunkOutput` (Transform category; no connector), `JmsConsumer`/`JmsProducer` (no Databricks JMS streaming)

## Registry health (achieved)

| Metric | Result |
|--------|--------|
| Empty `_TYPES` stubs in `_all_handlers` | **0** |
| Duplicate Formula registrations | **0** (`ScriptingFormulaHandler` only) |
| Registered types without structured parser | **0** |
| Registered types with only `GenericStepValidator` | **0** |
| Canonical gap tests | `tests/test_missing_gap_steps.py` |
| Completeness guard | `tests/test_registry_coverage.py` |

## How to re-audit

```bash
python scripts/audit_step_coverage.py
python -m unittest tests.test_registry_coverage tests.test_missing_gap_steps -v
```

Live machine-readable inventory: `docs/step_coverage_audit.json`.
