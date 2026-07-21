# Supported Pentaho Job Entries

This document tracks Job Entry (`.kjb`) support in the Pentaho → PySpark / Databricks converter.

## General category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Start** | `SPECIAL` + `start=Y` | Supported | Graph entry point. Only `is_start` entries start execution. Spoon scheduler fields (`schedulerType`, intervals, …) are preserved on the entry and logged as unsupported — use Databricks Job schedules. |
| **Dummy** | `DUMMY` or non-start `SPECIAL` | Supported | Always succeeds (pass-through). Default hop behaviour is unconditional when the XML flag is omitted. |
| **Job** | `JOB` | Supported | Resolves child `.kjb`, generates `jobs.<stem>`, runs synchronously. Passes variables/parameters (`pass_all_parameters`, explicit `<parameter>` list). Shares parent/root variable scopes for nested Set Variables. `exec_per_row=Y` and `wait_until_finished=N` emit warnings. |
| **Set Variables** | `SET_VARIABLES` | Supported | Fields + optional properties `filename`. Scopes: `JVM`, `CURRENT_JOB`, `PARENT_JOB`, `ROOT_JOB`. Supports `replace` / `replacevars`, `${var}` / `%%var%%` substitution, and `${n}+1` increment. `PARENT_JOB` fails (like PDI) when the current job has no parent. |
| **Success** | `SUCCESS` | Supported | Marks successful terminal completion; preferred job result when reached. |
| **Transformation** | `TRANS` | Supported | Inlines referenced `.ktr` as PySpark `run_*` and executes via the job engine. Missing KTR → conversion TODO + runtime failure. Parameter pass-through matches JOB. |

## Mail category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Mail** | `MAIL` | Supported | Driver-side SMTP via `smtplib` / `email` (Databricks-compatible). Parses server, port, To/Cc/Bcc, From (`replyto` / `sender_address`), Reply-To, auth, TLS/SSL, subject, body, HTML, priority, contact fields, `filetypes`, `embeddedimages`. Variable substitution on all string attributes. Set `MAIL_ENABLED=N` to skip send (success + warning) for dry-runs; default is to send. |
| **Get Mails (POP3/IMAP)** | `GET_POP` | Supported | Retrieves via `poplib` / `imaplib`. Supports protocol, host, SSL/`sslport`, credentials, folder, retrieve mode, delete, save message/attachments, basic sender/subject/body filters, IMAP list mode (unread/read/…), after-get delete/move. Creates local output folders when configured. |
| **Mail Validator** | `MAIL_VALIDATOR` | Supported | Validates `emailAddress` (space-separated list, fail-fast). Syntax check with relaxed/strict patterns. Returns success/failure consistent with PDI hop evaluation (`evaluates=true`). |

### Mail unsupported / partial

| Feature | Handling |
|---------|----------|
| PDI `Encrypted …` passwords | Warning; cannot decrypt kettle cipher — use plain/`${VAR}` secrets |
| `MAIL` `include_files` / result-file filetype attach | Uses runtime `result_filenames` when present; empty list → warning |
| `MAIL` `zip_files` | Warning; attachments sent uncompressed when present |
| `MAIL` HTML `embeddedimages` CID inlining | Warning; existing image files may be attached instead |
| `MAIL` `include_date` body injection | Warning; attribute preserved |
| `GET_POP` proxy (`useproxy`) | Warning; connects directly |
| `GET_POP` `includesubfolders` | Warning; selected folder only |
| `GET_POP` received-date search | Warning; not applied |
| `GET_POP` POP3 “unread only” | Warning; retrieves all (POP3 has no unread flag) |
| `MAIL_VALIDATOR` `smtpCheck=Y` (SMTP/MX probe) | Warning; structural validation only (requires `emailSender` when enabled, like PDI) |

## File Management category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Add filenames to result** | `ADD_RESULT_FILENAMES` | Supported | Wildcard / recursive listing into `JobRuntime.result_filenames`. `delete_all_before` clears the list first. |
| **Compare folders** | `FOLDERS_COMPARE` | Supported | Recursive, content/size/timestamp modes via `compareonly` / `compare_filesize` / `compare_filecontent`, wildcards, missing-file detection. |
| **Convert file between Windows and Unix** | `DOS_UNIX_CONVERTER` | Supported | DOS→Unix / Unix→DOS (`ConversionType` 1/2 or guess). Byte-level newline conversion preserves encoding. |
| **Copy files** | `COPY_FILES` | Supported | Flat attrs or `<fields>` rows; overwrite, wildcard, recursive, create destination, remove source, add to result list. |
| **Create a folder** | `CREATE_FOLDER` | Supported | Nested dirs; `fail_of_folder_exists`; DBFS/`dbutils` fallback. |
| **Create file** | `CREATE_FILE` | Supported | Empty file; parent dirs; `fail_if_file_exists`; optional add to result list. |
| **Delete file** | `DELETE_FILE` | Supported | Existence check via `fail_if_file_not_exists`. |
| **Delete filenames from result** | `DELETE_RESULT_FILENAMES` | Supported | Clear all or wildcard/exclude filter on the result list. |
| **Delete files** | `DELETE_FILES` | Supported | Multi-path `<fields>` (`name`/`filemask`), wildcard, recursive, `arg_from_previous`. |
| **Delete folders** | `DELETE_FOLDERS`, `DELETE_FOLDER` | Supported | Recursive `shutil.rmtree`; Retail alias `DELETE_FOLDER` registered. |
| **File compare** | `FILE_COMPARE` | Supported | Binary compare (`filecmp`) + MD5 confirmation. |
| **HTTP** | `HTTP` | Supported | GET/POST (upload file), basic auth, headers, proxy, download to file, append, SSL verify toggle. Uses `requests` when available, else `urllib`. |
| **Move files** | `MOVE_FILES` | Supported | Overwrite / unique-name / fail / skip via `iffileexists`; wildcard; recursive; create destination. |
| **Process result filenames** | `COPY_MOVE_RESULT_FILENAMES` | Supported | Copy/move/delete accumulated result filenames; wildcard filters; overwrite; update result list. |
| **Unzip file** | `UNZIP`, `UNZIP_FILE` | Supported | Destination folder, overwrite, wildcards, root zip folder, password (ZipCrypto), after-unzip delete/move. Retail alias `UNZIP_FILE`. |
| **Wait for file** | `WAIT_FOR_FILE` | Supported | Timeout, poll interval, `successOnTimeout`, optional size-stability check, wildcard filename, add to result. |
| **Write to file** | `WRITE_TO_FILE` | Supported | Append/overwrite, encoding, create parent folder, variable substitution in content. |
| **Zip file** | `ZIP_FILE` | Supported | Multi-file / recursive, compression rate (stored/deflated), date/time stamp, if-exists modes, wildcards. |

### File Management unsupported / partial

| Feature | Handling |
|---------|----------|
| PDI VFS / named-cluster remote URLs | Local/`resolve_data_path` only; remote VFS not emulated |
| Zip `compressionrate` best-speed / best-compression | Approximated with `ZIP_DEFLATED` + warning |
| Zip/Unzip AES encryption beyond ZipCrypto password | Warning / failure from stdlib `zipfile` |
| `MOVE_FILES` / `COPY_FILES` simulate mode | Warning; operation still executed |
| `HTTP` `run_every_row=Y` | Warning; runs once |
| `HTTP` ignoreSsl | Warning; verification disabled when requested |
| `DELETE_FOLDERS` `limit_folders` success condition | Warning; not applied |
| Folder compare exclude patterns beyond `wildcard` | Only include wildcard supported |
| Copy empty folders only (`copy_empty_folders`) | Not specially handled; file copy is primary |

## Conditions category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Check DB Connections** | `CHECK_DB_CONNECTIONS` | Supported | Parses job-level `<connection>` metas + entry `<connections>` list. Probes via Spark JDBC or TCP host:port; optional per-connection wait. |
| **Check Files Locked** | `CHECK_FILES_LOCKED` | Supported | Best-effort exclusive lock probe (`msvcrt` / `fcntl`). Success when no files are locked. |
| **Check if Folder is Empty** | `FOLDER_IS_EMPTY` | Supported | Optional recursive + wildcard filter. |
| **Check Webservice Availability** | `WEBSERVICE_AVAILABLE` | Supported | HTTP/HTTPS GET with connect/read timeout; `requests` preferred, `urllib` fallback. |
| **Checks if Files Exist** | `FILES_EXIST` | Supported | Multiple paths via `<fields>`; all must exist (`_fs_exists` / `_SUCCESS`). |
| **Columns Exist in a Table** | `COLUMNS_EXIST` | Supported | Spark `spark.table` schema check; case-insensitive by default. |
| **Evaluate Files Metrics** | `EVAL_FILES_METRICS` | Supported | Size or count metrics with numeric comparisons; scale bytes/KB/MB/GB; wildcards. |
| **Evaluate Rows Number in a Table** | `EVAL_TABLE_CONTENT` | Supported | `COUNT` via Spark table or custom SQL; comparison operators (`rows_count_*`). |
| **File Exists** | `FILE_EXISTS` | Supported | Local/Volumes paths with `resolve_data_path` + `_SUCCESS` marker. |
| **Simple Evaluation** | `SIMPLE_EVAL` | Supported | String / number / datetime / boolean operators; between; list; regex; `successwhenvarset`. Legacy aliases (`SMALLER`, …). |
| **Table Exists** | `TABLE_EXISTS` | Supported | Spark `catalog.tableExists` with catalog/schema from config. |
| **Wait For** | `DELAY`, `WAIT_FOR` | Supported | Sleep with `scaletime` (seconds/minutes/hours). |
| **Wait for SQL** | `WAIT_FOR_SQL` | Supported | Poll row-count condition with timeout / cycle / `success_on_timeout`. |

### Conditions unsupported / partial

| Feature | Handling |
|---------|----------|
| Full JDBC driver auth beyond Spark JDBC / TCP probe | Warning; prefers Spark session + JDBC options |
| `CHECK_FILES_LOCKED` on filesystems without advisory locks | Best-effort; may report unlocked |
| `EVAL_TABLE_CONTENT` `add_rows_result` | Warning; rows not pushed to result set |
| `EVAL_FILES_METRICS` source=previous result rows | Approximated via `result_filenames` |
| PDI VFS paths for file conditions | Local/`resolve_data_path` only |
| Datetime Simple Eval exotic masks | Falls back to ISO parse + warning |

## Scripting category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Shell** | `SHELL` | Supported | `insertScript` / filename + `<argument>` list, `%VAR%` + `${VAR}`, working directory, stdout/stderr/exit code via `subprocess`. Simple `echo`/`printf` run in Python for Databricks portability. |
| **SQL** | `SQL` | Supported | Connection name, inline or file SQL, `useVariableSubstitution`, `sendOneStatement`, multi-statement split. Executes via Spark `spark.sql`. Comment-only scripts succeed. |
| **JavaScript** | `EVAL` | Supported | Rhino EVAL → boolean. Translates simple literals / comparisons / `getVariable("X")` to Python. Complex JS → TODO + warning with original script preserved; entry fails pending manual conversion. |

### Scripting unsupported / partial

| Feature | Handling |
|---------|----------|
| `SHELL` `arg_from_previous` / `exec_per_row` | Warning; runs once |
| `SHELL` `set_logfile` / log file rotation | Warning; output to job logger/stdout |
| Windows-specific shell (cmd, `dir`, drive letters) | Warning; may fail on Databricks Linux |
| Full Rhino JS APIs (`Packages`, loops, functions) | TODO + failure; original script logged |
| SQL JDBC dialect / stored procedures | Spark SQL path; vendor SQL warned |
| SQL transaction commit/rollback controls | Not exposed; Spark session auto-commit semantics |

## Bulk Loading category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Bulk load from MySQL into file** | `MYSQL_BULK_FILE` | Supported | Spark JDBC read → CSV write. Supports schema/table, column list, separator/enclosed, limit, `iffileexists`, add-to-result. |
| **Bulk load into MySQL** | `MYSQL_BULK_LOAD` | Supported | Spark CSV read → JDBC write. `replacedata`→overwrite/append, `ignorelines`, column attribute list. `LOAD DATA LOCAL INFILE` replaced with Spark path + warning. |
| **Bulk load into MSSQL** | `MSSQL_BULK_LOAD` | Supported | Spark CSV read → JDBC write. `truncate`, `batchsize`, field terminator. bcp-only flags (`tablock`, `keepidentity`, `firetriggers`, format file, …) emit warnings. |

### Bulk Loading unsupported / partial

| Feature | Handling |
|---------|----------|
| `mysqldump` / `mysql` / `bcp` CLI utilities | Not used — Spark JDBC preferred for Databricks |
| MySQL `LOAD DATA LOCAL INFILE` | Warning; Spark JDBC write instead |
| MySQL `HIGH_PRIORITY` / `DUMPFILE` binary export | Warning; text CSV export |
| MSSQL format file, fire triggers, check constraints, keep identity/nulls, tablock, error file | Warning; not applied |
| MSSQL code pages other than UTF-8 reader | Warning |
| Exact `ignorelines` / `startfile`/`endfile` semantics | Approximated via row-index filter |

## XML category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Check if XML file is well formed** | `XML_WELL_FORMED` | Supported | Uses `xml.etree.ElementTree`. Multi-file via `<fields>` (`source_filefolder` + `wildcard`), `include_subfolders`, `arg_from_previous` (via `result_filenames`). Success conditions: no errors / at-least-N well-formed / bad-formed less than N. Result-filename filters: all / well-formed only / bad-formed only. |
| **DTD Validator** | `DTD_VALIDATOR` | Supported | Requires **lxml**. Validates against external `dtdfilename` or internal DTD (`dtdintern=Y`). Logs detailed validation errors. |
| **XSD Validator** | `XSD_VALIDATOR` | Supported | Requires **lxml**. Validates `xmlfilename` against `xsdfilename`. `allowExternalEntities=Y` is logged as a warning; entity resolution stays restricted. |
| **XSL Transformation** | `XSLT` | Supported | Requires **lxml**. Transforms `xmlfilename` with `xslfilename` → `outputfilename`. Supports XSL parameters (`<parameters>`), output properties (`encoding`/`method`/`indent`), `iffileexists` (0=unique, 1=skip, 2=fail, else overwrite), `addfiletoresult`. `xsltfactory` (JAXP/SAXON) warned — always uses libxslt via lxml. |

### XML unsupported / partial

| Feature | Handling |
|---------|----------|
| DTD / XSD / XSL without `lxml` installed | Failure with install guidance (`pip` / Databricks cluster library) |
| SAXON XSLT factory | Warning; lxml/libxslt used instead |
| Full JAXP output-property set | Partial — `method`, `encoding`, `indent` applied; others warned |
| `filenamesfromprevious` on XSLT (row triples) | Approximated; warning + configured paths / result filenames as XML input |
| External entity / network DTD fetch | Disabled (`no_network`) for Databricks safety |
| Namespace-aware XSD advanced options beyond schema file | Schema-driven via lxml; no separate namespace UI attrs in PDI job entry |

## Utility category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Abort Job** | `ABORT` | Supported | Terminates job with `JobExecutionError`; message is variable-substituted. Non-standard `errorcode` attrs are appended and warned. |
| **Display MsgBox Info** | `MSGBOX_INFO` | Supported | No GUI on Databricks — logs title/`bodymessage` and emits a warning. Always succeeds after logging. |
| **Write to Log** | `WRITE_TO_LOG` | Supported | Maps PDI log levels to Python logging; supports `logmessage`, `loglevel`, `logsubject`, header line. |
| **Wait for SQL** | `WAIT_FOR_SQL` | Supported | Shared Conditions implementation — polls row-count / custom SQL with timeout / `success_on_timeout`. |
| **Ping a Host** | `PING` | Supported | DNS + TCP port probe (no OS `ping`/ICMP). `classicPing` / `bothPings` warned. |
| **Telnet a Host** | `TELNET` | Supported | TCP connect probe (`hostname`/`port`/`timeout`). Full telnet session/credentials not in PDI XML — warned. |
| **Send Information Using Syslog** | `SYSLOG` | Supported | UDP syslog via sockets (`facility`/`priority`/`message`/`addTimestamp`/`addHostname`). |
| **Send Nagios Passive Check** | `SEND_NAGIOS_PASSIVE_CHECK` | Supported | Best-effort NSCA-style TCP payload. TripleDES warned; XOR is simple obfuscation only. |
| **Send SNMP Trap** | `SNMP_TRAP` | Partial | Uses `pysnmp` when installed; otherwise fails with clear TODO/install guidance. SNMPv3 partially warned. |
| **Truncate Tables** | `TRUNCATE_TABLES` | Supported | Spark `TRUNCATE TABLE` (DELETE fallback). Parses `<fields>` table/schema + connection name. |
| **HL7 MLLP Input** | `HL7MLLPInput` | Partial | Socket MLLP listener; stores raw message / type / version variables. No HAPI validation; inbound listeners uncommon on Databricks. |
| **HL7 MLLP Acknowledge** | `HL7MLLPAcknowledge` | Partial | Builds minimal AA ACK and sends over TCP (approximates PDI socket-cache). |

### Utility unsupported / partial

| Feature | Handling |
|---------|----------|
| MsgBox GUI dialog | Warning; structured log only |
| OS ICMP `ping` | Avoided; DNS/TCP probe |
| Full telnet protocol / login scripts | Not in PDI job XML; TCP probe only |
| Nagios TripleDES / wire-perfect NSCA | Warning; plaintext or simple XOR |
| SNMP without `pysnmp` / full SNMPv3 | Failure + install guidance / warnings |
| HL7 HAPI validation / PDI MLLP socket cache | Raw MLLP framing only |
| Abort numeric exit codes (non-PDI) | Optional custom attr warned |

## Repository category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Check if Connected to Repository** | `CONNECTED_TO_REPOSITORY` | Supported (adapted) | Parses `isspecificrep`/`repname`/`isspecificuser`/`username`. No Pentaho Repository API on Databricks — fails like PDI when disconnected unless `REPOSITORY_CONNECTED=Y` override or synthetic `runtime.repository` / `REPOSITORY_META_*` metadata matches. Config logged/preserved. |
| **Export Repository to XML File** | `EXPORT_REPOSITORY` | Supported (adapted) | Parses repository credentials, `targetfilename`, `export_type`, `directoryPath`, date/time naming, `iffileexists`, `createfolder`, `add_result_filesname`, success thresholds. Serializes local `.kjb`/`.ktr` trees into a converter export XML when `directoryPath` / `REPOSITORY_EXPORT_SOURCE` is available; otherwise fails with guidance, or writes a TODO stub when `EXPORT_REPOSITORY_ALLOW_STUB=Y`. |

### Repository unsupported / partial

| Feature | Handling |
|---------|----------|
| Live Pentaho / DI Repository session | Not available — warnings + override/synthetic meta |
| Native Spoon repository XML export format | Approximated manifest XML (paths/metadata), not byte-identical PDI export |
| `newfolder` repository folder creation | Warning; ignored |
| Export error thresholds (`success_condition` / `nr_errors_less_than`) | Warning; not applied to local serialization |
| Repository password at runtime | Not used; prefer `${VAR}`; warning if plaintext present |

## File Transfer category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Get a File with FTP** | `FTP` | Supported | `ftplib.FTP` — host/port/user/pass, remote dir, wildcard, binary/ASCII, passive/active, timeout, only_new, remove, add-to-result, success conditions. Proxies/SOCKS warned (direct connect). |
| **Put a File with FTP** | `FTP_PUT` | Supported | Upload local directory + wildcard; remote dir create best-effort; only_new / remove local. |
| **FTP Delete** | `FTP_DELETE` | Supported | Protocol `FTP` / `FTPS` / `SFTP`. Wildcard delete; optional key auth for SFTP. |
| **Get a File with FTPS** | `FTPS_GET` | Supported | `ftplib.FTP_TLS` + `PROT P`. Explicit TLS preferred; implicit warned. |
| **Upload Files to FTPS** | `FTPS_PUT` | Supported | FTPS upload of local files matching wildcard. |
| **Get a File with SFTP** | `SFTP` | Supported | **paramiko** — password or private key, wildcard get, optional remove, create target folder. |
| **Put a File with SFTP** | `SFTPPUT` | Supported | **paramiko** — upload, create remote folder, aftersftpput delete/move, successWhenNoFile. |

### File Transfer unsupported / partial

| Feature | Handling |
|---------|----------|
| HTTP / SOCKS FTP proxies | Warning; direct connect |
| FTPS implicit TLS | Warning; approximated via `FTP_TLS` |
| Client certificate / custom trust stores | Not applied |
| SFTP proxy (HTTP/SOCKS) | Warning; direct TCP |
| Remote move-after-get folder semantics | Best-effort RNFR/RNTO + warning |
| Recursive directory tree sync | Files in listed directory only (not deep recursive sync) |

## File Encryption category

| Entry | Pentaho type(s) | Status | Notes |
|-------|-----------------|--------|--------|
| **Encrypt Files with PGP** | `PGP_ENCRYPT_FILES` | Supported | `python-gnupg` + GnuPG binary (`gpglocation`). Field rows: `source_filefolder`, `userid` (recipient), `destination_filefolder`, `wildcard`, `action_type` (encrypt / sign / sign_and_encrypt). `asciiMode`, overwrite/`iffileexists`, success conditions. Optional `publickeyfile` import. |
| **Decrypt Files with PGP** | `PGP_DECRYPT_FILES` | Supported | Field rows with `passphrase` (prefer `${VAR}` / `GPG_PASSPHRASE`). Optional `privatekeyfile` / `secretkeyfile` import. Integrity via GnuPG MDC. |
| **Verify File Signature with PGP** | `PGP_VERIFY_FILES` | Supported | Embedded or detached (`useDetachedSignature` + `detachedfilename`). Optional public key import. Success only when `valid=True`. |

### File Encryption unsupported / partial

| Feature | Handling |
|---------|----------|
| Move-to-folder after encrypt/decrypt | Attributes preserved; warning — not fully applied |
| Date/time rename formats on outputs | Partial via `iffileexists=unique`; full PDI naming not replicated |
| PGPy-only path (no GnuPG binary) | Not used — requires `python-gnupg` + `gpg` on cluster |
| Passphrase / private keys in plaintext XML | Discouraged; use variables/env/Secrets — values never logged |

## Workflow semantics

| Concern | Behaviour |
|---------|-----------|
| Execution order | BFS/queue from Start following hop evaluation |
| Nested jobs | `JOB` → `jobs.<module>.run` with inherited config/variables |
| Calling transformations | `TRANS` → inlined runner with substituted config |
| Variable inheritance | Child receives parent variables (copy) plus shared parent/root dicts for upward scopes |
| Variable overriding | Child config/parameters override inherited keys; Set Variables `replace=Y/N` respected |
| Exit status | Child exceptions → entry failure → failure/unconditional hops or re-raise |
| Start | Must exist (`is_start=Y`); Dummy SPECIAL is never treated as Start |
| Success | Terminal success when executed successfully |
| Dummy | No-op success; continues via hops |

## Hop evaluation

| Kind | When it fires |
|------|----------------|
| Unconditional | Always (explicit flag, or default from Start/Dummy/SPECIAL) |
| On success | Prior entry succeeded |
| On failure | Prior entry failed |
| Disabled | Never |

## Variable resolution

Lookup order for `${VAR}` / `%%VAR%%`:

1. Current job `runtime.variables`
2. Process environment (`os.environ`) — JVM-style fallback
3. Date patterns `${%yyyy-MM-dd}` etc.

Internal variables seeded per job: `Internal.Job.Name`, `Internal.Job.Filename.Name`, `Internal.Job.Filename.Directory`, `Internal.Entry.Current.Directory`.

## Unsupported / partial (General)

| Feature | Handling |
|---------|----------|
| Start entry scheduling / repeat timers | Warning; not executed |
| `JOB` / `TRANS` `exec_per_row=Y` | Warning; runs once |
| `JOB` / `TRANS` `wait_until_finished=N` | Warning; always synchronous |
| Async / remote slave-server job execution | Not supported |
| Repository-based JOB/TRANS references (no filename) | Conversion TODO if unresolved |
| Full PDI named-parameter UI (distinct from variables) | Approximated via config + variables |

## Related handlers (outside General / Mail / File Management / Conditions / Scripting / Bulk Loading / XML / Utility / Repository / File Transfer / File Encryption)

Unknown types use a failing TODO handler.

## Tests

See `tests/test_general_job_entries.py` for parser, hop, Start/Dummy/Success, Set Variables scopes, TRANS/JOB, and end-to-end General workflow coverage.

See `tests/test_mail_job_entries.py` for Mail / Get Mails / Mail Validator parser coverage, SMTP send (mocked), auth/config failures, variable substitution, and hop semantics.

See `tests/test_file_management_job_entries.py` for File Management parser coverage, filesystem ops, result-filename list, zip/unzip, HTTP (mocked), variable substitution, and success/failure hop behaviour.

See `tests/test_conditions_job_entries.py` for Conditions parser (incl. job connections), Simple Eval operators, file/folder checks, Spark-mocked table/column/SQL waits, webservice, and registration.

See `tests/test_scripting_job_entries.py` for Shell / SQL / JavaScript (EVAL) parser coverage, echo portability, Spark SQL execution, JS translation / TODO path, and hop semantics.

See `tests/test_bulk_loading_job_entries.py` for MySQL/MSSQL bulk export/load parser coverage, Spark JDBC mocked success/failure, bcp-option warnings, and variable substitution.

See `tests/test_xml_job_entries.py` for XML well-formed / DTD / XSD / XSLT parser coverage, success/failure paths, variable substitution, and lxml-gated validators.

See `tests/test_utility_job_entries.py` for Utility entries (Abort, MsgBox, Ping, Telnet, Syslog, Nagios, SNMP, Truncate, HL7, Write to Log) parser coverage and runtime behaviour.

See `tests/test_repository_job_entries.py` for Repository connect-check / export parser coverage, overrides, local serialization, and stub export.

See `tests/test_file_transfer_job_entries.py` for FTP / FTPS / SFTP get-put-delete parser coverage, mocked transfers, auth failures, and variable substitution.

See `tests/test_file_encryption_job_entries.py` for PGP encrypt / decrypt / verify parser coverage, mocked GnuPG success/failure, invalid keys/signatures, and variable substitution.
