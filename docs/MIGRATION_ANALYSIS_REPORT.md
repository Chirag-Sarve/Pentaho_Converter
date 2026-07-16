# Pentaho → PySpark Migration Analysis Report

**Status:** Analysis complete — awaiting approval before PySpark generation  
**Analyzed:** 2026-07-15  
**Scope:** All `.kjb` / `.ktr` files under this repository (12 files)

---

## 1. Executive summary

| Metric | Count |
|--------|------:|
| Jobs (`.kjb`) | 1 |
| Transformations (`.ktr`) | 11 |
| Root jobs | 1 (`Master`) |
| Child jobs | 0 |
| Transformations executed by root job | 2 |
| Orphan / unwired transformations | 9 |
| Unique step types | 22 |
| Named DB connections (logical) | 2 (`MyDB`, `WH`) |
| Connection XML definitions present | 0 |
| Declared parameters | 4 |
| Variables referenced | 2 (both built-in path variables) |

**Primary migration candidate (production sample graph):**

```
Master.kjb
  ├── Customer_Load.ktr
  └── Sales_Load.ktr

Complex_Business_Logic.ktr   ← present under samples/ but NOT referenced by Master
```

**Secondary set (`tests/samples/`):** 8 fixture transformations used to exercise the converter — not part of the job dependency graph.

---

## 2. Inventory of parsed files

### 2.1 Jobs

| File | Name | Entries | Job hops | Calls transformations | Calls jobs |
|------|------|--------:|--------:|------------------------|------------|
| `samples/Jobs/Master.kjb` | Master | 4 | 3 | Customer_Load, Sales_Load | — |

### 2.2 Transformations — sample scope (`samples/`)

| File | Name | Steps | Trans hops | Wired to Master? |
|------|------|------:|-----------:|------------------|
| `samples/Transformations/Customer_Load.ktr` | Customer_Load | 4 | 3 | Yes |
| `samples/Transformations/Sales_Load.ktr` | Sales_Load | 3 |: 2 | Yes |
| `samples/Transformations/Complex_Business_Logic.ktr` | Complex_Business_Logic | 16 | 15 | **No (orphan)** |

### 2.3 Transformations — test fixtures (`tests/samples/`)

| File | Name | Steps | Trans hops | Purpose |
|------|------|------:|-----------:|---------|
| `Abort_Pass_Fail.ktr` | Abort_Pass_Fail | 4 | 3 | Abort / Filter branch |
| `Calculator_Variants.ktr` | Calculator_Variants | 7 | 6 | Calculator edge cases |
| `Calc_SelectValues_Chain.ktr` | Calc_SelectValues_Chain | 4 | 3 | Calc ↔ SelectValues chain |
| `Generator_Audit_Workflow.ktr` | Generator_Audit_Workflow | 8 | 7 | Multi-step audit flow |
| `SelectValues_Variants.ktr` | SelectValues_Variants | 7 | 6 | SelectValues variants |
| `Step_Converter_Test.ktr` | Step_Converter_Test | 5 | 4 | Basic converter smoke |
| `TextFile_Write_Read.ktr` | TextFile_Write_Read | 3 | 2 | File write → read |
| `ValueMapper_Runtime.ktr` | ValueMapper_Runtime | 2 | 1 | ValueMapper |

---

## 3. Dependency graph

### 3.1 Job → transformation (execution lineage)

```
                    ┌─────────────┐
                    │   Master    │  (ROOT JOB)
                    │ Master.kjb  │
                    └──────┬──────┘
           Start ──────────┤
                           ▼
                  ┌────────────────┐
                  │ Customer Load  │──► Customer_Load.ktr
                  └────────┬───────┘
                           ▼
                  ┌────────────────┐
                  │  Sales Load    │──► Sales_Load.ktr
                  └────────┬───────┘
                           ▼
                      Success
```

**Edges:**

| From | To | Edge type | Via entry |
|------|----|-----------|-----------|
| `Master.kjb` | `Customer_Load.ktr` | executes_trans | Customer Load |
| `Master.kjb` | `Sales_Load.ktr` | executes_trans | Sales Load |

**Child jobs:** none  
**Root job:** `Master` (never referenced by another job)

### 3.2 Orphans (no inbound job edge)

- `Complex_Business_Logic.ktr` (sample — migrate separately or wire into Master)
- All 8 `tests/samples/*.ktr` fixtures

---

## 4. Root job detail — `Master`

### Entries

| Entry | Type | Filename / target |
|-------|------|-------------------|
| Start | SPECIAL | — |
| Customer Load | TRANS | `${Internal.Job.Filename.Directory}/../Transformations/Customer_Load.ktr` |
| Sales Load | TRANS | `${Internal.Job.Filename.Directory}/../Transformations/Sales_Load.ktr` |
| Success | SUCCESS | — |

### Job hops

| From | To | Enabled |
|------|----|---------|
| Start | Customer Load | Y |
| Customer Load | Sales Load | Y |
| Sales Load | Success | Y |

**Execution order (topological):** Start → Customer Load → Sales Load → Success  
**Semantics:** strict sequential success path; no unconditional / error hops defined.

---

## 5. Transformation hop graphs (sample scope)

### 5.1 `Customer_Load`

```
Table input → Filter rows → Select values → Table output
```

| From | To |
|------|----|
| Table input | Filter rows |
| Filter rows | Select values |
| Select values | Table output |

### 5.2 `Sales_Load`

```
CSV file input → Sort rows → Group by
```

| From | To |
|------|----|
| CSV file input | Sort rows |
| Sort rows | Group by |

**Gap:** no sink step (no TableOutput / TextFileOutput). Aggregated result is not persisted in the KTR.

### 5.3 `Complex_Business_Logic` (orphan sample)

```
Table input orders → Filter → ReplaceNull → Constant → Calculator → Formula
  → StringOperations → ReplaceInString → SortRows ─┐
CSV product prices ────────────────────────────────┤→ MergeJoin → StreamLookup → GroupBy ─┬→ Table output summary
Region reference grid ─────────────────────────────┘                                      └→ Text file audit log
```

Parallel sources: CSV prices + DataGrid regions join the main order stream before aggregation; GroupBy fans out to DB + file sinks.

---

## 6. Inputs

### Sample / production-like

| Source transformation | Kind | Detail | Connection |
|----------------------|------|--------|------------|
| Customer_Load | SQL table | `SELECT customer_id, customer_name, status FROM customers` | MyDB |
| Sales_Load | File | `/data/sales.csv` (CsvInput, `,`, header) | — |
| Complex_Business_Logic | SQL table | `staging.customer_orders` (filtered by `order_date >= '2026-07-01'`) | WH |
| Complex_Business_Logic | File | `/data/inbound/product_prices.csv` | — |
| Complex_Business_Logic | DataGrid | Region reference (NA/EU/APAC/LATAM/MEA) | — |

### Test fixtures

| Source | Kind | Detail |
|--------|------|--------|
| Most test KTRs | DataGrid | Synthetic rows |
| TextFile_Write_Read | TextFileInput | `${Internal.Transformation.Filename.Directory}/runtime_orders.csv` |
| TextFile_Write_Read | TextFileOutput (upstream) | `C:\pentaho\data\runtime_orders` |

---

## 7. Outputs (tables / files)

| Source transformation | Kind | Target | Notes |
|----------------------|------|--------|-------|
| Customer_Load | Table | `analytics.dim_customer` | TableOutput; connection not set on step |
| Complex_Business_Logic | Table | `analytics.fact_order_summary` | TableOutput; connection not set on step |
| Complex_Business_Logic | File | `/data/outbound/order_summary_audit.txt` | TextFileOutput |
| Sales_Load | — | **None** | Ends at GroupBy |
| TextFile_Write_Read | File | `C:\pentaho\data\runtime_orders` | Test fixture write |

---

## 8. Variables

| Variable | Where used | Category |
|----------|------------|----------|
| `${Internal.Job.Filename.Directory}` | Master.kjb TRANS filenames | Built-in (job path resolution) |
| `${Internal.Transformation.Filename.Directory}` | TextFile_Write_Read ReadCSV | Built-in (trans path resolution) |

No custom kettle variables (e.g. `${BATCH_DATE}`) are referenced in step XML despite parameters being declared.

---

## 9. Parameters

| Parameter | Default | Declared in | Used in SQL/steps? |
|-----------|---------|-------------|--------------------|
| `BATCH_DATE` | `2026-01-01` | Customer_Load | **No** (not referenced) |
| `BATCH_DATE` | `2026-07-01` | Complex_Business_Logic | **No** (date hard-coded in SQL / Constant) |
| `MIN_ORDER_AMOUNT` | `100` | Complex_Business_Logic | **No** (filter uses literal `100`) |
| `DISCOUNT_RATE` | `0.05` | Complex_Business_Logic | **No** (Formula uses literal `0.05`) |

**Migration note:** parameters exist for configuration but logic currently uses literals — PySpark generation should either bind parameters or preserve literals and flag inconsistency.

---

## 10. Database connections

| Logical name | Used by | Hosted definition in KJB/KTR? | Usage |
|--------------|---------|-------------------------------|--------|
| `MyDB` | Customer_Load (TableInput) | **Missing** | Source `customers` |
| `WH` | Complex_Business_Logic (TableInput) | **Missing** | Source `staging.customer_orders` |

No `<connection>` blocks with server/database/port/credentials were found in any parsed file. Connections are **name-only references** and must be supplied via shared kettle DB cache, spoon shared.xml, or external mapping during migration.

TableOutput steps do not declare a `<connection>` element in the sample XML.

---

## 11. Step-type inventory (all files)

| Step type | Occurrences (files containing type) |
|-----------|------------------------------------:|
| DataGrid | 9 |
| Calculator | 5 |
| FilterRows | 5 |
| SelectValues | 4 |
| Constant | 3 |
| CsvInput | 2 |
| GroupBy | 2 |
| SortRows | 2 |
| StringOperations | 2 |
| TableInput | 2 |
| TableOutput | 2 |
| TextFileOutput | 2 |
| Abort | 2 |
| ValueMapper | 2 |
| Formula | 1 |
| MergeJoin | 1 |
| ReplaceInString | 1 |
| ReplaceNull | 1 |
| StreamLookup | 1 |
| Dummy | 1 |
| SystemInfo | 1 |
| TextFileInput | 1 |

All observed types are covered by the existing converter coverage matrix (`docs/TRANSFORMATION_COVERAGE_REPORT.md`) as supported/partial Spark-mappable families.

---

## 12. Migration findings & risks

| ID | Severity | Finding | Impact |
|----|----------|---------|--------|
| F1 | High | No physical JDBC connection definitions in artifacts | Requires external connection map before generated PySpark can run |
| F2 | High | `Sales_Load` has no output sink | Need to confirm intended target or treat as incomplete sample |
| F3 | Medium | `Complex_Business_Logic` not on Master | Decide: include in migration batch, attach to job, or defer |
| F4 | Medium | Parameters declared but unused (literals elsewhere) | Config drift; Spark jobs should parameterize dates/amounts/rates |
| F5 | Medium | TableOutput steps omit connection name | Infer from job context / convention (`analytics` schema → warehouse) |
| F6 | Low | Job uses only sequential success hops | Maps cleanly to a linear orchestration script / Databricks Workflow |
| F7 | Low | Test fixtures mix Windows absolute paths | Out of scope for production sample migration unless generating tests |
| F8 | Info | Calculator `discount_amount` multiplies `line_total * unit_price` | Likely business-logic bug vs intended `line_total * DISCOUNT_RATE` — verify before porting |

---

## 13. Recommended migration scope (pending approval)

### Proposed Phase 1 — Master graph

1. Orchestrator for `Master` (Start → Customer_Load → Sales_Load → Success)
2. `Customer_Load.ktr` → PySpark
3. `Sales_Load.ktr` → PySpark (**clarify output sink first**)

### Proposed Phase 2 — Extended sample

4. `Complex_Business_Logic.ktr` → PySpark (richest business logic coverage)

### Explicitly deferred unless requested

5. All `tests/samples/*.ktr` (converter fixtures, not ETL product)

---

## 14. Approval checklist

Please confirm before any PySpark generation:

- [ ] Scope = Phase 1 only / Phase 1+2 / all files including tests
- [ ] Connection mapping for `MyDB` and `WH` (and TableOutput targets)
- [ ] Intended sink for `Sales_Load` aggregates
- [ ] Whether to wire/include `Complex_Business_Logic`
- [ ] Whether to honor parameters (`BATCH_DATE`, `MIN_ORDER_AMOUNT`, `DISCOUNT_RATE`) instead of literals
- [ ] Whether to preserve or fix Calculator discount formula (F8)

---

*No PySpark code was generated. Awaiting approval.*
