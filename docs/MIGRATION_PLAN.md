# Pentaho → Databricks Migration Plan

**Status:** Plan complete — no code generated  
**Based on:** `docs/MIGRATION_ANALYSIS_REPORT.md` (2026-07-15)  
**Constraint:** Design only; await approval before PySpark generation

---

## 0. Migration strategy overview

| Wave | Scope | Orchestration | Priority |
|------|--------|---------------|----------|
| **Wave 1** | `Master` job + `Customer_Load` + `Sales_Load` | Databricks Workflow mirroring Master hops | P0 — production sample graph |
| **Wave 2** | `Complex_Business_Logic` | Standalone job or optional Workflow task | P1 — richest business logic |
| **Wave 3** | `tests/samples/*` (8 KTRs) | Unit/integration tests only | P2 — converter fixtures, not product ETL |

**Orchestration mapping rule:** one Pentaho Job → one Databricks Workflow; each TRANS entry → one task invoking a notebook or Python wheel module; job hops → `depends_on` / sequential task graph.

**Storage mapping defaults (to confirm):**

| Pentaho | Databricks target |
|---------|-------------------|
| JDBC TableInput | Spark SQL / JDBC / Lakehouse table read |
| TableOutput `analytics.*` | Unity Catalog `analytics` schema (Delta) |
| CSV / TextFile paths under `/data/...` | Volume / ADLS path (e.g. `/Volumes/...` or `abfss://...`) |
| DataGrid | Inline `spark.createDataFrame` / seed seed Delta |
| Logical `MyDB`, `WH` | Named Databricks secrets + connection config |

---

## 1. Jobs

### 1.1 Master

| Field | Detail |
|-------|--------|
| **Asset** | `samples/Jobs/Master.kjb` |
| **Purpose** | Root orchestrator that loads customer dimension data first, then runs the sales aggregation pipeline, and exits on success. |
| **Inputs** | None of its own. Indirectly depends on all inputs of `Customer_Load` and `Sales_Load`. Resolves child KTR paths via `${Internal.Job.Filename.Directory}/../Transformations/...`. |
| **Outputs** | Indirect only — outputs produced by child transformations (`analytics.dim_customer`; Sales_Load currently has no sink). Job itself has no writers. |
| **Business logic** | Sequential success-only control flow. No branching, no error hops, no variables set, no SQL. Entries: `Start` (SPECIAL) → `Customer Load` (TRANS) → `Sales Load` (TRANS) → `Success` (SUCCESS). |
| **Dependencies** | **Requires:** `Customer_Load.ktr`, `Sales_Load.ktr`. **Upstream jobs:** none (root). **Child jobs:** none. |
| **Execution order** | 1. Start → 2. Customer Load → 3. Sales Load → 4. Success (strict sequential; Sales must not start until Customer succeeds). |

**Databricks mapping (planned):** Workflow `wf_master` with tasks `customer_load` → `sales_load` → `success_marker` (optional no-op / logging).

---

## 2. Transformations — Wave 1 (wired to Master)

### 2.1 Customer_Load

| Field | Detail |
|-------|--------|
| **Asset** | `samples/Transformations/Customer_Load.ktr` |
| **Purpose** | Extract active customers from source DB and land a slim dimension into the analytics warehouse. |
| **Inputs** | **Table:** `customers` via connection `MyDB`. SQL: `SELECT customer_id, customer_name, status FROM customers`. **Parameter:** `BATCH_DATE` (default `2026-01-01`) — declared but unused. |
| **Outputs** | **Table:** `analytics.dim_customer` (TableOutput; connection attribute missing in XML — infer warehouse / `analytics` catalog). Columns written after SelectValues: `customer_id`, `customer_name`. |
| **Business logic** | 1) Read all customers. 2) Filter `status = 'ACTIVE'`. 3) Project to `customer_id`, `customer_name`. 4) Write to `analytics.dim_customer`. |
| **Dependencies** | **Called by:** `Master` (entry `Customer Load`). **DB:** `MyDB`. **Downstream consumers:** none declared in repo (Sales_Load does not join customers). **May be prerequisite** for any future sales enrichment that needs dim_customer. |
| **Execution order** | **Job level:** first TRANS after Start. **Step level:** `Table input` → `Filter rows` → `Select values` → `Table output`. |

### 2.2 Sales_Load

| Field | Detail |
|-------|--------|
| **Asset** | `samples/Transformations/Sales_Load.ktr` |
| **Purpose** | Ingest daily sales CSV, sort by sale date, and aggregate by region. |
| **Inputs** | **File:** `/data/sales.csv` (CsvInput, separator `,`, header Y). Expected fields include at least `sale_date`, `region` (and any measures implied by GroupBy — GroupBy XML only lists group field `region`; aggregations are underspecified in sample). |
| **Outputs** | **None declared.** Pipeline ends at `Group by`. **Migration decision required:** define Delta target (e.g. `analytics.fact_sales_region`) or accept pass-through for testing. |
| **Business logic** | 1) Read CSV. 2) Sort by `sale_date`. 3) Group by `region` (aggregation functions not fully populated in sample XML — treat as region-level rollup TBD). |
| **Dependencies** | **Called by:** `Master` (entry `Sales Load`), after `Customer_Load` succeeds. **Files:** `/data/sales.csv`. **No DB connection.** No hop to Customer_Load data (sequential dependency is orchestration-only, not data lineage). |
| **Execution order** | **Job level:** second TRANS after Customer Load. **Step level:** `CSV file input` → `Sort rows` → `Group by`. |

---

## 3. Transformations — Wave 2 (sample orphan)

### 3.1 Complex_Business_Logic

| Field | Detail |
|-------|--------|
| **Asset** | `samples/Transformations/Complex_Business_Logic.ktr` |
| **Purpose** | End-to-end order enrichment: filter/qualify orders, compute line economics, join product prices and region reference, aggregate net revenue by region, land summary table + audit file. |
| **Inputs** | **SQL (WH):** `staging.customer_orders` with columns `order_id, customer_id, customer_name, product_code, quantity, unit_price, order_amount, order_date, status, region_code`; filter `order_date >= '2026-07-01'`. **File:** `/data/inbound/product_prices.csv`. **DataGrid:** region_code / region_name / region_tier (NA, EU, APAC, LATAM, MEA). **Parameters (unused):** `BATCH_DATE=2026-07-01`, `MIN_ORDER_AMOUNT=100`, `DISCOUNT_RATE=0.05`. |
| **Outputs** | **Table:** `analytics.fact_order_summary` (`total_net_revenue` SUM, `order_count` COUNT, grouped by `region_name`, `region_tier`). **File:** `/data/outbound/order_summary_audit.txt` (TextFileOutput, CSV-like, header, UTF-8). |
| **Business logic** | Parallel streams merge into one fact path: **(A)** Orders: filter ACTIVE / amount ≥ 100 / qty > 0 → null-replace amounts → add constants (`batch_date`, `source_system=ERP_ORDERS`, `is_premium`) → Calculator (`line_total`, `discount_amount`, `net_revenue`, upper/length name helpers) → Formula `adjusted_net_revenue = net_revenue - (discount_amount * 0.05)` → string normalize / product code standardize (`PRD-`→`PROD-`) → sort by `order_date`, `product_code_std`. **(B)** CSV prices → INNER MergeJoin on product code. **(C)** Region grid → StreamLookup on `region_code`. Then GroupBy region → dual sink (Delta + audit file). **Note:** Calculator `discount_amount = line_total * unit_price` looks inconsistent with `DISCOUNT_RATE` — verify before porting. |
| **Dependencies** | **Job:** not called by Master (orphan). **Connections:** `WH` (input); TableOutput connection unset. **Files:** inbound prices, outbound audit. **Reference data:** inline region grid. |
| **Execution order** | **Job level:** N/A today; propose optional task after Sales_Load or separate Workflow `wf_order_summary`. **Step level (topo):** orders path (filter→…→sort) + CSV prices join at MergeJoin; region grid joins at StreamLookup; then GroupBy → (Table output ∥ Text file audit). |

---

## 4. Transformations — Wave 3 (test fixtures)

These are **converter validation samples**, not product ETL. Plan: map to `tests/` under the Databricks project, not production Workflows.

### 4.1 Abort_Pass_Fail

| Field | Detail |
|-------|--------|
| **Purpose** | Exercise FilterRows true/false branches with Abort on failure path. |
| **Inputs** | DataGrid column `status`. |
| **Outputs** | None (Dummy on OK path; Abort on fail). |
| **Business logic** | Generate → Filter → OK (Dummy) \| Abort. |
| **Dependencies** | None external. |
| **Execution order** | Generate → Filter → (OK \| Abort). |

### 4.2 Calculator_Variants

| Field | Detail |
|-------|--------|
| **Purpose** | Cover Calculator short names, numeric IDs, long descriptions, multi-ops, unsupported, empty. |
| **Inputs** | DataGrid: `qty, price, rate, name, order_date, tmp_a`. |
| **Outputs** | None (in-memory chain). |
| **Business logic** | Sequential Calculator-only enrichment across six named calc steps. |
| **Dependencies** | None external. |
| **Execution order** | Generate → Calc short → numeric IDs → long desc → multi ops → unsupported → empty. |

### 4.3 Calc_SelectValues_Chain

| Field | Detail |
|-------|--------|
| **Purpose** | Validate Calculator → SelectValues → Calculator field contract. |
| **Inputs** | DataGrid: `qty, unit_price, discount_pct, sku, scratch`. |
| **Outputs** | None. |
| **Business logic** | Calc amounts → select/rename subset → final calc. |
| **Dependencies** | None external. |
| **Execution order** | Generate → Calc amounts → Select values → Calc final. |

### 4.4 Generator_Audit_Workflow

| Field | Detail |
|-------|--------|
| **Purpose** | Miniature audit pipeline combining constants, calc, filter, select, value map, system info, abort guard. |
| **Inputs** | DataGrid: `id, name, qty, price, status`. |
| **Outputs** | None (ends at Abort guard — fixture pattern). |
| **Business logic** | Add `currency` constant → metrics → keep active → project columns → map status → add run date/ts → Abort. |
| **Dependencies** | None external. |
| **Execution order** | Generate → Constant → Calc → Filter → Select → ValueMapper → SystemInfo → Abort. |

### 4.5 SelectValues_Variants

| Field | Detail |
|-------|--------|
| **Purpose** | Exhaust SelectValues modes: rename, remove-under-fields, meta-only, step-level remove, unspecified select, full meta. |
| **Inputs** | DataGrid: `customer_id, customer_name, amount, status, tmp_col, debug, as_of`. |
| **Outputs** | None. |
| **Business logic** | Six consecutive SelectValues configurations. |
| **Dependencies** | None external. |
| **Execution order** | Generate → SV rename → remove under fields → meta only → remove step level → select unspecified → full meta. |

### 4.6 Step_Converter_Test

| Field | Detail |
|-------|--------|
| **Purpose** | Smoke test for Constant + Calculator + StringOperations + FilterRows. |
| **Inputs** | DataGrid: `id, name, age`. |
| **Outputs** | None. |
| **Business logic** | Add `Country` → calc → string ops → filter. |
| **Dependencies** | None external. |
| **Execution order** | Generate → Constant → Calculator → String ops → Filter. |

### 4.7 TextFile_Write_Read

| Field | Detail |
|-------|--------|
| **Purpose** | Round-trip text file output then input. |
| **Inputs** | DataGrid `id, name`; later TextFileInput from `${Internal.Transformation.Filename.Directory}/runtime_orders.csv`. |
| **Outputs** | TextFileOutput base `C:\pentaho\data\runtime_orders`. |
| **Business logic** | Generate → write CSV → read CSV back. |
| **Dependencies** | Filesystem path (Windows absolute in fixture). |
| **Execution order** | Generate → WriteCSV → ReadCSV. |

### 4.8 ValueMapper_Runtime

| Field | Detail |
|-------|--------|
| **Purpose** | Runtime ValueMapper on `status`. |
| **Inputs** | DataGrid `status`. |
| **Outputs** | None. |
| **Business logic** | Generate → Map status. |
| **Dependencies** | None external. |
| **Execution order** | Generate → Map status. |

---

## 5. Cross-cutting execution model

### 5.1 End-to-end order (proposed production)

```
[Workflow: wf_master]
  1. task_customer_load     ← Customer_Load
  2. task_sales_load        ← Sales_Load          (depends_on: 1)
  3. (optional) task_success_log

[Workflow: wf_order_summary]   ← Wave 2, optional
  1. task_complex_business_logic  ← Complex_Business_Logic
```

**Data lineage (Wave 1):** Customer and Sales are **orchestrated sequentially** but do **not** share columns/tables today. Do not invent a join unless product owners request it.

### 5.2 Parameterization plan (when coding later)

| Parameter | Apply to | Proposed Spark use |
|-----------|----------|--------------------|
| `BATCH_DATE` | Customer_Load, Complex_Business_Logic | Widget / job param; filter & constant seed |
| `MIN_ORDER_AMOUNT` | Complex_Business_Logic | Replace filter literal `100` |
| `DISCOUNT_RATE` | Complex_Business_Logic | Replace Formula literal `0.05` |

### 5.3 Open decisions (block code gen)

1. Sales_Load sink table/path  
2. JDBC → Unity Catalog / external location map for `MyDB`, `WH`  
3. Include Wave 2 in `wf_master` or keep separate  
4. Preserve vs fix Calculator discount formula  
5. Volume paths for `/data/...` file I/O  

---

## 6. Proposed Databricks project structure

Aligns with existing repo `databricks/` client helpers and a standard lakehouse layout. **Folders only — no source files created in this step.**

```
databricks_project/                          # Databricks Asset Bundle / repo root for deployable ETL
├── README.md
├── databricks.yml                           # Asset Bundle: jobs, clusters, permissions
├── requirements.txt                         # PySpark / connector pins if not using serverless builtins
│
├── conf/
│   ├── connections.yml                      # MyDB → JDBC/UC; WH → warehouse (secrets refs)
│   ├── paths.yml                            # /data/sales.csv → Volumes/abfss mappings
│   └── parameters.yml                       # BATCH_DATE, MIN_ORDER_AMOUNT, DISCOUNT_RATE defaults
│
├── src/
│   └── pentaho_migration/                   # Installable Python package (wheel)
│       ├── __init__.py
│       ├── common/
│       │   ├── spark_session.py             # session / catalog helpers
│       │   ├── params.py                    # job param resolution
│       │   ├── io.py                        # read_csv, read_table, write_delta, write_text
│       │   └── connections.py               # secret-backed JDBC / UC readers
│       │
│       ├── jobs/                            # 1:1 with Pentaho Jobs
│       │   └── master/
│       │       ├── __init__.py
│       │       └── orchestrate.py           # optional programmatic runner (tests)
│       │
│       └── transformations/                 # 1:1 with Pentaho Transformations
│           ├── customer_load.py             # Wave 1
│           ├── sales_load.py                # Wave 1
│           └── complex_business_logic.py    # Wave 2
│
├── notebooks/                               # Thin Databricks entrypoints (optional if using wheel tasks)
│   ├── 01_customer_load.py
│   ├── 02_sales_load.py
│   └── 03_complex_business_logic.py
│
├── workflows/                               # Human-readable Workflow specs (mirrored in databricks.yml)
│   ├── wf_master.yml                        # Start → customer_load → sales_load → success
│   └── wf_order_summary.yml                 # Complex_Business_Logic standalone
│
├── resources/
│   └── reference/
│       └── region_reference.csv             # Export of Complex_Business_Logic DataGrid (or keep inline)
│
├── tests/
│   ├── unit/                                # Pure Python tests for transform functions
│   │   ├── test_customer_load.py
│   │   ├── test_sales_load.py
│   │   └── test_complex_business_logic.py
│   ├── fixtures/                            # Ports of tests/samples/*.ktr behavior
│   │   ├── abort_pass_fail.py
│   │   ├── calculator_variants.py
│   │   ├── calc_selectvalues_chain.py
│   │   ├── generator_audit_workflow.py
│   │   ├── selectvalues_variants.py
│   │   ├── step_converter_test.py
│   │   ├── textfile_write_read.py
│   │   └── valuemapper_runtime.py
│   └── data/                                # Small CSVs for local/Spark Connect tests
│       └── sales_sample.csv
│
└── docs/
    ├── MIGRATION_ANALYSIS_REPORT.md         # (symlink or copy from converter repo)
    ├── MIGRATION_PLAN.md                    # this document
    └── LINEAGE.md                           # table/file lineage after approval
```

### 6.1 Naming conventions

| Pentaho | Databricks |
|---------|------------|
| `Master.kjb` | Workflow `wf_master` |
| `Customer_Load.ktr` | Module `transformations/customer_load.py` + task `customer_load` |
| `Sales_Load.ktr` | Module `transformations/sales_load.py` + task `sales_load` |
| `Complex_Business_Logic.ktr` | Module `transformations/complex_business_logic.py` + Workflow `wf_order_summary` |
| Job hop order | Workflow `depends_on` / `run_if` |
| `${Internal.*.Filename.Directory}` | Bundle-relative / Volume base from `conf/paths.yml` |

### 6.2 Workflow skeleton (conceptual)

```
wf_master
├── task: customer_load
│     entry: notebooks/01_customer_load.py  OR  python_wheel_task:customer_load.main
│     params: BATCH_DATE
├── task: sales_load
│     depends_on: [customer_load]
│     entry: notebooks/02_sales_load.py
│     params: (paths)
└── task: success (optional email/log)

wf_order_summary
└── task: complex_business_logic
      params: BATCH_DATE, MIN_ORDER_AMOUNT, DISCOUNT_RATE
```

### 6.3 Layering inside each transformation module (when coded later)

```
read inputs → business transforms (DataFrame ops mirroring hops) → write outputs
```

Mirror hop order inside each file with named sections (`# step: Filter rows`, etc.) for traceability back to Pentaho — **implementation deferred**.

### 6.4 Relationship to this converter repo

| This repo path | Role after migration |
|----------------|----------------------|
| `samples/` | Source of truth for Wave 1–2 semantics |
| `tests/samples/` | Behavior oracles for Wave 3 fixture ports |
| `pentaho_converter/` | Codegen engine (future) feeding `databricks_project/src/...` |
| `databricks/` (existing client) | Optional deploy/runtime API helper — keep separate from ETL package |

---

## 7. Delivery checklist (post-approval)

- [ ] Confirm Waves 1 / 2 / 3 scope  
- [ ] Approve Databricks folder layout above (or adjust naming)  
- [ ] Resolve open decisions §5.3  
- [ ] Then generate PySpark modules + Workflow YAML (next phase)

---

*No application code or notebooks were generated in this step.*
