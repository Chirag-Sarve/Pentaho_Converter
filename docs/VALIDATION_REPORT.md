# Pentaho → PySpark Validation Report

**Generated:** 2026-07-15 12:15:47 UTC
**Scope:** Comparison of generated modules vs original Pentaho artifacts
**Code modified:** Fixes applied for validation gaps; this file is re-validated output

## Executive summary

| Status | Count |
|--------|------:|
| PASS | 385 |
| PARTIAL | 0 |
| FAIL | 0 |
| N/A | 8 |
| **Total checks** | **393** |

### Category breakdown

| Category | PASS | PARTIAL | FAIL | N/A |
|----------|-----:|--------:|-----:|----:|
| aggregation | 2 | 0 | 0 | 0 |
| api | 11 | 0 | 0 | 0 |
| calculated_field | 24 | 0 | 0 | 0 |
| child_job | 10 | 0 | 0 | 0 |
| filter | 5 | 0 | 0 | 0 |
| hop | 52 | 0 | 0 | 0 |
| job | 2 | 0 | 0 | 0 |
| job_child_trans | 2 | 0 | 0 | 0 |
| job_entry | 37 | 0 | 0 | 0 |
| job_handler | 14 | 0 | 0 | 0 |
| job_hop | 46 | 0 | 0 | 0 |
| job_trans_ref | 51 | 0 | 0 | 0 |
| join | 1 | 0 | 0 | 0 |
| lookup | 1 | 0 | 0 | 0 |
| output | 4 | 0 | 0 | 8 |
| retail_transformation | 49 | 0 | 0 | 0 |
| step | 63 | 0 | 0 | 0 |
| transformation | 11 | 0 | 0 | 0 |

## 1. Transformations (`.ktr` → PySpark modules)

| KTR | Module | Exists | Steps PASS/FAIL | Hops P/F | Overall fails |
|-----|--------|:------:|----------------:|---------:|--------------:|
| `samples\Transformations\Complex_Business_Logic.ktr` | `databricks_project\src\pentaho_migration\transformations\complex_business_logic.py` | Y | 16/0 | 15/0 | 0 |
| `samples\Transformations\Customer_Load.ktr` | `databricks_project\src\pentaho_migration\transformations\customer_load.py` | Y | 4/0 | 3/0 | 0 |
| `samples\Transformations\Sales_Load.ktr` | `databricks_project\src\pentaho_migration\transformations\sales_load.py` | Y | 3/0 | 2/0 | 0 |
| `tests\samples\Abort_Pass_Fail.ktr` | `databricks_project\src\pentaho_migration\transformations\abort_pass_fail.py` | Y | 4/0 | 3/0 | 0 |
| `tests\samples\Calc_SelectValues_Chain.ktr` | `databricks_project\src\pentaho_migration\transformations\calc_select_values_chain.py` | Y | 4/0 | 3/0 | 0 |
| `tests\samples\Calculator_Variants.ktr` | `databricks_project\src\pentaho_migration\transformations\calculator_variants.py` | Y | 7/0 | 6/0 | 0 |
| `tests\samples\Generator_Audit_Workflow.ktr` | `databricks_project\src\pentaho_migration\transformations\generator_audit_workflow.py` | Y | 8/0 | 7/0 | 0 |
| `tests\samples\SelectValues_Variants.ktr` | `databricks_project\src\pentaho_migration\transformations\select_values_variants.py` | Y | 7/0 | 6/0 | 0 |
| `tests\samples\Step_Converter_Test.ktr` | `databricks_project\src\pentaho_migration\transformations\step_converter_test.py` | Y | 5/0 | 4/0 | 0 |
| `tests\samples\TextFile_Write_Read.ktr` | `databricks_project\src\pentaho_migration\transformations\text_file_write_read.py` | Y | 3/0 | 2/0 | 0 |
| `tests\samples\ValueMapper_Runtime.ktr` | `databricks_project\src\pentaho_migration\transformations\value_mapper_runtime.py` | Y | 2/0 | 1/0 | 0 |

### `samples\Transformations\Complex_Business_Logic.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\complex_business_logic.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| CSV product prices [CsvInput] | **PASS** | token=df_CSV_product_prices |
| Region reference grid [DataGrid] | **PASS** | token=df_Region_reference_grid |
| Table input orders [TableInput] | **PASS** | token=df_Table_input_orders |
| Filter active orders [FilterRows] | **PASS** | token=df_Filter_active_orders |
| Replace null amounts [ReplaceNull] | **PASS** | token=df_Replace_null_amounts |
| Add batch constants [Constant] | **PASS** | token=df_Add_batch_constants |
| Calculate line metrics [Calculator] | **PASS** | token=df_Calculate_line_metrics |
| Apply discount formula [Formula] | **PASS** | token=df_Apply_discount_formula |
| Normalize customer name [StringOperations] | **PASS** | token=df_Normalize_customer_name |
| Fix product codes [ReplaceInString] | **PASS** | token=df_Fix_product_codes |
| Sort by order date [SortRows] | **PASS** | token=df_Sort_by_order_date |
| Merge join with products [MergeJoin] | **PASS** | token=df_Merge_join_with_products |
| Stream lookup regions [StreamLookup] | **PASS** | token=df_Stream_lookup_regions |
| Aggregate by region [GroupBy] | **PASS** | token=df_Aggregate_by_region |
| Table output summary [TableOutput] | **PASS** | token=df_Table_output_summary |
| Text file audit log [TextFileOutput] | **PASS** | token=df_Text_file_audit_log |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Table input orders → Filter active orders | **PASS** | destination assignment references source DF |
| Filter active orders → Replace null amounts | **PASS** | destination assignment references source DF |
| Replace null amounts → Add batch constants | **PASS** | destination assignment references source DF |
| Add batch constants → Calculate line metrics | **PASS** | destination assignment references source DF |
| Calculate line metrics → Apply discount formula | **PASS** | destination assignment references source DF |
| Apply discount formula → Normalize customer name | **PASS** | destination assignment references source DF |
| Normalize customer name → Fix product codes | **PASS** | destination assignment references source DF |
| Fix product codes → Sort by order date | **PASS** | destination assignment references source DF |
| Sort by order date → Merge join with products | **PASS** | both DF tokens present (join/multi-input) |
| CSV product prices → Merge join with products | **PASS** | both DF tokens present (join/multi-input) |
| Merge join with products → Stream lookup regions | **PASS** | destination assignment references source DF |
| Region reference grid → Stream lookup regions | **PASS** | both DF tokens present (join/multi-input) |
| Stream lookup regions → Aggregate by region | **PASS** | destination assignment references source DF |
| Aggregate by region → Table output summary | **PASS** | destination assignment references source DF |
| Aggregate by region → Text file audit log | **PASS** | destination assignment references source DF |

#### filter

| Item | Status | Detail |
|------|--------|--------|
| Filter active orders | **PASS** | filter_call=True token_present=True |

#### lookup

| Item | Status | Detail |
|------|--------|--------|
| Stream lookup regions | **PASS** | join=True broadcast=True keys=['region_code', ''] |

#### join

| Item | Status | Detail |
|------|--------|--------|
| Merge join with products (INNER) | **PASS** | keys_1=['product_code_std'] keys_2=['product_code'] |

#### calculated_field

| Item | Status | Detail |
|------|--------|--------|
| Calculate line metrics.line_total (MULTIPLY) | **PASS** | a=quantity b=unit_price formula= |
| Calculate line metrics.discount_amount (MULTIPLY) | **PASS** | a=line_total b=unit_price formula= |
| Calculate line metrics.net_revenue (SUBTRACT) | **PASS** | a=line_total b=discount_amount formula= |
| Calculate line metrics.customer_name_upper (UPPER) | **PASS** | a=customer_name b= formula= |
| Calculate line metrics.name_length (LENGTH) | **PASS** | a=customer_name b= formula= |
| Apply discount formula.adjusted_net_revenue (FORMULA) | **PASS** | a=None b=None formula=[net_revenue] - ([discount_amount] * 0.05) |

#### aggregation

| Item | Status | Detail |
|------|--------|--------|
| Aggregate by region | **PASS** | aggs=[{'name': 'total_net_revenue', 'aggregate': 'SUM', 'subject': 'adjusted_net_revenue'}, {'name': 'order_count', 'aggregate': 'COUNT', 'subject': 'order_id'}] missing_in_code=[] |

#### output

| Item | Status | Detail |
|------|--------|--------|
| Table output summary [TableOutput] | **PASS** | table=fact_order_summary, schema=analytics; write=True |
| Text file audit log [TextFileOutput] | **PASS** | file=/data/outbound/order_summary_audit.txt; write=True |

### `samples\Transformations\Customer_Load.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\customer_load.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Table input [TableInput] | **PASS** | token=df_Table_input |
| Filter rows [FilterRows] | **PASS** | token=df_Filter_rows |
| Select values [SelectValues] | **PASS** | token=df_Select_values |
| Table output [TableOutput] | **PASS** | token=df_Table_output |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Table input → Filter rows | **PASS** | destination assignment references source DF |
| Filter rows → Select values | **PASS** | destination assignment references source DF |
| Select values → Table output | **PASS** | destination assignment references source DF |

#### filter

| Item | Status | Detail |
|------|--------|--------|
| Filter rows | **PASS** | filter_call=True token_present=True |

#### output

| Item | Status | Detail |
|------|--------|--------|
| Table output [TableOutput] | **PASS** | table=dim_customer, schema=analytics; write=True |

### `samples\Transformations\Sales_Load.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\sales_load.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| CSV file input [CsvInput] | **PASS** | token=df_CSV_file_input |
| Sort rows [SortRows] | **PASS** | token=df_Sort_rows |
| Group by [GroupBy] | **PASS** | token=df_Group_by |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| CSV file input → Sort rows | **PASS** | destination assignment references source DF |
| Sort rows → Group by | **PASS** | destination assignment references source DF |

#### aggregation

| Item | Status | Detail |
|------|--------|--------|
| Group by | **PASS** | aggs=[{'name': 'region', 'aggregate': '', 'subject': ''}] missing_in_code=[] |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

### `tests\samples\Abort_Pass_Fail.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\abort_pass_fail.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate [DataGrid] | **PASS** | token=df_Generate |
| Filter [FilterRows] | **PASS** | token=df_Filter |
| OK [Dummy] | **PASS** | token=df_OK |
| Abort [Abort] | **PASS** | token=df_Abort |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate → Filter | **PASS** | both DF tokens present (join/multi-input) |
| Filter → OK | **PASS** | filter branch tokens present |
| Filter → Abort | **PASS** | filter branch tokens present |

#### filter

| Item | Status | Detail |
|------|--------|--------|
| Filter | **PASS** | filter_call=True token_present=True |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

### `tests\samples\Calc_SelectValues_Chain.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\calc_select_values_chain.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows [DataGrid] | **PASS** | token=df_Generate_Rows |
| Calc amounts [Calculator] | **PASS** | token=df_Calc_amounts |
| Select values [SelectValues] | **PASS** | token=df_Select_values |
| Calc final [Calculator] | **PASS** | token=df_Calc_final |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows → Calc amounts | **PASS** | destination assignment references source DF |
| Calc amounts → Select values | **PASS** | destination assignment references source DF |
| Select values → Calc final | **PASS** | destination assignment references source DF |

#### calculated_field

| Item | Status | Detail |
|------|--------|--------|
| Calc amounts.gross (A * B) | **PASS** | a=qty b=unit_price formula= |
| Calc amounts.net (PERCENT_2) | **PASS** | a=gross b=discount_pct formula= |
| Calc amounts.sku_upper (UPPER) | **PASS** | a=sku b= formula= |
| Calc final.unit_net (DIVIDE) | **PASS** | a=net b=qty formula= |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

### `tests\samples\Calculator_Variants.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\calculator_variants.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows [DataGrid] | **PASS** | token=df_Generate_Rows |
| Calc short names [Calculator] | **PASS** | token=df_Calc_short_names |
| Calc numeric IDs [Calculator] | **PASS** | token=df_Calc_numeric_IDs |
| Calc long desc [Calculator] | **PASS** | token=df_Calc_long_desc |
| Calc multi ops [Calculator] | **PASS** | token=df_Calc_multi_ops |
| Calc unsupported [Calculator] | **PASS** | token=df_Calc_unsupported |
| Calc empty [Calculator] | **PASS** | token=df_Calc_empty |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows → Calc short names | **PASS** | destination assignment references source DF |
| Calc short names → Calc numeric IDs | **PASS** | destination assignment references source DF |
| Calc numeric IDs → Calc long desc | **PASS** | destination assignment references source DF |
| Calc long desc → Calc multi ops | **PASS** | destination assignment references source DF |
| Calc multi ops → Calc unsupported | **PASS** | destination assignment references source DF |
| Calc unsupported → Calc empty | **PASS** | destination assignment references source DF |

#### calculated_field

| Item | Status | Detail |
|------|--------|--------|
| Calc short names.line_total (MULTIPLY) | **PASS** | a=qty b=price formula= |
| Calc short names.name_upper (UPPER_CASE) | **PASS** | a=name b= formula= |
| Calc numeric IDs.sum_ids (3) | **PASS** | a=qty b=tmp_a formula= |
| Calc numeric IDs.name_len (56) | **PASS** | a=name b= formula= |
| Calc long desc.pct (100 * A / B) | **PASS** | a=line_total b=rate formula= |
| Calc long desc.diff (A - B) | **PASS** | a=line_total b=tmp_a formula= |
| Calc multi ops.days_out (ADD_DAYS) | **PASS** | a=order_date b=1 formula= |
| Calc multi ops.name_lower (LOWER) | **PASS** | a=name b= formula= |
| Calc multi ops.discounted (PERCENT_2) | **PASS** | a=line_total b=rate formula= |
| Calc unsupported.jaro_score (JARO) | **PASS** | a=name b=name_upper formula= |
| Calc empty: (empty) | **PASS** | no calculation elements in KTR |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

### `tests\samples\Generator_Audit_Workflow.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\generator_audit_workflow.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows [DataGrid] | **PASS** | token=df_Generate_Rows |
| Add constants [Constant] | **PASS** | token=df_Add_constants |
| Calc metrics [Calculator] | **PASS** | token=df_Calc_metrics |
| Filter active [FilterRows] | **PASS** | token=df_Filter_active |
| Select values [SelectValues] | **PASS** | token=df_Select_values |
| Map status [ValueMapper] | **PASS** | token=df_Map_status |
| System info [SystemInfo] | **PASS** | token=df_System_info |
| Guard abort [Abort] | **PASS** | token=df_Guard_abort |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows → Add constants | **PASS** | destination assignment references source DF |
| Add constants → Calc metrics | **PASS** | destination assignment references source DF |
| Calc metrics → Filter active | **PASS** | destination assignment references source DF |
| Filter active → Select values | **PASS** | destination assignment references source DF |
| Select values → Map status | **PASS** | destination assignment references source DF |
| Map status → System info | **PASS** | destination assignment references source DF |
| System info → Guard abort | **PASS** | destination assignment references source DF |

#### filter

| Item | Status | Detail |
|------|--------|--------|
| Filter active | **PASS** | filter_call=True token_present=True |

#### calculated_field

| Item | Status | Detail |
|------|--------|--------|
| Calc metrics.total (MULTIPLY) | **PASS** | a=qty b=price formula= |
| Calc metrics.name_u (UPPER_CASE) | **PASS** | a=name b= formula= |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

### `tests\samples\SelectValues_Variants.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\select_values_variants.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows [DataGrid] | **PASS** | token=df_Generate_Rows |
| SV select rename [SelectValues] | **PASS** | token=df_SV_select_rename |
| SV remove under fields [SelectValues] | **PASS** | token=df_SV_remove_under_fields |
| SV meta only [SelectValues] | **PASS** | token=df_SV_meta_only |
| SV remove step level [SelectValues] | **PASS** | token=df_SV_remove_step_level |
| SV select unspecified [SelectValues] | **PASS** | token=df_SV_select_unspecified |
| SV full meta [SelectValues] | **PASS** | token=df_SV_full_meta |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows → SV select rename | **PASS** | destination assignment references source DF |
| SV select rename → SV remove under fields | **PASS** | destination assignment references source DF |
| SV remove under fields → SV meta only | **PASS** | destination assignment references source DF |
| SV meta only → SV remove step level | **PASS** | destination assignment references source DF |
| SV remove step level → SV select unspecified | **PASS** | destination assignment references source DF |
| SV select unspecified → SV full meta | **PASS** | destination assignment references source DF |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

### `tests\samples\Step_Converter_Test.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\step_converter_test.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows [DataGrid] | **PASS** | token=df_Generate_Rows |
| Add constants [Constant] | **PASS** | token=df_Add_constants |
| Calculator [Calculator] | **PASS** | token=df_Calculator |
| String ops [StringOperations] | **PASS** | token=df_String_ops |
| Filter rows [FilterRows] | **PASS** | token=df_Filter_rows |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate Rows → Add constants | **PASS** | destination assignment references source DF |
| Add constants → Calculator | **PASS** | destination assignment references source DF |
| Calculator → String ops | **PASS** | destination assignment references source DF |
| String ops → Filter rows | **PASS** | destination assignment references source DF |

#### filter

| Item | Status | Detail |
|------|--------|--------|
| Filter rows | **PASS** | filter_call=True token_present=True |

#### calculated_field

| Item | Status | Detail |
|------|--------|--------|
| Calculator.age_plus_one (ADD) | **PASS** | a=age b=id formula= |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

### `tests\samples\TextFile_Write_Read.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\text_file_write_read.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate [DataGrid] | **PASS** | token=df_Generate |
| WriteCSV [TextFileOutput] | **PASS** | token=df_WriteCSV |
| ReadCSV [TextFileInput] | **PASS** | token=df_ReadCSV |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate → WriteCSV | **PASS** | destination assignment references source DF |
| WriteCSV → ReadCSV | **PASS** | both DF tokens present (join/multi-input) |

#### output

| Item | Status | Detail |
|------|--------|--------|
| WriteCSV [TextFileOutput] | **PASS** | file=C:\pentaho\data\runtime_orders; write=True |

### `tests\samples\ValueMapper_Runtime.ktr`

**Module:** `databricks_project\src\pentaho_migration\transformations\value_mapper_runtime.py`

#### step

| Item | Status | Detail |
|------|--------|--------|
| Generate [DataGrid] | **PASS** | token=df_Generate |
| Map status [ValueMapper] | **PASS** | token=df_Map_status |

#### hop

| Item | Status | Detail |
|------|--------|--------|
| Generate → Map status | **PASS** | destination assignment references source DF |

#### output

| Item | Status | Detail |
|------|--------|--------|
| (none in KTR) | **N/A** | transformation has no output step in source |

## 2. Sample job `Master.kjb`

**Module:** `databricks_project\src\pentaho_migration\jobs\master.py`

| Item | Status | Detail |
|------|--------|--------|
| job: Master | **PASS** | databricks_project\src\pentaho_migration\jobs\master.py |
| job_entry: Start | **PASS** |  |
| job_entry: Customer Load | **PASS** |  |
| job_entry: Sales Load | **PASS** |  |
| job_entry: Success | **PASS** |  |
| job_hop: Start → Customer Load | **PASS** |  |
| job_hop: Customer Load → Sales Load | **PASS** |  |
| job_hop: Sales Load → Success | **PASS** |  |
| job_child_trans: customer_load | **PASS** |  |
| job_child_trans: sales_load | **PASS** |  |

## 3. Retail job `Master_ETL.kjb`

**Source:** `C:\Users\Prateek.Kotian\Desktop\Pentaho\Retail & E-commerce\Retail_ETL_Project\jobs\master\Master_ETL.kjb`
**Module:** `databricks_project\src\pentaho_migration\jobs\Master_ETL.py`

| Item | Status | Detail |
|------|--------|--------|
| job: Master_ETL | **PASS** | module=True graph=True |
| job_entry: Start [SPECIAL] | **PASS** |  |
| job_entry: Log Job Started [WRITE_TO_LOG] | **PASS** |  |
| job_entry: Initialize Variables [SET_VARIABLES] | **PASS** |  |
| job_entry: Echo Batch Context [SHELL] | **PASS** |  |
| job_entry: Create Log Folder [CREATE_FOLDER] | **PASS** |  |
| job_entry: Create Reject Folder [CREATE_FOLDER] | **PASS** |  |
| job_entry: Create Output Folder [CREATE_FOLDER] | **PASS** |  |
| job_entry: Create Archive Folder [CREATE_FOLDER] | **PASS** |  |
| job_entry: Create Audit Folder [CREATE_FOLDER] | **PASS** |  |
| job_entry: Folders Ready [SPECIAL] | **PASS** |  |
| job_entry: Wait For Source Flag Optional [WAIT_FOR_FILE] | **PASS** |  |
| job_entry: Archive Previous Outputs [ZIP_FILE] | **PASS** |  |
| job_entry: Copy Prior Logs To Archive [COPY_FILES] | **PASS** |  |
| job_entry: Log Init Complete [WRITE_TO_LOG] | **PASS** |  |
| job_entry: Load Master Data [JOB] | **PASS** |  |
| child_job: Load Master Data → load_master_data.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Load Customers [JOB] | **PASS** |  |
| child_job: Load Customers → load_customer_data.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Load Products [JOB] | **PASS** |  |
| child_job: Load Products → load_product_data.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Load Inventory [JOB] | **PASS** |  |
| child_job: Load Inventory → load_inventory_data.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Load Sales [JOB] | **PASS** |  |
| child_job: Load Sales → load_sales_data.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Load Finance [JOB] | **PASS** |  |
| child_job: Load Finance → load_finance_data.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Load Reporting [JOB] | **PASS** |  |
| child_job: Load Reporting → load_reporting_data.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Cleanup Temporary Files [JOB] | **PASS** |  |
| child_job: Cleanup Temporary Files → cleanup.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Generate Audit Report [JOB] | **PASS** |  |
| child_job: Generate Audit Report → audit.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Success Email Dummy [JOB] | **PASS** |  |
| child_job: Success Email Dummy → notification.py | **PASS** | expanded child JOB graph with hop engine |
| job_entry: Log Job Completed [WRITE_TO_LOG] | **PASS** |  |
| job_entry: Success [SUCCESS] | **PASS** |  |
| job_entry: Evaluate Retry Allowed [SIMPLE_EVAL] | **PASS** |  |
| job_entry: Retry Delay [DELAY] | **PASS** |  |
| job_entry: Increment Retry Count [SET_VARIABLES] | **PASS** |  |
| job_entry: Log Failure [WRITE_TO_LOG] | **PASS** |  |
| job_entry: Failure Email Dummy [MAIL] | **PASS** |  |
| job_entry: Abort on Failure [ABORT] | **PASS** |  |
| job_entry: Retry Gate [SPECIAL] | **PASS** |  |
| job_handler: ABORT | **PASS** | handle_abort |
| job_handler: COPY_FILES | **PASS** | handle_copy_files |
| job_handler: CREATE_FOLDER | **PASS** | handle_create_folder |
| job_handler: DELAY | **PASS** | handle_delay |
| job_handler: JOB | **PASS** | handle_job |
| job_handler: MAIL | **PASS** | handle_mail |
| job_handler: SET_VARIABLES | **PASS** | handle_set_variables |
| job_handler: SHELL | **PASS** | handle_shell |
| job_handler: SIMPLE_EVAL | **PASS** | handle_simple_eval |
| job_handler: SPECIAL | **PASS** | handle_special |
| job_handler: SUCCESS | **PASS** | handle_success |
| job_handler: WAIT_FOR_FILE | **PASS** | handle_wait_for_file |
| job_handler: WRITE_TO_LOG | **PASS** | handle_write_to_log |
| job_handler: ZIP_FILE | **PASS** | handle_zip_file |

**Job hops:** 43 PASS / 0 FAIL (of 43 total; full hop list in JSON artifact).

## 3b. Retail transformation / JOB TRANS coverage

Retail coverage checks: **100 PASS** / **0 FAIL** (of 100).

## 4. Failures and partials

No FAIL or PARTIAL findings.
## 5. Verdict

- Transformations covered: **11/11** modules exist for KTRs.
- Transformations with any FAIL check: **0**.
- Master_ETL child JOBs: **10** PASS, **0** PARTIAL, **0** FAIL.
- Retail KTR/TRANS modules: **100/100** PASS.
- Sample `Master.kjb` workflow maps to `jobs/master.py` and calls `customer_load` / `sales_load`.

**Result: zero FAIL and zero PARTIAL checks.**
