# Complete Migration Audit Report

**Generated:** 2026-07-15T12:23:28.600450+00:00
**Target:** 100%
**Overall migration:** **93.32%**

## Source inventory

- `.ktr` on disk: **60**
- `.kjb` on disk: **12**
- `.ktr` missing on disk but referenced by JOB TRANS entries: **38**

## Checklist scorecard

| Checklist item | PASS | PARTIAL | FAIL | N/A | Scored | % | 100%? |
|----------------|-----:|--------:|-----:|----:|-------:|--:|:-----:|
| Every .ktr converted | 60 | 38 | 0 | 0 | 98 | 80.61% | ✗ |
| Every .kjb converted | 12 | 0 | 0 | 0 | 12 | 100.0% | ✓ |
| Every transformation step converted | 1545 | 49 | 178 | 0 | 1772 | 88.57% | ✗ |
| Every job entry converted | 255 | 0 | 0 | 0 | 255 | 100.0% | ✓ |
| Every hop converted | 2263 | 0 | 0 | 2 | 2263 | 100.0% | ✓ |
| Every variable converted | 125 | 0 | 0 | 37 | 125 | 100.0% | ✓ |
| Every parameter converted | 488 | 0 | 12 | 10 | 500 | 97.6% | ✗ |
| Every lookup converted | 52 | 16 | 11 | 0 | 79 | 75.95% | ✗ |
| Every output converted | 285 | 17 | 0 | 0 | 302 | 97.19% | ✗ |

**Equal-weight average of checklist categories = 93.32%** (target 100%).

## Gap summary

- FAIL items: **201**
- PARTIAL items: **120**

### Top gaps (first 80)

| Status | Checklist | Key | Detail |
|--------|-----------|-----|--------|
| PARTIAL | Every .ktr converted | `FX_Rate_Lookup_Prep.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Payments_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Payments_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Payments_Fact_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Shipments_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Shipments_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Shipments_Fact_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Date_Dimension_Generate.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Region_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Region_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Region_Dimension_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Category_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Category_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Category_Dimension_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Supplier_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Supplier_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Supplier_Dimension_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Store_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Store_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Store_Dimension_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Employee_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Employee_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Employee_Dimension_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Promotion_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Promotion_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Promotion_Dimension_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Reporting_Daily_Sales.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Reporting_Monthly_Sales.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Reporting_Store_Performance.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Reporting_Product_Performance.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Reporting_Inventory_Summary.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Reporting_Customer_Segmentation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Reporting_Promotion_Effectiveness.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Returns_Validation.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Returns_Cleansing.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Returns_Fact_Load.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Audit_Collect_Metrics.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every .ktr converted | `Audit_Write_Report.ktr (missing)` | placeholder module closes orchestration gap |
| PARTIAL | Every transformation step converted | `Calculator_Variants:Calc empty [Calculator]` |  |
| PARTIAL | Every transformation step converted | `Customer_Cleansing:Normalize Phone Numbers [ReplaceString]` |  |
| PARTIAL | Every transformation step converted | `Customer_Cleansing:Remove Invalid Characters [ReplaceString]` |  |
| PARTIAL | Every transformation step converted | `Inventory_Cleansing:Normalize Product Codes Spaces [ReplaceString]` |  |
| FAIL | Every transformation step converted | `Inventory_Cleansing:Null If Placeholder Tokens [NullIf]` |  |
| FAIL | Every transformation step converted | `Inventory_Cleansing:Standardize Units [ValueMapper]` |  |
| PARTIAL | Every transformation step converted | `Product_Cleansing:Standardize SKU Separators [ReplaceString]` |  |
| PARTIAL | Every transformation step converted | `Sales_Business_Transform:Normalize Promo Code [ReplaceString]` |  |
| FAIL | Every transformation step converted | `Sales_Business_Transform:Lookup Holiday Flag [StreamLookup]` |  |
| PARTIAL | Every transformation step converted | `Sales_Business_Transform:Customer Lifetime Revenue [MemoryGroupBy]` |  |
| PARTIAL | Every transformation step converted | `Sales_Business_Transform:Store Revenue Aggregate [MemoryGroupBy]` |  |
| PARTIAL | Every transformation step converted | `Sales_Business_Transform:Employee Sales Aggregate [MemoryGroupBy]` |  |
| PARTIAL | Every transformation step converted | `Sales_Business_Transform:Returned Revenue Aggregate [MemoryGroupBy]` |  |
| FAIL | Every transformation step converted | `Sales_Business_Transform:Cancelled For Aggregate? [FilterRows]` |  |
| FAIL | Every transformation step converted | `Sales_Business_Transform:Sum Cancelled Revenue [MemoryGroupBy]` |  |
| PARTIAL | Every transformation step converted | `Sales_Business_Transform:Average Basket Metrics [MemoryGroupBy]` |  |
| FAIL | Every transformation step converted | `Sales_Business_Transform:Attach Customer LTV [StreamLookup]` |  |
| FAIL | Every transformation step converted | `Sales_Business_Transform:Attach Store Revenue [StreamLookup]` |  |
| FAIL | Every transformation step converted | `Sales_Business_Transform:Attach Employee Sales [StreamLookup]` |  |
| FAIL | Every transformation step converted | `Sales_Cleansing:Empty Stream Guard? [FilterRows]` |  |
| FAIL | Every transformation step converted | `Sales_Cleansing:Add Validation Batch [Constant]` |  |
| PARTIAL | Every transformation step converted | `Sales_Cleansing:Soft Lookup DimCustomer [DBJoin]` |  |
| PARTIAL | Every transformation step converted | `Sales_Cleansing:Soft Lookup DimProduct [DBJoin]` |  |
| PARTIAL | Every transformation step converted | `Sales_Cleansing:Soft Lookup DimStore [DBJoin]` |  |
| FAIL | Every transformation step converted | `Sales_Cleansing:Valid Sales Row? [FilterRows]` |  |
| FAIL | Every transformation step converted | `Sales_Cleansing:Write Valid Sales [TextFileOutput]` |  |
| FAIL | Every transformation step converted | `Sales_Cleansing:Route Rejects [Constant]` |  |
| PARTIAL | Every transformation step converted | `TR_Customer_Cleansing:Normalize Phone Numbers [ReplaceString]` |  |
| PARTIAL | Every transformation step converted | `TR_Customer_Cleansing:Remove Invalid Characters [ReplaceString]` |  |
| FAIL | Every transformation step converted | `TR_Customer_Enrichment:Region Lookup [StreamLookup]` |  |
| FAIL | Every transformation step converted | `TR_Customer_Enrichment:Store Lookup [StreamLookup]` |  |
| FAIL | Every transformation step converted | `TR_Inventory_Business_Rules:Dead Stock? [FilterRows]` |  |
| PARTIAL | Every transformation step converted | `TR_Inventory_Business_Rules:Warehouse Balancing [MemoryGroupBy]` |  |
| PARTIAL | Every transformation step converted | `TR_Inventory_Business_Rules:Inventory Summary Agg [MemoryGroupBy]` |  |
| FAIL | Every transformation step converted | `TR_Inventory_Business_Rules:Reorder Rows? [FilterRows]` |  |
| FAIL | Every transformation step converted | `TR_Inventory_Business_Rules:Dead Rows For Report? [FilterRows]` |  |
| FAIL | Every transformation step converted | `TR_Inventory_Business_Rules:Write Dead Stock Report [TextFileOutput]` |  |
| FAIL | Every transformation step converted | `TR_Inventory_Business_Rules:Write Reorder Report [TextFileOutput]` |  |
| PARTIAL | Every transformation step converted | `TR_Inventory_Cleansing:Normalize Product Codes Spaces [ReplaceString]` |  |
| FAIL | Every transformation step converted | `TR_Inventory_Cleansing:Null If Placeholder Tokens [NullIf]` |  |
| FAIL | Every transformation step converted | `TR_Inventory_Cleansing:Standardize Units [ValueMapper]` |  |
| PARTIAL | Every transformation step converted | `TR_Product_Cleansing:Standardize SKU Separators [ReplaceString]` |  |

_… 241 more gaps in JSON report._

## Notes on scoring

- **PASS** = 100% credit; **PARTIAL** = 50% credit; **FAIL** = 0%; **N/A** excluded.
- Missing-on-disk `.ktr` files referenced by retail JOBs score as **PARTIAL** when a `SOURCE_MISSING` placeholder module exists (orchestration gap closed, logic absent).
- Step quality uses generator markers `[converted]` / `[partial]` / `[failed]` when present.
- Overall % is the **unweighted mean** of the nine checklist category percentages.
