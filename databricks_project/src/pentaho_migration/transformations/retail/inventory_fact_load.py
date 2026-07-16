"""PySpark module migrated from Pentaho transformation: TR_FactInventory_Load.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/fact/Inventory_Fact_Load.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    current_date,
    length,
    lit,
    when,
    coalesce,
    row_number,
)
from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    cache_for_reuse,
    get_logger,
    log_event,
    timed,
    unpersist_quiet,
    write_delta,
)

_LOG = get_logger("pentaho_migration.transformations.retail.inventory_fact_load")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_FactInventory_Load`` step-for-step."""
    config = dict(config or {})
    apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "transformation_start")
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "analytics")
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")

    # Parameters
    PROJECT_HOME = ''
    DATASET_PATH = ''
    RUN_ID = ''
    CURRENT_DATE = ''
    LOG_PATH = '${PROJECT_HOME}/logs'
    REJECT_PATH = '${PROJECT_HOME}/rejects'
    OUTPUT_PATH = '${PROJECT_HOME}/output'
    VAR_PATH_AUDIT = '${PROJECT_HOME}/audit'
    VAR_ETL_BATCH_ID = '${RUN_ID}'

    # Step: Get Fact Load Variables (GetVariable) [converted]
    # Get Variables: Get Fact Load Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Fact_Load_Variables = spark.range(1).select(lit(1).alias('_row'))
    # field 'batch_id' from variable string '${VAR_ETL_BATCH_ID}'
    # preserved.field.batch_id.trim_type='none'
    # preserved.field.batch_id.type='String'
    _batch_id_resolved = None
    _dbu__batch_id_resolved = globals().get('dbutils')
    if _dbu__batch_id_resolved is not None and hasattr(_dbu__batch_id_resolved, 'widgets'):
        try:
            _batch_id_resolved = _dbu__batch_id_resolved.widgets.get('VAR_ETL_BATCH_ID')
        except Exception:
            _batch_id_resolved = None
    if _batch_id_resolved in (None, ''):
        import os as _os__batch_id_resolved
        _batch_id_resolved = _os__batch_id_resolved.environ.get('VAR_ETL_BATCH_ID')
    if _batch_id_resolved in (None, ''):
        try:
            _batch_id_resolved = spark.conf.get('pentaho.var.VAR_ETL_BATCH_ID')
        except Exception:
            _batch_id_resolved = None
    if _batch_id_resolved in (None, ''):
        _batch_id_resolved = '${RUN_ID}'
    if _batch_id_resolved is None:
        _batch_id_resolved = ''
    df_Get_Fact_Load_Variables = df_Get_Fact_Load_Variables.withColumn('batch_id', lit(_batch_id_resolved))
    # field 'run_id' from variable string '${RUN_ID}'
    # preserved.field.run_id.trim_type='none'
    # preserved.field.run_id.type='String'
    _run_id_resolved = None
    _dbu__run_id_resolved = globals().get('dbutils')
    if _dbu__run_id_resolved is not None and hasattr(_dbu__run_id_resolved, 'widgets'):
        try:
            _run_id_resolved = _dbu__run_id_resolved.widgets.get('RUN_ID')
        except Exception:
            _run_id_resolved = None
    if _run_id_resolved in (None, ''):
        import os as _os__run_id_resolved
        _run_id_resolved = _os__run_id_resolved.environ.get('RUN_ID')
    if _run_id_resolved in (None, ''):
        try:
            _run_id_resolved = spark.conf.get('pentaho.var.RUN_ID')
        except Exception:
            _run_id_resolved = None
    if _run_id_resolved in (None, ''):
        _run_id_resolved = ''
    if _run_id_resolved is None:
        _run_id_resolved = ''
    df_Get_Fact_Load_Variables = df_Get_Fact_Load_Variables.withColumn('run_id', lit(_run_id_resolved))
    # field 'current_date' from variable string '${CURRENT_DATE}'
    # preserved.field.current_date.trim_type='none'
    # preserved.field.current_date.type='String'
    _current_date_resolved = None
    _dbu__current_date_resolved = globals().get('dbutils')
    if _dbu__current_date_resolved is not None and hasattr(_dbu__current_date_resolved, 'widgets'):
        try:
            _current_date_resolved = _dbu__current_date_resolved.widgets.get('CURRENT_DATE')
        except Exception:
            _current_date_resolved = None
    if _current_date_resolved in (None, ''):
        import os as _os__current_date_resolved
        _current_date_resolved = _os__current_date_resolved.environ.get('CURRENT_DATE')
    if _current_date_resolved in (None, ''):
        try:
            _current_date_resolved = spark.conf.get('pentaho.var.CURRENT_DATE')
        except Exception:
            _current_date_resolved = None
    if _current_date_resolved in (None, ''):
        _current_date_resolved = ''
    if _current_date_resolved is None:
        _current_date_resolved = ''
    df_Get_Fact_Load_Variables = df_Get_Fact_Load_Variables.withColumn('current_date', lit(_current_date_resolved))

    # Step: Read Existing FactInventory (CsvInput) [converted]
    # CSV Input: Read Existing FactInventory
    df_Read_Existing_FactInventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_sk INT, inventory_id STRING, inventory_bk_checksum STRING, version_number INT, stock_value DOUBLE')
        .load(f'{data_dir}/fact_inventory_current.csv')
    )

    # Step: Write Fact Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Fact Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/inventory/inventory_fact_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Fact_Rejects = df_Write_Fact_Rejects
    _out_df_Write_Fact_Rejects = df_Write_Fact_Rejects.select('inventory_id', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Fact_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_fact_rejects_.csv')

    # Step: Read Lookuped Inventory (CsvInput) [converted]
    # CSV Input: Read Lookuped Inventory
    df_Read_Lookuped_Inventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, product_id STRING, store_id STRING, supplier_id STRING, region_id STRING, last_stocktake_date STRING, warehouse_code STRING, quantity_clean STRING, available_qty_clean STRING, stock_value STRING, reorder_flag STRING, abc_classification STRING, xyz_classification STRING, safety_stock STRING, economic_order_quantity STRING, dead_stock_flag STRING, excess_stock_flag STRING, shortage_flag STRING, inventory_bk_checksum STRING, product_sk STRING, supplier_sk STRING, store_sk STRING, region_sk STRING, date_sk STRING, inventory_combo_sk STRING, inventory_sk STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/inventory_lookuped_.csv')
    )

    # Step: Prepare Existing Fact Keys (SelectValues) [converted]
    # Select Values: Prepare Existing Fact Keys
    df_Prepare_Existing_Fact_Keys = df_Read_Existing_FactInventory.select(col("inventory_sk").alias("existing_inventory_sk"), col("inventory_id").alias("existing_inventory_id"), col("inventory_bk_checksum").alias("existing_checksum"), col("version_number").alias("existing_version"))

    # Step: Inject CDC And Audit Constants (Constant) [converted]
    # Add Constants: Inject CDC And Audit Constants
    df_Inject_CDC_And_Audit_Constants = df_Read_Lookuped_Inventory
    df_Inject_CDC_And_Audit_Constants = df_Inject_CDC_And_Audit_Constants.withColumn("current_date", lit('${CURRENT_DATE}'))
    # preserved.current_date: length='-1', precision='-1'
    df_Inject_CDC_And_Audit_Constants = df_Inject_CDC_And_Audit_Constants.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Inject_CDC_And_Audit_Constants = df_Inject_CDC_And_Audit_Constants.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Inject_CDC_And_Audit_Constants = df_Inject_CDC_And_Audit_Constants.withColumn("load_ts", lit('${CURRENT_DATE}'))
    # preserved.load_ts: length='-1', precision='-1'

    # Step: Merge Inventory CDC (MergeRows) [converted]
    # Merge Rows (Diff): Merge Inventory CDC
    # preserved.flag_field='merge_flag'
    # preserved.reference='Prepare Existing Fact Keys'
    # preserved.compare='Inject CDC And Audit Constants'
    # preserved.key_fields=['inventory_id']
    # preserved.value_fields=['inventory_bk_checksum']
    _ref_df_Merge_Inventory_CDC = df_Prepare_Existing_Fact_Keys.alias("r")
    _cmp_df_Merge_Inventory_CDC = df_Inject_CDC_And_Audit_Constants.alias("c")
    # WARNING: MergeRows 'Merge Inventory CDC': null join keys do not match under Spark equality; duplicate keys expand to a product within the key group
    df_Merge_Inventory_CDC = _ref_df_Merge_Inventory_CDC.join(_cmp_df_Merge_Inventory_CDC, (col("r.inventory_id") == col("c.inventory_id")), 'full_outer')
    df_Merge_Inventory_CDC = df_Merge_Inventory_CDC.withColumn('merge_flag', when(col("c.inventory_id").isNull(), lit("deleted")).when(col("r.inventory_id").isNull(), lit("new")).when((~col("r.inventory_bk_checksum").eqNullSafe(col("c.inventory_bk_checksum"))), lit("changed")).otherwise(lit("identical")))
    # NOTE: MergeRows 'Merge Inventory CDC': output prefers compare values (CDC-style); deleted rows keep reference values
    df_Merge_Inventory_CDC = df_Merge_Inventory_CDC.select(coalesce(col("c.inventory_id"), col("r.inventory_id")).alias('inventory_id'), coalesce(col("c.inventory_bk_checksum"), col("r.inventory_bk_checksum")).alias('inventory_bk_checksum'), col('merge_flag'))
    # NOTE: MergeRows flags — deleted / new / changed / identical (requires pre-sorted inputs in PDI; Spark join does not enforce sort order)

    # Step: CDC Changed Rows? (FilterRows) [failed]
    # Filter Rows: CDC Changed Rows?
    df_Apply_CDC_Date_Gate = df_Merge_Inventory_CDC.filter(col("inventory_id").isNotNull())
    df_Skip_Empty_CDC = df_Merge_Inventory_CDC.filter(~(col("inventory_id").isNotNull()))
    df_CDC_Changed_Rows? = df_Apply_CDC_Date_Gate

    # Step: Apply CDC Date Gate (Formula) [failed]
    # Formula: Apply CDC Date Gate
    df_Apply_CDC_Date_Gate = df_CDC_Changed_Rows?
    df_Apply_CDC_Date_Gate = df_Apply_CDC_Date_Gate.withColumn('formula_result', lit(None))  # empty formula

    # Step: Skip Empty CDC (Dummy) [converted]
    # Dummy: Skip Empty CDC
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Empty_CDC = df_Skip_Empty_CDC

    # Step: Include CDC Row? (FilterRows) [failed]
    # Filter Rows: Include CDC Row?
    df_Fact_Product_Dimension_Lookup = df_Apply_CDC_Date_Gate.filter((col("cdc_include") == lit('Y')))
    df_Skip_Unchanged_Inventory = df_Apply_CDC_Date_Gate.filter(~((col("cdc_include") == lit('Y'))))
    df_Include_CDC_Row? = df_Fact_Product_Dimension_Lookup

    # Step: Fact Product Dimension Lookup (DimensionLookup) [failed]
    # Dimension Lookup/Update: Fact Product Dimension Lookup
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Fact Product Dimension Lookup': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_product' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Fact Product Dimension Lookup'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (product_id)
    _dim_df_Fact_Product_Dimension_Lookup = spark.table('main.retail_dwh.dim_product').select("product_sk", "product_id", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Fact_Product_Dimension_Lookup = broadcast(_dim_df_Fact_Product_Dimension_Lookup)
    # Effective dating: last_stocktake_date between effective_start_date and effective_end_date
    _dim_active_df_Fact_Product_Dimension_Lookup = _dim_df_Fact_Product_Dimension_Lookup
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_Include_CDC_Row?.join(_dim_active_df_Fact_Product_Dimension_Lookup, on=((df_Include_CDC_Row?["product_id"] == _dim_active_df_Fact_Product_Dimension_Lookup["product_id"]) & (df_Include_CDC_Row?["last_stocktake_date"] >= _dim_active_df_Fact_Product_Dimension_Lookup["effective_start_date"]) & (df_Include_CDC_Row?["last_stocktake_date"] < _dim_active_df_Fact_Product_Dimension_Lookup["effective_end_date"])), how='left')
    df_Fact_Product_Dimension_Lookup = _dim_joined
    # Lookup-only: null 'product_sk' indicates cache miss / unknown BK

    # Step: Skip Unchanged Inventory (Dummy) [converted]
    # Dummy: Skip Unchanged Inventory
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Unchanged_Inventory = df_Skip_Unchanged_Inventory

    # Step: Fact Combination Lookup (CombinationLookup) [converted]
    # Combination Lookup/Update: Fact Combination Lookup
    # preserved.connection='conn_dev_dwh'
    # WARNING: CombinationLookup 'Fact Combination Lookup': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_inventory_combo' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=9999
    # preserved.preload_cache=True
    # preserved.use_hash=True
    # preserved.hash_field='inventory_bk_crc'
    # WARNING: CombinationLookup 'Fact Combination Lookup': CRC/hash cache ('inventory_bk_crc') is database-specific — business-key equi-join used instead; metadata preserved.
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Fact Combination Lookup'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Edge cases: null business keys skipped from insert; duplicate combinations deduplicated before MERGE
    _combo_dim_df_Fact_Combination_Lookup = spark.table('main.retail_dwh.dim_inventory_combo').select("technical_key", "inventory_id", "product_id", "store_id")
    # Cache: broadcast join approximates Pentaho preload/cache
    _combo_dim_df_Fact_Combination_Lookup = broadcast(_combo_dim_df_Fact_Combination_Lookup)
    _combo_dim_df_Fact_Combination_Lookup = _combo_dim_df_Fact_Combination_Lookup.select(col("inventory_id"), col("product_id"), col("store_id"), col("technical_key"))
    _combo_joined = df_Fact_Product_Dimension_Lookup.join(_combo_dim_df_Fact_Combination_Lookup, on=["inventory_id", "product_id", "store_id"], how='left')
    _combo_miss_df_Fact_Combination_Lookup = _combo_joined.filter(col("technical_key").isNull() & ~(col("inventory_id").isNull() | col("product_id").isNull() | col("store_id").isNull()))
    _combo_miss_df_Fact_Combination_Lookup = _combo_miss_df_Fact_Combination_Lookup.dropDuplicates(["inventory_id", "product_id", "store_id"])
    _combo_ins_df_Fact_Combination_Lookup = _combo_miss_df_Fact_Combination_Lookup.select(col("inventory_id").alias("inventory_id"), col("product_id").alias("product_id"), col("store_id").alias("store_id"))
    # tablemax + row_number (IDENTITY would omit tk from INSERT below)
    _max_tk = spark.sql("SELECT COALESCE(MAX(`technical_key`), 0) AS m FROM main.retail_dwh.dim_inventory_combo").collect()[0][0]
    from pyspark.sql.window import Window as _DWWindow
    _combo_ins_df_Fact_Combination_Lookup = _combo_ins_df_Fact_Combination_Lookup.withColumn("_dw_rn", row_number().over(_DWWindow.orderBy(lit(1))))
    _combo_ins_df_Fact_Combination_Lookup = _combo_ins_df_Fact_Combination_Lookup.withColumn("technical_key", (lit(_max_tk) + col("_dw_rn")).cast("long")).drop("_dw_rn")
    _combo_ins_df_Fact_Combination_Lookup.createOrReplaceTempView('_combo_insert_src_df_Fact_Combination_Lookup')
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_inventory_combo').alias("t")
        .merge(spark.table('_combo_insert_src_df_Fact_Combination_Lookup').alias("s"), 't.`inventory_id` <=> s.`inventory_id` AND t.`product_id` <=> s.`product_id` AND t.`store_id` <=> s.`store_id`')
        .whenNotMatchedInsert(values={'inventory_id': 's.`inventory_id`', 'product_id': 's.`product_id`', 'store_id': 's.`store_id`', 'technical_key': 's.`technical_key`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Attach TK without re-scanning dimension: union prior keys with inserts (map table fields back to stream names)
    _combo_new_keys = _combo_ins_df_Fact_Combination_Lookup.select(col("inventory_id"), col("product_id"), col("store_id"), col("technical_key"))
    _combo_dim_df_Fact_Combination_Lookup = _combo_dim_df_Fact_Combination_Lookup.unionByName(_combo_new_keys)
    _combo_dim_df_Fact_Combination_Lookup = broadcast(_combo_dim_df_Fact_Combination_Lookup)
    df_Fact_Combination_Lookup = df_Fact_Product_Dimension_Lookup.join(_combo_dim_df_Fact_Combination_Lookup, on=["inventory_id", "product_id", "store_id"], how='left')
    # Null surrogate keys after MERGE indicate unresolved/null business keys

    # Step: Add Fact Audit And Version (Formula) [converted]
    # Formula: Add Fact Audit And Version
    df_Add_Fact_Audit_And_Version = df_Fact_Combination_Lookup
    df_Add_Fact_Audit_And_Version = df_Add_Fact_Audit_And_Version.withColumn('formula_result', lit(None))  # empty formula

    # Step: Increment Version Control (Calculator) [converted]
    # Calculator: Increment Version Control
    df_Increment_Version_Control = df_Add_Fact_Audit_And_Version
    df_Increment_Version_Control = df_Increment_Version_Control.withColumn("version_number_next", (col("version_number")).cast('int'))

    # Step: Route Fact Action (SwitchCase) [converted]
    # Switch / Case: Route Fact Action
    # preserved.fieldname='fact_action'
    # preserved.switch_field='fact_action'
    # preserved.cases=[{'value': 'INSERT', 'target_step': 'Insert Inventory Fact'}, {'value': 'UPDATE', 'target_step': 'Update Inventory Fact'}]
    # preserved.default_target_step='Update Inventory Fact'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'INSERT', 'target_step': 'Insert Inventory Fact'}, {'value': 'UPDATE', 'target_step': 'Update Inventory Fact'}]
    _routed_df_Route_Fact_Action = df_Increment_Version_Control.withColumn('_route_Route_Fact_Action', when(col("fact_action") == lit('INSERT'), lit('Insert Inventory Fact')).when(col("fact_action") == lit('UPDATE'), lit('Update Inventory Fact')).otherwise(lit('Update Inventory Fact')))
    df_Insert_Inventory_Fact = _routed_df_Route_Fact_Action.filter(col('_route_Route_Fact_Action') == lit('Insert Inventory Fact')).drop('_route_Route_Fact_Action')
    df_Update_Inventory_Fact = _routed_df_Route_Fact_Action.filter(col('_route_Route_Fact_Action') == lit('Update Inventory Fact')).drop('_route_Route_Fact_Action')
    df_Route_Fact_Action = df_Insert_Inventory_Fact

    # Step: Insert Inventory Fact (InsertUpdate) [converted]
    # Insert/Update: Insert Inventory Fact
    _upsert_src = df_Route_Fact_Action
    _upsert_src.createOrReplaceTempView('_upsert_src')
    spark.sql('''MERGE INTO main.retail_dwh.fact_inventory t USING _upsert_src s ON t.`inventory_id` = s.`inventory_id` WHEN MATCHED THEN UPDATE SET t.`inventory_sk` = s.`inventory_sk`, t.`product_sk` = s.`product_sk`, t.`store_sk` = s.`store_sk`, t.`supplier_sk` = s.`supplier_sk`, t.`date_sk` = s.`date_sk`, t.`quantity` = s.`quantity`, t.`available_qty` = s.`available_qty`, t.`stock_value` = s.`stock_value`, t.`version_number` = s.`version_number`, t.`batch_id` = s.`batch_id`, t.`run_id` = s.`run_id`, t.`load_ts` = s.`load_ts` WHEN NOT MATCHED THEN INSERT (`inventory_sk`, `product_sk`, `store_sk`, `supplier_sk`, `date_sk`, `quantity`, `available_qty`, `stock_value`, `version_number`, `batch_id`, `run_id`, `load_ts`) VALUES (s.`inventory_sk`, s.`product_sk`, s.`store_sk`, s.`supplier_sk`, s.`date_sk`, s.`quantity`, s.`available_qty`, s.`stock_value`, s.`version_number`, s.`batch_id`, s.`run_id`, s.`load_ts`)''')
    df_Insert_Inventory_Fact = df_Route_Fact_Action

    # Step: Update Inventory Fact (InsertUpdate) [converted]
    # Insert/Update: Update Inventory Fact
    _upsert_src = df_Route_Fact_Action
    _upsert_src.createOrReplaceTempView('_upsert_src')
    spark.sql('''MERGE INTO main.retail_dwh.fact_inventory t USING _upsert_src s ON t.`inventory_id` = s.`inventory_id` WHEN MATCHED THEN UPDATE SET t.`quantity` = s.`quantity`, t.`available_qty` = s.`available_qty`, t.`stock_value` = s.`stock_value`, t.`version_number_next` = s.`version_number`, t.`batch_id` = s.`batch_id`, t.`run_id` = s.`run_id`, t.`load_ts` = s.`load_ts` WHEN NOT MATCHED THEN INSERT (`quantity`, `available_qty`, `stock_value`, `version_number_next`, `batch_id`, `run_id`, `load_ts`) VALUES (s.`quantity`, s.`available_qty`, s.`stock_value`, s.`version_number`, s.`batch_id`, s.`run_id`, s.`load_ts`)''')
    df_Update_Inventory_Fact = df_Route_Fact_Action

    # Step: Table Output FactInventory (TableOutput) [converted]
    # Pentaho step: Table Output FactInventory (type: TableOutput) (Pentaho schema: retail_dwh)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Table_Output_FactInventory = df_Insert_Inventory_Fact.select(col('inventory_sk'), col('inventory_id'), col('product_sk'), col('store_sk'), col('supplier_sk'), col('date_sk'), col('quantity'), col('available_qty'), col('stock_value'), col('version_number'), col('batch_id'), col('run_id'), col('load_ts'))
    df_Table_Output_FactInventory = _mapped_df_Table_Output_FactInventory
    write_delta(
        df_Table_Output_FactInventory,
        f"{catalog}.{schema}.fact_inventory",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.fact_inventory", mode='append')

    # Step: Write Fact Inventory Snapshot (TextFileOutput) [converted]
    # Pentaho step: Write Fact Inventory Snapshot (type: TextFileOutput)
    # Pentaho filename: /output/inventory/fact/fact_inventory_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='available_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='stock_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='version_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='fact_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='load_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Fact_Inventory_Snapshot = df_Table_Output_FactInventory
    _out_df_Write_Fact_Inventory_Snapshot = df_Write_Fact_Inventory_Snapshot.select('inventory_sk', 'inventory_id', 'product_sk', 'store_sk', 'supplier_sk', 'date_sk', 'quantity', 'available_qty', 'stock_value', 'version_number', 'fact_action', 'batch_id', 'run_id', 'load_ts')
    writer = _out_df_Write_Fact_Inventory_Snapshot.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/fact_inventory_.csv')

    # Step: Count Fact Actions (MemoryGroupBy) [converted]
    # Memory Group By: Count Fact Actions
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Fact_Actions = df_Write_Fact_Inventory_Snapshot.groupBy('fact_action').agg(count(lit(1)).alias('action_count'))

    # Step: Single Thread Fact Completion (SingleThreader) [converted]
    # Single Threader: Single Thread Fact Completion
    # preserved.filename='${PROJECT_HOME}/transformations/cleansing/TR_Inventory_Cleansing.ktr'
    # preserved.pass_parameters=False
    # LIMITATION: Spark executes distributively; single-threaded inject/retrieve sub-pipelines cannot be preserved. Inline or call child notebook sequentially.
    _single_threader_df_Single_Thread_Fact_Completion = {'child': '${PROJECT_HOME}/transformations/cleansing/TR_Inventory_Cleansing.ktr', 'inject_step': '', 'retrieve_step': '', 'batch_size': '', 'parameters': []}
    df_Single_Thread_Fact_Completion = df_Write_Fact_Inventory_Snapshot

    # Step: Write Fact Metrics Log (TextFileOutput) [converted]
    # Pentaho step: Write Fact Metrics Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/inventory/TR_FactInventory_Load_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='fact_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='action_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Fact_Metrics_Log = df_Count_Fact_Actions
    _out_df_Write_Fact_Metrics_Log = df_Write_Fact_Metrics_Log.select('fact_action', 'action_count')
    writer = _out_df_Write_Fact_Metrics_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_FactInventory_Load_.log')

    # Step: Execute Inventory Audit Subtrans (TransExecutor) [converted]
    # Transformation Executor: Execute Inventory Audit Subtrans
    # preserved.filename='${PROJECT_HOME}/transformations/utilities/TR_Inventory_Audit.ktr'
    # preserved.inherit_all_variables=False
    # LIMITATION: Nested Transformation Executor should become a separate notebook/job invoked with parameter mappings; row-batch grouping is not inlined.
    _exec_meta_df_Execute_Inventory_Audit_Subtrans = {'child': '${PROJECT_HOME}/transformations/utilities/TR_Inventory_Audit.ktr', 'parameters': [], 'group_size': '', 'group_field': '', 'group_time': ''}
    # TODO: dbutils.notebook.run / Jobs API using _exec_meta_df_Execute_Inventory_Audit_Subtrans
    df_Execute_Inventory_Audit_Subtrans = df_Single_Thread_Fact_Completion

    # Step: Block Fact Completion (BlockingStep) [converted]
    # Blocking Step: Block Fact Completion
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Fact_Completion = cache_for_reuse(df_Execute_Inventory_Audit_Subtrans)
    _ = df_Block_Fact_Completion.count()  # synchronize: wait for all upstream rows

    # Step: Log Fact Load Complete (WriteToLog) [converted]
    # Write to Log: Log Fact Load Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=FACT_LOAD_OK | TRANS=TR_FactInventory_Load | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Fact_Load_Complete = logging.getLogger('pentaho.writetolog.Log_Fact_Load_Complete')
    _log_df_Log_Fact_Load_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Fact_Load_Complete = df_Block_Fact_Completion
    _log_rows_df_Log_Fact_Load_Complete = _log_df_df_Log_Fact_Load_Complete.take(20)
    _log_df_Log_Fact_Load_Complete.info('Log Fact Load Complete' + ' | columns=' + str(_log_df_df_Log_Fact_Load_Complete.columns))
    _log_df_Log_Fact_Load_Complete.info('AUDIT | EVENT=FACT_LOAD_OK | TRANS=TR_FactInventory_Load | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Fact_Load_Complete:
        _log_df_Log_Fact_Load_Complete.info('Log Fact Load Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Fact_Load_Complete = df_Block_Fact_Completion

    # Step: Fact Inventory Complete (Dummy) [converted]
    # Dummy: Fact Inventory Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Fact_Inventory_Complete = df_Log_Fact_Load_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Fact_Inventory_Complete
