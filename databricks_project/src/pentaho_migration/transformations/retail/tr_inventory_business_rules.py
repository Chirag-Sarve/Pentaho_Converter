"""PySpark module migrated from Pentaho transformation: TR_Inventory_Business_Rules.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/TR_Inventory_Business_Rules.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_date,
    length,
    lit,
    when,
    coalesce,
    md5,
    concat,
    sum as _sum,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_inventory_business_rules")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Inventory_Business_Rules`` step-for-step."""
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

    # Step: Get Business Rule Vars (GetVariable) [converted]
    # Get Variables: Get Business Rule Vars
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Business_Rule_Vars = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Business_Rule_Vars = df_Get_Business_Rule_Vars.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Business_Rule_Vars = df_Get_Business_Rule_Vars.withColumn('run_id', lit(_run_id_resolved))
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
    df_Get_Business_Rule_Vars = df_Get_Business_Rule_Vars.withColumn('current_date', lit(_current_date_resolved))

    # Step: Read Inventory Policy For Rules (JsonInput) [converted]
    # JSON Input: Read Inventory Policy For Rules
    df_Read_Inventory_Policy_For_Rules = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/inventory_validation_policy.json')

    # Step: Read Clean Inventory (CsvInput) [converted]
    # CSV Input: Read Clean Inventory
    df_Read_Clean_Inventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, store_id STRING, product_id STRING, quantity_on_hand STRING, quantity_reserved STRING, reorder_level STRING, reorder_quantity STRING, last_stocktake_date STRING, bin_location STRING, is_low_stock STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, store_name STRING, store_type STRING, region_id STRING, square_footage STRING, supplier_name STRING, lead_time_days STRING, supplier_active STRING, warehouse_code STRING, bin_slot STRING, batch_number STRING, expiry_date STRING, maximum_stock STRING, minimum_stock STRING, quantity STRING, available_qty STRING, batch_id STRING, run_id STRING, extract_ts STRING, source_row_num STRING, warehouse_name STRING, unit_of_measure_std STRING, quantity_clean STRING, available_qty_clean STRING, inventory_nk STRING, inventory_bk_checksum STRING, merge_flag STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/inventory_clean_.csv')
    )

    # Step: Prepare Policy Lookup (SelectValues) [converted]
    # Select Values: Prepare Policy Lookup
    df_Prepare_Policy_Lookup = df_Read_Inventory_Policy_For_Rules.select(col("carrying_cost_rate").alias("pol_carry"), col("safety_stock_z").alias("pol_z"), col("dead_stock_days").alias("pol_dead"))

    # Step: Cast Business Rule Numerics (SelectValues) [converted]
    # Select Values: Cast Business Rule Numerics
    df_Cast_Business_Rule_Numerics = df_Read_Clean_Inventory.select(col("inventory_id").alias("inventory_id"), col("store_id").alias("store_id"), col("product_id").alias("product_id"), col("quantity_on_hand").alias("quantity_on_hand"), col("quantity_reserved").alias("quantity_reserved"), col("reorder_level").alias("reorder_level"), col("reorder_quantity").alias("reorder_quantity"), col("last_stocktake_date").alias("last_stocktake_date"), col("bin_location").alias("bin_location"), col("is_low_stock").alias("is_low_stock"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("store_name").alias("store_name"), col("store_type").alias("store_type"), col("region_id").alias("region_id"), col("square_footage").alias("square_footage"), col("supplier_name").alias("supplier_name"), col("lead_time_days").alias("lead_time_days"), col("supplier_active").alias("supplier_active"), col("warehouse_code").alias("warehouse_code"), col("bin_slot").alias("bin_slot"), col("batch_number").alias("batch_number"), col("expiry_date").alias("expiry_date"), col("maximum_stock").alias("maximum_stock"), col("minimum_stock").alias("minimum_stock"), col("quantity").alias("quantity"), col("available_qty").alias("available_qty"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"), col("warehouse_name").alias("warehouse_name"), col("unit_of_measure_std").alias("unit_of_measure_std"), col("quantity_clean").alias("quantity_clean"), col("available_qty_clean").alias("available_qty_clean"), col("inventory_nk").alias("inventory_nk"), col("inventory_bk_checksum").alias("inventory_bk_checksum"), col("merge_flag").alias("merge_flag"))

    # Step: Policy Stream Consumed (Dummy) [converted]
    # Dummy: Policy Stream Consumed
    # Pass-through step - DataFrame unchanged
    df_Dummy_Policy_Stream_Consumed = df_Prepare_Policy_Lookup

    # Step: Inject Policy Defaults (Constant) [converted]
    # Add Constants: Inject Policy Defaults
    df_Inject_Policy_Defaults = df_Cast_Business_Rule_Numerics
    df_Inject_Policy_Defaults = df_Inject_Policy_Defaults.withColumn("carrying_cost_rate", lit(0.25))
    # preserved.carrying_cost_rate: length='-1', precision='-1'
    df_Inject_Policy_Defaults = df_Inject_Policy_Defaults.withColumn("safety_stock_z", lit(1.65))
    # preserved.safety_stock_z: length='-1', precision='-1'
    df_Inject_Policy_Defaults = df_Inject_Policy_Defaults.withColumn("dead_stock_days", lit(180.0))
    # preserved.dead_stock_days: length='-1', precision='-1'

    # Step: Calculate Stock Value (Calculator) [converted]
    # Calculator: Calculate Stock Value
    df_Calculate_Stock_Value = df_Inject_Policy_Defaults
    df_Calculate_Stock_Value = df_Calculate_Stock_Value.withColumn("stock_value", ((col("quantity_clean") * col("unit_cost"))).cast('decimal(38,4)'))

    # Step: Calculate Inventory KPIs (Formula) [converted]
    # Formula: Calculate Inventory KPIs
    df_Calculate_Inventory_KPIs = df_Calculate_Stock_Value
    df_Calculate_Inventory_KPIs = df_Calculate_Inventory_KPIs.withColumn('formula_result', lit(None))  # empty formula

    # Step: ABC Classification (NumberRange) [converted]
    # Number Range: ABC Classification
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_ABC_Classification = df_Calculate_Inventory_KPIs.withColumn('abc_classification', when(col("stock_value").isNull(), lit('C')).otherwise(when((col("stock_value").cast("double") >= lit(0.0)) & (col("stock_value").cast("double") < lit(500.0)), lit('C')).when((col("stock_value").cast("double") >= lit(500.01)) & (col("stock_value").cast("double") < lit(5000.0)), lit('B')).when((col("stock_value").cast("double") >= lit(5000.01)) & (col("stock_value").cast("double") < lit(999999999.0)), lit('A')).otherwise(lit('C'))))
    # preserved.fallback='C' rules=3 lower_inclusive=True upper_inclusive=False

    # Step: XYZ Classification (NumberRange) [converted]
    # Number Range: XYZ Classification
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_XYZ_Classification = df_ABC_Classification.withColumn('xyz_classification', when(col("xyz_cv_proxy").isNull(), lit('Z')).otherwise(when((col("xyz_cv_proxy").cast("double") >= lit(0.0)) & (col("xyz_cv_proxy").cast("double") < lit(0.5)), lit('X')).when((col("xyz_cv_proxy").cast("double") >= lit(0.5001)) & (col("xyz_cv_proxy").cast("double") < lit(1.0)), lit('Y')).when((col("xyz_cv_proxy").cast("double") >= lit(1.0001)) & (col("xyz_cv_proxy").cast("double") < lit(999.0)), lit('Z')).otherwise(lit('Z'))))
    # preserved.fallback='Z' rules=3 lower_inclusive=True upper_inclusive=False

    # Step: Set Rule Engine Version (SetValueField) [converted]
    # Set Field Value: Set Rule Engine Version
    df_Set_Rule_Engine_Version = df_XYZ_Classification
    df_Set_Rule_Engine_Version = df_Set_Rule_Engine_Version.withColumn("rule_engine_version", col("INV-BR-1.0"))

    # Step: Hash Enriched Inventory (CheckSum) [converted]
    # Add a Checksum: Hash Enriched Inventory
    df_Hash_Enriched_Inventory = df_Set_Rule_Engine_Version
    df_Hash_Enriched_Inventory = df_Hash_Enriched_Inventory.withColumn("inventory_rule_crc", md5(concat(coalesce(col("inventory_id").cast("string"), lit("")), coalesce(col("stock_value").cast("string"), lit("")), coalesce(col("reorder_flag").cast("string"), lit("")), coalesce(col("abc_classification").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['inventory_id', 'stock_value', 'reorder_flag', 'abc_classification']

    # Step: Dead Stock? (FilterRows) [failed]
    # Filter Rows: Dead Stock?
    df_Dead_Stock_Branch = df_Hash_Enriched_Inventory.filter((col("dead_stock_flag") == lit('Y')))
    df_Live_Stock_Branch = df_Hash_Enriched_Inventory.filter(~((col("dead_stock_flag") == lit('Y'))))
    df_Dead_Stock? = df_Dead_Stock_Branch

    # Step: Route Stock Status (SwitchCase) [converted]
    # Switch / Case: Route Stock Status
    # preserved.fieldname='reorder_flag'
    # preserved.switch_field='reorder_flag'
    # preserved.cases=[{'value': 'Y', 'target_step': 'Flag Reorder Path'}, {'value': 'N', 'target_step': 'Flag Balanced Path'}]
    # preserved.default_target_step='Flag Balanced Path'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'Y', 'target_step': 'Flag Reorder Path'}, {'value': 'N', 'target_step': 'Flag Balanced Path'}]
    _routed_df_Route_Stock_Status = df_Hash_Enriched_Inventory.withColumn('_route_Route_Stock_Status', when(col("reorder_flag") == lit('Y'), lit('Flag Reorder Path')).when(col("reorder_flag") == lit('N'), lit('Flag Balanced Path')).otherwise(lit('Flag Balanced Path')))
    df_Flag_Reorder_Path = _routed_df_Route_Stock_Status.filter(col('_route_Route_Stock_Status') == lit('Flag Reorder Path')).drop('_route_Route_Stock_Status')
    df_Flag_Balanced_Path = _routed_df_Route_Stock_Status.filter(col('_route_Route_Stock_Status') == lit('Flag Balanced Path')).drop('_route_Route_Stock_Status')
    df_Route_Stock_Status = df_Flag_Reorder_Path

    # Step: Dead Stock Branch (Dummy) [converted]
    # Dummy: Dead Stock Branch
    # Pass-through step - DataFrame unchanged
    df_Dummy_Dead_Stock_Branch = df_Dead_Stock_Branch

    # Step: Live Stock Branch (Dummy) [converted]
    # Dummy: Live Stock Branch
    # Pass-through step - DataFrame unchanged
    df_Dummy_Live_Stock_Branch = df_Live_Stock_Branch

    # Step: Flag Balanced Path (Dummy) [converted]
    # Dummy: Flag Balanced Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_Flag_Balanced_Path = df_Flag_Balanced_Path

    # Step: Flag Reorder Path (Dummy) [converted]
    # Dummy: Flag Reorder Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_Flag_Reorder_Path = df_Flag_Reorder_Path

    # Step: Clone Cycle Count Simulation (CloneRow) [converted]
    # Clone Row: Clone Cycle Count Simulation
    # preserved.nr_clones=2
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='2'
    _clone_parts_df_Clone_Cycle_Count_Simulation = []
    _base_df_Clone_Cycle_Count_Simulation = df_Dummy_Flag_Reorder_Path
    _orig_df_Clone_Cycle_Count_Simulation = _base_df_Clone_Cycle_Count_Simulation
    _clone_parts_df_Clone_Cycle_Count_Simulation.append(_orig_df_Clone_Cycle_Count_Simulation)
    for _ci in range(1, 2 + 1):
        _c = _base_df_Clone_Cycle_Count_Simulation
        _clone_parts_df_Clone_Cycle_Count_Simulation.append(_c)
    df_Clone_Cycle_Count_Simulation = _clone_parts_df_Clone_Cycle_Count_Simulation[0]
    for _part in _clone_parts_df_Clone_Cycle_Count_Simulation[1:]:
        df_Clone_Cycle_Count_Simulation = df_Clone_Cycle_Count_Simulation.unionByName(_part, allowMissingColumns=True)

    # Step: Tag Cycle Count Pass (Constant) [converted]
    # Add Constants: Tag Cycle Count Pass
    df_Tag_Cycle_Count_Pass = df_Clone_Cycle_Count_Simulation
    df_Tag_Cycle_Count_Pass = df_Tag_Cycle_Count_Pass.withColumn("cycle_count_pass", lit('1'))
    # preserved.cycle_count_pass: length='-1', precision='-1'

    # Step: Simulate Stock Transfer Qty (Formula) [converted]
    # Formula: Simulate Stock Transfer Qty
    df_Simulate_Stock_Transfer_Qty = df_Tag_Cycle_Count_Pass
    df_Simulate_Stock_Transfer_Qty = df_Simulate_Stock_Transfer_Qty.withColumn('formula_result', lit(None))  # empty formula

    # Step: Dead Rows For Report? (FilterRows) [failed]
    # Filter Rows: Dead Rows For Report?
    df_Write_Dead_Stock_Report = df_Simulate_Stock_Transfer_Qty.filter((col("dead_stock_flag") == lit('Y')))
    df_Skip_Non_Dead = df_Simulate_Stock_Transfer_Qty.filter(~((col("dead_stock_flag") == lit('Y'))))
    df_Dead_Rows_For_Report? = df_Write_Dead_Stock_Report

    # Step: Inventory Summary Agg (MemoryGroupBy) [partial]
    # Memory Group By: Inventory Summary Agg
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Inventory_Summary_Agg = df_Simulate_Stock_Transfer_Qty.groupBy('abc_classification').agg(count(lit(1)).alias('inventory_count'), _sum(col("stock_value")).alias('total_stock_value'), avg(col("days_of_inventory")).alias('avg_doi'))

    # Step: Reorder Rows? (FilterRows) [failed]
    # Filter Rows: Reorder Rows?
    df_Write_Reorder_Report = df_Simulate_Stock_Transfer_Qty.filter((col("reorder_flag") == lit('Y')))
    df_Skip_Non_Reorder = df_Simulate_Stock_Transfer_Qty.filter(~((col("reorder_flag") == lit('Y'))))
    df_Reorder_Rows? = df_Write_Reorder_Report

    # Step: Supplier Performance (MemoryGroupBy) [converted]
    # Memory Group By: Supplier Performance
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Supplier_Performance = df_Simulate_Stock_Transfer_Qty.groupBy('supplier_id').agg(count(lit(1)).alias('sup_rows'), _sum(col("stock_value")).alias('sup_value'), avg(col("lead_time_days")).alias('sup_lead'))

    # Step: Warehouse Balancing (MemoryGroupBy) [partial]
    # Memory Group By: Warehouse Balancing
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Warehouse_Balancing = df_Simulate_Stock_Transfer_Qty.groupBy('warehouse_code').agg(_sum(col("quantity_clean")).alias('wh_qty'), _sum(col("stock_value")).alias('wh_value'), avg(col("warehouse_utilization")).alias('wh_util'))

    # Step: Write Enriched Inventory (TextFileOutput) [converted]
    # Pentaho step: Write Enriched Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/enriched/inventory_enriched_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_on_hand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_reserved' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reorder_level' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reorder_quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='last_stocktake_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='bin_location' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_low_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='brand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='weight_kg' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_type' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='square_footage' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='lead_time_days' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='bin_slot' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='expiry_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='maximum_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='minimum_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='available_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_of_measure_std' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_clean' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='available_qty_clean' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_nk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_bk_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='merge_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='stock_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='days_of_inventory' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_turnover' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reorder_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='carrying_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_age' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dead_stock_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='excess_stock_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shortage_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='safety_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='economic_order_quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='abc_classification' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='xyz_classification' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='expiry_watch_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_utilization' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='transfer_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='cycle_count_pass' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_rule_crc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rule_engine_version' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Enriched_Inventory = df_Simulate_Stock_Transfer_Qty
    _out_df_Write_Enriched_Inventory = df_Write_Enriched_Inventory.select('inventory_id', 'store_id', 'product_id', 'quantity_on_hand', 'quantity_reserved', 'reorder_level', 'reorder_quantity', 'last_stocktake_date', 'bin_location', 'is_low_stock', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'store_name', 'store_type', 'region_id', 'square_footage', 'supplier_name', 'lead_time_days', 'supplier_active', 'warehouse_code', 'bin_slot', 'batch_number', 'expiry_date', 'maximum_stock', 'minimum_stock', 'quantity', 'available_qty', 'batch_id', 'run_id', 'extract_ts', 'source_row_num', 'warehouse_name', 'unit_of_measure_std', 'quantity_clean', 'available_qty_clean', 'inventory_nk', 'inventory_bk_checksum', 'merge_flag', 'batch_id', 'run_id', 'stock_value', 'days_of_inventory', 'inventory_turnover', 'reorder_flag', 'carrying_cost', 'inventory_age', 'dead_stock_flag', 'excess_stock_flag', 'shortage_flag', 'safety_stock', 'economic_order_quantity', 'abc_classification', 'xyz_classification', 'expiry_watch_flag', 'warehouse_utilization', 'transfer_qty', 'cycle_count_pass', 'inventory_rule_crc', 'rule_engine_version')
    writer = _out_df_Write_Enriched_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_enriched_.csv')

    # Step: Skip Non Dead (Dummy) [converted]
    # Dummy: Skip Non Dead
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Non_Dead = df_Skip_Non_Dead

    # Step: Write Dead Stock Report (TextFileOutput) [failed]
    # Pentaho step: Write Dead Stock Report (type: TextFileOutput)
    # Pentaho filename: /output/inventory/reports/dead_stock_report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_age' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='stock_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Dead_Stock_Report = df_Dead_Rows_For_Report?
    _out_df_Write_Dead_Stock_Report = df_Write_Dead_Stock_Report.select('inventory_id', 'product_id', 'store_id', 'inventory_age', 'stock_value', 'batch_id', 'run_id')
    writer = _out_df_Write_Dead_Stock_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/dead_stock_report_.csv')

    # Step: Write Inventory Summary (TextFileOutput) [converted]
    # Pentaho step: Write Inventory Summary (type: TextFileOutput)
    # Pentaho filename: /output/inventory/reports/inventory_summary_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='abc_classification' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_stock_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_doi' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Inventory_Summary = df_Inventory_Summary_Agg
    _out_df_Write_Inventory_Summary = df_Write_Inventory_Summary.select('abc_classification', 'inventory_count', 'total_stock_value', 'avg_doi', 'batch_id', 'run_id')
    writer = _out_df_Write_Inventory_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_summary_.csv')

    # Step: Skip Non Reorder (Dummy) [converted]
    # Dummy: Skip Non Reorder
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Non_Reorder = df_Skip_Non_Reorder

    # Step: Write Reorder Report (TextFileOutput) [failed]
    # Pentaho step: Write Reorder Report (type: TextFileOutput)
    # Pentaho filename: /output/inventory/reports/reorder_report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='available_qty_clean' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reorder_level' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='economic_order_quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='safety_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Reorder_Report = df_Reorder_Rows?
    _out_df_Write_Reorder_Report = df_Write_Reorder_Report.select('inventory_id', 'product_id', 'store_id', 'available_qty_clean', 'reorder_level', 'economic_order_quantity', 'safety_stock', 'batch_id', 'run_id')
    writer = _out_df_Write_Reorder_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/reorder_report_.csv')

    # Step: Write Supplier Summary (TextFileOutput) [converted]
    # Pentaho step: Write Supplier Summary (type: TextFileOutput)
    # Pentaho filename: /output/inventory/reports/supplier_summary_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sup_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sup_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sup_lead' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Supplier_Summary = df_Supplier_Performance
    _out_df_Write_Supplier_Summary = df_Write_Supplier_Summary.select('supplier_id', 'sup_rows', 'sup_value', 'sup_lead', 'batch_id', 'run_id')
    writer = _out_df_Write_Supplier_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/supplier_summary_.csv')

    # Step: Append Summary Streams (Append) [converted]
    # Append Streams: Append Summary Streams
    # preserved.head_name='Inventory Summary Agg'
    # preserved.tail_name='Warehouse Balancing'
    # preserved.stream_order=['Inventory Summary Agg', 'Warehouse Balancing']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Summary_Streams = df_Inventory_Summary_Agg.unionByName(df_Warehouse_Balancing, allowMissingColumns=True)

    # Step: Write Warehouse Summary (TextFileOutput) [converted]
    # Pentaho step: Write Warehouse Summary (type: TextFileOutput)
    # Pentaho filename: /output/inventory/reports/warehouse_summary_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='warehouse_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='wh_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='wh_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='wh_util' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Warehouse_Summary = df_Warehouse_Balancing
    _out_df_Write_Warehouse_Summary = df_Write_Warehouse_Summary.select('warehouse_code', 'wh_qty', 'wh_value', 'wh_util', 'batch_id', 'run_id')
    writer = _out_df_Write_Warehouse_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/warehouse_summary_.csv')

    # Step: Log Business Rules Complete (WriteToLog) [converted]
    # Write to Log: Log Business Rules Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=BUSINESS_RULES_OK | TRANS=TR_Inventory_Business_Rules | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Business_Rules_Complete = logging.getLogger('pentaho.writetolog.Log_Business_Rules_Complete')
    _log_df_Log_Business_Rules_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Business_Rules_Complete = df_Write_Enriched_Inventory
    _log_rows_df_Log_Business_Rules_Complete = _log_df_df_Log_Business_Rules_Complete.take(20)
    _log_df_Log_Business_Rules_Complete.info('Log Business Rules Complete' + ' | columns=' + str(_log_df_df_Log_Business_Rules_Complete.columns))
    _log_df_Log_Business_Rules_Complete.info('AUDIT | EVENT=BUSINESS_RULES_OK | TRANS=TR_Inventory_Business_Rules | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Business_Rules_Complete:
        _log_df_Log_Business_Rules_Complete.info('Log Business Rules Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Business_Rules_Complete = df_Write_Enriched_Inventory

    # Step: Business Rules Complete (Dummy) [converted]
    # Dummy: Business Rules Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Business_Rules_Complete = df_Write_Inventory_Summary

    log_event(_LOG, "transformation_end")
    return df_Dummy_Business_Rules_Complete
