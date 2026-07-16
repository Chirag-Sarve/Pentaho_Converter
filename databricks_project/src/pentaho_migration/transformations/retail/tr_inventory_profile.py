"""PySpark module migrated from Pentaho transformation: TR_Inventory_Profile.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Inventory_Profile.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    length,
    lit,
    lower,
    when,
    coalesce,
    row_number,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_inventory_profile")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Inventory_Profile`` step-for-step."""
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

    # Step: Get Profile Variables (GetVariable) [converted]
    # Get Variables: Get Profile Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Profile_Variables = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Profile_Variables = df_Get_Profile_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Profile_Variables = df_Get_Profile_Variables.withColumn('run_id', lit(_run_id_resolved))

    # Step: Read Staged Joined Inventory (CsvInput) [converted]
    # CSV Input: Read Staged Joined Inventory
    df_Read_Staged_Joined_Inventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, store_id STRING, product_id STRING, quantity_on_hand STRING, quantity_reserved STRING, reorder_level STRING, reorder_quantity STRING, last_stocktake_date STRING, bin_location STRING, is_low_stock STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, store_name STRING, store_type STRING, region_id STRING, square_footage STRING, supplier_name STRING, lead_time_days STRING, supplier_active STRING, warehouse_code STRING, bin_slot STRING, batch_number STRING, expiry_date STRING, maximum_stock STRING, minimum_stock STRING, quantity STRING, available_qty STRING, batch_id STRING, run_id STRING, extract_ts STRING, source_row_num STRING')
        .load(f'{data_dir}/stg_joined_inventory_.csv')
    )

    # Step: Add Profile Batch (Constant) [converted]
    # Add Constants: Add Profile Batch
    df_Add_Profile_Batch = df_Read_Staged_Joined_Inventory
    df_Add_Profile_Batch = df_Add_Profile_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Profile_Batch = df_Add_Profile_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Cast Profile Numerics (SelectValues) [converted]
    # Select Values: Cast Profile Numerics
    df_Cast_Profile_Numerics = df_Add_Profile_Batch.select(col("inventory_id").alias("inventory_id"), col("store_id").alias("store_id"), col("product_id").alias("product_id"), col("quantity_on_hand").alias("quantity_on_hand"), col("quantity_reserved").alias("quantity_reserved"), col("reorder_level").alias("reorder_level"), col("reorder_quantity").alias("reorder_quantity"), col("last_stocktake_date").alias("last_stocktake_date"), col("bin_location").alias("bin_location"), col("is_low_stock").alias("is_low_stock"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("store_name").alias("store_name"), col("store_type").alias("store_type"), col("region_id").alias("region_id"), col("square_footage").alias("square_footage"), col("supplier_name").alias("supplier_name"), col("lead_time_days").alias("lead_time_days"), col("supplier_active").alias("supplier_active"), col("warehouse_code").alias("warehouse_code"), col("bin_slot").alias("bin_slot"), col("batch_number").alias("batch_number"), col("expiry_date").alias("expiry_date"), col("maximum_stock").alias("maximum_stock"), col("minimum_stock").alias("minimum_stock"), col("quantity").alias("quantity"), col("available_qty").alias("available_qty"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"))

    # Step: Compute Age And Movement Flags (Formula) [converted]
    # Formula: Compute Age And Movement Flags
    df_Compute_Age_And_Movement_Flags = df_Cast_Profile_Numerics
    df_Compute_Age_And_Movement_Flags = df_Compute_Age_And_Movement_Flags.withColumn('formula_result', lit(None))  # empty formula

    # Step: Clone Profile Fanout (CloneRow) [converted]
    # Clone Row: Clone Profile Fanout
    # preserved.nr_clones=8
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='8'
    _clone_parts_df_Clone_Profile_Fanout = []
    _base_df_Clone_Profile_Fanout = df_Compute_Age_And_Movement_Flags
    _orig_df_Clone_Profile_Fanout = _base_df_Clone_Profile_Fanout
    _clone_parts_df_Clone_Profile_Fanout.append(_orig_df_Clone_Profile_Fanout)
    for _ci in range(1, 8 + 1):
        _c = _base_df_Clone_Profile_Fanout
        _clone_parts_df_Clone_Profile_Fanout.append(_c)
    df_Clone_Profile_Fanout = _clone_parts_df_Clone_Profile_Fanout[0]
    for _part in _clone_parts_df_Clone_Profile_Fanout[1:]:
        df_Clone_Profile_Fanout = df_Clone_Profile_Fanout.unionByName(_part, allowMissingColumns=True)

    # Step: Detect Empty Profile Stream (DetectEmptyStream) [converted]
    # Detect Empty Stream: Detect Empty Profile Stream
    _empty_flag_df_Detect_Empty_Profile_Stream = df_Compute_Age_And_Movement_Flags.limit(1).count() == 0
    # Pentaho semantics: if empty → one null row with input schema; else → empty DataFrame (no rows forwarded)
    if _empty_flag_df_Detect_Empty_Profile_Stream:
        _schema_df_Detect_Empty_Profile_Stream = df_Compute_Age_And_Movement_Flags.schema
        if len(df_Compute_Age_And_Movement_Flags.columns) == 0:
            df_Detect_Empty_Profile_Stream = spark.createDataFrame([], _schema_df_Detect_Empty_Profile_Stream)
        else:
            df_Detect_Empty_Profile_Stream = spark.createDataFrame([tuple(None for _ in df_Compute_Age_And_Movement_Flags.columns)], _schema_df_Detect_Empty_Profile_Stream)
    else:
        df_Detect_Empty_Profile_Stream = df_Compute_Age_And_Movement_Flags.limit(0)
    # Downstream hops receive this single output stream (empty-detection row or zero rows).

    # Step: Sample Profile Peek (SampleRows) [converted]
    # Sample Rows: Sample Profile Peek
    _w_sr_df_Sample_Profile_Peek = Window.orderBy(monotonically_increasing_id())
    df_Sample_Profile_Peek = df_Compute_Age_And_Movement_Flags.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Profile_Peek))
    # preserved.lines_range='1..10' ranges=[(1, 10)]
    df_Sample_Profile_Peek = df_Sample_Profile_Peek.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 10)))
    df_Sample_Profile_Peek = df_Sample_Profile_Peek.drop('_sr_rn')

    # Step: Inactive Products? (FilterRows) [failed]
    # Filter Rows: Inactive Products?
    df_Write_Inactive_Products = df_Clone_Profile_Fanout.filter((col("is_active") == lit('0')))
    df_Skip_Active_Products = df_Clone_Profile_Fanout.filter(~((col("is_active") == lit('0'))))
    df_Inactive_Products? = df_Write_Inactive_Products

    # Step: Inventory Aging Bands (NumberRange) [converted]
    # Number Range: Inventory Aging Bands
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Inventory_Aging_Bands = df_Clone_Profile_Fanout.withColumn('aging_band', when(col("inventory_age_days").isNull(), lit('OTHER')).otherwise(when((col("inventory_age_days").cast("double") >= lit(0.0)) & (col("inventory_age_days").cast("double") < lit(30.0)), lit('FRESH')).when((col("inventory_age_days").cast("double") >= lit(31.0)) & (col("inventory_age_days").cast("double") < lit(90.0)), lit('AGING')).when((col("inventory_age_days").cast("double") >= lit(91.0)) & (col("inventory_age_days").cast("double") < lit(180.0)), lit('SLOW')).when((col("inventory_age_days").cast("double") >= lit(181.0)) & (col("inventory_age_days").cast("double") < lit(99999.0)), lit('STALE')).otherwise(lit('OTHER'))))
    # preserved.fallback='OTHER' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: Negative Inventory? (FilterRows) [failed]
    # Filter Rows: Negative Inventory?
    df_Write_Negative_Inventory = df_Clone_Profile_Fanout.filter((col("negative_flag") == lit('Y')))
    df_Skip_Non_Negative = df_Clone_Profile_Fanout.filter(~((col("negative_flag") == lit('Y'))))
    df_Negative_Inventory? = df_Write_Negative_Inventory

    # Step: Normalise Profile Measures (Normaliser) [converted]
    # Row Normaliser: Normalise Profile Measures
    _norm_df_Normalise_Profile_Measures_0 = df_Clone_Profile_Fanout.select(col("batch_id"), col("batch_number"), col("bin_location"), col("bin_slot"), col("brand"), col("category_id"), col("currency_code"), col("expiry_date"), col("extract_ts"), col("formula_result"), col("inventory_id"), col("is_active"), col("is_low_stock"), col("last_stocktake_date"), col("lead_time_days"), col("maximum_stock"), col("minimum_stock"), col("product_id"), col("product_name"), col("quantity_on_hand"), col("quantity_reserved"), col("region_id"), col("reorder_quantity"), col("run_id"), col("sku"), col("source_row_num"), col("square_footage"), col("store_id"), col("store_name"), col("store_type"), col("supplier_active"), col("supplier_id"), col("supplier_name"), col("unit_cost"), col("unit_price"), col("warehouse_code"), col("weight_kg"), lit('quantity').alias("measure_name"), col("quantity").alias("QTY"))
    _norm_df_Normalise_Profile_Measures_1 = df_Clone_Profile_Fanout.select(col("batch_id"), col("batch_number"), col("bin_location"), col("bin_slot"), col("brand"), col("category_id"), col("currency_code"), col("expiry_date"), col("extract_ts"), col("formula_result"), col("inventory_id"), col("is_active"), col("is_low_stock"), col("last_stocktake_date"), col("lead_time_days"), col("maximum_stock"), col("minimum_stock"), col("product_id"), col("product_name"), col("quantity_on_hand"), col("quantity_reserved"), col("region_id"), col("reorder_quantity"), col("run_id"), col("sku"), col("source_row_num"), col("square_footage"), col("store_id"), col("store_name"), col("store_type"), col("supplier_active"), col("supplier_id"), col("supplier_name"), col("unit_cost"), col("unit_price"), col("warehouse_code"), col("weight_kg"), lit('available_qty').alias("measure_name"), col("available_qty").alias("AVAILABLE"))
    _norm_df_Normalise_Profile_Measures_2 = df_Clone_Profile_Fanout.select(col("batch_id"), col("batch_number"), col("bin_location"), col("bin_slot"), col("brand"), col("category_id"), col("currency_code"), col("expiry_date"), col("extract_ts"), col("formula_result"), col("inventory_id"), col("is_active"), col("is_low_stock"), col("last_stocktake_date"), col("lead_time_days"), col("maximum_stock"), col("minimum_stock"), col("product_id"), col("product_name"), col("quantity_on_hand"), col("quantity_reserved"), col("region_id"), col("reorder_quantity"), col("run_id"), col("sku"), col("source_row_num"), col("square_footage"), col("store_id"), col("store_name"), col("store_type"), col("supplier_active"), col("supplier_id"), col("supplier_name"), col("unit_cost"), col("unit_price"), col("warehouse_code"), col("weight_kg"), lit('reorder_level').alias("measure_name"), col("reorder_level").alias("REORDER"))
    df_Normalise_Profile_Measures = _norm_df_Normalise_Profile_Measures_0
    df_Normalise_Profile_Measures = df_Normalise_Profile_Measures.unionByName(_norm_df_Normalise_Profile_Measures_1, allowMissingColumns=True)
    df_Normalise_Profile_Measures = df_Normalise_Profile_Measures.unionByName(_norm_df_Normalise_Profile_Measures_2, allowMissingColumns=True)

    # Step: Profile Row Fingerprint (CheckSum) [converted]
    # Add a Checksum: Profile Row Fingerprint
    df_Profile_Row_Fingerprint = df_Clone_Profile_Fanout
    df_Profile_Row_Fingerprint = df_Profile_Row_Fingerprint.withColumn("profile_checksum", md5(concat(coalesce(col("inventory_id").cast("string"), lit("")), coalesce(col("product_id").cast("string"), lit("")), coalesce(col("store_id").cast("string"), lit("")), coalesce(col("quantity").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['inventory_id', 'product_id', 'store_id', 'quantity']

    # Step: Slow Moving? (FilterRows) [failed]
    # Filter Rows: Slow Moving?
    df_Write_Slow_Moving_Items = df_Clone_Profile_Fanout.filter((col("slow_flag") == lit('Y')))
    df_Check_Fast_Moving = df_Clone_Profile_Fanout.filter(~((col("slow_flag") == lit('Y'))))
    df_Slow_Moving? = df_Write_Slow_Moving_Items

    # Step: Sort For Duplicate Check (SortRows) [converted]
    # Sort Rows: Sort For Duplicate Check
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_For_Duplicate_Check = df_Clone_Profile_Fanout
    _sort_df_Sort_For_Duplicate_Check = _sort_df_Sort_For_Duplicate_Check.withColumn("_sort_ci_inventory_id", lower(col("inventory_id").cast("string")))
    df_Sort_For_Duplicate_Check = _sort_df_Sort_For_Duplicate_Check.orderBy(col("_sort_ci_inventory_id").asc_nulls_last())
    df_Sort_For_Duplicate_Check = df_Sort_For_Duplicate_Check.drop("_sort_ci_inventory_id")

    # Step: Stock Distribution Bands (NumberRange) [converted]
    # Number Range: Stock Distribution Bands
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Stock_Distribution_Bands = df_Clone_Profile_Fanout.withColumn('stock_band', when(col("quantity").isNull(), lit('OTHER')).otherwise(when((col("quantity").cast("double") >= lit(0.0)) & (col("quantity").cast("double") < lit(0.0)), lit('ZERO')).when((col("quantity").cast("double") >= lit(1.0)) & (col("quantity").cast("double") < lit(25.0)), lit('LOW')).when((col("quantity").cast("double") >= lit(26.0)) & (col("quantity").cast("double") < lit(150.0)), lit('MEDIUM')).when((col("quantity").cast("double") >= lit(151.0)) & (col("quantity").cast("double") < lit(999999.0)), lit('HIGH')).otherwise(lit('OTHER'))))
    # preserved.fallback='OTHER' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: Supplier Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Supplier Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Supplier_Distribution = df_Clone_Profile_Fanout.groupBy('supplier_id').agg(count(lit(1)).alias('supplier_rows'), _sum(col("quantity")).alias('supplier_qty'))

    # Step: Warehouse Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Warehouse Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Warehouse_Distribution = df_Clone_Profile_Fanout.groupBy('warehouse_code').agg(count(lit(1)).alias('warehouse_rows'), _sum(col("quantity")).alias('warehouse_qty'))

    # Step: Skip Active Products (Dummy) [converted]
    # Dummy: Skip Active Products
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Active_Products = df_Skip_Active_Products

    # Step: Write Inactive Products (TextFileOutput) [failed]
    # Pentaho step: Write Inactive Products (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/inactive_products_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Inactive_Products = df_Inactive_Products?
    _out_df_Write_Inactive_Products = df_Write_Inactive_Products.select('inventory_id', 'product_id', 'sku', 'is_active', 'batch_id', 'run_id')
    writer = _out_df_Write_Inactive_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inactive_products_.csv')

    # Step: Inventory Aging (MemoryGroupBy) [converted]
    # Memory Group By: Inventory Aging
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Inventory_Aging = df_Inventory_Aging_Bands.groupBy('aging_band').agg(count(lit(1)).alias('aging_rows'))

    # Step: Skip Non Negative (Dummy) [converted]
    # Dummy: Skip Non Negative
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Non_Negative = df_Skip_Non_Negative

    # Step: Write Negative Inventory (TextFileOutput) [failed]
    # Pentaho step: Write Negative Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/negative_inventory_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Negative_Inventory = df_Negative_Inventory?
    _out_df_Write_Negative_Inventory = df_Write_Negative_Inventory.select('inventory_id', 'product_id', 'store_id', 'quantity', 'batch_id', 'run_id')
    writer = _out_df_Write_Negative_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/negative_inventory_.csv')

    # Step: Aggregate Normalised Measures (GroupBy) [converted]
    # Group By: Aggregate Normalised Measures
    df_Aggregate_Normalised_Measures = df_Normalise_Profile_Measures.groupBy('measure_name').agg(count(lit(1)).alias('measure_total'))

    # Step: Check Fast Moving (FilterRows) [failed]
    # Filter Rows: Check Fast Moving
    df_Write_Fast_Moving_Items = df_Slow_Moving?.filter((col("fast_flag") == lit('Y')))
    df_Skip_Mid_Movers = df_Slow_Moving?.filter(~((col("fast_flag") == lit('Y'))))
    df_Check_Fast_Moving = df_Write_Fast_Moving_Items

    # Step: Write Slow Moving Items (TextFileOutput) [failed]
    # Pentaho step: Write Slow Moving Items (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/slow_moving_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_age_days' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Slow_Moving_Items = df_Slow_Moving?
    _out_df_Write_Slow_Moving_Items = df_Write_Slow_Moving_Items.select('inventory_id', 'product_id', 'inventory_age_days', 'batch_id', 'run_id')
    writer = _out_df_Write_Slow_Moving_Items.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/slow_moving_.csv')

    # Step: Duplicate Inventory Records (Unique) [converted]
    # Unique Rows: Duplicate Inventory Records
    # preserved.reject_duplicate_row=N error_description=''
    # Unique Rows expects sorted input in Pentaho; Spark dropDuplicates is order-independent
    # preserved.count_rows=True count_field='dup_count' compare_fields=['inventory_id']
    df_Duplicate_Inventory_Records = df_Sort_For_Duplicate_Check
    _w_cnt_df_Duplicate_Inventory_Records = Window.partitionBy(col("inventory_id"))
    df_Duplicate_Inventory_Records = df_Duplicate_Inventory_Records.withColumn("dup_count", count(lit(1)).over(_w_cnt_df_Duplicate_Inventory_Records))
    _w_rn_df_Duplicate_Inventory_Records = Window.partitionBy(col("inventory_id")).orderBy(monotonically_increasing_id())
    df_Duplicate_Inventory_Records = df_Duplicate_Inventory_Records.withColumn('_uniq_rn', row_number().over(_w_rn_df_Duplicate_Inventory_Records))
    df_Duplicate_Inventory_Records = df_Duplicate_Inventory_Records.filter(col('_uniq_rn') == 1).drop('_uniq_rn')

    # Step: Stock Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Stock Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Stock_Distribution = df_Stock_Distribution_Bands.groupBy('stock_band').agg(count(lit(1)).alias('stock_rows'), avg(col("quantity")).alias('avg_qty'))

    # Step: Write Supplier Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Supplier Distribution (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/supplier_distribution_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Supplier_Distribution = df_Supplier_Distribution
    _out_df_Write_Supplier_Distribution = df_Write_Supplier_Distribution.select('supplier_id', 'supplier_rows', 'supplier_qty', 'batch_id', 'run_id')
    writer = _out_df_Write_Supplier_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/supplier_distribution_.csv')

    # Step: Write Warehouse Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Warehouse Distribution (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/warehouse_distribution_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='warehouse_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Warehouse_Distribution = df_Warehouse_Distribution
    _out_df_Write_Warehouse_Distribution = df_Write_Warehouse_Distribution.select('warehouse_code', 'warehouse_rows', 'warehouse_qty', 'batch_id', 'run_id')
    writer = _out_df_Write_Warehouse_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/warehouse_distribution_.csv')

    # Step: Write Inventory Aging (TextFileOutput) [converted]
    # Pentaho step: Write Inventory Aging (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/inventory_aging_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='aging_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='aging_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Inventory_Aging = df_Inventory_Aging
    _out_df_Write_Inventory_Aging = df_Write_Inventory_Aging.select('aging_band', 'aging_rows', 'batch_id', 'run_id')
    writer = _out_df_Write_Inventory_Aging.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_aging_.csv')

    # Step: Denormalise Profile Pivot (Denormaliser) [converted]
    # Row Denormaliser: Denormalise Profile Pivot
    # preserved.target 'qty_cnt' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'avail_cnt' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    df_Denormalise_Profile_Pivot = df_Aggregate_Normalised_Measures.groupBy("warehouse_code").agg(first(when(col("measure_name") == lit('QTY'), col("measure_total")), ignorenulls=True).alias('qty_cnt'), first(when(col("measure_name") == lit('AVAILABLE'), col("measure_total")), ignorenulls=True).alias('avail_cnt'))
    df_Denormalise_Profile_Pivot = df_Denormalise_Profile_Pivot.withColumn("qty_cnt", col("qty_cnt").cast("double"))
    df_Denormalise_Profile_Pivot = df_Denormalise_Profile_Pivot.withColumn("avail_cnt", col("avail_cnt").cast("double"))

    # Step: Skip Mid Movers (Dummy) [converted]
    # Dummy: Skip Mid Movers
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Mid_Movers = df_Skip_Mid_Movers

    # Step: Write Fast Moving Items (TextFileOutput) [converted]
    # Pentaho step: Write Fast Moving Items (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/fast_moving_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_age_days' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Fast_Moving_Items = df_Check_Fast_Moving
    _out_df_Write_Fast_Moving_Items = df_Write_Fast_Moving_Items.select('inventory_id', 'product_id', 'inventory_age_days', 'batch_id', 'run_id')
    writer = _out_df_Write_Fast_Moving_Items.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/fast_moving_.csv')

    # Step: Has Duplicates? (FilterRows) [failed]
    # Filter Rows: Has Duplicates?
    df_Skip_Unique_Keys = df_Duplicate_Inventory_Records.filter((col("dup_count") == lit('1')))
    df_Write_Duplicate_Inventory = df_Duplicate_Inventory_Records.filter(~((col("dup_count") == lit('1'))))
    df_Has_Duplicates? = df_Skip_Unique_Keys

    # Step: Append Dist Profiles A (Append) [converted]
    # Append Streams: Append Dist Profiles A
    # preserved.head_name='Stock Distribution'
    # preserved.tail_name='Warehouse Distribution'
    # preserved.stream_order=['Stock Distribution', 'Warehouse Distribution']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Dist_Profiles_A = df_Stock_Distribution.unionByName(df_Warehouse_Distribution, allowMissingColumns=True)

    # Step: Write Stock Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Stock Distribution (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/stock_distribution_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='stock_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='stock_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Stock_Distribution = df_Stock_Distribution
    _out_df_Write_Stock_Distribution = df_Write_Stock_Distribution.select('stock_band', 'stock_rows', 'avg_qty', 'batch_id', 'run_id')
    writer = _out_df_Write_Stock_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stock_distribution_.csv')

    # Step: Skip Unique Keys (Dummy) [converted]
    # Dummy: Skip Unique Keys
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Unique_Keys = df_Skip_Unique_Keys

    # Step: Write Duplicate Inventory (TextFileOutput) [failed]
    # Pentaho step: Write Duplicate Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/duplicate_inventory_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dup_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Duplicate_Inventory = df_Has_Duplicates?
    _out_df_Write_Duplicate_Inventory = df_Write_Duplicate_Inventory.select('inventory_id', 'dup_count', 'batch_id', 'run_id')
    writer = _out_df_Write_Duplicate_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/duplicate_inventory_.csv')

    # Step: Append Dist Profiles B (Append) [converted]
    # Append Streams: Append Dist Profiles B
    # preserved.head_name='Append Dist Profiles A'
    # preserved.tail_name='Supplier Distribution'
    # preserved.stream_order=['Append Dist Profiles A', 'Supplier Distribution']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Dist_Profiles_B = df_Append_Dist_Profiles_A.unionByName(df_Supplier_Distribution, allowMissingColumns=True)

    # Step: Write Profiling Report JSON (JsonOutput) [converted]
    # Pentaho step: Write Profiling Report JSON (type: JsonOutput)
    df_Write_Profiling_Report_JSON = df_Append_Dist_Profiles_B
    df_Write_Profiling_Report_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/inventory_profile_report_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Write Profiling Report CSV (TextFileOutput) [converted]
    # Pentaho step: Write Profiling Report CSV (type: TextFileOutput)
    # Pentaho filename: /output/inventory/profiling/inventory_profile_report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='stock_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='stock_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Profiling_Report_CSV = df_Write_Profiling_Report_JSON
    _out_df_Write_Profiling_Report_CSV = df_Write_Profiling_Report_CSV.select('stock_band', 'stock_rows', 'warehouse_code', 'warehouse_rows', 'batch_id', 'run_id')
    writer = _out_df_Write_Profiling_Report_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_profile_report_.csv')

    # Step: Log Profile Complete (WriteToLog) [converted]
    # Write to Log: Log Profile Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=PROFILE_OK | TRANS=TR_Inventory_Profile | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Profile_Complete = logging.getLogger('pentaho.writetolog.Log_Profile_Complete')
    _log_df_Log_Profile_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Profile_Complete = df_Write_Profiling_Report_CSV
    _log_rows_df_Log_Profile_Complete = _log_df_df_Log_Profile_Complete.take(20)
    _log_df_Log_Profile_Complete.info('Log Profile Complete' + ' | columns=' + str(_log_df_df_Log_Profile_Complete.columns))
    _log_df_Log_Profile_Complete.info('AUDIT | EVENT=PROFILE_OK | TRANS=TR_Inventory_Profile | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Profile_Complete:
        _log_df_Log_Profile_Complete.info('Log Profile Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Profile_Complete = df_Write_Profiling_Report_CSV

    # Step: Profile Complete (Dummy) [converted]
    # Dummy: Profile Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Profile_Complete = df_Log_Profile_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Profile_Complete
