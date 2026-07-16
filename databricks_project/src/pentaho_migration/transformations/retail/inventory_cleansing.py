"""PySpark module migrated from Pentaho transformation: TR_Inventory_Cleansing.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/Inventory_Cleansing.ktr
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
    trim,
    upper,
    when,
    coalesce,
    row_number,
    md5,
    concat,
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

_LOG = get_logger("pentaho_migration.transformations.retail.inventory_cleansing")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Inventory_Cleansing`` step-for-step."""
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

    # Step: Get Cleansing Variables (GetVariable) [converted]
    # Get Variables: Get Cleansing Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Cleansing_Variables = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Cleansing_Variables = df_Get_Cleansing_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Cleansing_Variables = df_Get_Cleansing_Variables.withColumn('run_id', lit(_run_id_resolved))

    # Step: Read Prior Clean Snapshot (CsvInput) [converted]
    # CSV Input: Read Prior Clean Snapshot
    df_Read_Prior_Clean_Snapshot = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, inventory_bk_checksum STRING')
        .load(f'{data_dir}/inventory_clean_prior.csv')
    )

    # Step: Write Cleansing Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Cleansing Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/inventory/inventory_cleanse_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Cleansing_Rejects = df_Write_Cleansing_Rejects
    _out_df_Write_Cleansing_Rejects = df_Write_Cleansing_Rejects.select('inventory_id', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Cleansing_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_cleanse_rejects_.csv')

    # Step: Read Valid Inventory (CsvInput) [converted]
    # CSV Input: Read Valid Inventory
    df_Read_Valid_Inventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, store_id STRING, product_id STRING, quantity_on_hand STRING, quantity_reserved STRING, reorder_level STRING, reorder_quantity STRING, last_stocktake_date STRING, bin_location STRING, is_low_stock STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, store_name STRING, store_type STRING, region_id STRING, square_footage STRING, supplier_name STRING, lead_time_days STRING, supplier_active STRING, warehouse_code STRING, bin_slot STRING, batch_number STRING, expiry_date STRING, maximum_stock STRING, minimum_stock STRING, quantity STRING, available_qty STRING, batch_id STRING, run_id STRING, extract_ts STRING, source_row_num STRING, validation_status STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/inventory_valid_.csv')
    )

    # Step: Prepare Prior Clean Keys (SelectValues) [converted]
    # Select Values: Prepare Prior Clean Keys
    df_Prepare_Prior_Clean_Keys = df_Read_Prior_Clean_Snapshot.select(col("inventory_id").alias("prior_inventory_id"), col("inventory_bk_checksum").alias("prior_checksum"))

    # Step: Inventory Mapping Input (MappingInput) [converted]
    # Mapping Input Specification: Inventory Mapping Input
    # preserved.select_unspecified=False
    # preserved.include_unspecified_fields=False
    try:
        df_Inventory_Mapping_Input = spark.table('_pentaho_mapping_input')
    except Exception:
        try:
            df_Inventory_Mapping_Input = spark.table('_pentaho_mapping_input_Inventory_Mapping_Input')
        except Exception:
            df_Inventory_Mapping_Input = df_Read_Valid_Inventory
    # Null/empty input streams: schema is still validated; zero rows are valid

    # Step: Trim Inventory Strings (StringOperations) [converted]
    # String Operations: Trim Inventory Strings
    df_Trim_Inventory_Strings = df_Inventory_Mapping_Input
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("inventory_id", upper(trim(col("inventory_id").cast("string"))))
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("product_id", upper(trim(col("product_id").cast("string"))))
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("store_id", upper(trim(col("store_id").cast("string"))))
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("supplier_id", upper(trim(col("supplier_id").cast("string"))))
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("warehouse_code", upper(trim(col("warehouse_code").cast("string"))))
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("sku", upper(trim(col("sku").cast("string"))))
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("bin_location", trim(col("bin_location").cast("string")))
    df_Trim_Inventory_Strings = df_Trim_Inventory_Strings.withColumn("batch_number", upper(trim(col("batch_number").cast("string"))))

    # Step: Normalize Warehouse Names (ValueMapper) [converted]
    # Value Mapper: Normalize Warehouse Names
    df_Normalize_Warehouse_Names = df_Trim_Inventory_Strings.withColumn("warehouse_name", when((lower(col("warehouse_code")) == lower(lit('W'))), lit('WAREHOUSE_W')).when((lower(col("warehouse_code")) == lower(lit('X'))), lit('WAREHOUSE_X')).when((lower(col("warehouse_code")) == lower(lit('N'))), lit('WAREHOUSE_N')).when((lower(col("warehouse_code")) == lower(lit('M'))), lit('WAREHOUSE_M')).when((lower(col("warehouse_code")) == lower(lit('K'))), lit('WAREHOUSE_K')).when((col("warehouse_code").isNull() | (col("warehouse_code") == lit(''))), col("warehouse_code")).otherwise(lit('WAREHOUSE_OTHER')))
    # preserved.case_sensitive=False mappings=5 default='WAREHOUSE_OTHER'

    # Step: Normalize Product Codes Spaces (ReplaceString) [partial]
    # ReplaceString: Normalize Product Codes Spaces
    df_Normalize_Product_Codes_Spaces = df_Normalize_Warehouse_Names

    # Step: Standardize SKU Pattern (RegexEval) [converted]
    # Regex Evaluation: Standardize SKU Pattern
    # preserved.matcher='sku'
    # preserved.pattern='^[A-Z0-9-]+$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Standardize_SKU_Pattern = df_Normalize_Product_Codes_Spaces
    df_Standardize_SKU_Pattern = df_Standardize_SKU_Pattern.withColumn('result', when(col('sku').rlike('^[A-Z0-9-]+$'), lit("Y")).otherwise(lit("N")))

    # Step: Cast Cleansing Numerics (SelectValues) [converted]
    # Select Values: Cast Cleansing Numerics
    df_Cast_Cleansing_Numerics = df_Standardize_SKU_Pattern.select(col("inventory_id").alias("inventory_id"), col("store_id").alias("store_id"), col("product_id").alias("product_id"), col("quantity_on_hand").alias("quantity_on_hand"), col("quantity_reserved").alias("quantity_reserved"), col("reorder_level").alias("reorder_level"), col("reorder_quantity").alias("reorder_quantity"), col("last_stocktake_date").alias("last_stocktake_date"), col("bin_location").alias("bin_location"), col("is_low_stock").alias("is_low_stock"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("store_name").alias("store_name"), col("store_type").alias("store_type"), col("region_id").alias("region_id"), col("square_footage").alias("square_footage"), col("supplier_name").alias("supplier_name"), col("lead_time_days").alias("lead_time_days"), col("supplier_active").alias("supplier_active"), col("warehouse_code").alias("warehouse_code"), col("bin_slot").alias("bin_slot"), col("batch_number").alias("batch_number"), col("expiry_date").alias("expiry_date"), col("maximum_stock").alias("maximum_stock"), col("minimum_stock").alias("minimum_stock"), col("quantity").alias("quantity"), col("available_qty").alias("available_qty"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"), col("warehouse_name").alias("warehouse_name"), col("sku_std_flag").alias("sku_std_flag"), col("validation_status").alias("validation_status"))

    # Step: Correct Invalid Quantities (Formula) [converted]
    # Formula: Correct Invalid Quantities
    df_Correct_Invalid_Quantities = df_Cast_Cleansing_Numerics
    df_Correct_Invalid_Quantities = df_Correct_Invalid_Quantities.withColumn('formula_result', lit(None))  # empty formula

    # Step: Recalc Available Qty (Calculator) [converted]
    # Calculator: Recalc Available Qty
    df_Recalc_Available_Qty = df_Correct_Invalid_Quantities
    df_Recalc_Available_Qty = df_Recalc_Available_Qty.withColumn("available_qty", (col("available_qty_clean")).cast('decimal(38,2)'))

    # Step: Replace NULL Attributes (IfNull) [converted]
    # If Field Value Is Null: Replace NULL Attributes
    df_Replace_NULL_Attributes = df_Recalc_Available_Qty
    df_Replace_NULL_Attributes = df_Replace_NULL_Attributes.withColumn('supplier_name', when(col('supplier_name').isNull(), lit('UNKNOWN_SUPPLIER')).otherwise(col('supplier_name')))
    df_Replace_NULL_Attributes = df_Replace_NULL_Attributes.withColumn('store_name', when(col('store_name').isNull(), lit('UNKNOWN_STORE')).otherwise(col('store_name')))
    df_Replace_NULL_Attributes = df_Replace_NULL_Attributes.withColumn('warehouse_code', when(col('warehouse_code').isNull(), lit('UNK')).otherwise(col('warehouse_code')))
    df_Replace_NULL_Attributes = df_Replace_NULL_Attributes.withColumn('bin_slot', when(col('bin_slot').isNull(), lit(0)).otherwise(col('bin_slot')))
    df_Replace_NULL_Attributes = df_Replace_NULL_Attributes.withColumn('brand', when(col('brand').isNull(), lit('UNKNOWN')).otherwise(col('brand')))
    df_Replace_NULL_Attributes = df_Replace_NULL_Attributes.withColumn('unit_of_measure', when(col('unit_of_measure').isNull(), lit('EA')).otherwise(col('unit_of_measure')))

    # Step: Null If Placeholder Tokens (NullIf) [failed]
    # Null If: Null If Placeholder Tokens
    # preserved.fields=[{'name': 'bin_slot', 'value': 'NULL', 'type': ''}, {'name': 'bin_slot', 'value': 'N/A', 'type': ''}]
    df_Null_If_Placeholder_Tokens = df_Replace_NULL_Attributes
    df_Null_If_Placeholder_Tokens = df_Null_If_Placeholder_Tokens.withColumn('bin_slot', when((col('bin_slot') == lit('NULL')), lit(None)).otherwise(col('bin_slot')))
    df_Null_If_Placeholder_Tokens = df_Null_If_Placeholder_Tokens.withColumn('bin_slot', when((col('bin_slot') == lit('N/A')), lit(None)).otherwise(col('bin_slot')))

    # Step: Standardize Units (ValueMapper) [failed]
    # Value Mapper: Standardize Units
    df_Standardize_Units = df_Null_If_Placeholder_Tokens.withColumn("unit_of_measure_std", when((lower(col("unit_of_measure")) == lower(lit('EA'))), lit('EACH')).when((lower(col("unit_of_measure")) == lower(lit('CS'))), lit('CASE')).when((lower(col("unit_of_measure")) == lower(lit('KG'))), lit('KILOGRAM')).when((lower(col("unit_of_measure")) == lower(lit('LB'))), lit('POUND')).when((lower(col("unit_of_measure")) == lower(lit('PAL'))), lit('PALLET')).when((col("unit_of_measure").isNull() | (col("unit_of_measure") == lit(''))), col("unit_of_measure")).otherwise(lit('EACH')))
    # preserved.case_sensitive=False mappings=5 default='EACH'

    # Step: Build Natural Key (ConcatFields) [converted]
    # Concat Fields: Build Natural Key
    df_Build_Natural_Key = df_Standardize_Units
    df_Build_Natural_Key = df_Build_Natural_Key.withColumn("inventory_nk", concat(concat(lit('"'), coalesce(col("inventory_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("product_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("store_id").cast("string"), lit("")), lit('"'))))
    # preserved.encoding='UTF-8'

    # Step: Sort Inventory Duplicates (SortRows) [converted]
    # Sort Rows: Sort Inventory Duplicates
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Inventory_Duplicates = df_Build_Natural_Key
    _sort_df_Sort_Inventory_Duplicates = _sort_df_Sort_Inventory_Duplicates.withColumn("_sort_ci_inventory_id", lower(col("inventory_id").cast("string")))
    _sort_df_Sort_Inventory_Duplicates = _sort_df_Sort_Inventory_Duplicates.withColumn("_sort_ci_last_stocktake_date", lower(col("last_stocktake_date").cast("string")))
    df_Sort_Inventory_Duplicates = _sort_df_Sort_Inventory_Duplicates.orderBy(col("_sort_ci_inventory_id").asc_nulls_last(), col("_sort_ci_last_stocktake_date").asc_nulls_last())
    df_Sort_Inventory_Duplicates = df_Sort_Inventory_Duplicates.drop("_sort_ci_inventory_id", "_sort_ci_last_stocktake_date")

    # Step: Remove Duplicate Inventory (Unique) [converted]
    # Unique Rows: Remove Duplicate Inventory
    # preserved.reject_duplicate_row=N error_description=''
    # Unique Rows expects sorted input in Pentaho; Spark dropDuplicates is order-independent
    # preserved.count_rows=True count_field='dedupe_count' compare_fields=['inventory_id']
    df_Remove_Duplicate_Inventory = df_Sort_Inventory_Duplicates
    _w_cnt_df_Remove_Duplicate_Inventory = Window.partitionBy(col("inventory_id"))
    df_Remove_Duplicate_Inventory = df_Remove_Duplicate_Inventory.withColumn("dedupe_count", count(lit(1)).over(_w_cnt_df_Remove_Duplicate_Inventory))
    _w_rn_df_Remove_Duplicate_Inventory = Window.partitionBy(col("inventory_id")).orderBy(monotonically_increasing_id())
    df_Remove_Duplicate_Inventory = df_Remove_Duplicate_Inventory.withColumn('_uniq_rn', row_number().over(_w_rn_df_Remove_Duplicate_Inventory))
    df_Remove_Duplicate_Inventory = df_Remove_Duplicate_Inventory.filter(col('_uniq_rn') == 1).drop('_uniq_rn')

    # Step: Hash Unique Inventory (UniqueRowsByHashSet) [converted]
    # Unique Rows (HashSet): Hash Unique Inventory
    # preserved.reject_duplicate_row=N error_description=''
    # preserved.store_values=True
    # preserved.count_rows=False count_field='count' compare_fields=['inventory_id', 'product_id', 'store_id']
    df_Hash_Unique_Inventory = df_Remove_Duplicate_Inventory.dropDuplicates(["inventory_id", "product_id", "store_id"])

    # Step: Checksum Inventory BK (CheckSum) [converted]
    # Add a Checksum: Checksum Inventory BK
    df_Checksum_Inventory_BK = df_Hash_Unique_Inventory
    df_Checksum_Inventory_BK = df_Checksum_Inventory_BK.withColumn("inventory_bk_checksum", md5(concat(coalesce(col("inventory_id").cast("string"), lit("")), coalesce(col("product_id").cast("string"), lit("")), coalesce(col("store_id").cast("string"), lit("")), coalesce(col("quantity_clean").cast("string"), lit("")), coalesce(col("warehouse_code").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['inventory_id', 'product_id', 'store_id', 'quantity_clean', 'warehouse_code']

    # Step: Compare Clean Delta (MergeRows) [converted]
    # Merge Rows (Diff): Compare Clean Delta
    # preserved.flag_field='merge_flag'
    # preserved.reference='Prepare Prior Clean Keys'
    # preserved.compare='Checksum Inventory BK'
    # preserved.key_fields=['inventory_id']
    # preserved.value_fields=['inventory_bk_checksum']
    _ref_df_Compare_Clean_Delta = df_Prepare_Prior_Clean_Keys.alias("r")
    _cmp_df_Compare_Clean_Delta = df_Checksum_Inventory_BK.alias("c")
    # WARNING: MergeRows 'Compare Clean Delta': null join keys do not match under Spark equality; duplicate keys expand to a product within the key group
    df_Compare_Clean_Delta = _ref_df_Compare_Clean_Delta.join(_cmp_df_Compare_Clean_Delta, (col("r.inventory_id") == col("c.inventory_id")), 'full_outer')
    df_Compare_Clean_Delta = df_Compare_Clean_Delta.withColumn('merge_flag', when(col("c.inventory_id").isNull(), lit("deleted")).when(col("r.inventory_id").isNull(), lit("new")).when((~col("r.inventory_bk_checksum").eqNullSafe(col("c.inventory_bk_checksum"))), lit("changed")).otherwise(lit("identical")))
    # NOTE: MergeRows 'Compare Clean Delta': output prefers compare values (CDC-style); deleted rows keep reference values
    df_Compare_Clean_Delta = df_Compare_Clean_Delta.select(coalesce(col("c.inventory_id"), col("r.inventory_id")).alias('inventory_id'), coalesce(col("c.inventory_bk_checksum"), col("r.inventory_bk_checksum")).alias('inventory_bk_checksum'), col('merge_flag'))
    # NOTE: MergeRows flags — deleted / new / changed / identical (requires pre-sorted inputs in PDI; Spark join does not enforce sort order)

    # Step: Inventory Mapping Output (MappingOutput) [converted]
    # Mapping Output Specification: Inventory Mapping Output
    df_Inventory_Mapping_Output = df_Compare_Clean_Delta
    # No static output field list — pass stream through (parent Mapping may apply renames)
    df_Inventory_Mapping_Output.createOrReplaceTempView('_pentaho_mapping_output')
    df_Inventory_Mapping_Output.createOrReplaceTempView('_pentaho_mapping_output_Inventory_Mapping_Output')
    # Parent Mapping helper prefers '_pentaho_mapping_output' when present

    # Step: Write Clean Inventory (TextFileOutput) [converted]
    # Pentaho step: Write Clean Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/cleansed/inventory_clean_
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
    df_Write_Clean_Inventory = df_Inventory_Mapping_Output
    _out_df_Write_Clean_Inventory = df_Write_Clean_Inventory.select('inventory_id', 'store_id', 'product_id', 'quantity_on_hand', 'quantity_reserved', 'reorder_level', 'reorder_quantity', 'last_stocktake_date', 'bin_location', 'is_low_stock', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'store_name', 'store_type', 'region_id', 'square_footage', 'supplier_name', 'lead_time_days', 'supplier_active', 'warehouse_code', 'bin_slot', 'batch_number', 'expiry_date', 'maximum_stock', 'minimum_stock', 'quantity', 'available_qty', 'batch_id', 'run_id', 'extract_ts', 'source_row_num', 'warehouse_name', 'unit_of_measure_std', 'quantity_clean', 'available_qty_clean', 'inventory_nk', 'inventory_bk_checksum', 'merge_flag', 'batch_id', 'run_id')
    writer = _out_df_Write_Clean_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_clean_.csv')

    # Step: Count Cleansed Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Cleansed Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Cleansed_Rows = df_Write_Clean_Inventory.groupBy().agg(count(lit(1)).alias('rows_cleansed'))

    # Step: Write Cleansing Report (TextFileOutput) [converted]
    # Pentaho step: Write Cleansing Report (type: TextFileOutput)
    # Pentaho filename: /logs/execution/inventory/TR_Inventory_Cleansing_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='rows_cleansed' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Cleansing_Report = df_Count_Cleansed_Rows
    _out_df_Write_Cleansing_Report = df_Write_Cleansing_Report.select('rows_cleansed', 'batch_id', 'run_id')
    writer = _out_df_Write_Cleansing_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Inventory_Cleansing_.log')

    # Step: Log Cleansing Complete (WriteToLog) [converted]
    # Write to Log: Log Cleansing Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=CLEANSE_OK | TRANS=TR_Inventory_Cleansing | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Cleansing_Complete = logging.getLogger('pentaho.writetolog.Log_Cleansing_Complete')
    _log_df_Log_Cleansing_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Cleansing_Complete = df_Write_Cleansing_Report
    _log_rows_df_Log_Cleansing_Complete = _log_df_df_Log_Cleansing_Complete.take(20)
    _log_df_Log_Cleansing_Complete.info('Log Cleansing Complete' + ' | columns=' + str(_log_df_df_Log_Cleansing_Complete.columns))
    _log_df_Log_Cleansing_Complete.info('AUDIT | EVENT=CLEANSE_OK | TRANS=TR_Inventory_Cleansing | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Cleansing_Complete:
        _log_df_Log_Cleansing_Complete.info('Log Cleansing Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Cleansing_Complete = df_Write_Cleansing_Report

    # Step: Cleansing Complete (Dummy) [converted]
    # Dummy: Cleansing Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Cleansing_Complete = df_Inventory_Mapping_Output

    log_event(_LOG, "transformation_end")
    return df_Dummy_Cleansing_Complete
