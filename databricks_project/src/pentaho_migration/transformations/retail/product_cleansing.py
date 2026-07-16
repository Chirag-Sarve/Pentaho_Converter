"""PySpark module migrated from Pentaho transformation: TR_Product_Cleansing.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/Product_Cleansing.ktr
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
    split,
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

_LOG = get_logger("pentaho_migration.transformations.retail.product_cleansing")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Product_Cleansing`` step-for-step."""
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

    # Step: Read Valid Products (CsvInput) [converted]
    # CSV Input: Read Valid Products
    df_Read_Valid_Products = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_id STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, created_date STRING, description STRING, upc STRING, barcode STRING, length_cm STRING, width_cm STRING, height_cm STRING, status STRING, batch_id STRING, run_id STRING, source_row_num INT')
        .load(f'{data_dir}/products_valid_.csv')
    )

    # Step: Add Cleanse Batch (Constant) [converted]
    # Add Constants: Add Cleanse Batch
    df_Add_Cleanse_Batch = df_Read_Valid_Products
    df_Add_Cleanse_Batch = df_Add_Cleanse_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Cleanse_Batch = df_Add_Cleanse_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Trim And Normalise Casing (StringOperations) [converted]
    # String Operations: Trim And Normalise Casing
    df_Trim_And_Normalise_Casing = df_Add_Cleanse_Batch
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("product_id", upper(trim(col("product_id").cast("string"))))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("sku", upper(trim(col("sku").cast("string"))))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("product_name", trim(col("product_name").cast("string")))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("brand", trim(col("brand").cast("string")))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("category_id", upper(trim(col("category_id").cast("string"))))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("supplier_id", upper(trim(col("supplier_id").cast("string"))))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("currency_code", upper(trim(col("currency_code").cast("string"))))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("status", upper(trim(col("status").cast("string"))))
    df_Trim_And_Normalise_Casing = df_Trim_And_Normalise_Casing.withColumn("description", trim(col("description").cast("string")))

    # Step: Standardize SKU Separators (ReplaceString) [partial]
    # ReplaceString: Standardize SKU Separators
    df_Standardize_SKU_Separators = df_Trim_And_Normalise_Casing

    # Step: Standardize Weight Units Flag (ValueMapper) [converted]
    # Value Mapper: Standardize Weight Units Flag
    df_Standardize_Weight_Units_Flag = df_Standardize_SKU_Separators.withColumn("weight_unit", when((lower(col("currency_code")) == lower(lit('INR'))), lit('KG')).when((lower(col("currency_code")) == lower(lit('USD'))), lit('KG')).when((lower(col("currency_code")) == lower(lit('EUR'))), lit('KG')).when((lower(col("currency_code")) == lower(lit('GBP'))), lit('KG')).when((col("currency_code").isNull() | (col("currency_code") == lit(''))), col("currency_code")).otherwise(lit('KG')))
    # preserved.case_sensitive=False mappings=4 default='KG'

    # Step: Blank To Null Sentinels (NullIf) [converted]
    # Null If: Blank To Null Sentinels
    # preserved.fields=[{'name': 'description', 'value': '', 'type': ''}, {'name': 'brand', 'value': '', 'type': ''}, {'name': 'upc', 'value': '', 'type': ''}, {'name': 'barcode', 'value': '', 'type': ''}]
    df_Blank_To_Null_Sentinels = df_Standardize_Weight_Units_Flag
    df_Blank_To_Null_Sentinels = df_Blank_To_Null_Sentinels.withColumn('description', when((col('description').isNull() | (col('description').cast('string') == lit(''))), lit(None)).otherwise(col('description')))
    df_Blank_To_Null_Sentinels = df_Blank_To_Null_Sentinels.withColumn('brand', when((col('brand').isNull() | (col('brand').cast('string') == lit(''))), lit(None)).otherwise(col('brand')))
    df_Blank_To_Null_Sentinels = df_Blank_To_Null_Sentinels.withColumn('upc', when((col('upc').isNull() | (col('upc').cast('string') == lit(''))), lit(None)).otherwise(col('upc')))
    df_Blank_To_Null_Sentinels = df_Blank_To_Null_Sentinels.withColumn('barcode', when((col('barcode').isNull() | (col('barcode').cast('string') == lit(''))), lit(None)).otherwise(col('barcode')))

    # Step: Replace NULLs With Defaults (IfNull) [converted]
    # If Field Value Is Null: Replace NULLs With Defaults
    df_Replace_NULLs_With_Defaults = df_Blank_To_Null_Sentinels
    df_Replace_NULLs_With_Defaults = df_Replace_NULLs_With_Defaults.withColumn('description', when(col('description').isNull(), lit('N/A')).otherwise(col('description')))
    df_Replace_NULLs_With_Defaults = df_Replace_NULLs_With_Defaults.withColumn('brand', when(col('brand').isNull(), lit('UNKNOWN')).otherwise(col('brand')))
    df_Replace_NULLs_With_Defaults = df_Replace_NULLs_With_Defaults.withColumn('currency_code', when(col('currency_code').isNull(), lit('USD')).otherwise(col('currency_code')))
    df_Replace_NULLs_With_Defaults = df_Replace_NULLs_With_Defaults.withColumn('status', when(col('status').isNull(), lit('ACTIVE')).otherwise(col('status')))
    df_Replace_NULLs_With_Defaults = df_Replace_NULLs_With_Defaults.withColumn('length_cm', when(col('length_cm').isNull(), lit(0)).otherwise(col('length_cm')))
    df_Replace_NULLs_With_Defaults = df_Replace_NULLs_With_Defaults.withColumn('width_cm', when(col('width_cm').isNull(), lit(0)).otherwise(col('width_cm')))
    df_Replace_NULLs_With_Defaults = df_Replace_NULLs_With_Defaults.withColumn('height_cm', when(col('height_cm').isNull(), lit(0)).otherwise(col('height_cm')))

    # Step: Cast Cleanse Numerics (SelectValues) [converted]
    # Select Values: Cast Cleanse Numerics
    df_Cast_Cleanse_Numerics = df_Replace_NULLs_With_Defaults.select(col("product_id").alias("product_id"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("created_date").alias("created_date"), col("description").alias("description"), col("upc").alias("upc"), col("barcode").alias("barcode"), col("length_cm").alias("length_cm"), col("width_cm").alias("width_cm"), col("height_cm").alias("height_cm"), col("status").alias("status"), col("weight_unit").alias("weight_unit"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("source_row_num").alias("source_row_num"))

    # Step: Correct Negatives And Dimensions (Formula) [converted]
    # Formula: Correct Negatives And Dimensions
    df_Correct_Negatives_And_Dimensions = df_Cast_Cleanse_Numerics
    df_Correct_Negatives_And_Dimensions = df_Correct_Negatives_And_Dimensions.withColumn('formula_result', lit(None))  # empty formula

    # Step: Checksum Product Business Key (CheckSum) [converted]
    # Add a Checksum: Checksum Product Business Key
    df_Checksum_Product_Business_Key = df_Correct_Negatives_And_Dimensions
    df_Checksum_Product_Business_Key = df_Checksum_Product_Business_Key.withColumn("product_bk_checksum", md5(concat(coalesce(col("product_id").cast("string"), lit("")), coalesce(col("sku").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['product_id', 'sku']

    # Step: Concat Dedupe Key (ConcatFields) [converted]
    # Concat Fields: Concat Dedupe Key
    df_Concat_Dedupe_Key = df_Checksum_Product_Business_Key
    df_Concat_Dedupe_Key = df_Concat_Dedupe_Key.withColumn("dedupe_key", concat(concat(lit('"'), coalesce(col("product_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("sku").cast("string"), lit("")), lit('"'))))
    # preserved.encoding='UTF-8'

    # Step: Split SKU Parts (FieldSplitter) [converted]
    # Split Fields: Split SKU Parts
    df_Split_SKU_Parts = df_Concat_Dedupe_Key
    df_Split_SKU_Parts = df_Split_SKU_Parts.withColumn("_parts_df_Split_SKU_Parts", split(col("sku").cast("string"), '-'))
    # preserved.field 'sku_prefix' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_SKU_Parts = df_Split_SKU_Parts.withColumn("sku_prefix", trim(element_at(col("_parts_df_Split_SKU_Parts"), 1)))
    # preserved.field 'sku_cat_code' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_SKU_Parts = df_Split_SKU_Parts.withColumn("sku_cat_code", trim(element_at(col("_parts_df_Split_SKU_Parts"), 2)))
    # preserved.field 'sku_seq' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_SKU_Parts = df_Split_SKU_Parts.withColumn("sku_seq", trim(element_at(col("_parts_df_Split_SKU_Parts"), 3)))
    df_Split_SKU_Parts = df_Split_SKU_Parts.drop("_parts_df_Split_SKU_Parts", "sku")

    # Step: Sort Before Dedupe (SortRows) [converted]
    # Sort Rows: Sort Before Dedupe
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Before_Dedupe = df_Split_SKU_Parts
    _sort_df_Sort_Before_Dedupe = _sort_df_Sort_Before_Dedupe.withColumn("_sort_ci_product_id", lower(col("product_id").cast("string")))
    _sort_df_Sort_Before_Dedupe = _sort_df_Sort_Before_Dedupe.withColumn("_sort_ci_sku", lower(col("sku").cast("string")))
    df_Sort_Before_Dedupe = _sort_df_Sort_Before_Dedupe.orderBy(col("_sort_ci_product_id").asc_nulls_last(), col("_sort_ci_sku").asc_nulls_last())
    df_Sort_Before_Dedupe = df_Sort_Before_Dedupe.drop("_sort_ci_product_id", "_sort_ci_sku")

    # Step: Remove Duplicates Hash Set (UniqueRowsByHashSet) [converted]
    # Unique Rows (HashSet): Remove Duplicates Hash Set
    # preserved.reject_duplicate_row=N error_description=''
    # preserved.store_values=True
    # preserved.count_rows=False count_field='count' compare_fields=['product_id', 'sku']
    df_Remove_Duplicates_Hash_Set = df_Sort_Before_Dedupe.dropDuplicates(["product_id", "sku"])

    # Step: Unique Rows Business Key (Unique) [converted]
    # Unique Rows: Unique Rows Business Key
    # preserved.reject_duplicate_row=N error_description=''
    # Unique Rows expects sorted input in Pentaho; Spark dropDuplicates is order-independent
    # preserved.count_rows=True count_field='dup_count' compare_fields=['product_id']
    df_Unique_Rows_Business_Key = df_Remove_Duplicates_Hash_Set
    _w_cnt_df_Unique_Rows_Business_Key = Window.partitionBy(col("product_id"))
    df_Unique_Rows_Business_Key = df_Unique_Rows_Business_Key.withColumn("dup_count", count(lit(1)).over(_w_cnt_df_Unique_Rows_Business_Key))
    _w_rn_df_Unique_Rows_Business_Key = Window.partitionBy(col("product_id")).orderBy(monotonically_increasing_id())
    df_Unique_Rows_Business_Key = df_Unique_Rows_Business_Key.withColumn('_uniq_rn', row_number().over(_w_rn_df_Unique_Rows_Business_Key))
    df_Unique_Rows_Business_Key = df_Unique_Rows_Business_Key.filter(col('_uniq_rn') == 1).drop('_uniq_rn')

    # Step: Cleansing Counters (MemoryGroupBy) [converted]
    # Memory Group By: Cleansing Counters
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Cleansing_Counters = df_Unique_Rows_Business_Key.groupBy().agg(count(lit(1)).alias('rows_cleansed'))

    # Step: Write Cleansed Products (TextFileOutput) [converted]
    # Pentaho step: Write Cleansed Products (type: TextFileOutput)
    # Pentaho filename: /output/product/cleansed/products_cleansed_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
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
    # INFO: preserved.field_format name='created_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='description' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='upc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='barcode' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='length_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='width_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='height_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='weight_unit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_bk_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dedupe_key' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku_prefix' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku_cat_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku_seq' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dup_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Cleansed_Products = df_Unique_Rows_Business_Key
    _out_df_Write_Cleansed_Products = df_Write_Cleansed_Products.select('product_id', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'created_date', 'description', 'upc', 'barcode', 'length_cm', 'width_cm', 'height_cm', 'status', 'weight_unit', 'product_bk_checksum', 'dedupe_key', 'sku_prefix', 'sku_cat_code', 'sku_seq', 'dup_count', 'batch_id', 'run_id', 'source_row_num')
    writer = _out_df_Write_Cleansed_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/products_cleansed_.csv')

    # Step: Tag Cleansing Report (Constant) [converted]
    # Add Constants: Tag Cleansing Report
    df_Tag_Cleansing_Report = df_Cleansing_Counters
    df_Tag_Cleansing_Report = df_Tag_Cleansing_Report.withColumn("report_name", lit('PRODUCT_CLEANSING'))
    # preserved.report_name: length='-1', precision='-1'
    df_Tag_Cleansing_Report = df_Tag_Cleansing_Report.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Tag_Cleansing_Report = df_Tag_Cleansing_Report.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Cleansing_Report = df_Tag_Cleansing_Report.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Cleansing Report (TextFileOutput) [converted]
    # Pentaho step: Write Cleansing Report (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_cleansing_report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='report_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_cleansed' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Cleansing_Report = df_Tag_Cleansing_Report
    _out_df_Write_Cleansing_Report = df_Write_Cleansing_Report.select('report_name', 'rows_cleansed', 'status', 'batch_id', 'run_id')
    writer = _out_df_Write_Cleansing_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_cleansing_report_.csv')

    # Step: Write Cleansing Report JSON (JsonOutput) [converted]
    # Pentaho step: Write Cleansing Report JSON (type: JsonOutput)
    df_Write_Cleansing_Report_JSON = df_Write_Cleansing_Report
    df_Write_Cleansing_Report_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/product_cleansing_report__summary.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Log Cleansing Complete (WriteToLog) [converted]
    # Write to Log: Log Cleansing Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=CLEANSING_COMPLETE | TRANS=TR_Product_Cleansing | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Cleansing_Complete = logging.getLogger('pentaho.writetolog.Log_Cleansing_Complete')
    _log_df_Log_Cleansing_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Cleansing_Complete = df_Write_Cleansing_Report_JSON
    _log_rows_df_Log_Cleansing_Complete = _log_df_df_Log_Cleansing_Complete.take(20)
    _log_df_Log_Cleansing_Complete.info('Log Cleansing Complete' + ' | columns=' + str(_log_df_df_Log_Cleansing_Complete.columns))
    _log_df_Log_Cleansing_Complete.info('AUDIT | EVENT=CLEANSING_COMPLETE | TRANS=TR_Product_Cleansing | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Cleansing_Complete:
        _log_df_Log_Cleansing_Complete.info('Log Cleansing Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Cleansing_Complete = df_Write_Cleansing_Report_JSON

    # Step: Cleansing Complete (Dummy) [converted]
    # Dummy: Cleansing Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Cleansing_Complete = df_Log_Cleansing_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Cleansing_Complete
