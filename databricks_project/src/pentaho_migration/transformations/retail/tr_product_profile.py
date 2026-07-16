"""PySpark module migrated from Pentaho transformation: TR_Product_Profile.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Product_Profile.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    length,
    lit,
    lower,
    trim,
    when,
    coalesce,
    sum as _sum,
    max as _max,
    min as _min,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_product_profile")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Product_Profile`` step-for-step."""
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

    # Step: Collect Duplicate Keys (Dummy) [converted]
    # Dummy: Collect Duplicate Keys
    # Pass-through step - DataFrame unchanged
    df_Dummy_Collect_Duplicate_Keys = spark.createDataFrame([], '_init STRING').limit(0)

    # Step: Duplicates Only? (FilterRows) [failed]
    # Filter Rows: Duplicates Only?
    df_Duplicates_Only? = spark.createDataFrame([], '_placeholder STRING')

    # Step: Has Supplier (Dummy) [converted]
    # Dummy: Has Supplier
    # Pass-through step - DataFrame unchanged
    df_Dummy_Has_Supplier = spark.createDataFrame([], '_init STRING').limit(0)

    # Step: Missing Supplier? (FilterRows) [failed]
    # Filter Rows: Missing Supplier?
    df_Missing_Supplier? = spark.createDataFrame([], '_placeholder STRING')

    # Step: Read Staged Categories (TextFileInput) [converted]
    # Pentaho step: Read Staged Categories (type: TextFileInput)
    # INFO: preserved Legacy Text File Input option: date_format_lenient='Y'
    # Pentaho filename: ${PROJECT_HOME}/output/product/staging/stg_raw_categories_.csv
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    df_Read_Staged_Categories = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("quote", '"')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('category_id STRING, category_name STRING, parent_category_id STRING, description STRING, is_active STRING, source_row_num INT')
        .csv(f'{data_dir}/stg_raw_categories_.csv')
    )
    # INFO: preserved.field_format name='category_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='category_name' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='parent_category_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='description' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='is_active' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='source_row_num' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    df_Read_Staged_Categories = df_Read_Staged_Categories.select(col('category_id').alias('category_id'), col('category_name').alias('category_name'), col('parent_category_id').alias('parent_category_id'), col('description').alias('description'), col('is_active').alias('is_active'), col('source_row_num').cast('int').alias('source_row_num'))
    df_Read_Staged_Categories = df_Read_Staged_Categories.filter(~((col('category_id').isNull() | (length(trim(col('category_id').cast('string'))) == 0)) & (col('category_name').isNull() | (length(trim(col('category_name').cast('string'))) == 0)) & (col('parent_category_id').isNull() | (length(trim(col('parent_category_id').cast('string'))) == 0)) & (col('description').isNull() | (length(trim(col('description').cast('string'))) == 0)) & (col('is_active').isNull() | (length(trim(col('is_active').cast('string'))) == 0)) & (col('source_row_num').isNull() | (length(trim(col('source_row_num').cast('string'))) == 0))))
    df_Read_Staged_Categories = df_Read_Staged_Categories.withColumn('source_row_num', monotonically_increasing_id())

    # Step: Read Staged Products (CsvInput) [converted]
    # CSV Input: Read Staged Products
    df_Read_Staged_Products = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_id STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, created_date STRING, description STRING, upc STRING, barcode STRING, length_cm STRING, width_cm STRING, height_cm STRING, status STRING, source_row_num INT, batch_id STRING, run_id STRING, etl_layer STRING, extract_ts STRING')
        .load(f'{data_dir}/stg_raw_products_.csv')
    )

    # Step: Read Staged Suppliers (CsvInput) [converted]
    # CSV Input: Read Staged Suppliers
    df_Read_Staged_Suppliers = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('supplier_id STRING, supplier_name STRING, contact_name STRING, email STRING, phone STRING, address_line1 STRING, city STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, lead_time_days STRING, is_active STRING, created_date STRING, source_row_num INT')
        .load(f'{data_dir}/stg_raw_suppliers_.csv')
    )

    # Step: Skip Non Dupes (Dummy) [converted]
    # Dummy: Skip Non Dupes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Non_Dupes = spark.createDataFrame([], '_init STRING').limit(0)

    # Step: Write Missing Supplier (Dummy) [converted]
    # Dummy: Write Missing Supplier
    # Pass-through step - DataFrame unchanged
    df_Dummy_Write_Missing_Supplier = spark.createDataFrame([], '_init STRING').limit(0)

    # Step: Sort Categories By ID (SortRows) [converted]
    # Sort Rows: Sort Categories By ID
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Categories_By_ID = df_Read_Staged_Categories
    _sort_df_Sort_Categories_By_ID = _sort_df_Sort_Categories_By_ID.withColumn("_sort_ci_category_id", lower(col("category_id").cast("string")))
    df_Sort_Categories_By_ID = _sort_df_Sort_Categories_By_ID.orderBy(col("_sort_ci_category_id").asc_nulls_last())
    df_Sort_Categories_By_ID = df_Sort_Categories_By_ID.drop("_sort_ci_category_id")

    # Step: Add Profile Batch (Constant) [converted]
    # Add Constants: Add Profile Batch
    df_Add_Profile_Batch = df_Read_Staged_Products
    df_Add_Profile_Batch = df_Add_Profile_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Profile_Batch = df_Add_Profile_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Rename Category Lookup Keys (SelectValues) [converted]
    # Select Values: Rename Category Lookup Keys
    df_Rename_Category_Lookup_Keys = df_Sort_Categories_By_ID.select(col("category_id").alias("ref_category_id"), col("category_name").alias("ref_category_name"))

    # Step: Cast Price Weight Numeric (SelectValues) [converted]
    # Select Values: Cast Price Weight Numeric
    df_Cast_Price_Weight_Numeric = df_Add_Profile_Batch.select(col("product_id").alias("product_id"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("created_date").alias("created_date"), col("description").alias("description"), col("status").alias("status"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"))

    # Step: Category Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Category Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Category_Distribution = df_Cast_Price_Weight_Numeric.groupBy('category_id').agg(count(lit(1)).alias('product_count'))

    # Step: Detect Blank Supplier (Formula) [converted]
    # Formula: Detect Blank Supplier
    df_Detect_Blank_Supplier = df_Cast_Price_Weight_Numeric
    df_Detect_Blank_Supplier = df_Detect_Blank_Supplier.withColumn('formula_result', lit(None))  # empty formula

    # Step: Flag Nulls (Formula) [converted]
    # Formula: Flag Nulls
    df_Flag_Nulls = df_Cast_Price_Weight_Numeric
    df_Flag_Nulls = df_Flag_Nulls.withColumn('formula_result', lit(None))  # empty formula

    # Step: Inactive Products? (FilterRows) [failed]
    # Filter Rows: Inactive Products?
    df_Write_Inactive_Products = df_Cast_Price_Weight_Numeric.filter((col("is_active") == lit('0')))
    df_Active_Path_Continue = df_Cast_Price_Weight_Numeric.filter(~((col("is_active") == lit('0'))))
    df_Inactive_Products? = df_Write_Inactive_Products

    # Step: Price Band Range (NumberRange) [converted]
    # Number Range: Price Band Range
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Price_Band_Range = df_Cast_Price_Weight_Numeric.withColumn('price_band', when(col("unit_price").isNull(), lit('UNPRICED')).otherwise(when((col("unit_price").cast("double") >= lit(0.0)) & (col("unit_price").cast("double") < lit(25.0)), lit('BUDGET')).when((col("unit_price").cast("double") >= lit(25.0)) & (col("unit_price").cast("double") < lit(100.0)), lit('STANDARD')).when((col("unit_price").cast("double") >= lit(100.0)) & (col("unit_price").cast("double") < lit(500.0)), lit('PREMIUM')).when((col("unit_price").cast("double") >= lit(500.0)) & (col("unit_price").cast("double") < lit(999999.0)), lit('LUXURY')).otherwise(lit('UNPRICED'))))
    # preserved.fallback='UNPRICED' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: Sort Products By Category (SortRows) [converted]
    # Sort Rows: Sort Products By Category
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Products_By_Category = df_Cast_Price_Weight_Numeric
    _sort_df_Sort_Products_By_Category = _sort_df_Sort_Products_By_Category.withColumn("_sort_ci_category_id", lower(col("category_id").cast("string")))
    df_Sort_Products_By_Category = _sort_df_Sort_Products_By_Category.orderBy(col("_sort_ci_category_id").asc_nulls_last())
    df_Sort_Products_By_Category = df_Sort_Products_By_Category.drop("_sort_ci_category_id")

    # Step: Supplier Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Supplier Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Supplier_Distribution = df_Cast_Price_Weight_Numeric.groupBy('supplier_id').agg(count(lit(1)).alias('product_count'))

    # Step: Write Category Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Category Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_category_dist_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Category_Distribution = df_Category_Distribution
    _out_df_Write_Category_Distribution = df_Write_Category_Distribution.select('category_id', 'product_count')
    writer = _out_df_Write_Category_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_category_dist_.csv')

    # Step: Keep Missing Supplier (FilterRows) [converted]
    # Filter Rows: Keep Missing Supplier
    df_Write_Missing_Supplier_Rows = df_Detect_Blank_Supplier.filter((col("missing_supplier") == lit('Y')))
    df_Supplier_OK = df_Detect_Blank_Supplier.filter(~((col("missing_supplier") == lit('Y'))))
    df_Keep_Missing_Supplier = df_Write_Missing_Supplier_Rows

    # Step: Null Analysis Aggregate (MemoryGroupBy) [partial]
    # Memory Group By: Null Analysis Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Null_Analysis_Aggregate = df_Flag_Nulls.groupBy().agg(_sum(col("null_product_id")).alias('cnt_null_product_id'), _sum(col("null_sku")).alias('cnt_null_sku'), _sum(col("null_category")).alias('cnt_null_category'), _sum(col("null_supplier")).alias('cnt_null_supplier'), _sum(col("null_price")).alias('cnt_null_price'), _sum(col("null_weight")).alias('cnt_null_weight'), count(lit(1)).alias('rows_profiled'))

    # Step: Sort For Duplicate Check (SortRows) [converted]
    # Sort Rows: Sort For Duplicate Check
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_For_Duplicate_Check = df_Flag_Nulls
    _sort_df_Sort_For_Duplicate_Check = _sort_df_Sort_For_Duplicate_Check.withColumn("_sort_ci_product_id", lower(col("product_id").cast("string")))
    _sort_df_Sort_For_Duplicate_Check = _sort_df_Sort_For_Duplicate_Check.withColumn("_sort_ci_sku", lower(col("sku").cast("string")))
    df_Sort_For_Duplicate_Check = _sort_df_Sort_For_Duplicate_Check.orderBy(col("_sort_ci_product_id").asc_nulls_last(), col("_sort_ci_sku").asc_nulls_last())
    df_Sort_For_Duplicate_Check = df_Sort_For_Duplicate_Check.drop("_sort_ci_product_id", "_sort_ci_sku")

    # Step: Active Path Continue (Dummy) [converted]
    # Dummy: Active Path Continue
    # Pass-through step - DataFrame unchanged
    df_Dummy_Active_Path_Continue = df_Active_Path_Continue

    # Step: Write Inactive Products (TextFileOutput) [failed]
    # Pentaho step: Write Inactive Products (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_inactive_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Inactive_Products = df_Inactive_Products?
    _out_df_Write_Inactive_Products = df_Write_Inactive_Products.select('product_id', 'sku', 'product_name', 'is_active', 'status')
    writer = _out_df_Write_Inactive_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_inactive_.csv')

    # Step: Price Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Price Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Price_Distribution = df_Price_Band_Range.groupBy('price_band').agg(count(lit(1)).alias('product_count'), avg(col("unit_price")).alias('avg_price'), _min(col("unit_price")).alias('min_price'), _max(col("unit_price")).alias('max_price'))

    # Step: Join Categories For Validity (MergeJoin) [converted]
    # Merge Join: Join Categories For Validity
    # preserved.join_type='LEFT OUTER'
    # preserved.join_keys=[{'left': 'category_id', 'right': 'ref_category_id'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Join Categories For Validity': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Join_Categories_For_Validity = df_Sort_Products_By_Category.join(df_Rename_Category_Lookup_Keys, (df_Sort_Products_By_Category["category_id"] == df_Rename_Category_Lookup_Keys["ref_category_id"]), how='left')
    # WARNING: MergeJoin 'Join Categories For Validity': column lineage unavailable — cannot disambiguate join output columns
    df_Join_Categories_For_Validity = _joined_df_Join_Categories_For_Validity

    # Step: Write Supplier Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Supplier Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_supplier_dist_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Supplier_Distribution = df_Supplier_Distribution
    _out_df_Write_Supplier_Distribution = df_Write_Supplier_Distribution.select('supplier_id', 'product_count')
    writer = _out_df_Write_Supplier_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_supplier_dist_.csv')

    # Step: Supplier OK (Dummy) [converted]
    # Dummy: Supplier OK
    # Pass-through step - DataFrame unchanged
    df_Dummy_Supplier_OK = df_Supplier_OK

    # Step: Write Missing Supplier Rows (TextFileOutput) [converted]
    # Pentaho step: Write Missing Supplier Rows (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_missing_supplier_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Missing_Supplier_Rows = df_Keep_Missing_Supplier
    _out_df_Write_Missing_Supplier_Rows = df_Write_Missing_Supplier_Rows.select('product_id', 'sku', 'supplier_id')
    writer = _out_df_Write_Missing_Supplier_Rows.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_missing_supplier_.csv')

    # Step: Seed Profile Report Header (Constant) [converted]
    # Add Constants: Seed Profile Report Header
    df_Seed_Profile_Report_Header = df_Null_Analysis_Aggregate
    df_Seed_Profile_Report_Header = df_Seed_Profile_Report_Header.withColumn("report_name", lit('PRODUCT_PROFILING'))
    # preserved.report_name: length='-1', precision='-1'
    df_Seed_Profile_Report_Header = df_Seed_Profile_Report_Header.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Seed_Profile_Report_Header = df_Seed_Profile_Report_Header.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Tag Profile Pivot Key (Constant) [converted]
    # Add Constants: Tag Profile Pivot Key
    df_Tag_Profile_Pivot_Key = df_Null_Analysis_Aggregate
    df_Tag_Profile_Pivot_Key = df_Tag_Profile_Pivot_Key.withColumn("profile_group", lit('PRODUCT_NULL_PROFILE'))
    # preserved.profile_group: length='-1', precision='-1'

    # Step: Duplicate Analysis By Product ID (GroupBy) [converted]
    # Group By: Duplicate Analysis By Product ID
    df_Duplicate_Analysis_By_Product_ID = df_Sort_For_Duplicate_Check.groupBy('product_id').agg(count(lit(1)).alias('dup_count'))

    # Step: Write Price Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Price Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_price_dist_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='price_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='min_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='max_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Price_Distribution = df_Price_Distribution
    _out_df_Write_Price_Distribution = df_Write_Price_Distribution.select('price_band', 'product_count', 'avg_price', 'min_price', 'max_price')
    writer = _out_df_Write_Price_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_price_dist_.csv')

    # Step: Flag Invalid Category (Formula) [converted]
    # Formula: Flag Invalid Category
    df_Flag_Invalid_Category = df_Join_Categories_For_Validity
    df_Flag_Invalid_Category = df_Flag_Invalid_Category.withColumn('formula_result', lit(None))  # empty formula

    # Step: Write Profiling Report JSON (JsonOutput) [converted]
    # Pentaho step: Write Profiling Report JSON (type: JsonOutput)
    df_Write_Profiling_Report_JSON = df_Seed_Profile_Report_Header
    df_Write_Profiling_Report_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/product_profiling_report__summary.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Normalise Null Metrics (Normaliser) [converted]
    # Row Normaliser: Normalise Null Metrics
    _norm_df_Normalise_Null_Metrics_0 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('cnt_null_product_id').alias("metric_name"), col("cnt_null_product_id").alias("null_product_id"))
    _norm_df_Normalise_Null_Metrics_1 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('cnt_null_sku').alias("metric_name"), col("cnt_null_sku").alias("null_sku"))
    _norm_df_Normalise_Null_Metrics_2 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('cnt_null_category').alias("metric_name"), col("cnt_null_category").alias("null_category"))
    _norm_df_Normalise_Null_Metrics_3 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('cnt_null_supplier').alias("metric_name"), col("cnt_null_supplier").alias("null_supplier"))
    _norm_df_Normalise_Null_Metrics_4 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('cnt_null_price').alias("metric_name"), col("cnt_null_price").alias("null_price"))
    _norm_df_Normalise_Null_Metrics_5 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('cnt_null_weight').alias("metric_name"), col("cnt_null_weight").alias("null_weight"))
    _norm_df_Normalise_Null_Metrics_6 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('rows_profiled').alias("metric_name"), col("rows_profiled").alias("rows_profiled"))
    df_Normalise_Null_Metrics = _norm_df_Normalise_Null_Metrics_0
    df_Normalise_Null_Metrics = df_Normalise_Null_Metrics.unionByName(_norm_df_Normalise_Null_Metrics_1, allowMissingColumns=True)
    df_Normalise_Null_Metrics = df_Normalise_Null_Metrics.unionByName(_norm_df_Normalise_Null_Metrics_2, allowMissingColumns=True)
    df_Normalise_Null_Metrics = df_Normalise_Null_Metrics.unionByName(_norm_df_Normalise_Null_Metrics_3, allowMissingColumns=True)
    df_Normalise_Null_Metrics = df_Normalise_Null_Metrics.unionByName(_norm_df_Normalise_Null_Metrics_4, allowMissingColumns=True)
    df_Normalise_Null_Metrics = df_Normalise_Null_Metrics.unionByName(_norm_df_Normalise_Null_Metrics_5, allowMissingColumns=True)
    df_Normalise_Null_Metrics = df_Normalise_Null_Metrics.unionByName(_norm_df_Normalise_Null_Metrics_6, allowMissingColumns=True)

    # Step: Flag Duplicate Rows (Formula) [converted]
    # Formula: Flag Duplicate Rows
    df_Flag_Duplicate_Rows = df_Duplicate_Analysis_By_Product_ID
    df_Flag_Duplicate_Rows = df_Flag_Duplicate_Rows.withColumn('formula_result', lit(None))  # empty formula

    # Step: Invalid Category? (FilterRows) [failed]
    # Filter Rows: Invalid Category?
    df_Write_Invalid_Category_Products = df_Flag_Invalid_Category.filter((col("invalid_category") == lit('Y')))
    df_Valid_Category_Path = df_Flag_Invalid_Category.filter(~((col("invalid_category") == lit('Y'))))
    df_Invalid_Category? = df_Write_Invalid_Category_Products

    # Step: Denormalise Null Metrics (Denormaliser) [converted]
    # Row Denormaliser: Denormalise Null Metrics
    # preserved.target 'cnt_null_product_id' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'cnt_null_sku' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'cnt_null_category' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'cnt_null_supplier' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'rows_profiled' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    df_Denormalise_Null_Metrics = df_Normalise_Null_Metrics.groupBy("profile_group").agg(first(when(col("metric_name") == lit('null_product_id'), col("null_product_id")), ignorenulls=True).alias('cnt_null_product_id'), first(when(col("metric_name") == lit('null_sku'), col("null_sku")), ignorenulls=True).alias('cnt_null_sku'), first(when(col("metric_name") == lit('null_category'), col("null_category")), ignorenulls=True).alias('cnt_null_category'), first(when(col("metric_name") == lit('null_supplier'), col("null_supplier")), ignorenulls=True).alias('cnt_null_supplier'), first(when(col("metric_name") == lit('rows_profiled'), col("rows_profiled")), ignorenulls=True).alias('rows_profiled'))
    df_Denormalise_Null_Metrics = df_Denormalise_Null_Metrics.withColumn("cnt_null_product_id", col("cnt_null_product_id").cast("double"))
    df_Denormalise_Null_Metrics = df_Denormalise_Null_Metrics.withColumn("cnt_null_sku", col("cnt_null_sku").cast("double"))
    df_Denormalise_Null_Metrics = df_Denormalise_Null_Metrics.withColumn("cnt_null_category", col("cnt_null_category").cast("double"))
    df_Denormalise_Null_Metrics = df_Denormalise_Null_Metrics.withColumn("cnt_null_supplier", col("cnt_null_supplier").cast("double"))
    df_Denormalise_Null_Metrics = df_Denormalise_Null_Metrics.withColumn("rows_profiled", col("rows_profiled").cast("double"))

    # Step: Write Profiling Report (TextFileOutput) [converted]
    # Pentaho step: Write Profiling Report (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_profiling_report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='report_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='metric_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='null_product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Profiling_Report = df_Normalise_Null_Metrics
    _out_df_Write_Profiling_Report = df_Write_Profiling_Report.select('report_name', 'metric_name', 'null_product_id', 'batch_id', 'run_id')
    writer = _out_df_Write_Profiling_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_profiling_report_.csv')

    # Step: Keep Duplicates (FilterRows) [converted]
    # Filter Rows: Keep Duplicates
    df_Write_Duplicate_Report_Slice = df_Flag_Duplicate_Rows.filter((col("is_duplicate") == lit('Y')))
    df_Discard_Non_Dupes = df_Flag_Duplicate_Rows.filter(~((col("is_duplicate") == lit('Y'))))
    df_Keep_Duplicates = df_Write_Duplicate_Report_Slice

    # Step: Valid Category Path (Dummy) [converted]
    # Dummy: Valid Category Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_Valid_Category_Path = df_Valid_Category_Path

    # Step: Write Invalid Category Products (TextFileOutput) [failed]
    # Pentaho step: Write Invalid Category Products (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_invalid_category_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='invalid_category' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Invalid_Category_Products = df_Invalid_Category?
    _out_df_Write_Invalid_Category_Products = df_Write_Invalid_Category_Products.select('product_id', 'sku', 'category_id', 'invalid_category')
    writer = _out_df_Write_Invalid_Category_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_invalid_category_.csv')

    # Step: Log Profiling Complete (WriteToLog) [converted]
    # Write to Log: Log Profiling Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=PROFILE_COMPLETE | TRANS=TR_Product_Profile | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Profiling_Complete = logging.getLogger('pentaho.writetolog.Log_Profiling_Complete')
    _log_df_Log_Profiling_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Profiling_Complete = df_Write_Profiling_Report
    _log_rows_df_Log_Profiling_Complete = _log_df_df_Log_Profiling_Complete.take(20)
    _log_df_Log_Profiling_Complete.info('Log Profiling Complete' + ' | columns=' + str(_log_df_df_Log_Profiling_Complete.columns))
    _log_df_Log_Profiling_Complete.info('AUDIT | EVENT=PROFILE_COMPLETE | TRANS=TR_Product_Profile | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Profiling_Complete:
        _log_df_Log_Profiling_Complete.info('Log Profiling Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Profiling_Complete = df_Write_Profiling_Report

    # Step: Discard Non Dupes (Dummy) [converted]
    # Dummy: Discard Non Dupes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Discard_Non_Dupes = df_Discard_Non_Dupes

    # Step: Write Duplicate Report Slice (TextFileOutput) [converted]
    # Pentaho step: Write Duplicate Report Slice (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/product_duplicates_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dup_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Duplicate_Report_Slice = df_Keep_Duplicates
    _out_df_Write_Duplicate_Report_Slice = df_Write_Duplicate_Report_Slice.select('product_id', 'dup_count', 'batch_id', 'run_id')
    writer = _out_df_Write_Duplicate_Report_Slice.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_duplicates_.csv')

    # Step: Profile Complete (Dummy) [converted]
    # Dummy: Profile Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Profile_Complete = df_Log_Profiling_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Profile_Complete
