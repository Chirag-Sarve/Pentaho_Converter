"""PySpark module migrated from Pentaho transformation: TR_Product_Enrichment.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/TR_Product_Enrichment.ktr
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
    lower,
    trim,
    when,
    coalesce,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_product_enrichment")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Product_Enrichment`` step-for-step."""
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

    # Step: Metadata Inject Staging Reader (MetaInject) [converted]
    # ETL Metadata Injection: Metadata Inject Staging Reader
    # preserved.filename='${PROJECT_HOME}/transformations/staging/TR_Product_Extract.ktr'
    # preserved.no_execution=False
    # preserved.mappings=[{'source_field': 'dataset_path', 'target_step': '', 'target_attribute': '', 'target_detail': ''}]
    # LIMITATION: Runtime metadata injection into a child transformation is not available in Spark; mappings preserved as placeholders.
    _meta_inject_df_Metadata_Inject_Staging_Reader = {'target': '${PROJECT_HOME}/transformations/staging/TR_Product_Extract.ktr', 'mappings': [{'source_field': 'dataset_path', 'target_step': '', 'target_attribute': '', 'target_detail': ''}], 'parameters': [], 'no_execution': False}
    # TODO: apply _meta_inject_df_Metadata_Inject_Staging_Reader mappings before running child notebook/job
    df_Metadata_Inject_Staging_Reader = spark.createDataFrame([], '_metainject STRING').limit(0)

    # Step: Optional Excel Attributes (ExcelInput) [converted]
    # Excel Input: Optional Excel Attributes
    df_Optional_Excel_Attributes = (
        spark.read.format('com.crealytics.spark.excel')
        .option('sheetName', 'Sheet1')
        .option('header', 'true')
        .load('/product_attributes.xlsx')
    )

    # Step: Read Categories For Join (TextFileInput) [converted]
    # Pentaho step: Read Categories For Join (type: TextFileInput)
    # INFO: preserved Legacy Text File Input option: date_format_lenient='Y'
    # Pentaho filename: /categories.csv
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    df_Read_Categories_For_Join = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("quote", '"')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('category_id STRING, category_name STRING, parent_category_id STRING, description STRING, is_active STRING')
        .csv(f'{data_dir}/categories.csv')
    )
    # INFO: preserved.field_format name='category_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='category_name' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='parent_category_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='description' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='is_active' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    df_Read_Categories_For_Join = df_Read_Categories_For_Join.select(col('category_id').alias('category_id'), col('category_name').alias('category_name'), col('parent_category_id').alias('parent_category_id'), col('description').alias('description'), col('is_active').alias('is_active'))
    df_Read_Categories_For_Join = df_Read_Categories_For_Join.filter(~((col('category_id').isNull() | (length(trim(col('category_id').cast('string'))) == 0)) & (col('category_name').isNull() | (length(trim(col('category_name').cast('string'))) == 0)) & (col('parent_category_id').isNull() | (length(trim(col('parent_category_id').cast('string'))) == 0)) & (col('description').isNull() | (length(trim(col('description').cast('string'))) == 0)) & (col('is_active').isNull() | (length(trim(col('is_active').cast('string'))) == 0))))
    df_Read_Categories_For_Join = df_Read_Categories_For_Join.withColumn('source_row_num', monotonically_increasing_id())

    # Step: Read Cleansed Products (CsvInput) [converted]
    # CSV Input: Read Cleansed Products
    df_Read_Cleansed_Products = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_id STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, created_date STRING, description STRING, upc STRING, barcode STRING, length_cm DOUBLE, width_cm DOUBLE, height_cm DOUBLE, status STRING, weight_unit STRING, product_bk_checksum STRING, dedupe_key STRING, sku_prefix STRING, sku_cat_code STRING, sku_seq STRING, dup_count INT, batch_id STRING, run_id STRING, source_row_num INT')
        .load(f'{data_dir}/products_cleansed_.csv')
    )

    # Step: Read Suppliers For Join (CsvInput) [converted]
    # CSV Input: Read Suppliers For Join
    df_Read_Suppliers_For_Join = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('supplier_id STRING, supplier_name STRING, contact_name STRING, email STRING, phone STRING, address_line1 STRING, city STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, lead_time_days STRING, is_active STRING, created_date STRING')
        .load('/suppliers.csv')
    )

    # Step: Prepare Category Stream (SelectValues) [converted]
    # Select Values: Prepare Category Stream
    df_Prepare_Category_Stream = df_Read_Categories_For_Join.select(col("category_id").alias("cat_category_id"), col("category_name").alias("category_name"), col("parent_category_id").alias("parent_category_id"), col("is_active").alias("category_is_active"))

    # Step: Sort Products Category Key (SortRows) [converted]
    # Sort Rows: Sort Products Category Key
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Products_Category_Key = df_Read_Cleansed_Products
    _sort_df_Sort_Products_Category_Key = _sort_df_Sort_Products_Category_Key.withColumn("_sort_ci_category_id", lower(col("category_id").cast("string")))
    df_Sort_Products_Category_Key = _sort_df_Sort_Products_Category_Key.orderBy(col("_sort_ci_category_id").asc_nulls_last())
    df_Sort_Products_Category_Key = df_Sort_Products_Category_Key.drop("_sort_ci_category_id")

    # Step: Prepare Supplier Stream (SelectValues) [converted]
    # Select Values: Prepare Supplier Stream
    df_Prepare_Supplier_Stream = df_Read_Suppliers_For_Join.select(col("supplier_id").alias("sup_supplier_id"), col("supplier_name").alias("supplier_name"), col("country_code").alias("supplier_country"), col("lead_time_days").alias("supplier_lead_time_days"), col("preferred_currency").alias("supplier_currency"), col("is_active").alias("supplier_is_active"))

    # Step: Sort Categories Key (SortRows) [converted]
    # Sort Rows: Sort Categories Key
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Categories_Key = df_Prepare_Category_Stream
    _sort_df_Sort_Categories_Key = _sort_df_Sort_Categories_Key.withColumn("_sort_ci_cat_category_id", lower(col("cat_category_id").cast("string")))
    df_Sort_Categories_Key = _sort_df_Sort_Categories_Key.orderBy(col("_sort_ci_cat_category_id").asc_nulls_last())
    df_Sort_Categories_Key = df_Sort_Categories_Key.drop("_sort_ci_cat_category_id")

    # Step: Sort Suppliers Key (SortRows) [converted]
    # Sort Rows: Sort Suppliers Key
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Suppliers_Key = df_Prepare_Supplier_Stream
    _sort_df_Sort_Suppliers_Key = _sort_df_Sort_Suppliers_Key.withColumn("_sort_ci_sup_supplier_id", lower(col("sup_supplier_id").cast("string")))
    df_Sort_Suppliers_Key = _sort_df_Sort_Suppliers_Key.orderBy(col("_sort_ci_sup_supplier_id").asc_nulls_last())
    df_Sort_Suppliers_Key = df_Sort_Suppliers_Key.drop("_sort_ci_sup_supplier_id")

    # Step: Join Category (MergeJoin) [converted]
    # Merge Join: Join Category
    # preserved.join_type='LEFT OUTER'
    # preserved.join_keys=[{'left': 'category_id', 'right': 'cat_category_id'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Join Category': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Join_Category = df_Sort_Products_Category_Key.join(df_Sort_Categories_Key, (df_Sort_Products_Category_Key["category_id"] == df_Sort_Categories_Key["cat_category_id"]), how='left')
    # WARNING: MergeJoin 'Join Category': column lineage unavailable — cannot disambiguate join output columns
    df_Join_Category = _joined_df_Join_Category

    # Step: Sort After Category Join (SortRows) [converted]
    # Sort Rows: Sort After Category Join
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_After_Category_Join = df_Join_Category
    _sort_df_Sort_After_Category_Join = _sort_df_Sort_After_Category_Join.withColumn("_sort_ci_supplier_id", lower(col("supplier_id").cast("string")))
    df_Sort_After_Category_Join = _sort_df_Sort_After_Category_Join.orderBy(col("_sort_ci_supplier_id").asc_nulls_last())
    df_Sort_After_Category_Join = df_Sort_After_Category_Join.drop("_sort_ci_supplier_id")

    # Step: Join Supplier (MergeJoin) [converted]
    # Merge Join: Join Supplier
    # preserved.join_type='LEFT OUTER'
    # preserved.join_keys=[{'left': 'supplier_id', 'right': 'sup_supplier_id'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Join Supplier': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Join_Supplier = df_Sort_After_Category_Join.join(df_Sort_Suppliers_Key, (df_Sort_After_Category_Join["supplier_id"] == df_Sort_Suppliers_Key["sup_supplier_id"]), how='left')
    # WARNING: MergeJoin 'Join Supplier': column lineage unavailable — cannot disambiguate join output columns
    df_Join_Supplier = _joined_df_Join_Supplier

    # Step: Database Join Category SK (DBJoin) [partial]
    # Database Join: Database Join Category SK
    # preserved.connection='conn_dev_dwh'
    # preserved.sql="SELECT category_sk FROM retail_dwh.dim_category WHERE category_id = ? AND is_current = 'Y'"
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'category_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_Database_Join_Category_SK = "SELECT category_sk FROM retail_dwh.dim_category WHERE category_id = ? AND is_current = 'Y'"
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template="SELECT category_sk FROM retail_dwh.dim_category WHERE category_id = :category_id AND is_current = 'Y'"
    _param_fields_df_Database_Join_Category_SK = ['category_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement("SELECT category_sk FROM retail_dwh.dim_category WHERE category_id = ? AND is_current = 'Y'")
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_Database_Join_Category_SK, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_Database_Join_Category_SK = df_Join_Supplier
    # Join type preserved as 'left'; join keys=['category_id', '\n        ']

    # Step: Capture Enrichment Now (SystemInfo) [converted]
    # System Info: Capture Enrichment Now
    df_Capture_Enrichment_Now = df_Database_Join_Category_SK
    df_Capture_Enrichment_Now = df_Capture_Enrichment_Now.withColumn("sys_today", lit(''))
    df_Capture_Enrichment_Now = df_Capture_Enrichment_Now.withColumn("sys_now", current_date())

    # Step: Cast Enrichment Numerics (SelectValues) [converted]
    # Select Values: Cast Enrichment Numerics
    df_Cast_Enrichment_Numerics = df_Capture_Enrichment_Now.select(col("product_id").alias("product_id"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("category_name").alias("category_name"), col("parent_category_id").alias("parent_category_id"), col("supplier_id").alias("supplier_id"), col("supplier_name").alias("supplier_name"), col("supplier_country").alias("supplier_country"), col("supplier_lead_time_days").alias("supplier_lead_time_days"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("length_cm").alias("length_cm"), col("width_cm").alias("width_cm"), col("height_cm").alias("height_cm"), col("is_active").alias("is_active"), col("status").alias("status"), col("created_date").alias("created_date"), col("description").alias("description"), col("upc").alias("upc"), col("barcode").alias("barcode"), col("product_bk_checksum").alias("product_bk_checksum"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("sys_today").alias("sys_today"))

    # Step: Calculate Margin And Volume (Calculator) [converted]
    # Calculator: Calculate Margin And Volume
    df_Calculate_Margin_And_Volume = df_Cast_Enrichment_Numerics
    df_Calculate_Margin_And_Volume = df_Calculate_Margin_And_Volume.withColumn("margin_amount", ((col("unit_price") - col("unit_cost"))).cast('decimal(38,4)'))
    df_Calculate_Margin_And_Volume = df_Calculate_Margin_And_Volume.withColumn("volume_cm3", ((col("length_cm") * col("width_cm"))).cast('decimal(38,4)'))
    df_Calculate_Margin_And_Volume = df_Calculate_Margin_And_Volume.withColumn("volume_cm3", ((col("volume_cm3") * col("height_cm"))).cast('decimal(38,4)'))

    # Step: Calculate Commercial Metrics (Formula) [converted]
    # Formula: Calculate Commercial Metrics
    df_Calculate_Commercial_Metrics = df_Calculate_Margin_And_Volume
    df_Calculate_Commercial_Metrics = df_Calculate_Commercial_Metrics.withColumn('formula_result', lit(None))  # empty formula

    # Step: Calculate Lifecycle Stage (Formula) [converted]
    # Formula: Calculate Lifecycle Stage
    df_Calculate_Lifecycle_Stage = df_Calculate_Commercial_Metrics
    df_Calculate_Lifecycle_Stage = df_Calculate_Lifecycle_Stage.withColumn('formula_result', lit(None))  # empty formula

    # Step: ABC Classification (NumberRange) [converted]
    # Number Range: ABC Classification
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_ABC_Classification = df_Calculate_Lifecycle_Stage.withColumn('abc_class', when(col("unit_price").isNull(), lit('C')).otherwise(when((col("unit_price").cast("double") >= lit(500.0)) & (col("unit_price").cast("double") < lit(999999.0)), lit('A')).when((col("unit_price").cast("double") >= lit(100.0)) & (col("unit_price").cast("double") < lit(500.0)), lit('B')).when((col("unit_price").cast("double") >= lit(0.0)) & (col("unit_price").cast("double") < lit(100.0)), lit('C')).otherwise(lit('C'))))
    # preserved.fallback='C' rules=3 lower_inclusive=True upper_inclusive=False

    # Step: Price Band Enrichment (NumberRange) [converted]
    # Number Range: Price Band Enrichment
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Price_Band_Enrichment = df_ABC_Classification.withColumn('price_band', when(col("selling_price").isNull(), lit('UNPRICED')).otherwise(when((col("selling_price").cast("double") >= lit(0.0)) & (col("selling_price").cast("double") < lit(25.0)), lit('BUDGET')).when((col("selling_price").cast("double") >= lit(25.0)) & (col("selling_price").cast("double") < lit(100.0)), lit('STANDARD')).when((col("selling_price").cast("double") >= lit(100.0)) & (col("selling_price").cast("double") < lit(500.0)), lit('PREMIUM')).when((col("selling_price").cast("double") >= lit(500.0)) & (col("selling_price").cast("double") < lit(999999.0)), lit('LUXURY')).otherwise(lit('UNPRICED'))))
    # preserved.fallback='UNPRICED' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: Map Lifecycle Labels (ValueMapper) [converted]
    # Value Mapper: Map Lifecycle Labels
    df_Map_Lifecycle_Labels = df_Price_Band_Enrichment.withColumn("lifecycle_label", when((lower(col("lifecycle_stage")) == lower(lit('INTRODUCTION'))), lit('New')).when((lower(col("lifecycle_stage")) == lower(lit('GROWTH'))), lit('Expanding')).when((lower(col("lifecycle_stage")) == lower(lit('MATURITY'))), lit('Core')).when((lower(col("lifecycle_stage")) == lower(lit('DECLINE'))), lit('Wind-down')).when((lower(col("lifecycle_stage")) == lower(lit('RETIRED'))), lit('Discontinued')).when((col("lifecycle_stage").isNull() | (col("lifecycle_stage") == lit(''))), col("lifecycle_stage")).otherwise(lit('Core')))
    # preserved.case_sensitive=False mappings=5 default='Core'

    # Step: Route By ABC Class (SwitchCase) [converted]
    # Switch / Case: Route By ABC Class
    # preserved.fieldname='abc_class'
    # preserved.switch_field='abc_class'
    # preserved.cases=[{'value': 'A', 'target_step': 'Tag Class A'}, {'value': 'B', 'target_step': 'Tag Class B'}, {'value': 'C', 'target_step': 'Tag Class C'}]
    # preserved.default_target_step='Tag Class C'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'A', 'target_step': 'Tag Class A'}, {'value': 'B', 'target_step': 'Tag Class B'}, {'value': 'C', 'target_step': 'Tag Class C'}]
    _routed_df_Route_By_ABC_Class = df_Map_Lifecycle_Labels.withColumn('_route_Route_By_ABC_Class', when(col("abc_class") == lit('A'), lit('Tag Class A')).when(col("abc_class") == lit('B'), lit('Tag Class B')).when(col("abc_class") == lit('C'), lit('Tag Class C')).otherwise(lit('Tag Class C')))
    df_Tag_Class_A = _routed_df_Route_By_ABC_Class.filter(col('_route_Route_By_ABC_Class') == lit('Tag Class A')).drop('_route_Route_By_ABC_Class')
    df_Tag_Class_B = _routed_df_Route_By_ABC_Class.filter(col('_route_Route_By_ABC_Class') == lit('Tag Class B')).drop('_route_Route_By_ABC_Class')
    df_Tag_Class_C = _routed_df_Route_By_ABC_Class.filter(col('_route_Route_By_ABC_Class') == lit('Tag Class C')).drop('_route_Route_By_ABC_Class')
    df_Route_By_ABC_Class = df_Tag_Class_A

    # Step: Tag Class A (Constant) [converted]
    # Add Constants: Tag Class A
    df_Tag_Class_A = df_Route_By_ABC_Class
    df_Tag_Class_A = df_Tag_Class_A.withColumn("abc_priority", lit('HIGH'))
    # preserved.abc_priority: length='-1', precision='-1'

    # Step: Tag Class B (Constant) [converted]
    # Add Constants: Tag Class B
    df_Tag_Class_B = df_Route_By_ABC_Class
    df_Tag_Class_B = df_Tag_Class_B.withColumn("abc_priority", lit('MEDIUM'))
    # preserved.abc_priority: length='-1', precision='-1'

    # Step: Tag Class C (Constant) [converted]
    # Add Constants: Tag Class C
    df_Tag_Class_C = df_Route_By_ABC_Class
    df_Tag_Class_C = df_Tag_Class_C.withColumn("abc_priority", lit('LOW'))
    # preserved.abc_priority: length='-1', precision='-1'

    # Step: Unify ABC Routes (Dummy) [converted]
    # Dummy: Unify ABC Routes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Unify_ABC_Routes = df_Tag_Class_A

    # Step: Clone Enrichment Audit Fanout (CloneRow) [converted]
    # Clone Row: Clone Enrichment Audit Fanout
    # preserved.nr_clones=1
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='1'
    _clone_parts_df_Clone_Enrichment_Audit_Fanout = []
    _base_df_Clone_Enrichment_Audit_Fanout = df_Dummy_Unify_ABC_Routes
    _orig_df_Clone_Enrichment_Audit_Fanout = _base_df_Clone_Enrichment_Audit_Fanout
    _clone_parts_df_Clone_Enrichment_Audit_Fanout.append(_orig_df_Clone_Enrichment_Audit_Fanout)
    for _ci in range(1, 1 + 1):
        _c = _base_df_Clone_Enrichment_Audit_Fanout
        _clone_parts_df_Clone_Enrichment_Audit_Fanout.append(_c)
    df_Clone_Enrichment_Audit_Fanout = _clone_parts_df_Clone_Enrichment_Audit_Fanout[0]
    for _part in _clone_parts_df_Clone_Enrichment_Audit_Fanout[1:]:
        df_Clone_Enrichment_Audit_Fanout = df_Clone_Enrichment_Audit_Fanout.unionByName(_part, allowMissingColumns=True)

    # Step: Write Enriched Products (TextFileOutput) [converted]
    # Pentaho step: Write Enriched Products (type: TextFileOutput)
    # Pentaho filename: /output/product/enriched/products_enriched_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='parent_category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_country' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_lead_time_days' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='brand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='profit_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='selling_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='volume_cm3' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_age_years' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='lifecycle_stage' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='lifecycle_label' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='abc_class' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='abc_priority' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='price_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='weight_kg' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='length_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='width_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='height_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='upc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='barcode' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='created_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='description' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_bk_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Enriched_Products = df_Clone_Enrichment_Audit_Fanout
    _out_df_Write_Enriched_Products = df_Write_Enriched_Products.select('product_id', 'sku', 'product_name', 'category_id', 'category_name', 'parent_category_id', 'supplier_id', 'supplier_name', 'supplier_country', 'supplier_lead_time_days', 'brand', 'unit_cost', 'unit_price', 'margin_amount', 'profit_pct', 'discount_pct', 'selling_price', 'tax_amount', 'volume_cm3', 'product_age_years', 'lifecycle_stage', 'lifecycle_label', 'abc_class', 'abc_priority', 'price_band', 'currency_code', 'weight_kg', 'length_cm', 'width_cm', 'height_cm', 'upc', 'barcode', 'is_active', 'status', 'created_date', 'description', 'product_bk_checksum', 'batch_id', 'run_id')
    writer = _out_df_Write_Enriched_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/products_enriched_.csv')

    # Step: Log Enrichment Complete (WriteToLog) [converted]
    # Write to Log: Log Enrichment Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=ENRICHMENT_COMPLETE | TRANS=TR_Product_Enrichment | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Enrichment_Complete = logging.getLogger('pentaho.writetolog.Log_Enrichment_Complete')
    _log_df_Log_Enrichment_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Enrichment_Complete = df_Write_Enriched_Products
    _log_rows_df_Log_Enrichment_Complete = _log_df_df_Log_Enrichment_Complete.take(20)
    _log_df_Log_Enrichment_Complete.info('Log Enrichment Complete' + ' | columns=' + str(_log_df_df_Log_Enrichment_Complete.columns))
    _log_df_Log_Enrichment_Complete.info('AUDIT | EVENT=ENRICHMENT_COMPLETE | TRANS=TR_Product_Enrichment | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Enrichment_Complete:
        _log_df_Log_Enrichment_Complete.info('Log Enrichment Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Enrichment_Complete = df_Write_Enriched_Products

    # Step: Copy Enriched To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Enriched To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Enriched To Result'
    _result_rows_df_Copy_Enriched_To_Result = df_Log_Enrichment_Complete
    _pentaho_result_rows['Copy Enriched To Result'] = _result_rows_df_Copy_Enriched_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Enriched_To_Result
    df_Copy_Enriched_To_Result = df_Log_Enrichment_Complete

    # Step: Enrichment Complete (Dummy) [converted]
    # Dummy: Enrichment Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Enrichment_Complete = df_Copy_Enriched_To_Result

    log_event(_LOG, "transformation_end")
    return df_Dummy_Enrichment_Complete
