"""PySpark module migrated from Pentaho transformation: TR_Inventory_Extract.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Inventory_Extract.ktr
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
    upper,
    when,
    coalesce,
    row_number,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_inventory_extract")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Inventory_Extract`` step-for-step."""
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

    # Step: Generate Extract Control Row (RowGenerator) [converted]
    # Generate Rows: Generate Extract Control Row
    data = [('INVENTORY', 'FULL')]
    df_Generate_Extract_Control_Row = spark.createDataFrame(data, ['extract_module', 'extract_mode'])

    # Step: Read inventory.csv (CsvInput) [converted]
    # CSV Input: Read inventory.csv
    df_Read_inventory.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, store_id STRING, product_id STRING, quantity_on_hand STRING, quantity_reserved STRING, reorder_level STRING, reorder_quantity STRING, last_stocktake_date STRING, bin_location STRING, is_low_stock STRING')
        .load('/inventory.csv')
    )

    # Step: Read products.csv (CsvInput) [converted]
    # CSV Input: Read products.csv
    df_Read_products.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_id STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, created_date STRING, description STRING')
        .load('/products.csv')
    )

    # Step: Read regions.csv (CsvInput) [converted]
    # CSV Input: Read regions.csv
    df_Read_regions.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('region_id STRING, region_name STRING, country_code STRING, country_name STRING, continent STRING, currency_code STRING, timezone STRING, is_active STRING')
        .load('/regions.csv')
    )

    # Step: Read stores.csv (TextFileInput) [converted]
    # Pentaho step: Read stores.csv (type: TextFileInput)
    # INFO: preserved Legacy Text File Input option: date_format_lenient='Y'
    # Pentaho filename: /stores.csv
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    df_Read_stores.csv = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("quote", '"')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('store_id STRING, store_name STRING, store_type STRING, region_id STRING, address_line1 STRING, city STRING, postal_code STRING, country_code STRING, phone STRING, manager_name STRING, square_footage STRING, open_date STRING, is_active STRING')
        .csv(f'{data_dir}/stores.csv')
    )
    # INFO: preserved.field_format name='store_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='store_name' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='store_type' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='region_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='address_line1' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='city' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='postal_code' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='country_code' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='phone' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='manager_name' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='square_footage' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='open_date' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='is_active' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    df_Read_stores.csv = df_Read_stores.csv.select(col('store_id').alias('store_id'), col('store_name').alias('store_name'), col('store_type').alias('store_type'), col('region_id').alias('region_id'), col('address_line1').alias('address_line1'), col('city').alias('city'), col('postal_code').alias('postal_code'), col('country_code').alias('country_code'), col('phone').alias('phone'), col('manager_name').alias('manager_name'), col('square_footage').alias('square_footage'), col('open_date').alias('open_date'), col('is_active').alias('is_active'))
    df_Read_stores.csv = df_Read_stores.csv.filter(~((col('store_id').isNull() | (length(trim(col('store_id').cast('string'))) == 0)) & (col('store_name').isNull() | (length(trim(col('store_name').cast('string'))) == 0)) & (col('store_type').isNull() | (length(trim(col('store_type').cast('string'))) == 0)) & (col('region_id').isNull() | (length(trim(col('region_id').cast('string'))) == 0)) & (col('address_line1').isNull() | (length(trim(col('address_line1').cast('string'))) == 0)) & (col('city').isNull() | (length(trim(col('city').cast('string'))) == 0)) & (col('postal_code').isNull() | (length(trim(col('postal_code').cast('string'))) == 0)) & (col('country_code').isNull() | (length(trim(col('country_code').cast('string'))) == 0)) & (col('phone').isNull() | (length(trim(col('phone').cast('string'))) == 0)) & (col('manager_name').isNull() | (length(trim(col('manager_name').cast('string'))) == 0)) & (col('square_footage').isNull() | (length(trim(col('square_footage').cast('string'))) == 0)) & (col('open_date').isNull() | (length(trim(col('open_date').cast('string'))) == 0)) & (col('is_active').isNull() | (length(trim(col('is_active').cast('string'))) == 0))))
    df_Read_stores.csv = df_Read_stores.csv.withColumn('source_row_num', monotonically_increasing_id())

    # Step: Read suppliers.csv (CsvInput) [converted]
    # CSV Input: Read suppliers.csv
    df_Read_suppliers.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('supplier_id STRING, supplier_name STRING, contact_name STRING, email STRING, phone STRING, address_line1 STRING, city STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, lead_time_days STRING, is_active STRING, created_date STRING')
        .load('/suppliers.csv')
    )

    # Step: Write Extract Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Extract Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/inventory/inventory_extract_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extract_Rejects = df_Write_Extract_Rejects
    _out_df_Write_Extract_Rejects = df_Write_Extract_Rejects.select('inventory_id', 'product_id', 'store_id', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Extract_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_extract_rejects_.csv')

    # Step: Get Variables (GetVariable) [converted]
    # Get Variables: Get Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'dataset_path', 'variable': '${DATASET_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'dataset_path', 'project_home', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Variables = df_Generate_Extract_Control_Row
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
    df_Get_Variables = df_Get_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Variables = df_Get_Variables.withColumn('run_id', lit(_run_id_resolved))
    # field 'dataset_path' from variable string '${DATASET_PATH}'
    # preserved.field.dataset_path.trim_type='none'
    # preserved.field.dataset_path.type='String'
    _dataset_path_resolved = None
    _dbu__dataset_path_resolved = globals().get('dbutils')
    if _dbu__dataset_path_resolved is not None and hasattr(_dbu__dataset_path_resolved, 'widgets'):
        try:
            _dataset_path_resolved = _dbu__dataset_path_resolved.widgets.get('DATASET_PATH')
        except Exception:
            _dataset_path_resolved = None
    if _dataset_path_resolved in (None, ''):
        import os as _os__dataset_path_resolved
        _dataset_path_resolved = _os__dataset_path_resolved.environ.get('DATASET_PATH')
    if _dataset_path_resolved in (None, ''):
        try:
            _dataset_path_resolved = spark.conf.get('pentaho.var.DATASET_PATH')
        except Exception:
            _dataset_path_resolved = None
    if _dataset_path_resolved in (None, ''):
        _dataset_path_resolved = ''
    if _dataset_path_resolved is None:
        _dataset_path_resolved = ''
    df_Get_Variables = df_Get_Variables.withColumn('dataset_path', lit(_dataset_path_resolved))
    # field 'project_home' from variable string '${PROJECT_HOME}'
    # preserved.field.project_home.trim_type='none'
    # preserved.field.project_home.type='String'
    _project_home_resolved = None
    _dbu__project_home_resolved = globals().get('dbutils')
    if _dbu__project_home_resolved is not None and hasattr(_dbu__project_home_resolved, 'widgets'):
        try:
            _project_home_resolved = _dbu__project_home_resolved.widgets.get('PROJECT_HOME')
        except Exception:
            _project_home_resolved = None
    if _project_home_resolved in (None, ''):
        import os as _os__project_home_resolved
        _project_home_resolved = _os__project_home_resolved.environ.get('PROJECT_HOME')
    if _project_home_resolved in (None, ''):
        try:
            _project_home_resolved = spark.conf.get('pentaho.var.PROJECT_HOME')
        except Exception:
            _project_home_resolved = None
    if _project_home_resolved in (None, ''):
        _project_home_resolved = ''
    if _project_home_resolved is None:
        _project_home_resolved = ''
    df_Get_Variables = df_Get_Variables.withColumn('project_home', lit(_project_home_resolved))
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
    df_Get_Variables = df_Get_Variables.withColumn('current_date', lit(_current_date_resolved))

    # Step: Sample Inventory Peek (SampleRows) [converted]
    # Sample Rows: Sample Inventory Peek
    _w_sr_df_Sample_Inventory_Peek = Window.orderBy(monotonically_increasing_id())
    df_Sample_Inventory_Peek = df_Read_inventory.csv.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Inventory_Peek))
    # preserved.lines_range='1..5' ranges=[(1, 5)]
    df_Sample_Inventory_Peek = df_Sample_Inventory_Peek.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 5)))
    df_Sample_Inventory_Peek = df_Sample_Inventory_Peek.drop('_sr_rn')

    # Step: Write Staging Land Inventory (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/staging/stg_raw_inventory_
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
    df_Write_Staging_Land_Inventory = df_Read_inventory.csv
    _out_df_Write_Staging_Land_Inventory = df_Write_Staging_Land_Inventory.select('inventory_id', 'store_id', 'product_id', 'quantity_on_hand', 'quantity_reserved', 'reorder_level', 'reorder_quantity', 'last_stocktake_date', 'bin_location', 'is_low_stock')
    writer = _out_df_Write_Staging_Land_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_inventory_.csv')

    # Step: Prepare Product Join Keys (SelectValues) [converted]
    # Select Values: Prepare Product Join Keys
    df_Prepare_Product_Join_Keys = df_Read_products.csv.select(col("product_id").alias("prd_product_id"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"))

    # Step: Write Staging Land Products (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Products (type: TextFileOutput)
    # Pentaho filename: /output/inventory/staging/stg_raw_products_
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
    df_Write_Staging_Land_Products = df_Read_products.csv
    _out_df_Write_Staging_Land_Products = df_Write_Staging_Land_Products.select('product_id', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'created_date', 'description')
    writer = _out_df_Write_Staging_Land_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_products_.csv')

    # Step: Prepare Region Lookup (SelectValues) [converted]
    # Select Values: Prepare Region Lookup
    df_Prepare_Region_Lookup = df_Read_regions.csv.select(col("region_id").alias("lkp_region_id"), col("region_name").alias("region_name"), col("country_code").alias("region_country"))

    # Step: Prepare Store Lookup (SelectValues) [converted]
    # Select Values: Prepare Store Lookup
    df_Prepare_Store_Lookup = df_Read_stores.csv.select(col("store_id").alias("lkp_store_id"), col("store_name").alias("store_name"), col("store_type").alias("store_type"), col("region_id").alias("region_id"), col("square_footage").alias("square_footage"))

    # Step: Write Staging Land Stores (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Stores (type: TextFileOutput)
    # Pentaho filename: /output/inventory/staging/stg_raw_stores_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_type' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line1' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='city' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='postal_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='phone' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='manager_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='square_footage' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='open_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Stores = df_Read_stores.csv
    _out_df_Write_Staging_Land_Stores = df_Write_Staging_Land_Stores.select('store_id', 'store_name', 'store_type', 'region_id', 'address_line1', 'city', 'postal_code', 'country_code', 'phone', 'manager_name', 'square_footage', 'open_date', 'is_active', 'source_row_num')
    writer = _out_df_Write_Staging_Land_Stores.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_stores_.csv')

    # Step: Prepare Supplier Lookup (SelectValues) [converted]
    # Select Values: Prepare Supplier Lookup
    df_Prepare_Supplier_Lookup = df_Read_suppliers.csv.select(col("supplier_id").alias("lkp_supplier_id"), col("supplier_name").alias("supplier_name"), col("lead_time_days").alias("lead_time_days"), col("is_active").alias("supplier_active"))

    # Step: Write Staging Land Suppliers (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Suppliers (type: TextFileOutput)
    # Pentaho filename: /output/inventory/staging/stg_raw_suppliers_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='contact_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='email' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='phone' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line1' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='city' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='postal_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='preferred_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='lead_time_days' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='created_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Suppliers = df_Read_suppliers.csv
    _out_df_Write_Staging_Land_Suppliers = df_Write_Staging_Land_Suppliers.select('supplier_id', 'supplier_name', 'contact_name', 'email', 'phone', 'address_line1', 'city', 'postal_code', 'country_code', 'country_name', 'preferred_currency', 'lead_time_days', 'is_active', 'created_date')
    writer = _out_df_Write_Staging_Land_Suppliers.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_suppliers_.csv')

    # Step: Validate inventory.csv (GetFileNames) [failed]
    # Get File Names: Validate inventory.csv
    _list_path_df_Validate_inventory.csv = '/'
    try:
        _fs_entries_df_Validate_inventory.csv = dbutils.fs.ls(_list_path_df_Validate_inventory.csv)
        df_Validate_inventory.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_inventory.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_inventory.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_inventory.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate products.csv (GetFileNames) [failed]
    # Get File Names: Validate products.csv
    _list_path_df_Validate_products.csv = '/'
    try:
        _fs_entries_df_Validate_products.csv = dbutils.fs.ls(_list_path_df_Validate_products.csv)
        df_Validate_products.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_products.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_products.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_products.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate stores.csv (GetFileNames) [failed]
    # Get File Names: Validate stores.csv
    _list_path_df_Validate_stores.csv = '/'
    try:
        _fs_entries_df_Validate_stores.csv = dbutils.fs.ls(_list_path_df_Validate_stores.csv)
        df_Validate_stores.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_stores.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_stores.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_stores.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate suppliers.csv (GetFileNames) [failed]
    # Get File Names: Validate suppliers.csv
    _list_path_df_Validate_suppliers.csv = '/'
    try:
        _fs_entries_df_Validate_suppliers.csv = dbutils.fs.ls(_list_path_df_Validate_suppliers.csv)
        df_Validate_suppliers.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_suppliers.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_suppliers.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_suppliers.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Capture Extract Timestamp (SystemInfo) [converted]
    # System Info: Capture Extract Timestamp
    df_Capture_Extract_Timestamp = df_Sample_Inventory_Peek
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_ts", current_date())
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_start", current_date())

    # Step: Sort Products By Product (SortRows) [converted]
    # Sort Rows: Sort Products By Product
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Products_By_Product = df_Prepare_Product_Join_Keys
    _sort_df_Sort_Products_By_Product = _sort_df_Sort_Products_By_Product.withColumn("_sort_ci_prd_product_id", lower(col("prd_product_id").cast("string")))
    df_Sort_Products_By_Product = _sort_df_Sort_Products_By_Product.orderBy(col("_sort_ci_prd_product_id").asc_nulls_last())
    df_Sort_Products_By_Product = df_Sort_Products_By_Product.drop("_sort_ci_prd_product_id")

    # Step: Tag Inventory File Check (SelectValues) [converted]
    # Select Values: Tag Inventory File Check
    df_Tag_Inventory_File_Check = df_Validate_inventory.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Products File Check (SelectValues) [converted]
    # Select Values: Tag Products File Check
    df_Tag_Products_File_Check = df_Validate_products.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Stores File Check (SelectValues) [converted]
    # Select Values: Tag Stores File Check
    df_Tag_Stores_File_Check = df_Validate_stores.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Suppliers File Check (SelectValues) [converted]
    # Select Values: Tag Suppliers File Check
    df_Tag_Suppliers_File_Check = df_Validate_suppliers.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Inventory Batch Metadata (Constant) [converted]
    # Add Constants: Tag Inventory Batch Metadata
    df_Tag_Inventory_Batch_Metadata = df_Capture_Extract_Timestamp
    df_Tag_Inventory_Batch_Metadata = df_Tag_Inventory_Batch_Metadata.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Inventory_Batch_Metadata = df_Tag_Inventory_Batch_Metadata.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Inventory_Batch_Metadata = df_Tag_Inventory_Batch_Metadata.withColumn("etl_layer", lit('EXTRACT'))
    # preserved.etl_layer: length='-1', precision='-1'
    df_Tag_Inventory_Batch_Metadata = df_Tag_Inventory_Batch_Metadata.withColumn("source_entity", lit('inventory'))
    # preserved.source_entity: length='-1', precision='-1'
    df_Tag_Inventory_Batch_Metadata = df_Tag_Inventory_Batch_Metadata.withColumn("unit_of_measure", lit('EA'))
    # preserved.unit_of_measure: length='-1', precision='-1'

    # Step: Append File Checks A (Append) [converted]
    # Append Streams: Append File Checks A
    # preserved.head_name='Tag Inventory File Check'
    # preserved.tail_name='Tag Stores File Check'
    # preserved.stream_order=['Tag Inventory File Check', 'Tag Stores File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_A = df_Tag_Inventory_File_Check.unionByName(df_Tag_Stores_File_Check, allowMissingColumns=True)

    # Step: Prepare Inventory Join Keys (SelectValues) [converted]
    # Select Values: Prepare Inventory Join Keys
    df_Prepare_Inventory_Join_Keys = df_Tag_Inventory_Batch_Metadata.select(col("inventory_id").alias("inventory_id"), col("store_id").alias("store_id"), col("product_id").alias("inv_product_id"), col("quantity_on_hand").alias("quantity_on_hand"), col("quantity_reserved").alias("quantity_reserved"), col("reorder_level").alias("reorder_level"), col("reorder_quantity").alias("reorder_quantity"), col("last_stocktake_date").alias("last_stocktake_date"), col("bin_location").alias("bin_location"), col("is_low_stock").alias("is_low_stock"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("etl_layer").alias("etl_layer"), col("source_entity").alias("source_entity"), col("unit_of_measure").alias("unit_of_measure"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"))

    # Step: Append File Checks B (Append) [converted]
    # Append Streams: Append File Checks B
    # preserved.head_name='Append File Checks A'
    # preserved.tail_name='Tag Suppliers File Check'
    # preserved.stream_order=['Append File Checks A', 'Tag Suppliers File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_B = df_Append_File_Checks_A.unionByName(df_Tag_Suppliers_File_Check, allowMissingColumns=True)

    # Step: Sort Inventory By Product (SortRows) [converted]
    # Sort Rows: Sort Inventory By Product
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Inventory_By_Product = df_Prepare_Inventory_Join_Keys
    _sort_df_Sort_Inventory_By_Product = _sort_df_Sort_Inventory_By_Product.withColumn("_sort_ci_inv_product_id", lower(col("inv_product_id").cast("string")))
    df_Sort_Inventory_By_Product = _sort_df_Sort_Inventory_By_Product.orderBy(col("_sort_ci_inv_product_id").asc_nulls_last())
    df_Sort_Inventory_By_Product = df_Sort_Inventory_By_Product.drop("_sort_ci_inv_product_id")

    # Step: Append File Checks C (Append) [converted]
    # Append Streams: Append File Checks C
    # preserved.head_name='Append File Checks B'
    # preserved.tail_name='Tag Products File Check'
    # preserved.stream_order=['Append File Checks B', 'Tag Products File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_C = df_Append_File_Checks_B.unionByName(df_Tag_Products_File_Check, allowMissingColumns=True)

    # Step: Merge Join Inventory To Products (MergeJoin) [converted]
    # Merge Join: Merge Join Inventory To Products
    # preserved.join_type='LEFT OUTER'
    # preserved.join_keys=[{'left': 'inv_product_id', 'right': 'prd_product_id'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Merge Join Inventory To Products': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Merge_Join_Inventory_To_Products = df_Sort_Inventory_By_Product.join(broadcast(df_Sort_Products_By_Product), (df_Sort_Inventory_By_Product["inv_product_id"] == df_Sort_Products_By_Product["prd_product_id"]), how='left')
    # WARNING: MergeJoin 'Merge Join Inventory To Products': column lineage unavailable — cannot disambiguate join output columns
    df_Merge_Join_Inventory_To_Products = _joined_df_Merge_Join_Inventory_To_Products

    # Step: All Files Found? (FilterRows) [failed]
    # Filter Rows: All Files Found?
    df_Log_Files_Ready = df_Append_File_Checks_C.filter((col("file_exists") == lit('Y')))
    df_Abort_Missing_Source_File = df_Append_File_Checks_C.filter(~((col("file_exists") == lit('Y'))))
    df_All_Files_Found? = df_Log_Files_Ready

    # Step: Stream Lookup Stores (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Stores
    # StreamLookup 'Stream Lookup Stores': no join keys — lookup join not generated
    df_Stream_Lookup_Stores = df_Merge_Join_Inventory_To_Products

    # Step: Abort Missing Source File (Abort) [converted]
    # Abort: Abort Missing Source File
    # preserved.row_threshold=0
    # preserved.message='Required Inventory source file missing under ${DATASET_PATH}. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Missing_Source_File (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Missing Source File', df_Abort_Missing_Source_File.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Missing_Source_File = df_Abort_Missing_Source_File.count()
    if _abort_count_df_Abort_Missing_Source_File > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Required Inventory source file missing under ${DATASET_PATH}. RUN_ID=${RUN_ID}')

    # Step: Log Files Ready (WriteToLog) [failed]
    # Write to Log: Log Files Ready
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=FILES_OK | TRANS=TR_Inventory_Extract | FILES=inventory,stores,suppliers,products | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Files_Ready = logging.getLogger('pentaho.writetolog.Log_Files_Ready')
    _log_df_Log_Files_Ready.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Files_Ready = df_All_Files_Found?
    _log_rows_df_Log_Files_Ready = _log_df_df_Log_Files_Ready.take(20)
    _log_df_Log_Files_Ready.info('Log Files Ready' + ' | columns=' + str(_log_df_df_Log_Files_Ready.columns))
    _log_df_Log_Files_Ready.info('AUDIT | EVENT=FILES_OK | TRANS=TR_Inventory_Extract | FILES=inventory,stores,suppliers,products | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Files_Ready:
        _log_df_Log_Files_Ready.info('Log Files Ready' + ' | ' + str(_lr.asDict()))
    df_Log_Files_Ready = df_All_Files_Found?

    # Step: Stream Lookup Suppliers (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Suppliers
    # StreamLookup 'Stream Lookup Suppliers': no join keys — lookup join not generated
    df_Stream_Lookup_Suppliers = df_Stream_Lookup_Stores

    # Step: Optional Excel Inventory Overlay (ExcelInput) [converted]
    # Excel Input: Optional Excel Inventory Overlay
    df_Optional_Excel_Inventory_Overlay = (
        spark.read.format('com.crealytics.spark.excel')
        .option('sheetName', 'Sheet1')
        .option('header', 'true')
        .load('/inventory_attributes.xlsx')
    )

    # Step: Stream Lookup Regions (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Regions
    # StreamLookup 'Stream Lookup Regions': no join keys — lookup join not generated
    df_Stream_Lookup_Regions = df_Stream_Lookup_Suppliers

    # Step: Read Inventory Policy JSON (JsonInput) [converted]
    # JSON Input: Read Inventory Policy JSON
    df_Read_Inventory_Policy_JSON = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/inventory_validation_policy.json')

    # Step: Split Bin Location (FieldSplitter) [converted]
    # Split Fields: Split Bin Location
    df_Split_Bin_Location = df_Stream_Lookup_Regions
    df_Split_Bin_Location = df_Split_Bin_Location.withColumn("_parts_df_Split_Bin_Location", split(col("bin_location").cast("string"), '-'))
    # preserved.field 'warehouse_code' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_Bin_Location = df_Split_Bin_Location.withColumn("warehouse_code", trim(element_at(col("_parts_df_Split_Bin_Location"), 1)))
    # preserved.field 'bin_slot' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_Bin_Location = df_Split_Bin_Location.withColumn("bin_slot", trim(element_at(col("_parts_df_Split_Bin_Location"), 2)))
    df_Split_Bin_Location = df_Split_Bin_Location.drop("_parts_df_Split_Bin_Location", "bin_location")

    # Step: Read Inventory Validation Rules XML (getXMLData) [converted]
    # XML Input: Read Inventory Validation Rules XML
    df_Read_Inventory_Validation_Rules_XML = spark.read.format('xml').option('rowTag', 'row').load('${PROJECT_HOME}/metadata/rules/inventory_validation_rules.xml')

    # Step: Normalize Warehouse Code Case (StringOperations) [converted]
    # String Operations: Normalize Warehouse Code Case
    df_Normalize_Warehouse_Code_Case = df_Split_Bin_Location
    df_Normalize_Warehouse_Code_Case = df_Normalize_Warehouse_Code_Case.withColumn("warehouse_code", upper(trim(col("warehouse_code").cast("string"))))

    # Step: Inject Cleansing Metadata (MetaInject) [converted]
    # ETL Metadata Injection: Inject Cleansing Metadata
    # preserved.filename='${PROJECT_HOME}/transformations/cleansing/TR_Inventory_Cleansing.ktr'
    # preserved.no_execution=False
    # preserved.mappings=[{'source_field': 'dataset_path', 'target_step': '', 'target_attribute': '', 'target_detail': ''}]
    # LIMITATION: Runtime metadata injection into a child transformation is not available in Spark; mappings preserved as placeholders.
    _meta_inject_df_Inject_Cleansing_Metadata = {'target': '${PROJECT_HOME}/transformations/cleansing/TR_Inventory_Cleansing.ktr', 'mappings': [{'source_field': 'dataset_path', 'target_step': '', 'target_attribute': '', 'target_detail': ''}], 'parameters': [], 'no_execution': False}
    # TODO: apply _meta_inject_df_Inject_Cleansing_Metadata mappings before running child notebook/job
    df_Inject_Cleansing_Metadata = df_Read_Inventory_Validation_Rules_XML

    # Step: Build Batch Number (ConcatFields) [converted]
    # Concat Fields: Build Batch Number
    df_Build_Batch_Number = df_Normalize_Warehouse_Code_Case
    df_Build_Batch_Number = df_Build_Batch_Number.withColumn("batch_number", concat(concat(lit('"'), coalesce(col("inventory_id").cast("string"), lit("")), lit('"')), lit('-'), concat(lit('"'), coalesce(col("last_stocktake_date").cast("string"), lit("")), lit('"'))))
    # preserved.encoding='UTF-8'

    # Step: Derive Stock Boundaries (Formula) [converted]
    # Formula: Derive Stock Boundaries
    df_Derive_Stock_Boundaries = df_Build_Batch_Number
    df_Derive_Stock_Boundaries = df_Derive_Stock_Boundaries.withColumn('formula_result', lit(None))  # empty formula

    # Step: Calculate Available Quantity (Calculator) [converted]
    # Calculator: Calculate Available Quantity
    df_Calculate_Available_Quantity = df_Derive_Stock_Boundaries
    df_Calculate_Available_Quantity = df_Calculate_Available_Quantity.withColumn("available_qty", ((col("quantity_on_hand") - col("quantity_reserved"))).cast('decimal(38,2)'))

    # Step: Select Joined Inventory Columns (SelectValues) [converted]
    # Select Values: Select Joined Inventory Columns
    df_Select_Joined_Inventory_Columns = df_Calculate_Available_Quantity.select(col("inventory_id").alias("inventory_id"), col("store_id").alias("store_id"), col("product_id").alias("product_id"), col("quantity_on_hand").alias("quantity_on_hand"), col("quantity_reserved").alias("quantity_reserved"), col("reorder_level").alias("reorder_level"), col("reorder_quantity").alias("reorder_quantity"), col("last_stocktake_date").alias("last_stocktake_date"), col("bin_location").alias("bin_location"), col("is_low_stock").alias("is_low_stock"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("store_name").alias("store_name"), col("store_type").alias("store_type"), col("region_id").alias("region_id"), col("square_footage").alias("square_footage"), col("supplier_name").alias("supplier_name"), col("lead_time_days").alias("lead_time_days"), col("supplier_active").alias("supplier_active"), col("warehouse_code").alias("warehouse_code"), col("bin_slot").alias("bin_slot"), col("batch_number").alias("batch_number"), col("expiry_date").alias("expiry_date"), col("maximum_stock").alias("maximum_stock"), col("minimum_stock").alias("minimum_stock"), col("quantity").alias("quantity"), col("available_qty").alias("available_qty"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"))

    # Step: Count By Warehouse (GroupBy) [converted]
    # Group By: Count By Warehouse
    df_Count_By_Warehouse = df_Select_Joined_Inventory_Columns.groupBy('warehouse_code').agg(count(lit(1)).alias('warehouse_rows'))

    # Step: Count Extracted Inventory Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Extracted Inventory Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Extracted_Inventory_Rows = df_Select_Joined_Inventory_Columns.groupBy().agg(count(lit(1)).alias('rows_extracted'))

    # Step: Write Staging Joined Inventory (TextFileOutput) [converted]
    # Pentaho step: Write Staging Joined Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/staging/stg_joined_inventory_
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
    df_Write_Staging_Joined_Inventory = df_Select_Joined_Inventory_Columns
    _out_df_Write_Staging_Joined_Inventory = df_Write_Staging_Joined_Inventory.select('inventory_id', 'store_id', 'product_id', 'quantity_on_hand', 'quantity_reserved', 'reorder_level', 'reorder_quantity', 'last_stocktake_date', 'bin_location', 'is_low_stock', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'store_name', 'store_type', 'region_id', 'square_footage', 'supplier_name', 'lead_time_days', 'supplier_active', 'warehouse_code', 'bin_slot', 'batch_number', 'expiry_date', 'maximum_stock', 'minimum_stock', 'quantity', 'available_qty', 'batch_id', 'run_id', 'extract_ts', 'source_row_num')
    writer = _out_df_Write_Staging_Joined_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_joined_inventory_.csv')

    # Step: Add Extract Audit Fields (Constant) [converted]
    # Add Constants: Add Extract Audit Fields
    df_Add_Extract_Audit_Fields = df_Count_Extracted_Inventory_Rows
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("object_name", lit('inventory'))
    # preserved.object_name: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("layer", lit('EXTRACT'))
    # preserved.layer: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Staging Inventory Table (TableOutput) [converted]
    # Pentaho step: Write Staging Inventory Table (type: TableOutput) (Pentaho schema: retail_stg)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Staging_Inventory_Table = df_Write_Staging_Joined_Inventory.select(col('inventory_id'), col('product_id'), col('store_id'), col('supplier_id'), col('warehouse_code'), col('quantity'), col('available_qty'), col('reorder_level'), col('batch_id'), col('run_id'))
    df_Write_Staging_Inventory_Table = _mapped_df_Write_Staging_Inventory_Table
    write_delta(
        df_Write_Staging_Inventory_Table,
        f"{catalog}.{schema}.stg_joined_inventory",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.stg_joined_inventory", mode='append')

    # Step: Write Extraction Log (TextFileOutput) [converted]
    # Pentaho step: Write Extraction Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/inventory/TR_Inventory_Extract_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_extracted' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extraction_Log = df_Add_Extract_Audit_Fields
    _out_df_Write_Extraction_Log = df_Write_Extraction_Log.select('object_name', 'layer', 'rows_extracted', 'status', 'batch_id', 'run_id')
    writer = _out_df_Write_Extraction_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Inventory_Extract_.log')

    # Step: Block Until Extract Complete (BlockingStep) [converted]
    # Blocking Step: Block Until Extract Complete
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Extract_Complete = cache_for_reuse(df_Write_Staging_Inventory_Table)
    _ = df_Block_Until_Extract_Complete.count()  # synchronize: wait for all upstream rows

    # Step: Write Extraction Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Extraction Audit JSON (type: JsonOutput)
    df_Write_Extraction_Audit_JSON = df_Write_Extraction_Log
    df_Write_Extraction_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/inventory_extract_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Extract Complete (Dummy) [converted]
    # Dummy: Extract Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Extract_Complete = df_Inject_Cleansing_Metadata

    # Step: Copy Extract Metrics To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Extract Metrics To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Extract Metrics To Result'
    _result_rows_df_Copy_Extract_Metrics_To_Result = df_Write_Extraction_Audit_JSON
    _pentaho_result_rows['Copy Extract Metrics To Result'] = _result_rows_df_Copy_Extract_Metrics_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Extract_Metrics_To_Result
    df_Copy_Extract_Metrics_To_Result = df_Write_Extraction_Audit_JSON

    log_event(_LOG, "transformation_end")
    return df_Copy_Extract_Metrics_To_Result
