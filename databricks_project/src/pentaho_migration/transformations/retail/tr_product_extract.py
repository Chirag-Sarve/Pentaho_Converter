"""PySpark module migrated from Pentaho transformation: TR_Product_Extract.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Product_Extract.ktr
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
    trim,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_product_extract")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Product_Extract`` step-for-step."""
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
    data = [('PRODUCT', 'FULL')]
    df_Generate_Extract_Control_Row = spark.createDataFrame(data, ['extract_module', 'extract_mode'])

    # Step: Read categories.csv (TextFileInput) [converted]
    # Pentaho step: Read categories.csv (type: TextFileInput)
    # INFO: preserved Legacy Text File Input option: date_format_lenient='Y'
    # Pentaho filename: /categories.csv
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    df_Read_categories.csv = (
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
    df_Read_categories.csv = df_Read_categories.csv.select(col('category_id').alias('category_id'), col('category_name').alias('category_name'), col('parent_category_id').alias('parent_category_id'), col('description').alias('description'), col('is_active').alias('is_active'))
    df_Read_categories.csv = df_Read_categories.csv.filter(~((col('category_id').isNull() | (length(trim(col('category_id').cast('string'))) == 0)) & (col('category_name').isNull() | (length(trim(col('category_name').cast('string'))) == 0)) & (col('parent_category_id').isNull() | (length(trim(col('parent_category_id').cast('string'))) == 0)) & (col('description').isNull() | (length(trim(col('description').cast('string'))) == 0)) & (col('is_active').isNull() | (length(trim(col('is_active').cast('string'))) == 0))))
    df_Read_categories.csv = df_Read_categories.csv.withColumn('source_row_num', monotonically_increasing_id())

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
    # Pentaho filename: /rejects/rejected_rows/product/products_extract_rejects_
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
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_FIELDS' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extract_Rejects = df_Write_Extract_Rejects
    _out_df_Write_Extract_Rejects = df_Write_Extract_Rejects.select('product_id', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'created_date', 'description', 'ERR_CODE', 'ERR_DESC', 'ERR_FIELDS', 'batch_id', 'run_id')
    writer = _out_df_Write_Extract_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/products_extract_rejects_.csv')

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

    # Step: Write Staging Land Categories (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Categories (type: TextFileOutput)
    # Pentaho filename: /output/product/staging/stg_raw_categories_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='parent_category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='description' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Categories = df_Read_categories.csv
    _out_df_Write_Staging_Land_Categories = df_Write_Staging_Land_Categories.select('category_id', 'category_name', 'parent_category_id', 'description', 'is_active', 'source_row_num')
    writer = _out_df_Write_Staging_Land_Categories.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_categories_.csv')

    # Step: Sample Product Peek (SampleRows) [converted]
    # Sample Rows: Sample Product Peek
    _w_sr_df_Sample_Product_Peek = Window.orderBy(monotonically_increasing_id())
    df_Sample_Product_Peek = df_Read_products.csv.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Product_Peek))
    # preserved.lines_range='1..5' ranges=[(1, 5)]
    df_Sample_Product_Peek = df_Sample_Product_Peek.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 5)))
    df_Sample_Product_Peek = df_Sample_Product_Peek.drop('_sr_rn')

    # Step: Write Staging Land Suppliers (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Suppliers (type: TextFileOutput)
    # Pentaho filename: /output/product/staging/stg_raw_suppliers_
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
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Suppliers = df_Read_suppliers.csv
    _out_df_Write_Staging_Land_Suppliers = df_Write_Staging_Land_Suppliers.select('supplier_id', 'supplier_name', 'contact_name', 'email', 'phone', 'address_line1', 'city', 'postal_code', 'country_code', 'country_name', 'preferred_currency', 'lead_time_days', 'is_active', 'created_date', 'source_row_num')
    writer = _out_df_Write_Staging_Land_Suppliers.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_suppliers_.csv')

    # Step: Validate categories.csv (GetFileNames) [failed]
    # Get File Names: Validate categories.csv
    _list_path_df_Validate_categories.csv = '/'
    try:
        _fs_entries_df_Validate_categories.csv = dbutils.fs.ls(_list_path_df_Validate_categories.csv)
        df_Validate_categories.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_categories.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_categories.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_categories.csv = spark.createDataFrame(
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
    df_Capture_Extract_Timestamp = df_Sample_Product_Peek
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_ts", current_date())
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_start", current_date())

    # Step: Tag Categories File Check (SelectValues) [converted]
    # Select Values: Tag Categories File Check
    df_Tag_Categories_File_Check = df_Validate_categories.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Products File Check (SelectValues) [converted]
    # Select Values: Tag Products File Check
    df_Tag_Products_File_Check = df_Validate_products.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Suppliers File Check (SelectValues) [converted]
    # Select Values: Tag Suppliers File Check
    df_Tag_Suppliers_File_Check = df_Validate_suppliers.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Product Batch Metadata (Constant) [converted]
    # Add Constants: Tag Product Batch Metadata
    df_Tag_Product_Batch_Metadata = df_Capture_Extract_Timestamp
    df_Tag_Product_Batch_Metadata = df_Tag_Product_Batch_Metadata.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Product_Batch_Metadata = df_Tag_Product_Batch_Metadata.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Product_Batch_Metadata = df_Tag_Product_Batch_Metadata.withColumn("etl_layer", lit('EXTRACT'))
    # preserved.etl_layer: length='-1', precision='-1'
    df_Tag_Product_Batch_Metadata = df_Tag_Product_Batch_Metadata.withColumn("source_entity", lit('products'))
    # preserved.source_entity: length='-1', precision='-1'

    # Step: Append File Checks A (Append) [converted]
    # Append Streams: Append File Checks A
    # preserved.head_name='Tag Products File Check'
    # preserved.tail_name='Tag Categories File Check'
    # preserved.stream_order=['Tag Products File Check', 'Tag Categories File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_A = df_Tag_Products_File_Check.unionByName(df_Tag_Categories_File_Check, allowMissingColumns=True)

    # Step: Derive Extract Technical Fields (Formula) [converted]
    # Formula: Derive Extract Technical Fields
    df_Derive_Extract_Technical_Fields = df_Tag_Product_Batch_Metadata
    df_Derive_Extract_Technical_Fields = df_Derive_Extract_Technical_Fields.withColumn('formula_result', lit(None))  # empty formula

    # Step: Append File Checks B (Append) [converted]
    # Append Streams: Append File Checks B
    # preserved.head_name='Append File Checks A'
    # preserved.tail_name='Tag Suppliers File Check'
    # preserved.stream_order=['Append File Checks A', 'Tag Suppliers File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_B = df_Append_File_Checks_A.unionByName(df_Tag_Suppliers_File_Check, allowMissingColumns=True)

    # Step: Select Product Extract Columns (SelectValues) [converted]
    # Select Values: Select Product Extract Columns
    df_Select_Product_Extract_Columns = df_Derive_Extract_Technical_Fields.select(col("product_id").alias("product_id"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("created_date").alias("created_date"), col("description").alias("description"), col("upc").alias("upc"), col("barcode").alias("barcode"), col("length_cm").alias("length_cm"), col("width_cm").alias("width_cm"), col("height_cm").alias("height_cm"), col("status").alias("status"), col("source_row_num").alias("source_row_num"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("etl_layer").alias("etl_layer"), col("extract_ts").alias("extract_ts"))

    # Step: All Files Found? (FilterRows) [failed]
    # Filter Rows: All Files Found?
    df_Log_Files_Ready = df_Append_File_Checks_B.filter((col("file_exists") == lit('Y')))
    df_Abort_Missing_Source_File = df_Append_File_Checks_B.filter(~((col("file_exists") == lit('Y'))))
    df_All_Files_Found? = df_Log_Files_Ready

    # Step: Count Extracted Products (MemoryGroupBy) [converted]
    # Memory Group By: Count Extracted Products
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Extracted_Products = df_Select_Product_Extract_Columns.groupBy().agg(count(lit(1)).alias('rows_extracted'))

    # Step: Write Staging Land Products (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Products (type: TextFileOutput)
    # Pentaho filename: /output/product/staging/stg_raw_products_
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
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='etl_layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Products = df_Select_Product_Extract_Columns
    _out_df_Write_Staging_Land_Products = df_Write_Staging_Land_Products.select('product_id', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'created_date', 'description', 'upc', 'barcode', 'length_cm', 'width_cm', 'height_cm', 'status', 'source_row_num', 'batch_id', 'run_id', 'etl_layer', 'extract_ts')
    writer = _out_df_Write_Staging_Land_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_products_.csv')

    # Step: Abort Missing Source File (Abort) [converted]
    # Abort: Abort Missing Source File
    # preserved.row_threshold=0
    # preserved.message='Required Product source file missing under ${DATASET_PATH}. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Missing_Source_File (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Missing Source File', df_Abort_Missing_Source_File.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Missing_Source_File = df_Abort_Missing_Source_File.count()
    if _abort_count_df_Abort_Missing_Source_File > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Required Product source file missing under ${DATASET_PATH}. RUN_ID=${RUN_ID}')

    # Step: Log Files Ready (WriteToLog) [failed]
    # Write to Log: Log Files Ready
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=FILES_OK | TRANS=TR_Product_Extract | FILES=products,categories,suppliers | RUN_ID=${RUN_ID}'
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
    _log_df_Log_Files_Ready.info('AUDIT | EVENT=FILES_OK | TRANS=TR_Product_Extract | FILES=products,categories,suppliers | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Files_Ready:
        _log_df_Log_Files_Ready.info('Log Files Ready' + ' | ' + str(_lr.asDict()))
    df_Log_Files_Ready = df_All_Files_Found?

    # Step: Add Extract Audit Fields (Constant) [converted]
    # Add Constants: Add Extract Audit Fields
    df_Add_Extract_Audit_Fields = df_Count_Extracted_Products
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("object_name", lit('products'))
    # preserved.object_name: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("layer", lit('EXTRACT'))
    # preserved.layer: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Staging Product Table (TableOutput) [converted]
    # Pentaho step: Write Staging Product Table (type: TableOutput) (Pentaho schema: retail_stg)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Staging_Product_Table = df_Write_Staging_Land_Products.select(col('product_id'), col('sku'), col('product_name'), col('category_id'), col('supplier_id'), col('brand'), col('unit_cost'), col('unit_price'), col('currency_code'), col('weight_kg'), col('is_active'), col('created_date'), col('description'), col('upc'), col('barcode'), col('status'), col('batch_id'), col('run_id'), col('etl_layer'))
    df_Write_Staging_Product_Table = _mapped_df_Write_Staging_Product_Table
    write_delta(
        df_Write_Staging_Product_Table,
        f"{catalog}.{schema}.stg_raw_products",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.stg_raw_products", mode='append')

    # Step: Write Extraction Log (TextFileOutput) [converted]
    # Pentaho step: Write Extraction Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/product/TR_Product_Extract_
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
    writer.mode('overwrite').save(f'{data_dir}/TR_Product_Extract_.log')

    # Step: Block Until Extract Complete (BlockingStep) [converted]
    # Blocking Step: Block Until Extract Complete
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Extract_Complete = cache_for_reuse(df_Write_Staging_Product_Table)
    _ = df_Block_Until_Extract_Complete.count()  # synchronize: wait for all upstream rows

    # Step: Write Extraction Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Extraction Audit JSON (type: JsonOutput)
    df_Write_Extraction_Audit_JSON = df_Write_Extraction_Log
    df_Write_Extraction_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/product_extract_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Extract Complete (Dummy) [converted]
    # Dummy: Extract Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Extract_Complete = df_Block_Until_Extract_Complete

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
