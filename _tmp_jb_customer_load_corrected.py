"""Databricks job for jb_customer_load."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Mapping

def _project_root() -> Path:
    """Resolve package root for local scripts and Databricks notebooks."""
    try:
        root = Path(__file__).resolve()
        for _ in range(2):
            root = root.parent
        return root
    except NameError:
        pass
    # Databricks notebook: /Workspace/.../Master_ETL or .../jobs/JobName
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        _spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        _nb = (
            DBUtils(_spark)
            .notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        if _nb:
            root = Path(str(_nb))
            for _ in range(2):
                root = root.parent
            return root
    except Exception:
        pass
    return Path.cwd()


_ROOT = _project_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, when, expr, count, coalesce, broadcast
from delta.tables import DeltaTable
from pyspark.sql.functions import upper, lower, trim, ltrim, rtrim, initcap, length
from pyspark.sql.functions import substring, round, abs, sqrt, ceil, floor, pow
from pyspark.sql.functions import concat, concat_ws, isnull, regexp_replace, regexp_extract, explode, explode_outer, array
from pyspark.sql.functions import split, element_at, collect_list, from_csv
from pyspark.sql.functions import md5, sha1, sha2, crc32, hex, unhex, soundex, lag, lead, rand, randn
from pyspark.sql.functions import lpad, rpad, greatest, conv, dayofyear, quarter, hour, minute, second
from pyspark.sql.functions import to_date, to_timestamp, datediff, date_add, add_months, date_format
from pyspark.sql.functions import unix_timestamp, from_unixtime, current_date, current_timestamp
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, repeat
from pyspark.sql.functions import row_number, rank, dense_rank, monotonically_increasing_id
from pyspark.sql.functions import countDistinct, first, last, levenshtein, sum as _sum, avg, max as _max, min as _min
from pyspark.sql.functions import stddev_samp, var_samp as variance_samp, to_json, struct

import config
from engine.runtime import execute_registered_job
from engine.df_guards import log_step_dataframe, require_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger('jb_customer_load')

TARGET_CATALOG = config.TARGET_CATALOG
TARGET_SCHEMA = config.TARGET_SCHEMA
PENTAHO_DATA_DIR = config.PENTAHO_DATA_DIR

# Step 1 : Read Customers
def step_01_Read_Customers(spark):
    logging.info("Running TextFileInput: Read Customers")
    read_customers_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('customer_id INT, customer_name STRING, status STRING, region STRING')
        .csv(f'{PENTAHO_DATA_DIR}/customers.csv')
    )
    read_customers_df = read_customers_df.select(col('customer_id').cast('int').alias('customer_id'), col('customer_name').alias('customer_name'), col('status').alias('status'), col('region').alias('region'))
    read_customers_df = require_dataframe(read_customers_df, transformation='tr_customer_filter', step_name='Read Customers', func_name='step_01_Read_Customers', required_columns=[])
    log_step_dataframe(read_customers_df, step_name='Read Customers', phase='after', transformation='tr_customer_filter', func_name='step_01_Read_Customers')
    return read_customers_df

# Step 2 : Keep Active
def step_02_Keep_Active(spark, read_customers_df):
    logging.info("Running FilterRows: Keep Active")
    read_customers_df = require_dataframe(read_customers_df, transformation='tr_customer_filter', step_name='Keep Active', func_name='step_02_Keep_Active', required_columns=['customer_id', 'customer_name', 'region', 'status'])
    log_step_dataframe(read_customers_df, step_name='Keep Active', phase='before', transformation='tr_customer_filter', func_name='step_02_Keep_Active')
    _filter_required = ['status']
    _filter_missing = [c for c in _filter_required if c not in read_customers_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Keep Active step "f"(missing={_filter_missing}, available={list(read_customers_df.columns)})")
    keep_active_df = read_customers_df.filter((col("status") == lit('ACTIVE')))
    keep_active_df = require_dataframe(keep_active_df, transformation='tr_customer_filter', step_name='Keep Active', func_name='step_02_Keep_Active', required_columns=[])
    log_step_dataframe(keep_active_df, step_name='Keep Active', phase='after', transformation='tr_customer_filter', func_name='step_02_Keep_Active')
    return keep_active_df

# Step 3 : Select Fields
def step_03_Select_Fields(spark, keep_active_df):
    logging.info("Running SelectValues: Select Fields")
    keep_active_df = require_dataframe(keep_active_df, transformation='tr_customer_filter', step_name='Select Fields', func_name='step_03_Select_Fields', required_columns=['customer_id', 'customer_name', 'region', 'status'])
    log_step_dataframe(keep_active_df, step_name='Select Fields', phase='before', transformation='tr_customer_filter', func_name='step_03_Select_Fields')
    _sv_required = ['customer_id', 'customer_name', 'region']
    _sv_missing = [c for c in _sv_required if c not in keep_active_df.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Fields step "f"(missing={_sv_missing}, available={list(keep_active_df.columns)})")
    select_fields_df = keep_active_df.select(col("customer_id"), col("customer_name"), col("region"))
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_customer_filter', step_name='Select Fields', func_name='step_03_Select_Fields', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Select Fields', phase='after', transformation='tr_customer_filter', func_name='step_03_Select_Fields')
    return select_fields_df

# Step 4 : Write Active
def step_04_Write_Active(spark, select_fields_df):
    logging.info("Running TextFileOutput: Write Active")
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_customer_filter', step_name='Write Active', func_name='step_04_Write_Active', required_columns=['customer_id', 'customer_name', 'region'])
    log_step_dataframe(select_fields_df, step_name='Write Active', phase='before', transformation='tr_customer_filter', func_name='step_04_Write_Active')
    write_active_df = select_fields_df
    _tfo_declared = ['customer_id', 'customer_name', 'region']
    _tfo_missing = [c for c in _tfo_declared if c not in write_active_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Active step "f"(missing={_tfo_missing}, available={list(write_active_df.columns)})")
    selected_output_df = write_active_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/customers_active_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/customers_active_out.csv'
    logging.info('Text File Output written to %s', _tfo_out_path)
    try:
        from pyspark.dbutils import DBUtils as _TfoDBUtils
        from pyspark.sql import SparkSession as _TfoSpark
        _tfo_spark = _TfoSpark.getActiveSession()
        if _tfo_spark is not None:
            _tfo_dbu = _TfoDBUtils(_tfo_spark)
            _tfo_listing = _tfo_dbu.fs.ls(_tfo_out_path)
            logging.info('Text File Output listing (%s entries): %s', len(_tfo_listing), [x.path for x in _tfo_listing[:5]])
    except Exception as _tfo_list_exc:
        logging.warning('Text File Output post-write check: %s', _tfo_list_exc)
    write_active_df = require_dataframe(write_active_df, transformation='tr_customer_filter', step_name='Write Active', func_name='step_04_Write_Active', required_columns=[])
    log_step_dataframe(write_active_df, step_name='Write Active', phase='after', transformation='tr_customer_filter', func_name='step_04_Write_Active')
    return write_active_df

def run_tr_customer_filter(spark, config=None):
    """Run tr_customer_filter."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_customer_filter (tr_customer_filter.ktr)")

    read_customers_df = step_01_Read_Customers(spark)
    read_customers_df = require_dataframe(read_customers_df, transformation='tr_customer_filter', step_name='Read Customers', func_name='step_01_Read_Customers', required_columns=[])
    read_customers_df = require_dataframe(read_customers_df, transformation='tr_customer_filter', step_name='Keep Active', func_name='step_02_Keep_Active', required_columns=['customer_id', 'customer_name', 'region', 'status'])
    keep_active_df = step_02_Keep_Active(spark, read_customers_df)
    keep_active_df = require_dataframe(keep_active_df, transformation='tr_customer_filter', step_name='Keep Active', func_name='step_02_Keep_Active', required_columns=[])
    keep_active_df = require_dataframe(keep_active_df, transformation='tr_customer_filter', step_name='Select Fields', func_name='step_03_Select_Fields', required_columns=['customer_id', 'customer_name', 'region', 'status'])
    select_fields_df = step_03_Select_Fields(spark, keep_active_df)
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_customer_filter', step_name='Select Fields', func_name='step_03_Select_Fields', required_columns=[])
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_customer_filter', step_name='Write Active', func_name='step_04_Write_Active', required_columns=['customer_id', 'customer_name', 'region'])
    write_active_df = step_04_Write_Active(spark, select_fields_df)
    write_active_df = require_dataframe(write_active_df, transformation='tr_customer_filter', step_name='Write Active', func_name='step_04_Write_Active', required_columns=[])

    logging.info("Finished transformation: tr_customer_filter")
    return write_active_df



def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Run the 'jb_customer_load' transformation flow."""
    return execute_registered_job(
        'jb_customer_load',
        spark=spark,
        config_overrides=config,
        trans_runners={
            'Run Customer Filter': run_tr_customer_filter
        },
    )


if __name__ == "__main__":
    _spark = SparkSession.builder.appName('jb_customer_load').getOrCreate()
    run(_spark, None)
