"""PySpark module migrated from Pentaho transformation: TextFile_Write_Read.

Source: TextFile_Write_Read.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
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

_LOG = get_logger("pentaho_migration.transformations.text_file_write_read")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TextFile_Write_Read`` step-for-step.

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Runtime configuration. Recognized keys include ``catalog``, ``schema``,
        ``data_dir``, transformation parameters, and path overrides.

    Returns
    -------
    DataFrame
        Downstream DataFrame after the final hop(s).
    """
    config = dict(config or {})
    apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "transformation_start")
    catalog = config.get("catalog", "main")
    schema = config.get("schema", 'default')
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")


    # Step: Generate (DataGrid) [converted]
    # Generate Rows: Generate
    data = [
        (1, 'alice'),
        (2, 'bob'),
    ]
    df_Generate = spark.createDataFrame(data, ['id', 'name'])

    # Step: WriteCSV (TextFileOutput) [converted]
    # Pentaho step: WriteCSV (type: TextFileOutput)
    # Pentaho filename: C:\pentaho\data\runtime_orders
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    df_WriteCSV = df_Generate
    _out_df_WriteCSV = df_WriteCSV.select('id', 'name')
    writer = _out_df_WriteCSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/runtime_orders.csv')

    # Step: ReadCSV (TextFileInput) [converted]
    # Pentaho step: ReadCSV (type: TextFileInput)
    # Pentaho filename: ${Internal.Transformation.Filename.Directory}/runtime_orders.csv
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    df_ReadCSV = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('id INT, name STRING')
        .csv(f'{data_dir}/runtime_orders.csv')
    )
    df_ReadCSV = df_ReadCSV.select(col('id').cast('int').alias('id'), col('name').alias('name'))

    log_event(_LOG, "transformation_end")
    return df_ReadCSV
