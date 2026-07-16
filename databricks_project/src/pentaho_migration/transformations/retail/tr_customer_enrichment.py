"""PySpark module migrated from Pentaho transformation: TR_Customer_Enrichment.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/TR_Customer_Enrichment.ktr
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
    lower,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_customer_enrichment")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Customer_Enrichment`` step-for-step."""
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

    # Step: Read Cleansed Customers (CsvInput) [converted]
    # CSV Input: Read Cleansed Customers
    df_Read_Cleansed_Customers = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, address_line1 STRING, address_line2 STRING, city STRING, state_province STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, loyalty_tier STRING, registration_date STRING, date_of_birth STRING, is_active STRING, source_row_num INT, batch_id STRING, run_id STRING, dup_count INT')
        .load(f'{data_dir}/customers_cleansed_.csv')
    )

    # Step: Read Regions Lookup (CsvInput) [converted]
    # CSV Input: Read Regions Lookup
    df_Read_Regions_Lookup = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('region_id STRING, region_name STRING, country_code STRING, country_name STRING, continent STRING, currency_code STRING, timezone STRING, is_active STRING')
        .load('/regions.csv')
    )

    # Step: Read Stores Lookup (CsvInput) [converted]
    # CSV Input: Read Stores Lookup
    df_Read_Stores_Lookup = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('store_id STRING, store_name STRING, store_type STRING, region_id STRING, address_line1 STRING, city STRING, postal_code STRING, country_code STRING, phone STRING, manager_name STRING, square_footage STRING, open_date STRING, is_active STRING')
        .load('/stores.csv')
    )

    # Step: Write Enrichment Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Enrichment Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/customer/customers_enrich_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Enrichment_Rejects = df_Write_Enrichment_Rejects
    _out_df_Write_Enrichment_Rejects = df_Write_Enrichment_Rejects.select('customer_id', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Enrichment_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_enrich_rejects_.csv')

    # Step: Select Region Fields (SelectValues) [converted]
    # Select Values: Select Region Fields
    df_Select_Region_Fields = df_Read_Regions_Lookup.select(col("region_id").alias("lkp_region_id"), col("region_name").alias("lkp_region_name"), col("country_code").alias("lkp_region_country"), col("continent").alias("lkp_continent"), col("currency_code").alias("lkp_region_currency"))

    # Step: Select Store Fields (SelectValues) [converted]
    # Select Values: Select Store Fields
    df_Select_Store_Fields = df_Read_Stores_Lookup.select(col("store_id").alias("lkp_store_id"), col("store_name").alias("lkp_store_name"), col("country_code").alias("lkp_store_country"), col("region_id").alias("lkp_store_region"), col("city").alias("lkp_store_city"))

    # Step: Region Lookup (StreamLookup) [failed]
    # Stream Lookup: Region Lookup
    # StreamLookup 'Region Lookup': no join keys — lookup join not generated
    df_Region_Lookup = df_Read_Cleansed_Customers

    # Step: Store Lookup (StreamLookup) [failed]
    # Stream Lookup: Store Lookup
    # StreamLookup 'Store Lookup': no join keys — lookup join not generated
    df_Store_Lookup = df_Region_Lookup

    # Step: Capture Business Date (SystemInfo) [converted]
    # System Info: Capture Business Date
    df_Capture_Business_Date = df_Store_Lookup
    df_Capture_Business_Date = df_Capture_Business_Date.withColumn("sys_today", lit(''))
    df_Capture_Business_Date = df_Capture_Business_Date.withColumn("sys_now", current_date())

    # Step: Calculate Derived Fields (Formula) [converted]
    # Formula: Calculate Derived Fields
    df_Calculate_Derived_Fields = df_Capture_Business_Date
    df_Calculate_Derived_Fields = df_Calculate_Derived_Fields.withColumn('formula_result', lit(None))  # empty formula

    # Step: Calculate Age Group Segment Risk (Formula) [converted]
    # Formula: Calculate Age Group Segment Risk
    df_Calculate_Age_Group_Segment_Risk = df_Calculate_Derived_Fields
    df_Calculate_Age_Group_Segment_Risk = df_Calculate_Age_Group_Segment_Risk.withColumn('formula_result', lit(None))  # empty formula

    # Step: Map Loyalty Tier Labels (ValueMapper) [converted]
    # Value Mapper: Map Loyalty Tier Labels
    df_Map_Loyalty_Tier_Labels = df_Calculate_Age_Group_Segment_Risk.withColumn("loyalty_tier_enriched", when((lower(col("loyalty_tier")) == lower(lit('Bronze'))), lit('BRONZE')).when((lower(col("loyalty_tier")) == lower(lit('Silver'))), lit('SILVER')).when((lower(col("loyalty_tier")) == lower(lit('Gold'))), lit('GOLD')).when((lower(col("loyalty_tier")) == lower(lit('Platinum'))), lit('PLATINUM')).when((col("loyalty_tier").isNull() | (col("loyalty_tier") == lit(''))), col("loyalty_tier")).otherwise(lit('BRONZE')))
    # preserved.case_sensitive=False mappings=4 default='BRONZE'

    # Step: Route By Risk Category (SwitchCase) [converted]
    # Switch / Case: Route By Risk Category
    # preserved.fieldname='risk_category'
    # preserved.switch_field='risk_category'
    # preserved.cases=[{'value': 'HIGH', 'target_step': 'Tag High Risk'}, {'value': 'MEDIUM', 'target_step': 'Tag Medium Risk'}, {'value': 'LOW', 'target_step': 'Tag Low Risk'}]
    # preserved.default_target_step='Tag Low Risk'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'HIGH', 'target_step': 'Tag High Risk'}, {'value': 'MEDIUM', 'target_step': 'Tag Medium Risk'}, {'value': 'LOW', 'target_step': 'Tag Low Risk'}]
    _routed_df_Route_By_Risk_Category = df_Map_Loyalty_Tier_Labels.withColumn('_route_Route_By_Risk_Category', when(col("risk_category") == lit('HIGH'), lit('Tag High Risk')).when(col("risk_category") == lit('MEDIUM'), lit('Tag Medium Risk')).when(col("risk_category") == lit('LOW'), lit('Tag Low Risk')).otherwise(lit('Tag Low Risk')))
    df_Tag_High_Risk = _routed_df_Route_By_Risk_Category.filter(col('_route_Route_By_Risk_Category') == lit('Tag High Risk')).drop('_route_Route_By_Risk_Category')
    df_Tag_Medium_Risk = _routed_df_Route_By_Risk_Category.filter(col('_route_Route_By_Risk_Category') == lit('Tag Medium Risk')).drop('_route_Route_By_Risk_Category')
    df_Tag_Low_Risk = _routed_df_Route_By_Risk_Category.filter(col('_route_Route_By_Risk_Category') == lit('Tag Low Risk')).drop('_route_Route_By_Risk_Category')
    df_Route_By_Risk_Category = df_Tag_High_Risk

    # Step: Tag High Risk (Constant) [converted]
    # Add Constants: Tag High Risk
    df_Tag_High_Risk = df_Route_By_Risk_Category
    df_Tag_High_Risk = df_Tag_High_Risk.withColumn("risk_route", lit('HIGH'))
    # preserved.risk_route: length='-1', precision='-1'

    # Step: Tag Low Risk (Constant) [converted]
    # Add Constants: Tag Low Risk
    df_Tag_Low_Risk = df_Route_By_Risk_Category
    df_Tag_Low_Risk = df_Tag_Low_Risk.withColumn("risk_route", lit('LOW'))
    # preserved.risk_route: length='-1', precision='-1'

    # Step: Tag Medium Risk (Constant) [converted]
    # Add Constants: Tag Medium Risk
    df_Tag_Medium_Risk = df_Route_By_Risk_Category
    df_Tag_Medium_Risk = df_Tag_Medium_Risk.withColumn("risk_route", lit('MEDIUM'))
    # preserved.risk_route: length='-1', precision='-1'

    # Step: Merge Risk Routes (Dummy) [converted]
    # Dummy: Merge Risk Routes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Merge_Risk_Routes = df_Tag_High_Risk

    # Step: Select Enriched Columns (SelectValues) [converted]
    # Select Values: Select Enriched Columns
    df_Select_Enriched_Columns = df_Dummy_Merge_Risk_Routes.select(col("customer_id").alias("customer_id"), col("first_name").alias("first_name"), col("last_name").alias("last_name"), col("email").alias("email"), col("phone").alias("phone"), col("address_line1").alias("address_line1"), col("address_line2").alias("address_line2"), col("city").alias("city"), col("state_province").alias("state_province"), col("postal_code").alias("postal_code"), col("country_code").alias("country_code"), col("country_name").alias("country_name"), col("preferred_currency").alias("preferred_currency"), col("loyalty_tier").alias("loyalty_tier"), col("registration_date").alias("registration_date"), col("date_of_birth").alias("date_of_birth"), col("is_active").alias("is_active"), col("full_name").alias("full_name"), col("customer_age").alias("customer_age"), col("age_group").alias("age_group"), col("loyalty_tier_enriched").alias("loyalty_tier_code"), col("risk_category").alias("risk_category"), col("customer_segment").alias("customer_segment"), col("years_as_customer").alias("years_as_customer"), col("preferred_language").alias("preferred_language"), col("preferred_currency_enriched").alias("preferred_currency_final"), col("region_id").alias("region_id"), col("region_name").alias("region_name"), col("continent").alias("continent"), col("store_id").alias("store_id"), col("store_name").alias("store_name"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"))

    # Step: Write Enriched Customers (TextFileOutput) [converted]
    # Pentaho step: Write Enriched Customers (type: TextFileOutput)
    # Pentaho filename: /output/customer/enriched/customers_enriched_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='first_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='last_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='full_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='email' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='phone' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line1' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line2' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='city' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='state_province' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='postal_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='preferred_currency_final' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='loyalty_tier' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='loyalty_tier_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='registration_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_of_birth' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_age' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='age_group' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='risk_category' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_segment' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='years_as_customer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='preferred_language' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='continent' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Enriched_Customers = df_Select_Enriched_Columns
    _out_df_Write_Enriched_Customers = df_Write_Enriched_Customers.select('customer_id', 'first_name', 'last_name', 'full_name', 'email', 'phone', 'address_line1', 'address_line2', 'city', 'state_province', 'postal_code', 'country_code', 'country_name', 'preferred_currency_final', 'loyalty_tier', 'loyalty_tier_code', 'registration_date', 'date_of_birth', 'is_active', 'customer_age', 'age_group', 'risk_category', 'customer_segment', 'years_as_customer', 'preferred_language', 'region_id', 'region_name', 'continent', 'store_id', 'store_name', 'batch_id', 'run_id')
    writer = _out_df_Write_Enriched_Customers.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_enriched_.csv')

    # Step: Log Enrichment (WriteToLog) [converted]
    # Write to Log: Log Enrichment
    # preserved.log_level='Basic'
    # preserved.log_message='ENRICH | TRANS=TR_Customer_Enrichment | customer_id=${customer_id} | segment=${customer_segment} | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Enrichment = logging.getLogger('pentaho.writetolog.Log_Enrichment')
    _log_df_Log_Enrichment.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Enrichment = df_Write_Enriched_Customers
    _log_rows_df_Log_Enrichment = _log_df_df_Log_Enrichment.take(20)
    _log_df_Log_Enrichment.info('Log Enrichment' + ' | columns=' + str(_log_df_df_Log_Enrichment.columns))
    _log_df_Log_Enrichment.info('ENRICH | TRANS=TR_Customer_Enrichment | customer_id=${customer_id} | segment=${customer_segment} | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Enrichment:
        _log_df_Log_Enrichment.info('Log Enrichment' + ' | ' + str(_lr.asDict()))
    df_Log_Enrichment = df_Write_Enriched_Customers

    # Step: Write Enrichment Sample JSON (JsonOutput) [converted]
    # Pentaho step: Write Enrichment Sample JSON (type: JsonOutput)
    df_Write_Enrichment_Sample_JSON = df_Write_Enriched_Customers
    df_Write_Enrichment_Sample_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/customers_enriched_sample_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Enrichment Complete (Dummy) [converted]
    # Dummy: Enrichment Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Enrichment_Complete = df_Write_Enrichment_Sample_JSON

    log_event(_LOG, "transformation_end")
    return df_Dummy_Enrichment_Complete
