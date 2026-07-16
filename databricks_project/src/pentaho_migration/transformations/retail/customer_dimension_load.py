"""PySpark module migrated from Pentaho transformation: TR_Customer_Dimension_Load.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/dimension/Customer_Dimension_Load.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.customer_dimension_load")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Customer_Dimension_Load`` step-for-step."""
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

    # Step: Read Enriched Customers (CsvInput) [converted]
    # CSV Input: Read Enriched Customers
    df_Read_Enriched_Customers = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, address_line1 STRING, address_line2 STRING, city STRING, state_province STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, loyalty_tier STRING, registration_date STRING, date_of_birth STRING, is_active STRING, full_name STRING, customer_age INT, age_group STRING, loyalty_tier_code STRING, risk_category STRING, customer_segment STRING, years_as_customer INT, preferred_language STRING, preferred_currency_final STRING, region_id STRING, region_name STRING, continent STRING, store_id STRING, store_name STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/customers_enriched_.csv')
    )

    # Step: Read Existing DimCustomer (CsvInput) [converted]
    # CSV Input: Read Existing DimCustomer
    df_Read_Existing_DimCustomer = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_sk INT, customer_id STRING, email STRING, loyalty_tier STRING, address_line1 STRING, city STRING, country_code STRING, preferred_currency STRING, is_active STRING, is_current STRING, scd_hash STRING')
        .load(f'{data_dir}/dim_customer_current.csv')
    )

    # Step: Write Dimension Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Dimension Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/customer/customers_dim_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='scd_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Dimension_Rejects = df_Write_Dimension_Rejects
    _out_df_Write_Dimension_Rejects = df_Write_Dimension_Rejects.select('customer_id', 'ERR_CODE', 'ERR_DESC', 'scd_action', 'batch_id')
    writer = _out_df_Write_Dimension_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_dim_rejects_.csv')

    # Step: Build SCD Hash (Formula) [converted]
    # Formula: Build SCD Hash
    df_Build_SCD_Hash = df_Read_Enriched_Customers
    df_Build_SCD_Hash = df_Build_SCD_Hash.withColumn('formula_result', lit(None))  # empty formula

    # Step: Prepare Existing Keys (SelectValues) [converted]
    # Select Values: Prepare Existing Keys
    df_Prepare_Existing_Keys = df_Read_Existing_DimCustomer.select(col("customer_sk").alias("existing_customer_sk"), col("customer_id").alias("existing_customer_id"), col("scd_hash").alias("existing_scd_hash"), col("is_current").alias("existing_is_current"))

    # Step: Detect Existing Customers (StreamLookup) [failed]
    # Stream Lookup: Detect Existing Customers
    # StreamLookup 'Detect Existing Customers': no join keys — lookup join not generated
    df_Detect_Existing_Customers = df_Build_SCD_Hash

    # Step: Classify SCD Action (Formula) [converted]
    # Formula: Classify SCD Action
    df_Classify_SCD_Action = df_Detect_Existing_Customers
    df_Classify_SCD_Action = df_Classify_SCD_Action.withColumn('formula_result', lit(None))  # empty formula

    # Step: Route SCD Action (SwitchCase) [converted]
    # Switch / Case: Route SCD Action
    # preserved.fieldname='scd_action'
    # preserved.switch_field='scd_action'
    # preserved.cases=[{'value': 'INSERT', 'target_step': 'Prepare Insert'}, {'value': 'UPDATE', 'target_step': 'Prepare Update History'}, {'value': 'UNCHANGED', 'target_step': 'Skip Unchanged'}]
    # preserved.default_target_step='Skip Unchanged'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'INSERT', 'target_step': 'Prepare Insert'}, {'value': 'UPDATE', 'target_step': 'Prepare Update History'}, {'value': 'UNCHANGED', 'target_step': 'Skip Unchanged'}]
    _routed_df_Route_SCD_Action = df_Classify_SCD_Action.withColumn('_route_Route_SCD_Action', when(col("scd_action") == lit('INSERT'), lit('Prepare Insert')).when(col("scd_action") == lit('UPDATE'), lit('Prepare Update History')).when(col("scd_action") == lit('UNCHANGED'), lit('Skip Unchanged')).otherwise(lit('Skip Unchanged')))
    df_Prepare_Insert = _routed_df_Route_SCD_Action.filter(col('_route_Route_SCD_Action') == lit('Prepare Insert')).drop('_route_Route_SCD_Action')
    df_Prepare_Update_History = _routed_df_Route_SCD_Action.filter(col('_route_Route_SCD_Action') == lit('Prepare Update History')).drop('_route_Route_SCD_Action')
    df_Skip_Unchanged = _routed_df_Route_SCD_Action.filter(col('_route_Route_SCD_Action') == lit('Skip Unchanged')).drop('_route_Route_SCD_Action')
    df_Route_SCD_Action = df_Prepare_Insert

    # Step: Prepare Insert (Constant) [converted]
    # Add Constants: Prepare Insert
    df_Prepare_Insert = df_Route_SCD_Action
    df_Prepare_Insert = df_Prepare_Insert.withColumn("is_current", lit('Y'))
    # preserved.is_current: length='-1', precision='-1'
    df_Prepare_Insert = df_Prepare_Insert.withColumn("effective_end_date", lit('9999-12-31'))
    # preserved.effective_end_date: length='-1', precision='-1'
    df_Prepare_Insert = df_Prepare_Insert.withColumn("dw_insert_ts", lit('${Internal.Transformation.Name}'))
    # preserved.dw_insert_ts: length='-1', precision='-1'

    # Step: Prepare Update History (Constant) [converted]
    # Add Constants: Prepare Update History
    df_Prepare_Update_History = df_Route_SCD_Action
    df_Prepare_Update_History = df_Prepare_Update_History.withColumn("is_current_old", lit('N'))
    # preserved.is_current_old: length='-1', precision='-1'
    df_Prepare_Update_History = df_Prepare_Update_History.withColumn("history_flag", lit('Y'))
    # preserved.history_flag: length='-1', precision='-1'

    # Step: Skip Unchanged (Constant) [converted]
    # Add Constants: Skip Unchanged
    df_Skip_Unchanged = df_Route_SCD_Action
    df_Skip_Unchanged = df_Skip_Unchanged.withColumn("load_status", lit('SKIPPED'))
    # preserved.load_status: length='-1', precision='-1'

    # Step: Generate Surrogate Keys (Sequence) [converted]
    # Add Sequence: Generate Surrogate Keys
    # preserved.use_counter=True counter_name='customer_sk_counter'
    _w_seq_df_Generate_Surrogate_Keys = Window.orderBy(monotonically_increasing_id())
    # preserved.max_value=999999999 — wrap to start (Pentaho counter)
    df_Generate_Surrogate_Keys = df_Prepare_Insert.withColumn("customer_sk", lit(1) + ((row_number().over(_w_seq_df_Generate_Surrogate_Keys) - lit(1)) % greatest(((lit(999999999) - lit(1)) // lit(1)) + lit(1), lit(1))) * lit(1))
    # WARNING: Spark row_number over monotonically_increasing_id is order-based; sort upstream if deterministic sequencing across partitions is required

    # Step: Clone For History Maintain (CloneRow) [converted]
    # Clone Row: Clone For History Maintain
    # preserved.nr_clones=1
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='1'
    _clone_parts_df_Clone_For_History_Maintain = []
    _base_df_Clone_For_History_Maintain = df_Prepare_Update_History
    _orig_df_Clone_For_History_Maintain = _base_df_Clone_For_History_Maintain
    _clone_parts_df_Clone_For_History_Maintain.append(_orig_df_Clone_For_History_Maintain)
    for _ci in range(1, 1 + 1):
        _c = _base_df_Clone_For_History_Maintain
        _clone_parts_df_Clone_For_History_Maintain.append(_c)
    df_Clone_For_History_Maintain = _clone_parts_df_Clone_For_History_Maintain[0]
    for _part in _clone_parts_df_Clone_For_History_Maintain[1:]:
        df_Clone_For_History_Maintain = df_Clone_For_History_Maintain.unionByName(_part, allowMissingColumns=True)

    # Step: Dimension Statistics (MemoryGroupBy) [converted]
    # Memory Group By: Dimension Statistics
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Dimension_Statistics = df_Classify_SCD_Action.groupBy('scd_action').agg(count(lit(1)).alias('action_count'))

    # Step: Insert Path Ready (Dummy) [converted]
    # Dummy: Insert Path Ready
    # Pass-through step - DataFrame unchanged
    df_Dummy_Insert_Path_Ready = df_Generate_Surrogate_Keys

    # Step: Write Dimension Statistics (TextFileOutput) [converted]
    # Pentaho step: Write Dimension Statistics (type: TextFileOutput)
    # Pentaho filename: /audit/load_audit/customer_dimension_stats_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='scd_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='action_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Dimension_Statistics = df_Dimension_Statistics
    _out_df_Write_Dimension_Statistics = df_Write_Dimension_Statistics.select('scd_action', 'action_count')
    writer = _out_df_Write_Dimension_Statistics.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_dimension_stats_.csv')

    # Step: Merge Insert And History (MergeJoin) [converted]
    # Merge Join: Merge Insert And History
    # preserved.join_type='FULL OUTER'
    # preserved.join_keys=[{'left': 'customer_id', 'right': 'customer_id'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Merge Insert And History': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Merge_Insert_And_History = df_Dummy_Insert_Path_Ready.join(df_Clone_For_History_Maintain, on=["customer_id"], how='outer')
    # WARNING: MergeJoin 'Merge Insert And History': column lineage unavailable — join output may contain ambiguous duplicate column names
    df_Merge_Insert_And_History = _joined_df_Merge_Insert_And_History

    # Step: Stamp Effective Dates (SystemInfo) [converted]
    # System Info: Stamp Effective Dates
    df_Stamp_Effective_Dates = df_Dummy_Insert_Path_Ready
    df_Stamp_Effective_Dates = df_Stamp_Effective_Dates.withColumn("effective_start_date", current_date())
    df_Stamp_Effective_Dates = df_Stamp_Effective_Dates.withColumn("dw_update_ts", current_date())

    # Step: Copy Dim Stats To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Dim Stats To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Dim Stats To Result'
    _result_rows_df_Copy_Dim_Stats_To_Result = df_Write_Dimension_Statistics
    _pentaho_result_rows['Copy Dim Stats To Result'] = _result_rows_df_Copy_Dim_Stats_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Dim_Stats_To_Result
    df_Copy_Dim_Stats_To_Result = df_Write_Dimension_Statistics

    # Step: SCD2 Dimension Lookup Update (DimensionLookup) [converted]
    # Dimension Lookup/Update: SCD2 Dimension Lookup Update
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'SCD2 Dimension Lookup Update': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_customer' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: update; Type1=0 Type2=25 PunchThrough=2 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'SCD2 Dimension Lookup Update'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (customer_id)
    # Update mode: skip pre-MERGE lookup join; re-join after MERGEs below
    _scd_max_ts = lit("2199-12-31 23:59:59.999").cast("timestamp")
    _scd_min_ts = lit("1900-01-01 00:00:00").cast("timestamp")
    # preserved.scd_min_ts / scd_max_ts for open-ended version bounds
    _dim_src_df_SCD2_Dimension_Lookup_Update = df_Stamp_Effective_Dates.withColumn("_scd_effective", col("scd_change_date"))
    _dim_src_df_SCD2_Dimension_Lookup_Update = _dim_src_df_SCD2_Dimension_Lookup_Update.filter(~(col("customer_id").isNull())).dropDuplicates(["customer_id"])
    # Null business keys skipped; duplicate BK rows deduplicated before MERGE
    _dim_src_df_SCD2_Dimension_Lookup_Update.createOrReplaceTempView('_dim_scd_src_df_SCD2_Dimension_Lookup_Update')
    # Active version predicate approximated via date_to >= max_year boundary
    _dim_cmp_active = spark.table('main.retail_dwh.dim_customer').select("customer_sk", "customer_id", "version", "first_name", "last_name", "full_name", "address_line1", "address_line2", "city", "state_province", "postal_code", "country_code", "country_name", "preferred_currency", "loyalty_tier", "registration_date", "date_of_birth", "is_active", "customer_age", "age_group", "risk_category", "customer_segment", "years_as_customer", "preferred_language", "region_id", "region_name", "preferred_store_id", "batch_id")
    _dim_cmp_active = _dim_cmp_active.filter(col('effective_end_date') >= _scd_max_ts)
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_cmp_active = broadcast(_dim_cmp_active)
    _dim_cmp_active = _dim_cmp_active.select(col("customer_id"), col("customer_sk").alias("_prior_tk"), col("version").alias("_prior_version"), col("first_name").alias("_prior_first_name"), col("last_name").alias("_prior_last_name"), col("full_name").alias("_prior_full_name"), col("address_line1").alias("_prior_address_line1"), col("address_line2").alias("_prior_address_line2"), col("city").alias("_prior_city"), col("state_province").alias("_prior_state_province"), col("postal_code").alias("_prior_postal_code"), col("country_code").alias("_prior_country_code"), col("country_name").alias("_prior_country_name"), col("preferred_currency").alias("_prior_preferred_currency"), col("loyalty_tier").alias("_prior_loyalty_tier"), col("registration_date").alias("_prior_registration_date"), col("date_of_birth").alias("_prior_date_of_birth"), col("is_active").alias("_prior_is_active"), col("customer_age").alias("_prior_customer_age"), col("age_group").alias("_prior_age_group"), col("risk_category").alias("_prior_risk_category"), col("customer_segment").alias("_prior_customer_segment"), col("years_as_customer").alias("_prior_years_as_customer"), col("preferred_language").alias("_prior_preferred_language"), col("region_id").alias("_prior_region_id"), col("region_name").alias("_prior_region_name"), col("preferred_store_id").alias("_prior_preferred_store_id"), col("batch_id").alias("_prior_batch_id"))
    _dim_cmp_df_SCD2_Dimension_Lookup_Update = _dim_src_df_SCD2_Dimension_Lookup_Update.join(_dim_cmp_active, on='customer_id', how='left')
    # PunchThrough (2 fields): UPDATE all historical versions
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_customer').alias("t")
        .merge(spark.table('_dim_scd_src_df_SCD2_Dimension_Lookup_Update').alias("s"), 't.`customer_id` <=> s.`customer_id`')
        .whenMatchedUpdate(set={'email': 's.`email`', 'phone': 's.`phone`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Version field preserved: version
    # Type 2 expire active version when attributes change
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_customer').alias("t")
        .merge(spark.table('_dim_scd_src_df_SCD2_Dimension_Lookup_Update').alias("s"), "t.`customer_id` <=> s.`customer_id` AND t.`effective_end_date` >= TIMESTAMP '2199-12-31 23:59:59.999'")
        .whenMatchedUpdate(condition='(NOT (t.`first_name` <=> s.`first_name`) OR NOT (t.`last_name` <=> s.`last_name`) OR NOT (t.`full_name` <=> s.`full_name`) OR NOT (t.`address_line1` <=> s.`address_line1`) OR NOT (t.`address_line2` <=> s.`address_line2`) OR NOT (t.`city` <=> s.`city`) OR NOT (t.`state_province` <=> s.`state_province`) OR NOT (t.`postal_code` <=> s.`postal_code`) OR NOT (t.`country_code` <=> s.`country_code`) OR NOT (t.`country_name` <=> s.`country_name`) OR NOT (t.`preferred_currency` <=> s.`preferred_currency`) OR NOT (t.`loyalty_tier` <=> s.`loyalty_tier`) OR NOT (t.`registration_date` <=> s.`registration_date`) OR NOT (t.`date_of_birth` <=> s.`date_of_birth`) OR NOT (t.`is_active` <=> s.`is_active`) OR NOT (t.`customer_age` <=> s.`customer_age`) OR NOT (t.`age_group` <=> s.`age_group`) OR NOT (t.`risk_category` <=> s.`risk_category`) OR NOT (t.`customer_segment` <=> s.`customer_segment`) OR NOT (t.`years_as_customer` <=> s.`years_as_customer`) OR NOT (t.`preferred_language` <=> s.`preferred_language`) OR NOT (t.`region_id` <=> s.`region_id`) OR NOT (t.`region_name` <=> s.`region_name`) OR NOT (t.`preferred_store_id` <=> s.`store_id`) OR NOT (t.`batch_id` <=> s.`batch_id`))', set={'effective_end_date': 's.`_scd_effective`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Build insert candidates (new BK and/or Type 2 attribute changes)
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_cmp_df_SCD2_Dimension_Lookup_Update.filter(col("_prior_tk").isNull() | ((~col("_prior_first_name").eqNullSafe(col("first_name"))) | (~col("_prior_last_name").eqNullSafe(col("last_name"))) | (~col("_prior_full_name").eqNullSafe(col("full_name"))) | (~col("_prior_address_line1").eqNullSafe(col("address_line1"))) | (~col("_prior_address_line2").eqNullSafe(col("address_line2"))) | (~col("_prior_city").eqNullSafe(col("city"))) | (~col("_prior_state_province").eqNullSafe(col("state_province"))) | (~col("_prior_postal_code").eqNullSafe(col("postal_code"))) | (~col("_prior_country_code").eqNullSafe(col("country_code"))) | (~col("_prior_country_name").eqNullSafe(col("country_name"))) | (~col("_prior_preferred_currency").eqNullSafe(col("preferred_currency"))) | (~col("_prior_loyalty_tier").eqNullSafe(col("loyalty_tier"))) | (~col("_prior_registration_date").eqNullSafe(col("registration_date"))) | (~col("_prior_date_of_birth").eqNullSafe(col("date_of_birth"))) | (~col("_prior_is_active").eqNullSafe(col("is_active"))) | (~col("_prior_customer_age").eqNullSafe(col("customer_age"))) | (~col("_prior_age_group").eqNullSafe(col("age_group"))) | (~col("_prior_risk_category").eqNullSafe(col("risk_category"))) | (~col("_prior_customer_segment").eqNullSafe(col("customer_segment"))) | (~col("_prior_years_as_customer").eqNullSafe(col("years_as_customer"))) | (~col("_prior_preferred_language").eqNullSafe(col("preferred_language"))) | (~col("_prior_region_id").eqNullSafe(col("region_id"))) | (~col("_prior_region_name").eqNullSafe(col("region_name"))) | (~col("_prior_preferred_store_id").eqNullSafe(col("store_id"))) | (~col("_prior_batch_id").eqNullSafe(col("batch_id")))) | (col("_prior_tk").isNotNull() & ((~col("_prior_first_name").eqNullSafe(col("first_name"))) | (~col("_prior_last_name").eqNullSafe(col("last_name"))) | (~col("_prior_full_name").eqNullSafe(col("full_name"))) | (~col("_prior_address_line1").eqNullSafe(col("address_line1"))) | (~col("_prior_address_line2").eqNullSafe(col("address_line2"))) | (~col("_prior_city").eqNullSafe(col("city"))) | (~col("_prior_state_province").eqNullSafe(col("state_province"))) | (~col("_prior_postal_code").eqNullSafe(col("postal_code"))) | (~col("_prior_country_code").eqNullSafe(col("country_code"))) | (~col("_prior_country_name").eqNullSafe(col("country_name"))) | (~col("_prior_preferred_currency").eqNullSafe(col("preferred_currency"))) | (~col("_prior_loyalty_tier").eqNullSafe(col("loyalty_tier"))) | (~col("_prior_registration_date").eqNullSafe(col("registration_date"))) | (~col("_prior_date_of_birth").eqNullSafe(col("date_of_birth"))) | (~col("_prior_is_active").eqNullSafe(col("is_active"))) | (~col("_prior_customer_age").eqNullSafe(col("customer_age"))) | (~col("_prior_age_group").eqNullSafe(col("age_group"))) | (~col("_prior_risk_category").eqNullSafe(col("risk_category"))) | (~col("_prior_customer_segment").eqNullSafe(col("customer_segment"))) | (~col("_prior_years_as_customer").eqNullSafe(col("years_as_customer"))) | (~col("_prior_preferred_language").eqNullSafe(col("preferred_language"))) | (~col("_prior_region_id").eqNullSafe(col("region_id"))) | (~col("_prior_region_name").eqNullSafe(col("region_name"))) | (~col("_prior_preferred_store_id").eqNullSafe(col("store_id"))) | (~col("_prior_batch_id").eqNullSafe(col("batch_id"))))))
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.withColumn('version', (coalesce(col("_prior_version"), lit(0)) + lit(1)).cast("long"))
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.drop('_prior_tk', '_prior_version', '_prior_first_name', '_prior_last_name', '_prior_full_name', '_prior_address_line1', '_prior_address_line2', '_prior_city', '_prior_state_province', '_prior_postal_code', '_prior_country_code', '_prior_country_name', '_prior_preferred_currency', '_prior_loyalty_tier', '_prior_registration_date', '_prior_date_of_birth', '_prior_is_active', '_prior_customer_age', '_prior_age_group', '_prior_risk_category', '_prior_customer_segment', '_prior_years_as_customer', '_prior_preferred_language', '_prior_region_id', '_prior_region_name', '_prior_preferred_store_id', '_prior_batch_id')
    # Assign surrogate keys for new / Type2 rows (tablemax)
    # tablemax + row_number (IDENTITY would omit tk from INSERT below)
    _max_tk = spark.sql("SELECT COALESCE(MAX(`customer_sk`), 0) AS m FROM main.retail_dwh.dim_customer").collect()[0][0]
    from pyspark.sql.window import Window as _DWWindow
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.withColumn("_dw_rn", row_number().over(_DWWindow.orderBy(lit(1))))
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.withColumn("customer_sk", (lit(_max_tk) + col("_dw_rn")).cast("long")).drop("_dw_rn")
    _dim_new_df_SCD2_Dimension_Lookup_Update.createOrReplaceTempView('_dim_scd_new_df_SCD2_Dimension_Lookup_Update')
    # Single MERGE INSERT on technical key (new SK never matches existing)
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_customer').alias("t")
        .merge(spark.table('_dim_scd_new_df_SCD2_Dimension_Lookup_Update').alias("s"), 't.`customer_sk` <=> s.`customer_sk`')
        .whenNotMatchedInsert(values={'customer_sk': 's.`customer_sk`', 'customer_id': 's.`customer_id`', 'first_name': 's.`first_name`', 'last_name': 's.`last_name`', 'full_name': 's.`full_name`', 'email': 's.`email`', 'phone': 's.`phone`', 'address_line1': 's.`address_line1`', 'address_line2': 's.`address_line2`', 'city': 's.`city`', 'state_province': 's.`state_province`', 'postal_code': 's.`postal_code`', 'country_code': 's.`country_code`', 'country_name': 's.`country_name`', 'preferred_currency': 's.`preferred_currency`', 'loyalty_tier': 's.`loyalty_tier`', 'registration_date': 's.`registration_date`', 'date_of_birth': 's.`date_of_birth`', 'is_active': 's.`is_active`', 'customer_age': 's.`customer_age`', 'age_group': 's.`age_group`', 'risk_category': 's.`risk_category`', 'customer_segment': 's.`customer_segment`', 'years_as_customer': 's.`years_as_customer`', 'preferred_language': 's.`preferred_language`', 'region_id': 's.`region_id`', 'region_name': 's.`region_name`', 'preferred_store_id': 's.`store_id`', 'batch_id': 's.`batch_id`', 'effective_start_date': 's.`_scd_effective`', 'effective_end_date': "TIMESTAMP '2199-12-31 23:59:59.999'", 'version': 's.`version`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Schema evolution: add new attribute columns with ALTER TABLE before MERGE
    # Effective date overlaps / multiple actives: enforce with constraints or dedupe window on (business_keys, date_from)
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_SCD2_Dimension_Lookup_Update = spark.table('main.retail_dwh.dim_customer').select("customer_sk", "customer_id", "effective_start_date", "effective_end_date", "version", "first_name", "last_name", "full_name", "address_line1", "address_line2", "city", "state_province", "postal_code", "country_code", "country_name", "preferred_currency", "loyalty_tier", "registration_date", "date_of_birth", "is_active", "customer_age", "age_group", "risk_category", "customer_segment", "years_as_customer", "preferred_language", "region_id", "region_name", "preferred_store_id", "batch_id", "email", "phone")
    _dim_df_SCD2_Dimension_Lookup_Update = broadcast(_dim_df_SCD2_Dimension_Lookup_Update)
    df_SCD2_Dimension_Lookup_Update = df_Stamp_Effective_Dates.join(_dim_df_SCD2_Dimension_Lookup_Update, on=((df_Stamp_Effective_Dates["customer_id"] == _dim_df_SCD2_Dimension_Lookup_Update["customer_id"]) & (df_Stamp_Effective_Dates["scd_change_date"] >= _dim_df_SCD2_Dimension_Lookup_Update["effective_start_date"]) & (df_Stamp_Effective_Dates["scd_change_date"] < _dim_df_SCD2_Dimension_Lookup_Update["effective_end_date"])), how='left')
    # Null 'customer_sk' after update indicates unresolved BK / null keys

    # Step: Write Customer Dimension File (TextFileOutput) [converted]
    # Pentaho step: Write Customer Dimension File (type: TextFileOutput)
    # Pentaho filename: /output/customer/dimension/dim_customer_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
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
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='effective_start_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='effective_end_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_current' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='scd_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_Dimension_File = df_SCD2_Dimension_Lookup_Update
    _out_df_Write_Customer_Dimension_File = df_Write_Customer_Dimension_File.select('customer_sk', 'customer_id', 'first_name', 'last_name', 'full_name', 'email', 'phone', 'address_line1', 'address_line2', 'city', 'state_province', 'postal_code', 'country_code', 'country_name', 'preferred_currency_final', 'loyalty_tier', 'registration_date', 'date_of_birth', 'is_active', 'customer_age', 'age_group', 'risk_category', 'customer_segment', 'years_as_customer', 'preferred_language', 'region_id', 'region_name', 'store_id', 'effective_start_date', 'effective_end_date', 'is_current', 'batch_id', 'scd_action')
    writer = _out_df_Write_Customer_Dimension_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/dim_customer_.csv')

    # Step: Insert DimCustomer Table (TableOutput) [converted]
    # Pentaho step: Insert DimCustomer Table (type: TableOutput) (Pentaho schema: retail_dwh)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Insert_DimCustomer_Table = df_Write_Customer_Dimension_File.select(col('customer_sk'), col('customer_id'), col('first_name'), col('last_name'), col('full_name'), col('email'), col('phone'), col('address_line1'), col('address_line2'), col('city'), col('state_province'), col('postal_code'), col('country_code'), col('country_name'), col('preferred_currency_final'), col('loyalty_tier'), col('registration_date'), col('date_of_birth'), col('is_active'), col('effective_start_date'), col('effective_end_date'), col('is_current'), col('batch_id'))
    df_Insert_DimCustomer_Table = _mapped_df_Insert_DimCustomer_Table
    write_delta(
        df_Insert_DimCustomer_Table,
        f"{catalog}.{schema}.dim_customer",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.dim_customer", mode='append')

    # Step: Log Dimension Load (WriteToLog) [converted]
    # Write to Log: Log Dimension Load
    # preserved.log_level='Basic'
    # preserved.log_message='DIM | TRANS=TR_Customer_Dimension_Load | action=${scd_action} | customer_id=${customer_id} | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Dimension_Load = logging.getLogger('pentaho.writetolog.Log_Dimension_Load')
    _log_df_Log_Dimension_Load.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Dimension_Load = df_Write_Customer_Dimension_File
    _log_rows_df_Log_Dimension_Load = _log_df_df_Log_Dimension_Load.take(20)
    _log_df_Log_Dimension_Load.info('Log Dimension Load' + ' | columns=' + str(_log_df_df_Log_Dimension_Load.columns))
    _log_df_Log_Dimension_Load.info('DIM | TRANS=TR_Customer_Dimension_Load | action=${scd_action} | customer_id=${customer_id} | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Dimension_Load:
        _log_df_Log_Dimension_Load.info('Log Dimension Load' + ' | ' + str(_lr.asDict()))
    df_Log_Dimension_Load = df_Write_Customer_Dimension_File

    # Step: Dimension Load Complete (Dummy) [converted]
    # Dummy: Dimension Load Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Dimension_Load_Complete = df_Insert_DimCustomer_Table

    log_event(_LOG, "transformation_end")
    return df_Dummy_Dimension_Load_Complete
