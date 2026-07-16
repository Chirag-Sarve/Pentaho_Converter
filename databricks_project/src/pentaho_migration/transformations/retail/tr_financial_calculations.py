"""PySpark module migrated from Pentaho transformation: TR_Financial_Calculations.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/reporting/TR_Financial_Calculations.ktr
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
    md5,
    concat,
    sum as _sum,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_financial_calculations")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Financial_Calculations`` step-for-step."""
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

    # Step: Get Calc Variables (GetVariable) [converted]
    # Get Variables: Get Calc Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'tax_rate_default', 'variable': '0.08', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'cogs_ratio_default', 'variable': '0.62', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'opex_ratio', 'variable': '0.18', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'current_date', 'tax_rate_default', 'cogs_ratio_default', 'opex_ratio']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Calc_Variables = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Calc_Variables = df_Get_Calc_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Calc_Variables = df_Get_Calc_Variables.withColumn('run_id', lit(_run_id_resolved))
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
    df_Get_Calc_Variables = df_Get_Calc_Variables.withColumn('current_date', lit(_current_date_resolved))
    # field 'tax_rate_default' from variable string '0.08'
    # preserved.field.tax_rate_default.trim_type='none'
    # preserved.field.tax_rate_default.type='String'
    _tax_rate_default_resolved = None
    _dbu__tax_rate_default_resolved = globals().get('dbutils')
    if _dbu__tax_rate_default_resolved is not None and hasattr(_dbu__tax_rate_default_resolved, 'widgets'):
        try:
            _tax_rate_default_resolved = _dbu__tax_rate_default_resolved.widgets.get('0.08')
        except Exception:
            _tax_rate_default_resolved = None
    if _tax_rate_default_resolved in (None, ''):
        import os as _os__tax_rate_default_resolved
        _tax_rate_default_resolved = _os__tax_rate_default_resolved.environ.get('0.08')
    if _tax_rate_default_resolved in (None, ''):
        try:
            _tax_rate_default_resolved = spark.conf.get('pentaho.var.0.08')
        except Exception:
            _tax_rate_default_resolved = None
    if _tax_rate_default_resolved in (None, ''):
        _tax_rate_default_resolved = ''
    if _tax_rate_default_resolved is None:
        _tax_rate_default_resolved = ''
    df_Get_Calc_Variables = df_Get_Calc_Variables.withColumn('tax_rate_default', lit(_tax_rate_default_resolved))
    # field 'cogs_ratio_default' from variable string '0.62'
    # preserved.field.cogs_ratio_default.trim_type='none'
    # preserved.field.cogs_ratio_default.type='String'
    _cogs_ratio_default_resolved = None
    _dbu__cogs_ratio_default_resolved = globals().get('dbutils')
    if _dbu__cogs_ratio_default_resolved is not None and hasattr(_dbu__cogs_ratio_default_resolved, 'widgets'):
        try:
            _cogs_ratio_default_resolved = _dbu__cogs_ratio_default_resolved.widgets.get('0.62')
        except Exception:
            _cogs_ratio_default_resolved = None
    if _cogs_ratio_default_resolved in (None, ''):
        import os as _os__cogs_ratio_default_resolved
        _cogs_ratio_default_resolved = _os__cogs_ratio_default_resolved.environ.get('0.62')
    if _cogs_ratio_default_resolved in (None, ''):
        try:
            _cogs_ratio_default_resolved = spark.conf.get('pentaho.var.0.62')
        except Exception:
            _cogs_ratio_default_resolved = None
    if _cogs_ratio_default_resolved in (None, ''):
        _cogs_ratio_default_resolved = ''
    if _cogs_ratio_default_resolved is None:
        _cogs_ratio_default_resolved = ''
    df_Get_Calc_Variables = df_Get_Calc_Variables.withColumn('cogs_ratio_default', lit(_cogs_ratio_default_resolved))
    # field 'opex_ratio' from variable string '0.18'
    # preserved.field.opex_ratio.trim_type='none'
    # preserved.field.opex_ratio.type='String'
    _opex_ratio_resolved = None
    _dbu__opex_ratio_resolved = globals().get('dbutils')
    if _dbu__opex_ratio_resolved is not None and hasattr(_dbu__opex_ratio_resolved, 'widgets'):
        try:
            _opex_ratio_resolved = _dbu__opex_ratio_resolved.widgets.get('0.18')
        except Exception:
            _opex_ratio_resolved = None
    if _opex_ratio_resolved in (None, ''):
        import os as _os__opex_ratio_resolved
        _opex_ratio_resolved = _os__opex_ratio_resolved.environ.get('0.18')
    if _opex_ratio_resolved in (None, ''):
        try:
            _opex_ratio_resolved = spark.conf.get('pentaho.var.0.18')
        except Exception:
            _opex_ratio_resolved = None
    if _opex_ratio_resolved in (None, ''):
        _opex_ratio_resolved = ''
    if _opex_ratio_resolved is None:
        _opex_ratio_resolved = ''
    df_Get_Calc_Variables = df_Get_Calc_Variables.withColumn('opex_ratio', lit(_opex_ratio_resolved))

    # Step: Read Calc Policy JSON (JsonInput) [converted]
    # JSON Input: Read Calc Policy JSON
    df_Read_Calc_Policy_JSON = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/finance_reporting_policy.json')

    # Step: Read Finance Staging (CsvInput) [converted]
    # CSV Input: Read Finance Staging
    df_Read_Finance_Staging = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, order_date STRING, store_sk INT, product_sk INT, customer_sk INT, employee_sk INT, promotion_sk INT, channel_mapped STRING, currency_code STRING, quantity_sold DOUBLE, extended_price DOUBLE, discount_amount_calc DOUBLE, net_sales_amount DOUBLE, tax_amount_calc DOUBLE, shipping_cost_calc DOUBLE, total_revenue DOUBLE, profit DOUBLE, converted_amount_usd DOUBLE, refund_amount_calc DOUBLE, return_amount DOUBLE, batch_id STRING, run_id STRING, revenue_usd DOUBLE, refund_amount DOUBLE, inventory_cost DOUBLE, unit_cost DOUBLE, quantity_on_hand DOUBLE, payment_amount DOUBLE, exchange_rate DOUBLE, sales_target_amount DOUBLE, extract_checksum STRING')
        .load(f'{data_dir}/stg_finance_joined_.csv')
    )

    # Step: Read Targets For Variance (CsvInput) [converted]
    # CSV Input: Read Targets For Variance
    df_Read_Targets_For_Variance = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('target_id STRING, store_id STRING, region_id STRING, target_date STRING, sales_target_amount STRING, orders_target STRING, currency_code STRING, channel STRING, created_by STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/stg_daily_targets_.csv')
    )

    # Step: Null Safe Measures (IfNull) [converted]
    # If Field Value Is Null: Null Safe Measures
    df_Null_Safe_Measures = df_Read_Finance_Staging
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('total_revenue', when(col('total_revenue').isNull(), lit(0)).otherwise(col('total_revenue')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('extended_price', when(col('extended_price').isNull(), lit(0)).otherwise(col('extended_price')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('discount_amount_calc', when(col('discount_amount_calc').isNull(), lit(0)).otherwise(col('discount_amount_calc')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('tax_amount_calc', when(col('tax_amount_calc').isNull(), lit(0)).otherwise(col('tax_amount_calc')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('shipping_cost_calc', when(col('shipping_cost_calc').isNull(), lit(0)).otherwise(col('shipping_cost_calc')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('refund_amount', when(col('refund_amount').isNull(), lit(0)).otherwise(col('refund_amount')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('inventory_cost', when(col('inventory_cost').isNull(), lit(0)).otherwise(col('inventory_cost')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('exchange_rate', when(col('exchange_rate').isNull(), lit(1)).otherwise(col('exchange_rate')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('sales_target_amount', when(col('sales_target_amount').isNull(), lit(0)).otherwise(col('sales_target_amount')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('quantity_sold', when(col('quantity_sold').isNull(), lit(0)).otherwise(col('quantity_sold')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('converted_amount_usd', when(col('converted_amount_usd').isNull(), lit(0)).otherwise(col('converted_amount_usd')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('revenue_usd', when(col('revenue_usd').isNull(), lit(0)).otherwise(col('revenue_usd')))
    df_Null_Safe_Measures = df_Null_Safe_Measures.withColumn('unit_cost', when(col('unit_cost').isNull(), lit(0)).otherwise(col('unit_cost')))

    # Step: Map Channel Labels (ValueMapper) [converted]
    # Value Mapper: Map Channel Labels
    df_Map_Channel_Labels = df_Null_Safe_Measures.withColumn("channel_mapped", when((lower(col("channel_mapped")) == lower(lit('WEB'))), lit('E-Commerce')).when((lower(col("channel_mapped")) == lower(lit('ONLINE'))), lit('E-Commerce')).when((lower(col("channel_mapped")) == lower(lit('STORE'))), lit('In-Store')).when((lower(col("channel_mapped")) == lower(lit('IN-STORE'))), lit('In-Store')).when((lower(col("channel_mapped")) == lower(lit('BOTH'))), lit('Omnichannel')).when((col("channel_mapped").isNull() | (col("channel_mapped") == lit(''))), col("channel_mapped")).otherwise(lit('Other')))
    # preserved.case_sensitive=False mappings=5 default='Other'

    # Step: Validate Order ID Pattern (RegexEval) [converted]
    # Regex Evaluation: Validate Order ID Pattern
    # preserved.matcher='order_id'
    # preserved.pattern='^ORD[0-9]{7}$'
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
    df_Validate_Order_ID_Pattern = df_Map_Channel_Labels
    df_Validate_Order_ID_Pattern = df_Validate_Order_ID_Pattern.withColumn('result', when(col('order_id').rlike('^ORD[0-9]{7}$'), lit("Y")).otherwise(lit("N")))

    # Step: Valid Order Id? (FilterRows) [failed]
    # Filter Rows: Valid Order Id?
    df_Calculate_Core_Financials = df_Validate_Order_ID_Pattern.filter((col("order_id_valid") == lit('Y')))
    df_Route_Calc_Rejects = df_Validate_Order_ID_Pattern.filter(~((col("order_id_valid") == lit('Y'))))
    df_Valid_Order_Id? = df_Calculate_Core_Financials

    # Step: Calculate Core Financials (Calculator) [failed]
    # Calculator: Calculate Core Financials
    df_Calculate_Core_Financials = df_Valid_Order_Id?
    df_Calculate_Core_Financials = df_Calculate_Core_Financials.withColumn("revenue", (col("revenue_usd")).cast('decimal(38,4)'))
    df_Calculate_Core_Financials = df_Calculate_Core_Financials.withColumn("discount_amount", (col("discount_amount_calc")).cast('decimal(38,4)'))
    df_Calculate_Core_Financials = df_Calculate_Core_Financials.withColumn("shipping_cost", (col("shipping_cost_calc")).cast('decimal(38,4)'))
    df_Calculate_Core_Financials = df_Calculate_Core_Financials.withColumn("tax_amount_src", (col("tax_amount_calc")).cast('decimal(38,4)'))

    # Step: Route Calc Rejects (Dummy) [converted]
    # Dummy: Route Calc Rejects
    # Pass-through step - DataFrame unchanged
    df_Dummy_Route_Calc_Rejects = df_Route_Calc_Rejects

    # Step: Compute P&L Measures (Formula) [failed]
    # Formula: Compute P&L Measures
    df_Compute_P&L_Measures = df_Calculate_Core_Financials
    df_Compute_P&L_Measures = df_Compute_P&L_Measures.withColumn('formula_result', lit(None))  # empty formula

    # Step: Write Calc Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Calc Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/finance/finance_calc_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id_valid' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Calc_Rejects = df_Dummy_Route_Calc_Rejects
    _out_df_Write_Calc_Rejects = df_Write_Calc_Rejects.select('order_item_id', 'order_id', 'order_id_valid', 'batch_id', 'run_id')
    writer = _out_df_Write_Calc_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/finance_calc_rejects_.csv')

    # Step: Add Reporting Currency Code (Constant) [failed]
    # Add Constants: Add Reporting Currency Code
    df_Add_Reporting_Currency_Code = df_Compute_P&L_Measures
    df_Add_Reporting_Currency_Code = df_Add_Reporting_Currency_Code.withColumn("reporting_currency_code", lit('USD'))
    # preserved.reporting_currency_code: length='-1', precision='-1'
    df_Add_Reporting_Currency_Code = df_Add_Reporting_Currency_Code.withColumn("forecast_factor", lit(1.05))
    # preserved.forecast_factor: length='-1', precision='-1'

    # Step: Set Reporting Currency USD (SetValueField) [converted]
    # Set Field Value: Set Reporting Currency USD
    df_Set_Reporting_Currency_USD = df_Add_Reporting_Currency_Code
    df_Set_Reporting_Currency_USD = df_Set_Reporting_Currency_USD.withColumn("currency_code", col("reporting_currency_code"))

    # Step: Currency Conversion And Variances (Formula) [converted]
    # Formula: Currency Conversion And Variances
    df_Currency_Conversion_And_Variances = df_Set_Reporting_Currency_USD
    df_Currency_Conversion_And_Variances = df_Currency_Conversion_And_Variances.withColumn('formula_result', lit(None))  # empty formula

    # Step: Classify Margin Health (NumberRange) [converted]
    # Number Range: Classify Margin Health
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Classify_Margin_Health = df_Currency_Conversion_And_Variances.withColumn('margin_band', when(col("gross_margin_pct").isNull(), lit('Unknown')).otherwise(when((col("gross_margin_pct").cast("double") >= lit(-999.0)) & (col("gross_margin_pct").cast("double") < lit(0.05)), lit('Critical')).when((col("gross_margin_pct").cast("double") >= lit(0.05)) & (col("gross_margin_pct").cast("double") < lit(0.15)), lit('Watch')).when((col("gross_margin_pct").cast("double") >= lit(0.15)) & (col("gross_margin_pct").cast("double") < lit(0.35)), lit('Healthy')).when((col("gross_margin_pct").cast("double") >= lit(0.35)) & (col("gross_margin_pct").cast("double") < lit(999.0)), lit('Premium')).otherwise(lit('Unknown'))))
    # preserved.fallback='Unknown' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: DB Join Store Dim Optional (DBJoin) [partial]
    # Database Join: DB Join Store Dim Optional
    # preserved.connection='conn_dev_dwh'
    # preserved.sql='SELECT store_sk, store_id, region_id FROM retail_dwh.dim_store WHERE store_sk = ?'
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'store_sk', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_DB_Join_Store_Dim_Optional = 'SELECT store_sk, store_id, region_id FROM retail_dwh.dim_store WHERE store_sk = ?'
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template='SELECT store_sk, store_id, region_id FROM retail_dwh.dim_store WHERE store_sk = :store_sk'
    _param_fields_df_DB_Join_Store_Dim_Optional = ['store_sk', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement('SELECT store_sk, store_id, region_id FROM retail_dwh.dim_store WHERE store_sk = ?')
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_DB_Join_Store_Dim_Optional, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_DB_Join_Store_Dim_Optional = df_Classify_Margin_Health
    # Join type preserved as 'left'; join keys=['store_sk', '\n        ']

    # Step: Route By Margin Band (SwitchCase) [converted]
    # Switch / Case: Route By Margin Band
    # preserved.fieldname='margin_band'
    # preserved.switch_field='margin_band'
    # preserved.cases=[{'value': 'Critical', 'target_step': 'Flag Critical Margin'}, {'value': 'Watch', 'target_step': 'Flag Watch Margin'}, {'value': 'Healthy', 'target_step': 'Flag Healthy Margin'}, {'value': 'Premium', 'target_step': 'Flag Premium Margin'}]
    # preserved.default_target_step='Flag Unknown Margin'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'Critical', 'target_step': 'Flag Critical Margin'}, {'value': 'Watch', 'target_step': 'Flag Watch Margin'}, {'value': 'Healthy', 'target_step': 'Flag Healthy Margin'}, {'value': 'Premium', 'target_step': 'Flag Premium Margin'}]
    _routed_df_Route_By_Margin_Band = df_Classify_Margin_Health.withColumn('_route_Route_By_Margin_Band', when(col("margin_band") == lit('Critical'), lit('Flag Critical Margin')).when(col("margin_band") == lit('Watch'), lit('Flag Watch Margin')).when(col("margin_band") == lit('Healthy'), lit('Flag Healthy Margin')).when(col("margin_band") == lit('Premium'), lit('Flag Premium Margin')).otherwise(lit('Flag Unknown Margin')))
    df_Flag_Critical_Margin = _routed_df_Route_By_Margin_Band.filter(col('_route_Route_By_Margin_Band') == lit('Flag Critical Margin')).drop('_route_Route_By_Margin_Band')
    df_Flag_Watch_Margin = _routed_df_Route_By_Margin_Band.filter(col('_route_Route_By_Margin_Band') == lit('Flag Watch Margin')).drop('_route_Route_By_Margin_Band')
    df_Flag_Healthy_Margin = _routed_df_Route_By_Margin_Band.filter(col('_route_Route_By_Margin_Band') == lit('Flag Healthy Margin')).drop('_route_Route_By_Margin_Band')
    df_Flag_Premium_Margin = _routed_df_Route_By_Margin_Band.filter(col('_route_Route_By_Margin_Band') == lit('Flag Premium Margin')).drop('_route_Route_By_Margin_Band')
    df_Flag_Unknown_Margin = _routed_df_Route_By_Margin_Band.filter(col('_route_Route_By_Margin_Band') == lit('Flag Unknown Margin')).drop('_route_Route_By_Margin_Band')
    df_Route_By_Margin_Band = df_Flag_Critical_Margin

    # Step: Flag Critical Margin (Constant) [converted]
    # Add Constants: Flag Critical Margin
    df_Flag_Critical_Margin = df_Route_By_Margin_Band
    df_Flag_Critical_Margin = df_Flag_Critical_Margin.withColumn("margin_route", lit('Critical Margin'))
    # preserved.margin_route: length='-1', precision='-1'

    # Step: Flag Healthy Margin (Constant) [converted]
    # Add Constants: Flag Healthy Margin
    df_Flag_Healthy_Margin = df_Route_By_Margin_Band
    df_Flag_Healthy_Margin = df_Flag_Healthy_Margin.withColumn("margin_route", lit('Healthy Margin'))
    # preserved.margin_route: length='-1', precision='-1'

    # Step: Flag Premium Margin (Constant) [converted]
    # Add Constants: Flag Premium Margin
    df_Flag_Premium_Margin = df_Route_By_Margin_Band
    df_Flag_Premium_Margin = df_Flag_Premium_Margin.withColumn("margin_route", lit('Premium Margin'))
    # preserved.margin_route: length='-1', precision='-1'

    # Step: Flag Unknown Margin (Constant) [converted]
    # Add Constants: Flag Unknown Margin
    df_Flag_Unknown_Margin = df_Route_By_Margin_Band
    df_Flag_Unknown_Margin = df_Flag_Unknown_Margin.withColumn("margin_route", lit('Unknown Margin'))
    # preserved.margin_route: length='-1', precision='-1'

    # Step: Flag Watch Margin (Constant) [converted]
    # Add Constants: Flag Watch Margin
    df_Flag_Watch_Margin = df_Route_By_Margin_Band
    df_Flag_Watch_Margin = df_Flag_Watch_Margin.withColumn("margin_route", lit('Watch Margin'))
    # preserved.margin_route: length='-1', precision='-1'

    # Step: Append Margin Routes A (Append) [converted]
    # Append Streams: Append Margin Routes A
    # preserved.head_name='Flag Critical Margin'
    # preserved.tail_name='Flag Watch Margin'
    # preserved.stream_order=['Flag Critical Margin', 'Flag Watch Margin']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Margin_Routes_A = df_Flag_Critical_Margin.unionByName(df_Flag_Watch_Margin, allowMissingColumns=True)

    # Step: Append Margin Routes B (Append) [converted]
    # Append Streams: Append Margin Routes B
    # preserved.head_name='Append Margin Routes A'
    # preserved.tail_name='Flag Healthy Margin'
    # preserved.stream_order=['Append Margin Routes A', 'Flag Healthy Margin']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Margin_Routes_B = df_Append_Margin_Routes_A.unionByName(df_Flag_Healthy_Margin, allowMissingColumns=True)

    # Step: Append Margin Routes C (Append) [converted]
    # Append Streams: Append Margin Routes C
    # preserved.head_name='Append Margin Routes B'
    # preserved.tail_name='Flag Premium Margin'
    # preserved.stream_order=['Append Margin Routes B', 'Flag Premium Margin']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Margin_Routes_C = df_Append_Margin_Routes_B.unionByName(df_Flag_Premium_Margin, allowMissingColumns=True)

    # Step: Append Margin Routes D (Append) [converted]
    # Append Streams: Append Margin Routes D
    # preserved.head_name='Append Margin Routes C'
    # preserved.tail_name='Flag Unknown Margin'
    # preserved.stream_order=['Append Margin Routes C', 'Flag Unknown Margin']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Margin_Routes_D = df_Append_Margin_Routes_C.unionByName(df_Flag_Unknown_Margin, allowMissingColumns=True)

    # Step: Sort By Order Date (SortRows) [converted]
    # Sort Rows: Sort By Order Date
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_By_Order_Date = df_Append_Margin_Routes_D
    _sort_df_Sort_By_Order_Date = _sort_df_Sort_By_Order_Date.withColumn("_sort_ci_order_date", lower(col("order_date").cast("string")))
    _sort_df_Sort_By_Order_Date = _sort_df_Sort_By_Order_Date.withColumn("_sort_ci_order_item_id", lower(col("order_item_id").cast("string")))
    df_Sort_By_Order_Date = _sort_df_Sort_By_Order_Date.orderBy(col("_sort_ci_order_date").asc_nulls_last(), col("_sort_ci_order_item_id").asc_nulls_last())
    df_Sort_By_Order_Date = df_Sort_By_Order_Date.drop("_sort_ci_order_date", "_sort_ci_order_item_id")

    # Step: Derive Date Keys For Rolling (Formula) [converted]
    # Formula: Derive Date Keys For Rolling
    df_Derive_Date_Keys_For_Rolling = df_Sort_By_Order_Date
    df_Derive_Date_Keys_For_Rolling = df_Derive_Date_Keys_For_Rolling.withColumn('formula_result', lit(None))  # empty formula

    # Step: Daily Revenue For Rolling (GroupBy) [converted]
    # Group By: Daily Revenue For Rolling
    df_Daily_Revenue_For_Rolling = df_Derive_Date_Keys_For_Rolling.groupBy('sales_day').agg(_sum(col("converted_revenue_usd")).alias('daily_revenue'), countDistinct(col("order_id")).alias('daily_orders'))

    # Step: Sort Daily Revenue (SortRows) [converted]
    # Sort Rows: Sort Daily Revenue
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Daily_Revenue = df_Daily_Revenue_For_Rolling
    _sort_df_Sort_Daily_Revenue = _sort_df_Sort_Daily_Revenue.withColumn("_sort_ci_sales_day", lower(col("sales_day").cast("string")))
    df_Sort_Daily_Revenue = _sort_df_Sort_Daily_Revenue.orderBy(col("_sort_ci_sales_day").asc_nulls_last())
    df_Sort_Daily_Revenue = df_Sort_Daily_Revenue.drop("_sort_ci_sales_day")

    # Step: Running Total And Rolling 30D Proxy (Formula) [converted]
    # Formula: Running Total And Rolling 30D Proxy
    df_Running_Total_And_Rolling_30D_Proxy = df_Sort_Daily_Revenue
    df_Running_Total_And_Rolling_30D_Proxy = df_Running_Total_And_Rolling_30D_Proxy.withColumn('formula_result', lit(None))  # empty formula

    # Step: Lookup Daily Rolling Onto Lines (StreamLookup) [failed]
    # Stream Lookup: Lookup Daily Rolling Onto Lines
    # StreamLookup 'Lookup Daily Rolling Onto Lines': no join keys — lookup join not generated
    df_Lookup_Daily_Rolling_Onto_Lines = df_Derive_Date_Keys_For_Rolling

    # Step: Unique Finance Calc Keys (Unique) [converted]
    # Unique Rows: Unique Finance Calc Keys
    # preserved.reject_duplicate_row=N error_description=''
    # Unique Rows expects sorted input in Pentaho; Spark dropDuplicates is order-independent
    # preserved.count_rows=False count_field='count' compare_fields=['order_item_id']
    df_Unique_Finance_Calc_Keys = df_Lookup_Daily_Rolling_Onto_Lines.dropDuplicates(["order_item_id"])

    # Step: MD5 Financial Row Checksum (CheckSum) [converted]
    # Add a Checksum: MD5 Financial Row Checksum
    df_MD5_Financial_Row_Checksum = df_Unique_Finance_Calc_Keys
    df_MD5_Financial_Row_Checksum = df_MD5_Financial_Row_Checksum.withColumn("finance_row_md5", md5(concat(coalesce(col("order_item_id").cast("string"), lit("")), coalesce(col("revenue").cast("string"), lit("")), coalesce(col("gross_profit").cast("string"), lit("")), coalesce(col("net_profit").cast("string"), lit("")), coalesce(col("batch_id").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['order_item_id', 'revenue', 'gross_profit', 'net_profit', 'batch_id']

    # Step: Hash Dedup Calc Output (UniqueRowsByHashSet) [converted]
    # Unique Rows (HashSet): Hash Dedup Calc Output
    # preserved.reject_duplicate_row=N error_description=''
    # preserved.store_values=True
    # preserved.count_rows=False count_field='count' compare_fields=['order_item_id', 'finance_row_md5']
    df_Hash_Dedup_Calc_Output = df_MD5_Financial_Row_Checksum.dropDuplicates(["order_item_id", "finance_row_md5"])

    # Step: Clone Calc For Dual Output (CloneRow) [converted]
    # Clone Row: Clone Calc For Dual Output
    # preserved.nr_clones=2
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='2'
    _clone_parts_df_Clone_Calc_For_Dual_Output = []
    _base_df_Clone_Calc_For_Dual_Output = df_Hash_Dedup_Calc_Output
    _orig_df_Clone_Calc_For_Dual_Output = _base_df_Clone_Calc_For_Dual_Output
    _clone_parts_df_Clone_Calc_For_Dual_Output.append(_orig_df_Clone_Calc_For_Dual_Output)
    for _ci in range(1, 2 + 1):
        _c = _base_df_Clone_Calc_For_Dual_Output
        _clone_parts_df_Clone_Calc_For_Dual_Output.append(_c)
    df_Clone_Calc_For_Dual_Output = _clone_parts_df_Clone_Calc_For_Dual_Output[0]
    for _part in _clone_parts_df_Clone_Calc_For_Dual_Output[1:]:
        df_Clone_Calc_For_Dual_Output = df_Clone_Calc_For_Dual_Output.unionByName(_part, allowMissingColumns=True)

    # Step: Select Financial Output Fields (SelectValues) [converted]
    # Select Values: Select Financial Output Fields
    df_Select_Financial_Output_Fields = df_Clone_Calc_For_Dual_Output.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("order_date").alias("order_date"), col("sales_day").alias("sales_day"), col("sales_year").alias("sales_year"), col("sales_month").alias("sales_month"), col("sales_quarter").alias("sales_quarter"), col("store_sk").alias("store_sk"), col("product_sk").alias("product_sk"), col("customer_sk").alias("customer_sk"), col("employee_sk").alias("employee_sk"), col("promotion_sk").alias("promotion_sk"), col("channel_mapped").alias("channel_mapped"), col("currency_code").alias("currency_code"), col("quantity_sold").alias("quantity_sold"), col("revenue").alias("revenue"), col("converted_revenue_usd").alias("converted_revenue_usd"), col("cogs").alias("cogs"), col("gross_profit").alias("gross_profit"), col("operating_profit").alias("operating_profit"), col("net_profit").alias("net_profit"), col("gross_margin_pct").alias("gross_margin_pct"), col("net_margin_pct").alias("net_margin_pct"), col("tax_amount").alias("tax_amount"), col("discount_final").alias("discount_amount"), col("refund_amount_final").alias("refund_amount"), col("shipping_cost_final").alias("shipping_cost"), col("inventory_cost_calc").alias("inventory_cost"), col("budget_amount").alias("budget_amount"), col("budget_variance").alias("budget_variance"), col("forecast_amount").alias("forecast_amount"), col("forecast_variance").alias("forecast_variance"), col("daily_revenue").alias("daily_revenue"), col("running_total_revenue").alias("running_total_revenue"), col("rolling_30d_revenue").alias("rolling_30d_revenue"), col("margin_band").alias("margin_band"), col("margin_route").alias("margin_route"), col("finance_row_md5").alias("finance_row_md5"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"))

    # Step: Calc Execution Stats (MemoryGroupBy) [converted]
    # Memory Group By: Calc Execution Stats
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Calc_Execution_Stats = df_Select_Financial_Output_Fields.groupBy().agg(count(lit(1)).alias('rows_calculated'), _sum(col("revenue")).alias('revenue_total'), _sum(col("gross_profit")).alias('gross_profit_total'), _sum(col("net_profit")).alias('net_profit_total'))

    # Step: Table Output Financial Calc (TableOutput) [converted]
    # Pentaho step: Table Output Financial Calc (type: TableOutput) (Pentaho schema: retail_rpt)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Table_Output_Financial_Calc = df_Select_Financial_Output_Fields.select(col('order_item_id'), col('order_id'), col('order_date'), col('sales_day'), col('sales_year'), col('sales_month'), col('sales_quarter'), col('store_sk'), col('product_sk'), col('customer_sk'), col('employee_sk'), col('promotion_sk'), col('channel_mapped'), col('currency_code'), col('quantity_sold'), col('revenue'), col('converted_revenue_usd'), col('cogs'), col('gross_profit'), col('operating_profit'), col('net_profit'), col('gross_margin_pct'), col('net_margin_pct'), col('tax_amount'), col('discount_amount'), col('refund_amount'), col('shipping_cost'), col('inventory_cost'), col('budget_amount'), col('budget_variance'), col('forecast_amount'), col('forecast_variance'), col('daily_revenue'), col('running_total_revenue'), col('rolling_30d_revenue'), col('margin_band'), col('margin_route'), col('finance_row_md5'), col('batch_id'), col('run_id'))
    df_Table_Output_Financial_Calc = _mapped_df_Table_Output_Financial_Calc
    write_delta(
        df_Table_Output_Financial_Calc,
        f"{catalog}.{schema}.rpt_financial_calculations",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_financial_calculations", mode='append')

    # Step: Write Financial Calculations (TextFileOutput) [converted]
    # Pentaho step: Write Financial Calculations (type: TextFileOutput)
    # Pentaho filename: /output/finance/calc/financial_calculations_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_day' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_month' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_quarter' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel_mapped' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_sold' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='converted_revenue_usd' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='cogs' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='operating_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='forecast_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='forecast_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='daily_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='running_total_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rolling_30d_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin_route' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='finance_row_md5' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Financial_Calculations = df_Select_Financial_Output_Fields
    _out_df_Write_Financial_Calculations = df_Write_Financial_Calculations.select('order_item_id', 'order_id', 'order_date', 'sales_day', 'sales_year', 'sales_month', 'sales_quarter', 'store_sk', 'product_sk', 'customer_sk', 'employee_sk', 'promotion_sk', 'channel_mapped', 'currency_code', 'quantity_sold', 'revenue', 'converted_revenue_usd', 'cogs', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_amount', 'budget_variance', 'forecast_amount', 'forecast_variance', 'daily_revenue', 'running_total_revenue', 'rolling_30d_revenue', 'margin_band', 'margin_route', 'finance_row_md5', 'batch_id', 'run_id')
    writer = _out_df_Write_Financial_Calculations.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/financial_calculations_.csv')

    # Step: Write Calc Execution Log (TextFileOutput) [converted]
    # Pentaho step: Write Calc Execution Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/finance/TR_Financial_Calculations_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='rows_calculated' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Calc_Execution_Log = df_Calc_Execution_Stats
    _out_df_Write_Calc_Execution_Log = df_Write_Calc_Execution_Log.select('rows_calculated', 'revenue_total', 'gross_profit_total', 'net_profit_total', 'batch_id', 'run_id')
    writer = _out_df_Write_Calc_Execution_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Financial_Calculations_.log')

    # Step: Block Financial Calc (BlockingStep) [converted]
    # Blocking Step: Block Financial Calc
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Financial_Calc = cache_for_reuse(df_Write_Calc_Execution_Log)
    _ = df_Block_Financial_Calc.count()  # synchronize: wait for all upstream rows

    # Step: Log Financial Calculations (WriteToLog) [converted]
    # Write to Log: Log Financial Calculations
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=CALC_OK | TRANS=TR_Financial_Calculations | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Financial_Calculations = logging.getLogger('pentaho.writetolog.Log_Financial_Calculations')
    _log_df_Log_Financial_Calculations.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Financial_Calculations = df_Block_Financial_Calc
    _log_rows_df_Log_Financial_Calculations = _log_df_df_Log_Financial_Calculations.take(20)
    _log_df_Log_Financial_Calculations.info('Log Financial Calculations' + ' | columns=' + str(_log_df_df_Log_Financial_Calculations.columns))
    _log_df_Log_Financial_Calculations.info('AUDIT | EVENT=CALC_OK | TRANS=TR_Financial_Calculations | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Financial_Calculations:
        _log_df_Log_Financial_Calculations.info('Log Financial Calculations' + ' | ' + str(_lr.asDict()))
    df_Log_Financial_Calculations = df_Block_Financial_Calc

    # Step: Calculations Complete (Dummy) [converted]
    # Dummy: Calculations Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Calculations_Complete = df_Log_Financial_Calculations

    log_event(_LOG, "transformation_end")
    return df_Dummy_Calculations_Complete
