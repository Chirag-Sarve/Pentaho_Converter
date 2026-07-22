"""Databricks job for jb_master."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Mapping

def _workspace_path_variants(path: Path) -> list[Path]:
    """Databricks notebook paths may omit or include the ``/Workspace`` FS prefix."""
    raw = str(path).replace("\\", "/")
    out: list[Path] = []
    seen: set[str] = set()

    def _add(p: Path) -> None:
        key = str(p).replace("\\", "/")
        if key not in seen:
            seen.add(key)
            out.append(p)

    _add(Path(raw))
    if raw.startswith("/Workspace/"):
        _add(Path(raw[len("/Workspace") :]))
    elif raw.startswith(("/Users/", "/Repos/", "/Shared/")):
        _add(Path("/Workspace" + raw))
    return out


def _is_project_root(path: Path) -> bool:
    try:
        return (
            (path / "jobs").is_dir()
            and (path / "engine").is_dir()
            and (path / "config.py").is_file()
        )
    except Exception:
        return False


def _climb_to_project_root(start: Path) -> Path | None:
    cur = start
    for _ in range(8):
        for variant in _workspace_path_variants(cur):
            if _is_project_root(variant):
                return variant
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def _notebook_path() -> str | None:
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
        return str(_nb) if _nb else None
    except Exception:
        return None


def _spark_files_root() -> Path | None:
    try:
        from pyspark.files import SparkFiles

        root = Path(SparkFiles.getRootDirectory())
        return root if root.exists() else None
    except Exception:
        return None


def _project_root() -> Path:
    """Resolve package root for local scripts, Repos, Workspace, and Job clusters."""
    anchors: list[Path] = []

    # 1) __file__ (local / Repos / Workspace Files run as .py)
    try:
        anchors.append(Path(__file__).resolve())
    except NameError:
        pass

    # 2) Databricks notebook path (Master_ETL uploaded as a notebook)
    _nb = _notebook_path()
    if _nb:
        anchors.append(Path(str(_nb)))

    # 3) SparkFiles root (job cluster --py-files / distributed artifacts)
    _sf = _spark_files_root()
    if _sf is not None:
        anchors.append(_sf)

    for anchor in anchors:
        # Walk parents_up from the file/notebook, then climb further if needed
        root = anchor
        for _ in range(2):
            root = root.parent
        found = _climb_to_project_root(root)
        if found is not None:
            return found
        found = _climb_to_project_root(anchor)
        if found is not None:
            return found

    # Last resort: only accept cwd if it (or an ancestor) is a real project root
    _cwd = Path.cwd()
    found = _climb_to_project_root(_cwd)
    if found is not None:
        return found

    print("ERROR: could not resolve project root containing jobs/, engine/, config.py")
    print("  cwd           =", _cwd)
    print("  sys.path      =", list(sys.path))
    print("  __file__      =", globals().get("__file__", "<undefined>"))
    print("  notebook path =", _nb)
    print("  SparkFiles    =", _sf)
    print("  anchors       =", [str(a) for a in anchors])
    raise ModuleNotFoundError(
        "Project root not found. Upload the full package (Master_ETL.py, config.py, "
        "engine/, jobs/) and run Master_ETL from that folder."
    )


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
logger = logging.getLogger('jb_master')

TARGET_CATALOG = config.TARGET_CATALOG
TARGET_SCHEMA = config.TARGET_SCHEMA
PENTAHO_DATA_DIR = config.PENTAHO_DATA_DIR

# Step 1 : Read Clinics
def step_01_Read_Clinics(spark):
    logging.info("Running TextFileInput: Read Clinics")
    read_clinics_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('clinic_id STRING, clinic_name STRING, district STRING, capacity INT, provider_id STRING, clinic_type STRING')
        .csv(f'{PENTAHO_DATA_DIR}/clinics.csv')
    )
    read_clinics_df = read_clinics_df.select(col('clinic_id').alias('clinic_id'), col('clinic_name').alias('clinic_name'), col('district').alias('district'), col('capacity').cast('int').alias('capacity'), col('provider_id').alias('provider_id'), col('clinic_type').alias('clinic_type'))
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_visit_enrich', step_name='Read Clinics', func_name='step_01_Read_Clinics', required_columns=[])
    log_step_dataframe(read_clinics_df, step_name='Read Clinics', phase='after', transformation='tr_visit_enrich', func_name='step_01_Read_Clinics')
    return read_clinics_df

# Step 2 : Read Patients
def step_02_Read_Patients(spark):
    logging.info("Running TextFileInput: Read Patients")
    read_patients_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('patient_id STRING, patient_name STRING, status STRING, zip_code STRING, dob STRING, notes STRING')
        .csv(f'{PENTAHO_DATA_DIR}/patients.csv')
    )
    read_patients_df = read_patients_df.select(col('patient_id').alias('patient_id'), col('patient_name').alias('patient_name'), col('status').alias('status'), col('zip_code').alias('zip_code'), col('dob').alias('dob'), col('notes').alias('notes'))
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_visit_enrich', step_name='Read Patients', func_name='step_02_Read_Patients', required_columns=[])
    log_step_dataframe(read_patients_df, step_name='Read Patients', phase='after', transformation='tr_visit_enrich', func_name='step_02_Read_Patients')
    return read_patients_df

# Step 3 : Read Providers
def step_03_Read_Providers(spark):
    logging.info("Running TextFileInput: Read Providers")
    read_providers_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('provider_id STRING, provider_name STRING, specialty STRING, license_no STRING, active_flag STRING')
        .csv(f'{PENTAHO_DATA_DIR}/providers.csv')
    )
    read_providers_df = read_providers_df.select(col('provider_id').alias('provider_id'), col('provider_name').alias('provider_name'), col('specialty').alias('specialty'), col('license_no').alias('license_no'), col('active_flag').alias('active_flag'))
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_visit_enrich', step_name='Read Providers', func_name='step_03_Read_Providers', required_columns=[])
    log_step_dataframe(read_providers_df, step_name='Read Providers', phase='after', transformation='tr_visit_enrich', func_name='step_03_Read_Providers')
    return read_providers_df

# Step 4 : Read Visits
def step_04_Read_Visits(spark):
    logging.info("Running TextFileInput: Read Visits")
    read_visits_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('visit_id STRING, patient_id STRING, clinic_id STRING, visit_date STRING, status STRING, reason_code STRING')
        .csv(f'{PENTAHO_DATA_DIR}/visits.csv')
    )
    read_visits_df = read_visits_df.select(col('visit_id').alias('visit_id'), col('patient_id').alias('patient_id'), col('clinic_id').alias('clinic_id'), col('visit_date').alias('visit_date'), col('status').alias('status'), col('reason_code').alias('reason_code'))
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_visit_enrich', step_name='Read Visits', func_name='step_04_Read_Visits', required_columns=[])
    log_step_dataframe(read_visits_df, step_name='Read Visits', phase='after', transformation='tr_visit_enrich', func_name='step_04_Read_Visits')
    return read_visits_df

# Step 5 : Select Clinic Cols
def step_05_Select_Clinic_Cols(spark, read_clinics_df):
    logging.info("Running SelectValues: Select Clinic Cols")
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_visit_enrich', step_name='Select Clinic Cols', func_name='step_05_Select_Clinic_Cols', required_columns=[])
    log_step_dataframe(read_clinics_df, step_name='Select Clinic Cols', phase='before', transformation='tr_visit_enrich', func_name='step_05_Select_Clinic_Cols')
    _sv_required = ['clinic_id', 'clinic_name', 'district', 'provider_id', 'clinic_type']
    _sv_missing = [c for c in _sv_required if c not in read_clinics_df.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Clinic Cols step (missing={_sv_missing}, available={list(read_clinics_df.columns)})")
    select_clinic_cols_df = read_clinics_df.select(col("clinic_id"), col("clinic_name"), col("district"), col("provider_id"), col("clinic_type"))
    select_clinic_cols_df = require_dataframe(select_clinic_cols_df, transformation='tr_visit_enrich', step_name='Select Clinic Cols', func_name='step_05_Select_Clinic_Cols', required_columns=[])
    log_step_dataframe(select_clinic_cols_df, step_name='Select Clinic Cols', phase='after', transformation='tr_visit_enrich', func_name='step_05_Select_Clinic_Cols')
    return select_clinic_cols_df

# Step 6 : Select Patient Cols
def step_06_Select_Patient_Cols(spark, read_patients_df):
    logging.info("Running SelectValues: Select Patient Cols")
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_visit_enrich', step_name='Select Patient Cols', func_name='step_06_Select_Patient_Cols', required_columns=[])
    log_step_dataframe(read_patients_df, step_name='Select Patient Cols', phase='before', transformation='tr_visit_enrich', func_name='step_06_Select_Patient_Cols')
    _sv_required = ['patient_id', 'patient_name', 'zip_code']
    _sv_missing = [c for c in _sv_required if c not in read_patients_df.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Patient Cols step (missing={_sv_missing}, available={list(read_patients_df.columns)})")
    select_patient_cols_df = read_patients_df.select(col("patient_id"), col("patient_name"), col("zip_code"))
    select_patient_cols_df = require_dataframe(select_patient_cols_df, transformation='tr_visit_enrich', step_name='Select Patient Cols', func_name='step_06_Select_Patient_Cols', required_columns=[])
    log_step_dataframe(select_patient_cols_df, step_name='Select Patient Cols', phase='after', transformation='tr_visit_enrich', func_name='step_06_Select_Patient_Cols')
    return select_patient_cols_df

# Step 7 : Join Patient
def step_07_Join_Patient(spark, read_visits_df, select_patient_cols_df):
    logging.info("Running MergeJoin: Join Patient")
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_visit_enrich', step_name='Join Patient', func_name='step_07_Join_Patient', required_columns=[])
    log_step_dataframe(read_visits_df, step_name='Join Patient', phase='before', transformation='tr_visit_enrich', func_name='step_07_Join_Patient')
    select_patient_cols_df = require_dataframe(select_patient_cols_df, transformation='tr_visit_enrich', step_name='Join Patient', func_name='step_07_Join_Patient', required_columns=[])
    _joined_join_patient_df = read_visits_df.join(select_patient_cols_df, on=["patient_id"], how='left')
    join_patient_df = _joined_join_patient_df
    join_patient_df = require_dataframe(join_patient_df, transformation='tr_visit_enrich', step_name='Join Patient', func_name='step_07_Join_Patient', required_columns=[])
    log_step_dataframe(join_patient_df, step_name='Join Patient', phase='after', transformation='tr_visit_enrich', func_name='step_07_Join_Patient')
    return join_patient_df

# Step 8 : Join Clinic
def step_08_Join_Clinic(spark, join_patient_df, select_clinic_cols_df):
    logging.info("Running MergeJoin: Join Clinic")
    join_patient_df = require_dataframe(join_patient_df, transformation='tr_visit_enrich', step_name='Join Clinic', func_name='step_08_Join_Clinic', required_columns=[])
    log_step_dataframe(join_patient_df, step_name='Join Clinic', phase='before', transformation='tr_visit_enrich', func_name='step_08_Join_Clinic')
    select_clinic_cols_df = require_dataframe(select_clinic_cols_df, transformation='tr_visit_enrich', step_name='Join Clinic', func_name='step_08_Join_Clinic', required_columns=[])
    _joined_join_clinic_df = join_patient_df.join(select_clinic_cols_df, on=["clinic_id"], how='left')
    join_clinic_df = _joined_join_clinic_df
    join_clinic_df = require_dataframe(join_clinic_df, transformation='tr_visit_enrich', step_name='Join Clinic', func_name='step_08_Join_Clinic', required_columns=[])
    log_step_dataframe(join_clinic_df, step_name='Join Clinic', phase='after', transformation='tr_visit_enrich', func_name='step_08_Join_Clinic')
    return join_clinic_df

# Step 9 : Lookup Provider
def step_09_Lookup_Provider(spark, join_clinic_df, read_providers_df):
    logging.info("Running StreamLookup: Lookup Provider")
    join_clinic_df = require_dataframe(join_clinic_df, transformation='tr_visit_enrich', step_name='Lookup Provider', func_name='step_09_Lookup_Provider', required_columns=[])
    log_step_dataframe(join_clinic_df, step_name='Lookup Provider', phase='before', transformation='tr_visit_enrich', func_name='step_09_Lookup_Provider')
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_visit_enrich', step_name='Lookup Provider', func_name='step_09_Lookup_Provider', required_columns=[])
    _lkp_src_lookup_provider_df = read_providers_df.filter(col('provider_id').isNotNull())
    _lkp_src_lookup_provider_df = _lkp_src_lookup_provider_df.select(col('provider_id'), col('provider_name').alias('__sl_provider_name'), col('specialty').alias('__sl_specialty'))
    _lkp_lookup_provider_df = broadcast(_lkp_src_lookup_provider_df)
    lookup_provider_df = join_clinic_df.join(_lkp_lookup_provider_df, on=["provider_id"], how='left')
    lookup_provider_df = lookup_provider_df.withColumn('attending', coalesce(col('__sl_provider_name'), lit('UNKNOWN')))
    lookup_provider_df = lookup_provider_df.drop('__sl_provider_name')
    lookup_provider_df = lookup_provider_df.withColumn('attending_specialty', col('__sl_specialty'))
    lookup_provider_df = lookup_provider_df.drop('__sl_specialty')
    lookup_provider_df = require_dataframe(lookup_provider_df, transformation='tr_visit_enrich', step_name='Lookup Provider', func_name='step_09_Lookup_Provider', required_columns=[])
    log_step_dataframe(lookup_provider_df, step_name='Lookup Provider', phase='after', transformation='tr_visit_enrich', func_name='step_09_Lookup_Provider')
    return lookup_provider_df

# Step 10 : Map Visit Status
def step_10_Map_Visit_Status(spark, lookup_provider_df):
    logging.info("Running ValueMapper: Map Visit Status")
    lookup_provider_df = require_dataframe(lookup_provider_df, transformation='tr_visit_enrich', step_name='Map Visit Status', func_name='step_10_Map_Visit_Status', required_columns=[])
    log_step_dataframe(lookup_provider_df, step_name='Map Visit Status', phase='before', transformation='tr_visit_enrich', func_name='step_10_Map_Visit_Status')
    map_visit_status_df = lookup_provider_df.withColumn("status_label", when((col("status") == lit('C')), lit('COMPLETED')).when((col("status") == lit('S')), lit('SCHEDULED')).when((col("status") == lit('W')), lit('WAITING')).when((col("status") == lit('N')), lit('NO_SHOW')).when((col("status").isNull() | (col("status") == lit(''))), col("status")).otherwise(lit('OTHER')))
    map_visit_status_df = require_dataframe(map_visit_status_df, transformation='tr_visit_enrich', step_name='Map Visit Status', func_name='step_10_Map_Visit_Status', required_columns=[])
    log_step_dataframe(map_visit_status_df, step_name='Map Visit Status', phase='after', transformation='tr_visit_enrich', func_name='step_10_Map_Visit_Status')
    return map_visit_status_df

# Step 11 : Map Reason
def step_11_Map_Reason(spark, map_visit_status_df):
    logging.info("Running ValueMapper: Map Reason")
    map_visit_status_df = require_dataframe(map_visit_status_df, transformation='tr_visit_enrich', step_name='Map Reason', func_name='step_11_Map_Reason', required_columns=[])
    log_step_dataframe(map_visit_status_df, step_name='Map Reason', phase='before', transformation='tr_visit_enrich', func_name='step_11_Map_Reason')
    map_reason_df = map_visit_status_df.withColumn("reason_label", when((col("reason_code") == lit('CHK')), lit('CHECKUP')).when((col("reason_code") == lit('VAX')), lit('VACCINATION')).when((col("reason_code") == lit('LAB')), lit('LAB_WORK')).when((col("reason_code") == lit('CHR')), lit('CHRONIC_CARE')).when((col("reason_code") == lit('FLW')), lit('FOLLOW_UP')).when((col("reason_code") == lit('PED')), lit('PEDIATRICS')).when((col("reason_code") == lit('PRE')), lit('PRENATAL')).when((col("reason_code") == lit('CTF')), lit('CONTACT_TRACING')).when((col("reason_code").isNull() | (col("reason_code") == lit(''))), col("reason_code")).otherwise(lit('OTHER')))
    map_reason_df = require_dataframe(map_reason_df, transformation='tr_visit_enrich', step_name='Map Reason', func_name='step_11_Map_Reason', required_columns=[])
    log_step_dataframe(map_reason_df, step_name='Map Reason', phase='after', transformation='tr_visit_enrich', func_name='step_11_Map_Reason')
    return map_reason_df

# Step 12 : Add Report Period
def step_12_Add_Report_Period(spark, map_reason_df):
    logging.info("Running Constant: Add Report Period")
    map_reason_df = require_dataframe(map_reason_df, transformation='tr_visit_enrich', step_name='Add Report Period', func_name='step_12_Add_Report_Period', required_columns=[])
    log_step_dataframe(map_reason_df, step_name='Add Report Period', phase='before', transformation='tr_visit_enrich', func_name='step_12_Add_Report_Period')
    add_report_period_df = map_reason_df
    add_report_period_df = add_report_period_df.withColumn("report_period", lit('2026-Q2'))
    add_report_period_df = require_dataframe(add_report_period_df, transformation='tr_visit_enrich', step_name='Add Report Period', func_name='step_12_Add_Report_Period', required_columns=[])
    log_step_dataframe(add_report_period_df, step_name='Add Report Period', phase='after', transformation='tr_visit_enrich', func_name='step_12_Add_Report_Period')
    return add_report_period_df

# Step 13 : Add Run Info
def step_13_Add_Run_Info(spark, add_report_period_df):
    logging.info("Running SystemInfo: Add Run Info")
    add_report_period_df = require_dataframe(add_report_period_df, transformation='tr_visit_enrich', step_name='Add Run Info', func_name='step_13_Add_Run_Info', required_columns=[])
    log_step_dataframe(add_report_period_df, step_name='Add Run Info', phase='before', transformation='tr_visit_enrich', func_name='step_13_Add_Run_Info')
    add_run_info_df = add_report_period_df
    add_run_info_df = add_run_info_df.withColumn("run_date", current_date())
    add_run_info_df = add_run_info_df.withColumn("run_ts", current_date())
    add_run_info_df = require_dataframe(add_run_info_df, transformation='tr_visit_enrich', step_name='Add Run Info', func_name='step_13_Add_Run_Info', required_columns=[])
    log_step_dataframe(add_run_info_df, step_name='Add Run Info', phase='after', transformation='tr_visit_enrich', func_name='step_13_Add_Run_Info')
    return add_run_info_df

# Step 14 : Write Enriched
def step_14_Write_Enriched(spark, add_run_info_df):
    logging.info("Running TextFileOutput: Write Enriched")
    add_run_info_df = require_dataframe(add_run_info_df, transformation='tr_visit_enrich', step_name='Write Enriched', func_name='step_14_Write_Enriched', required_columns=[])
    log_step_dataframe(add_run_info_df, step_name='Write Enriched', phase='before', transformation='tr_visit_enrich', func_name='step_14_Write_Enriched')
    write_enriched_df = add_run_info_df
    _tfo_declared = ['visit_id', 'patient_id', 'patient_name', 'zip_code', 'clinic_id', 'clinic_name', 'district', 'attending', 'status_label', 'reason_label', 'report_period', 'run_date', 'run_ts']
    _tfo_missing = [c for c in _tfo_declared if c not in write_enriched_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Enriched step (missing={_tfo_missing}, available={list(write_enriched_df.columns)})")
    selected_output_df = write_enriched_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/visits_enriched_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/visits_enriched_out.csv'
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
    write_enriched_df = require_dataframe(write_enriched_df, transformation='tr_visit_enrich', step_name='Write Enriched', func_name='step_14_Write_Enriched', required_columns=[])
    log_step_dataframe(write_enriched_df, step_name='Write Enriched', phase='after', transformation='tr_visit_enrich', func_name='step_14_Write_Enriched')
    return write_enriched_df

def run_tr_visit_enrich(spark, config=None):
    """Run tr_visit_enrich."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_visit_enrich (tr_visit_enrich.ktr)")

    read_clinics_df = step_01_Read_Clinics(spark)
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_visit_enrich', step_name='Read Clinics', func_name='step_01_Read_Clinics', required_columns=[])
    read_patients_df = step_02_Read_Patients(spark)
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_visit_enrich', step_name='Read Patients', func_name='step_02_Read_Patients', required_columns=[])
    read_providers_df = step_03_Read_Providers(spark)
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_visit_enrich', step_name='Read Providers', func_name='step_03_Read_Providers', required_columns=[])
    read_visits_df = step_04_Read_Visits(spark)
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_visit_enrich', step_name='Read Visits', func_name='step_04_Read_Visits', required_columns=[])
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_visit_enrich', step_name='Select Clinic Cols', func_name='step_05_Select_Clinic_Cols', required_columns=[])
    select_clinic_cols_df = step_05_Select_Clinic_Cols(spark, read_clinics_df)
    select_clinic_cols_df = require_dataframe(select_clinic_cols_df, transformation='tr_visit_enrich', step_name='Select Clinic Cols', func_name='step_05_Select_Clinic_Cols', required_columns=[])
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_visit_enrich', step_name='Select Patient Cols', func_name='step_06_Select_Patient_Cols', required_columns=[])
    select_patient_cols_df = step_06_Select_Patient_Cols(spark, read_patients_df)
    select_patient_cols_df = require_dataframe(select_patient_cols_df, transformation='tr_visit_enrich', step_name='Select Patient Cols', func_name='step_06_Select_Patient_Cols', required_columns=[])
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_visit_enrich', step_name='Join Patient', func_name='step_07_Join_Patient', required_columns=[])
    select_patient_cols_df = require_dataframe(select_patient_cols_df, transformation='tr_visit_enrich', step_name='Join Patient', func_name='step_07_Join_Patient', required_columns=[])
    join_patient_df = step_07_Join_Patient(spark, read_visits_df, select_patient_cols_df)
    join_patient_df = require_dataframe(join_patient_df, transformation='tr_visit_enrich', step_name='Join Patient', func_name='step_07_Join_Patient', required_columns=[])
    join_patient_df = require_dataframe(join_patient_df, transformation='tr_visit_enrich', step_name='Join Clinic', func_name='step_08_Join_Clinic', required_columns=[])
    select_clinic_cols_df = require_dataframe(select_clinic_cols_df, transformation='tr_visit_enrich', step_name='Join Clinic', func_name='step_08_Join_Clinic', required_columns=[])
    join_clinic_df = step_08_Join_Clinic(spark, join_patient_df, select_clinic_cols_df)
    join_clinic_df = require_dataframe(join_clinic_df, transformation='tr_visit_enrich', step_name='Join Clinic', func_name='step_08_Join_Clinic', required_columns=[])
    join_clinic_df = require_dataframe(join_clinic_df, transformation='tr_visit_enrich', step_name='Lookup Provider', func_name='step_09_Lookup_Provider', required_columns=[])
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_visit_enrich', step_name='Lookup Provider', func_name='step_09_Lookup_Provider', required_columns=[])
    lookup_provider_df = step_09_Lookup_Provider(spark, join_clinic_df, read_providers_df)
    lookup_provider_df = require_dataframe(lookup_provider_df, transformation='tr_visit_enrich', step_name='Lookup Provider', func_name='step_09_Lookup_Provider', required_columns=[])
    lookup_provider_df = require_dataframe(lookup_provider_df, transformation='tr_visit_enrich', step_name='Map Visit Status', func_name='step_10_Map_Visit_Status', required_columns=[])
    map_visit_status_df = step_10_Map_Visit_Status(spark, lookup_provider_df)
    map_visit_status_df = require_dataframe(map_visit_status_df, transformation='tr_visit_enrich', step_name='Map Visit Status', func_name='step_10_Map_Visit_Status', required_columns=[])
    map_visit_status_df = require_dataframe(map_visit_status_df, transformation='tr_visit_enrich', step_name='Map Reason', func_name='step_11_Map_Reason', required_columns=[])
    map_reason_df = step_11_Map_Reason(spark, map_visit_status_df)
    map_reason_df = require_dataframe(map_reason_df, transformation='tr_visit_enrich', step_name='Map Reason', func_name='step_11_Map_Reason', required_columns=[])
    map_reason_df = require_dataframe(map_reason_df, transformation='tr_visit_enrich', step_name='Add Report Period', func_name='step_12_Add_Report_Period', required_columns=[])
    add_report_period_df = step_12_Add_Report_Period(spark, map_reason_df)
    add_report_period_df = require_dataframe(add_report_period_df, transformation='tr_visit_enrich', step_name='Add Report Period', func_name='step_12_Add_Report_Period', required_columns=[])
    add_report_period_df = require_dataframe(add_report_period_df, transformation='tr_visit_enrich', step_name='Add Run Info', func_name='step_13_Add_Run_Info', required_columns=[])
    add_run_info_df = step_13_Add_Run_Info(spark, add_report_period_df)
    add_run_info_df = require_dataframe(add_run_info_df, transformation='tr_visit_enrich', step_name='Add Run Info', func_name='step_13_Add_Run_Info', required_columns=[])
    add_run_info_df = require_dataframe(add_run_info_df, transformation='tr_visit_enrich', step_name='Write Enriched', func_name='step_14_Write_Enriched', required_columns=[])
    write_enriched_df = step_14_Write_Enriched(spark, add_run_info_df)
    write_enriched_df = require_dataframe(write_enriched_df, transformation='tr_visit_enrich', step_name='Write Enriched', func_name='step_14_Write_Enriched', required_columns=[])

    logging.info("Finished transformation: tr_visit_enrich")
    return write_enriched_df

# Step 15 : Read Labs
def step_15_Read_Labs(spark):
    logging.info("Running TextFileInput: Read Labs")
    read_labs_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('lab_id STRING, visit_id STRING, patient_id STRING, clinic_id STRING, test_name STRING, result_value DOUBLE, ref_high DOUBLE, units STRING')
        .csv(f'{PENTAHO_DATA_DIR}/lab_results.csv')
    )
    read_labs_df = read_labs_df.select(col('lab_id').alias('lab_id'), col('visit_id').alias('visit_id'), col('patient_id').alias('patient_id'), col('clinic_id').alias('clinic_id'), col('test_name').alias('test_name'), col('result_value').cast('double').alias('result_value'), col('ref_high').cast('double').alias('ref_high'), col('units').alias('units'))
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_lab_rollup', step_name='Read Labs', func_name='step_15_Read_Labs', required_columns=[])
    log_step_dataframe(read_labs_df, step_name='Read Labs', phase='after', transformation='tr_lab_rollup', func_name='step_15_Read_Labs')
    return read_labs_df

# Step 16 : Calc Over Ref
def step_16_Calc_Over_Ref(spark, read_labs_df):
    logging.info("Running Calculator: Calc Over Ref")
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_lab_rollup', step_name='Calc Over Ref', func_name='step_16_Calc_Over_Ref', required_columns=[])
    log_step_dataframe(read_labs_df, step_name='Calc Over Ref', phase='before', transformation='tr_lab_rollup', func_name='step_16_Calc_Over_Ref')
    calc_over_ref_df = read_labs_df
    calc_over_ref_df = calc_over_ref_df.withColumn("over_ref", ((col("result_value") - col("ref_high"))).cast('double'))
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_lab_rollup', step_name='Calc Over Ref', func_name='step_16_Calc_Over_Ref', required_columns=[])
    log_step_dataframe(calc_over_ref_df, step_name='Calc Over Ref', phase='after', transformation='tr_lab_rollup', func_name='step_16_Calc_Over_Ref')
    return calc_over_ref_df

# Step 17 : Sort By Visit
def step_17_Sort_By_Visit(spark, calc_over_ref_df):
    logging.info("Running SortRows: Sort By Visit")
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_lab_rollup', step_name='Sort By Visit', func_name='step_17_Sort_By_Visit', required_columns=[])
    log_step_dataframe(calc_over_ref_df, step_name='Sort By Visit', phase='before', transformation='tr_lab_rollup', func_name='step_17_Sort_By_Visit')
    sort_by_visit_df = calc_over_ref_df.orderBy(col("visit_id").asc_nulls_last(), col("patient_id").asc_nulls_last())
    sort_by_visit_df = require_dataframe(sort_by_visit_df, transformation='tr_lab_rollup', step_name='Sort By Visit', func_name='step_17_Sort_By_Visit', required_columns=[])
    log_step_dataframe(sort_by_visit_df, step_name='Sort By Visit', phase='after', transformation='tr_lab_rollup', func_name='step_17_Sort_By_Visit')
    return sort_by_visit_df

# Step 18 : Agg By Visit
def step_18_Agg_By_Visit(spark, sort_by_visit_df):
    logging.info("Running GroupBy: Agg By Visit")
    sort_by_visit_df = require_dataframe(sort_by_visit_df, transformation='tr_lab_rollup', step_name='Agg By Visit', func_name='step_18_Agg_By_Visit', required_columns=[])
    log_step_dataframe(sort_by_visit_df, step_name='Agg By Visit', phase='before', transformation='tr_lab_rollup', func_name='step_18_Agg_By_Visit')
    agg_by_visit_df = sort_by_visit_df.groupBy('visit_id', 'patient_id', 'clinic_id').agg(avg(col("result_value")).alias('avg_result'), _max(col("over_ref")).alias('max_over_ref'), count(lit(1)).alias('test_count'), _max(col("result_value")).alias('max_result'), _min(col("result_value")).alias('min_result'))
    agg_by_visit_df = require_dataframe(agg_by_visit_df, transformation='tr_lab_rollup', step_name='Agg By Visit', func_name='step_18_Agg_By_Visit', required_columns=[])
    log_step_dataframe(agg_by_visit_df, step_name='Agg By Visit', phase='after', transformation='tr_lab_rollup', func_name='step_18_Agg_By_Visit')
    return agg_by_visit_df

# Step 19 : Add Row Num
def step_19_Add_Row_Num(spark, agg_by_visit_df):
    logging.info("Running Sequence: Add Row Num")
    agg_by_visit_df = require_dataframe(agg_by_visit_df, transformation='tr_lab_rollup', step_name='Add Row Num', func_name='step_19_Add_Row_Num', required_columns=[])
    log_step_dataframe(agg_by_visit_df, step_name='Add Row Num', phase='before', transformation='tr_lab_rollup', func_name='step_19_Add_Row_Num')
    _w_seq_add_row_num_df = Window.orderBy(monotonically_increasing_id())
    add_row_num_df = agg_by_visit_df.withColumn("row_num", lit(1) + ((row_number().over(_w_seq_add_row_num_df) - lit(1)) % lit(999999999)) * lit(1))
    add_row_num_df = require_dataframe(add_row_num_df, transformation='tr_lab_rollup', step_name='Add Row Num', func_name='step_19_Add_Row_Num', required_columns=[])
    log_step_dataframe(add_row_num_df, step_name='Add Row Num', phase='after', transformation='tr_lab_rollup', func_name='step_19_Add_Row_Num')
    return add_row_num_df

# Step 20 : Select Fields
def step_20_Select_Fields(spark, add_row_num_df):
    logging.info("Running SelectValues: Select Fields")
    add_row_num_df = require_dataframe(add_row_num_df, transformation='tr_lab_rollup', step_name='Select Fields', func_name='step_20_Select_Fields', required_columns=[])
    log_step_dataframe(add_row_num_df, step_name='Select Fields', phase='before', transformation='tr_lab_rollup', func_name='step_20_Select_Fields')
    _sv_required = ['row_num', 'visit_id', 'patient_id', 'clinic_id', 'avg_result', 'max_over_ref', 'test_count', 'max_result', 'min_result']
    _sv_missing = [c for c in _sv_required if c not in add_row_num_df.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Fields step (missing={_sv_missing}, available={list(add_row_num_df.columns)})")
    select_fields_df = add_row_num_df.select(col("row_num"), col("visit_id"), col("patient_id"), col("clinic_id"), col("avg_result"), col("max_over_ref"), col("test_count"), col("max_result"), col("min_result"))
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_lab_rollup', step_name='Select Fields', func_name='step_20_Select_Fields', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Select Fields', phase='after', transformation='tr_lab_rollup', func_name='step_20_Select_Fields')
    return select_fields_df

# Step 21 : Write Rollup
def step_21_Write_Rollup(spark, select_fields_df):
    logging.info("Running TextFileOutput: Write Rollup")
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_lab_rollup', step_name='Write Rollup', func_name='step_21_Write_Rollup', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Write Rollup', phase='before', transformation='tr_lab_rollup', func_name='step_21_Write_Rollup')
    write_rollup_df = select_fields_df
    _tfo_declared = ['row_num', 'visit_id', 'patient_id', 'clinic_id', 'avg_result', 'max_over_ref', 'test_count', 'max_result', 'min_result']
    _tfo_missing = [c for c in _tfo_declared if c not in write_rollup_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Rollup step (missing={_tfo_missing}, available={list(write_rollup_df.columns)})")
    selected_output_df = write_rollup_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/labs_rollup_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/labs_rollup_out.csv'
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
    write_rollup_df = require_dataframe(write_rollup_df, transformation='tr_lab_rollup', step_name='Write Rollup', func_name='step_21_Write_Rollup', required_columns=[])
    log_step_dataframe(write_rollup_df, step_name='Write Rollup', phase='after', transformation='tr_lab_rollup', func_name='step_21_Write_Rollup')
    return write_rollup_df

def run_tr_lab_rollup(spark, config=None):
    """Run tr_lab_rollup."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_lab_rollup (tr_lab_rollup.ktr)")

    read_labs_df = step_15_Read_Labs(spark)
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_lab_rollup', step_name='Read Labs', func_name='step_15_Read_Labs', required_columns=[])
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_lab_rollup', step_name='Calc Over Ref', func_name='step_16_Calc_Over_Ref', required_columns=[])
    calc_over_ref_df = step_16_Calc_Over_Ref(spark, read_labs_df)
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_lab_rollup', step_name='Calc Over Ref', func_name='step_16_Calc_Over_Ref', required_columns=[])
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_lab_rollup', step_name='Sort By Visit', func_name='step_17_Sort_By_Visit', required_columns=[])
    sort_by_visit_df = step_17_Sort_By_Visit(spark, calc_over_ref_df)
    sort_by_visit_df = require_dataframe(sort_by_visit_df, transformation='tr_lab_rollup', step_name='Sort By Visit', func_name='step_17_Sort_By_Visit', required_columns=[])
    sort_by_visit_df = require_dataframe(sort_by_visit_df, transformation='tr_lab_rollup', step_name='Agg By Visit', func_name='step_18_Agg_By_Visit', required_columns=[])
    agg_by_visit_df = step_18_Agg_By_Visit(spark, sort_by_visit_df)
    agg_by_visit_df = require_dataframe(agg_by_visit_df, transformation='tr_lab_rollup', step_name='Agg By Visit', func_name='step_18_Agg_By_Visit', required_columns=[])
    agg_by_visit_df = require_dataframe(agg_by_visit_df, transformation='tr_lab_rollup', step_name='Add Row Num', func_name='step_19_Add_Row_Num', required_columns=[])
    add_row_num_df = step_19_Add_Row_Num(spark, agg_by_visit_df)
    add_row_num_df = require_dataframe(add_row_num_df, transformation='tr_lab_rollup', step_name='Add Row Num', func_name='step_19_Add_Row_Num', required_columns=[])
    add_row_num_df = require_dataframe(add_row_num_df, transformation='tr_lab_rollup', step_name='Select Fields', func_name='step_20_Select_Fields', required_columns=[])
    select_fields_df = step_20_Select_Fields(spark, add_row_num_df)
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_lab_rollup', step_name='Select Fields', func_name='step_20_Select_Fields', required_columns=[])
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_lab_rollup', step_name='Write Rollup', func_name='step_21_Write_Rollup', required_columns=[])
    write_rollup_df = step_21_Write_Rollup(spark, select_fields_df)
    write_rollup_df = require_dataframe(write_rollup_df, transformation='tr_lab_rollup', step_name='Write Rollup', func_name='step_21_Write_Rollup', required_columns=[])

    logging.info("Finished transformation: tr_lab_rollup")
    return write_rollup_df

# Step 22 : Read Vaccinations
def step_22_Read_Vaccinations(spark):
    logging.info("Running TextFileInput: Read Vaccinations")
    read_vaccinations_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('vax_id STRING, patient_id STRING, clinic_id STRING, vax_date STRING, status STRING, vaccine_code STRING, dose_no INT')
        .csv(f'{PENTAHO_DATA_DIR}/vaccinations.csv')
    )
    read_vaccinations_df = read_vaccinations_df.select(col('vax_id').alias('vax_id'), col('patient_id').alias('patient_id'), col('clinic_id').alias('clinic_id'), col('vax_date').alias('vax_date'), col('status').alias('status'), col('vaccine_code').alias('vaccine_code'), col('dose_no').cast('int').alias('dose_no'))
    read_vaccinations_df = require_dataframe(read_vaccinations_df, transformation='tr_vaccination_route', step_name='Read Vaccinations', func_name='step_22_Read_Vaccinations', required_columns=[])
    log_step_dataframe(read_vaccinations_df, step_name='Read Vaccinations', phase='after', transformation='tr_vaccination_route', func_name='step_22_Read_Vaccinations')
    return read_vaccinations_df

# Step 23 : Map Status Code
def step_23_Map_Status_Code(spark, read_vaccinations_df):
    logging.info("Running ValueMapper: Map Status Code")
    read_vaccinations_df = require_dataframe(read_vaccinations_df, transformation='tr_vaccination_route', step_name='Map Status Code', func_name='step_23_Map_Status_Code', required_columns=[])
    log_step_dataframe(read_vaccinations_df, step_name='Map Status Code', phase='before', transformation='tr_vaccination_route', func_name='step_23_Map_Status_Code')
    map_status_code_df = read_vaccinations_df.withColumn("status_label", when((col("status") == lit('A')), lit('ADMINISTERED')).when((col("status") == lit('B')), lit('REFUSED')).when((col("status") == lit('C')), lit('CONTRAINDICATED')).when((col("status").isNull() | (col("status") == lit(''))), col("status")).otherwise(lit('UNKNOWN')))
    map_status_code_df = require_dataframe(map_status_code_df, transformation='tr_vaccination_route', step_name='Map Status Code', func_name='step_23_Map_Status_Code', required_columns=[])
    log_step_dataframe(map_status_code_df, step_name='Map Status Code', phase='after', transformation='tr_vaccination_route', func_name='step_23_Map_Status_Code')
    return map_status_code_df

# Step 24 : Route Administered
def step_24_Route_Administered(spark, map_status_code_df):
    logging.info("Running FilterRows: Route Administered")
    map_status_code_df = require_dataframe(map_status_code_df, transformation='tr_vaccination_route', step_name='Route Administered', func_name='step_24_Route_Administered', required_columns=[])
    log_step_dataframe(map_status_code_df, step_name='Route Administered', phase='before', transformation='tr_vaccination_route', func_name='step_24_Route_Administered')
    _filter_required = ['status']
    _filter_missing = [c for c in _filter_required if c not in map_status_code_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Route Administered step (missing={_filter_missing}, available={list(map_status_code_df.columns)})")
    df_Label_Administered = map_status_code_df.filter((col("status") == lit('A')))
    df_Label_Exception = map_status_code_df.filter(~((col("status") == lit('A'))))
    route_administered_df = df_Label_Administered
    route_administered_df = require_dataframe(route_administered_df, transformation='tr_vaccination_route', step_name='Route Administered', func_name='step_24_Route_Administered', required_columns=[])
    log_step_dataframe(route_administered_df, step_name='Route Administered', phase='after', transformation='tr_vaccination_route', func_name='step_24_Route_Administered')
    return route_administered_df, df_Label_Administered, df_Label_Exception

# Step 25 : Label Administered
def step_25_Label_Administered(spark, df_Label_Administered):
    logging.info("Running Constant: Label Administered")
    df_Label_Administered = require_dataframe(df_Label_Administered, transformation='tr_vaccination_route', step_name='Label Administered', func_name='step_25_Label_Administered', required_columns=[])
    log_step_dataframe(df_Label_Administered, step_name='Label Administered', phase='before', transformation='tr_vaccination_route', func_name='step_25_Label_Administered')
    label_administered_df = df_Label_Administered
    label_administered_df = label_administered_df.withColumn("route_bucket", lit('OK_PATH'))
    label_administered_df = require_dataframe(label_administered_df, transformation='tr_vaccination_route', step_name='Label Administered', func_name='step_25_Label_Administered', required_columns=[])
    log_step_dataframe(label_administered_df, step_name='Label Administered', phase='after', transformation='tr_vaccination_route', func_name='step_25_Label_Administered')
    return label_administered_df

# Step 26 : Label Exception
def step_26_Label_Exception(spark, df_Label_Exception):
    logging.info("Running Constant: Label Exception")
    df_Label_Exception = require_dataframe(df_Label_Exception, transformation='tr_vaccination_route', step_name='Label Exception', func_name='step_26_Label_Exception', required_columns=[])
    log_step_dataframe(df_Label_Exception, step_name='Label Exception', phase='before', transformation='tr_vaccination_route', func_name='step_26_Label_Exception')
    label_exception_df = df_Label_Exception
    label_exception_df = label_exception_df.withColumn("route_bucket", lit('EXCEPTION_PATH'))
    label_exception_df = require_dataframe(label_exception_df, transformation='tr_vaccination_route', step_name='Label Exception', func_name='step_26_Label_Exception', required_columns=[])
    log_step_dataframe(label_exception_df, step_name='Label Exception', phase='after', transformation='tr_vaccination_route', func_name='step_26_Label_Exception')
    return label_exception_df

# Step 27 : Merge Streams
def step_27_Merge_Streams(spark, label_administered_df, label_exception_df):
    logging.info("Running Append: Merge Streams")
    label_administered_df = require_dataframe(label_administered_df, transformation='tr_vaccination_route', step_name='Merge Streams', func_name='step_27_Merge_Streams', required_columns=[])
    log_step_dataframe(label_administered_df, step_name='Merge Streams', phase='before', transformation='tr_vaccination_route', func_name='step_27_Merge_Streams')
    label_exception_df = require_dataframe(label_exception_df, transformation='tr_vaccination_route', step_name='Merge Streams', func_name='step_27_Merge_Streams', required_columns=[])
    merge_streams_df = label_administered_df.unionByName(label_exception_df, allowMissingColumns=True)
    merge_streams_df = require_dataframe(merge_streams_df, transformation='tr_vaccination_route', step_name='Merge Streams', func_name='step_27_Merge_Streams', required_columns=[])
    log_step_dataframe(merge_streams_df, step_name='Merge Streams', phase='after', transformation='tr_vaccination_route', func_name='step_27_Merge_Streams')
    return merge_streams_df

# Step 28 : Write Routed
def step_28_Write_Routed(spark, merge_streams_df):
    logging.info("Running TextFileOutput: Write Routed")
    merge_streams_df = require_dataframe(merge_streams_df, transformation='tr_vaccination_route', step_name='Write Routed', func_name='step_28_Write_Routed', required_columns=[])
    log_step_dataframe(merge_streams_df, step_name='Write Routed', phase='before', transformation='tr_vaccination_route', func_name='step_28_Write_Routed')
    write_routed_df = merge_streams_df
    _tfo_declared = ['vax_id', 'patient_id', 'clinic_id', 'vax_date', 'status', 'status_label', 'vaccine_code', 'dose_no', 'route_bucket']
    _tfo_missing = [c for c in _tfo_declared if c not in write_routed_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Routed step (missing={_tfo_missing}, available={list(write_routed_df.columns)})")
    selected_output_df = write_routed_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/vaccinations_routed_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/vaccinations_routed_out.csv'
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
    write_routed_df = require_dataframe(write_routed_df, transformation='tr_vaccination_route', step_name='Write Routed', func_name='step_28_Write_Routed', required_columns=[])
    log_step_dataframe(write_routed_df, step_name='Write Routed', phase='after', transformation='tr_vaccination_route', func_name='step_28_Write_Routed')
    return write_routed_df

def run_tr_vaccination_route(spark, config=None):
    """Run tr_vaccination_route."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_vaccination_route (tr_vaccination_route.ktr)")

    read_vaccinations_df = step_22_Read_Vaccinations(spark)
    read_vaccinations_df = require_dataframe(read_vaccinations_df, transformation='tr_vaccination_route', step_name='Read Vaccinations', func_name='step_22_Read_Vaccinations', required_columns=[])
    read_vaccinations_df = require_dataframe(read_vaccinations_df, transformation='tr_vaccination_route', step_name='Map Status Code', func_name='step_23_Map_Status_Code', required_columns=[])
    map_status_code_df = step_23_Map_Status_Code(spark, read_vaccinations_df)
    map_status_code_df = require_dataframe(map_status_code_df, transformation='tr_vaccination_route', step_name='Map Status Code', func_name='step_23_Map_Status_Code', required_columns=[])
    map_status_code_df = require_dataframe(map_status_code_df, transformation='tr_vaccination_route', step_name='Route Administered', func_name='step_24_Route_Administered', required_columns=[])
    route_administered_df, df_Label_Administered, df_Label_Exception = step_24_Route_Administered(spark, map_status_code_df)
    route_administered_df = require_dataframe(route_administered_df, transformation='tr_vaccination_route', step_name='Route Administered', func_name='step_24_Route_Administered', required_columns=[])
    df_Label_Administered = require_dataframe(df_Label_Administered, transformation='tr_vaccination_route', step_name='Label Administered', func_name='step_25_Label_Administered', required_columns=[])
    label_administered_df = step_25_Label_Administered(spark, df_Label_Administered)
    label_administered_df = require_dataframe(label_administered_df, transformation='tr_vaccination_route', step_name='Label Administered', func_name='step_25_Label_Administered', required_columns=[])
    df_Label_Exception = require_dataframe(df_Label_Exception, transformation='tr_vaccination_route', step_name='Label Exception', func_name='step_26_Label_Exception', required_columns=[])
    label_exception_df = step_26_Label_Exception(spark, df_Label_Exception)
    label_exception_df = require_dataframe(label_exception_df, transformation='tr_vaccination_route', step_name='Label Exception', func_name='step_26_Label_Exception', required_columns=[])
    label_administered_df = require_dataframe(label_administered_df, transformation='tr_vaccination_route', step_name='Merge Streams', func_name='step_27_Merge_Streams', required_columns=[])
    label_exception_df = require_dataframe(label_exception_df, transformation='tr_vaccination_route', step_name='Merge Streams', func_name='step_27_Merge_Streams', required_columns=[])
    merge_streams_df = step_27_Merge_Streams(spark, label_administered_df, label_exception_df)
    merge_streams_df = require_dataframe(merge_streams_df, transformation='tr_vaccination_route', step_name='Merge Streams', func_name='step_27_Merge_Streams', required_columns=[])
    merge_streams_df = require_dataframe(merge_streams_df, transformation='tr_vaccination_route', step_name='Write Routed', func_name='step_28_Write_Routed', required_columns=[])
    write_routed_df = step_28_Write_Routed(spark, merge_streams_df)
    write_routed_df = require_dataframe(write_routed_df, transformation='tr_vaccination_route', step_name='Write Routed', func_name='step_28_Write_Routed', required_columns=[])

    logging.info("Finished transformation: tr_vaccination_route")
    return write_routed_df

# Step 29 : Read Labs
def step_29_Read_Labs(spark):
    logging.info("Running TextFileInput: Read Labs")
    read_labs_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('lab_id STRING, visit_id STRING, patient_id STRING, clinic_id STRING, test_name STRING, result_value DOUBLE, ref_high DOUBLE, units STRING')
        .csv(f'{PENTAHO_DATA_DIR}/lab_results.csv')
    )
    read_labs_df = read_labs_df.select(col('lab_id').alias('lab_id'), col('visit_id').alias('visit_id'), col('patient_id').alias('patient_id'), col('clinic_id').alias('clinic_id'), col('test_name').alias('test_name'), col('result_value').cast('double').alias('result_value'), col('ref_high').cast('double').alias('ref_high'), col('units').alias('units'))
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_outbreak_flag', step_name='Read Labs', func_name='step_29_Read_Labs', required_columns=[])
    log_step_dataframe(read_labs_df, step_name='Read Labs', phase='after', transformation='tr_outbreak_flag', func_name='step_29_Read_Labs')
    return read_labs_df

# Step 30 : Read Visits
def step_30_Read_Visits(spark):
    logging.info("Running TextFileInput: Read Visits")
    read_visits_df = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('visit_id STRING, patient_id STRING, clinic_id STRING, visit_date STRING, status STRING, reason_code STRING')
        .csv(f'{PENTAHO_DATA_DIR}/visits.csv')
    )
    read_visits_df = read_visits_df.select(col('visit_id').alias('visit_id'), col('patient_id').alias('patient_id'), col('clinic_id').alias('clinic_id'), col('visit_date').alias('visit_date'), col('status').alias('status'), col('reason_code').alias('reason_code'))
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_outbreak_flag', step_name='Read Visits', func_name='step_30_Read_Visits', required_columns=[])
    log_step_dataframe(read_visits_df, step_name='Read Visits', phase='after', transformation='tr_outbreak_flag', func_name='step_30_Read_Visits')
    return read_visits_df

# Step 31 : Calc Over Ref
def step_31_Calc_Over_Ref(spark, read_labs_df):
    logging.info("Running Calculator: Calc Over Ref")
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_outbreak_flag', step_name='Calc Over Ref', func_name='step_31_Calc_Over_Ref', required_columns=[])
    log_step_dataframe(read_labs_df, step_name='Calc Over Ref', phase='before', transformation='tr_outbreak_flag', func_name='step_31_Calc_Over_Ref')
    calc_over_ref_df = read_labs_df
    calc_over_ref_df = calc_over_ref_df.withColumn("over_ref", ((col("result_value") - col("ref_high"))).cast('double'))
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_outbreak_flag', step_name='Calc Over Ref', func_name='step_31_Calc_Over_Ref', required_columns=[])
    log_step_dataframe(calc_over_ref_df, step_name='Calc Over Ref', phase='after', transformation='tr_outbreak_flag', func_name='step_31_Calc_Over_Ref')
    return calc_over_ref_df

# Step 32 : Keep Trace Visits
def step_32_Keep_Trace_Visits(spark, read_visits_df):
    logging.info("Running FilterRows: Keep Trace Visits")
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_outbreak_flag', step_name='Keep Trace Visits', func_name='step_32_Keep_Trace_Visits', required_columns=[])
    log_step_dataframe(read_visits_df, step_name='Keep Trace Visits', phase='before', transformation='tr_outbreak_flag', func_name='step_32_Keep_Trace_Visits')
    _filter_required = ['reason_code']
    _filter_missing = [c for c in _filter_required if c not in read_visits_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Keep Trace Visits step (missing={_filter_missing}, available={list(read_visits_df.columns)})")
    df_Sort_Visits = read_visits_df.filter(((col("reason_code") == lit('CTF')) | (col("reason_code") == lit('CHR'))))
    keep_trace_visits_df = df_Sort_Visits
    keep_trace_visits_df = require_dataframe(keep_trace_visits_df, transformation='tr_outbreak_flag', step_name='Keep Trace Visits', func_name='step_32_Keep_Trace_Visits', required_columns=[])
    log_step_dataframe(keep_trace_visits_df, step_name='Keep Trace Visits', phase='after', transformation='tr_outbreak_flag', func_name='step_32_Keep_Trace_Visits')
    return keep_trace_visits_df, df_Sort_Visits

# Step 33 : Keep Elevated
def step_33_Keep_Elevated(spark, calc_over_ref_df):
    logging.info("Running FilterRows: Keep Elevated")
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_outbreak_flag', step_name='Keep Elevated', func_name='step_33_Keep_Elevated', required_columns=[])
    log_step_dataframe(calc_over_ref_df, step_name='Keep Elevated', phase='before', transformation='tr_outbreak_flag', func_name='step_33_Keep_Elevated')
    _filter_required = ['over_ref']
    _filter_missing = [c for c in _filter_required if c not in calc_over_ref_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Keep Elevated step (missing={_filter_missing}, available={list(calc_over_ref_df.columns)})")
    df_Sort_Labs = calc_over_ref_df.filter((col("over_ref") > lit(0.0)))
    keep_elevated_df = df_Sort_Labs
    keep_elevated_df = require_dataframe(keep_elevated_df, transformation='tr_outbreak_flag', step_name='Keep Elevated', func_name='step_33_Keep_Elevated', required_columns=[])
    log_step_dataframe(keep_elevated_df, step_name='Keep Elevated', phase='after', transformation='tr_outbreak_flag', func_name='step_33_Keep_Elevated')
    return keep_elevated_df, df_Sort_Labs

# Step 34 : Sort Visits
def step_34_Sort_Visits(spark, df_Sort_Visits):
    logging.info("Running SortRows: Sort Visits")
    df_Sort_Visits = require_dataframe(df_Sort_Visits, transformation='tr_outbreak_flag', step_name='Sort Visits', func_name='step_34_Sort_Visits', required_columns=[])
    log_step_dataframe(df_Sort_Visits, step_name='Sort Visits', phase='before', transformation='tr_outbreak_flag', func_name='step_34_Sort_Visits')
    sort_visits_df = df_Sort_Visits.orderBy(col("patient_id").asc_nulls_last())
    sort_visits_df = require_dataframe(sort_visits_df, transformation='tr_outbreak_flag', step_name='Sort Visits', func_name='step_34_Sort_Visits', required_columns=[])
    log_step_dataframe(sort_visits_df, step_name='Sort Visits', phase='after', transformation='tr_outbreak_flag', func_name='step_34_Sort_Visits')
    return sort_visits_df

# Step 35 : Sort Labs
def step_35_Sort_Labs(spark, df_Sort_Labs):
    logging.info("Running SortRows: Sort Labs")
    df_Sort_Labs = require_dataframe(df_Sort_Labs, transformation='tr_outbreak_flag', step_name='Sort Labs', func_name='step_35_Sort_Labs', required_columns=[])
    log_step_dataframe(df_Sort_Labs, step_name='Sort Labs', phase='before', transformation='tr_outbreak_flag', func_name='step_35_Sort_Labs')
    sort_labs_df = df_Sort_Labs.orderBy(col("patient_id").asc_nulls_last())
    sort_labs_df = require_dataframe(sort_labs_df, transformation='tr_outbreak_flag', step_name='Sort Labs', func_name='step_35_Sort_Labs', required_columns=[])
    log_step_dataframe(sort_labs_df, step_name='Sort Labs', phase='after', transformation='tr_outbreak_flag', func_name='step_35_Sort_Labs')
    return sort_labs_df

# Step 36 : Count Trace Visits
def step_36_Count_Trace_Visits(spark, sort_visits_df):
    logging.info("Running GroupBy: Count Trace Visits")
    sort_visits_df = require_dataframe(sort_visits_df, transformation='tr_outbreak_flag', step_name='Count Trace Visits', func_name='step_36_Count_Trace_Visits', required_columns=[])
    log_step_dataframe(sort_visits_df, step_name='Count Trace Visits', phase='before', transformation='tr_outbreak_flag', func_name='step_36_Count_Trace_Visits')
    count_trace_visits_df = sort_visits_df.groupBy('patient_id').agg(count(lit(1)).alias('priority_visit_count'))
    count_trace_visits_df = require_dataframe(count_trace_visits_df, transformation='tr_outbreak_flag', step_name='Count Trace Visits', func_name='step_36_Count_Trace_Visits', required_columns=[])
    log_step_dataframe(count_trace_visits_df, step_name='Count Trace Visits', phase='after', transformation='tr_outbreak_flag', func_name='step_36_Count_Trace_Visits')
    return count_trace_visits_df

# Step 37 : Count Elevated
def step_37_Count_Elevated(spark, sort_labs_df):
    logging.info("Running GroupBy: Count Elevated")
    sort_labs_df = require_dataframe(sort_labs_df, transformation='tr_outbreak_flag', step_name='Count Elevated', func_name='step_37_Count_Elevated', required_columns=[])
    log_step_dataframe(sort_labs_df, step_name='Count Elevated', phase='before', transformation='tr_outbreak_flag', func_name='step_37_Count_Elevated')
    count_elevated_df = sort_labs_df.groupBy('patient_id').agg(count(lit(1)).alias('elevated_lab_count'), _max(col("over_ref")).alias('max_over_ref'))
    count_elevated_df = require_dataframe(count_elevated_df, transformation='tr_outbreak_flag', step_name='Count Elevated', func_name='step_37_Count_Elevated', required_columns=[])
    log_step_dataframe(count_elevated_df, step_name='Count Elevated', phase='after', transformation='tr_outbreak_flag', func_name='step_37_Count_Elevated')
    return count_elevated_df

# Step 38 : Join Visits
def step_38_Join_Visits(spark, count_elevated_df, count_trace_visits_df):
    logging.info("Running MergeJoin: Join Visits")
    count_elevated_df = require_dataframe(count_elevated_df, transformation='tr_outbreak_flag', step_name='Join Visits', func_name='step_38_Join_Visits', required_columns=[])
    log_step_dataframe(count_elevated_df, step_name='Join Visits', phase='before', transformation='tr_outbreak_flag', func_name='step_38_Join_Visits')
    count_trace_visits_df = require_dataframe(count_trace_visits_df, transformation='tr_outbreak_flag', step_name='Join Visits', func_name='step_38_Join_Visits', required_columns=[])
    _joined_join_visits_df = count_elevated_df.join(count_trace_visits_df, on=["patient_id"], how='left')
    join_visits_df = _joined_join_visits_df
    join_visits_df = require_dataframe(join_visits_df, transformation='tr_outbreak_flag', step_name='Join Visits', func_name='step_38_Join_Visits', required_columns=[])
    log_step_dataframe(join_visits_df, step_name='Join Visits', phase='after', transformation='tr_outbreak_flag', func_name='step_38_Join_Visits')
    return join_visits_df

# Step 39 : Flag Watchlist
def step_39_Flag_Watchlist(spark, join_visits_df):
    logging.info("Running FilterRows: Flag Watchlist")
    join_visits_df = require_dataframe(join_visits_df, transformation='tr_outbreak_flag', step_name='Flag Watchlist', func_name='step_39_Flag_Watchlist', required_columns=[])
    log_step_dataframe(join_visits_df, step_name='Flag Watchlist', phase='before', transformation='tr_outbreak_flag', func_name='step_39_Flag_Watchlist')
    _filter_required = ['elevated_lab_count', 'priority_visit_count']
    _filter_missing = [c for c in _filter_required if c not in join_visits_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Flag Watchlist step (missing={_filter_missing}, available={list(join_visits_df.columns)})")
    df_Add_Risk_Label = join_visits_df.filter(((col("elevated_lab_count") > lit(1)) | (col("priority_visit_count") > lit(0))))
    flag_watchlist_df = df_Add_Risk_Label
    flag_watchlist_df = require_dataframe(flag_watchlist_df, transformation='tr_outbreak_flag', step_name='Flag Watchlist', func_name='step_39_Flag_Watchlist', required_columns=[])
    log_step_dataframe(flag_watchlist_df, step_name='Flag Watchlist', phase='after', transformation='tr_outbreak_flag', func_name='step_39_Flag_Watchlist')
    return flag_watchlist_df, df_Add_Risk_Label

# Step 40 : Add Risk Label
def step_40_Add_Risk_Label(spark, df_Add_Risk_Label):
    logging.info("Running Constant: Add Risk Label")
    df_Add_Risk_Label = require_dataframe(df_Add_Risk_Label, transformation='tr_outbreak_flag', step_name='Add Risk Label', func_name='step_40_Add_Risk_Label', required_columns=[])
    log_step_dataframe(df_Add_Risk_Label, step_name='Add Risk Label', phase='before', transformation='tr_outbreak_flag', func_name='step_40_Add_Risk_Label')
    add_risk_label_df = df_Add_Risk_Label
    add_risk_label_df = add_risk_label_df.withColumn("risk_flag", lit('WATCHLIST'))
    add_risk_label_df = add_risk_label_df.withColumn("action", lit('EPIDEMIOLOGY_REVIEW'))
    add_risk_label_df = require_dataframe(add_risk_label_df, transformation='tr_outbreak_flag', step_name='Add Risk Label', func_name='step_40_Add_Risk_Label', required_columns=[])
    log_step_dataframe(add_risk_label_df, step_name='Add Risk Label', phase='after', transformation='tr_outbreak_flag', func_name='step_40_Add_Risk_Label')
    return add_risk_label_df

# Step 41 : Select Fields
def step_41_Select_Fields(spark, add_risk_label_df):
    logging.info("Running SelectValues: Select Fields")
    add_risk_label_df = require_dataframe(add_risk_label_df, transformation='tr_outbreak_flag', step_name='Select Fields', func_name='step_41_Select_Fields', required_columns=[])
    log_step_dataframe(add_risk_label_df, step_name='Select Fields', phase='before', transformation='tr_outbreak_flag', func_name='step_41_Select_Fields')
    _sv_required = ['patient_id', 'elevated_lab_count', 'max_over_ref', 'priority_visit_count', 'risk_flag', 'action']
    _sv_missing = [c for c in _sv_required if c not in add_risk_label_df.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Fields step (missing={_sv_missing}, available={list(add_risk_label_df.columns)})")
    select_fields_df = add_risk_label_df.select(col("patient_id"), col("elevated_lab_count"), col("max_over_ref"), col("priority_visit_count"), col("risk_flag"), col("action"))
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_outbreak_flag', step_name='Select Fields', func_name='step_41_Select_Fields', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Select Fields', phase='after', transformation='tr_outbreak_flag', func_name='step_41_Select_Fields')
    return select_fields_df

# Step 42 : Write Risk
def step_42_Write_Risk(spark, select_fields_df):
    logging.info("Running TextFileOutput: Write Risk")
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_outbreak_flag', step_name='Write Risk', func_name='step_42_Write_Risk', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Write Risk', phase='before', transformation='tr_outbreak_flag', func_name='step_42_Write_Risk')
    write_risk_df = select_fields_df
    _tfo_declared = ['patient_id', 'elevated_lab_count', 'max_over_ref', 'priority_visit_count', 'risk_flag', 'action']
    _tfo_missing = [c for c in _tfo_declared if c not in write_risk_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Risk step (missing={_tfo_missing}, available={list(write_risk_df.columns)})")
    selected_output_df = write_risk_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/outbreak_watchlist_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/outbreak_watchlist_out.csv'
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
    write_risk_df = require_dataframe(write_risk_df, transformation='tr_outbreak_flag', step_name='Write Risk', func_name='step_42_Write_Risk', required_columns=[])
    log_step_dataframe(write_risk_df, step_name='Write Risk', phase='after', transformation='tr_outbreak_flag', func_name='step_42_Write_Risk')
    return write_risk_df

def run_tr_outbreak_flag(spark, config=None):
    """Run tr_outbreak_flag."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_outbreak_flag (tr_outbreak_flag.ktr)")

    read_labs_df = step_29_Read_Labs(spark)
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_outbreak_flag', step_name='Read Labs', func_name='step_29_Read_Labs', required_columns=[])
    read_visits_df = step_30_Read_Visits(spark)
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_outbreak_flag', step_name='Read Visits', func_name='step_30_Read_Visits', required_columns=[])
    read_labs_df = require_dataframe(read_labs_df, transformation='tr_outbreak_flag', step_name='Calc Over Ref', func_name='step_31_Calc_Over_Ref', required_columns=[])
    calc_over_ref_df = step_31_Calc_Over_Ref(spark, read_labs_df)
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_outbreak_flag', step_name='Calc Over Ref', func_name='step_31_Calc_Over_Ref', required_columns=[])
    read_visits_df = require_dataframe(read_visits_df, transformation='tr_outbreak_flag', step_name='Keep Trace Visits', func_name='step_32_Keep_Trace_Visits', required_columns=[])
    keep_trace_visits_df, df_Sort_Visits = step_32_Keep_Trace_Visits(spark, read_visits_df)
    keep_trace_visits_df = require_dataframe(keep_trace_visits_df, transformation='tr_outbreak_flag', step_name='Keep Trace Visits', func_name='step_32_Keep_Trace_Visits', required_columns=[])
    calc_over_ref_df = require_dataframe(calc_over_ref_df, transformation='tr_outbreak_flag', step_name='Keep Elevated', func_name='step_33_Keep_Elevated', required_columns=[])
    keep_elevated_df, df_Sort_Labs = step_33_Keep_Elevated(spark, calc_over_ref_df)
    keep_elevated_df = require_dataframe(keep_elevated_df, transformation='tr_outbreak_flag', step_name='Keep Elevated', func_name='step_33_Keep_Elevated', required_columns=[])
    df_Sort_Visits = require_dataframe(df_Sort_Visits, transformation='tr_outbreak_flag', step_name='Sort Visits', func_name='step_34_Sort_Visits', required_columns=[])
    sort_visits_df = step_34_Sort_Visits(spark, df_Sort_Visits)
    sort_visits_df = require_dataframe(sort_visits_df, transformation='tr_outbreak_flag', step_name='Sort Visits', func_name='step_34_Sort_Visits', required_columns=[])
    df_Sort_Labs = require_dataframe(df_Sort_Labs, transformation='tr_outbreak_flag', step_name='Sort Labs', func_name='step_35_Sort_Labs', required_columns=[])
    sort_labs_df = step_35_Sort_Labs(spark, df_Sort_Labs)
    sort_labs_df = require_dataframe(sort_labs_df, transformation='tr_outbreak_flag', step_name='Sort Labs', func_name='step_35_Sort_Labs', required_columns=[])
    sort_visits_df = require_dataframe(sort_visits_df, transformation='tr_outbreak_flag', step_name='Count Trace Visits', func_name='step_36_Count_Trace_Visits', required_columns=[])
    count_trace_visits_df = step_36_Count_Trace_Visits(spark, sort_visits_df)
    count_trace_visits_df = require_dataframe(count_trace_visits_df, transformation='tr_outbreak_flag', step_name='Count Trace Visits', func_name='step_36_Count_Trace_Visits', required_columns=[])
    sort_labs_df = require_dataframe(sort_labs_df, transformation='tr_outbreak_flag', step_name='Count Elevated', func_name='step_37_Count_Elevated', required_columns=[])
    count_elevated_df = step_37_Count_Elevated(spark, sort_labs_df)
    count_elevated_df = require_dataframe(count_elevated_df, transformation='tr_outbreak_flag', step_name='Count Elevated', func_name='step_37_Count_Elevated', required_columns=[])
    count_elevated_df = require_dataframe(count_elevated_df, transformation='tr_outbreak_flag', step_name='Join Visits', func_name='step_38_Join_Visits', required_columns=[])
    count_trace_visits_df = require_dataframe(count_trace_visits_df, transformation='tr_outbreak_flag', step_name='Join Visits', func_name='step_38_Join_Visits', required_columns=[])
    join_visits_df = step_38_Join_Visits(spark, count_elevated_df, count_trace_visits_df)
    join_visits_df = require_dataframe(join_visits_df, transformation='tr_outbreak_flag', step_name='Join Visits', func_name='step_38_Join_Visits', required_columns=[])
    join_visits_df = require_dataframe(join_visits_df, transformation='tr_outbreak_flag', step_name='Flag Watchlist', func_name='step_39_Flag_Watchlist', required_columns=[])
    flag_watchlist_df, df_Add_Risk_Label = step_39_Flag_Watchlist(spark, join_visits_df)
    flag_watchlist_df = require_dataframe(flag_watchlist_df, transformation='tr_outbreak_flag', step_name='Flag Watchlist', func_name='step_39_Flag_Watchlist', required_columns=[])
    df_Add_Risk_Label = require_dataframe(df_Add_Risk_Label, transformation='tr_outbreak_flag', step_name='Add Risk Label', func_name='step_40_Add_Risk_Label', required_columns=[])
    add_risk_label_df = step_40_Add_Risk_Label(spark, df_Add_Risk_Label)
    add_risk_label_df = require_dataframe(add_risk_label_df, transformation='tr_outbreak_flag', step_name='Add Risk Label', func_name='step_40_Add_Risk_Label', required_columns=[])
    add_risk_label_df = require_dataframe(add_risk_label_df, transformation='tr_outbreak_flag', step_name='Select Fields', func_name='step_41_Select_Fields', required_columns=[])
    select_fields_df = step_41_Select_Fields(spark, add_risk_label_df)
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_outbreak_flag', step_name='Select Fields', func_name='step_41_Select_Fields', required_columns=[])
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_outbreak_flag', step_name='Write Risk', func_name='step_42_Write_Risk', required_columns=[])
    write_risk_df = step_42_Write_Risk(spark, select_fields_df)
    write_risk_df = require_dataframe(write_risk_df, transformation='tr_outbreak_flag', step_name='Write Risk', func_name='step_42_Write_Risk', required_columns=[])

    logging.info("Finished transformation: tr_outbreak_flag")
    return write_risk_df



def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Run the 'jb_master' transformation flow."""
    return execute_registered_job(
        'jb_master',
        spark=spark,
        config_overrides=config,
        trans_runners={
            'Enrich Visits': run_tr_visit_enrich, 'Rollup Labs': run_tr_lab_rollup, 'Route Vaccinations': run_tr_vaccination_route, 'Flag Outbreak Risk': run_tr_outbreak_flag
        },
    )


if __name__ == "__main__":
    _spark = SparkSession.builder.appName('jb_master').getOrCreate()
    run(_spark, None)
