"""Databricks job for jb_prep."""

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
logger = logging.getLogger('jb_prep')

TARGET_CATALOG = config.TARGET_CATALOG
TARGET_SCHEMA = config.TARGET_SCHEMA
PENTAHO_DATA_DIR = config.PENTAHO_DATA_DIR

# Step 1 : Read Patients
def step_01_Read_Patients(spark):
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
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_patient_clean', step_name='Read Patients', func_name='step_01_Read_Patients', required_columns=[])
    log_step_dataframe(read_patients_df, step_name='Read Patients', phase='after', transformation='tr_patient_clean', func_name='step_01_Read_Patients')
    return read_patients_df

# Step 2 : Normalize Notes
def step_02_Normalize_Notes(spark, read_patients_df):
    logging.info("Running ReplaceString: Normalize Notes")
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_patient_clean', step_name='Normalize Notes', func_name='step_02_Normalize_Notes', required_columns=[])
    log_step_dataframe(read_patients_df, step_name='Normalize Notes', phase='before', transformation='tr_patient_clean', func_name='step_02_Normalize_Notes')
    normalize_notes_df = read_patients_df
    normalize_notes_df = normalize_notes_df.withColumn("notes_clean", regexp_replace(col("notes").cast("string"), '(?i)high\\ risk', 'HIGH_RISK'))
    normalize_notes_df = require_dataframe(normalize_notes_df, transformation='tr_patient_clean', step_name='Normalize Notes', func_name='step_02_Normalize_Notes', required_columns=[])
    log_step_dataframe(normalize_notes_df, step_name='Normalize Notes', phase='after', transformation='tr_patient_clean', func_name='step_02_Normalize_Notes')
    return normalize_notes_df

# Step 3 : Null Placeholder
def step_03_Null_Placeholder(spark, normalize_notes_df):
    logging.info("Running NullIf: Null Placeholder")
    normalize_notes_df = require_dataframe(normalize_notes_df, transformation='tr_patient_clean', step_name='Null Placeholder', func_name='step_03_Null_Placeholder', required_columns=[])
    log_step_dataframe(normalize_notes_df, step_name='Null Placeholder', phase='before', transformation='tr_patient_clean', func_name='step_03_Null_Placeholder')
    null_placeholder_df = normalize_notes_df
    null_placeholder_df = null_placeholder_df.withColumn('notes_clean', when((col('notes_clean') == lit('N/A')), lit(None)).otherwise(col('notes_clean')))
    null_placeholder_df = require_dataframe(null_placeholder_df, transformation='tr_patient_clean', step_name='Null Placeholder', func_name='step_03_Null_Placeholder', required_columns=[])
    log_step_dataframe(null_placeholder_df, step_name='Null Placeholder', phase='after', transformation='tr_patient_clean', func_name='step_03_Null_Placeholder')
    return null_placeholder_df

# Step 4 : Fill Status
def step_04_Fill_Status(spark, null_placeholder_df):
    logging.info("Running IfNull: Fill Status")
    null_placeholder_df = require_dataframe(null_placeholder_df, transformation='tr_patient_clean', step_name='Fill Status', func_name='step_04_Fill_Status', required_columns=[])
    log_step_dataframe(null_placeholder_df, step_name='Fill Status', phase='before', transformation='tr_patient_clean', func_name='step_04_Fill_Status')
    fill_status_df = null_placeholder_df
    fill_status_df = fill_status_df.withColumn('status', when(col('status').isNull(), lit('UNKNOWN')).otherwise(col('status')))
    fill_status_df = require_dataframe(fill_status_df, transformation='tr_patient_clean', step_name='Fill Status', func_name='step_04_Fill_Status', required_columns=[])
    log_step_dataframe(fill_status_df, step_name='Fill Status', phase='after', transformation='tr_patient_clean', func_name='step_04_Fill_Status')
    return fill_status_df

# Step 5 : Keep Active Roster
def step_05_Keep_Active_Roster(spark, fill_status_df):
    logging.info("Running FilterRows: Keep Active Roster")
    fill_status_df = require_dataframe(fill_status_df, transformation='tr_patient_clean', step_name='Keep Active Roster', func_name='step_05_Keep_Active_Roster', required_columns=[])
    log_step_dataframe(fill_status_df, step_name='Keep Active Roster', phase='before', transformation='tr_patient_clean', func_name='step_05_Keep_Active_Roster')
    _filter_required = ['status']
    _filter_missing = [c for c in _filter_required if c not in fill_status_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Keep Active Roster step (missing={_filter_missing}, available={list(fill_status_df.columns)})")
    df_Select_Fields = fill_status_df.filter(((col("status") == lit('ACTIVE')) | (col("status") == lit('PENDING'))))
    keep_active_roster_df = df_Select_Fields
    keep_active_roster_df = require_dataframe(keep_active_roster_df, transformation='tr_patient_clean', step_name='Keep Active Roster', func_name='step_05_Keep_Active_Roster', required_columns=[])
    log_step_dataframe(keep_active_roster_df, step_name='Keep Active Roster', phase='after', transformation='tr_patient_clean', func_name='step_05_Keep_Active_Roster')
    return keep_active_roster_df, df_Select_Fields

# Step 6 : Select Fields
def step_06_Select_Fields(spark, df_Select_Fields):
    logging.info("Running SelectValues: Select Fields")
    df_Select_Fields = require_dataframe(df_Select_Fields, transformation='tr_patient_clean', step_name='Select Fields', func_name='step_06_Select_Fields', required_columns=[])
    log_step_dataframe(df_Select_Fields, step_name='Select Fields', phase='before', transformation='tr_patient_clean', func_name='step_06_Select_Fields')
    _sv_required = ['patient_id', 'patient_name', 'status', 'zip_code', 'dob', 'notes_clean']
    _sv_missing = [c for c in _sv_required if c not in df_Select_Fields.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Fields step (missing={_sv_missing}, available={list(df_Select_Fields.columns)})")
    select_fields_df = df_Select_Fields.select(col("patient_id"), col("patient_name"), col("status"), col("zip_code"), col("dob"), col("notes_clean").alias("notes"))
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_patient_clean', step_name='Select Fields', func_name='step_06_Select_Fields', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Select Fields', phase='after', transformation='tr_patient_clean', func_name='step_06_Select_Fields')
    return select_fields_df

# Step 7 : Dedup Patients
def step_07_Dedup_Patients(spark, select_fields_df):
    logging.info("Running Unique: Dedup Patients")
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_patient_clean', step_name='Dedup Patients', func_name='step_07_Dedup_Patients', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Dedup Patients', phase='before', transformation='tr_patient_clean', func_name='step_07_Dedup_Patients')
    dedup_patients_df = select_fields_df.dropDuplicates(["patient_id"])
    dedup_patients_df = require_dataframe(dedup_patients_df, transformation='tr_patient_clean', step_name='Dedup Patients', func_name='step_07_Dedup_Patients', required_columns=[])
    log_step_dataframe(dedup_patients_df, step_name='Dedup Patients', phase='after', transformation='tr_patient_clean', func_name='step_07_Dedup_Patients')
    return dedup_patients_df

# Step 8 : Write Clean
def step_08_Write_Clean(spark, dedup_patients_df):
    logging.info("Running TextFileOutput: Write Clean")
    dedup_patients_df = require_dataframe(dedup_patients_df, transformation='tr_patient_clean', step_name='Write Clean', func_name='step_08_Write_Clean', required_columns=[])
    log_step_dataframe(dedup_patients_df, step_name='Write Clean', phase='before', transformation='tr_patient_clean', func_name='step_08_Write_Clean')
    write_clean_df = dedup_patients_df
    _tfo_declared = ['patient_id', 'patient_name', 'status', 'zip_code', 'dob', 'notes']
    _tfo_missing = [c for c in _tfo_declared if c not in write_clean_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Clean step (missing={_tfo_missing}, available={list(write_clean_df.columns)})")
    selected_output_df = write_clean_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/patients_clean_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/patients_clean_out.csv'
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
    write_clean_df = require_dataframe(write_clean_df, transformation='tr_patient_clean', step_name='Write Clean', func_name='step_08_Write_Clean', required_columns=[])
    log_step_dataframe(write_clean_df, step_name='Write Clean', phase='after', transformation='tr_patient_clean', func_name='step_08_Write_Clean')
    return write_clean_df

def run_tr_patient_clean(spark, config=None):
    """Run tr_patient_clean."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_patient_clean (tr_patient_clean.ktr)")

    read_patients_df = step_01_Read_Patients(spark)
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_patient_clean', step_name='Read Patients', func_name='step_01_Read_Patients', required_columns=[])
    read_patients_df = require_dataframe(read_patients_df, transformation='tr_patient_clean', step_name='Normalize Notes', func_name='step_02_Normalize_Notes', required_columns=[])
    normalize_notes_df = step_02_Normalize_Notes(spark, read_patients_df)
    normalize_notes_df = require_dataframe(normalize_notes_df, transformation='tr_patient_clean', step_name='Normalize Notes', func_name='step_02_Normalize_Notes', required_columns=[])
    normalize_notes_df = require_dataframe(normalize_notes_df, transformation='tr_patient_clean', step_name='Null Placeholder', func_name='step_03_Null_Placeholder', required_columns=[])
    null_placeholder_df = step_03_Null_Placeholder(spark, normalize_notes_df)
    null_placeholder_df = require_dataframe(null_placeholder_df, transformation='tr_patient_clean', step_name='Null Placeholder', func_name='step_03_Null_Placeholder', required_columns=[])
    null_placeholder_df = require_dataframe(null_placeholder_df, transformation='tr_patient_clean', step_name='Fill Status', func_name='step_04_Fill_Status', required_columns=[])
    fill_status_df = step_04_Fill_Status(spark, null_placeholder_df)
    fill_status_df = require_dataframe(fill_status_df, transformation='tr_patient_clean', step_name='Fill Status', func_name='step_04_Fill_Status', required_columns=[])
    fill_status_df = require_dataframe(fill_status_df, transformation='tr_patient_clean', step_name='Keep Active Roster', func_name='step_05_Keep_Active_Roster', required_columns=[])
    keep_active_roster_df, df_Select_Fields = step_05_Keep_Active_Roster(spark, fill_status_df)
    keep_active_roster_df = require_dataframe(keep_active_roster_df, transformation='tr_patient_clean', step_name='Keep Active Roster', func_name='step_05_Keep_Active_Roster', required_columns=[])
    df_Select_Fields = require_dataframe(df_Select_Fields, transformation='tr_patient_clean', step_name='Select Fields', func_name='step_06_Select_Fields', required_columns=[])
    select_fields_df = step_06_Select_Fields(spark, df_Select_Fields)
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_patient_clean', step_name='Select Fields', func_name='step_06_Select_Fields', required_columns=[])
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_patient_clean', step_name='Dedup Patients', func_name='step_07_Dedup_Patients', required_columns=[])
    dedup_patients_df = step_07_Dedup_Patients(spark, select_fields_df)
    dedup_patients_df = require_dataframe(dedup_patients_df, transformation='tr_patient_clean', step_name='Dedup Patients', func_name='step_07_Dedup_Patients', required_columns=[])
    dedup_patients_df = require_dataframe(dedup_patients_df, transformation='tr_patient_clean', step_name='Write Clean', func_name='step_08_Write_Clean', required_columns=[])
    write_clean_df = step_08_Write_Clean(spark, dedup_patients_df)
    write_clean_df = require_dataframe(write_clean_df, transformation='tr_patient_clean', step_name='Write Clean', func_name='step_08_Write_Clean', required_columns=[])

    logging.info("Finished transformation: tr_patient_clean")
    return write_clean_df

# Step 9 : Read Clinics
def step_09_Read_Clinics(spark):
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
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_clinic_normalize', step_name='Read Clinics', func_name='step_09_Read_Clinics', required_columns=[])
    log_step_dataframe(read_clinics_df, step_name='Read Clinics', phase='after', transformation='tr_clinic_normalize', func_name='step_09_Read_Clinics')
    return read_clinics_df

# Step 10 : Read Providers
def step_10_Read_Providers(spark):
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
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_clinic_normalize', step_name='Read Providers', func_name='step_10_Read_Providers', required_columns=[])
    log_step_dataframe(read_providers_df, step_name='Read Providers', phase='after', transformation='tr_clinic_normalize', func_name='step_10_Read_Providers')
    return read_providers_df

# Step 11 : Select Provider Cols
def step_11_Select_Provider_Cols(spark, read_providers_df):
    logging.info("Running SelectValues: Select Provider Cols")
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_clinic_normalize', step_name='Select Provider Cols', func_name='step_11_Select_Provider_Cols', required_columns=[])
    log_step_dataframe(read_providers_df, step_name='Select Provider Cols', phase='before', transformation='tr_clinic_normalize', func_name='step_11_Select_Provider_Cols')
    _sv_required = ['provider_id', 'provider_name', 'specialty', 'active_flag']
    _sv_missing = [c for c in _sv_required if c not in read_providers_df.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Provider Cols step (missing={_sv_missing}, available={list(read_providers_df.columns)})")
    select_provider_cols_df = read_providers_df.select(col("provider_id"), col("provider_name"), col("specialty"), col("active_flag"))
    select_provider_cols_df = require_dataframe(select_provider_cols_df, transformation='tr_clinic_normalize', step_name='Select Provider Cols', func_name='step_11_Select_Provider_Cols', required_columns=[])
    log_step_dataframe(select_provider_cols_df, step_name='Select Provider Cols', phase='after', transformation='tr_clinic_normalize', func_name='step_11_Select_Provider_Cols')
    return select_provider_cols_df

# Step 12 : Join Provider
def step_12_Join_Provider(spark, read_clinics_df, select_provider_cols_df):
    logging.info("Running MergeJoin: Join Provider")
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_clinic_normalize', step_name='Join Provider', func_name='step_12_Join_Provider', required_columns=[])
    log_step_dataframe(read_clinics_df, step_name='Join Provider', phase='before', transformation='tr_clinic_normalize', func_name='step_12_Join_Provider')
    select_provider_cols_df = require_dataframe(select_provider_cols_df, transformation='tr_clinic_normalize', step_name='Join Provider', func_name='step_12_Join_Provider', required_columns=[])
    _joined_join_provider_df = read_clinics_df.join(select_provider_cols_df, on=["provider_id"], how='left')
    join_provider_df = _joined_join_provider_df
    join_provider_df = require_dataframe(join_provider_df, transformation='tr_clinic_normalize', step_name='Join Provider', func_name='step_12_Join_Provider', required_columns=[])
    log_step_dataframe(join_provider_df, step_name='Join Provider', phase='after', transformation='tr_clinic_normalize', func_name='step_12_Join_Provider')
    return join_provider_df

# Step 13 : Keep Active Providers
def step_13_Keep_Active_Providers(spark, join_provider_df):
    logging.info("Running FilterRows: Keep Active Providers")
    join_provider_df = require_dataframe(join_provider_df, transformation='tr_clinic_normalize', step_name='Keep Active Providers', func_name='step_13_Keep_Active_Providers', required_columns=[])
    log_step_dataframe(join_provider_df, step_name='Keep Active Providers', phase='before', transformation='tr_clinic_normalize', func_name='step_13_Keep_Active_Providers')
    _filter_required = ['active_flag']
    _filter_missing = [c for c in _filter_required if c not in join_provider_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Keep Active Providers step (missing={_filter_missing}, available={list(join_provider_df.columns)})")
    df_Map_Clinic_Type = join_provider_df.filter((col("active_flag") == lit('Y')))
    keep_active_providers_df = df_Map_Clinic_Type
    keep_active_providers_df = require_dataframe(keep_active_providers_df, transformation='tr_clinic_normalize', step_name='Keep Active Providers', func_name='step_13_Keep_Active_Providers', required_columns=[])
    log_step_dataframe(keep_active_providers_df, step_name='Keep Active Providers', phase='after', transformation='tr_clinic_normalize', func_name='step_13_Keep_Active_Providers')
    return keep_active_providers_df, df_Map_Clinic_Type

# Step 14 : Map Clinic Type
def step_14_Map_Clinic_Type(spark, df_Map_Clinic_Type):
    logging.info("Running ValueMapper: Map Clinic Type")
    df_Map_Clinic_Type = require_dataframe(df_Map_Clinic_Type, transformation='tr_clinic_normalize', step_name='Map Clinic Type', func_name='step_14_Map_Clinic_Type', required_columns=[])
    log_step_dataframe(df_Map_Clinic_Type, step_name='Map Clinic Type', phase='before', transformation='tr_clinic_normalize', func_name='step_14_Map_Clinic_Type')
    map_clinic_type_df = df_Map_Clinic_Type.withColumn("type_code", when((col("clinic_type") == lit('Primary')), lit('PRI')).when((col("clinic_type") == lit('Specialty')), lit('SPC')).when((col("clinic_type") == lit('Mobile')), lit('MOB')).when((col("clinic_type") == lit('Vaccination')), lit('VAX')).when((col("clinic_type") == lit('Lab')), lit('LAB')).when((col("clinic_type").isNull() | (col("clinic_type") == lit(''))), col("clinic_type")).otherwise(lit('OTH')))
    map_clinic_type_df = require_dataframe(map_clinic_type_df, transformation='tr_clinic_normalize', step_name='Map Clinic Type', func_name='step_14_Map_Clinic_Type', required_columns=[])
    log_step_dataframe(map_clinic_type_df, step_name='Map Clinic Type', phase='after', transformation='tr_clinic_normalize', func_name='step_14_Map_Clinic_Type')
    return map_clinic_type_df

# Step 15 : Add Network Tag
def step_15_Add_Network_Tag(spark, map_clinic_type_df):
    logging.info("Running Constant: Add Network Tag")
    map_clinic_type_df = require_dataframe(map_clinic_type_df, transformation='tr_clinic_normalize', step_name='Add Network Tag', func_name='step_15_Add_Network_Tag', required_columns=[])
    log_step_dataframe(map_clinic_type_df, step_name='Add Network Tag', phase='before', transformation='tr_clinic_normalize', func_name='step_15_Add_Network_Tag')
    add_network_tag_df = map_clinic_type_df
    add_network_tag_df = add_network_tag_df.withColumn("network_tag", lit('SF_HD_NET'))
    add_network_tag_df = require_dataframe(add_network_tag_df, transformation='tr_clinic_normalize', step_name='Add Network Tag', func_name='step_15_Add_Network_Tag', required_columns=[])
    log_step_dataframe(add_network_tag_df, step_name='Add Network Tag', phase='after', transformation='tr_clinic_normalize', func_name='step_15_Add_Network_Tag')
    return add_network_tag_df

# Step 16 : Write Clinics
def step_16_Write_Clinics(spark, add_network_tag_df):
    logging.info("Running TextFileOutput: Write Clinics")
    add_network_tag_df = require_dataframe(add_network_tag_df, transformation='tr_clinic_normalize', step_name='Write Clinics', func_name='step_16_Write_Clinics', required_columns=[])
    log_step_dataframe(add_network_tag_df, step_name='Write Clinics', phase='before', transformation='tr_clinic_normalize', func_name='step_16_Write_Clinics')
    write_clinics_df = add_network_tag_df
    _tfo_declared = ['clinic_id', 'clinic_name', 'district', 'capacity', 'provider_name', 'specialty', 'type_code', 'network_tag']
    _tfo_missing = [c for c in _tfo_declared if c not in write_clinics_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Clinics step (missing={_tfo_missing}, available={list(write_clinics_df.columns)})")
    selected_output_df = write_clinics_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/clinics_norm_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/clinics_norm_out.csv'
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
    write_clinics_df = require_dataframe(write_clinics_df, transformation='tr_clinic_normalize', step_name='Write Clinics', func_name='step_16_Write_Clinics', required_columns=[])
    log_step_dataframe(write_clinics_df, step_name='Write Clinics', phase='after', transformation='tr_clinic_normalize', func_name='step_16_Write_Clinics')
    return write_clinics_df

def run_tr_clinic_normalize(spark, config=None):
    """Run tr_clinic_normalize."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_clinic_normalize (tr_clinic_normalize.ktr)")

    read_clinics_df = step_09_Read_Clinics(spark)
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_clinic_normalize', step_name='Read Clinics', func_name='step_09_Read_Clinics', required_columns=[])
    read_providers_df = step_10_Read_Providers(spark)
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_clinic_normalize', step_name='Read Providers', func_name='step_10_Read_Providers', required_columns=[])
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_clinic_normalize', step_name='Select Provider Cols', func_name='step_11_Select_Provider_Cols', required_columns=[])
    select_provider_cols_df = step_11_Select_Provider_Cols(spark, read_providers_df)
    select_provider_cols_df = require_dataframe(select_provider_cols_df, transformation='tr_clinic_normalize', step_name='Select Provider Cols', func_name='step_11_Select_Provider_Cols', required_columns=[])
    read_clinics_df = require_dataframe(read_clinics_df, transformation='tr_clinic_normalize', step_name='Join Provider', func_name='step_12_Join_Provider', required_columns=[])
    select_provider_cols_df = require_dataframe(select_provider_cols_df, transformation='tr_clinic_normalize', step_name='Join Provider', func_name='step_12_Join_Provider', required_columns=[])
    join_provider_df = step_12_Join_Provider(spark, read_clinics_df, select_provider_cols_df)
    join_provider_df = require_dataframe(join_provider_df, transformation='tr_clinic_normalize', step_name='Join Provider', func_name='step_12_Join_Provider', required_columns=[])
    join_provider_df = require_dataframe(join_provider_df, transformation='tr_clinic_normalize', step_name='Keep Active Providers', func_name='step_13_Keep_Active_Providers', required_columns=[])
    keep_active_providers_df, df_Map_Clinic_Type = step_13_Keep_Active_Providers(spark, join_provider_df)
    keep_active_providers_df = require_dataframe(keep_active_providers_df, transformation='tr_clinic_normalize', step_name='Keep Active Providers', func_name='step_13_Keep_Active_Providers', required_columns=[])
    df_Map_Clinic_Type = require_dataframe(df_Map_Clinic_Type, transformation='tr_clinic_normalize', step_name='Map Clinic Type', func_name='step_14_Map_Clinic_Type', required_columns=[])
    map_clinic_type_df = step_14_Map_Clinic_Type(spark, df_Map_Clinic_Type)
    map_clinic_type_df = require_dataframe(map_clinic_type_df, transformation='tr_clinic_normalize', step_name='Map Clinic Type', func_name='step_14_Map_Clinic_Type', required_columns=[])
    map_clinic_type_df = require_dataframe(map_clinic_type_df, transformation='tr_clinic_normalize', step_name='Add Network Tag', func_name='step_15_Add_Network_Tag', required_columns=[])
    add_network_tag_df = step_15_Add_Network_Tag(spark, map_clinic_type_df)
    add_network_tag_df = require_dataframe(add_network_tag_df, transformation='tr_clinic_normalize', step_name='Add Network Tag', func_name='step_15_Add_Network_Tag', required_columns=[])
    add_network_tag_df = require_dataframe(add_network_tag_df, transformation='tr_clinic_normalize', step_name='Write Clinics', func_name='step_16_Write_Clinics', required_columns=[])
    write_clinics_df = step_16_Write_Clinics(spark, add_network_tag_df)
    write_clinics_df = require_dataframe(write_clinics_df, transformation='tr_clinic_normalize', step_name='Write Clinics', func_name='step_16_Write_Clinics', required_columns=[])

    logging.info("Finished transformation: tr_clinic_normalize")
    return write_clinics_df



def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Run the 'jb_prep' transformation flow."""
    return execute_registered_job(
        'jb_prep',
        spark=spark,
        config_overrides=config,
        trans_runners={
            'Clean Patients': run_tr_patient_clean, 'Normalize Clinics': run_tr_clinic_normalize
        },
    )


if __name__ == "__main__":
    _spark = SparkSession.builder.appName('jb_prep').getOrCreate()
    run(_spark, None)
