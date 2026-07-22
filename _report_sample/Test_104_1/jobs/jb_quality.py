"""Databricks job for jb_quality."""

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
logger = logging.getLogger('jb_quality')

TARGET_CATALOG = config.TARGET_CATALOG
TARGET_SCHEMA = config.TARGET_SCHEMA
PENTAHO_DATA_DIR = config.PENTAHO_DATA_DIR

# Step 1 : Read Providers
def step_01_Read_Providers(spark):
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
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_provider_check', step_name='Read Providers', func_name='step_01_Read_Providers', required_columns=[])
    log_step_dataframe(read_providers_df, step_name='Read Providers', phase='after', transformation='tr_provider_check', func_name='step_01_Read_Providers')
    return read_providers_df

# Step 2 : Find Inactive
def step_02_Find_Inactive(spark, read_providers_df):
    logging.info("Running FilterRows: Find Inactive")
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_provider_check', step_name='Find Inactive', func_name='step_02_Find_Inactive', required_columns=[])
    log_step_dataframe(read_providers_df, step_name='Find Inactive', phase='before', transformation='tr_provider_check', func_name='step_02_Find_Inactive')
    _filter_required = ['active_flag']
    _filter_missing = [c for c in _filter_required if c not in read_providers_df.columns]
    if _filter_missing:
        raise ValueError(f"Column {_filter_missing[0]} missing before Find Inactive step (missing={_filter_missing}, available={list(read_providers_df.columns)})")
    df_Flag_Issue = read_providers_df.filter((col("active_flag") == lit('N')))
    find_inactive_df = df_Flag_Issue
    find_inactive_df = require_dataframe(find_inactive_df, transformation='tr_provider_check', step_name='Find Inactive', func_name='step_02_Find_Inactive', required_columns=[])
    log_step_dataframe(find_inactive_df, step_name='Find Inactive', phase='after', transformation='tr_provider_check', func_name='step_02_Find_Inactive')
    return find_inactive_df, df_Flag_Issue

# Step 3 : Flag Issue
def step_03_Flag_Issue(spark, df_Flag_Issue):
    logging.info("Running Constant: Flag Issue")
    df_Flag_Issue = require_dataframe(df_Flag_Issue, transformation='tr_provider_check', step_name='Flag Issue', func_name='step_03_Flag_Issue', required_columns=[])
    log_step_dataframe(df_Flag_Issue, step_name='Flag Issue', phase='before', transformation='tr_provider_check', func_name='step_03_Flag_Issue')
    flag_issue_df = df_Flag_Issue
    flag_issue_df = flag_issue_df.withColumn("issue_type", lit('INACTIVE_PROVIDER'))
    flag_issue_df = flag_issue_df.withColumn("severity", lit('WARN'))
    flag_issue_df = require_dataframe(flag_issue_df, transformation='tr_provider_check', step_name='Flag Issue', func_name='step_03_Flag_Issue', required_columns=[])
    log_step_dataframe(flag_issue_df, step_name='Flag Issue', phase='after', transformation='tr_provider_check', func_name='step_03_Flag_Issue')
    return flag_issue_df

# Step 4 : Select Fields
def step_04_Select_Fields(spark, flag_issue_df):
    logging.info("Running SelectValues: Select Fields")
    flag_issue_df = require_dataframe(flag_issue_df, transformation='tr_provider_check', step_name='Select Fields', func_name='step_04_Select_Fields', required_columns=[])
    log_step_dataframe(flag_issue_df, step_name='Select Fields', phase='before', transformation='tr_provider_check', func_name='step_04_Select_Fields')
    _sv_required = ['provider_id', 'provider_name', 'specialty', 'license_no', 'issue_type', 'severity']
    _sv_missing = [c for c in _sv_required if c not in flag_issue_df.columns]
    if _sv_missing:
        raise ValueError(f"Column {_sv_missing[0]} missing before Select Fields step (missing={_sv_missing}, available={list(flag_issue_df.columns)})")
    select_fields_df = flag_issue_df.select(col("provider_id"), col("provider_name"), col("specialty"), col("license_no"), col("issue_type"), col("severity"))
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_provider_check', step_name='Select Fields', func_name='step_04_Select_Fields', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Select Fields', phase='after', transformation='tr_provider_check', func_name='step_04_Select_Fields')
    return select_fields_df

# Step 5 : Write Issues
def step_05_Write_Issues(spark, select_fields_df):
    logging.info("Running TextFileOutput: Write Issues")
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_provider_check', step_name='Write Issues', func_name='step_05_Write_Issues', required_columns=[])
    log_step_dataframe(select_fields_df, step_name='Write Issues', phase='before', transformation='tr_provider_check', func_name='step_05_Write_Issues')
    write_issues_df = select_fields_df
    _tfo_declared = ['provider_id', 'provider_name', 'specialty', 'license_no', 'issue_type', 'severity']
    _tfo_missing = [c for c in _tfo_declared if c not in write_issues_df.columns]
    if _tfo_missing:
        raise ValueError(f"Column {_tfo_missing[0]} missing before Write Issues step (missing={_tfo_missing}, available={list(write_issues_df.columns)})")
    selected_output_df = write_issues_df.select(*_tfo_declared)
    (
        selected_output_df.write
        .mode('overwrite')
        .option("header", True)
        .option("sep", ',')
        .option("encoding", 'UTF-8')
        .csv(f'{PENTAHO_DATA_DIR}/provider_issues_out.csv')
    )
    _tfo_out_path = f'{PENTAHO_DATA_DIR}/provider_issues_out.csv'
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
    write_issues_df = require_dataframe(write_issues_df, transformation='tr_provider_check', step_name='Write Issues', func_name='step_05_Write_Issues', required_columns=[])
    log_step_dataframe(write_issues_df, step_name='Write Issues', phase='after', transformation='tr_provider_check', func_name='step_05_Write_Issues')
    return write_issues_df

def run_tr_provider_check(spark, config=None):
    """Run tr_provider_check."""
    config = dict(config or {})
    logging.info("Starting transformation: tr_provider_check (tr_provider_check.ktr)")

    read_providers_df = step_01_Read_Providers(spark)
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_provider_check', step_name='Read Providers', func_name='step_01_Read_Providers', required_columns=[])
    read_providers_df = require_dataframe(read_providers_df, transformation='tr_provider_check', step_name='Find Inactive', func_name='step_02_Find_Inactive', required_columns=[])
    find_inactive_df, df_Flag_Issue = step_02_Find_Inactive(spark, read_providers_df)
    find_inactive_df = require_dataframe(find_inactive_df, transformation='tr_provider_check', step_name='Find Inactive', func_name='step_02_Find_Inactive', required_columns=[])
    df_Flag_Issue = require_dataframe(df_Flag_Issue, transformation='tr_provider_check', step_name='Flag Issue', func_name='step_03_Flag_Issue', required_columns=[])
    flag_issue_df = step_03_Flag_Issue(spark, df_Flag_Issue)
    flag_issue_df = require_dataframe(flag_issue_df, transformation='tr_provider_check', step_name='Flag Issue', func_name='step_03_Flag_Issue', required_columns=[])
    flag_issue_df = require_dataframe(flag_issue_df, transformation='tr_provider_check', step_name='Select Fields', func_name='step_04_Select_Fields', required_columns=[])
    select_fields_df = step_04_Select_Fields(spark, flag_issue_df)
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_provider_check', step_name='Select Fields', func_name='step_04_Select_Fields', required_columns=[])
    select_fields_df = require_dataframe(select_fields_df, transformation='tr_provider_check', step_name='Write Issues', func_name='step_05_Write_Issues', required_columns=[])
    write_issues_df = step_05_Write_Issues(spark, select_fields_df)
    write_issues_df = require_dataframe(write_issues_df, transformation='tr_provider_check', step_name='Write Issues', func_name='step_05_Write_Issues', required_columns=[])

    logging.info("Finished transformation: tr_provider_check")
    return write_issues_df



def run(spark: Any = None, config: Mapping[str, Any] | None = None) -> Any:
    """Run the 'jb_quality' transformation flow."""
    return execute_registered_job(
        'jb_quality',
        spark=spark,
        config_overrides=config,
        trans_runners={
            'Check Providers': run_tr_provider_check
        },
    )


if __name__ == "__main__":
    _spark = SparkSession.builder.appName('jb_quality').getOrCreate()
    run(_spark, None)
