"""Tests for generated PySpark code navigation metadata."""

from types import SimpleNamespace

from pentaho_converter.code_navigation import build_code_navigation

_SAMPLE_CODE = '''"""
Auto-generated
"""

# ============================================================
# Transformation: tr_extract_customers
# Source: tr_extract_customers.ktr
# ============================================================

def run_tr_extract_customers(spark):
    pass

# ============================================================
# Transformation: tr_build_fact_sales
# Source: tr_build_fact_sales.ktr
# ============================================================

def run_tr_build_fact_sales(spark):
    pass
'''

_STEP_CODE = '''"""
Auto-generated
"""

# ============================================================
# Transformation: tr_extract_customers
# Source: tr_extract_customers.ktr
# ============================================================

def run_tr_extract_customers(spark):
    # Step: Filter customers (FilterRows) [failed]
    # Filter Rows: Filter customers
    df_filter = spark.createDataFrame([], '_placeholder STRING')

    # Step: Calc amounts (Calculator) [failed]
    # Calculator: Calc amounts
    df_calc = spark.createDataFrame([], '_calculator_unresolved STRING')
'''

_FILTER_RESULT = SimpleNamespace(
    step_name="Filter customers",
    step_type="FilterRows",
    status="failed",
    errors=["FilterRows must generate a .filter() call."],
    warnings=[],
    detail="FilterRows must generate a .filter() call.",
)

_CONVERTED_RESULT = SimpleNamespace(
    step_name="Read customers",
    step_type="TableInput",
    status="converted",
    errors=[],
    warnings=[],
    detail="Transformation: tr_extract_customers",
    semantic_score=1.0,
)

_CONVERTED_STEP_CODE = '''"""
Auto-generated
"""

def run_tr_extract_customers(spark):
    # Step: Read customers (TableInput) [converted]
    # Table Input: Read customers
    df_read = spark.read.csv("customers.csv", header=True)
'''

_CALC_RESULT = SimpleNamespace(
    step_name="Calc amounts",
    step_type="Calculator",
    status="failed",
    errors=["No calculations found in XML."],
    warnings=[],
    detail="No calculations found in XML.",
)

_LINEAGE = {
    "root_jobs": ["Master.kjb"],
    "nodes": [
        {
            "id": "Master.kjb",
            "file": "Master.kjb",
            "name": "Master",
            "type": "job",
            "children": ["tr_extract_customers.ktr", "job_nested.kjb"],
        },
        {
            "id": "job_nested.kjb",
            "file": "job_nested.kjb",
            "name": "job_nested",
            "type": "job",
            "children": ["tr_build_fact_sales.ktr"],
        },
        {
            "id": "tr_extract_customers.ktr",
            "file": "tr_extract_customers.ktr",
            "name": "tr_extract_customers",
            "type": "transformation",
            "children": [],
        },
        {
            "id": "tr_build_fact_sales.ktr",
            "file": "tr_build_fact_sales.ktr",
            "name": "tr_build_fact_sales",
            "type": "transformation",
            "children": [],
        },
    ],
    "edges": [
        {"from": "Master.kjb", "to": "tr_extract_customers.ktr", "sequence": 0},
        {"from": "Master.kjb", "to": "job_nested.kjb", "sequence": 1},
        {"from": "job_nested.kjb", "to": "tr_build_fact_sales.ktr", "sequence": 2},
    ],
}

_INVENTORY = [
    {
        "file": "tr_extract_customers.ktr",
        "name": "tr_extract_customers",
        "type": ".ktr",
    },
    {
        "file": "tr_build_fact_sales.ktr",
        "name": "tr_build_fact_sales",
        "type": ".ktr",
    },
    {
        "file": "Master.kjb",
        "name": "Master",
        "type": ".kjb",
    },
    {
        "file": "job_nested.kjb",
        "name": "job_nested",
        "type": ".kjb",
    },
]


def test_maps_ktr_file_to_transformation_section_line():
    nav = build_code_navigation(_SAMPLE_CODE, _LINEAGE, _INVENTORY)
    section = nav["sections_by_file"]["tr_extract_customers.ktr"]
    assert section["line"] == 6
    assert "tr_extract_customers" in section["anchor"]


def test_maps_kjb_file_to_first_child_transformation():
    nav = build_code_navigation(_SAMPLE_CODE, _LINEAGE, _INVENTORY)
    section = nav["sections_by_file"]["Master.kjb"]
    assert section["line"] == 6
    assert section["via"] == "first_transformation"
    assert section["anchor"] == "# Job: Master"
    assert section["section_type"] == "job"


def test_maps_job_section_when_present():
    code = _SAMPLE_CODE.replace(
        "# Transformation: tr_extract_customers",
        "# Job: Master\n# Transformation: tr_extract_customers",
        1,
    )
    nav = build_code_navigation(code, _LINEAGE, _INVENTORY)
    section = nav["sections_by_file"]["Master.kjb"]
    assert section["line"] == 6
    assert section["anchor"] == "# Job: Master"
    assert section.get("via") is None


def test_maps_steps_to_precise_generated_lines():
    step_to_file = {
        "Filter customers": "tr_extract_customers.ktr",
        "Calc amounts": "tr_extract_customers.ktr",
    }
    nav = build_code_navigation(
        _STEP_CODE,
        _LINEAGE,
        _INVENTORY,
        step_to_file=step_to_file,
        step_results=[_FILTER_RESULT, _CALC_RESULT],
        generated_file="project.py",
    )
    filter_step = nav["steps_by_name"]["Filter customers"]
    assert filter_step["file"] == "project.py"
    assert filter_step["issues"][0]["message"] == "FilterRows must generate a .filter() call."
    assert filter_step["issues"][0]["line"] == 13
    assert filter_step["issues"][0]["expected"] == "customers.filter(...)"

    calc_step = nav["steps_by_name"]["Calc amounts"]
    assert calc_step["issues"][0]["message"] == "No calculations found in XML."
    assert calc_step["issues"][0]["line"] == 16


def test_converted_step_uses_success_highlight():
    nav = build_code_navigation(
        _CONVERTED_STEP_CODE,
        _LINEAGE,
        _INVENTORY,
        step_results=[_CONVERTED_RESULT],
        generated_file="project.py",
    )
    step = nav["steps_by_name"]["Read customers"]
    assert step["highlight_level"] == "success"
    assert step["score"] == 100
    assert step["issues"] == []


def test_failed_step_uses_error_highlight():
    nav = build_code_navigation(
        _STEP_CODE,
        _LINEAGE,
        _INVENTORY,
        step_results=[_FILTER_RESULT, _CALC_RESULT],
        generated_file="project.py",
    )
    assert nav["steps_by_name"]["Filter customers"]["highlight_level"] == "error"
    assert nav["steps_by_name"]["Calc amounts"]["highlight_level"] == "error"


def test_outline_preserves_nested_jobs():
    nav = build_code_navigation(_SAMPLE_CODE, _LINEAGE, _INVENTORY)
    root = nav["outline"][0]
    assert root["id"] == "Master.kjb"
    child_ids = [child["id"] for child in root["children"]]
    assert "tr_extract_customers.ktr" in child_ids
    assert "job_nested.kjb" in child_ids
