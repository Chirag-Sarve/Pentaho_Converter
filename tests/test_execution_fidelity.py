"""Regression: generated execution must match Pentaho job/trans graphs.

Covers Master_ETL orchestration, nested jobs, multi Text File Outputs,
FilterRows true/false branches, and ReplaceString (KTR type id).
"""

from __future__ import annotations

import io
import re
import tempfile
import textwrap
import unittest
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from pentaho_converter.graph import StepDAG
from pentaho_converter.models import PentahoHop, PentahoStep, PentahoTransformation
from pentaho_converter.pipeline import convert_pentaho_project
from pentaho_converter.project_generator import DatabricksProjectGenerator
from pentaho_converter.step_context import StepContext
from pentaho_converter.steps.base import build_default_registry


TEST_104 = Path(r"c:\Users\Prateek.Kotian\Desktop\Pentaho\Test_104")


def _zip_dir(root: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path in root.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(root).as_posix())
    return buf.getvalue()


class TestReplaceStringAlias(unittest.TestCase):
    def test_replacestring_ktr_type_generates_regexp_replace(self):
        xml = textwrap.dedent(
            """
            <step>
              <name>Normalize Notes</name>
              <type>ReplaceString</type>
              <fields>
                <field>
                  <in_stream_name>notes</in_stream_name>
                  <out_stream_name>notes_clean</out_stream_name>
                  <use_regex>N</use_regex>
                  <replace_string>high risk</replace_string>
                  <replace_by_string>HIGH_RISK</replace_by_string>
                  <case_sensitive>N</case_sensitive>
                </field>
              </fields>
            </step>
            """
        ).strip()
        el = ET.fromstring(xml)
        step = PentahoStep(
            name="Normalize Notes",
            step_type="ReplaceString",
            attributes={},
            raw_element=el,
        )
        src = PentahoStep(name="Read", step_type="TextFileInput", attributes={}, raw_element=None)
        trans = PentahoTransformation(name="t", file_path=Path("t.ktr"))
        trans.steps = [src, step]
        hops = [PentahoHop(from_name="Read", to_name="Normalize Notes")]
        dag = StepDAG(trans.steps, hops)
        ctx = StepContext(
            transformation=trans,
            step=step,
            dag=dag,
            df_variable_map={"Read": "read_df", "Normalize Notes": "normalize_notes_df"},
            extra={"input_columns": ["notes", "patient_id"]},
        )
        lines, status = build_default_registry().generate_code("ReplaceString", ctx)
        code = "\n".join(lines)
        self.assertIn(status, ("converted", "partial", "partially_supported"))
        self.assertIn("notes_clean", code)
        self.assertIn("regexp_replace", code)
        self.assertNotIn("_placeholder", code)


class TestFilterRowsInlinedBranches(unittest.TestCase):
    def test_inlined_filter_returns_both_branch_dataframes(self):
        ktr = textwrap.dedent(
            """\
            <?xml version="1.0" encoding="UTF-8"?>
            <transformation>
              <info><name>tr_branch</name></info>
              <order>
                <hop><from>Read</from><to>Route</to><enabled>Y</enabled></hop>
                <hop><from>Route</from><to>Ok Path</to><enabled>Y</enabled></hop>
                <hop><from>Route</from><to>Bad Path</to><enabled>Y</enabled></hop>
                <hop><from>Ok Path</from><to>Merge</to><enabled>Y</enabled></hop>
                <hop><from>Bad Path</from><to>Merge</to><enabled>Y</enabled></hop>
                <hop><from>Merge</from><to>Write Out</to><enabled>Y</enabled></hop>
              </order>
              <step>
                <name>Read</name><type>RowGenerator</type>
                <fields><field><name>status</name><type>String</type></field></fields>
                <limit>1</limit>
              </step>
              <step>
                <name>Route</name><type>FilterRows</type>
                <send_true_to>Ok Path</send_true_to>
                <send_false_to>Bad Path</send_false_to>
                <compare>
                  <condition>
                    <negated>N</negated>
                    <leftvalue>status</leftvalue>
                    <function>=</function>
                    <value><name>c</name><type>String</type><text>A</text></value>
                  </condition>
                </compare>
              </step>
              <step>
                <name>Ok Path</name><type>Constant</type>
                <fields><field><name>bucket</name><type>String</type><value>OK</value></field></fields>
              </step>
              <step>
                <name>Bad Path</name><type>Constant</type>
                <fields><field><name>bucket</name><type>String</type><value>BAD</value></field></fields>
              </step>
              <step>
                <name>Merge</name><type>Append</type>
                <head_name>Ok Path</head_name>
                <tail_name>Bad Path</tail_name>
              </step>
              <step>
                <name>Write Out</name><type>TextFileOutput</type>
                <file><name>${Internal.Transformation.Filename.Directory}/out</name><extention>csv</extention></file>
                <fields><field><name>status</name></field><field><name>bucket</name></field></fields>
                <header>Y</header>
              </step>
            </transformation>
            """
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Transformations").mkdir()
            (root / "Jobs").mkdir()
            (root / "Transformations" / "tr_branch.ktr").write_text(ktr, encoding="utf-8")
            job = textwrap.dedent(
                """\
                <?xml version="1.0" encoding="UTF-8"?>
                <job>
                  <name>jb_branch</name>
                  <entries>
                    <entry><name>Start</name><type>SPECIAL</type><start>Y</start></entry>
                    <entry>
                      <name>Run Branch</name><type>TRANS</type>
                      <filename>${Internal.Job.Filename.Directory}/../Transformations/tr_branch.ktr</filename>
                      <transname>tr_branch</transname>
                    </entry>
                    <entry><name>Success</name><type>SUCCESS</type></entry>
                  </entries>
                  <hops>
                    <hop><from>Start</from><to>Run Branch</to><enabled>Y</enabled>
                      <evaluation>Y</evaluation><unconditional>Y</unconditional></hop>
                    <hop><from>Run Branch</from><to>Success</to><enabled>Y</enabled>
                      <evaluation>Y</evaluation><unconditional>Y</unconditional></hop>
                  </hops>
                </job>
                """
            )
            (root / "Jobs" / "jb_branch.kjb").write_text(job, encoding="utf-8")
            result = convert_pentaho_project(_zip_dir(root), "BranchProj")

        job_py = next(v for k, v in result.files.items() if k.endswith("jobs/jb_branch.py"))
        self.assertIn("df_Ok_Path = ", job_py)
        self.assertIn("df_Bad_Path = ", job_py)
        # Filter step must return both branch streams (not discard locals)
        self.assertRegex(
            job_py,
            r"return\s+route_df,\s*df_Ok_Path,\s*df_Bad_Path"
            r"|return\s+\w+_df,\s*df_Ok_Path,\s*df_Bad_Path",
        )
        # Downstream Constant steps must consume branch streams, not filter primary
        self.assertIn("df_Ok_Path", job_py)
        self.assertIn("df_Bad_Path", job_py)
        self.assertIn(".csv(", job_py)
        self.assertIn("TextFileOutput", job_py)


@unittest.skipUnless(TEST_104.is_dir(), "Test_104 reference project not present")
class TestTest104ExecutionGraph(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = convert_pentaho_project(_zip_dir(TEST_104), "Test_104_1")

    def test_master_etl_runs_primary_not_children_only(self):
        master = self.result.files["Test_104_1/Master_ETL.py"]
        self.assertIn("from jobs.jb_master import run as _run_primary", master)
        self.assertIn("_run_primary(spark, cfg)", master)
        self.assertNotIn('orchestrated": "child_jobs"', master)
        self.assertNotIn("from jobs.jb_prep import run as jb_prep_job", master)

    def test_all_seven_text_file_outputs_generated(self):
        outputs = [
            "patients_clean_out.csv",
            "clinics",  # clinics normalize out
            "provider",  # provider check out
            "visits_enriched_out.csv",
            "labs_rollup_out.csv",
            "vaccinations_routed_out.csv",
            "outbreak_watchlist_out.csv",
        ]
        all_code = "\n".join(
            v for k, v in self.result.files.items() if k.endswith(".py")
        )
        for fragment in outputs:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, all_code)

    def test_patient_clean_replace_string_not_passthrough(self):
        prep = self.result.files["Test_104_1/jobs/jb_prep.py"]
        self.assertIn("notes_clean", prep)
        self.assertIn("regexp_replace", prep)
        # Normalize Notes must not be an empty passthrough
        m = re.search(
            r"def step_\d+_Normalize_Notes\(.*?\n(.*?)(?=\ndef step_|\ndef run_)",
            prep,
            flags=re.S,
        )
        self.assertIsNotNone(m)
        body = m.group(1)
        self.assertIn("notes_clean", body)
        self.assertIn("regexp_replace", body)

    def test_patients_clean_out_filter_true_only_assigns_branch_stream(self):
        """Keep Active Roster has send_true_to only — must define df_Select_Fields."""
        prep = self.result.files["Test_104_1/jobs/jb_prep.py"]
        m = re.search(
            r"def step_\d+_Keep_Active_Roster\(.*?\n(.*?)(?=\ndef step_|\ndef run_)",
            prep,
            flags=re.S,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn("df_Select_Fields = ", body)
        self.assertIn("return keep_active_roster_df, df_Select_Fields", body)
        self.assertIn("patients_clean_out.csv", prep)

    def test_clinics_norm_out_filter_true_only_assigns_branch_stream(self):
        """Keep Active Providers → Map Clinic Type (true-only FilterRows)."""
        prep = self.result.files["Test_104_1/jobs/jb_prep.py"]
        m = re.search(
            r"def step_\d+_Keep_Active_Providers\(.*?\n(.*?)(?=\ndef step_|\ndef run_)",
            prep,
            flags=re.S,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn("df_Map_Clinic_Type = ", body)
        self.assertIn("return keep_active_providers_df, df_Map_Clinic_Type", body)
        self.assertIn("clinics_norm_out.csv", prep)

    def test_provider_issues_out_filter_true_only_assigns_branch_stream(self):
        """Find Inactive → Flag Issue (true-only FilterRows)."""
        quality = self.result.files["Test_104_1/jobs/jb_quality.py"]
        m = re.search(
            r"def step_\d+_Find_Inactive\(.*?\n(.*?)(?=\ndef step_|\ndef run_)",
            quality,
            flags=re.S,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn("df_Flag_Issue = ", body)
        self.assertIn("return find_inactive_df, df_Flag_Issue", body)
        self.assertIn("provider_issues_out.csv", quality)

    def test_outbreak_watchlist_out_filter_true_only_assigns_branch_stream(self):
        """Flag Watchlist → Add Risk Label (true-only FilterRows)."""
        master_job = self.result.files["Test_104_1/jobs/jb_master.py"]
        m = re.search(
            r"def step_\d+_Flag_Watchlist\(.*?\n(.*?)(?=\ndef step_|\ndef run_)",
            master_job,
            flags=re.S,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn("df_Add_Risk_Label = ", body)
        self.assertIn("return flag_watchlist_df, df_Add_Risk_Label", body)
        self.assertIn("outbreak_watchlist_out.csv", master_job)

    def test_vaccination_route_preserves_filter_branches(self):
        master_job = self.result.files["Test_104_1/jobs/jb_master.py"]
        self.assertIn("df_Label_Administered", master_job)
        self.assertIn("df_Label_Exception", master_job)
        self.assertRegex(
            master_job,
            r"return\s+route_administered_df,\s*df_Label_Administered,\s*df_Label_Exception",
        )
        # Label Exception must take the false-branch stream
        self.assertIn(
            "step_26_Label_Exception(spark, df_Label_Exception)",
            master_job,
        )

    def test_job_specs_include_nested_and_master_trans_entries(self):
        specs = self.result.files["Test_104_1/engine/job_specs.py"]
        for name in (
            "Enrich Visits",
            "Rollup Labs",
            "Route Vaccinations",
            "Flag Outbreak Risk",
            "Clean Patients",
            "Normalize Clinics",
            "Check Providers",
            "Run Prep Job",
            "Run Quality Job",
        ):
            with self.subTest(entry=name):
                self.assertIn(name, specs)

    def test_primary_job_selection_prefers_jb_master(self):
        master = self.result.files["Test_104_1/Master_ETL.py"]
        self.assertIn("jb_master", master)


class TestMasterEtlAlwaysRunsPrimary(unittest.TestCase):
    def test_generator_emits_primary_even_when_children_exist(self):
        """Unit-level: primary with nested JOB + TRANS must call primary module."""
        from pentaho_converter.models import PentahoJob, PentahoJobEntry, PentahoHop

        primary = PentahoJob(name="jb_master", file_path=Path("Jobs/jb_master.kjb"))
        primary.entries = [
            PentahoJobEntry(name="Start", entry_type="SPECIAL", is_start=True),
            PentahoJobEntry(
                name="Run Prep Job",
                entry_type="JOB",
                filename="jb_prep.kjb",
                jobname="jb_prep",
            ),
            PentahoJobEntry(
                name="Enrich Visits",
                entry_type="TRANS",
                filename="tr_visit_enrich.ktr",
                transname="tr_visit_enrich",
            ),
            PentahoJobEntry(name="Success", entry_type="SUCCESS"),
        ]
        primary.hops = [
            PentahoHop(from_name="Start", to_name="Run Prep Job", enabled=True),
            PentahoHop(from_name="Run Prep Job", to_name="Enrich Visits", enabled=True),
            PentahoHop(from_name="Enrich Visits", to_name="Success", enabled=True),
        ]
        child = PentahoJob(name="jb_prep", file_path=Path("Jobs/jb_prep.kjb"))
        child.entries = [
            PentahoJobEntry(name="Start", entry_type="SPECIAL", is_start=True),
            PentahoJobEntry(name="Success", entry_type="SUCCESS"),
        ]
        jobs = {
            str(primary.file_path): primary,
            str(child.file_path): child,
        }
        gen = DatabricksProjectGenerator()
        logs: list[str] = []
        src = gen._generate_master_etl(
            root="Pkg",
            primary=primary,
            jobs=jobs,
            job_index={"jb_prep": child, "jb_master": primary},
            transformations=[],
            ordered_transformations=[],
            logs=logs,
        )
        self.assertIn("from jobs.jb_master import run as _run_primary", src)
        self.assertIn("_run_primary(spark, cfg)", src)
        self.assertNotIn("jb_prep_job", src)


if __name__ == "__main__":
    unittest.main()
