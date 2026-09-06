# Databricks notebook source
# DBTITLE 1,Chapter 5 — Lab Title
# MAGIC %md
# MAGIC # Chapter 5 — SHACL Validation: Graph Quality Before Reasoning
# MAGIC
# MAGIC This lab applies **SHACL** (Shapes Constraint Language) constraints to the materialised knowledge graph from Chapter 4. SHACL validates data *before* reasoning — catching structural issues, missing properties, and governance flags that would otherwise propagate through OWL closure in Chapter 6.
# MAGIC
# MAGIC | Step | What it does |
# MAGIC | --- | --- |
# MAGIC | **Validate** | Run SHACL shapes against the instance data, collect the validation report |
# MAGIC | **Persist** | Extract findings from the report graph and write them to a UC Delta table |
# MAGIC | **Surface** | Query the results for dashboard tiles: severity rollup and violation worklist |
# MAGIC
# MAGIC **Key insight:** SHACL reports are themselves RDF graphs — they can be queried with SPARQL, joined back to the data graph, and persisted as structured telemetry.
# MAGIC
# MAGIC **Artifact produced:** `benoit_cayla.book.validation_results` — a Delta table of validation findings, append-only across runs.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# Install the two libraries for SHACL validation:
#   rdflib   — parse RDF graphs (data + shapes are both Turtle)
#   pyshacl  — W3C SHACL validator (also pulls in owlrl)
!pip install rdflib
!pip install pyshacl
import rdflib
dbutils.library.restartPython() 

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration — adjust to your environment:
CATALOG_SCHEMA = "benoit_cayla.book"

# COMMAND ----------

# DBTITLE 1,Part 1 — SHACL Validation
# MAGIC %md
# MAGIC ## Part 1 — SHACL Validation: Run the Shapes
# MAGIC
# MAGIC **SHACL** defines constraints (“shapes”) that data nodes must satisfy. Our `shapes.ttl` file encodes rules like:
# MAGIC * Every `Customer` must have exactly one `hasLifetimeValue`
# MAGIC * LTV must be `xsd:decimal` and ≥ 0
# MAGIC * A governance info-level flag for Premium customers above €50 000 (candidate for key-account review)
# MAGIC
# MAGIC The `run_validation()` function loads both the **data graph** (Chapter 4’s instance triples) and the **shapes graph**, then calls `pyshacl.validate()` with `inference="none"` — we validate the raw data, not its closure.
# MAGIC
# MAGIC **What to expect:** The report should flag `Customer4471` with an **Info** severity: her LTV (18 400) does not exceed 50 000, so the key-account review shape fires as informational, not as a violation.

# COMMAND ----------

# DBTITLE 1,Define the SHACL validation function
from rdflib import Graph
from pyshacl import validate

def run_validation(data_ttl: str, shapes_ttl: str):
    """Run SHACL validation and return (conforms, report_graph).
    The report_graph is itself RDF — queryable with SPARQL."""
    data   = Graph().parse(data_ttl,   format="turtle")   # Ch4 materialised triples
    shapes = Graph().parse(shapes_ttl, format="turtle")   # our SHACL shapes

    conforms, report_graph, report_text = validate(
        data_graph=data,
        shacl_graph=shapes,
        inference="none",          # validate RAW data; reasoning is Chapter 6
        advanced=True,             # enable SHACL-SPARQL constraints (sh:sparql)
        meta_shacl=False,
    )
    return conforms, report_graph   # report_graph is itself RDF

# COMMAND ----------

# DBTITLE 1,Execute validation against sample data
# Run validation: sample-data.ttl contains the instance triples,
# shapes.ttl contains the SHACL constraint shapes.
conforms, report = run_validation("sample-data.ttl", "shapes.ttl")

# COMMAND ----------

# DBTITLE 1,Print the SHACL validation report
# The report is an rdflib Graph — print() serialises it as Turtle.
# Look for sh:conforms, sh:resultSeverity, and sh:resultMessage.
report.print()

# COMMAND ----------

# DBTITLE 1,Results — Validation
# MAGIC %md
# MAGIC > **Results — Validation:** The report shows `sh:conforms false` with one finding:
# MAGIC > * **Focus node:** `Customer4471`
# MAGIC > * **Severity:** `sh:Info` (not a violation — informational governance flag)
# MAGIC > * **Message:** *"Premium customer above EUR 50,000 — candidate for key-account review"*
# MAGIC > * **Value:** 18 400 (below the 50 000 threshold, so the shape fires as info)
# MAGIC >
# MAGIC > No `sh:Violation` findings — the data is structurally clean. The `conforms: false` is because SHACL counts any finding (including Info) as non-conformance; filter on severity to distinguish real issues from governance flags.

# COMMAND ----------

# DBTITLE 1,Part 2 — Persist Results
# MAGIC %md
# MAGIC ## Part 2 — Persist: Write Findings to Unity Catalog
# MAGIC
# MAGIC SHACL reports are RDF graphs — we extract the structured findings via SPARQL (`focus_node`, `result_path`, `severity`, `message`) and write them to a Delta table. Each run appends with a timestamp, building an **audit trail** of validation history.
# MAGIC
# MAGIC This pattern makes validation results queryable by SQL dashboards and alerting pipelines, not just by graph tooling.

# COMMAND ----------

# DBTITLE 1,Extract findings via SPARQL and persist to Delta
# Extract structured findings from the SHACL report graph using SPARQL.
# Each finding has a focus_node, result_path, source_shape, severity, and message.
# We persist these as rows in a Delta table for SQL-based dashboarding.
from rdflib import Namespace
SH = Namespace("http://www.w3.org/ns/shacl#")

EXTRACT = """
PREFIX sh: <http://www.w3.org/ns/shacl#>
SELECT ?focus ?path ?shape ?severity ?message WHERE {
    ?r a sh:ValidationResult ;
       sh:focusNode      ?focus ;
       sh:resultSeverity ?severity ;
       sh:resultMessage  ?message .
    OPTIONAL { ?r sh:resultPath  ?path  }
    OPTIONAL { ?r sh:sourceShape ?shape }
}
"""

def persist(report_graph, spark, run_ts):
    rows = [
        (str(f), str(p), str(s), str(sev).split("#")[-1], str(m), run_ts)
        for f, p, s, sev, m in report_graph.query(EXTRACT)
    ]
    df = spark.createDataFrame(
        rows, ["focus_node","result_path","source_shape","severity","message","run_ts"]
    )
    (df.write.mode("append")
       .saveAsTable(CATALOG_SCHEMA + ".validation_results"))


# COMMAND ----------

# MAGIC %md
# MAGIC This cell defines the `persist()` function, which does two things:
# MAGIC
# MAGIC 1. **SPARQL extraction:** It queries the SHACL validation report graph (which is itself RDF) using a SPARQL `SELECT` query. For each `sh:ValidationResult` node it extracts:
# MAGIC    - `focus_node` — the data node that triggered the finding (e.g. `Customer4471`)
# MAGIC    - `result_path` — the property path involved (if any)
# MAGIC    - `source_shape` — which SHACL shape produced the finding
# MAGIC    - `severity` — `Violation`, `Warning`, or `Info` (extracted from the URI fragment)
# MAGIC    - `message` — the human-readable description of the finding
# MAGIC
# MAGIC 2. **Delta persistence:** It converts those extracted rows into a Spark DataFrame and **appends** them (`.mode("append")`) to the Unity Catalog table `benoit_cayla.book.validation_results`. This append-only pattern builds an audit trail across runs.

# COMMAND ----------

# DBTITLE 1,Persist findings to Unity Catalog
# Append findings to the validation_results table with a UTC timestamp.
# Each run adds rows; the table is an append-only audit trail.
from datetime import datetime, timezone
run_ts = datetime.now(timezone.utc).isoformat()
persist(report, spark, run_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This cell calls `persist(report, spark, run_ts)` with a UTC timestamp. The outcome is:
# MAGIC - The single **Info-level** finding for `Customer4471` (the governance flag about key-account review) is written as **one new row** in the `validation_results` Delta table.
# MAGIC - The `run_ts` column tags this row with the current run's timestamp, so downstream queries can filter to the latest run.
# MAGIC - Because the mode is `append`, **no existing rows are overwritten** — each validation run simply adds its findings to the table.
# MAGIC
# MAGIC > **Net result:** After these two cells execute, the table `benoit_cayla.book.validation_results` contains one new row with severity `Info`, referencing `Customer4471`, timestamped to this run. The table is now ready for the SQL dashboard queries in Part 3.

# COMMAND ----------

# DBTITLE 1,Part 3 — Dashboard Queries
# MAGIC %md
# MAGIC ## Part 3 — Surface: Dashboard Queries
# MAGIC
# MAGIC With findings persisted as a Delta table, standard SQL powers the governance dashboard:
# MAGIC * **Tile 1 — Severity rollup:** count of findings by severity for the latest run (Violation / Warning / Info)
# MAGIC * **Tile 2 — Violation worklist:** the actionable items — only `Violation`-severity findings from the latest run
# MAGIC
# MAGIC An empty worklist means the data is clean. Info-level findings are governance telemetry, not blockers.

# COMMAND ----------

# DBTITLE 1,Tile 1 — Severity rollup for latest run
# MAGIC %sql
# MAGIC -- Tile 1: severity rollup for the most recent validation run.
# MAGIC -- Groups findings by severity (Violation > Warning > Info) for a dashboard gauge.
# MAGIC WITH latest AS (SELECT max(run_ts) AS ts FROM benoit_cayla.book.validation_results)
# MAGIC SELECT severity, count(*) AS findings
# MAGIC FROM   benoit_cayla.book.validation_results
# MAGIC WHERE  run_ts = (SELECT ts FROM latest)
# MAGIC GROUP  BY severity
# MAGIC ORDER  BY CASE severity WHEN 'Violation' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END;

# COMMAND ----------

# DBTITLE 1,Tile 2 — Open-violation worklist
# MAGIC %sql
# MAGIC -- Tile 2: the open-violation worklist (latest run, Violation severity only).
# MAGIC -- An empty result means the data is clean — no structural issues to fix.
# MAGIC WITH latest AS (SELECT max(run_ts) AS ts FROM benoit_cayla.book.validation_results)
# MAGIC SELECT focus_node, source_shape, message
# MAGIC FROM   benoit_cayla.book.validation_results
# MAGIC WHERE  run_ts = (SELECT ts FROM latest) AND severity = 'Violation'
# MAGIC ORDER  BY source_shape;

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC > The knowledge graph has been **validated before reasoning**:
# MAGIC > * **0 violations** — the data is structurally clean (all required properties present, types correct)
# MAGIC > * **1 info-level finding** — `Customer4471` flagged for potential key-account review (LTV governance rule)
# MAGIC > * **Findings persisted** to `benoit_cayla.book.validation_results` with run timestamps for audit trails
# MAGIC > * **Dashboard-ready** — severity rollup and violation worklist queries power governance tiles
# MAGIC >
# MAGIC > This validation gate is critical: OWL closure (Chapter 6) **amplifies** data issues — a missing type triple becomes hundreds of spurious inferences. Validate with SHACL first, reason second.