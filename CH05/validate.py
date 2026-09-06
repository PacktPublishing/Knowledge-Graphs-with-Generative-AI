"""labs/ch05/validate.py

Chapter 5 validation pipeline for the Customer 360 graph.

The intellectual content lives in shapes.ttl (the SHACL checklist). This script
is glue: it hands the materialized graph and the shapes to pySHACL, then turns
the resulting RDF validation report into graph-native rows that a Databricks SQL
/ Lakeview dashboard can read.

Consumes : the materialized Customer 360 triples from Chapter 4
           (Delta table ontobricks.customer360.triples, or a local .ttl slice).
Produces : Delta table ontobricks.customer360.validation_results
           (one row per sh:ValidationResult), the chapter's headline artifact alongside shapes.ttl.

Run locally (no Databricks needed) for a quick conformance check:
    python validate.py
Inject the deliberate defect from the chapter and prove the loop catches it:
    python validate.py --break
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q", "rdflib", "pyshacl"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

from rdflib import Graph, Namespace
from pyshacl import validate

SH   = Namespace("http://www.w3.org/ns/shacl#")
C360 = Namespace("https://ontobricks.example/customer360#")

SHAPES_FILE = "shapes.ttl"
DATA_FILE   = "sample-data.ttl"        # stands in for the Ch4 Delta table locally
RESULTS_TABLE = "benoit_cayla.book.validation_results"

# SPARQL that shreds the RDF validation report into flat rows.
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


# --------------------------------------------------------------------------- #
# Core: validate a data graph against the shapes graph.
# inference="none" is deliberate -- this step is the INSPECTOR, not the
# REASONER. Reasoning (Chapter 6) would type premium customers before we look,
# so PremiumConsistencyShape would never fire. advanced=True enables the
# SHACL-SPARQL constraint.
# --------------------------------------------------------------------------- #
def run_validation_graph(data: Graph, shapes_ttl: str = SHAPES_FILE):
    shapes = Graph().parse(shapes_ttl, format="turtle")
    conforms, report_graph, _ = validate(
        data_graph=data,
        shacl_graph=shapes,
        inference="none",
        advanced=True,
        meta_shacl=False,
    )
    return conforms, report_graph


def run_validation(data_ttl: str = DATA_FILE, shapes_ttl: str = SHAPES_FILE):
    data = Graph().parse(data_ttl, format="turtle")
    return run_validation_graph(data, shapes_ttl)


# --------------------------------------------------------------------------- #
# Databricks: load the materialized graph out of the Chapter 4 Delta table.
# The triples table is assumed to carry (s, p, o, o_is_literal, o_datatype).
# --------------------------------------------------------------------------- #
def load_graph_from_delta(spark, table: str = "ontobricks.customer360.triples") -> Graph:
    from rdflib import URIRef, Literal
    g = Graph()
    for row in spark.table(table).collect():
        s = URIRef(row["s"])
        p = URIRef(row["p"])
        o = Literal(row["o"], datatype=URIRef(row["o_datatype"])) if row["o_is_literal"] \
            else URIRef(row["o"])
        g.add((s, p, o))
    return g


# --------------------------------------------------------------------------- #
# Close the loop: persist the report graph as queryable rows.
# Append mode so each run extends the quality time series.
# --------------------------------------------------------------------------- #
def persist(report_graph: Graph, spark, run_ts: str):
    rows = [
        (str(f), str(p) if p else None, str(sh) if sh else None,
         str(sev).split("#")[-1], str(m), run_ts)
        for f, p, sh, sev, m in report_graph.query(EXTRACT)
    ]
    df = spark.createDataFrame(
        rows,
        ["focus_node", "result_path", "source_shape", "severity", "message", "run_ts"],
    )
    (df.write.mode("append").saveAsTable(RESULTS_TABLE))
    return len(rows)


def validate_and_persist(spark, source_table: str = "benoit_cayla.book.kg_triples"):
    """The full Databricks entry point: load -> validate -> write back."""
    data = load_graph_from_delta(spark, source_table)
    conforms, report = run_validation_graph(data)
    run_ts = datetime.now(timezone.utc).isoformat()
    n = persist(report, spark, run_ts)
    print(f"conforms={conforms}  findings_persisted={n}  run_ts={run_ts}")
    return conforms


# --------------------------------------------------------------------------- #
# Local CLI: conformance check, and the deliberate-defect verification.
# --------------------------------------------------------------------------- #
def _summarize(report: Graph):
    counts = {}
    for (sev,) in report.query(
        "PREFIX sh: <http://www.w3.org/ns/shacl#> "
        "SELECT ?sev WHERE { ?r a sh:ValidationResult ; sh:resultSeverity ?sev }"
    ):
        key = str(sev).split("#")[-1]
        counts[key] = counts.get(key, 0) + 1
    return counts


def main():
    if "--break" in sys.argv:
        # Inject the chapter's deliberate defect: strip Customer4471's region.
        broken = Graph().parse(DATA_FILE, format="turtle")
        broken.remove((C360.Customer4471, C360.isLocatedIn, None))
        conforms, report = run_validation_graph(broken)
        assert conforms is False, "Expected the broken graph to FAIL validation"
        hits = list(report.query(
            "PREFIX sh: <http://www.w3.org/ns/shacl#> "
            "PREFIX c360: <https://ontobricks.example/customer360#> "
            "SELECT ?msg WHERE { ?r sh:focusNode c360:Customer4471 ; "
            "sh:resultPath c360:isLocatedIn ; sh:resultMessage ?msg . }"
        ))
        assert hits, "Expected a violation on Customer4471's isLocatedIn"
        print("Caught it:", str(hits[0][0]))
        print("Severity counts:", _summarize(report))
    else:
        conforms, report = run_validation()
        print(f"conforms={conforms}")
        print("Severity counts:", _summarize(report))


if __name__ == "__main__":
    sys.argv = ["--break"]
    main()
