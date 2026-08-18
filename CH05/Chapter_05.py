# Databricks notebook source
!pip install rdflib
!pip install pyshacl
import rdflib
dbutils.library.restartPython() 

# COMMAND ----------

# Replace your Catalog.schema with yours:
CATALOG_SCHEMA = "benoit_cayla.book"

# COMMAND ----------

# DBTITLE 1,Step 2: Run the validation
from rdflib import Graph
from pyshacl import validate

def run_validation(data_ttl: str, shapes_ttl: str):
    data   = Graph().parse(data_ttl,   format="turtle")   # Ch4 materialized triples
    shapes = Graph().parse(shapes_ttl, format="turtle")   # our shapes.ttl

    conforms, report_graph, report_text = validate(
        data_graph=data,
        shacl_graph=shapes,
        inference="none",          # we VALIDATE here; reasoning is Chapter 6
        advanced=True,             # enable SHACL-SPARQL constraints
        meta_shacl=False,
    )
    return conforms, report_graph   # report_graph is itself RDF

# COMMAND ----------

conforms, report = run_validation("sample-data.ttl", "shapes.ttl")

# COMMAND ----------

# DBTITLE 1,Step 3: Write the report back as graph-native triples
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

from datetime import datetime, timezone
run_ts = datetime.now(timezone.utc).isoformat()
persist(report, spark, run_ts)

# COMMAND ----------

# DBTITLE 1,Step 4: Surface violations to a dashboard
# MAGIC %sql
# MAGIC -- Tile 1: severity rollup for the most recent run
# MAGIC WITH latest AS (SELECT max(run_ts) AS ts FROM benoit_cayla.book.validation_results)
# MAGIC SELECT severity, count(*) AS findings
# MAGIC FROM   benoit_cayla.book.validation_results
# MAGIC WHERE  run_ts = (SELECT ts FROM latest)
# MAGIC GROUP  BY severity
# MAGIC ORDER  BY CASE severity WHEN 'Violation' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tile 2: the open-violation worklist (latest run, violations first)
# MAGIC WITH latest AS (SELECT max(run_ts) AS ts FROM benoit_cayla.book.validation_results)
# MAGIC SELECT focus_node, source_shape, message
# MAGIC FROM   benoit_cayla.book.validation_results
# MAGIC WHERE  run_ts = (SELECT ts FROM latest) AND severity = 'Violation'
# MAGIC ORDER  BY source_shape;