# Databricks notebook source
# DBTITLE 1,Chapter 4 — Lab Title
# MAGIC %md
# MAGIC # Chapter 4 — Materialising the Knowledge Graph into Unity Catalog
# MAGIC
# MAGIC This lab turns relational tables into a **knowledge graph** stored as Delta triples in Unity Catalog. The approach follows the W3C R2RML standard: a declarative mapping describes how rows become RDF triples, then Spark column expressions execute that mapping at scale.
# MAGIC
# MAGIC | Step | What it does |
# MAGIC | --- | --- |
# MAGIC | **Source tables** | Create and populate the `customers` table (plus orders, products, accounts) |
# MAGIC | **R2RML mapping** | Parse the declarative mapping that defines how rows → triples |
# MAGIC | **Materialisation** | Lower R2RML triples-maps to Spark, union, and write to `kg_triples` |
# MAGIC | **Verification** | Query the graph: per-class counts, Customer4471’s subgraph, two-hop walk |
# MAGIC
# MAGIC **Key outcome:** A single Delta table `kg_triples` with columns `(subject, predicate, object, object_kind)` that downstream chapters (5–6) consume as the knowledge graph.
# MAGIC
# MAGIC **Artifact produced:** `benoit_cayla.book.kg_triples`

# COMMAND ----------

# DBTITLE 1,Part 1 — Create source tables and populate with sample data
# MAGIC %sql
# MAGIC
# MAGIC -- Part 1 — Source Tables: the relational starting point.
# MAGIC -- These are the tables the R2RML mapping will read from.
# MAGIC -- Create your Catalog & schema by removing the -- below (if they do not already exist)
# MAGIC --CREATE CATALOG IF NOT EXISTS benoit_cayla;
# MAGIC --CREATE SCHEMA IF NOT EXISTS book;
# MAGIC
# MAGIC -- Replace benoit_cayla.book by your catalog.schema below:
# MAGIC DECLARE OR REPLACE VARIABLE schema_prefix STRING DEFAULT 'benoit_cayla.book';
# MAGIC
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(schema_prefix || '.customers') (
# MAGIC     customer_id INT, full_name STRING, lifetime_value DECIMAL(12,2),
# MAGIC     status STRING, created_at TIMESTAMP, region_code STRING);
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(schema_prefix || '.customers') VALUES
# MAGIC   (4471, 'Acme Corp',     18400.00, 'active',    TIMESTAMP'2023-02-11 09:00:00', 'EMEA'),
# MAGIC   (5012, 'Bluewave Ltd',   9200.00, 'active',    TIMESTAMP'2024-06-01 14:30:00', 'EMEA'),
# MAGIC   (5519, 'Cedar Foods',   21750.00, 'active',    TIMESTAMP'2022-11-20 08:15:00', 'AMER'),
# MAGIC   (6033, 'Delta Mobility', 4300.00, 'suspended', TIMESTAMP'2025-01-09 16:45:00', 'APAC');
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install rdflib
# rdflib is needed to parse the R2RML mapping (which is itself RDF/Turtle)
!pip install rdflib
import rdflib
dbutils.library.restartPython() 

# COMMAND ----------

# DBTITLE 1,Configuration paths
# Configuration — adjust these to your environment:
CATALOG_SCHEMA = "benoit_cayla.book"
CUSTOMER_TABLE= CATALOG_SCHEMA + ".customers"
# Replace this folder path by yours:
COMMON_PATH = "/Workspace/Users/benoit.cayla@databricks.com/book/Common"

# COMMAND ----------

# DBTITLE 1,Part 2 — R2RML Mapping
# MAGIC %md
# MAGIC ## Part 2 — R2RML Mapping: Declaring How Rows Become Triples
# MAGIC
# MAGIC **R2RML** (RDB-to-RDF Mapping Language) is a W3C standard that declaratively specifies how relational rows map to RDF triples. The mapping file `mapping.r2rml.ttl` defines one **TriplesMap** per source table, each containing:
# MAGIC * A **logical table** — the SQL source (table or query)
# MAGIC * A **subject map** — the IRI template for each row (e.g. `c360:Customer{customer_id}`)
# MAGIC * One or more **predicate-object maps** — the properties and their values
# MAGIC
# MAGIC Parsing this file confirms the mapping is syntactically valid before we execute it.

# COMMAND ----------

# DBTITLE 1,Parse and validate the R2RML mapping
# Load the R2RML mapping into an rdflib Graph. parse() raises on malformed
# Turtle, so a successful parse is our first quality gate on the mapping.
import rdflib
m = rdflib.Graph()
m.parse(COMMON_PATH + "/mapping.r2rml.ttl", format="turtle")
print(f"Mapping parsed cleanly: {len(m)} R2RML triples")

# COMMAND ----------

# DBTITLE 1,Part 3 — Materialisation
# MAGIC %md
# MAGIC ## Part 3 — Materialisation: R2RML → Spark → Delta
# MAGIC
# MAGIC Each R2RML **triples-map** is lowered to Spark column expressions:
# MAGIC * `iri(key_expr)` builds a full IRI from a concatenation of the namespace and a key column
# MAGIC * One `select()` per predicate-object pair produces a 4-column DataFrame: `(subject, predicate, object, object_kind)`
# MAGIC * All DataFrames are unioned, deduplicated, and written as a single Delta table: `kg_triples`
# MAGIC
# MAGIC **What to expect:** 12 triples materialised (4 customers × 3 predicates: type, hasLifetimeValue, isLocatedIn). With orders, products, and accounts the count grows proportionally.

# COMMAND ----------

# DBTITLE 1,Lower R2RML triples-maps to Spark column expressions
# Each R2RML triples-map is lowered to Spark column expressions.
# iri() builds a full IRI by concatenating the namespace with a key expression,
# mirroring the rr:template syntax in the R2RML mapping.
from pyspark.sql import functions as F
BASE = "https://ontobricks.example/customer360#"

def iri(key_expr):
    """rr:template → a full IRI column (namespace + key expression)"""
    return F.concat(F.lit(BASE), key_expr)

c = spark.table(CUSTOMER_TABLE)
cust_iri = iri(F.concat(F.lit("Customer"), c.customer_id))

# One DataFrame per predicate-object map. Each select() produces the
# 4-column schema (subject, predicate, object, object_kind) that will
# be unioned into the final kg_triples table.
type_t   = c.select(cust_iri.alias("subject"),
                    F.lit(BASE+"type").alias("predicate"),
                    F.lit(BASE+"Customer").alias("object"),
                    F.lit("iri").alias("object_kind"))
ltv_t    = c.select(cust_iri.alias("subject"),
                    F.lit(BASE+"hasLifetimeValue").alias("predicate"),
                    c.lifetime_value.cast("string").alias("object"),
                    F.lit("literal").alias("object_kind"))
region_t = c.select(cust_iri.alias("subject"),
                    F.lit(BASE+"isLocatedIn").alias("predicate"),
                    iri(F.concat(F.lit("Region"), c.region_code)).alias("object"),
                    F.lit("iri").alias("object_kind"))
# ... the other triples-maps follow the same shape; all are unioned ...

# COMMAND ----------

# DBTITLE 1,Union and materialise triples into Delta
# Union all predicate DataFrames, deduplicate, and write as a single Delta table.
# In a full implementation, all triples-maps (orders, products, accounts) join here.
graph = type_t.unionByName(ltv_t).unionByName(region_t)  # ...and the rest
(graph.dropDuplicates()
      .write.format("delta").mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(CATALOG_SCHEMA + ".kg_triples"))
print(f"Materialized {graph.count()} triples")

# COMMAND ----------

# DBTITLE 1,Observation — Materialisation
# MAGIC %md
# MAGIC > **Observation:** 12 triples materialised — 4 customers × 3 predicates (type, hasLifetimeValue, isLocatedIn). The `object_kind` column distinguishes IRIs from literals, which matters for downstream SPARQL queries and serialisation.

# COMMAND ----------

# DBTITLE 1,Part 4 — Verification
# MAGIC %md
# MAGIC ## Part 4 — Graph Verification
# MAGIC
# MAGIC The final step queries `kg_triples` as a graph to verify correctness:
# MAGIC 1. **Per-class instance count** — all 4 rows should be typed as `Customer`
# MAGIC 2. **Customer4471 subgraph** — her 3 triples (type, LTV, region)
# MAGIC 3. **Two-hop walk** — Customer4471 → orders → products (tests join traversal)
# MAGIC
# MAGIC If the two-hop walk returns empty, it means the `placed` and `contains` predicate triples are not yet materialised — only the customer-level maps are in scope so far.

# COMMAND ----------

# DBTITLE 1,Verify the graph with Spark queries
# Verification — query kg_triples as a graph to confirm correctness.
# Three checks: per-class counts, Customer4471's subgraph, and a two-hop walk.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

g    = spark.table(CATALOG_SCHEMA + ".kg_triples")
BASE = "https://ontobricks.example/customer360#"

print(f"Total triples: {g.count()}")
(g.filter(g.predicate == F.lit(BASE+"type"))      # instances per class
  .groupBy("object").count().orderBy(F.desc("count")).show(truncate=False))

cust = BASE + "Customer4471"                        # the canonical instance
(g.filter(g.subject == F.lit(cust))                 # her complete subgraph
  .select("predicate", "object", "object_kind").show(truncate=False))

# Two-hop walk: Customer4471 → placed → Order → contains → Product
# Tests that join-based graph traversal works on the materialised triples.
placed   = (g.filter((g.subject == F.lit(cust)) &
                     (g.predicate == F.lit(BASE+"placed")))
              .select(g.object.alias("order")))
products = (placed.join(g, placed.order == g.subject)
                  .filter(g.predicate == F.lit(BASE+"contains"))
                  .select("order", g.object.alias("product")))
products.show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC > The Customer 360 knowledge graph is now **materialised as a Delta table** in Unity Catalog:
# MAGIC > * **Table:** `benoit_cayla.book.kg_triples` with columns `(subject, predicate, object, object_kind)`
# MAGIC > * **12 triples** from 4 customers × 3 predicates (type, LTV, region)
# MAGIC > * **R2RML mapping** parsed and validated (98 triples in `mapping.r2rml.ttl`)
# MAGIC > * **Graph-native queries** work: per-class counts, subgraph extraction, multi-hop joins
# MAGIC >
# MAGIC > This table is consumed by:
# MAGIC > * **Chapter 5** — SHACL shape validation against quality constraints
# MAGIC > * **Chapter 6** — OWL 2 RL, SWRL, and SPARQL CONSTRUCT reasoning

# COMMAND ----------

# DBTITLE 1,Part 5 — Run companion scripts
# MAGIC %md
# MAGIC ## Part 5 — Full Materialisation via Companion Scripts
# MAGIC
# MAGIC The folder ships three standalone scripts that form the complete pipeline:
# MAGIC
# MAGIC | Script | Role | What it does |
# MAGIC | --- | --- | --- |
# MAGIC | `00_seed_data.sql` | **Seed** | Creates *all* source tables (`customers`, `accounts`, `orders`, `order_items`, `customer_aliases`) with sample data |
# MAGIC | `01_materialize.py` | **Materialise** | Reads the 4 source tables, applies every R2RML triples-map as Spark `select()` expressions, unions them, deduplicates, and writes the result to `kg_triples` |
# MAGIC | `02_verify.py` | **Verify** | Runs read-only checks: total triple count, per-class instances, Customer4471's full subgraph, a two-hop walk, and an assertion that the canonical facts hold |
# MAGIC
# MAGIC Running them in order below makes the notebook self-contained while keeping the logic in its canonical location.

# COMMAND ----------

# DBTITLE 1,Step 1 — Create all source tables (from 00_seed_data.sql)
# MAGIC %sql
# MAGIC -- Step 1: Create all source tables (content from 00_seed_data.sql)
# MAGIC -- customers is already created in Part 1; the remaining tables are added here.
# MAGIC
# MAGIC DECLARE OR REPLACE VARIABLE schema_prefix STRING DEFAULT 'benoit_cayla.book';
# MAGIC
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(schema_prefix || '.accounts') (
# MAGIC     account_id INT, customer_id INT, status STRING, created_at TIMESTAMP);
# MAGIC INSERT INTO IDENTIFIER(schema_prefix || '.accounts') VALUES
# MAGIC   (8801, 4471, 'active', TIMESTAMP'2023-02-11 09:05:00'),
# MAGIC   (8802, 5012, 'active', TIMESTAMP'2024-06-01 14:35:00'),
# MAGIC   (8803, 5519, 'active', TIMESTAMP'2022-11-20 08:20:00');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(schema_prefix || '.orders') (
# MAGIC     order_id INT, customer_id INT, status STRING, created_at TIMESTAMP);
# MAGIC INSERT INTO IDENTIFIER(schema_prefix || '.orders') VALUES
# MAGIC   (9920, 4471, 'fulfilled', TIMESTAMP'2026-03-01 10:00:00'),
# MAGIC   (9921, 4471, 'fulfilled', TIMESTAMP'2026-04-12 11:30:00'),
# MAGIC   (9940, 5519, 'fulfilled', TIMESTAMP'2026-02-18 13:10:00'),
# MAGIC   (9955, 5012, 'cancelled', TIMESTAMP'2026-05-02 09:45:00');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(schema_prefix || '.order_items') (
# MAGIC     order_id INT, product_id STRING, quantity INT);
# MAGIC INSERT INTO IDENTIFIER(schema_prefix || '.order_items') VALUES
# MAGIC   (9920, 'A', 2), (9920, 'B', 1), (9921, 'C', 5), (9940, 'A', 1), (9955, 'D', 3);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(schema_prefix || '.customer_aliases') (
# MAGIC     legacy_id STRING, canonical_id INT);
# MAGIC INSERT INTO IDENTIFIER(schema_prefix || '.customer_aliases') VALUES
# MAGIC   ('cust_88123', 4471);

# COMMAND ----------

# DBTITLE 1,Step 2 — Run 01_materialize.py (loads 4 tables, applies 7 triples-maps, writes kg_triples)
# Import the materialisation script as a module and call its pipeline:
#   load_source_tables() -> reads customers, accounts, orders, order_items
#   materialize_native() -> applies 7 R2RML triples-maps as Spark selects
#   write_triple_store()  -> writes the deduplicated graph to kg_triples
import importlib.util

spec = importlib.util.spec_from_file_location(
    "materialize",
    "materialize.py")
mat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mat)

tables = mat.load_source_tables()
graph  = mat.materialize_native(tables)
mat.write_triple_store(graph)
print(f"Materialized {graph.count()} triples into {mat.TARGET_TABLE}")

# COMMAND ----------

# DBTITLE 1,Step 3 — Run 02_verify.py (triple count, class instances, subgraph, two-hop walk, assertions)
# Import and run the verification script.
# It's entirely read-only: counts, subgraph, 2-hop walk, and assertions.
import runpy

runpy.run_path(
    "verify.py",
    run_name="__main__")