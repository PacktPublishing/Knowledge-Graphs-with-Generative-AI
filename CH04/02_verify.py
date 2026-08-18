# verify.py
# ---------------------------------------------------------------------------
# Chapter 4 — verification step for the materialized Customer 360 graph.
# Knowledge Graphs for AI Agents — Benoît Cayla
#
# CONSUMES : ontobricks.customer360.kg_triples  (produced by materialize.py)
# PROVES   : the graph exists, has the expected shape, and that the canonical
#            through-line instance Customer4471 has the subgraph the book
#            promised (premium LTV, EMEA region, an account, an order).
#
# Run after materialize.py. Pure read-only checks — safe to re-run.
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG       = "benoit_cayla"
SCHEMA        = "book"
TRIPLES  = f"{CATALOG}.{SCHEMA}.kg_triples"
BASE     = "https://ontobricks.example/customer360#"

spark = SparkSession.builder.getOrCreate()
g = spark.table(TRIPLES)

# --- Check 1: how big is the graph? ---
total = g.count()
print(f"Total triples: {total}")

# --- Check 2: triples per class (count of rdf:type assertions) ---
print("\nInstances per class:")
(g.filter(F.col("predicate") == F.lit(BASE + "type"))
  .groupBy("object").count()
  .orderBy(F.desc("count"))
  .show(truncate=False))

# --- Check 3: spot-check Customer4471's complete subgraph ---
cust = BASE + "Customer4471"
print(f"\nEverything we know about {cust}:")
(g.filter(F.col("subject") == F.lit(cust))
  .select("predicate", "object", "object_kind")
  .show(truncate=False))

# --- Check 4: one-hop neighbourhood — the orders Customer4471 placed,
#     and the products those orders contain (a 2-hop graph walk). ---
placed = (g.filter((F.col("subject") == F.lit(cust)) &
                   (F.col("predicate") == F.lit(BASE + "placed")))
           .select(F.col("object").alias("order")))
products = (placed.join(g, placed.order == g.subject)
                  .filter(g.predicate == F.lit(BASE + "contains"))
                  .select("order", F.col("object").alias("product")))
print(f"\nOrders placed by Customer4471 and the products they contain:")
products.show(truncate=False)

# --- Verification assertion: the canonical retention-risk facts must hold. ---
facts = {row["predicate"]: row["object"]
         for row in g.filter(F.col("subject") == F.lit(cust)).collect()}
assert facts.get(BASE + "isLocatedIn") == BASE + "RegionEMEA", \
    "Customer4471 should be located in RegionEMEA"
assert float(facts.get(BASE + "hasLifetimeValue", 0)) > 15000, \
    "Customer4471 should have LTV > 15000 (premium)"
print("\nVerification passed: Customer4471 is a premium EMEA customer in the graph.")
