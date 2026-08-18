# materialize.py
# ---------------------------------------------------------------------------
# Chapter 4 — From Tables to Triples: R2RML and Mapping
# Knowledge Graphs for AI Agents — Benoît Cayla
#
# CONSUMES : labs/ch04/mapping.r2rml.ttl   (the R2RML mapping)
#            labs/ch03/customer360.ttl       (the ontology, for context/sanity)
#            Unity Catalog tables in ontobricks.customer360
# PRODUCES : a Delta-backed triple store  ->  ontobricks.customer360.kg_triples
#            (one row per triple: subject, predicate, object, object_kind)
#
# Run as a Databricks notebook or with `databricks bundle run`. Python here is
# glue only: it reads the declarative mapping, lets an R2RML engine turn rows
# into triples, and lands them in a Delta table that any SPARQL layer can read.
# An AI assistant could scaffold every line of this from the mapping file; the
# intellectual content lives in mapping.r2rml.ttl, not here.
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Configuration: everything that changes between environments lives here ---
CATALOG       = "benoit_cayla"
SCHEMA        = "book"
MAPPING_FILE  = "/Workspace/Users/benoit.cayla@databricks.com/book/Common/mapping.r2rml.ttl"
TARGET_TABLE  = f"{CATALOG}.{SCHEMA}.kg_triples"     # the Delta triple store
BASE_IRI      = "https://ontobricks.example/customer360#"

spark = SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Step 1 — Read the UC tables the mapping references.
# We load each logical table the R2RML file names; in a fuller implementation
# the table list is parsed from the mapping, here we list them for clarity.
# ---------------------------------------------------------------------------
def load_source_tables():
    names = ["customers", "accounts", "orders", "order_items"]
    return {n: spark.table(f"{CATALOG}.{SCHEMA}.{n}") for n in names}


# ---------------------------------------------------------------------------
# Step 2 — Apply the R2RML mapping.
# Two paths are shown. The PRODUCTION path hands the mapping to a real R2RML
# engine (e.g. Morph-KGC) that consumes the TURTLE and emits triples; it
# understands templates, joins, and datatypes natively. The LAKEHOUSE-NATIVE
# path translates each triples-map into a Spark SELECT so materialization runs
# as a distributed Delta write with no data leaving the cluster. We use the
# native path here because it scales and stays inside Unity Catalog.
# ---------------------------------------------------------------------------
def iri(template_col):
    """Build a full IRI column from a base + a key column expression."""
    return F.concat(F.lit(BASE_IRI), template_col)

def materialize_native(tables):
    c   = tables["customers"]
    a   = tables["accounts"]
    o   = tables["orders"]
    oi  = tables["order_items"]

    # Each block below corresponds 1:1 to a triples-map in mapping.r2rml.ttl.
    # A triple is (subject, predicate, object, object_kind) where object_kind
    # is "iri" for resources and "literal" for datatype-property values.
    def triple(subj, pred, obj, kind):
        return (subj.alias("subject"),
                F.lit(BASE_IRI + pred).alias("predicate"),
                obj.alias("object"),
                F.lit(kind).alias("object_kind"))

    rows = []

    # --- CustomerMap ---
    cust_iri = iri(F.concat(F.lit("Customer"), c.customer_id))
    rows += [
        c.select(*triple(cust_iri, "type",
                         F.lit(BASE_IRI + "Customer"), "iri")),
        c.select(*triple(cust_iri, "hasLifetimeValue",
                         c.lifetime_value.cast("string"), "literal")),
        c.select(*triple(cust_iri, "status", c.status, "literal")),
        c.select(*triple(cust_iri, "createdAt",
                         c.created_at.cast("string"), "literal")),
        c.select(*triple(cust_iri, "isLocatedIn",
                         iri(F.concat(F.lit("Region"), c.region_code)), "iri")),
    ]

    # --- AccountMap + CustomerAccountMap ---
    acct_iri = iri(F.concat(F.lit("Account"), a.account_id))
    rows += [
        a.select(*triple(acct_iri, "type",
                         F.lit(BASE_IRI + "Account"), "iri")),
        a.select(*triple(acct_iri, "status", a.status, "literal")),
        a.select(*triple(iri(F.concat(F.lit("Customer"), a.customer_id)),
                         "hasAccount", acct_iri, "iri")),
    ]

    # --- OrderMap + CustomerOrderMap ---
    ord_iri = iri(F.concat(F.lit("Order"), o.order_id))
    rows += [
        o.select(*triple(ord_iri, "type",
                         F.lit(BASE_IRI + "Order"), "iri")),
        o.select(*triple(ord_iri, "status", o.status, "literal")),
        o.select(*triple(iri(F.concat(F.lit("Customer"), o.customer_id)),
                         "placed", ord_iri, "iri")),
    ]

    # --- OrderItemMap: the contains edge (Order -> Product) ---
    rows += [
        oi.select(*triple(iri(F.concat(F.lit("Order"), oi.order_id)),
                          "contains",
                          iri(F.concat(F.lit("Product"), oi.product_id)), "iri")),
    ]

    # --- OrderRegionJoinMap: the genuine JOIN (orders ⋈ customers) ---
    joined = (o.join(c, o.customer_id == c.customer_id)
                .select(iri(F.concat(F.lit("Order"), o.order_id)).alias("subject"),
                        F.lit(BASE_IRI + "orderedFromRegion").alias("predicate"),
                        iri(F.concat(F.lit("Region"), c.region_code)).alias("object"),
                        F.lit("iri").alias("object_kind")))
    rows.append(joined)

    # Union every triples-map into one graph and drop exact duplicates.
    graph = rows[0]
    for r in rows[1:]:
        graph = graph.unionByName(r)
    return graph.dropDuplicates()


# ---------------------------------------------------------------------------
# Step 3 — Land the graph in a Delta table (the triple store).
# Delta gives us ACID writes, time travel (every materialization is a version),
# and Unity Catalog governance for free.
# ---------------------------------------------------------------------------
def write_triple_store(graph):
    (graph.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(TARGET_TABLE))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    tables = load_source_tables()
    graph  = materialize_native(tables)
    write_triple_store(graph)
    print(f"Materialized {graph.count()} triples into {TARGET_TABLE}")
