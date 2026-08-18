# Databricks notebook source
# DBTITLE 1,creates the four tables and populates them with a handful of rows
# MAGIC %sql
# MAGIC
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

# DBTITLE 1,Imports
!pip install rdflib
import rdflib
dbutils.library.restartPython() 

# COMMAND ----------

# Replace your Catalog.schema with yours:
CATALOG_SCHEMA = "benoit_cayla.book"
CUSTOMER_TABLE= CATALOG_SCHEMA + ".customers"
# Replace this folder path by yours:
COMMON_PATH = "/Workspace/Users/benoit.cayla@databricks.com/book/Common"

# COMMAND ----------

# DBTITLE 1,R2RML check
import rdflib
m = rdflib.Graph()

m.parse(COMMON_PATH +"/mapping.r2rml.ttl", format="turtle")
print(f"Mapping parsed cleanly: {len(m)} R2RML triples")

# COMMAND ----------

# DBTITLE 1,One triples-map lowered to Spark column expressions
from pyspark.sql import functions as F
BASE = "https://ontobricks.example/customer360#"

def iri(key_expr): # rr:template -> a full IRI column
    return F.concat(F.lit(BASE), key_expr)

c = spark.table(CUSTOMER_TABLE)
cust_iri = iri(F.concat(F.lit("Customer"), c.customer_id))

# one DataFrame per predicate-object map; object_kind = iri | literal
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

# DBTITLE 1,Materialize triples
graph = type_t.unionByName(ltv_t).unionByName(region_t)  # ...and the rest
(graph.dropDuplicates()
      .write.format("delta").mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(CATALOG_SCHEMA + ".kg_triples"))
print(f"Materialized {graph.count()} triples")

# COMMAND ----------

# DBTITLE 1,Step 4: Verify the graph in a notebook
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

# the two-hop walk: her orders, and the products those orders contain
placed   = (g.filter((g.subject == F.lit(cust)) &
                     (g.predicate == F.lit(BASE+"placed")))
              .select(g.object.alias("order")))
products = (placed.join(g, placed.order == g.subject)
                  .filter(g.predicate == F.lit(BASE+"contains"))
                  .select("order", g.object.alias("product")))
products.show(truncate=False)
