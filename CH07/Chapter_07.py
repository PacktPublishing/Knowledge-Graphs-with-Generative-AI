# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Chapter 7 — Lab Title
# MAGIC %md
# MAGIC # Chapter 7 — Querying Knowledge: SPARQL, Cypher, and GraphQL Projection
# MAGIC
# MAGIC This lab exposes the **enriched Customer 360 knowledge graph** as a **typed GraphQL API generated from the ontology**, and proves that the API and raw SPARQL return the same answer to the capstone question:
# MAGIC
# MAGIC > *"Which premium customers in regulated regions have an open escalation and a product from the recalled batch … and who owns each account?"*
# MAGIC
# MAGIC | Step | Concept |
# MAGIC | --- | --- |
# MAGIC | **1** | SPARQL by example: pattern matching, property paths, the capstone question, federation |
# MAGIC | **2** | Project the GraphQL schema from the ontology: never write it by hand |
# MAGIC | **3** | Build the GraphQL API: every resolver is a thin shim over a named SPARQL query |
# MAGIC | **4** | Query the API: typed JSON, no SPARQL in sight |
# MAGIC | **5** | Verify: the same answer via SPARQL and via GraphQL |
# MAGIC
# MAGIC **Inputs:** `customer360_enriched.ttl` (the enriched graph, provided in `book/Common`), `customer360.ttl` (Ch3) and `queries.rq` (the named SPARQL queries, in this folder).

# COMMAND ----------

# DBTITLE 1,Inits
# MAGIC %pip install rdflib ariadne "pyparsing>=3.1"
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Configuration paths
import os
# Replace this folder path by yours:
BOOK_PATH   = "/Workspace/Users/benoit.cayla@databricks.com/book"
COMMON_PATH = BOOK_PATH + "/Common"      # customer360.ttl (Ch3), customer360_enriched.ttl
LAB_PATH    = BOOK_PATH + "/CH07"        # queries.rq
os.chdir(LAB_PATH)

# COMMAND ----------

# DBTITLE 1,Helpers — named queries and local names
from pathlib import Path

def load_named_queries(path: str) -> dict[str, str]:
    """Split queries.rq on '# === name ===' markers into a name->SPARQL dict."""
    queries, name, buf = {}, None, []
    for line in Path(path).read_text().splitlines():
        if line.startswith("# === ") and line.rstrip().endswith("==="):
            if name:
                queries[name] = "\n".join(buf)
            name = line.strip().strip("# =").split()[0]
            buf = []
        elif name:
            buf.append(line)
    if name:
        queries[name] = "\n".join(buf)
    return queries

def local(term) -> str:
    """Local name of an IRI (Customer4471), or the lexical form of a literal."""
    return str(term).split("#")[-1].split("/")[-1] if term is not None else None

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Step 1 — SPARQL
# MAGIC %md
# MAGIC ## Step 1 — Run SPARQL against the enriched graph
# MAGIC
# MAGIC The lab starts from `customer360_enriched.ttl`: the Chapter 6 enriched graph (materialized triples + OWL 2 RL closure + the Ch6 rules) plus three scenario facts the capstone needs: Ticket4471's status, the agent who owns each account, and the recalled production batch. **Listing 7.10** loads it and asks for premium customers (Listing 7.1). `Customer4471` must be among them, even though no source row ever labelled her premium.

# COMMAND ----------

# DBTITLE 1,Expectation — Listing 7.10
# MAGIC %md
# MAGIC The enriched graph loads (~570 triples), the named queries load, and **Listing 7.1** (`q1_premium_customers`) prints every premium customer with lifetime value and region, highest value first. `Customer4471` (18,400, EMEA) must be in the list.

# COMMAND ----------

# DBTITLE 1,Listing 7.10 — Notebook glue: run the first SPARQL query and confirm inferred premium status is queryable
from rdflib import Graph
g = Graph().parse(COMMON_PATH + "/customer360_enriched.ttl", format="turtle")  # Ch6 output + scenario facts

# Load the named query set and run the first one (load_named_queries: Helpers cell).
Q = load_named_queries("queries.rq")
for row in g.query(Q["q1_premium_customers"]):
    print(row.c, float(row.ltv), row.region)
# Expect Customer4471 near the top: premium (inferred), LTV 18400, RegionEMEA.

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.10
# MAGIC %md
# MAGIC **Outcome:** two premium customers:
# MAGIC ```
# MAGIC Customer5519  21750.0  RegionAMER
# MAGIC Customer4471  18400.0  RegionEMEA
# MAGIC ```
# MAGIC no source row says "premium". `PremiumCustomer` was **inferred** in Chapter 6 from the ontology's definition (lifetime value > €15,000), and the query only asks for the class. Customer5519 comes first because of `ORDER BY DESC(?ltv)`. Customer5012 (9,200) and Customer6033 (4,300) stay below the line.

# COMMAND ----------

# DBTITLE 1,Helper — run a named query as a table
import pandas as pd
def show(name):
    rows = [{str(v): local(r[v]) for v in r.labels} for r in g.query(Q[name])]
    print(f"{name}: {len(rows)} row(s)")
    if rows:
        display(pd.DataFrame(rows))

# COMMAND ----------

# DBTITLE 1,Listing 7.2 — Premium customers in GDPR regions
show("q2_regulated_premium")

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.2
# MAGIC %md
# MAGIC Customer5519 is premium too, but `RegionAMER` has no `governedBy GDPR` fact, so the extra pattern finds no match for her and she drops out.

# COMMAND ----------

# DBTITLE 1,Expectation — Listing 7.3
# MAGIC %md
# MAGIC **Expectation:** the products Customer4471 ordered, found with the **property path** `c360:placed/c360:contains` (customer → order → product) without naming the orders.

# COMMAND ----------

# DBTITLE 1,Listing 7.3 — Property path: products ordered by Customer4471
show("q3_property_path_orders_to_products")

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.3
# MAGIC %md
# MAGIC We have the Threse products: `ProductA`, `ProductB`, `ProductC`. because she placed two orders, Order9920 (A, B) and Order9921 (C). The path walks both hops in one step, and `DISTINCT` keeps each product once even if several orders contained it.

# COMMAND ----------

# DBTITLE 1,Listing 7.4 — Customers exposed to the recalled batch
show("q4_recalled_batch_path")

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.4
# MAGIC %md
# MAGIC Whe have two rows, `Customer4471 · ProductB · BatchB2024_17` and `Customer5012 · ProductD · BatchB2024_17`. **Why:** both products come from batch B2024_17, the one flagged as recalled. ProductA and ProductC belong to B2024_09, which is not recalled.

# COMMAND ----------

# DBTITLE 1,Listing 7.5 — The capstone multi-hop question
show("q5_capstone_multihop")

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.5
# MAGIC %md
# MAGIC We have`Customer4471 · GDPR · Ticket4471 · ProductB · Account8801 · AgentMarieDupont`.
# MAGIC We only have her because every pattern must match at once. Customer5012 bought from the recalled batch but is not premium. Customer5519 is premium but sits in an unregulated region and has no escalated ticket. The shared variables (`?customer`, `?region`, `?product`…) do all the joining; no `JOIN` keyword is written.

# COMMAND ----------

# DBTITLE 1,Expectation — Listing 7.6
# MAGIC %md
# MAGIC **Expectation:** **0 rows, no error.** The query federates with `SERVICE` to a sanctions registry, but the endpoint is illustrative, and the local half (`?customer c360:controlledBy ?legalEntity`) has nothing to match.

# COMMAND ----------

# DBTITLE 1,Listing 7.6 — Federation with SERVICE
# The registry endpoint is illustrative: our graph has no c360:controlledBy fact yet, so the local
# pattern matches nothing and the remote endpoint is never called (0 rows).
show("q7_federated_sanctions")

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.6
# MAGIC %md
# MAGIC rdflib evaluates the local pattern first; with no `controlledBy` facts it never calls the remote endpoint. With local facts and an unreachable endpoint, the query would fail instead. A federated query is only as available as the endpoint it calls.

# COMMAND ----------

# DBTITLE 1,Step 2 — Project the schema
# MAGIC %md
# MAGIC ## Step 2 — Project the GraphQL schema from the ontology
# MAGIC
# MAGIC Do not write the schema by hand: generate it. The capstone needs three terms the Chapter 3 ontology does not declare yet (who owns an account, which batch a product came from, whether the batch was recalled), so we add them in a small **ontology extension** and the API grows to match. `project()` (Listing 7.8, in full) applies the §7.2 rules. It then appends `API_VIEWS`, the only hand-written part: the view types and the root `Query`.

# COMMAND ----------

# DBTITLE 1,Expectation — Ch7 ontology extension
# MAGIC %md
# MAGIC **Expectation:** no output. Three terms the capstone needs that the Chapter 3 ontology does not declare: `c360:Agent` with `c360:ownedBy` (who owns an account), and `dt:Batch` with `dt:partOfBatch` and `dt:recalled`. `ownedBy` and `partOfBatch` are **functional** (one value at most), which the projection will turn into single-object fields.

# COMMAND ----------

# DBTITLE 1,Ch7 ontology extension
CH07_EXTENSION = """
@prefix c360: <https://ontobricks.example/customer360#> .
@prefix dt:   <https://ontobricks.example/digitaltwin#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

c360:Agent a owl:Class ; rdfs:label "Agent" .
c360:ownedBy a owl:ObjectProperty , owl:FunctionalProperty ;
    rdfs:domain c360:Account ; rdfs:range c360:Agent .

dt:Batch a owl:Class ; rdfs:label "Batch" .
dt:partOfBatch a owl:ObjectProperty , owl:FunctionalProperty ;
    rdfs:domain c360:Product ; rdfs:range dt:Batch .
dt:recalled a owl:DatatypeProperty ;
    rdfs:domain dt:Batch ; rdfs:range xsd:boolean .
"""

# COMMAND ----------

# DBTITLE 1,The projection — full version of Listing 7.8
# The §7.2 rules: owl:Class → type · rdfs:subClassOf → shared interface (<Super>Like)
# owl:ObjectProperty → field of the range type ([Range!], or Range if functional)
# owl:DatatypeProperty → scalar (hasX exposed as x)
from rdflib import RDF, RDFS, OWL, XSD, URIRef

SCALARS = {XSD.decimal: "Float", XSD.string: "String",
           XSD.boolean: "Boolean", XSD.dateTime: "DateTime"}

def project(onto: Graph, views: str) -> str:
    fields = {local(c): {"id": "ID!"} for c in onto.subjects(RDF.type, OWL.Class) if isinstance(c, URIRef)}
    for prop, _, domain in onto.triples((None, RDFS.domain, None)):
        rng = onto.value(prop, RDFS.range)
        if (prop, RDF.type, OWL.ObjectProperty) in onto:
            name, field_type = local(prop), local(rng)                     # range class
            if (prop, RDF.type, OWL.FunctionalProperty) not in onto:
                field_type = f"[{field_type}!]"                            # unbounded → list
        else:
            name, field_type = local(prop), SCALARS.get(rng, "String")     # scalar
            if name.startswith("has"):
                name = name[3].lower() + name[4:]                          # hasLifetimeValue → lifetimeValue
        fields[local(domain)][name] = field_type

    sdl, parents = ["scalar DateTime", ""], {}
    for sub, sup in onto.subject_objects(RDFS.subClassOf):
        if isinstance(sub, URIRef) and isinstance(sup, URIRef):
            parents[local(sub)] = local(sup)
            fields[local(sub)] = {**fields[local(sup)], **fields[local(sub)]}   # inherit fields
    for sup in sorted(set(parents.values())):
        sdl += [f"interface {sup}Like {{"] + [f"  {n}: {t}" for n, t in fields[sup].items()] + ["}", ""]
    for cls in sorted(fields):
        iface = parents.get(cls, cls if cls in parents.values() else None)
        impl = f" implements {iface}Like" if iface else ""
        sdl += [f"type {cls}{impl} {{"] + [f"  {n}: {t}" for n, t in fields[cls].items()] + ["}", ""]
    return "\n".join(sdl) + views

# COMMAND ----------

# DBTITLE 1,Expectation — API_VIEWS
# MAGIC %md
# MAGIC The only hand-written part of the API: two **view types** computed by queries (`RetentionRisk` from the q9 CONSTRUCT, `RecalledBatchExposure` from the capstone q5) and the root `Query` that lists the API's entry points.

# COMMAND ----------

# DBTITLE 1,API_VIEWS — the hand-written part of the API
# What an ontology does not describe: computed view types backed by a query
# (RetentionRisk → the q9 CONSTRUCT, RecalledBatchExposure → the capstone q5) and the root Query.
API_VIEWS = """
type RetentionRisk          { customer: PremiumCustomer!  atRiskIn: Region!  owningAgent: Agent! }
type RecalledBatchExposure  { customer: PremiumCustomer!  regulation: Regulation!  ticket: SupportTicket!
                              product: Product!  account: Account!  owner: Agent! }
type Query {
  premiumCustomers: [PremiumCustomer!]!
  recalledBatchExposure: [RecalledBatchExposure!]!   # the capstone question, one field
  retentionRisks: [RetentionRisk!]!
}
"""

# COMMAND ----------

# DBTITLE 1,Expectation — Listing 7.11
# MAGIC %md
# MAGIC The generated schema is written to `schema.graphql` and printed. Look for `interface CustomerLike`, **`type PremiumCustomer implements CustomerLike`**, list fields such as `placed: [Order!]`, single fields such as `ownedBy: Agent`, and `API_VIEWS` at the end.

# COMMAND ----------

# DBTITLE 1,Listing 7.11 — Generate the typed GraphQL schema from the ontology, then eyeball the output
# project() reads customer360.ttl (+ the Ch7 extension) and emits schema.graphql by the
# rules in §7.2: Class→type, ObjectProperty→field(range), DatatypeProperty→scalar.
onto = Graph().parse(COMMON_PATH + "/customer360.ttl", format="turtle")
onto.parse(data=CH07_EXTENSION, format="turtle")
Path("schema.graphql").write_text(project(onto, API_VIEWS))
print(open("schema.graphql").read())
# Inspect the output: you should see `type PremiumCustomer implements CustomerLike`
# even though "premium" was never a column — the projection inherits the reasoner.

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.11
# MAGIC %md
# MAGIC the result is a schema with the `CustomerLike` interface, 11 entity types (Account, Agent, Batch, Customer, Interaction, Order, PremiumCustomer, Product, Region, Regulation, SupportTicket), then `RetentionRisk`, `RecalledBatchExposure` and `Query`.
# MAGIC **Reading it against the rules:**
# MAGIC * `PremiumCustomer implements CustomerLike` comes from `rdfs:subClassOf c360:Customer`. The inferred class became an API type like any other.
# MAGIC * `placed: [Order!]` is a list because `placed` is not functional; `ownedBy: Agent` and `partOfBatch: Batch` are single objects because the extension declares them functional.
# MAGIC * `lifetimeValue: Float` comes from `hasLifetimeValue` (`xsd:decimal` → `Float`, with the `has` verb dropped).
# MAGIC * `status` and `createdAt` do not appear: the ontology deliberately gives them no `rdfs:domain`, and the projection only places a property on the type its domain names.

# COMMAND ----------

# DBTITLE 1,Step 3 — Build the GraphQL API
# MAGIC %md
# MAGIC ## Step 3 — Build the GraphQL API
# MAGIC
# MAGIC The API is thin by design: each GraphQL field is wired to the named SPARQL query that backs it (`recalledBatchExposure` → q5, `premiumCustomers` → q1, `retentionRisks` → the q9 CONSTRUCT). **Listing 7.12** shows the resolver pattern: run a named query, shape the rows, return typed objects. Ariadne then turns `schema.graphql` and the resolvers into an **executable schema**. `graphql()` runs a query against it in-process, exactly what an HTTP endpoint does for each request, without the web server.

# COMMAND ----------

# DBTITLE 1,Expectation — Resolver helper
# MAGIC %md
# MAGIC `run(name)` executes a named SPARQL query and returns one dict per row, keyed by variable name, ready to be shaped into GraphQL objects.

# COMMAND ----------

# DBTITLE 1,Resolver helper — run a named query, one dict per row
def run(name):
    return [{str(v): local(r[v]) for v in r.labels} for r in g.query(Q[name])]

# COMMAND ----------

# DBTITLE 1,Expectation — Listing 7.12
# MAGIC %md
# MAGIC This registers the resolver for `recalledBatchExposure`: run q5 (the capstone), turn each row into the nested objects the schema declares, and return them.

# COMMAND ----------

# DBTITLE 1,Listing 7.12 — A GraphQL resolver is a thin shim over a named SPARQL query
from ariadne import QueryType
query = QueryType()

@query.field("recalledBatchExposure")          # the capstone question as one field
def resolve_capstone(*_):
    return [
        {"customer": {"id": r["customer"]}, "regulation": {"id": r["regulation"]},
         "ticket": {"id": r["ticket"]}, "product": {"id": r["product"]},
         "account": {"id": r["account"]}, "owner": {"id": r["owner"]}}
        for r in run("q5_capstone_multihop")    # ← the SPARQL from Listing 7.5
    ]

# COMMAND ----------

# DBTITLE 1,Expectation — The other resolvers
# MAGIC %md
# MAGIC Same pattern for `premiumCustomers` (q1) and `retentionRisks`. The latter runs the q9 **CONSTRUCT**, which returns a small graph rather than rows, and reads each risk off that graph.

# COMMAND ----------

# DBTITLE 1,The other resolvers (same pattern)
from rdflib import Namespace
C360 = Namespace("https://ontobricks.example/customer360#")

@query.field("premiumCustomers")                 # Listing 7.1
def resolve_premium(*_):
    return [{"id": r["c"], "lifetimeValue": float(r["ltv"])} for r in run("q1_premium_customers")]

@query.field("retentionRisks")                   # the q9 CONSTRUCT: read the mini-graph it builds
def resolve_retention(*_):
    view = g.query(Q["q9_construct_retention_view"]).graph
    return [{"customer": {"id": local(c)},
             "atRiskIn": {"id": local(view.value(c, C360.atRiskIn))},
             "owningAgent": {"id": local(view.value(c, C360.owningAgent))}}
            for c in view.subjects(RDF.type, C360.RetentionRisk)]

# COMMAND ----------

# DBTITLE 1,Expectation — The executable schema
# MAGIC %md
# MAGIC Ariadne validates the generated SDL and binds the resolvers to it, raising here if the schema were invalid. `graphql(body)` is what an HTTP endpoint would do with a POST body `{"query": …}`.

# COMMAND ----------

# DBTITLE 1,The executable schema
from ariadne import make_executable_schema, graphql_sync

schema = make_executable_schema(Path("schema.graphql").read_text(), query)

def graphql(body):
    """Run a GraphQL request {"query": ...} against the schema — what an HTTP endpoint would do."""
    return graphql_sync(schema, body)[1]

# COMMAND ----------

# DBTITLE 1,Step 4 — Query the API
# MAGIC %md
# MAGIC ## Step 4 — Query the API from a notebook
# MAGIC
# MAGIC Now consume the graph the way a developer would: with a GraphQL query and no SPARQL in sight. **Listing 7.13** asks the capstone question through the API.

# COMMAND ----------

# DBTITLE 1,Expectation — Listing 7.13
# MAGIC %md
# MAGIC The capstone answer through the API: the customer and the agent who should pick up the phone. No SPARQL appears in this cell.

# COMMAND ----------

# DBTITLE 1,Listing 7.13 — The capstone question through the GraphQL API: typed JSON, no SPARQL
QUERY = """{ recalledBatchExposure {
              customer { id }  regulation { id }  ticket { id }
              product  { id }  account    { id }  owner  { id } } }"""
resp = graphql({"query": QUERY})
for row in resp["data"]["recalledBatchExposure"]:
    print(row["customer"]["id"], "→ owner", row["owner"]["id"])
# Expect Customer4471 → owner <her account's owning agent>.

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.13
# MAGIC %md
# MAGIC **Why it matches Listing 7.5:** the `recalledBatchExposure` resolver runs exactly q5. The developer asked for six nested ids and got exactly those back, in the shape they requested.

# COMMAND ----------

# DBTITLE 1,Expectation — The other API fields
# MAGIC %md
# MAGIC We expect here two JSON responses. `premiumCustomers` should list the same two customers as Listing 7.10 with numeric `lifetimeValue`, and `retentionRisks` should list Customer4471.

# COMMAND ----------

# DBTITLE 1,The other API fields
import json
print(json.dumps(graphql({"query": "{ premiumCustomers { id lifetimeValue } }"}), indent=1))
print(json.dumps(graphql({"query": "{ retentionRisks { customer { id } atRiskIn { id } owningAgent { id } } }"}), indent=1))

# COMMAND ----------

# DBTITLE 1,Outcome — The other API fields
# MAGIC %md
# MAGIC **Outcome:**
# MAGIC * `premiumCustomers` → Customer5519 (21750.0), Customer4471 (18400.0): the inferred class, served as a typed list.
# MAGIC * `retentionRisks` → Customer4471, `atRiskIn` RegionEMEA, `owningAgent` AgentMarieDupont.
# MAGIC
# MAGIC **Why it matters:** "retention risk" is a rule (a premium customer in a GDPR region with an escalated ticket), not a table. The API consumer just sees a type.

# COMMAND ----------

# DBTITLE 1,Step 5 — Verification
# MAGIC %md
# MAGIC ## Step 5 — Verification: the same answer two ways
# MAGIC
# MAGIC Ask the capstone question twice, once in raw SPARQL against the graph and once through the GraphQL API, and assert the two answer sets are identical. **Listing 7.14**.

# COMMAND ----------

# DBTITLE 1,Expectation — Listing 7.14
# MAGIC %md
# MAGIC The capstone answer is computed twice, from the graph and through the API, and the two sets must be identical and contain Customer4471. Otherwise an `AssertionError` stops the cell.

# COMMAND ----------

# DBTITLE 1,Listing 7.14 — The capstone answer is identical via SPARQL and via GraphQL
def via_sparql():   # straight from the graph
    return {local(r["customer"]) for r in g.query(Q["q5_capstone_multihop"])}

def via_graphql():  # through the projected API
    body = {"query": "{ recalledBatchExposure { customer { id } } }"}
    rows = graphql(body)["data"]["recalledBatchExposure"]
    return {r["customer"]["id"] for r in rows}

assert via_sparql() == via_graphql()          # the projection is faithful
assert "Customer4471" in via_sparql()         # the canonical instance is present
print("OK — SPARQL and GraphQL agree.")

# COMMAND ----------

# DBTITLE 1,Outcome — Listing 7.14
# MAGIC %md
# MAGIC Both sides return `{Customer4471}`. **Why it holds:** the schema is generated from the same ontology and every field resolves to the same SPARQL, so the API cannot drift from the graph. That is what lets downstream teams use the API and forget the graph underneath.

# COMMAND ----------

# DBTITLE 1,Going further
# MAGIC %md
# MAGIC ## Going further
# MAGIC
# MAGIC * **A web app.** Expose the schema over HTTP (Ariadne's ASGI app, `GraphQL(schema)`, under uvicorn, or deployed as a **Databricks App**). A fifty-line HTML page can then post the same query with `fetch` and render Customer4471 and her account owner in a table, with no framework, triples or ontology knowledge.
# MAGIC * **The Cypher bridge.** The §7.3 projection rules turn this RDF graph into an LPG cache (`rdf:type` → node label, datatype property → node property, object property → relationship). Running the §7.3 Cypher capstone query on it returns the same Customer4471.

# COMMAND ----------

# DBTITLE 1,Lab Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC > **Final state:** one ontology, one generated schema (`schema.graphql`), one executable GraphQL API, and the capstone question answered identically two ways.
# MAGIC >
# MAGIC > | Way of asking | Answer |
# MAGIC > | --- | --- |
# MAGIC > | Raw SPARQL (q5) | Customer4471 · GDPR · Ticket4471 · ProductB · Account8801 · AgentMarieDupont |
# MAGIC > | GraphQL API | Customer4471 → owner AgentMarieDupont |
# MAGIC >
# MAGIC > Listing 7.14 proves the API returns exactly what the graph does, so downstream teams can consume it and forget the graph is underneath.