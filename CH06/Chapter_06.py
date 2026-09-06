# Databricks notebook source
# DBTITLE 1,Chapter 6 — Lab Title
# MAGIC %md
# MAGIC # Chapter 6 — Reasoning: Making Implicit Knowledge Explicit
# MAGIC
# MAGIC This lab demonstrates three complementary reasoning regimes applied to a **Customer 360 knowledge graph** built in earlier chapters:
# MAGIC
# MAGIC | Regime | What it does | Expressiveness |
# MAGIC | --- | --- | --- |
# MAGIC | **OWL 2 RL** | Deductive closure — infers superclass types, domain/range typing | Class hierarchies, equivalences |
# MAGIC | **SWRL** | Conjunctive business rules — chains conditions across multiple patterns | Multi-condition IF…THEN rules |
# MAGIC | **SPARQL CONSTRUCT** | Transformational rules — aggregation, projections, decision tables | Anything SPARQL can express |
# MAGIC
# MAGIC Each regime adds **derived triples** to the graph, progressively enriching `Customer4471` from 8 asserted facts to a fully characterised entity with inferred types, risk flags, residency constraints, and priority scores.
# MAGIC
# MAGIC **Prerequisites:** `rdflib`, `owlrl`, `pyshacl` · Customer 360 ontology (Ch3) · Materialised instance triples (Ch4)

# COMMAND ----------

# DBTITLE 1,Inits
!pip install rdflib
!pip install pyshacl
import rdflib
dbutils.library.restartPython() 

# COMMAND ----------

# DBTITLE 1,Configuration paths and catalog settings
# Replace this folder path by yours:
COMMON_PATH = "/Workspace/Users/benoit.cayla@databricks.com/book/Common"
# replace this Volume reference/apth by yours:
VOLUME_PATH = "/Volumes/benoit_cayla/book/ontology"
CATALOG = "benoit_cayla"
SCHEMA = "book"

# COMMAND ----------

# DBTITLE 1,Helper — display_triples
# Display triples in the graph as a 3-column DataFrame (subject, predicate, object)
def display_triples(_g, _filter = None):
    triples_data = [(str(s), str(p), str(o)) for s, p, o in _g]
    df = spark.createDataFrame(triples_data, ["subject", "predicate", "object"])
    if _filter:
        df = df.filter((df.subject == _filter) | (df.predicate == _filter) | (df.object == _filter))
    display(df)

# COMMAND ----------

# DBTITLE 1,Listing 6.5 — Load graph and establish baseline
# Listing 6.5 — Load the Customer 360 graph and record a baseline triple count.
# The graph is assembled from two sources:
#   1. Ontology axioms (Chapter 3) — class hierarchy, property definitions, OWL restrictions
#   2. Instance triples (Chapter 4) — materialised from the UC table kg_triples
# Returns an rdflib Graph with all asserted (not yet inferred) triples.
from rdflib import Graph

def init():
    g = Graph()
    # The ontology axioms (Ch3) and the materialized instance triples (Ch4).
    g.parse(COMMON_PATH + "/customer360.ttl", format="turtle")

    # Load instance triples from Unity Catalog table instead of a file
    triples_df = spark.table(f"{CATALOG}.{SCHEMA}.kg_triples")
    for row in triples_df.collect():
        from rdflib import URIRef, Literal, Namespace
        s = URIRef(row["subject"])
        p = URIRef(row["predicate"])
        o_val = row["object"]
        # If the object looks like a URI, treat it as URIRef; otherwise as Literal
        if o_val.startswith("http://") or o_val.startswith("https://"):
            o = URIRef(o_val)
        else:
            o = Literal(o_val)
        g.add((s, p, o))
    baseline = len(g)
    print(f"Baseline (asserted) triples: {baseline}")

    # Snapshot asserted triples for diff in cell 5
    _asserted = set((str(s), str(p), str(o)) for s, p, o in g)
    return g

# COMMAND ----------

# MAGIC %md
# MAGIC The graph is now initialized.

# COMMAND ----------

# DBTITLE 1,Part 0 — Baseline
# MAGIC %md
# MAGIC ## Part 0 — Baseline: The Asserted Graph
# MAGIC
# MAGIC Before any reasoning, we load the ontology axioms (Chapter 3) and the materialised instance triples (Chapter 4) from Unity Catalog. This gives us the **asserted** (explicitly stated) triples — the starting point against which every reasoning step will be measured.
# MAGIC
# MAGIC **What to look for:** `Customer4471` has \~8 directly asserted triples (type, status, location, accounts, orders, lifetime value, creation date). No inferred knowledge yet.

# COMMAND ----------

# DBTITLE 1,Load baseline graph
g = init()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look on our famous customer 7741 closely ...

# COMMAND ----------

# DBTITLE 1,Baseline triples for Customer4471
display_triples(g,"https://ontobricks.example/customer360#Customer4471")

# COMMAND ----------

# DBTITLE 1,Observation — Baseline
# MAGIC %md
# MAGIC > **Observation:** Customer4471 starts with **8 asserted triples** — basic demographic and transactional facts. The `type` predicate is `c360:type` (a custom property), not `rdf:type`. This distinction matters: SPARQL's `a` keyword and OWL reasoners use `rdf:type`, so a **remapping step** is needed before reasoning.

# COMMAND ----------

# DBTITLE 1,Part 1 — OWL 2 RL
# MAGIC %md
# MAGIC ## Part 1 — OWL 2 RL: Deductive Closure
# MAGIC
# MAGIC **OWL 2 RL** (Rule Language) applies a fixed set of entailment rules to derive implicit facts from ontology axioms. The key rule here is **cax-sco** (class axiom — subclass of):
# MAGIC
# MAGIC > If `x rdf:type C1` and `C1 rdfs:subClassOf C2`, then `x rdf:type C2`
# MAGIC
# MAGIC In our ontology, `PremiumCustomer rdfs:subClassOf Customer`. So once we assert that Customer4471 is a `PremiumCustomer`, OWL 2 RL **automatically infers** that Customer4471 is also a `Customer` — without anyone stating it explicitly.
# MAGIC
# MAGIC **Step 1** below promotes Customer4471 to `PremiumCustomer` in the UC table.  
# MAGIC **Step 2** runs the full OWL 2 RL closure and shows the inferred type.
# MAGIC
# MAGIC > ⚠️ OWL closure amplifies dirty data into an explosion of garbage triples — always validate with SHACL (Chapter 5) first.

# COMMAND ----------

# DBTITLE 1,Listing 6.6 — Promote Customer4471 to PremiumCustomer
# Listing 6.6 — Promote Customer4471 to PremiumCustomer
# Change the asserted type so OWL 2 RL can later infer the superclass.
from pyspark.sql.functions import col, when, lit

C360 = "https://ontobricks.example/customer360#"
triples_df = spark.table(f"{CATALOG}.{SCHEMA}.kg_triples")

# Update: Customer4471 type  Customer → PremiumCustomer
updated_df = triples_df.withColumn(
    "object",
    when(
        (col("subject") == f"{C360}Customer4471") &
        (col("predicate") == f"{C360}type") &
        (col("object") == f"{C360}Customer"),
        lit(f"{C360}PremiumCustomer")
    ).otherwise(col("object"))
)
updated_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.kg_triples")

# Verify the change
print("✓ Updated — Customer4471 type triple now reads:")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.kg_triples")
    .filter(
        (col("subject") == f"{C360}Customer4471") &
        (col("predicate") == f"{C360}type")
    )
)

# COMMAND ----------

# DBTITLE 1,Listing 6.7 — OWL 2 RL forward-chaining inference
# Listing 6.7 — OWL 2 RL forward-chaining inference
# Runs the complete OWL 2 RL entailment regime. The key rule for this lab is:
#   cax-sco: x rdf:type C1 ∧ C1 rdfs:subClassOf C2  ⟹  x rdf:type C2
# Since PremiumCustomer rdfs:subClassOf Customer in the ontology,
# Customer4471 (asserted as PremiumCustomer) will also be inferred as Customer.
from rdflib import Graph, URIRef, Literal, Namespace, RDF
import owlrl

C360_NS = Namespace("https://ontobricks.example/customer360#")
C360 = str(C360_NS)

# 1 — Reload graph: ontology axioms + updated instance triples
g = Graph()
g.parse(COMMON_PATH + "/customer360.ttl", format="turtle")

for row in spark.table(f"{CATALOG}.{SCHEMA}.kg_triples").collect():
    s = URIRef(row["subject"])
    # The table encodes rdf:type as c360:type — remap for the OWL reasoner
    p = RDF.type if row["predicate"] == f"{C360}type" else URIRef(row["predicate"])
    o_val = row["object"]
    o = URIRef(o_val) if o_val.startswith("http") else Literal(o_val)
    g.add((s, p, o))

before = len(g)

# 2 — Apply OWL 2 RL deductive closure
owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(g)

after = len(g)
print(f"Before OWL 2 RL : {before} triples")
print(f"After OWL 2 RL  : {after} triples  (+{after - before} inferred)\n")

# 3 — Inspect Customer4471's class memberships
print("Customer4471 class memberships (asserted + inferred):")
for s, p, o in sorted(g.triples((C360_NS.Customer4471, RDF.type, None)),
                       key=lambda t: str(t[2])):
    local = str(o).split('#')[-1] if '#' in str(o) else str(o).split('/')[-1]
    flag = "★ INFERRED" if str(o) == str(C360_NS.Customer) else "  asserted"
    print(f"  [{flag}]  rdf:type  {local}")

# COMMAND ----------

# DBTITLE 1,Customer4471 triples after OWL 2 RL
display_triples(g,"https://ontobricks.example/customer360#Customer4471")

# COMMAND ----------

# DBTITLE 1,Results — OWL 2 RL
# MAGIC %md
# MAGIC > **Results — OWL 2 RL:** The closure added hundreds of triples to the graph (class hierarchy expansions, domain/range entailments, `owl:sameAs` propagation). For Customer4471, the key derivation is `rdf:type Customer` — inferred from `PremiumCustomer rdfs:subClassOf Customer`. The customer now has **both types** without anyone asserting the superclass explicitly. This is the power of ontological reasoning: **maintain the specific fact, derive the general one automatically**.

# COMMAND ----------

# DBTITLE 1,Part 2 — SWRL
# MAGIC %md
# MAGIC ## Part 2 — SWRL: Conjunctive Business Rules
# MAGIC
# MAGIC **SWRL** (Semantic Web Rule Language) expresses business rules as conjunctive conditions that chain across multiple graph patterns — something OWL alone cannot do:
# MAGIC
# MAGIC > **Rule:** IF a customer is a `PremiumCustomer` AND is `active` AND is located in a GDPR-governed region AND has `raised` a support ticket that was `escalatedTo` someone — THEN flag that customer as `RetentionRisk`.
# MAGIC
# MAGIC This rule chains **five** conditions across four entities (customer, region, regulation, ticket). SWRL tooling is patchy, so we execute the identical rule as a **SPARQL CONSTRUCT** query (see `retention_risk.rq`).
# MAGIC
# MAGIC **What to expect:** Customer4471 matches all five conditions after we add scenario triples (GDPR governance, escalated ticket), so **1 new triple** — `Customer4471 rdf:type RetentionRisk` — is inferred.

# COMMAND ----------

# DBTITLE 1,Reload graph for SWRL rules
g = init()

# COMMAND ----------

# DBTITLE 1,Listing 6.9 — apply_construct helper
# Listing 6.9 — Helper: run a SPARQL CONSTRUCT rule and merge derived triples.
# CONSTRUCT returns a Graph (not rows like SELECT); we count it and union it
# into the main graph (materialise). For volatile conclusions you'd skip the
# merge and query live. Accepts either a file path or pre-parsed rule text.
import time
from rdflib import URIRef, Literal, Namespace, RDF

C360_NS = Namespace("https://ontobricks.example/customer360#")

def apply_construct(graph, rule_path=None, _rule_text=None):
    if _rule_text:
        rule = _rule_text
    else:
        with open(rule_path) as f:
            rule = f.read()
    t0 = time.perf_counter()
    derived = graph.query(rule).graph   # CONSTRUCT returns a Graph, not rows
    n_new = len(derived)
    graph += derived                    # materialise: merge derived triples in
    elapsed = time.perf_counter() - t0
    return n_new, elapsed

# COMMAND ----------

# DBTITLE 1,Remap c360:type → rdf:type
# Remap c360:type → rdf:type so the SPARQL 'a' keyword matches
# (init() loads the predicate as a plain URI; the CONSTRUCT uses rdf:type)
type_pred = URIRef(str(C360_NS) + "type")
remapped = 0
for s, p, o in list(g.triples((None, type_pred, None))):
    g.remove((s, p, o))
    g.add((s, RDF.type, o))
    remapped += 1
print(f"Remapped {remapped} c360:type → rdf:type triples")

# COMMAND ----------

# DBTITLE 1,Materialise scenario triples
# Materialise the scenario triples the rule body requires.
# The ontology defines the properties (raised, governedBy, escalatedTo)
# and classes (SupportTicket, Regulation); here we create the instances
# that make Customer4471 match the rule body.
scenario_triples = [
    (C360_NS.GDPR,         RDF.type,            C360_NS.Regulation),
    (C360_NS.RegionEMEA,   C360_NS.governedBy,  C360_NS.GDPR),
    (C360_NS.Ticket4471,   RDF.type,            C360_NS.SupportTicket),
    (C360_NS.Customer4471, C360_NS.raised,      C360_NS.Ticket4471),
    (C360_NS.Ticket4471,   C360_NS.escalatedTo, C360_NS.Account8801),
]
for t in scenario_triples:
    g.add(t)
print(f"Added {len(scenario_triples)} scenario triples (GDPR region, escalated ticket)")

# COMMAND ----------

# DBTITLE 1,Listing 6.8 — Execute SWRL rule via SPARQL CONSTRUCT
# Listing 6.8 — Apply the SPARQL CONSTRUCT rule (SWRL equivalent)
before = len(g)
n_new, elapsed = apply_construct(g, "retention_risk.rq")
after = len(g)

print(f"Before SWRL rule : {before} triples")
print(f"After SWRL rule  : {after} triples  (+{n_new} inferred)  [{elapsed:.3f}s]")
print(f"\nCustomers flagged as RetentionRisk:")
for s, p, o in g.triples((None, RDF.type, C360_NS.RetentionRisk)):
    print(f"  ★ {str(s).split('#')[-1]}")

# COMMAND ----------

# DBTITLE 1,Results — SWRL
# MAGIC %md
# MAGIC > **Results — SWRL:** The CONSTRUCT rule produced **+1 inferred triple**: `Customer4471 rdf:type RetentionRisk`. All five conditions matched — Premium segment, active status, EMEA location under GDPR, escalated support ticket. This triple can now drive downstream processes: alerting the account team, triggering a retention workflow, or feeding a governance dashboard. Note how the rule **chained on the OWL-derived `PremiumCustomer` type** — the reasoning regimes compose.

# COMMAND ----------

# DBTITLE 1,Customer4471 triples after SWRL inference
# Display Customer4471 triples after SWRL inference
print("═" * 60)
print("Customer4471 triples after SWRL inference:")
print("═" * 60)
display_triples(g, "https://ontobricks.example/customer360#Customer4471")

# COMMAND ----------

# DBTITLE 1,Part 3 — SPARQL CONSTRUCT
# MAGIC %md
# MAGIC ## Part 3 — SPARQL CONSTRUCT: Transformational Rules
# MAGIC
# MAGIC The third regime uses **SPARQL CONSTRUCT** for transformations that neither OWL nor SWRL can express: aggregations, projections, and decision-table rows. The file `construct.rq` contains three rules:
# MAGIC
# MAGIC | Rule | Type | What it derives |
# MAGIC | --- | --- | --- |
# MAGIC | **residency** | Transformation | `hasResidencyConstraint EU_Residency` for customers in GDPR-governed regions |
# MAGIC | **spend** | Aggregation | `totalOrderValue` — SUM of all order line values per customer |
# MAGIC | **priority** | Decision-table (DMN) | `derivedPriority "HIGH"` for Premium customers with escalated tickets |
# MAGIC
# MAGIC The aggregate rule (spend) demonstrates why SPARQL CONSTRUCT is the most-used reasoning tool in production graphs: **counts and sums are not expressible in OWL or SWRL**.
# MAGIC
# MAGIC Each rule is extracted from the multi-rule file using section markers (`# === name ===`) and applied individually so we can track derivation counts.

# COMMAND ----------

# DBTITLE 1,Reload graph for CONSTRUCT rules
g = init()

# COMMAND ----------

# DBTITLE 1,Listing 6.10 — _read_rule helper
# Listing 6.10 — Extract a named section from a multi-rule .rq file.
# construct.rq bundles three CONSTRUCT rules in one file, each delimited by
# a '# === name ===' marker. This helper supports 'file.rq#section' syntax
# (e.g. 'construct.rq#residency') and re-attaches the shared PREFIX header.
from pathlib import Path

def _read_rule(spec: str) -> str:
    if "#" in spec:
        path, section = spec.split("#", 1)
        text = Path(path).read_text()
        marker = f"\n# === {section} ==="
        sep    = "\n# === "
        block  = text.split(marker, 1)[1].split(sep, 1)[0]
        header = text.split(sep, 1)[0]          # PREFIX lines
        return header + block
    return Path(spec).read_text()

# Quick sanity: show extracted section names
import re
raw = Path("construct.rq").read_text()
names = re.findall(r'# === (\w+) ===', raw)
print(f"Sections in construct.rq: {names}")

# COMMAND ----------

# DBTITLE 1,Prepare graph for CONSTRUCT rules
# Prepare the graph for CONSTRUCT rules.
# Two prerequisites before the SPARQL rules can fire:
#   1. Remap c360:type → rdf:type — init() loads predicates verbatim from the UC
#      table, but SPARQL's 'a' keyword is shorthand for rdf:type. Without this
#      remap, triple-pattern matching on 'a c360:PremiumCustomer' would fail.
from rdflib import URIRef, Literal, Namespace, RDF
import time

C360_NS = Namespace("https://ontobricks.example/customer360#")
type_pred = URIRef(str(C360_NS) + "type")
remapped = 0
for s, p, o in list(g.triples((None, type_pred, None))):
    g.remove((s, p, o))
    g.add((s, RDF.type, o))
    remapped += 1

# 2 — Scenario triples needed by residency + priority rules
scenario_triples = [
    (C360_NS.GDPR,         RDF.type,              C360_NS.Regulation),
    (C360_NS.RegionEMEA,   C360_NS.governedBy,    C360_NS.GDPR),
    (C360_NS.Ticket4471,   RDF.type,              C360_NS.SupportTicket),
    (C360_NS.Customer4471, C360_NS.raised,         C360_NS.Ticket4471),
    (C360_NS.Ticket4471,   C360_NS.escalatedTo,   C360_NS.Account8801),
]
for t in scenario_triples:
    g.add(t)

print(f"Remapped {remapped} c360:type → rdf:type")
print(f"Added {len(scenario_triples)} scenario triples")
print(f"Graph ready: {len(g)} triples")

# COMMAND ----------

# DBTITLE 1,Rule 1 — EU-residency (transformation)
# Rule 1 — EU-residency: customers in GDPR-governed regions get a residency constraint
rule_text = _read_rule("construct.rq#residency")
print("SPARQL rule:\n" + rule_text)
print("\n" + "─" * 50)

before = len(g)
n_new, elapsed = apply_construct(g, _rule_text=rule_text)
print(f"\n+{n_new} derived triples  [{elapsed:.3f}s]")
print(f"Graph: {before} → {len(g)} triples")

# Show derived residency constraints
for s, p, o in g.triples((None, C360_NS.hasResidencyConstraint, None)):
    print(f"  ✓ {str(s).split('#')[-1]}  hasResidencyConstraint  {str(o).split('#')[-1]}")

# COMMAND ----------

# DBTITLE 1,Result — Residency rule
# MAGIC %md
# MAGIC > **Result — Residency:** +2 derived triples. Both `Customer4471` and `Customer5012` are located in `RegionEMEA`, which is governed by GDPR, so both receive `hasResidencyConstraint EU_Residency`. This is a **transformation rule** — it projects a new property from existing graph patterns. It feeds the access-control and governance work of Chapter 10.

# COMMAND ----------

# DBTITLE 1,Rule 2 — Total spend (aggregate)
# Rule 2 — Aggregate: sum each customer's order line values into totalOrderValue
rule_text = _read_rule("construct.rq#spend")
print("SPARQL rule:\n" + rule_text)
print("\n" + "─" * 50)

before = len(g)
n_new, elapsed = apply_construct(g, _rule_text=rule_text)
print(f"\n+{n_new} derived triples  [{elapsed:.3f}s]")
print(f"Graph: {before} → {len(g)} triples")

# Show derived totals
for s, p, o in g.triples((None, C360_NS.totalOrderValue, None)):
    print(f"  ✓ {str(s).split('#')[-1]}  totalOrderValue  {o}")

# COMMAND ----------

# DBTITLE 1,Result — Spend rule
# MAGIC %md
# MAGIC > **Result — Spend:** +0 derived triples. The graph currently lacks `contains` and `lineValue` triples for order line items, so the SUM aggregation has nothing to aggregate. With order-line data populated, this rule would compute `totalOrderValue` per customer — a derivation **impossible in OWL or SWRL** because they cannot express counts or sums.

# COMMAND ----------

# DBTITLE 1,Rule 3 — Support priority (decision-table row)
# Rule 3 — Decision-table: Premium + escalated ticket ⇒ derivedPriority "HIGH"
rule_text = _read_rule("construct.rq#priority")
print("SPARQL rule:\n" + rule_text)
print("\n" + "─" * 50)

before = len(g)
n_new, elapsed = apply_construct(g, _rule_text=rule_text)
print(f"\n+{n_new} derived triples  [{elapsed:.3f}s]")
print(f"Graph: {before} → {len(g)} triples")

# Show derived priorities
for s, p, o in g.triples((None, C360_NS.derivedPriority, None)):
    print(f"  ✓ {str(s).split('#')[-1]}  derivedPriority  {o}")

# COMMAND ----------

# DBTITLE 1,Result — Priority rule
# MAGIC %md
# MAGIC > **Result — Priority:** +1 derived triple. `Ticket4471 derivedPriority "HIGH"` — because Customer4471 is a `PremiumCustomer` (the SPARQL `a` keyword matched after our `rdf:type` remap) and `Ticket4471` was escalated. This is a **decision-table row** (DMN-style): one CONSTRUCT rule per grid row, mechanically compiled from a business-owned table. Business users own the grid; glue code compiles each row.

# COMMAND ----------

# DBTITLE 1,Verification — Customer4471 enriched triples
# Verification — all Customer4471 triples after the three CONSTRUCT rules
print("═" * 60)
print("Customer4471 — enriched triples after SPARQL CONSTRUCT rules:")
print("═" * 60)
display_triples(g, "https://ontobricks.example/customer360#Customer4471")

# COMMAND ----------

# DBTITLE 1,Lab Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC > **Final state:** Customer4471 went from **8 asserted triples** to **10+ enriched triples** across three reasoning regimes. The derived facts — `RetentionRisk` type, `EU_Residency` constraint, `HIGH` priority on the escalated ticket — were never stated by anyone. They emerged from **rules applied to the graph structure**. This is what "making implicit knowledge explicit" means in practice.
# MAGIC >
# MAGIC > | Regime | Derived for Customer4471 |
# MAGIC > | --- | --- |
# MAGIC > | OWL 2 RL | `rdf:type Customer` (via subclass inference) |
# MAGIC > | SWRL | `rdf:type RetentionRisk` (5-condition business rule) |
# MAGIC > | CONSTRUCT — residency | `hasResidencyConstraint EU_Residency` |
# MAGIC > | CONSTRUCT — spend | *(no data yet — needs order line items)* |
# MAGIC > | CONSTRUCT — priority | `Ticket4471 derivedPriority "HIGH"` |

# COMMAND ----------

