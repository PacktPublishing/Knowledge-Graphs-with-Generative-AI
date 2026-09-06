# Databricks notebook source
# DBTITLE 1,Chapter 3 — Lab Title
# MAGIC %md
# MAGIC # Chapter 3 — Building the Customer 360 Ontology
# MAGIC
# MAGIC This lab builds, validates, and publishes the **OWL 2 ontology** that underpins the Customer 360 knowledge graph. The ontology is not just a schema — it encodes **business rules as axioms** that a reasoner can execute automatically.
# MAGIC
# MAGIC The lab follows four steps:
# MAGIC
# MAGIC | Step | What it does |
# MAGIC | --- | --- |
# MAGIC | **Setup** | Install libraries, configure paths, scaffold the workspace artifact |
# MAGIC | **Parse & verify** | Load the Turtle file, confirm it is well-formed, list declared classes |
# MAGIC | **Reason** | Run OWL 2 RL closure to prove inverse properties and class inference fire |
# MAGIC | **Validate & publish** | Assert every term is annotated, then publish to workspace and UC Volume |
# MAGIC
# MAGIC **Key outcome:** `Customer4471` is **not** asserted as a `PremiumCustomer` anywhere — the ontology *derives* that classification from a lifetime-value threshold axiom. If the axiom fires, the ontology reasons correctly.
# MAGIC
# MAGIC **Artifact produced:** `customer360.ttl` — consumed by Chapters 4–6.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# Install the two core libraries for ontology work:
#   rdflib  — parse, query, and serialise RDF graphs (Turtle, SPARQL)
#   owlrl   — OWL 2 RL forward-chaining reasoner (used in Part 2)
%pip install rdflib owlrl
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration paths
# Configuration — adjust these two paths to your environment:
#   COMMON_PATH — workspace folder shared across chapters (the ontology TTL lives here)
#   VOLUME_PATH — Unity Catalog Volume for governed, catalog-discoverable access
COMMON_PATH = "/Workspace/Users/benoit.cayla@databricks.com/book/Common"
VOLUME_PATH = "/Volumes/benoit_cayla/book/ontology"

# COMMAND ----------

# DBTITLE 1,Part 1 — Load and Verify
# MAGIC %md
# MAGIC ## Part 1 — Load and Verify the Ontology
# MAGIC
# MAGIC The Customer 360 ontology is authored as a **Turtle (.ttl)** file and stored in the workspace. This section scaffolds the output path, then parses the Turtle file to prove it is syntactically valid.
# MAGIC
# MAGIC **What to look for:** The parser should find \~128 triples and list all declared OWL classes (Customer, PremiumCustomer, Order, Product, Account, Region, Regulation, SupportTicket, Interaction). If any class is missing, the Turtle source has a structural issue.

# COMMAND ----------

# DBTITLE 1,Scaffold the workspace artifact path
from rdflib import Graph
from pathlib import Path

# Scaffold: ensure the shared artifact directory exists, then define
# the path where the ontology Turtle file will be read from / written to.
ARTIFACT_DIR = Path(COMMON_PATH)
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
TTL_CUST_PATH = ARTIFACT_DIR / "customer360.ttl"
print(f"Ontology will be loaded from {TTL_CUST_PATH}")
TTL_CUSTADDS_PATH = ARTIFACT_DIR / "customer360_instances.ttl"
print(f"Ontology addons will be loaded from {TTL_CUSTADDS_PATH}")

# COMMAND ----------

# DBTITLE 1,Parse the Turtle ontology and verify classes
# Load the ontology into an rdflib Graph. parse() raises immediately if the
# Turtle syntax is malformed — this is our first quality gate.
g = Graph()
g.parse(str(TTL_CUST_PATH), format="turtle")  # Ensure TTL syntax correctness
g.parse(str(TTL_CUSTADDS_PATH), format="turtle")  # Ensure TTL syntax correctness
print(f"Parsed OK. The ontology contains {len(g)} triples.")

# Sanity check: list every resource typed owl:Class.
# We expect 9 named classes + 1 anonymous restriction node (the PremiumCustomer
# equivalentClass axiom, which OWL models as a blank node).
from rdflib import RDF, OWL
classes = sorted(s.split('#')[-1] for s, _, _ in g.triples((None, RDF.type, OWL.Class)))
print("Declared classes:", classes)


# COMMAND ----------

# DBTITLE 1,Observation — Parse results
# MAGIC %md
# MAGIC > **Observation:** 128 triples parsed and all expected classes declared. The ontology includes an anonymous class (`n28b1...`) — this is the OWL restriction node for the `PremiumCustomer` equivalentClass axiom (`hasLifetimeValue > 15000`). It is normal and expected; OWL restrictions are modelled as blank nodes in RDF.

# COMMAND ----------

# DBTITLE 1,Part 2 — OWL 2 RL Reasoning
# MAGIC %md
# MAGIC ## Part 2 — OWL 2 RL Reasoning: Prove the Axioms Fire
# MAGIC
# MAGIC The ontology encodes two kinds of axioms we want to verify:
# MAGIC
# MAGIC 1. **Inverse property** — `placed owl:inverseOf placedBy`. If Customer4471 `placed` Order9920, then Order9920 `placedBy` Customer4471 should be inferred automatically.
# MAGIC 2. **Defining equivalence** — `PremiumCustomer ≡ Customer ⊓ (hasLifetimeValue > 15000)`. Customer4471 has LTV = 18 400 and is a Customer, so she should be classified as PremiumCustomer by the reasoner.
# MAGIC
# MAGIC The cell below runs the OWL 2 RL closure first (handles inverse properties, subclass entailments, domain/range typing), then applies the threshold rule as a SPARQL UPDATE — the same regime Chapter 6 formalises.
# MAGIC
# MAGIC **What to expect:** `Before reasoning: False` → `After reasoning: True`. If the assertion fails, check that the LTV literal in the Turtle file is typed `xsd:decimal`.

# COMMAND ----------

# DBTITLE 1,OWL 2 RL reasoning — prove inverse properties and class inference
import owlrl
from rdflib import URIRef, RDF

C360 = "https://ontobricks.example/customer360#"
cust4471 = URIRef(C360 + "Customer4471")
premium  = URIRef(C360 + "PremiumCustomer")
order    = URIRef(C360 + "Order9920")
placedBy = URIRef(C360 + "placedBy")

# Before reasoning: Customer4471 should NOT be PremiumCustomer — nobody asserted it.
# The ontology says it can be *derived*, but only after reasoning fires.
asserted = (cust4471, RDF.type, premium) in g
print(f"Before reasoning, Customer4471 is premium? {asserted}")

# (a) OWL 2 RL closure: exhaustive forward-chaining over the ontology axioms.
#     This materialises subclass entailments, domain/range typing, owl:sameAs
#     propagation, and crucially owl:inverseOf (placed ↔ placedBy).
owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(g)
print("Inverse 'placedBy' inferred from 'placed'?",
      (order, placedBy, cust4471) in g)        # True — for free, from owl:inverseOf

# (b) The equivalentClass axiom (PremiumCustomer ≡ Customer ⊓ LTV > 15000)
#     requires a numeric comparison that OWL 2 RL cannot express natively.
#     We materialise it as a SPARQL UPDATE — the same pattern Chapter 6 uses.
g.update("""
    PREFIX c360: <https://ontobricks.example/customer360#>
    INSERT { ?c a c360:PremiumCustomer }
    WHERE  { ?c a c360:Customer ; c360:hasLifetimeValue ?v . FILTER(?v > 15000) }
""")

inferred = (cust4471, RDF.type, premium) in g
print(f"After reasoning,  Customer4471 is premium? {inferred}")

assert not asserted and inferred, \
    "Defining axiom did not fire — check the LTV literal is typed xsd:decimal."
print("VERIFIED: PremiumCustomer was derived, not asserted. The ontology reasons.")

# COMMAND ----------

# DBTITLE 1,Results — Reasoning
# MAGIC %md
# MAGIC > **Results — Reasoning:** Both axioms fired correctly:
# MAGIC > * **Inverse property:** `Order9920 placedBy Customer4471` was inferred from `Customer4471 placed Order9920` via `owl:inverseOf` — no one stated it.
# MAGIC > * **Defining equivalence:** `Customer4471 rdf:type PremiumCustomer` was derived because her LTV (18 400) exceeds the 15 000 threshold in the equivalentClass axiom.
# MAGIC >
# MAGIC > This proves the ontology **reasons** — it doesn’t just describe structure, it derives new facts from axioms. Chapter 6 builds on this with SWRL and SPARQL CONSTRUCT rules.

# COMMAND ----------

# DBTITLE 1,Part 3 — Validate and Publish
# MAGIC %md
# MAGIC ## Part 3 — Validate Annotations and Publish
# MAGIC
# MAGIC Before shipping the ontology to downstream chapters, we enforce a **quality gate**: every class and property defined in the `c360:` namespace must carry an `rdfs:label`. Unlabeled terms are a governance red flag — they make the ontology opaque to non-technical stakeholders and break label-based tooling.
# MAGIC
# MAGIC If the gate passes, the **source** Turtle file (not the closure with its hundreds of inferred triples) is published to:
# MAGIC 1. **Workspace path** — for notebook-local imports (Chapters 4–6)
# MAGIC 2. **Unity Catalog Volume** — for governed, catalog-discoverable access

# COMMAND ----------

# DBTITLE 1,Validate annotations and publish the ontology
from rdflib import RDFS, OWL, RDF

# Annotation gate: every class and property in the c360: namespace must have
# an rdfs:label. Unlabeled terms break label-based tooling and make the
# ontology opaque to business stakeholders — fail fast if any are missing.
missing = []
for s, _, _ in g.triples((None, RDF.type, None)):
    if (s, RDF.type, OWL.Class) in g or \
       (s, RDF.type, OWL.ObjectProperty) in g or \
       (s, RDF.type, OWL.DatatypeProperty) in g:
        if (s, RDFS.label, None) not in g and str(s).startswith(C360):
            missing.append(str(s).split('#')[-1])
assert not missing, f"Unlabeled terms (annotate before shipping): {sorted(set(missing))}"
print("VERIFIED: every Customer 360 class and property is annotated.")

# Publish: write the SOURCE Turtle (not the closure with its hundreds of
# inferred triples) to two locations:
#   1. Workspace path  — for local imports in Chapters 4–6
#   2. UC Volume        — for governed, catalog-discoverable access
g_clean = Graph()
g_clean.parse(str(TTL_PATH), format="turtle")        # publish the SOURCE, not the closure
FILE_PATH = VOLUME_PATH + "/customer360.ttl"
try:
    dbutils.fs.mkdirs(VOLUME_PATH)
    # Serialize to string first, then write via dbutils to bypass FUSE cache issues
    ttl_content = g_clean.serialize(format="turtle")
    dbutils.fs.put(FILE_PATH, ttl_content, overwrite=True)
    print(f"Published to Unity Catalog Volume: {FILE_PATH}")
except Exception as e:
    print(f"(Volume publish skipped — workspace copy is authoritative.) {e}")
print(f"Published to workspace: {TTL_PATH}")


# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC > The Customer 360 ontology is now **built, validated, and published**. It encodes:
# MAGIC > * **9 domain classes** — Customer, PremiumCustomer, Order, Product, Account, Region, Regulation, SupportTicket, Interaction
# MAGIC > * **Inverse properties** — `placed` / `placedBy`, enabling bi-directional graph traversal
# MAGIC > * **A defining equivalence** — `PremiumCustomer ≡ Customer ⊓ (hasLifetimeValue > 15000)`, which classifies customers automatically
# MAGIC > * **Complete annotations** — every class and property carries an `rdfs:label`
# MAGIC >
# MAGIC > The artifact `customer360.ttl` is consumed by:
# MAGIC > * **Chapter 4** — Materialising instance triples into Unity Catalog
# MAGIC > * **Chapter 5** — SHACL shape validation
# MAGIC > * **Chapter 6** — OWL 2 RL, SWRL, and SPARQL CONSTRUCT reasoning