# Databricks notebook source
# DBTITLE 1,Init
# Databricks notebook cell — environment setup (glue, not the star)
%pip install rdflib owlrl
dbutils.library.restartPython()

# COMMAND ----------

# Replace this folder path by yours:
COMMON_PATH = "/Workspace/Users/benoit.cayla@databricks.com/book/Common"
# replace this Volume reference/apth by yours:
VOLUME_PATH = "/Volumes/benoit_cayla/book/ontology"

# COMMAND ----------

# DBTITLE 1,Scaffold the workspace and install the glue
from rdflib import Graph
from pathlib import Path

# Where the artifact will live in the workspace. Adjust to your repo path.
ARTIFACT_DIR = Path(COMMON_PATH)
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
TTL_PATH = ARTIFACT_DIR / "customer360.ttl"
print(f"Ontology will be written to: {TTL_PATH}")

# COMMAND ----------

# DBTITLE 1,Load and parse: prove it's well-formed
g = Graph()
g.parse(str(TTL_PATH), format="turtle")    # raises on malformed Turtle
print(f"Parsed OK. The ontology contains {len(g)} triples.")

# Sanity check: did all eight classes land?
from rdflib import RDF, OWL
classes = sorted(s.split('#')[-1] for s, _, _ in g.triples((None, RDF.type, OWL.Class)))
print("Declared classes:", classes)


# COMMAND ----------

# DBTITLE 1,Reason, prove it's coherent and that the axiom fires
import owlrl
from rdflib import URIRef, RDF

C360 = "https://ontobricks.example/customer360#"
cust4471 = URIRef(C360 + "Customer4471")
premium  = URIRef(C360 + "PremiumCustomer")
order    = URIRef(C360 + "Order9920")
placedBy = URIRef(C360 + "placedBy")

# Before reasoning: is she premium? (She should NOT be — nobody said so.)
asserted = (cust4471, RDF.type, premium) in g
print(f"Before reasoning, Customer4471 is premium? {asserted}")

# (a) Structural OWL 2 RL closure: subclass, inverse property, etc.
owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(g)
print("Inverse 'placedBy' inferred from 'placed'?",
      (order, placedBy, cust4471) in g)        # True — for free, from owl:inverseOf

# (b) Apply the PremiumCustomer threshold (the equivalentClass axiom),
#     materialized as a SPARQL rule — the regime Chapter 6 formalizes.
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

# DBTITLE 1,Validate annotations and publish
from rdflib import RDFS, OWL, RDF

# Annotation gate: every class and property must have a label.
missing = []
for s, _, _ in g.triples((None, RDF.type, None)):
    if (s, RDF.type, OWL.Class) in g or \
       (s, RDF.type, OWL.ObjectProperty) in g or \
       (s, RDF.type, OWL.DatatypeProperty) in g:
        if (s, RDFS.label, None) not in g and str(s).startswith(C360):
            missing.append(str(s).split('#')[-1])
assert not missing, f"Unlabeled terms (annotate before shipping): {sorted(set(missing))}"
print("VERIFIED: every Customer 360 class and property is annotated.")

# Publish: write the validated ontology to the workspace path,
# and optionally to a Unity Catalog Volume for downstream chapters.
g_clean = Graph()
g_clean.parse(str(TTL_PATH), format="turtle")        # publish the SOURCE, not the closure
VOLUME_PATH = VOLUME_PATH + "/customer360.ttl"
try:
    dbutils.fs.mkdirs(VOLUME_PATH)
    g_clean.serialize(destination=VOLUME_PATH, format="turtle")
    print(f"Published to Unity Catalog Volume: {VOLUME_PATH}")
except Exception as e:
    print(f"(Volume publish skipped — workspace copy is authoritative.) {e}")
print(f"Published to workspace: {TTL_PATH}")
