# How to Create a Volume in Databricks (Using the UI)

A **Volume** is a storage location inside Databricks where you can keep files — such as CSV data, images, configuration files, or ontology artifacts — organized under Unity Catalog. Think of it as a managed folder that lives alongside your tables and is governed by the same permissions.

---

## Prerequisites

Before you begin, make sure you have:

* A Databricks workspace you can log into.
* At least **`CREATE VOLUME`** privilege on the schema (folder) where the Volume will live. If you are a workspace admin or the schema owner, you already have this.
* A **catalog** and a **schema** already created. If you don't have one yet, see the short primer at the end of this guide.

---

## Step-by-Step Instructions

### Step 1 — Open Catalog Explorer

1. Log into your Databricks workspace.
2. In the left sidebar, click the **Catalog** icon (it looks like a small database cylinder).
3. This opens the **Catalog Explorer**, the central place to browse all your data assets.

### Step 2 — Navigate to the Target Schema

1. In the Catalog Explorer tree on the left, expand the **catalog** where you want the Volume (e.g., `my_catalog`).
2. Then expand the **schema** (sometimes called a database) where the Volume should live (e.g., `my_schema`).
3. You should now see tabs or sections for Tables, Views, Volumes, Functions, and Models.

### Step 3 — Open the Volumes Section

1. Click on the **Volumes** tab (or section) inside the schema you selected.
2. You will see a list of existing Volumes (if any) and a **Create Volume** button.

### Step 4 — Click "Create Volume"

1. Click the **Create Volume** button in the upper-right area.
2. A creation dialog (or form) appears.

### Step 5 — Fill in the Volume Details

| Field | What to enter | Example |
| --- | --- | --- |
| **Volume name** | A short, lowercase name using letters, numbers, and underscores. No spaces. | `ontology` |
| **Volume type** | Choose **Managed** (Databricks handles the storage) unless you need to point to an external cloud path. | Managed |
| **Comment** *(optional)* | A human-readable description of what the Volume is for. | `Stores OWL/TTL ontology files for the Customer 360 project.` |

> **Managed vs. External**
> * **Managed** — Databricks stores the files for you. Simplest option; recommended for beginners.
> * **External** — You provide your own cloud storage path (S3, ADLS, or GCS). Use this when files must live in a specific bucket you already own.

### Step 6 — Confirm and Create

1. Review your entries.
2. Click **Create**.
3. The Volume appears in the list. Done!

---

## Verifying Your Volume

After creation you can verify it in two ways:

### Option A — Browse in Catalog Explorer

Click on the Volume name in the list. You'll see an empty file browser. You can use the **Upload** button here to drag-and-drop files into the Volume.

### Option B — Check from a Notebook

Open any notebook attached to a cluster and run:

```sql
SHOW VOLUMES IN my_catalog.my_schema;
```

You should see your new Volume listed.

To list files inside it:

```python
dbutils.fs.ls("/Volumes/my_catalog/my_schema/ontology")
```

*(Replace `my_catalog`, `my_schema`, and `ontology` with your actual names.)*

---

## Uploading Files to the Volume

You have several options:

* **Catalog Explorer UI** — Click the Volume, then use the **Upload** button to add files from your computer.
* **Notebook code** — Copy a file from the workspace or write content programmatically:

```python
# Example: copy a file from the workspace into the Volume
dbutils.fs.cp(
    "file:/Workspace/Users/you@example.com/data/myfile.csv",
    "/Volumes/my_catalog/my_schema/ontology/myfile.csv"
)
```

* **REST API / CLI** — For automation, use the Databricks CLI:

```bash
databricks fs cp ./local_file.csv /Volumes/my_catalog/my_schema/ontology/
```

---

## Quick Primer — Creating a Catalog and Schema First

If you don't already have a catalog and schema:

1. In **Catalog Explorer**, click **+ Create Catalog** at the top.
2. Give it a name (e.g., `my_catalog`) and click **Create**.
3. Inside the new catalog, click **+ Create Schema**.
4. Name it (e.g., `my_schema`) and click **Create**.

Now you can follow the Volume steps above inside that schema.

---

## Key Concepts Recap

| Concept | Analogy |
| --- | --- |
| **Catalog** | A top-level folder (like a drive letter). |
| **Schema** | A subfolder inside a catalog (groups related assets). |
| **Volume** | A file-storage area inside a schema (holds raw files). |
| **Table** | A structured dataset inside a schema (rows and columns). |

Volumes sit at the same level as tables inside a schema, but they hold **files** rather than structured data.

---

*Last updated: August 2026*
