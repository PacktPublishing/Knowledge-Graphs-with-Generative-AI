-- 00_seed_data.sql
-- ---------------------------------------------------------------------------
-- Chapter 4 — synthetic Customer 360 source tables for the lab.
-- Knowledge Graphs for AI Agents — Benoît Cayla
--
-- Run ONCE in a Databricks SQL editor or notebook to create the four Unity
-- Catalog tables the R2RML mapping consumes. The rows include the canonical
-- through-line instances from the project bible (Customer4471, Order9920) so
-- the verification step finds exactly the subgraph the book promises.
--
-- PRODUCES: ontobricks.customer360.{customers, accounts, orders, order_items}
-- ---------------------------------------------------------------------------

CREATE CATALOG IF NOT EXISTS ontobricks;
CREATE SCHEMA  IF NOT EXISTS ontobricks.customer360;

-- customers ------------------------------------------------------------------
CREATE OR REPLACE TABLE ontobricks.customer360.customers (
    customer_id     INT,
    full_name       STRING,
    lifetime_value  DECIMAL(12,2),   -- EUR
    status          STRING,          -- 'active' | 'suspended' | 'churned'
    created_at      TIMESTAMP,
    region_code     STRING           -- EMEA | AMER | APAC
);

INSERT INTO ontobricks.customer360.customers VALUES
  (4471, 'Acme Corp',     18400.00, 'active',    TIMESTAMP'2023-02-11 09:00:00', 'EMEA'),
  (5012, 'Bluewave Ltd',   9200.00, 'active',    TIMESTAMP'2024-06-01 14:30:00', 'EMEA'),
  (5519, 'Cedar Foods',   21750.00, 'active',    TIMESTAMP'2022-11-20 08:15:00', 'AMER'),
  (6033, 'Delta Mobility', 4300.00, 'suspended', TIMESTAMP'2025-01-09 16:45:00', 'APAC');

-- accounts -------------------------------------------------------------------
CREATE OR REPLACE TABLE ontobricks.customer360.accounts (
    account_id   INT,
    customer_id  INT,
    status       STRING,
    created_at   TIMESTAMP
);

INSERT INTO ontobricks.customer360.accounts VALUES
  (8801, 4471, 'active', TIMESTAMP'2023-02-11 09:05:00'),
  (8802, 5012, 'active', TIMESTAMP'2024-06-01 14:35:00'),
  (8803, 5519, 'active', TIMESTAMP'2022-11-20 08:20:00');

-- orders ---------------------------------------------------------------------
CREATE OR REPLACE TABLE ontobricks.customer360.orders (
    order_id     INT,
    customer_id  INT,
    status       STRING,
    created_at   TIMESTAMP
);

INSERT INTO ontobricks.customer360.orders VALUES
  (9920, 4471, 'fulfilled', TIMESTAMP'2026-03-01 10:00:00'),
  (9921, 4471, 'fulfilled', TIMESTAMP'2026-04-12 11:30:00'),
  (9940, 5519, 'fulfilled', TIMESTAMP'2026-02-18 13:10:00'),
  (9955, 5012, 'cancelled', TIMESTAMP'2026-05-02 09:45:00');

-- order_items (line items: which products each order contains) ---------------
CREATE OR REPLACE TABLE ontobricks.customer360.order_items (
    order_id    INT,
    product_id  STRING,
    quantity    INT
);

INSERT INTO ontobricks.customer360.order_items VALUES
  (9920, 'A', 2),
  (9920, 'B', 1),
  (9921, 'C', 5),
  (9940, 'A', 1),
  (9955, 'D', 3);

-- customer_aliases (identity reconciliation, Step 5) -------------------------
-- Records known duplicate identities: the legacy id is the SAME real-world
-- customer as the canonical id. Customer4471 also existed as legacy cust_88123
-- in the old CRM — the duplicate-identity story from Chapter 1.
CREATE OR REPLACE TABLE ontobricks.customer360.customer_aliases (
    legacy_id     STRING,
    canonical_id  INT
);

INSERT INTO ontobricks.customer360.customer_aliases VALUES
  ('cust_88123', 4471);
