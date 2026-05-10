--------------------------------------------------------------------------------
-- DS1_PostgreSQL_SparkSQL_Views_from_REST.sql
-- PostgreSQL / iis_db  (DS_2)
-- REST source: PostgREST on port 3000 → http://postgrest-pg:3000 (docker)
--                                     → http://localhost:3000       (local)
--
-- PostgREST pagination: append ?limit=N to avoid pulling 1M rows at once.
-- Schema inference uses ?limit=1; view definition uses ?limit=50000.
-- Adjust the limit values to match your available driver memory.
--------------------------------------------------------------------------------

-- ── 1. ORDERS ────────────────────────────────────────────────────────────────

-- STEP 1: sanity check — fetch a raw JSON sample (small page)
SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://postgrest-pg:3000/orders?limit=5');

-- STEP 2: create the live JSON view (schema inferred from limit=1, view fetches limit=50000)
SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'PG_ORDERS_JSON_VIEW',
    'http://postgrest-pg:3000/orders?limit=50000');

-- STEP 3: unwrap the JSON array into rows
CREATE OR REPLACE VIEW pg_orders_view AS
SELECT v.*
FROM PG_ORDERS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

-- STEP 4: verify
SELECT * FROM pg_orders_view LIMIT 10;
SELECT COUNT(*) AS total_orders FROM pg_orders_view;

--------------------------------------------------------------------------------
-- ── 2. ORDER ITEMS ────────────────────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://postgrest-pg:3000/order_items?limit=5');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'PG_ORDER_ITEMS_JSON_VIEW',
    'http://postgrest-pg:3000/order_items?limit=50000');

CREATE OR REPLACE VIEW pg_order_items_view AS
SELECT v.*
FROM PG_ORDER_ITEMS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM pg_order_items_view LIMIT 10;
SELECT COUNT(*) AS total_order_items FROM pg_order_items_view;

--------------------------------------------------------------------------------
-- ── 3. MARKETPLACE INVOICES ───────────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://postgrest-pg:3000/marketplace_invoices?limit=5');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'PG_MARKETPLACE_INVOICES_JSON_VIEW',
    'http://postgrest-pg:3000/marketplace_invoices?limit=50000');

CREATE OR REPLACE VIEW pg_marketplace_invoices_view AS
SELECT v.*
FROM PG_MARKETPLACE_INVOICES_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM pg_marketplace_invoices_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── 4. MARKETPLACE INVOICE LINES ─────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://postgrest-pg:3000/marketplace_invoice_lines?limit=5');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'PG_MARKETPLACE_INVOICE_LINES_JSON_VIEW',
    'http://postgrest-pg:3000/marketplace_invoice_lines?limit=50000');

CREATE OR REPLACE VIEW pg_marketplace_invoice_lines_view AS
SELECT v.*
FROM PG_MARKETPLACE_INVOICE_LINES_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM pg_marketplace_invoice_lines_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── WITH AUTHENTICATION (if you add PGRST_JWT_SECRET / role check) ────────────

-- SELECT java_method(
--     'org.spark.service.rest.RESTEnabledSQLService',
--     'createJSONViewFromREST',
--     'PG_ORDERS_JSON_VIEW',
--     'http://developer:iis@postgrest-pg:3000/orders?limit=50000');

--------------------------------------------------------------------------------
