--------------------------------------------------------------------------------
--- DS1_PostgreSQL_SparkSQL_Views_from_REST.sql
--- PostgreSQL Orders & Commerce Domain (DS_1)
--- DSA-port: 8091  Context: /DSA-SQL-postgres/rest
---
--- WORKFLOW:
---   STEP 1: createJSONViewFromREST  → creates JSON_VIEW in Spark memory
---   STEP 2: CREATE OR REPLACE VIEW  → explodes JSON_VIEW into typed live view
---   STEP 3: CREATE TABLE USING parquet → persists to disk (run once, offline-safe)
---   STEP 4: Shut down Spring + PostgreSQL, query *_persisted tables freely
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--- 1. orders
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/orders');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'PG_ORDERS_JSON_VIEW',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/orders');

CREATE OR REPLACE VIEW pg_orders_view AS
SELECT v.*
FROM PG_ORDERS_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM pg_orders_view LIMIT 10;
SELECT COUNT(*) AS total_orders FROM pg_orders_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS pg_orders_persisted;
CREATE TABLE IF NOT EXISTS pg_orders_persisted
    USING parquet AS SELECT * FROM pg_orders_view;

CACHE TABLE pg_orders_persisted;

CREATE OR REPLACE VIEW pg_orders_view_offline AS
SELECT * FROM pg_orders_persisted;

SELECT * FROM pg_orders_view_offline;
SELECT COUNT(*) AS total_orders FROM pg_orders_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 2. order_items
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/order_items');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'PG_ORDER_ITEMS_JSON_VIEW',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/order_items');

CREATE OR REPLACE VIEW pg_order_items_view AS
SELECT v.*
FROM PG_ORDER_ITEMS_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM pg_order_items_view LIMIT 10;
SELECT COUNT(*) AS total_order_items FROM pg_order_items_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS pg_order_items_persisted;
CREATE TABLE IF NOT EXISTS pg_order_items_persisted
    USING parquet AS SELECT * FROM pg_order_items_view;

CACHE TABLE pg_order_items_persisted;

CREATE OR REPLACE VIEW pg_order_items_view_offline AS
SELECT * FROM pg_order_items_persisted;

SELECT * FROM pg_order_items_view_offline LIMIT 10;
SELECT COUNT(*) AS total_order_items FROM pg_order_items_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 3. marketplace_invoices
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/marketplace_invoices');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'PG_MARKETPLACE_INVOICES_JSON_VIEW',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/marketplace_invoices');

CREATE OR REPLACE VIEW pg_marketplace_invoices_view AS
SELECT v.*
FROM PG_MARKETPLACE_INVOICES_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM pg_marketplace_invoices_view LIMIT 10;
SELECT COUNT(*) AS total_marketplace_invoices FROM pg_marketplace_invoices_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS pg_marketplace_invoices_persisted;
CREATE TABLE IF NOT EXISTS pg_marketplace_invoices_persisted
    USING parquet AS SELECT * FROM pg_marketplace_invoices_view;
CACHE TABLE pg_marketplace_invoices_persisted;

CREATE OR REPLACE VIEW pg_marketplace_invoices_view_offline AS
SELECT * FROM pg_marketplace_invoices_persisted;

SELECT * FROM pg_marketplace_invoices_view_offline LIMIT 10;
SELECT COUNT(*) AS total_marketplace_invoices FROM pg_marketplace_invoices_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 4. marketplace_invoice_lines
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/marketplace_invoice_lines');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'PG_MARKETPLACE_INVOICE_LINES_JSON_VIEW',
               'http://host.docker.internal:8091/DSA-SQL-postgres/rest/pg/marketplace_invoice_lines');

CREATE OR REPLACE VIEW pg_marketplace_invoice_lines_view AS
SELECT v.*
FROM PG_MARKETPLACE_INVOICE_LINES_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM pg_marketplace_invoice_lines_view LIMIT 10;
SELECT COUNT(*) AS total_invoice_lines FROM pg_marketplace_invoice_lines_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS pg_marketplace_invoice_lines_persisted;
CREATE TABLE IF NOT EXISTS pg_marketplace_invoice_lines_persisted
    USING parquet AS SELECT * FROM pg_marketplace_invoice_lines_view;
CACHE TABLE pg_marketplace_invoice_lines_persisted;

CREATE OR REPLACE VIEW pg_marketplace_invoice_lines_view_offline AS
SELECT * FROM pg_marketplace_invoice_lines_persisted;

SELECT * FROM pg_marketplace_invoice_lines_view_offline LIMIT 10;
SELECT COUNT(*) AS total_invoice_lines FROM pg_marketplace_invoice_lines_view_offline;