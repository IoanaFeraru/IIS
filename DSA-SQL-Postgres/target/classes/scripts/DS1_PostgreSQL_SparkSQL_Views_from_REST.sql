--------------------------------------------------------------------------------
--- DS1_PostgreSQL_SparkSQL_Views_from_REST.sql
--- PostgreSQL Orders & Commerce Domain (DS_2)
--- DSA-port: 8091  Context: /DSA-SQL-postgres/rest
--------------------------------------------------------------------------------
--- 1. orders
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM pg_orders_view;
--------------------------------------------------------------------------------
--- 2. order_items
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM pg_order_items_view;
--------------------------------------------------------------------------------
--- 3. marketplace_invoices
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM pg_marketplace_invoices_view;
--------------------------------------------------------------------------------
--- 4. marketplace_invoice_lines
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM pg_marketplace_invoice_lines_view;
--------------------------------------------------------------------------------
--- With AUTHENTICATION
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'PG_ORDERS_JSON_VIEW',
               'http://developer:iis@host.docker.internal:8091/DSA-SQL-postgres/rest/pg/orders');

SELECT * FROM PG_ORDERS_JSON_VIEW;

CREATE OR REPLACE VIEW pg_orders_view AS
SELECT v.*
FROM PG_ORDERS_JSON_VIEW AS json_view
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM pg_orders_view;
--------------------------------------------------------------------------------
