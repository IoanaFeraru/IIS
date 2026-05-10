--------------------------------------------------------------------------------
--- DS2_Oracle_SparkSQL_Views_from_REST.sql
--- Oracle Subscription & User Domain (DS_2 / FDBO schema)
--- DSA-port: 8090  Context: /DSA-SQL-oracle/rest
---
--- WORKFLOW:
---   STEP 1: createJSONViewFromREST  → creates JSON_VIEW in Spark memory
---   STEP 2: CREATE OR REPLACE VIEW  → explodes JSON_VIEW into typed live view
---   STEP 3: CREATE TABLE USING parquet → persists to disk (run once, offline-safe)
---   STEP 4: Shut down Spring + Oracle, query *_persisted tables freely
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--- 1. USERS
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/users');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ORCL_USERS_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/users');

CREATE OR REPLACE VIEW orcl_users_view AS
SELECT v.*
FROM ORCL_USERS_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM orcl_users_view LIMIT 10;
SELECT COUNT(*) AS total_users FROM orcl_users_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS orcl_users_persisted;
CREATE TABLE IF NOT EXISTS orcl_users_persisted
    USING parquet AS SELECT * FROM orcl_users_view;

CACHE TABLE orcl_users_persisted;

CREATE OR REPLACE VIEW orcl_users_view_offline AS
SELECT * FROM orcl_users_persisted;

SELECT * FROM orcl_users_view_offline LIMIT 10;
SELECT COUNT(*) AS total_users FROM orcl_users_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 2. SUBSCRIPTIONS
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscriptions');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ORCL_SUBSCRIPTIONS_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscriptions');

CREATE OR REPLACE VIEW orcl_subscriptions_view AS
SELECT v.*
FROM ORCL_SUBSCRIPTIONS_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM orcl_subscriptions_view LIMIT 10;
SELECT COUNT(*) AS total_subscriptions FROM orcl_subscriptions_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS orcl_subscriptions_persisted;
CREATE TABLE IF NOT EXISTS orcl_subscriptions_persisted
    USING parquet AS SELECT * FROM orcl_subscriptions_view;
CACHE TABLE orcl_subscriptions_persisted;

CREATE OR REPLACE VIEW orcl_subscriptions_view_offline AS
SELECT * FROM orcl_subscriptions_persisted;

SELECT * FROM orcl_subscriptions_view_offline LIMIT 10;
SELECT COUNT(*) AS total_subscriptions FROM orcl_subscriptions_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 3. SUBSCRIPTION_INVOICES
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_invoices');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ORCL_SUB_INVOICES_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_invoices');

CREATE OR REPLACE VIEW orcl_sub_invoices_view AS
SELECT v.*
FROM ORCL_SUB_INVOICES_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM orcl_sub_invoices_view LIMIT 10;
SELECT COUNT(*) AS total_sub_invoices FROM orcl_sub_invoices_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS orcl_sub_invoices_persisted;
CREATE TABLE IF NOT EXISTS orcl_sub_invoices_persisted
    USING parquet AS SELECT * FROM orcl_sub_invoices_view;
CACHE TABLE orcl_sub_invoices_persisted;

CREATE OR REPLACE VIEW orcl_sub_invoices_view_offline AS
SELECT * FROM orcl_sub_invoices_persisted;

SELECT * FROM orcl_sub_invoices_view_offline LIMIT 10;
SELECT COUNT(*) AS total_sub_invoices FROM orcl_sub_invoices_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 4. SUBSCRIPTION_INVOICE_LINES
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_invoice_lines');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ORCL_SUB_INVOICE_LINES_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_invoice_lines');

CREATE OR REPLACE VIEW orcl_sub_invoice_lines_view AS
SELECT v.*
FROM ORCL_SUB_INVOICE_LINES_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM orcl_sub_invoice_lines_view LIMIT 10;
SELECT COUNT(*) AS total_invoice_lines FROM orcl_sub_invoice_lines_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS orcl_sub_invoice_lines_persisted;
CREATE TABLE IF NOT EXISTS orcl_sub_invoice_lines_persisted
    USING parquet AS SELECT * FROM orcl_sub_invoice_lines_view;
CACHE TABLE orcl_sub_invoice_lines_persisted;

CREATE OR REPLACE VIEW orcl_sub_invoice_lines_view_offline AS
SELECT * FROM orcl_sub_invoice_lines_persisted;

SELECT * FROM orcl_sub_invoice_lines_view_offline LIMIT 10;
SELECT COUNT(*) AS total_invoice_lines FROM orcl_sub_invoice_lines_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 5. SUBSCRIPTION_TIERS
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_tiers');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ORCL_SUB_TIERS_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_tiers');

CREATE OR REPLACE VIEW orcl_sub_tiers_view AS
SELECT v.*
FROM ORCL_SUB_TIERS_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM orcl_sub_tiers_view LIMIT 10;
SELECT COUNT(*) AS total_tiers FROM orcl_sub_tiers_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS orcl_sub_tiers_persisted;
CREATE TABLE IF NOT EXISTS orcl_sub_tiers_persisted
    USING parquet AS SELECT * FROM orcl_sub_tiers_view;
CACHE TABLE orcl_sub_tiers_persisted;

CREATE OR REPLACE VIEW orcl_sub_tiers_view_offline AS
SELECT * FROM orcl_sub_tiers_persisted;

SELECT * FROM orcl_sub_tiers_view_offline LIMIT 10;
SELECT COUNT(*) AS total_tiers FROM orcl_sub_tiers_view_offline;
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 6. SUBSCRIPTION_TIER_PRICING
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_tier_pricing');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ORCL_TIER_PRICING_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-oracle/rest/oracle/subscription_tier_pricing');

CREATE OR REPLACE VIEW orcl_tier_pricing_view AS
SELECT v.*
FROM ORCL_TIER_PRICING_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM orcl_tier_pricing_view LIMIT 10;
SELECT COUNT(*) AS total_tier_pricing FROM orcl_tier_pricing_view;

--- PERSIST ---
-- DROP TABLE IF EXISTS orcl_tier_pricing_persisted;
CREATE TABLE IF NOT EXISTS orcl_tier_pricing_persisted
    USING parquet AS SELECT * FROM orcl_tier_pricing_view;
CACHE TABLE orcl_tier_pricing_persisted;

CREATE OR REPLACE VIEW orcl_tier_pricing_view_offline AS
SELECT * FROM orcl_tier_pricing_persisted;

SELECT * FROM orcl_tier_pricing_view_offline LIMIT 10;
SELECT COUNT(*) AS total_tier_pricing FROM orcl_tier_pricing_view_offline;
