--------------------------------------------------------------------------------
-- DS2_Oracle_SparkSQL_Views_from_REST.sql
-- Oracle XE 21c / FDBO schema  (DS_1)
-- REST source: Spring Boot service on port 8090
--              docker: http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle
--              local:  http://localhost:8090/DSA-SQL-oracle/rest/oracle
--
-- NOTE: The springboot service in docker-compose is currently commented out.
--       Uncomment it and adjust the port/context-path once deployed.
--       Until then use localhost:8090 from the IDE run config.
--------------------------------------------------------------------------------

-- ── 1. USERS ─────────────────────────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/users');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'ORCL_USERS_JSON_VIEW',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/users');

CREATE OR REPLACE VIEW orcl_users_view AS
SELECT v.*
FROM ORCL_USERS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM orcl_users_view LIMIT 10;
SELECT COUNT(*) AS total_users FROM orcl_users_view;

--------------------------------------------------------------------------------
-- ── 2. SUBSCRIPTIONS ─────────────────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscriptions');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'ORCL_SUBSCRIPTIONS_JSON_VIEW',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscriptions');

CREATE OR REPLACE VIEW orcl_subscriptions_view AS
SELECT v.*
FROM ORCL_SUBSCRIPTIONS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM orcl_subscriptions_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── 3. SUBSCRIPTION INVOICES ─────────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_invoices');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'ORCL_SUB_INVOICES_JSON_VIEW',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_invoices');

CREATE OR REPLACE VIEW orcl_sub_invoices_view AS
SELECT v.*
FROM ORCL_SUB_INVOICES_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM orcl_sub_invoices_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── 4. SUBSCRIPTION INVOICE LINES ────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_invoice_lines');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'ORCL_SUB_INVOICE_LINES_JSON_VIEW',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_invoice_lines');

CREATE OR REPLACE VIEW orcl_sub_invoice_lines_view AS
SELECT v.*
FROM ORCL_SUB_INVOICE_LINES_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM orcl_sub_invoice_lines_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── 5. SUBSCRIPTION TIERS ────────────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_tiers');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'ORCL_SUB_TIERS_JSON_VIEW',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_tiers');

CREATE OR REPLACE VIEW orcl_sub_tiers_view AS
SELECT v.*
FROM ORCL_SUB_TIERS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM orcl_sub_tiers_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── 6. SUBSCRIPTION TIER PRICING ─────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_tier_pricing');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'ORCL_TIER_PRICING_JSON_VIEW',
    'http://iis-springboot:8083/DSA-SQL-oracle/rest/oracle/subscription_tier_pricing');

CREATE OR REPLACE VIEW orcl_tier_pricing_view AS
SELECT v.*
FROM ORCL_TIER_PRICING_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM orcl_tier_pricing_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── WITH AUTHENTICATION ───────────────────────────────────────────────────────

-- SELECT java_method(
--     'org.spark.service.rest.RESTEnabledSQLService',
--     'createJSONViewFromREST',
--     'ORCL_USERS_JSON_VIEW',
--     'http://developer:iis@iis-springboot:8083/DSA-SQL-oracle/rest/oracle/users');

--------------------------------------------------------------------------------
