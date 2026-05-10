--------------------------------------------------------------------------------
--- DS2_Oracle_SparkSQL_Views_from_REST.sql
--- Oracle Subscription & User Domain (DS_1 / FDBO schema)
--- DSA-port: 8090  Context: /DSA-SQL-oracle/rest
--------------------------------------------------------------------------------
--- 1. USERS
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM orcl_users_view;
--------------------------------------------------------------------------------
--- 2. SUBSCRIPTIONS
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM orcl_subscriptions_view;
--------------------------------------------------------------------------------
--- 3. SUBSCRIPTION_INVOICES
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM orcl_sub_invoices_view;
--------------------------------------------------------------------------------
--- 4. SUBSCRIPTION_INVOICE_LINES
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM orcl_sub_invoice_lines_view;
--------------------------------------------------------------------------------
--- 5. SUBSCRIPTION_TIERS
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM orcl_sub_tiers_view;
--------------------------------------------------------------------------------
--- 6. SUBSCRIPTION_TIER_PRICING
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM orcl_tier_pricing_view;
--------------------------------------------------------------------------------
--- With AUTHENTICATION
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ORCL_USERS_JSON_VIEW',
               'http://developer:iis@localhost:8090/DSA-SQL-oracle/rest/oracle/users');

SELECT * FROM ORCL_USERS_JSON_VIEW;

CREATE OR REPLACE VIEW orcl_users_view AS
SELECT v.*
FROM ORCL_USERS_JSON_VIEW AS json_view
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM orcl_users_view;
--------------------------------------------------------------------------------
