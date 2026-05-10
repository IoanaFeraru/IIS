--------------------------------------------------------------------------------
--- DS0_CSV_SparkSQL_Views_from_REST.sql
--- Seller Profiles CSV (External Data Source)
--- DSA-port: 8097  Context: /DSA-DOC-CSVService/rest
---
--- WORKFLOW:
---   STEP 1: createJSONViewFromREST  → creates JSON_VIEW in Spark memory
---   STEP 2: CREATE OR REPLACE VIEW  → explodes JSON_VIEW into typed live view
---   STEP 3: CREATE TABLE USING parquet → persists to disk (run once, offline-safe)
---   STEP 4: Shut down Spring + CSV service, query *_persisted tables freely
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--- 1. seller_profiles
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8097/DSA-DOC-CSVService/rest/csv/seller_profiles');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CSV_SELLER_PROFILES_JSON_VIEW',
               'http://host.docker.internal:8097/DSA-DOC-CSVService/rest/csv/seller_profiles');

CREATE OR REPLACE VIEW csv_seller_profiles_view AS
SELECT v.*
FROM CSV_SELLER_PROFILES_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM csv_seller_profiles_view LIMIT 10;
SELECT COUNT(*) AS total_seller_profiles FROM csv_seller_profiles_view;
SELECT countryCode, COUNT(*) AS cnt FROM csv_seller_profiles_view GROUP BY countryCode ORDER BY cnt DESC;
SELECT isVerified, COUNT(*) AS cnt FROM csv_seller_profiles_view GROUP BY isVerified;

--- PERSIST ---
-- DROP TABLE IF EXISTS csv_seller_profiles_persisted;
CREATE TABLE IF NOT EXISTS csv_seller_profiles_persisted
    USING parquet AS SELECT * FROM csv_seller_profiles_view;
CACHE TABLE csv_seller_profiles_persisted;

CREATE OR REPLACE VIEW csv_seller_profiles_view_offline AS
SELECT * FROM csv_seller_profiles_persisted;

SELECT * FROM csv_seller_profiles_view_offline LIMIT 10;
SELECT COUNT(*) AS total_seller_profiles FROM csv_seller_profiles_view_offline;
SELECT countryCode, COUNT(*) AS cnt FROM csv_seller_profiles_view_offline GROUP BY countryCode ORDER BY cnt DESC;
SELECT isVerified, COUNT(*) AS cnt FROM csv_seller_profiles_view_offline GROUP BY isVerified;
