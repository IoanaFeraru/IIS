--------------------------------------------------------------------------------
--- DS4_MongoDB_SparkSQL_Views_from_REST.sql
--- MongoDB Products Catalog (DS_4)
--- DSA-port: 8093  Context: /DSA-NoSQL-MongoDBService/rest
---
--- WORKFLOW:
---   STEP 1: createJSONViewFromREST  → creates JSON_VIEW in Spark memory
---   STEP 2: CREATE OR REPLACE VIEW  → explodes JSON_VIEW into typed live view
---   STEP 3: CREATE TABLE USING parquet → persists to disk (run once, offline-safe)
---   STEP 4: Shut down Spring + MongoDB, query *_persisted tables freely
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--- 1. products
--------------------------------------------------------------------------------


SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8093/DSA-NoSQL-MongoDBService/rest/mongo/products');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'MG_PRODUCTS_JSON_VIEW',
               'http://host.docker.internal:8093/DSA-NoSQL-MongoDBService/rest/mongo/products');

CREATE OR REPLACE VIEW mg_products_view AS
SELECT v.*
FROM MG_PRODUCTS_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM mg_products_view LIMIT 10;
SELECT COUNT(*) AS total_products FROM mg_products_view;
SELECT productType, COUNT(*) AS cnt FROM mg_products_view GROUP BY productType ORDER BY cnt DESC;
SELECT isActive, COUNT(*) AS cnt FROM mg_products_view GROUP BY isActive;

--- PERSIST ---
-- DROP TABLE IF EXISTS mg_products_persisted;
CREATE TABLE IF NOT EXISTS mg_products_persisted
    USING parquet AS SELECT * FROM mg_products_view;
CACHE TABLE mg_products_persisted;

CREATE OR REPLACE VIEW mg_products_view_offline AS
SELECT * FROM mg_products_persisted;

SELECT * FROM mg_products_view_offline LIMIT 10;
SELECT COUNT(*) AS total_products FROM mg_products_view_offline;
SELECT productType, COUNT(*) AS cnt FROM mg_products_view_offline GROUP BY productType ORDER BY cnt DESC;
SELECT isActive, COUNT(*) AS cnt FROM mg_products_view_offline GROUP BY isActive;