--------------------------------------------------------------------------------
-- DS4_MongoDB_SparkSQL_Views_from_REST.sql
-- MongoDB / iis_db  (DS_4)
-- REST source: RestHeart 7 on port 8081 → http://restheart:8080 (docker)
--                                       → http://localhost:8081   (local)
--
-- RestHeart URL pattern: http://host:port/{db}/{collection}?pagesize=N
-- Your docker-compose mounts iis_db at '/' so collections are at:
--   http://restheart:8080/{collection_name}?pagesize=N
--
-- RestHeart wraps results in: { "_embedded": [ { ... }, ... ], "_size": N }
-- So the JSON view schema will show an _embedded array — unwrap with explode.
-- Max pagesize is 50000 (set in docker-compose RHO config).
--------------------------------------------------------------------------------

-- ── 1. Example: products (adjust collection name to match your MongoDB schema)

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://restheart:8080/products?pagesize=5');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'MONGO_PRODUCTS_JSON_VIEW',
    'http://restheart:8080/products?pagesize=25000');

-- RestHeart returns { "_embedded": [...], "_size": N, "_returned": N }
-- The array is in the _embedded field, not a top-level array.
CREATE OR REPLACE VIEW mongo_products_view AS
SELECT v.*
FROM MONGO_PRODUCTS_JSON_VIEW AS j
LATERAL VIEW explode(j.array._embedded) AS v;

SELECT * FROM mongo_products_view LIMIT 10;
SELECT COUNT(*) AS total_products FROM mongo_products_view;

--------------------------------------------------------------------------------
-- ── 2. Example: customers

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://restheart:8080/customers?pagesize=5');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'MONGO_CUSTOMERS_JSON_VIEW',
    'http://restheart:8080/customers?pagesize=25000');

CREATE OR REPLACE VIEW mongo_customers_view AS
SELECT v.*
FROM MONGO_CUSTOMERS_JSON_VIEW AS j
LATERAL VIEW explode(j.array._embedded) AS v;

SELECT * FROM mongo_customers_view LIMIT 10;

--------------------------------------------------------------------------------
-- ── NOTE on RestHeart response shape ─────────────────────────────────────────
--
-- If your LATERAL VIEW explode fails with "array field not found", check the
-- actual JSON structure first:
--
--   SELECT java_method(
--       'org.spark.service.rest.QueryRESTDataService',
--       'getRESTDataDocument',
--       'http://restheart:8080/your_collection?pagesize=1');
--
-- Then adjust the explode field path accordingly.
-- For top-level arrays use: LATERAL VIEW explode(j.array) AS v
-- For _embedded arrays use: LATERAL VIEW explode(j.array._embedded) AS v
--
--------------------------------------------------------------------------------
