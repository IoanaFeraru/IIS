--------------------------------------------------------------------------------
--- DS5_Neo4J_SparkSQL_Views_from_REST.sql
--- Neo4j Graph Relationships (DS_5) — BOUGHT_WITH co-purchase affinity
--- DSA-port: 8094  Context: /DSA-NoSQL-Neo4JService/rest
---
--- WORKFLOW:
---   STEP 1: createJSONViewFromREST  → creates JSON_VIEW in Spark memory
---   STEP 2: CREATE OR REPLACE VIEW  → explodes JSON_VIEW into typed live view
---   STEP 3: CREATE TABLE USING parquet → persists to disk (run once, offline-safe)
---   STEP 4: Shut down Spring + Neo4j, query *_persisted tables freely
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
--- 1. bought_with
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8094/DSA-NoSQL-Neo4JService/rest/neo4j/bought_with');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'NEO4J_BOUGHT_WITH_JSON_VIEW',
               'http://host.docker.internal:8094/DSA-NoSQL-Neo4JService/rest/neo4j/bought_with');

CREATE OR REPLACE VIEW neo4j_bought_with_view AS
SELECT v.*
FROM NEO4J_BOUGHT_WITH_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

SELECT * FROM neo4j_bought_with_view LIMIT 10;
SELECT COUNT(*) AS total_relationships FROM neo4j_bought_with_view;
SELECT product1Id, product1Name, product2Id, product2Name, coPurchaseCount
FROM neo4j_bought_with_view
ORDER BY coPurchaseCount DESC;

--- PERSIST ---
-- DROP TABLE IF EXISTS neo4j_bought_with_persisted;
CREATE TABLE IF NOT EXISTS neo4j_bought_with_persisted
    USING parquet AS SELECT * FROM neo4j_bought_with_view;
CACHE TABLE neo4j_bought_with_persisted;

CREATE OR REPLACE VIEW neo4j_bought_with_view_offline AS
SELECT * FROM neo4j_bought_with_persisted;

SELECT * FROM neo4j_bought_with_view_offline LIMIT 10;
SELECT COUNT(*) AS total_relationships FROM neo4j_bought_with_view_offline;
SELECT product1Id, product1Name, product2Id, product2Name, coPurchaseCount
FROM neo4j_bought_with_view_offline
ORDER BY coPurchaseCount DESC;