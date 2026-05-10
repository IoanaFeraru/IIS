--------------------------------------------------------------------------------
-- DS5_Neo4J_SparkSQL_Views_from_REST.sql
-- Neo4J 5 / iis graph  (DS_5)
-- REST source: Neo4J HTTP API on port 7474 → http://neo4j:7474 (docker)
--                                          → http://localhost:7474 (local)
--
-- Neo4J HTTP API endpoint:
--   POST http://neo4j:7474/db/neo4j/tx/commit
--   Body: { "statements": [ { "statement": "MATCH (n:Label) RETURN n LIMIT 100" } ] }
--   Auth: Basic neo4j/neo4j_admin
--
-- IMPORTANT: QueryRESTDataService.getRESTDataDocument() does GET requests.
-- Neo4J's transactional REST endpoint requires POST.
-- Use the Neo4J APOC REST endpoint instead (GET-compatible via APOC):
--   http://neo4j:7474/db/neo4j/query/v2  (Neo4J 5.x query API, GET not supported)
--
-- Recommended approach: expose Neo4J data via a small Spring Boot endpoint
-- (your existing DSA-NoSQL-Neo4JService) that wraps the Bolt driver and
-- returns a plain JSON array — then call that service URL here.
--
-- Placeholder (adjust once your Neo4J REST service is running):
--------------------------------------------------------------------------------

-- ── Via DSA-NoSQL-Neo4JService (Spring Boot wrapping Bolt) ───────────────────

SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://iis-neo4j-service:8085/DSA-NoSQL-neo4j/rest/neo4j/nodes?limit=5');

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'NEO4J_NODES_JSON_VIEW',
    'http://iis-neo4j-service:8085/DSA-NoSQL-neo4j/rest/neo4j/nodes?limit=10000');

CREATE OR REPLACE VIEW neo4j_nodes_view AS
SELECT v.*
FROM NEO4J_NODES_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM neo4j_nodes_view LIMIT 10;
SELECT COUNT(*) AS total_nodes FROM neo4j_nodes_view;

--------------------------------------------------------------------------------
-- ── Relationships ─────────────────────────────────────────────────────────────

SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'NEO4J_RELS_JSON_VIEW',
    'http://iis-neo4j-service:8085/DSA-NoSQL-neo4j/rest/neo4j/relationships?limit=10000');

CREATE OR REPLACE VIEW neo4j_rels_view AS
SELECT v.*
FROM NEO4J_RELS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

SELECT * FROM neo4j_rels_view LIMIT 10;

--------------------------------------------------------------------------------
