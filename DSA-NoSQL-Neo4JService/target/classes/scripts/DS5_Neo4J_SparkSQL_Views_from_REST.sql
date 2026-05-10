--------------------------------------------------------------------------------
--- DS5_Neo4J_SparkSQL_Views_from_REST.sql
--- Neo4j Graph Relationships (DS_5) -- BOUGHT_WITH co-purchase affinity
--- DSA-port: 8094  Context: /DSA-NoSQL-Neo4JService/rest
--------------------------------------------------------------------------------
--- 1. bought_with
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
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM neo4j_bought_with_view;

SELECT product1Id, product1Name, product2Id, product2Name, coPurchaseCount
FROM neo4j_bought_with_view
ORDER BY coPurchaseCount DESC;
--------------------------------------------------------------------------------
--- With AUTHENTICATION
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'NEO4J_BOUGHT_WITH_JSON_VIEW',
               'http://developer:iis@host.docker.internal:8094/DSA-NoSQL-Neo4JService/rest/neo4j/bought_with');

SELECT * FROM NEO4J_BOUGHT_WITH_JSON_VIEW;

CREATE OR REPLACE VIEW neo4j_bought_with_view AS
SELECT v.*
FROM NEO4J_BOUGHT_WITH_JSON_VIEW AS json_view
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM neo4j_bought_with_view;
--------------------------------------------------------------------------------
