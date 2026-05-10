--------------------------------------------------------------------------------
--- DS3_TimescaleDB_SparkSQL_Views_from_REST.sql
--- TimescaleDB Events Hypertable (DS_3)
--- DSA-port: 8092  Context: /DSA-SQL-timescale/rest
--------------------------------------------------------------------------------
--- 1. events
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8092/DSA-SQL-timescale/rest/ts/events');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'TS_EVENTS_JSON_VIEW',
               'http://host.docker.internal:8092/DSA-SQL-timescale/rest/ts/events');

CREATE OR REPLACE VIEW ts_events_view AS
SELECT v.*
FROM TS_EVENTS_JSON_VIEW AS json_view
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM ts_events_view;

SELECT COUNT(*) AS total_events FROM ts_events_view;
SELECT eventType, COUNT(*) AS cnt FROM ts_events_view GROUP BY eventType ORDER BY cnt DESC;
--------------------------------------------------------------------------------
--- With AUTHENTICATION
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'TS_EVENTS_JSON_VIEW',
               'http://developer:iis@host.docker.internal:8092/DSA-SQL-timescale/rest/ts/events');

SELECT * FROM TS_EVENTS_JSON_VIEW;

CREATE OR REPLACE VIEW ts_events_view AS
SELECT v.*
FROM TS_EVENTS_JSON_VIEW AS json_view
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM ts_events_view;
--------------------------------------------------------------------------------
