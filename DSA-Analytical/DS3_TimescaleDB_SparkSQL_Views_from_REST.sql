--------------------------------------------------------------------------------
--- DS3_TimescaleDB_SparkSQL_Views_from_REST.sql
--- TimescaleDB Events Hypertable (DS_3)
--- DSA-port: 8092  Context: /DSA-SQL-timescale/rest
---
--- TWO MODES:
---   A) LIVE VIEW    — always fetches from REST at query time (datasource must be UP)
---   B) PERSISTED TABLE — data is snapshotted into Spark/Hive metastore once,
---                        then queryable even with datasource DOWN
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--- STEP 1: Raw document check (optional — sanity check that REST is reachable)
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8092/DSA-SQL-timescale/rest/ts/events');


--------------------------------------------------------------------------------
--- STEP 2: Create the JSON view (schema auto-inferred from REST response)
---          Run once per Spark session — creates TS_EVENTS_JSON_VIEW in memory
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'TS_EVENTS_JSON_VIEW',
               'http://host.docker.internal:8092/DSA-SQL-timescale/rest/ts/events');

--------------------------------------------------------------------------------
--- MODE A: LIVE VIEW
--- Re-fetches from REST every time you SELECT from it.
--- Requires: Spring REST service + TimescaleDB both running.
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ts_events_view AS
SELECT v.*
FROM TS_EVENTS_JSON_VIEW AS json_view
    LATERAL VIEW explode(json_view.`array`) AS v;

--- Verify live view
SELECT * FROM ts_events_view LIMIT 10;
SELECT COUNT(*) AS total_events FROM ts_events_view;
SELECT eventType, COUNT(*) AS cnt FROM ts_events_view GROUP BY eventType ORDER BY cnt DESC;

--------------------------------------------------------------------------------
--- MODE B: PERSISTED TABLE (offline-safe snapshot)
--- Run this ONCE while datasource is UP.
--- Data is saved into Spark's built-in Hive metastore (Derby + local warehouse).
--- After this, ts_events_persisted works even with REST service and DB shut down.
--------------------------------------------------------------------------------

--- B1: Drop old snapshot if re-running (comment out on first run)
-- DROP TABLE IF EXISTS ts_events_persisted;

--- B2: Snapshot current live data into a permanent Spark/Hive managed table
CREATE TABLE IF NOT EXISTS ts_events_persisted
    USING parquet
AS SELECT * FROM ts_events_view;

--- B3: Verify persisted data
SELECT * FROM ts_events_persisted LIMIT 10;
SELECT COUNT(*) AS total_events FROM ts_events_persisted;
SELECT eventType, COUNT(*) AS cnt FROM ts_events_persisted GROUP BY eventType ORDER BY cnt DESC;

--- B4: Cache in memory for fastest query performance (optional, survives session)
CACHE TABLE ts_events_persisted;
