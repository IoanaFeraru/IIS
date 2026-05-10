--------------------------------------------------------------------------------
-- DS3_TimescaleDB_SparkSQL_Views_from_REST.sql
-- TimescaleDB / iis_events  (DS_3)
-- REST source: PostgREST-TS on port 3001 → http://postgrest-ts:3001 (docker)
--                                        → http://localhost:3001      (local)
--
-- TimescaleDB hypertables can be very large. Always use ?limit= on PostgREST
-- calls. The view definition caps at 100 000 rows — reduce if memory is tight.
--------------------------------------------------------------------------------

-- ── 1. EVENTS (hypertable) ───────────────────────────────────────────────────

-- STEP 1: sanity check
SELECT java_method(
    'org.spark.service.rest.QueryRESTDataService',
    'getRESTDataDocument',
    'http://postgrest-ts:3000/events?limit=5');

-- STEP 2: create live view — limit protects against huge hypertable pulls
SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'TS_EVENTS_JSON_VIEW',
    'http://postgrest-ts:3000/events?limit=100000');

-- STEP 3: unwrap rows
CREATE OR REPLACE VIEW ts_events_view AS
SELECT v.*
FROM TS_EVENTS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

-- STEP 4: verify
SELECT * FROM ts_events_view LIMIT 10;
SELECT COUNT(*) AS total_events FROM ts_events_view;

-- STEP 5: analytical spot-checks
SELECT eventType, COUNT(*) AS cnt
FROM ts_events_view
GROUP BY eventType
ORDER BY cnt DESC;

--------------------------------------------------------------------------------
-- ── 2. EVENTS — time-filtered (recent 30 days only, safer for large tables) ──

-- PostgREST supports column filters: ?occurred_at=gte.2025-01-01
-- Use this variant in production to avoid scanning the entire hypertable.

-- SELECT java_method(
--     'org.spark.service.rest.RESTEnabledSQLService',
--     'createJSONViewFromREST',
--     'TS_EVENTS_RECENT_JSON_VIEW',
--     'http://postgrest-ts:3000/events?occurred_at=gte.2025-01-01&limit=100000');

-- CREATE OR REPLACE VIEW ts_events_recent_view AS
-- SELECT v.*
-- FROM TS_EVENTS_RECENT_JSON_VIEW AS j
-- LATERAL VIEW explode(j.array) AS v;

-- SELECT COUNT(*) FROM ts_events_recent_view;

--------------------------------------------------------------------------------
-- ── WITH AUTHENTICATION ───────────────────────────────────────────────────────

-- SELECT java_method(
--     'org.spark.service.rest.RESTEnabledSQLService',
--     'createJSONViewFromREST',
--     'TS_EVENTS_JSON_VIEW',
--     'http://developer:iis@postgrest-ts:3000/events?limit=100000');

--------------------------------------------------------------------------------
