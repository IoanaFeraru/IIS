# IIS-SparkSQL-Service

Spring Boot 3.3.5 + Apache Spark 3.5.5 federation gateway.  
Federates Oracle, PostgreSQL, TimescaleDB, MongoDB, Neo4J, CSV data sources  
into a single SparkSQL engine queryable via REST or JDBC (DBeaver).

---

## Architecture

```
DBeaver (JDBC :10000)
      │
      ▼
┌─────────────────────────────────────────────────┐
│           iis-spark-sql container               │
│                                                 │
│  Spring Boot :9990                              │
│   GET  /rest/ping                               │
│   GET  /rest/view/{VIEW}?limit=N&offset=M       │
│   GET  /rest/STRUCT/{VIEW}                      │
│   POST /_sqlrest/query                          │
│   POST /_sqlrest/create-json-view-from-rest     │
│                                                 │
│  HiveThriftServer2 :10000  ← DBeaver connects   │
│  SparkSession (local[*])                        │
│  Spark UI :4040                                 │
└──────────────┬──────────────────────────────────┘
               │ HTTP (docker network iis-network)
    ┌──────────┼──────────┬──────────┬──────────┐
    │          │          │          │          │
postgrest-pg  postgrest-ts  restheart  iis-springboot  iis-neo4j-svc
port 3000    port 3001    port 8080   port 8083        port 8085
(postgres)   (timescale)  (mongodb)  (oracle)          (neo4j)
```

---

## Build

```bash
cd IIS-SparkSQL-Service
mvn clean package -DskipTests
```

---

## Run (Docker)

```bash
# Minimum — Spark only (DBeaver + REST, no data sources yet):
docker compose up iis-spark-sql -d

# With PostgreSQL (DS_2):
docker compose --profile pg up -d

# With TimescaleDB (DS_3):
docker compose --profile ts up -d

# Lab minimum — Spark + PG + TS (enough for OLAP with pg+ts data):
docker compose --profile pg --profile ts up -d

# Everything:
docker compose --profile pg --profile ts --profile oracle --profile mongo --profile neo4j up -d
```

---

## Run (Local / IDE)

Add to VM options:
```
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
-Dspark.rest.callback.url=http://localhost:9990/IIS-SparkSQL-Service
```

---

## DBeaver Connection

| Field    | Value                                |
|----------|--------------------------------------|
| Driver   | Apache Hive (or Spark SQL)           |
| Host     | localhost                            |
| Port     | 10000                                |
| URL      | `jdbc:hive2://localhost:10000/default` |

---

## Loading Views (order matters)

Open each script in DBeaver and run it **after** the corresponding service is healthy:

1. `DS1_PostgreSQL_SparkSQL_Views_from_REST.sql`   — needs: postgrest-pg
2. `DS2_Oracle_SparkSQL_Views_from_REST.sql`       — needs: iis-springboot
3. `DS3_TimescaleDB_SparkSQL_Views_from_REST.sql`  — needs: postgrest-ts
4. `DS4_MongoDB_SparkSQL_Views_from_REST.sql`      — needs: restheart
5. `DS5_Neo4J_SparkSQL_Views_from_REST.sql`        — needs: iis-neo4j-service
6. `IIS_SparkSQL_OLAP_Analytical.sql`              — needs: views from DS1+DS2+DS3

---

## REST Endpoints

### Health
```
GET http://localhost:9990/IIS-SparkSQL-Service/rest/ping
```

### View data (paginated — always use ?limit= for large tables)
```
GET http://localhost:9990/IIS-SparkSQL-Service/rest/view/pg_orders_view?limit=100
GET http://localhost:9990/IIS-SparkSQL-Service/rest/view/ts_events_view?limit=500&offset=1000
```

### OLAP analytical views (small result sets — safe without limit)
```
GET http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_REVENUE_BY_TIER
GET http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_REVENUE_BY_CALENDAR
GET http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_EVENTS_BY_TYPE
GET http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_USER_ACTIVITY
```

### Schema inspection
```
GET http://localhost:9990/IIS-SparkSQL-Service/rest/STRUCT/pg_orders_view
GET http://localhost:9990/IIS-SparkSQL-Service/rest/STRUCT/OLAP_FACTS_ORDER_REVENUE
```

### Execute arbitrary SQL (requires Basic Auth: spark / sql)
```bash
curl -u spark:sql -X POST \
     -H "Content-Type: text/plain" \
     --data "SELECT eventType, COUNT(*) AS cnt FROM ts_events_view GROUP BY eventType" \
     http://localhost:9990/IIS-SparkSQL-Service/_sqlrest/query
```

### Create a view from a REST source (requires Basic Auth)
```bash
curl -u spark:sql -X POST \
     -H "Content-Type: text/plain" \
     --data "http://postgrest-pg:3000/orders?limit=50000" \
     "http://localhost:9990/IIS-SparkSQL-Service/_sqlrest/create-json-view-from-rest?view_name=PG_ORDERS_JSON_VIEW"
```

---

## Large Dataset Safety

| Setting              | Value     | Reason                                           |
|----------------------|-----------|--------------------------------------------------|
| REST default limit   | 1 000 rows | Prevents OOM on unbounded SELECTs via REST      |
| REST hard cap        | 5 000 rows | PostgREST max-rows also set to 100 000          |
| PostgREST max rows   | 100 000   | Guards against full table scans via REST         |
| RestHeart page size  | 50 000    | MongoDB collection cap per page                  |
| Spark shuffle parts  | 8         | local[*] has no cluster; 200 is wasteful         |
| AQE enabled          | true      | Auto-coalesces partitions, handles join skew     |
| Off-heap memory      | 1 GB      | Helps with large JSON dataset processing         |

For the 1M-row PostgreSQL tables, always use `?limit=50000` in the view  
definition and paginate REST API calls with `?limit=N&offset=M`.

---

## java_method() Pattern Explained

```sql
-- 1. Creates the view by calling back to the Spring Boot service via HTTP:
SELECT java_method(
    'org.spark.service.rest.RESTEnabledSQLService',
    'createJSONViewFromREST',
    'PG_ORDERS_JSON_VIEW',
    'http://postgrest-pg:3000/orders?limit=50000');

-- 2. The view embeds its own java_method() — data re-fetched on every query:
-- CREATE OR REPLACE VIEW PG_ORDERS_JSON_VIEW AS
--     SELECT from_json(raw.data, 'ARRAY<STRUCT<...>>') AS array
--     FROM (SELECT java_method('...QueryRESTDataService', 'getRESTDataDocument',
--                              'http://postgrest-pg:3000/orders?limit=50000') AS data) raw

-- 3. Unwrap the array into rows:
CREATE OR REPLACE VIEW pg_orders_view AS
SELECT v.*
FROM PG_ORDERS_JSON_VIEW AS j
LATERAL VIEW explode(j.array) AS v;

-- 4. Query normally:
SELECT * FROM pg_orders_view LIMIT 100;
```
