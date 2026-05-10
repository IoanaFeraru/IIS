package org.spark.service.rest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.spark.service.SparkSQLService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.logging.Logger;

/**
 * Auto REST View Service — exposes any Spark SQL view as a JSON REST endpoint.
 *
 * Endpoints (base: /IIS-SparkSQL-Service/rest):
 *
 *   GET /rest/ping
 *     → health check
 *
 *   GET /rest/view/{VIEW_NAME}
 *     → SELECT * FROM view (with pagination — use ?limit=&offset= for large tables)
 *
 *   GET /rest/view/{VIEW_NAME}?limit=100&offset=0
 *     → paginated result — STRONGLY recommended for large sources (PG 1M rows!)
 *
 *   GET /rest/STRUCT/{VIEW_NAME}
 *     → returns the DDL schema (column names + types) of the view
 *
 * Large-dataset safety:
 *   Default page size is 1000 rows. Callers MUST use ?limit for views backed
 *   by large tables (pg_orders_view, orcl_users_view, etc.).
 *   The Spark action collectAsList() loads the full result into driver memory,
 *   so unbounded SELECTs on million-row tables WILL cause OOM.
 *
 * Example URLs for your IIS project:
 *   http://localhost:9990/IIS-SparkSQL-Service/rest/view/pg_orders_view?limit=100
 *   http://localhost:9990/IIS-SparkSQL-Service/rest/view/ts_events_view?limit=500
 *   http://localhost:9990/IIS-SparkSQL-Service/rest/view/orcl_users_view?limit=50
 *   http://localhost:9990/IIS-SparkSQL-Service/rest/STRUCT/pg_orders_view
 *   http://localhost:9990/IIS-SparkSQL-Service/rest/ping
 *
 * OLAP analytical views (safe — pre-aggregated, small result sets):
 *   http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_REVENUE_BY_TIER
 *   http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_EVENTS_BY_TYPE
 */
@RestController
@RequestMapping("/rest")
public class AutoRESTViewService {

    private static final Logger log = Logger.getLogger(AutoRESTViewService.class.getName());

    /** Hard cap — no single REST response will return more than this many rows */
    private static final int MAX_LIMIT = 5000;
    /** Default page size if caller does not supply ?limit= */
    private static final int DEFAULT_LIMIT = 1000;

    private final SparkSQLService sparkSQLService;

    public AutoRESTViewService(SparkSQLService sparkSQLService) {
        this.sparkSQLService = sparkSQLService;
    }

    // ── Health check ──────────────────────────────────────────────────────────

    @GetMapping(value = "/ping", produces = MediaType.TEXT_PLAIN_VALUE)
    public String ping() {
        log.info(">>> ping");
        return "IIS-SparkSQL-Service is UP!";
    }

    // ── View data ─────────────────────────────────────────────────────────────

    /**
     * Return paginated JSON rows from a Spark view.
     *
     * @param viewName  Spark view name (case-insensitive in Spark SQL)
     * @param limit     max rows to return (default 1000, hard cap 5000)
     * @param offset    row offset for pagination (default 0)
     */
    @GetMapping(value = "/view/{viewName}",
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public String getViewData(
            @PathVariable("viewName") String viewName,
            @RequestParam(value = "limit", defaultValue = "" + DEFAULT_LIMIT) int limit,
            @RequestParam(value = "offset", defaultValue = "0") int offset) {

        int safeLimit = Math.min(limit, MAX_LIMIT);
        log.info("GET /view/" + viewName + " limit=" + safeLimit + " offset=" + offset);

        // Spark SQL LIMIT + OFFSET (supported in Spark 3.x)
        String sql = String.format(
                "SELECT * FROM %s LIMIT %d OFFSET %d", viewName, safeLimit, offset);

        Dataset<Row> ds = sparkSQLService.getSpark().sql(sql);
        ds.printSchema();
        return ds.toJSON().collectAsList().toString();
    }

    // ── View schema ───────────────────────────────────────────────────────────

    /**
     * Return the DDL schema of a Spark view (column names and types).
     * Returns 0 rows — safe for any view size.
     */
    @GetMapping(value = "/STRUCT/{viewName}",
            produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public String getViewSchema(@PathVariable("viewName") String viewName) {
        log.info("GET /STRUCT/" + viewName);

        Dataset<Row> ds = sparkSQLService.getSpark().sql(
                "SELECT * FROM " + viewName + " WHERE 1=0");
        ds.printSchema();
        return ds.schema().sql();
    }
}
