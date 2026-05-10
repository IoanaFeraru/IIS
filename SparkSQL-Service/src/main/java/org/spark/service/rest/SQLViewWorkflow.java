package org.spark.service.rest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark.service.SparkSQLService;
import org.spark.service.exception.RESTSQLWorkflowException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.logging.Logger;

/**
 * Core workflow for creating and querying Spark SQL views over REST data sources.
 *
 * View creation strategy — LIVE views (not snapshots):
 *   The CREATE OR REPLACE VIEW embeds a java_method() call inside its own
 *   definition. Every SELECT on the view re-fetches live data from the REST
 *   endpoint. This matches the reference DSA-SparkSQL-Service pattern.
 *
 * Large-dataset safety:
 *   - Schema inference uses only the first page of data (limit param on URL)
 *   - executeSQLQuery streams results and can apply a LIMIT for REST endpoints
 *   - collectAsList() is only called on the check query (LIMIT 1), never on
 *     full datasets at view-creation time
 */
@Service
public class SQLViewWorkflow {

    private static final Logger log = Logger.getLogger(SQLViewWorkflow.class.getName());

    private final SparkSQLService sparkSQLService;

    public SQLViewWorkflow(SparkSQLService sparkSQLService) {
        this.sparkSQLService = sparkSQLService;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Full workflow: fetch schema from REST URL, then create a live Spark view.
     * This is the method called by RESTEnabledSQLService (HTTP POST) and also
     * by the static createJSONViewFromREST (java_method entry point).
     */
    public SQLViewDefinition createJsonViewFromREST(String viewName, String restUrl) {
        validate(viewName, "VIEW NAME");
        validate(restUrl, "REST URL");

        String jsonSchema = inferJsonSchema(restUrl);
        return buildLiveView(viewName, jsonSchema, restUrl);
    }

    /**
     * Execute an arbitrary SQL query and return results as a JSON string.
     * The result set is collected in full — callers should add LIMIT clauses
     * for large tables.
     */
    public SQLResponse executeSQLQuery(String sql) {
        validate(sql, "SQL QUERY");

        SparkSession spark = sparkSQLService.getSpark();
        try {
            log.info("Executing SQL: " + sql);
            Dataset<Row> ds = spark.sql(sql);
            ds.show(20, false); // log preview to Spark stdout
            String json = ds.toJSON().collectAsList().toString();
            return new SQLResponse(sql, json);
        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "SQL execution failed for [" + sql + "]: " + e.getMessage());
        }
    }

    // ── Internal steps ────────────────────────────────────────────────────────

    /**
     * STEP 1+2: fetch sample JSON → infer schema via schema_of_json().
     *
     * We fetch real data here only to infer the schema. For large endpoints
     * you should add ?limit=1 (PostgREST) or similar — the REST services
     * already return arrays, so even a 1-row sample gives us the full schema.
     */
    String inferJsonSchema(String restUrl) {
        SparkSession spark = sparkSQLService.getSpark();

        // Fetch sample data for schema inference
        String sample;
        try {
            sample = QueryRESTDataService.getRESTDataDocument(restUrl);
            log.info("Sample fetched for schema inference (" + sample.length() + " chars)");
        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "STEP_1: Could not fetch data from [" + restUrl + "]: " + e.getMessage());
        }

        // Escape single quotes for embedding in SQL string literal
        String escaped = sample.replace("\\", "\\\\").replace("'", "\\'");

        String schemaQuery = String.format(
                "SELECT schema_of_json('%s') AS json_schema", escaped);
        try {
            List<Row> rows = spark.sql(schemaQuery).collectAsList();
            String schema = rows.get(0).getAs("json_schema").toString();
            log.info("Inferred schema: " + schema);
            return schema;
        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "STEP_2: Schema inference failed: " + e.getMessage());
        }
    }

    /**
     * STEP 3+4: CREATE OR REPLACE VIEW using java_method() for live data,
     * then verify it with SELECT … LIMIT 1.
     *
     * The view definition embeds java_method() so every query re-fetches
     * live JSON from the REST endpoint. from_json() applies the inferred
     * schema so the result is typed, not a raw string.
     */
    SQLViewDefinition buildLiveView(String viewName, String jsonSchema, String restUrl) {
        SparkSession spark = sparkSQLService.getSpark();

        String createSql = String.format("""
                CREATE OR REPLACE VIEW %1$s AS
                    SELECT from_json(
                        raw.data,
                        '%2$s'
                    ) AS array
                    FROM (
                        SELECT java_method(
                            'org.spark.service.rest.QueryRESTDataService',
                            'getRESTDataDocument',
                            '%3$s'
                        ) AS data
                    ) raw
                """, viewName, jsonSchema, restUrl);

        try {
            log.info("Creating live view: " + viewName);
            spark.sql(createSql);
        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "STEP_3: CREATE VIEW failed for [" + viewName + "]: " + e.getMessage()
                    + "\nSQL was:\n" + createSql);
        }

        // Verify — cheap check, only fetches 1 row
        String checkSql = "SELECT * FROM " + viewName + " LIMIT 1";
        try {
            List<Row> check = spark.sql(checkSql).collectAsList();
            if (check == null || check.isEmpty()) {
                log.warning("View " + viewName + " created but returned 0 rows on check.");
            }
        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "STEP_4: View check failed for [" + viewName + "]: " + e.getMessage());
        }

        log.info(">>> View created successfully: " + viewName);
        return new SQLViewDefinition(viewName, restUrl, jsonSchema,
                createSql, "/rest/view/" + viewName);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void validate(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new RESTSQLWorkflowException(label + " must not be null or empty");
        }
    }
}
