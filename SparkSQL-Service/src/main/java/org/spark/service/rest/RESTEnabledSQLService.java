package org.spark.service.rest;

import org.spark.service.exception.RESTSQLWorkflowException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.logging.Logger;

/**
 * REST controller — exposes SQL execution and view-creation over HTTP.
 * Also provides static methods callable from SparkSQL via java_method().
 *
 * HTTP endpoints (base path: /IIS-SparkSQL-Service/_sqlrest):
 *
 *   POST /_sqlrest/query
 *     Body: SQL string
 *     → executes SQL, returns JSON array of rows
 *
 *   POST /_sqlrest/create-json-view-from-rest?view_name=MY_VIEW
 *     Body: REST endpoint URL (plain text)
 *     → creates a live Spark view over that REST source
 *
 * java_method() entry point (static):
 *   SELECT java_method(
 *       'org.spark.service.rest.RESTEnabledSQLService',
 *       'createJSONViewFromREST',
 *       'MY_VIEW',
 *       'http://service-host/path');
 *
 * CRITICAL: the static createJSONViewFromREST must NOT declare "throws Exception".
 * It self-calls back via HTTP to this service's own _sqlrest endpoint so the
 * workflow runs in the Spring context where SparkSQLService (and thus the
 * SparkSession) is fully initialised.
 */
@RestController
@RequestMapping("/_sqlrest")
public class RESTEnabledSQLService {

    private static final Logger log = Logger.getLogger(RESTEnabledSQLService.class.getName());

    /**
     * Callback URL — used by the static java_method() entry point to call back
     * into this service over HTTP. Must match server.port + context-path in
     * application.properties. Inside Docker the service reaches itself via
     * the loopback or the container's own hostname.
     *
     * Override with -Dspark.rest.callback.url=http://iis-spark-sql:9990/IIS-SparkSQL-Service
     */
    private static final String CALLBACK_BASE = System.getProperty(
            "spark.rest.callback.url",
            "http://localhost:9990/IIS-SparkSQL-Service");

    private final SQLViewWorkflow sqlViewWorkflow;

    public RESTEnabledSQLService(SQLViewWorkflow sqlViewWorkflow) {
        this.sqlViewWorkflow = sqlViewWorkflow;
    }

    // ── HTTP endpoints ────────────────────────────────────────────────────────

    /**
     * Execute an arbitrary SQL query.
     *
     * curl -u spark:sql -X POST \
     *      -H "Content-Type: text/plain" \
     *      --data "SELECT * FROM pg_orders_view LIMIT 10" \
     *      http://localhost:9990/IIS-SparkSQL-Service/_sqlrest/query
     */
    @PostMapping(value = "/query",
            consumes = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public SQLResponse executeQuery(@RequestBody String sql) {
        if (sql == null || sql.isBlank()) {
            throw new RuntimeException("query body must not be empty");
        }
        log.info("POST /query: " + sql.substring(0, Math.min(200, sql.length())));
        return sqlViewWorkflow.executeSQLQuery(sql);
    }

    /**
     * Create a live Spark view from a REST data source.
     *
     * curl -u spark:sql -X POST \
     *      -H "Content-Type: text/plain" \
     *      --data "http://iis-postgrest-pg:3000/orders" \
     *      "http://localhost:9990/IIS-SparkSQL-Service/_sqlrest/create-json-view-from-rest?view_name=PG_ORDERS_JSON_VIEW"
     */
    @PostMapping(value = "/create-json-view-from-rest",
            consumes = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public SQLViewDefinition createJsonViewFromRESTHttp(
            @RequestParam("view_name") String viewName,
            @RequestBody String restUrl) {

        if (viewName == null || viewName.isBlank()) {
            throw new RuntimeException("view_name parameter must not be empty");
        }
        if (restUrl == null || restUrl.isBlank()) {
            throw new RuntimeException("request body (REST URL) must not be empty");
        }
        log.info("POST /create-json-view-from-rest view=" + viewName + " url=" + restUrl.trim());
        return sqlViewWorkflow.createJsonViewFromREST(viewName, restUrl.trim());
    }

    // ── Static java_method() entry point ─────────────────────────────────────

    /**
     * Called from SparkSQL via:
     *   SELECT java_method(
     *       'org.spark.service.rest.RESTEnabledSQLService',
     *       'createJSONViewFromREST',
     *       'VIEW_NAME',
     *       'http://...');
     *
     * This method self-calls back to the running Spring Boot service via HTTP
     * so the full Spring context (and thus SparkSQLService) is available.
     *
     * NO "throws Exception" — mandatory for java_method() compatibility.
     */
    public static String createJSONViewFromREST(String viewName, String restDataServiceHttpURL) {
        try {
            RestTemplate restTemplate = new RestTemplate();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.TEXT_PLAIN);
            headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            // Basic auth — credentials must match application.properties
            headers.setBasicAuth("spark", "sql");

            String endpoint = CALLBACK_BASE
                    + "/_sqlrest/create-json-view-from-rest?view_name=" + viewName;

            log.info("java_method → callback POST " + endpoint);

            ResponseEntity<String> response = restTemplate.exchange(
                    endpoint,
                    HttpMethod.POST,
                    new HttpEntity<>(restDataServiceHttpURL, headers),
                    String.class);

            log.info("java_method ← " + response.getStatusCode() + " " + response.getBody());
            return response.getBody();

        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "createJSONViewFromREST callback failed for view ["
                    + viewName + "]: " + e.getMessage());
        }
    }
}
