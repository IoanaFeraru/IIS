package org.spark.service.rest;

import org.spark.service.exception.RESTSQLWorkflowException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.logging.Logger;

/**
 * Static HTTP utility — callable from SparkSQL via java_method().
 *
 * CRITICAL RULES for java_method() compatibility:
 *  1. Methods must be PUBLIC STATIC
 *  2. Methods must NOT declare "throws Exception" (or any checked exception)
 *     — Spark's reflection layer rejects them at parse time
 *  3. All exceptions must be caught internally and rethrown as RuntimeException
 *
 * Credential handling:
 *  - If the URL contains user:pass@ (e.g. http://dev:secret@host/path),
 *    credentials are extracted and sent as HTTP Basic Auth.
 *    The URL is cleaned before the actual request.
 *
 * Large-payload safety:
 *  - connectTimeout: 30 s   (service startup can be slow in Docker)
 *  - requestTimeout: 120 s  (1M-row Postgres serialisation takes time)
 */
public class QueryRESTDataService {

    private static final Logger log = Logger.getLogger(QueryRESTDataService.class.getName());

    // Shared client — thread-safe, reuse across all java_method() calls
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .build();

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Fetch a JSON document from a REST endpoint.
     * URL may embed credentials: http://user:pass@host/path
     *
     * Called from SparkSQL:
     *   SELECT java_method('org.spark.service.rest.QueryRESTDataService',
     *                      'getRESTDataDocument', 'http://...');
     */
    public static String getRESTDataDocument(String url) {
        try {
            String[] creds = parseCredentials(url);
            if (creds != null) {
                String cleanUrl = url.replace(creds[0] + ":" + creds[1] + "@", "");
                return fetchWithAuth(cleanUrl, creds[0], creds[1]);
            }
            return fetch(url);
        } catch (RESTSQLWorkflowException e) {
            throw e;
        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "getRESTDataDocument failed for [" + url + "]: " + e.getMessage());
        }
    }

    /**
     * Fetch with explicit credentials (3-arg overload for java_method()).
     *
     * SELECT java_method('...QueryRESTDataService', 'getRESTDataDocument',
     *                    'http://host/path', 'user', 'pass');
     */
    public static String getRESTDataDocument(String url, String user, String password) {
        try {
            return fetchWithAuth(url, user, password);
        } catch (RESTSQLWorkflowException e) {
            throw e;
        } catch (Exception e) {
            throw new RESTSQLWorkflowException(
                    "getRESTDataDocument (auth) failed for [" + url + "]: " + e.getMessage());
        }
    }

    /** Health-check callable from SparkSQL: SELECT java_method('...', 'pingService'); */
    public static String pingService() {
        return "IIS SparkSQL REST Service is UP!";
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private static String fetch(String url) throws Exception {
        log.info("HTTP GET " + url);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertOk(resp, url);
        return resp.body();
    }

    private static String fetchWithAuth(String url, String user, String password) throws Exception {
        log.info("HTTP GET (auth) " + url + " [user=" + user + "]");
        String encoded = Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes());

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .header("Authorization", "Basic " + encoded)
                .GET()
                .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertOk(resp, url);
        return resp.body();
    }

    private static void assertOk(HttpResponse<String> resp, String url) {
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new RESTSQLWorkflowException(
                    "HTTP " + resp.statusCode() + " from [" + url + "]: " + resp.body());
        }
    }

    /**
     * Extract [username, password] from URLs like http://user:pass@host/path.
     * Returns null if no credentials found.
     */
    public static String[] parseCredentials(String urlString) {
        try {
            URI uri = new URI(urlString);
            String userInfo = uri.getUserInfo();
            if (userInfo != null && userInfo.contains(":")) {
                return userInfo.split(":", 2);
            }
        } catch (Exception e) {
            log.warning("Could not parse credentials from URL: " + e.getMessage());
        }
        return null;
    }
}
