package org.spark.service.rest;

/**
 * Wraps the result of an ad-hoc SQL query executed via /_sqlrest/query.
 * query    — the original SQL string
 * response — JSON array of result rows
 */
public record SQLResponse(
        String query,
        String response
) {}
