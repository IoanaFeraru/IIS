package org.spark.service.rest;

/**
 * Descriptor returned after a view is created.
 * viewName         — Spark SQL view name
 * restUrl          — source REST endpoint URL
 * jsonViewSchema   — inferred Spark SQL schema string (ARRAY<STRUCT<...>>)
 * createViewQuery  — the actual CREATE OR REPLACE VIEW SQL that was executed
 * autoRESTViewPath — relative path to query this view via AutoRESTViewService
 */
public record SQLViewDefinition(
        String viewName,
        String restUrl,
        String jsonViewSchema,
        String createViewQuery,
        String autoRESTViewPath
) {}
