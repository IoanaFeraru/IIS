package org.spark.service.exception;

public record ErrorResponse(
        int statusCode,
        String message
) {}
