package org.spark.service.exception;

/** Thrown when any step of the REST→View workflow fails. */
public class RESTSQLWorkflowException extends RuntimeException {
    public RESTSQLWorkflowException(String message) {
        super(message);
    }
}
