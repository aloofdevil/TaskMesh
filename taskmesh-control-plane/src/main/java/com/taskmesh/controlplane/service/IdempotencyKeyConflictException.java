package com.taskmesh.controlplane.service;

/**
 * Thrown when an idempotency key is reused with a payload whose hash does
 * not match the payload stored against the first use of that key. Mapped
 * to 409 by ApiExceptionHandler.
 */
public class IdempotencyKeyConflictException extends RuntimeException {

    public IdempotencyKeyConflictException(String idempotencyKey) {
        super("Idempotency key '" + idempotencyKey + "' was already used with a different payload");
    }
}
