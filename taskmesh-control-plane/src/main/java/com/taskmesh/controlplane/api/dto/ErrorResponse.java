package com.taskmesh.controlplane.api.dto;

import java.time.Instant;

/** Uniform error body returned by ApiExceptionHandler. */
public record ErrorResponse(Instant timestamp, int status, String error, String message, String path) {
}
