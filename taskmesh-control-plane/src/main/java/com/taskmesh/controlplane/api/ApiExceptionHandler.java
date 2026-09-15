package com.taskmesh.controlplane.api;

import java.time.Instant;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import com.taskmesh.controlplane.api.dto.ErrorResponse;
import com.taskmesh.controlplane.service.IdempotencyKeyConflictException;
import com.taskmesh.controlplane.service.JobNotCancellableException;
import com.taskmesh.controlplane.service.JobNotFoundException;
import com.taskmesh.controlplane.service.StaleExecutionException;
import com.taskmesh.controlplane.service.WorkerNotActiveException;
import com.taskmesh.controlplane.service.WorkerNotFoundException;

import jakarta.servlet.http.HttpServletRequest;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(JobNotFoundException ex, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, ex.getMessage(), request);
    }

    @ExceptionHandler(IdempotencyKeyConflictException.class)
    public ResponseEntity<ErrorResponse> handleConflict(IdempotencyKeyConflictException ex, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, ex.getMessage(), request);
    }

    @ExceptionHandler(JobNotCancellableException.class)
    public ResponseEntity<ErrorResponse> handleNotCancellable(JobNotCancellableException ex, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, ex.getMessage(), request);
    }

    /**
     * Fencing rejection. Carries the {@code STALE_EXECUTION} code in the
     * {@code error} field so a worker can recognise "you have been fenced"
     * specifically, rather than having to guess from a generic 409, and
     * stop working on that execution.
     */
    @ExceptionHandler(StaleExecutionException.class)
    public ResponseEntity<ErrorResponse> handleStaleExecution(StaleExecutionException ex, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, StaleExecutionException.ERROR_CODE, ex.getMessage(), request);
    }

    @ExceptionHandler(WorkerNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleWorkerNotFound(WorkerNotFoundException ex, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, ex.getMessage(), request);
    }

    @ExceptionHandler(WorkerNotActiveException.class)
    public ResponseEntity<ErrorResponse> handleWorkerNotActive(WorkerNotActiveException ex, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, ex.getMessage(), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return build(HttpStatus.BAD_REQUEST, message.isBlank() ? "Validation failed" : message, request);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleMalformedBody(HttpMessageNotReadableException ex, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "Malformed request body", request);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException ex, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "Invalid value for parameter '" + ex.getName() + "'", request);
    }

    private ResponseEntity<ErrorResponse> build(HttpStatus status, String message, HttpServletRequest request) {
        return build(status, status.getReasonPhrase(), message, request);
    }

    /** Variant for errors that carry a specific machine-readable code rather than the HTTP reason phrase. */
    private ResponseEntity<ErrorResponse> build(HttpStatus status, String errorCode, String message,
            HttpServletRequest request) {
        ErrorResponse body = new ErrorResponse(
                Instant.now(), status.value(), errorCode, message, request.getRequestURI());
        return ResponseEntity.status(status).body(body);
    }
}
