package com.taskmesh.controlplane.service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;

/**
 * Computes a deterministic SHA-256 hash of a job payload, used to detect
 * whether a reused idempotency key is being submitted with the same
 * logical request or a different one (see docs/architecture.md,
 * "Idempotency & the delivery guarantee").
 * <p>
 * The payload is a {@code Map<String, Object>} deserialized from JSON, so
 * key order reflects whatever order the client's JSON happened to use -
 * two textually different but logically identical payloads (same keys,
 * different order) must still hash the same. {@link SerializationFeature#ORDER_MAP_ENTRIES_BY_KEYS}
 * sorts map entries by key recursively before serialization, which is what
 * makes the hash order-independent without hand-rolling canonical JSON.
 * <p>
 * Jackson 3's {@code ObjectMapper} is immutable once built, so rather than
 * mutating a copy (the Jackson 2 idiom), a per-call {@code ObjectWriter}
 * with the feature enabled is derived from the shared, Spring-managed
 * mapper via {@link ObjectMapper#writer(SerializationFeature)}.
 */
@Component
public class PayloadHasher {

    private final ObjectMapper objectMapper;

    public PayloadHasher(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String hash(Map<String, Object> payload) {
        try {
            byte[] canonicalJson = objectMapper.writer(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                    .writeValueAsBytes(payload);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonicalJson);
            return HexFormat.of().formatHex(digest);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("Payload could not be serialized for hashing", e);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed available on every JDK implementation.
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
