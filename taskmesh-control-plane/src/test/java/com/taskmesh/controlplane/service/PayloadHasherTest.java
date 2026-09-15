package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

class PayloadHasherTest {

    private final PayloadHasher hasher = new PayloadHasher(new ObjectMapper());

    @Test
    void sameKeysInDifferentOrderProduceTheSameHash() {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("message", "hello");
        a.put("count", 3);

        Map<String, Object> b = new LinkedHashMap<>();
        b.put("count", 3);
        b.put("message", "hello");

        assertThat(hasher.hash(a)).isEqualTo(hasher.hash(b));
    }

    @Test
    void differentPayloadsProduceDifferentHashes() {
        Map<String, Object> a = Map.of("message", "hello");
        Map<String, Object> b = Map.of("message", "goodbye");

        assertThat(hasher.hash(a)).isNotEqualTo(hasher.hash(b));
    }

    @Test
    void nestedMapsAreAlsoOrderIndependent() {
        Map<String, Object> a = new LinkedHashMap<>();
        Map<String, Object> nestedA = new LinkedHashMap<>();
        nestedA.put("z", 1);
        nestedA.put("a", 2);
        a.put("nested", nestedA);

        Map<String, Object> b = new LinkedHashMap<>();
        Map<String, Object> nestedB = new LinkedHashMap<>();
        nestedB.put("a", 2);
        nestedB.put("z", 1);
        b.put("nested", nestedB);

        assertThat(hasher.hash(a)).isEqualTo(hasher.hash(b));
    }

    @Test
    void hashIsHexEncodedSha256() {
        String hash = hasher.hash(Map.of("k", "v"));

        assertThat(hash).hasSize(64).matches("[0-9a-f]+");
    }
}
