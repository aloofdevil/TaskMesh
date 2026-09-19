package com.taskmesh.controlplane.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.controlplane.TestcontainersConfiguration;

/**
 * Day 21 ordering and idempotence contract under the shipped default,
 * {@code async-sends=false}. This is the control: it establishes what the
 * sequential path delivers, so the batched result next door is a comparison
 * rather than an isolated claim.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        "taskmesh.outbox.async-sends=false"
})
class OutboxSequentialOrderingTests extends OutboxOrderingContract {

    @Override
    boolean expectedAsyncSends() {
        return false;
    }
}
