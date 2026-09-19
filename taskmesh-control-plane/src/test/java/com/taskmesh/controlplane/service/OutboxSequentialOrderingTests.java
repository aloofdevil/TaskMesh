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
        // Its own topic: this class shares a broker with OutboxTests, whose
        // consumer counts every record on the topic before filtering by key, so
        // foreign records would make it miss its own.
        "taskmesh.outbox.job-events-topic=taskmesh.job-events.seq-ordering",
        "taskmesh.outbox.async-sends=false"
})
class OutboxSequentialOrderingTests extends OutboxOrderingContract {

    @Override
    boolean expectedAsyncSends() {
        return false;
    }
}
