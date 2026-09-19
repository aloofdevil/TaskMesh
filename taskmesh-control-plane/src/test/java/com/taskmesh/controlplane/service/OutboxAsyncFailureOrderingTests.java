package com.taskmesh.controlplane.service;

import java.util.List;

import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.controlplane.TestcontainersConfiguration;

/**
 * Partial-failure ordering under {@code async-sends=true}.
 * <p>
 * The batched path submits all five records before reading any acknowledgement,
 * so 4 and 5 are acknowledged even though 3 was rejected. Event 3 then publishes
 * on a later pass, which means a consumer observes this key as
 * {@code 1, 2, 4, 5, 3}.
 * <p>
 * Nothing is lost and nothing is duplicated - but per-key sequence order is not
 * preserved across a partial failure, which the sequential path does preserve.
 * This is the concrete cost of the prefix property Day 20 gave up, and it is
 * asserted here rather than left as prose.
 */
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        // Its own topic: this class shares a broker with OutboxTests, whose
        // consumer counts every record on the topic before filtering by key, so
        // foreign records would make it miss its own.
        "taskmesh.outbox.job-events-topic=taskmesh.job-events.async-failorder",
        "taskmesh.outbox.async-sends=true",
        "spring.kafka.producer.properties.max.request.size=2048"
})
class OutboxAsyncFailureOrderingTests extends OutboxFailureOrderingContract {

    @Override
    boolean expectedAsyncSends() {
        return true;
    }

    /** 1, 2, 4 and 5: every event except the rejected one. */
    @Override
    int expectedPublishedOnFirstPass() {
        return 4;
    }

    @Override
    List<Integer> expectedObservedOrder() {
        return List.of(1, 2, 4, 5, 3);
    }
}
