package com.taskmesh.controlplane.service;

import java.util.List;

import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.controlplane.TestcontainersConfiguration;

/**
 * Partial-failure ordering under the shipped default, {@code async-sends=false}.
 * <p>
 * The sequential path stops at the first failure, so events 4 and 5 are never
 * attempted while 3 is blocked. When 3 becomes sendable, the same pass publishes
 * 3, 4 and 5 in id order - so the consumer never sees the key out of sequence.
 */
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        "taskmesh.outbox.async-sends=false",
        "spring.kafka.producer.properties.max.request.size=2048"
})
class OutboxSequentialFailureOrderingTests extends OutboxFailureOrderingContract {

    @Override
    boolean expectedAsyncSends() {
        return false;
    }

    /** Only 1 and 2, the prefix ahead of the blockage. */
    @Override
    int expectedPublishedOnFirstPass() {
        return 2;
    }

    @Override
    List<Integer> expectedObservedOrder() {
        return List.of(1, 2, 3, 4, 5);
    }
}
