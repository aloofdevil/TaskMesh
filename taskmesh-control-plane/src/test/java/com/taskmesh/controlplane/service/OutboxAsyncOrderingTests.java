package com.taskmesh.controlplane.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.controlplane.TestcontainersConfiguration;

/**
 * Day 21 ordering and idempotence contract under {@code async-sends=true}, the
 * Day 20 path that submits a whole batch before awaiting any acknowledgement.
 * <p>
 * This is the arm the day exists for. Every record for a key is in flight
 * simultaneously here, which is precisely the condition under which a producer
 * without idempotence could reorder a partition on retry.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        "taskmesh.outbox.async-sends=true"
})
class OutboxAsyncOrderingTests extends OutboxOrderingContract {

    @Override
    boolean expectedAsyncSends() {
        return true;
    }
}
