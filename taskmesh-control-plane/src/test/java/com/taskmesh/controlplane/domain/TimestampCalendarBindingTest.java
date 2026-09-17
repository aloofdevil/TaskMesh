package com.taskmesh.controlplane.domain;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.controlplane.TestcontainersConfiguration;

/**
 * The configuration the project shipped before Day 14: {@code Instant} bound
 * through {@code java.sql.Timestamp} plus a UTC {@code Calendar}.
 * <p>
 * Runs the whole {@link TimestampSemanticsContract} against that binding, so
 * the Day 14 change can be shown to preserve timestamp meaning rather than
 * merely to be faster.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties =
        "spring.jpa.properties.hibernate.type.preferred_instant_jdbc_type=TIMESTAMP_UTC")
class TimestampCalendarBindingTest extends TimestampSemanticsContract {

    @Override
    String expectedJdbcTypeSimpleName() {
        return "TimestampUtcAsJdbcTimestampJdbcType";
    }
}
