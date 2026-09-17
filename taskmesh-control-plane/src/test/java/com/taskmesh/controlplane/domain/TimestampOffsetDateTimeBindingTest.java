package com.taskmesh.controlplane.domain;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.taskmesh.controlplane.TestcontainersConfiguration;

/**
 * The Day 14 change: {@code Instant} bound as an {@code OffsetDateTime},
 * avoiding the UTC {@code Calendar} and the {@code static synchronized}
 * {@code TimeZone.getTimeZone} call it makes on every bind and extract.
 * <p>
 * Takes the mapping from {@code application.yml} rather than overriding it,
 * so this runs against the configuration the application actually ships.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class TimestampOffsetDateTimeBindingTest extends TimestampSemanticsContract {

    @Override
    String expectedJdbcTypeSimpleName() {
        return "TimestampWithTimeZoneJdbcType";
    }
}
