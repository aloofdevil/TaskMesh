package com.taskmesh.controlplane.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.BasicValuedModelPart;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.controlplane.repository.JobRepository;

import jakarta.persistence.EntityManagerFactory;

/**
 * Proves that {@code Instant} timestamps round-trip through PostgreSQL
 * {@code TIMESTAMPTZ} identically, and that the value PostgreSQL actually
 * stores is the expected UTC instant.
 * <p>
 * This exists because Day 14 changes which Hibernate {@code JdbcType} binds
 * {@code Instant}. The shipped configuration uses
 * {@code TimestampUtcAsJdbcTimestampJdbcType}, which binds via
 * {@code java.sql.Timestamp} plus a UTC {@code Calendar} - and reaches the
 * {@code static synchronized java.util.TimeZone.getTimeZone(String)} on every
 * bind and extract. The candidate uses {@code TimestampWithTimeZoneJdbcType},
 * which binds an {@code OffsetDateTime} directly. Both must mean exactly the
 * same point in time.
 * <p>
 * The assertions are deliberately absolute rather than comparative: each
 * configuration is checked against an independently computed expected UTC
 * text rendered by PostgreSQL itself. A comparison of the two configurations
 * against each other could be satisfied by both being wrong in the same way.
 * <p>
 * This class is abstract and is deliberately <strong>not</strong> named
 * {@code *Test}. The two bindings are exercised by
 * {@link TimestampCalendarBindingTest} and
 * {@link TimestampOffsetDateTimeBindingTest}, which are top-level classes so
 * that Surefire discovers them: its default excludes drop inner classes
 * ({@code **}{@code /*$*}), so holding these as nested classes meant the
 * whole contract was silently never executed by {@code mvn verify}.
 */
abstract class TimestampSemanticsContract {

    /**
     * Instants chosen to expose timezone and precision bugs: the epoch, both
     * 2024 US DST transitions, microsecond precision at both ends of a second,
     * and a far-future value.
     */
    private static final List<Instant> FIXTURES = List.of(
            Instant.parse("1970-01-01T00:00:00Z"),
            Instant.parse("2024-03-10T07:30:00Z"),
            Instant.parse("2024-11-03T06:00:00Z"),
            Instant.parse("2026-01-01T00:00:00.123456Z"),
            Instant.parse("2026-06-30T23:59:59.999999Z"),
            Instant.parse("2031-12-31T12:00:00.000001Z"));

    private static final DateTimeFormatter UTC_SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    /** The text PostgreSQL should render for this instant, to microsecond precision. */
    private static String expectedUtcText(Instant instant) {
        return "%s.%06d".formatted(UTC_SECONDS.format(instant), instant.getNano() / 1_000);
    }

    @Autowired
    JobRepository jobRepository;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    EntityManagerFactory entityManagerFactory;

    /** The JdbcType this configuration is expected to select. */
    abstract String expectedJdbcTypeSimpleName();

    @Test
    void bindsInstantWithTheExpectedJdbcType() {
        SessionFactoryImplementor sf = entityManagerFactory.unwrap(SessionFactoryImplementor.class);
        BasicValuedModelPart attr = (BasicValuedModelPart) sf.getMappingMetamodel()
                .getEntityDescriptor(Job.class).findAttributeMapping("scheduledAt");

        assertThat(attr.getJdbcMapping().getJdbcType().getClass().getSimpleName())
                .as("a configuration regression here would silently invalidate this whole test")
                .isEqualTo(expectedJdbcTypeSimpleName());
    }

    @Test
    @Transactional
    void instantsRoundTripExactlyAndAreStoredAsTheCorrectUtcInstant() {
        for (Instant fixture : FIXTURES) {
            Job saved = jobRepository.saveAndFlush(Job.createQueued("ts-" + UUID.randomUUID(), "hash",
                    "TS_TEST", Map.of("k", "v"), (short) 5, 3, fixture));

            // 1. JPA round trip returns the identical Instant.
            assertThat(jobRepository.findById(saved.getId()).orElseThrow().getScheduledAt())
                    .as("scheduledAt must round-trip unchanged for %s", fixture)
                    .isEqualTo(fixture);

            // 2. What PostgreSQL actually stored, rendered by PostgreSQL in UTC.
            //    This bypasses Hibernate's extractor, so a bug symmetric across
            //    bind and extract cannot hide behind the round trip above.
            String storedUtc = jdbcTemplate.queryForObject("""
                    select to_char(scheduled_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US')
                      from jobs where id = ?""", String.class, saved.getId());
            assertThat(storedUtc)
                    .as("PostgreSQL must store %s as that exact UTC instant", fixture)
                    .isEqualTo(expectedUtcText(fixture));

            // 3. The numeric epoch value must match, independent of text rendering.
            Long epochMicros = jdbcTemplate.queryForObject("""
                    select (extract(epoch from scheduled_at) * 1000000)::bigint
                      from jobs where id = ?""", Long.class, saved.getId());
            assertThat(epochMicros)
                    .as("epoch microseconds must match for %s", fixture)
                    .isEqualTo(fixture.getEpochSecond() * 1_000_000L + fixture.getNano() / 1_000L);
        }
    }

    @Test
    @Transactional
    void leaseAndAuditTimestampsRoundTripExactly() {
        String workerId = "ts-worker-" + UUID.randomUUID();
        jdbcTemplate.update("""
                insert into workers (id, hostname, capacity, status) values (?, ?, ?, 'ACTIVE')""",
                workerId, "ts-host", 2);

        Instant scheduled = Instant.parse("2026-03-29T01:30:00.500000Z");
        Job job = jobRepository.saveAndFlush(Job.createQueued("lease-" + UUID.randomUUID(), "hash",
                "TS_TEST", Map.of(), (short) 5, 3, scheduled));

        Instant leaseUntil = Instant.parse("2026-03-29T02:00:00.250000Z");
        job.claimedBy(workerId, UUID.randomUUID(), leaseUntil);
        jobRepository.saveAndFlush(job);

        Job reloaded = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(reloaded.getLeaseUntil())
                .as("lease_until drives reaper decisions and must survive binding exactly")
                .isEqualTo(leaseUntil);
        assertThat(reloaded.getScheduledAt()).isEqualTo(scheduled);
        assertThat(reloaded.getCreatedAt()).isNotNull();
        assertThat(reloaded.getUpdatedAt()).isNotNull();

        String storedLease = jdbcTemplate.queryForObject("""
                select to_char(lease_until at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US')
                  from jobs where id = ?""", String.class, job.getId());
        assertThat(storedLease).isEqualTo(expectedUtcText(leaseUntil));
    }

    /**
     * The database clock stays the authority for scheduling, and comparisons
     * against it must behave identically under either binding.
     */
    @Test
    @Transactional
    void databaseComparisonsAgainstBoundTimestampsAreCorrect() {
        Instant dbNow = jobRepository.databaseTime();

        Job past = jobRepository.saveAndFlush(Job.createQueued("past-" + UUID.randomUUID(), "h",
                "TS_TEST", Map.of(), (short) 5, 3, dbNow.minusSeconds(60)));
        Job future = jobRepository.saveAndFlush(Job.createQueued("future-" + UUID.randomUUID(), "h",
                "TS_TEST", Map.of(), (short) 5, 3, dbNow.plusSeconds(3600)));

        assertThat(jdbcTemplate.queryForObject(
                "select scheduled_at <= now() from jobs where id = ?", Boolean.class, past.getId()))
                .as("a job scheduled in the past must be claimable").isTrue();
        assertThat(jdbcTemplate.queryForObject(
                "select scheduled_at <= now() from jobs where id = ?", Boolean.class, future.getId()))
                .as("a job scheduled in the future must not be claimable").isFalse();
    }
}
