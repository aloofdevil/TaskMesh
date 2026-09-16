package com.taskmesh.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class LoadGenConfigTest {

    @Test
    void defaultsMatchProductionWorkerTiming() {
        LoadGenConfig config = LoadGenConfig.defaults();

        // These are the values in taskmesh-worker/src/main/resources/application.yml.
        // If production timing changes, this test should fail and force the
        // simulator's defaults to be reconsidered rather than silently drift.
        assertThat(config.heartbeatInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.pollInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.leaseRenewInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.jobDuration()).isEqualTo(Duration.ofSeconds(2));
        assertThat(config.capacity()).isEqualTo(2);
    }

    @Test
    void parsesEveryDocumentedOption() {
        LoadGenConfig config = LoadGenConfig.parse(new String[] {
                "--workers=5000",
                "--capacity=4",
                "--heartbeat-interval=7s",
                "--poll-interval=250ms",
                "--lease-renew-interval=15s",
                "--job-duration=500ms",
                "--ramp-up=250",
                "--control-plane-url=http://cp:9999",
                "--seed=1234",
                "--run-duration=2m",
                "--request-timeout=3s",
                "--worker-id-prefix=bench"
        });

        assertThat(config.workers()).isEqualTo(5000);
        assertThat(config.capacity()).isEqualTo(4);
        assertThat(config.heartbeatInterval()).isEqualTo(Duration.ofSeconds(7));
        assertThat(config.pollInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(config.leaseRenewInterval()).isEqualTo(Duration.ofSeconds(15));
        assertThat(config.jobDuration()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.rampUpPerSecond()).isEqualTo(250);
        assertThat(config.controlPlaneUrl()).isEqualTo("http://cp:9999");
        assertThat(config.seed()).isEqualTo(1234);
        assertThat(config.runDuration()).isEqualTo(Duration.ofMinutes(2));
        assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(config.workerIdPrefix()).isEqualTo("bench");
    }

    @Test
    void jobSlotCeilingIsCapacityTimesWorkersAndLabelledAsACeiling() {
        LoadGenConfig config = LoadGenConfig.parse(new String[] {"--workers=25000", "--capacity=2"});

        assertThat(config.jobSlotCeiling()).isEqualTo(50_000);
        assertThat(config.describeEffective())
                .contains("job slot ceiling: 50000")
                .contains("NOT actual concurrent jobs");
    }

    @Test
    void durationsAcceptSuffixesAndBareMillis() {
        assertThat(LoadGenConfig.parseDuration("1500")).isEqualTo(Duration.ofMillis(1500));
        assertThat(LoadGenConfig.parseDuration("1500ms")).isEqualTo(Duration.ofMillis(1500));
        assertThat(LoadGenConfig.parseDuration("5s")).isEqualTo(Duration.ofSeconds(5));
        assertThat(LoadGenConfig.parseDuration("2m")).isEqualTo(Duration.ofMinutes(2));
        assertThat(LoadGenConfig.parseDuration("0.5s")).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void rejectsUnknownOptionsRatherThanIgnoringThem() {
        assertThatThrownBy(() -> LoadGenConfig.parse(new String[] {"--wrkers=100"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown option");
    }

    @Test
    void rejectsBarePositionalArguments() {
        assertThatThrownBy(() -> LoadGenConfig.parse(new String[] {"100"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unrecognised argument");
    }

    @Test
    void rejectsOptionWithoutValue() {
        assertThatThrownBy(() -> LoadGenConfig.parse(new String[] {"--workers"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a value");
    }

    @Test
    void rejectsCapacityOutsideControlPlaneValidationRange() {
        // The control plane validates capacity 1..64 on WorkerRegistrationRequest;
        // failing here beats discovering it as 25,000 HTTP 400s.
        assertThatThrownBy(() -> LoadGenConfig.parse(new String[] {"--capacity=0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 64");
        assertThatThrownBy(() -> LoadGenConfig.parse(new String[] {"--capacity=65"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 64");
    }

    @Test
    void rejectsNonPositiveWorkerCount() {
        assertThatThrownBy(() -> LoadGenConfig.parse(new String[] {"--workers=0"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void effectiveConfigurationIsRecordedForReproducibility() {
        LoadGenConfig config = LoadGenConfig.parse(new String[] {"--workers=500", "--seed=99"});
        String described = config.describeEffective();

        assertThat(described)
                .contains("logical workers      : 500")
                .contains("seed                 : 99")
                .contains("heartbeat interval   : 5000ms");
    }
}
