package com.taskmesh.worker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * Context load test. The register/heartbeat/poll loop is disabled: there is
 * no control plane to talk to here, and the loop's job is verified against
 * a real control plane in the control-plane module's tests.
 */
@SpringBootTest
@TestPropertySource(properties = "taskmesh.worker.enabled=false")
class TaskmeshWorkerApplicationTests {

	@Test
	void contextLoads() {
	}

}
