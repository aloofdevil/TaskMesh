package com.taskmesh.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the TaskMesh worker process.
 * <p>
 * As of Day 3 the worker registers with the control plane, heartbeats, and
 * polls it for work (see {@link WorkerRuntime}). Executing the claimed
 * payload and reporting the result back is Day 4.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(WorkerProperties.class)
public class TaskmeshWorkerApplication {

	public static void main(String[] args) {
		SpringApplication.run(TaskmeshWorkerApplication.class, args);
	}

}
