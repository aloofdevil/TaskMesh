package com.taskmesh.controlplane;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.taskmesh.controlplane.service.OutboxProperties;
import com.taskmesh.controlplane.service.ReliabilityProperties;
import com.taskmesh.controlplane.service.RetryProperties;

/** Scheduling drives the lease reaper and the outbox publisher. */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({ReliabilityProperties.class, RetryProperties.class, OutboxProperties.class})
public class TaskmeshControlPlaneApplication {

	public static void main(String[] args) {
		SpringApplication.run(TaskmeshControlPlaneApplication.class, args);
	}

}
