package com.taskmesh.controlplane;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.taskmesh.controlplane.service.ReliabilityProperties;

/** Scheduling drives the lease reaper (see {@code LeaseReaperScheduler}). */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ReliabilityProperties.class)
public class TaskmeshControlPlaneApplication {

	public static void main(String[] args) {
		SpringApplication.run(TaskmeshControlPlaneApplication.class, args);
	}

}
