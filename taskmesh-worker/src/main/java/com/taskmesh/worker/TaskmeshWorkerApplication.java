package com.taskmesh.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the TaskMesh worker process.
 * <p>
 * Skeleton only as of Day 1: the application starts and exposes actuator
 * health, but does not yet register with the control plane, claim jobs, or
 * execute anything. See docs/architecture.md for the intended lifecycle
 * (register -&gt; heartbeat -&gt; claim -&gt; execute -&gt; report), implemented from
 * Day 3 onward.
 */
@SpringBootApplication
public class TaskmeshWorkerApplication {

	public static void main(String[] args) {
		SpringApplication.run(TaskmeshWorkerApplication.class, args);
	}

}
