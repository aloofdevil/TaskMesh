package com.taskmesh.controlplane;

import org.springframework.boot.SpringApplication;

public class TestTaskmeshControlPlaneApplication {

	public static void main(String[] args) {
		SpringApplication.from(TaskmeshControlPlaneApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
