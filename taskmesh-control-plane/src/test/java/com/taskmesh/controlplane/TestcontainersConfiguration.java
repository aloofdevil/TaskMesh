package com.taskmesh.controlplane;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The backing services tests run against: PostgreSQL, Redis and Kafka.
 * <p>
 * Importing this class gives all three. The nested pieces exist so a test
 * that needs to control one service itself - to stop it and observe an
 * outage - can import the others and supply that one on its own. Two tests
 * do exactly that: {@code OutboxKafkaOutageTests} brings its own Kafka, and
 * {@code RedisOutageAndSchedulerTests} brings its own Redis.
 */
@TestConfiguration(proxyBeanMethods = false)
@Import({TestcontainersConfiguration.Database.class,
		TestcontainersConfiguration.RedisCache.class,
		TestcontainersConfiguration.SharedKafkaBroker.class})
public class TestcontainersConfiguration {

	@TestConfiguration(proxyBeanMethods = false)
	public static class Database {

		@Bean
		@ServiceConnection
		PostgreSQLContainer postgresContainer() {
			return new PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"));
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	public static class RedisCache {

		@Bean
		@ServiceConnection(name = "redis")
		GenericContainer<?> redisContainer() {
			return new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	public static class SharedKafkaBroker {

		/**
		 * A real Kafka broker, matching the version Docker Compose runs.
		 * <p>
		 * Started once per JVM and shared by every test context, rather than
		 * created per context like Postgres and Redis. Each Spring context
		 * that differs in properties gets its own containers, and a Kafka
		 * broker is slow enough to start that paying for it several times
		 * would dominate the suite. Sharing is safe because tests assert on
		 * PostgreSQL state and identify their own messages by key, rather
		 * than assuming a topic starts empty.
		 */
		@Bean
		@ServiceConnection
		KafkaContainer kafkaContainer() {
			return SharedKafka.INSTANCE;
		}
	}

	/** PostgreSQL + Redis, for a test that supplies its own Kafka. */
	@TestConfiguration(proxyBeanMethods = false)
	@Import({Database.class, RedisCache.class})
	public static class DatabaseAndRedis {
	}

	/** PostgreSQL + Kafka, for a test that supplies its own Redis. */
	@TestConfiguration(proxyBeanMethods = false)
	@Import({Database.class, SharedKafkaBroker.class})
	public static class DatabaseAndKafka {
	}

	private static final class SharedKafka {

		private static final KafkaContainer INSTANCE = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.1"));

		static {
			INSTANCE.start();
		}

		private SharedKafka() {
		}
	}

}
