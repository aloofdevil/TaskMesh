package com.taskmesh.controlplane.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.taskmesh.controlplane.service.OutboxProperties;

/**
 * Declares the two lifecycle topics.
 * <p>
 * Keyed by job id and worker id respectively, so all events for one
 * aggregate land on the same partition and a consumer sees that job's
 * history in order.
 * <p>
 * Spring's KafkaAdmin creates these at startup when a broker is reachable
 * and merely logs if one is not, which is the behaviour we want: a control
 * plane must still start and run its job lifecycle during a Kafka outage.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 1;

    @Bean
    NewTopic jobEventsTopic(OutboxProperties properties) {
        return TopicBuilder.name(properties.jobEventsTopic())
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .build();
    }

    @Bean
    NewTopic workerEventsTopic(OutboxProperties properties) {
        return TopicBuilder.name(properties.workerEventsTopic())
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .build();
    }
}
