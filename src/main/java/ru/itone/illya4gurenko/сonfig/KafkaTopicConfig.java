package ru.itone.illya4gurenko.сonfig;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    private static final String RETENTION_MS_VALUE = "259200000";

    @Bean
    public NewTopic firstTopic() {
        return TopicBuilder.name("first-topic")
                .partitions(3)
                .replicas(1)
                .config("retention.ms", RETENTION_MS_VALUE)
                .build();
    }

    @Bean
    public NewTopic secondTopic() {
        return TopicBuilder.name("second-topic")
                .partitions(3)
                .replicas(1)
                .config("retention.ms", RETENTION_MS_VALUE)
                .build();
    }
}
