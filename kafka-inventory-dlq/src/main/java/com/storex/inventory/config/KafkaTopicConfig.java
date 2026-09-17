package com.storex.inventory.config;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String MAIN_TOPIC = "storex-order-events";
    public static final String DLQ_TOPIC = "storex-order-events.DLQ";

    @Bean
    public NewTopic storexOrderEventsTopic() {
        return TopicBuilder.name(MAIN_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic storexOrderEventsDlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC).partitions(1).replicas(1).build();
    }
}
