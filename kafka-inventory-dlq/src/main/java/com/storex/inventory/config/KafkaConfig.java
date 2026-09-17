package com.storex.inventory.config;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.error("Da nem don hang bi loi vao DLQ: topic={} partition={} offset={} key={} value={} | ly do={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value(), ex.getMessage());
            return new TopicPartition(KafkaTopicConfig.DLQ_TOPIC, 0);
        });
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));
        handler.setCommitRecovered(true);
        // JSON sai dinh dang cung phai duoc retry 3 lan truoc khi vao DLQ
        handler.addRetryableExceptions(DeserializationException.class, MessageConversionException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Retry lan {} that bai cho offset={} key={} : {}",
                        attempt, record.offset(), record.key(), ex.getMessage()));
        return handler;
    }
}
