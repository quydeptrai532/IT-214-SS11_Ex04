package com.storex.inventory.publisher;
import com.storex.inventory.config.KafkaTopicConfig;
import com.storex.inventory.event.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(OrderEvent event) {
        kafkaTemplate.send(KafkaTopicConfig.MAIN_TOPIC, event.getOrderId(), event);
        log.info("Da gui event: orderId={} productId={} quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
    }
}
