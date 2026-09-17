package com.storex.inventory.consumer;
import com.storex.inventory.event.OrderEvent;
import com.storex.inventory.exception.OrderProcessingException;
import com.storex.inventory.service.InventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {
    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);
    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "storex-order-events", groupId = "inventory-group")
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        log.info("Nhan message: partition={} offset={} key={} value={}",
                record.partition(), record.offset(), record.key(), record.value());

        if (record.value() == null) {
            throw new OrderProcessingException("Khong the parse JSON thanh OrderEvent (JSON sai dinh dang)");
        }

        OrderEvent event = record.value();
        inventoryService.deductStock(event.getProductId(), event.getQuantity());
        log.info("Xu ly thanh cong don hang: orderId={}", event.getOrderId());
    }
}
