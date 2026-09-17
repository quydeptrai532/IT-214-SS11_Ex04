package com.storex.inventory;
import com.storex.inventory.event.OrderEvent;
import com.storex.inventory.publisher.OrderEventPublisher;
import com.storex.inventory.service.InventoryService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"storex-order-events", "storex-order-events.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class InventoryDlqTest {

    private static final String MAIN_TOPIC = "storex-order-events";
    private static final String DLQ_TOPIC = "storex-order-events.DLQ";

    @Autowired
    private OrderEventPublisher publisher;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> dlqConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("dlq-verify-" + UUID.randomUUID(), "true", embeddedKafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> c = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(c, DLQ_TOPIC);
        return c;
    }

    private void sendRawMalformedJson(String key, String rawJson) {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(MAIN_TOPIC, key, rawJson));
            producer.flush();
        }
    }

    @Test
    void loiSau3LanRetryDuocDuaVaoDlqVaConsumerKhongBiKet() throws Exception {
        Consumer<String, String> dlq = dlqConsumer();

        // 1) Message JSON sai dinh dang
        sendRawMalformedJson("ORD-BAD-JSON", "{\"orderId\":\"ORD-BAD-JSON\",\"productId\":\"P001\",\"quantity\":2");

        // 2) Message dung dinh dang nhung loi nghiep vu: productId = null
        publisher.publish(new OrderEvent("ORD-BAD-NULL", null, 1));

        Set<String> dlqKeys = new HashSet<>();
        long deadline = System.currentTimeMillis() + 90_000;
        while (System.currentTimeMillis() < deadline && dlqKeys.size() < 2) {
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(dlq, Duration.ofSeconds(3));
            for (ConsumerRecord<String, String> r : records) {
                dlqKeys.add(r.key());
                System.out.println(">>> DLQ nhan duoc: key=" + r.key() + " | value=" + r.value());
            }
        }
        dlq.close();

        assertThat(dlqKeys).contains("ORD-BAD-JSON", "ORD-BAD-NULL");

        // 3) Consumer khong bi ket: message hop le tiep theo van duoc xu ly
        int before = inventoryService.getStock().get("P002");
        publisher.publish(new OrderEvent("ORD-OK-01", "P002", 5));

        int after = before;
        deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            after = inventoryService.getStock().get("P002");
            if (after == before - 5) {
                break;
            }
            Thread.sleep(300);
        }

        System.out.println(">>> Consumer van xu ly duoc message hop le: P002 " + before + " -> " + after);
        assertThat(after).isEqualTo(before - 5);
    }
}
