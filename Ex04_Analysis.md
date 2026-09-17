# BÀI TẬP 4 (SS11) — CHỐNG CHỊU LỖI VÀ XỬ LÝ DEAD LETTER QUEUE

**Bối cảnh:** Inventory-Service nhận sự kiện từ topic `storex-order-events`. Một số message bị lỗi JSON sai định dạng, hoặc `productId = null`. Consumer cũ quăng exception → đọc đi đọc lại cùng một message (**infinite loop**) → **blocking partition**, hàng vạn đơn hợp lệ phía sau bị kẹt.

---

## PHẦN 1 — VÌ SAO CONSUMER BỊ KẸT?

- Kafka chỉ commit offset khi listener xử lý **thành công**. Khi listener ném exception, container **không commit** offset đó.
- Lần `poll()` kế tiếp, consumer vẫn đứng ở **cùng vị trí** ⇒ đọc lại đúng message lỗi ⇒ lại ném exception ⇒ **lặp vô hạn**.
- Trong một partition, các message phía sau **buộc phải xếp hàng** ⇒ partition bị "chặn cứng".

→ Cần một **ErrorHandler**: retry có giới hạn, và nếu vẫn lỗi thì **đẩy sang DLQ rồi commit offset** để consumer đi tiếp.

---

## PHẦN 2 — TRIỂN KHAI

### 2.1. Retry + DLQ (`config/KafkaConfig.java`)

```java
@Bean
public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
    // retry toi da 3 lan, moi lan cach 2 giay
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));
    handler.setCommitRecovered(true);                 // day DLQ xong moi commit -> khong bi ket
    handler.addRetryableExceptions(DeserializationException.class, MessageConversionException.class);
    handler.setRetryListeners((record, ex, attempt) ->
            log.warn("Retry lan {} that bai cho offset={} key={} : {}", attempt, record.offset(), record.key(), ex.getMessage()));
    return handler;
}
```

### 2.2. Đích DLQ = `storex-order-events.DLQ` + log ERROR (REQ-02)

```java
@Bean
public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> template) {
    return new DeadLetterPublishingRecoverer(template, (record, ex) -> {
        log.error("Da nem don hang bi loi vao DLQ: topic={} partition={} offset={} key={} value={} | ly do={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value(), ex.getMessage());
        return new TopicPartition("storex-order-events.DLQ", 0);
    });
}
```

### 2.3. BUG-05 — trusted packages (`application.yml`)

```yaml
spring:
  kafka:
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.value.default.type: com.storex.inventory.event.OrderEvent
        spring.json.trusted.packages: "*"     # ★ BUG-05: cho phep deserialize object Java
```

`ErrorHandlingDeserializer` bọc `JsonDeserializer`: khi JSON sai định dạng, nó **không làm chết vòng poll**, mà trả value `null` + gắn header `SerializationException` → để ErrorHandler xử lý.

### 2.4. Consumer sau khi sửa

```java
@KafkaListener(topics = "storex-order-events", groupId = "inventory-group")
public void consume(ConsumerRecord<String, OrderEvent> record) {
    if (record.value() == null) {
        throw new OrderProcessingException("Khong the parse JSON thanh OrderEvent (JSON sai dinh dang)");
    }
    inventoryService.deductStock(record.value().getProductId(), record.value().getQuantity());
}
```

---

## PHẦN 3 — TIÊU CHÍ NGHIỆM THU

| Mã | Yêu cầu | Kết quả |
|---|---|---|
| — | Retry tối đa 3 lần, cách nhau 2 giây | ✅ `FixedBackOff(2000L, 3L)` — log `Retry lan 1..4` |
| — | Đẩy sang `storex-order-events.DLQ` | ✅ Cả message sai JSON và `productId = null` đều vào DLQ |
| BUG-05 | `trusted.packages=*` | ✅ Có trong `application.yml` |
| REQ-02 | Log ERROR "Đã ném đơn hàng bị lỗi vào DLQ" | ✅ Log mức ERROR đúng nội dung |
| — | Consumer không bị kẹt | ✅ Message hợp lệ sau đó vẫn được xử lý (P002 50 → 45) |

Bằng chứng chạy thật: `Ex04_TestEvidence.txt`
