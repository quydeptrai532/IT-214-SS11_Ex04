# BÀI TẬP 4 (SS11) — FAULT TOLERANCE & DEAD LETTER QUEUE

Xem phân tích tại `Ex04_Analysis.md`, bằng chứng chạy thật tại `Ex04_TestEvidence.txt`.

## Cấu trúc

```
Ex04/
├── Ex04_Analysis.md
├── Ex04_TestEvidence.txt
└── kafka-inventory-dlq/
    ├── build.gradle
    └── src/main/java/com/storex/inventory/
        ├── InventoryDlqApplication.java
        ├── config/KafkaConfig.java        # ★ retry 3x2s + DLQ + log ERROR
        ├── config/KafkaTopicConfig.java   # storex-order-events + storex-order-events.DLQ
        ├── consumer/InventoryConsumer.java
        ├── service/InventoryService.java
        ├── publisher/OrderEventPublisher.java
        ├── controller/OrderController.java
        └── exception/OrderProcessingException.java
```

## Chạy test (không cần cài Kafka — dùng Embedded Kafka)

```bash
cd kafka-inventory-dlq
./gradlew test
```

## Chạy thật với Kafka ở localhost:9092

```bash
./gradlew bootRun      # port 8085
```

```bash
curl http://localhost:8085/api/inventory                       # ton kho ban dau
curl -X POST http://localhost:8085/api/orders/invalid          # event loi (productId = null)
curl -X POST http://localhost:8085/api/orders \
     -H "Content-Type: application/json" \
     -d '{"orderId":"ORD-OK-01","productId":"P002","quantity":5}'   # event hop le
curl http://localhost:8085/api/inventory                       # P002 giam 5

# Doc DLQ
kafka-console-consumer --bootstrap-server localhost:9092 --topic storex-order-events.DLQ --from-beginning
```

## Kết quả

```
Retry lan 1..4 that bai cho offset=0 key=ORD-BAD-JSON          (1 lan dau + 3 retry, cach 2s)
Da nem don hang bi loi vao DLQ: topic=storex-order-events ... key=ORD-BAD-JSON
>>> DLQ nhan duoc: key=ORD-BAD-JSON
>>> DLQ nhan duoc: key=ORD-BAD-NULL | value={"orderId":"ORD-BAD-NULL","productId":null,"quantity":1}
>>> Consumer van xu ly duoc message hop le: P002 50 -> 45       (khong bi ket)
```
a