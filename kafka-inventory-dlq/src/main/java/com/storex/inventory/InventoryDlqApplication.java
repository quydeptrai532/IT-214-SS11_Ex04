package com.storex.inventory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class InventoryDlqApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryDlqApplication.class, args);
    }
}
