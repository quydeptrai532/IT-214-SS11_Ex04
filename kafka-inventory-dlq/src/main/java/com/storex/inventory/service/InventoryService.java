package com.storex.inventory.service;
import com.storex.inventory.exception.OrderProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class InventoryService {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private final Map<String, Integer> stock = new ConcurrentHashMap<>();

    public InventoryService() {
        stock.put("P001", 100);
        stock.put("P002", 50);
        stock.put("P003", 20);
    }

    public void deductStock(String productId, Integer quantity) {
        if (productId == null) {
            throw new OrderProcessingException("productId is null - khong the xu ly don hang");
        }
        if (quantity == null || quantity <= 0) {
            throw new OrderProcessingException("quantity khong hop le: " + quantity);
        }
        Integer current = stock.get(productId);
        if (current == null) {
            throw new OrderProcessingException("Khong tim thay san pham: " + productId);
        }
        if (current < quantity) {
            throw new OrderProcessingException("Khong du ton kho cho " + productId + ", con " + current);
        }
        stock.put(productId, current - quantity);
        log.info("Tru kho thanh cong: productId={} quantity={} con lai={}", productId, quantity, current - quantity);
    }

    public Map<String, Integer> getStock() {
        return stock;
    }
}
