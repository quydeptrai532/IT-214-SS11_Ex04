package com.storex.inventory.controller;
import com.storex.inventory.event.OrderEvent;
import com.storex.inventory.publisher.OrderEventPublisher;
import com.storex.inventory.service.InventoryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class OrderController {
    private final OrderEventPublisher publisher;
    private final InventoryService inventoryService;

    public OrderController(OrderEventPublisher publisher, InventoryService inventoryService) {
        this.publisher = publisher;
        this.inventoryService = inventoryService;
    }

    @PostMapping("/orders")
    public ResponseEntity<String> sendValidOrder(@RequestBody OrderEvent event) {
        publisher.publish(event);
        return ResponseEntity.ok("Da gui event hop le: " + event);
    }

    @PostMapping("/orders/invalid")
    public ResponseEntity<String> sendInvalidOrder() {
        publisher.publish(new OrderEvent("ORD-BAD-01", null, 1));
        return ResponseEntity.ok("Da gui event loi (productId = null)");
    }

    @GetMapping("/inventory")
    public ResponseEntity<Map<String, Integer>> getInventory() {
        return ResponseEntity.ok(inventoryService.getStock());
    }
}
