package ru.yandex.practicum.order.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.order.exception.NoOrderFoundException;
import ru.yandex.practicum.order.service.OrderService;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @GetMapping
    public ResponseEntity<List<OrderDto>> getClientOrders(@RequestParam String username) {
        return ResponseEntity.ok(orderService.getClientOrders(username));
    }

    @PutMapping
    public ResponseEntity<OrderDto> createNewOrder(@RequestBody CreateNewOrderRequest request) {
        return ResponseEntity.ok(orderService.createNewOrder(request));
    }

    @PostMapping("/assembly")
    public ResponseEntity<OrderDto> assembly(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.assembly(orderId));
    }

    @PostMapping("/assembly/failed")
    public ResponseEntity<OrderDto> assemblyFailed(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.assemblyFailed(orderId));
    }

    @PostMapping("/calculate/delivery")
    public ResponseEntity<OrderDto> calculateDeliveryCost(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.calculateDeliveryCost(orderId));
    }

    @PostMapping("/calculate/total")
    public ResponseEntity<OrderDto> calculateTotalCost(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.calculateTotalCost(orderId));
    }

    @PostMapping("/payment")
    public ResponseEntity<OrderDto> payment(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.payment(orderId));
    }

    @PostMapping("/payment/failed")
    public ResponseEntity<OrderDto> paymentFailed(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.paymentFailed(orderId));
    }

    @PostMapping("/delivery")
    public ResponseEntity<OrderDto> delivery(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.delivery(orderId));
    }

    @PostMapping("/delivery/failed")
    public ResponseEntity<OrderDto> deliveryFailed(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.deliveryFailed(orderId));
    }

    @PostMapping("/completed")
    public ResponseEntity<OrderDto> complete(@RequestBody UUID orderId) {
        return ResponseEntity.ok(orderService.complete(orderId));
    }

    @PostMapping("/return")
    public ResponseEntity<OrderDto> productReturn(@RequestBody ProductReturnRequest request) {
        return ResponseEntity.ok(orderService.productReturn(request));
    }

    @ExceptionHandler(NoOrderFoundException.class)
    public ResponseEntity<Void> handleNoOrderFound(NoOrderFoundException ex) {
        return ResponseEntity.notFound().build();
    }
}