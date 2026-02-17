package ru.yandex.practicum.delivery.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.DeliveryDto;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.delivery.exception.NoDeliveryFoundException;
import ru.yandex.practicum.delivery.service.DeliveryService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
public class DeliveryController {

    private final DeliveryService deliveryService;

    @PutMapping
    public ResponseEntity<DeliveryDto> planDelivery(@RequestBody DeliveryDto delivery) {
        return ResponseEntity.ok(deliveryService.planDelivery(delivery));
    }

    @PostMapping("/cost")
    public ResponseEntity<Double> deliveryCost(@RequestBody OrderDto order) {
        return ResponseEntity.ok(deliveryService.deliveryCost(order));
    }

    @PostMapping("/picked")
    public ResponseEntity<Void> deliveryPicked(@RequestBody UUID deliveryId) {
        deliveryService.deliveryPicked(deliveryId);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/successful")
    public ResponseEntity<Void> deliverySuccessful(@RequestBody UUID deliveryId) {
        deliveryService.deliverySuccessful(deliveryId);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/failed")
    public ResponseEntity<Void> deliveryFailed(@RequestBody UUID deliveryId) {
        deliveryService.deliveryFailed(deliveryId);
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(NoDeliveryFoundException.class)
    public ResponseEntity<Void> handleNoDeliveryFound(NoDeliveryFoundException ex) {
        return ResponseEntity.notFound().build();
    }
}