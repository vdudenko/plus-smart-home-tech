package ru.yandex.practicum.payment.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.commerce.dto.PaymentDto;
import ru.yandex.practicum.payment.exception.NoOrderFoundException;
import ru.yandex.practicum.payment.service.PaymentService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/productCost")
    public ResponseEntity<Double> productCost(@RequestBody OrderDto order) {
        return ResponseEntity.ok(paymentService.productCost(order));
    }

    @PostMapping("/totalCost")
    public ResponseEntity<Double> getTotalCost(@RequestBody OrderDto order) {
        return ResponseEntity.ok(paymentService.getTotalCost(order));
    }

    @PostMapping
    public ResponseEntity<PaymentDto> payment(@RequestBody OrderDto order) {
        return ResponseEntity.ok(paymentService.payment(order));
    }

    @PostMapping("/refund")
    public ResponseEntity<Void> paymentSuccess(@RequestBody UUID paymentId) {
        paymentService.paymentSuccess(paymentId);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/failed")
    public ResponseEntity<Void> paymentFailed(@RequestBody UUID paymentId) {
        paymentService.paymentFailed(paymentId);
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(NoOrderFoundException.class)
    public ResponseEntity<Void> handleNoOrderFound(NoOrderFoundException ex) {
        return ResponseEntity.notFound().build();
    }
}