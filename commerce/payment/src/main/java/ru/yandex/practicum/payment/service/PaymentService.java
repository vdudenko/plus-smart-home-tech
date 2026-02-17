package ru.yandex.practicum.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.payment.exception.NoOrderFoundException;
import ru.yandex.practicum.payment.model.Payment;
import ru.yandex.practicum.payment.repository.PaymentRepository;
import ru.yandex.practicum.payment.mapper.PaymentMapper;
import ru.yandex.practicum.payment.feign.OrderFeignClient;
import ru.yandex.practicum.payment.feign.ShoppingStoreFeignClient;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final OrderFeignClient orderFeignClient;
    private final ShoppingStoreFeignClient shoppingStoreFeignClient;

    @Transactional(readOnly = true)
    public Double productCost(OrderDto order) {
        Map<UUID, Long> products = order.getProducts();
        double totalCost = 0.0;

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            Long quantity = entry.getValue();

            ProductDto product = shoppingStoreFeignClient.getProduct(productId);
            totalCost += product.getPrice() * quantity;
        }

        log.info("Calculated product cost: {}", totalCost);
        return totalCost;
    }

    @Transactional(readOnly = true)
    public Double getTotalCost(OrderDto order) {
        Double productCost = productCost(order);

        Double vat = productCost * 0.1;

        Double deliveryCost = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0.0;

        Double totalCost = productCost + vat + deliveryCost;

        log.info("Calculated total cost: {}", totalCost);
        return totalCost;
    }

    @Transactional
    public PaymentDto payment(OrderDto order) {
        Double productCost = productCost(order);
        Double vat = productCost * 0.1;
        Double deliveryCost = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0.0;
        Double totalCost = productCost + vat + deliveryCost;

        Payment payment = Payment.builder()
                .orderId(order.getOrderId())
                .productTotal(productCost)
                .deliveryTotal(deliveryCost)
                .feeTotal(vat)
                .totalPayment(totalCost)
                .state(PaymentState.PENDING)
                .build();

        payment = paymentRepository.save(payment);

        log.info("Created payment for order {}: {}", order.getOrderId(), payment.getPaymentId());
        return paymentMapper.toDto(payment);
    }

    @Transactional
    public void paymentSuccess(UUID paymentId) {
        Payment payment = paymentRepository.findByPaymentId(paymentId)
                .orElseThrow(() -> new NoOrderFoundException("Payment not found: " + paymentId));

        payment.setState(PaymentState.SUCCESS);
        paymentRepository.save(payment);

        orderFeignClient.paymentSuccess(payment.getOrderId());

        log.info("Payment {} succeeded", paymentId);
    }

    @Transactional
    public void paymentFailed(UUID paymentId) {
        Payment payment = paymentRepository.findByPaymentId(paymentId)
                .orElseThrow(() -> new NoOrderFoundException("Payment not found: " + paymentId));

        payment.setState(PaymentState.FAILED);
        paymentRepository.save(payment);

        orderFeignClient.paymentFailed(payment.getOrderId());

        log.info("Payment {} failed", paymentId);
    }
}