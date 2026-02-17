package ru.yandex.practicum.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.delivery.exception.NoDeliveryFoundException;
import ru.yandex.practicum.delivery.model.Delivery;
import ru.yandex.practicum.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.delivery.mapper.DeliveryMapper;
import ru.yandex.practicum.delivery.feign.OrderFeignClient;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper deliveryMapper;
    private final OrderFeignClient orderFeignClient;
    private final DeliveryCostCalculator deliveryCostCalculator;

    @Transactional
    public DeliveryDto planDelivery(DeliveryDto deliveryDto) {
        Delivery delivery = deliveryMapper.toEntity(deliveryDto);
        delivery.setDeliveryState(DeliveryState.CREATED);

        delivery = deliveryRepository.save(delivery);

        log.info("Planned delivery: {}", delivery.getDeliveryId());
        return deliveryMapper.toDto(delivery);
    }

    @Transactional(readOnly = true)
    public Double deliveryCost(OrderDto order) {
        return deliveryCostCalculator.calculateDeliveryCost(order);
    }

    @Transactional
    public void deliveryPicked(UUID deliveryId) {
        Delivery delivery = deliveryRepository.findByDeliveryId(deliveryId)
                .orElseThrow(() -> new NoDeliveryFoundException("Delivery not found: " + deliveryId));

        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);

        orderFeignClient.assembly(delivery.getOrderId());

        log.info("Delivery {} picked", deliveryId);
    }

    @Transactional
    public void deliverySuccessful(UUID deliveryId) {
        Delivery delivery = deliveryRepository.findByDeliveryId(deliveryId)
                .orElseThrow(() -> new NoDeliveryFoundException("Delivery not found: " + deliveryId));

        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);

        orderFeignClient.delivery(delivery.getOrderId());

        log.info("Delivery {} successful", deliveryId);
    }

    @Transactional
    public void deliveryFailed(UUID deliveryId) {
        Delivery delivery = deliveryRepository.findByDeliveryId(deliveryId)
                .orElseThrow(() -> new NoDeliveryFoundException("Delivery not found: " + deliveryId));

        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);

        orderFeignClient.deliveryFailed(delivery.getOrderId());

        log.info("Delivery {} failed", deliveryId);
    }
}