package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.order.exception.NoOrderFoundException;
import ru.yandex.practicum.order.model.Order;
import ru.yandex.practicum.order.repository.OrderRepository;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.feign.DeliveryFeignClient;
import ru.yandex.practicum.order.feign.PaymentFeignClient;
import ru.yandex.practicum.order.feign.WarehouseFeignClient;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final DeliveryFeignClient deliveryFeignClient;
    private final PaymentFeignClient paymentFeignClient;
    private final WarehouseFeignClient warehouseFeignClient;

    @Transactional(readOnly = true)
    public List<OrderDto> getClientOrders(String username) {
        List<Order> orders = orderRepository.findByShoppingCartId(UUID.fromString(username));
        return orders.stream()
                .map(orderMapper::toDto)
                .collect(Collectors.toList());
    }

    @Transactional
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        AssemblyRequest assemblyRequest = AssemblyRequest.builder()
                .orderId(UUID.randomUUID())
                .products(request.getShoppingCart().getProducts())
                .build();

        warehouseFeignClient.assemblyProductForOrderFromShoppingCart(assemblyRequest);

        AddressDto warehouseAddress = warehouseFeignClient.getWarehouseAddress();

        DeliveryDto delivery = DeliveryDto.builder()
                .fromAddress(warehouseAddress)
                .toAddress(request.getDeliveryAddress())
                .orderId(assemblyRequest.getOrderId())
                .deliveryState(DeliveryState.CREATED)
                .build();

        DeliveryDto createdDelivery = deliveryFeignClient.planDelivery(delivery);

        Order order = Order.builder()
                .orderId(assemblyRequest.getOrderId())
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .products(request.getShoppingCart().getProducts())
                .deliveryId(createdDelivery.getDeliveryId())
                .state(OrderState.NEW)
                .build();

        order = orderRepository.save(order);

        log.info("Created new order: {}", order.getOrderId());
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto assembly(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        order.setState(OrderState.ASSEMBLED);
        order = orderRepository.save(order);

        log.info("Order {} assembled", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        order.setState(OrderState.ASSEMBLY_FAILED);
        order = orderRepository.save(order);

        log.info("Order {} assembly failed", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto calculateDeliveryCost(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        OrderDto orderDto = orderMapper.toDto(order);
        Double deliveryCost = deliveryFeignClient.deliveryCost(orderDto);

        order.setDeliveryPrice(deliveryCost);
        order = orderRepository.save(order);

        log.info("Calculated delivery cost for order {}: {}", orderId, deliveryCost);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto calculateTotalCost(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        OrderDto orderDto = orderMapper.toDto(order);
        Double productCost = paymentFeignClient.productCost(orderDto);

        Double totalCost = paymentFeignClient.getTotalCost(orderDto);

        order.setProductPrice(productCost);
        order.setTotalPrice(totalCost);
        order = orderRepository.save(order);

        log.info("Calculated total cost for order {}: {}", orderId, totalCost);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto payment(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        order.setState(OrderState.PAID);
        order = orderRepository.save(order);

        log.info("Order {} paid", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        order.setState(OrderState.PAYMENT_FAILED);
        order = orderRepository.save(order);

        log.info("Order {} payment failed", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto delivery(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        order.setState(OrderState.DELIVERED);
        order = orderRepository.save(order);

        log.info("Order {} delivered", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        order.setState(OrderState.DELIVERY_FAILED);
        order = orderRepository.save(order);

        log.info("Order {} delivery failed", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto complete(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));

        order.setState(OrderState.COMPLETED);
        order = orderRepository.save(order);

        log.info("Order {} completed", orderId);
        return orderMapper.toDto(order);
    }

    @Transactional
    public OrderDto productReturn(ProductReturnRequest request) {
        Order order = orderRepository.findById(request.getOrderId())
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + request.getOrderId()));

        warehouseFeignClient.returnProduct(request.getProducts());

        order.setState(OrderState.PRODUCT_RETURNED);
        order = orderRepository.save(order);

        log.info("Order {} product returned", request.getOrderId());
        return orderMapper.toDto(order);
    }
}