package ru.yandex.practicum.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.AssemblyRequest;
import ru.yandex.practicum.commerce.dto.ShippedToDeliveryRequest;
import ru.yandex.practicum.warehouse.exception.NoProductsInWarehouseException;
import ru.yandex.practicum.warehouse.model.OrderBooking;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.warehouse.repository.OrderBookingRepository;
import ru.yandex.practicum.warehouse.repository.WarehouseProductRepository;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class AssemblyService {

    private final WarehouseProductRepository warehouseProductRepository;
    private final OrderBookingRepository orderBookingRepository;

    @Transactional
    public void assemblyProductForOrderFromShoppingCart(AssemblyRequest request) {
        Map<UUID, Long> products = request.getProducts();

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            Long quantity = entry.getValue();

            WarehouseProduct product = warehouseProductRepository.findByProductId(productId)
                    .orElseThrow(() -> new NoProductsInWarehouseException(
                            "Product not found in warehouse: " + productId));

            if (product.getQuantity() < quantity) {
                throw new NoProductsInWarehouseException(
                        "Not enough quantity for product: " + productId);
            }
        }

        OrderBooking booking = OrderBooking.builder()
                .orderId(request.getOrderId())
                .products(products)
                .build();

        orderBookingRepository.save(booking);

        List<WarehouseProduct> productsToUpdate = products.entrySet().stream()
                .map(entry -> {
                    WarehouseProduct product = warehouseProductRepository.findByProductId(entry.getKey())
                            .orElseThrow(() -> new NoProductsInWarehouseException("Product not found"));
                    product.setQuantity(product.getQuantity() - entry.getValue());
                    return product;
                })
                .collect(Collectors.toList());

        warehouseProductRepository.saveAll(productsToUpdate);

        log.info("Assembled products for order: {}", request.getOrderId());
    }

    @Transactional
    public void shippedToDelivery(ShippedToDeliveryRequest request) {
        OrderBooking booking = orderBookingRepository.findByOrderId(request.getOrderId())
                .orElseThrow(() -> new NoProductsInWarehouseException(
                        "Order booking not found: " + request.getOrderId()));

        booking.setDeliveryId(request.getDeliveryId());
        orderBookingRepository.save(booking);

        log.info("Shipped order {} to delivery {}", request.getOrderId(), request.getDeliveryId());
    }

    @Transactional
    public void returnProduct(Map<UUID, Long> products) {
        List<WarehouseProduct> productsToUpdate = products.entrySet().stream()
                .map(entry -> {
                    UUID productId = entry.getKey();
                    Long quantityToReturn = entry.getValue();

                    WarehouseProduct product = warehouseProductRepository.findByProductId(productId)
                            .orElseGet(() -> {
                                WarehouseProduct newProduct = new WarehouseProduct();
                                newProduct.setProductId(productId);
                                newProduct.setQuantity(0L);
                                newProduct.setWidth(0.0);
                                newProduct.setHeight(0.0);
                                newProduct.setDepth(0.0);
                                newProduct.setWeight(0.0);
                                newProduct.setFragile(false);
                                return newProduct;
                            });

                    product.setQuantity(product.getQuantity() + quantityToReturn);
                    return product;
                })
                .collect(Collectors.toList());

        warehouseProductRepository.saveAll(productsToUpdate);

        log.info("Returned {} products to warehouse", products.size());
    }
}