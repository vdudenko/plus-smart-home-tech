package ru.yandex.practicum.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.WarehouseAddress;
import ru.yandex.practicum.warehouse.mapper.WarehouseProductMapper;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.warehouse.repository.WarehouseProductRepository;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class WarehouseService {
    private final WarehouseProductRepository warehouseProductRepository;
    private final WarehouseProductMapper warehouseProductMapper;

    @Transactional(readOnly = true)
    public WarehouseAddress getWarehouseAddress() {
        return warehouseProductMapper.getCurrentWarehouseAddress();
    }

    @Transactional
    public void addProductToWarehouse(Long productId, Integer quantity,
                                      Double width, Double height, Double depth,
                                      Double weight, Boolean fragile) {
        if (warehouseProductRepository.existsByProductId(productId)) {
            log.warn("Product with id {} already exists in warehouse", productId);
            throw new RuntimeException("Product already exists in warehouse: " + productId);
        }

        WarehouseProduct product = WarehouseProduct.builder()
                .productId(productId)
                .quantity(quantity)
                .width(BigDecimal.valueOf(width))
                .height(BigDecimal.valueOf(height))
                .depth(BigDecimal.valueOf(depth))
                .weight(BigDecimal.valueOf(weight))
                .fragile(fragile)
                .build();

        warehouseProductRepository.save(product);
        log.info("Product {} added to warehouse with quantity {}", productId, quantity);
    }

    @Transactional
    public void increaseProductQuantity(Long productId, Integer quantity) {
        WarehouseProduct product = warehouseProductRepository.findByProductId(productId)
                .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + productId));

        product.setQuantity(product.getQuantity() + quantity);
        warehouseProductRepository.save(product);
        log.info("Increased quantity for product {} by {}. New quantity: {}",
                productId, quantity, product.getQuantity());
    }

    @Transactional(readOnly = true)
    public Map<Long, Integer> checkProductsAvailability(List<Long> productIds) {
        List<WarehouseProduct> products = warehouseProductRepository.findByProductIdIn(productIds);

        Map<Long, Integer> availabilityMap = new HashMap<>();
        for (Long productId : productIds) {
            availabilityMap.put(productId, 0);
        }

        for (WarehouseProduct product : products) {
            availabilityMap.put(product.getProductId(), product.getQuantity());
        }

        return availabilityMap;
    }

    @Transactional
    public void reserveProducts(Map<Long, Integer> productQuantities) {
        for (Map.Entry<Long, Integer> entry : productQuantities.entrySet()) {
            Long productId = entry.getKey();
            Integer requestedQuantity = entry.getValue();

            WarehouseProduct product = warehouseProductRepository.findByProductId(productId)
                    .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + productId));

            if (product.getQuantity() < requestedQuantity) {
                throw new RuntimeException(String.format(
                        "Insufficient quantity for product %d. Available: %d, Requested: %d",
                        productId, product.getQuantity(), requestedQuantity));
            }

            product.setQuantity(product.getQuantity() - requestedQuantity);
            warehouseProductRepository.save(product);
            log.info("Reserved {} units of product {}", requestedQuantity, productId);
        }
    }
}
