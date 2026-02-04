package ru.yandex.practicum.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.warehouse.mapper.WarehouseProductMapper;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.warehouse.repository.WarehouseProductRepository;

import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    public void newProductInWarehouse(NewProductInWarehouseRequest request) {
        if (warehouseProductRepository.existsByProductId(request.getProductId())) {
            log.warn("Product with id {} already exists in warehouse", request.getProductId());
            throw new RuntimeException("Product already exists in warehouse: " + request.getProductId());
        }

        WarehouseProduct product = warehouseProductMapper.toEntity(request);
        warehouseProductRepository.save(product);
        log.info("Product {} added to warehouse", request.getProductId());
    }

    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        WarehouseProduct product = warehouseProductRepository.findByProductId(request.getProductId())
                .orElseThrow(() -> new RuntimeException("Product not found in warehouse: " + request.getProductId()));

        product.setQuantity(product.getQuantity() + request.getQuantity());
        warehouseProductRepository.save(product);
        log.info("Increased quantity for product {} by {}. New quantity: {}",
                request.getProductId(), request.getQuantity(), product.getQuantity());
    }

    @Transactional(readOnly = true)
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto shoppingCartDto) {
        Map<UUID, Long> products = shoppingCartDto.getProducts();
        List<WarehouseProduct> warehouseProducts = warehouseProductRepository.findByProductIdIn(List.copyOf(products.keySet()));

        // Проверяем достаточность количества
        for (WarehouseProduct wp : warehouseProducts) {
            Long requestedQuantity = products.get(wp.getProductId());
            if (wp.getQuantity() < requestedQuantity) {
                throw new RuntimeException(String.format(
                        "Insufficient quantity for product %s. Available: %d, Requested: %d",
                        wp.getProductId(), wp.getQuantity(), requestedQuantity));
            }
        }

        // Рассчитываем общие параметры доставки
        double totalWeight = 0.0;
        double totalVolume = 0.0;
        boolean hasFragile = false;

        for (WarehouseProduct wp : warehouseProducts) {
            Long requestedQuantity = products.get(wp.getProductId());
            totalWeight += wp.getWeight() * requestedQuantity;
            totalVolume += wp.getWidth() * wp.getHeight() * wp.getDepth() * requestedQuantity;
            if (wp.getFragile()) {
                hasFragile = true;
            }
        }

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();
    }
}
