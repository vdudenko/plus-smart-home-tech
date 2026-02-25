package ru.yandex.practicum.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.warehouse.feign.ShoppingStoreClient;
import ru.yandex.practicum.warehouse.mapper.WarehouseProductMapper;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.warehouse.provider.WarehouseAddressProvider;
import ru.yandex.practicum.warehouse.repository.WarehouseProductRepository;
import ru.yandex.practicum.warehouse.util.QuantityStateCalculator;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class WarehouseService {
    private final WarehouseProductRepository warehouseProductRepository;
    private final WarehouseProductMapper warehouseProductMapper;
    private final ShoppingStoreClient shoppingStoreClient;
    private final WarehouseAddressProvider warehouseAddressProvider;

    @Transactional(readOnly = true)
    public WarehouseAddress getWarehouseAddress() {
        return warehouseAddressProvider.getCurrentWarehouseAddress();
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

        Long oldQuantity = product.getQuantity();
        product.setQuantity(oldQuantity + request.getQuantity());
        warehouseProductRepository.save(product);
        log.info("Increased quantity for product {} from {} to {}",
                request.getProductId(), oldQuantity, product.getQuantity());

        updateQuantityStateOnShoppingStore(request.getProductId(), product.getQuantity());
    }

    @Transactional(readOnly = true)
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto shoppingCartDto) {
        Map<UUID, Long> products = shoppingCartDto.getProducts();
        List<WarehouseProduct> warehouseProducts = warehouseProductRepository.findByProductIdIn(List.copyOf(products.keySet()));

        for (WarehouseProduct wp : warehouseProducts) {
            Long requestedQuantity = products.get(wp.getProductId());
            if (wp.getQuantity() < requestedQuantity) {
                throw new RuntimeException(String.format(
                        "Insufficient quantity for product %s. Available: %d, Requested: %d",
                        wp.getProductId(), wp.getQuantity(), requestedQuantity));
            }
        }

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

    private void updateQuantityStateOnShoppingStore(UUID productId, Long quantity) {
        try {
            QuantityState newState = QuantityStateCalculator.calculate(quantity);

            SetProductQuantityStateRequest request = SetProductQuantityStateRequest.builder()
                    .productId(productId)
                    .quantityState(newState)
                    .build();

            Boolean success = shoppingStoreClient.setProductQuantityState(request);

            if (Boolean.TRUE.equals(success)) {
                log.info("Successfully updated quantity state for product {} to {}", productId, newState);
            } else {
                log.warn("Failed to update quantity state for product {} to {}", productId, newState);
            }
        } catch (Exception e) {
            log.error("Error updating quantity state for product {} on shopping-store: {}", productId, e.getMessage());
        }
    }
}