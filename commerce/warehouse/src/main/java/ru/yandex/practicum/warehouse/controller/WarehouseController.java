package ru.yandex.practicum.warehouse.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.WarehouseApi;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
import ru.yandex.practicum.commerce.dto.WarehouseAddress;
import ru.yandex.practicum.warehouse.dto.AddProductRequest;
import ru.yandex.practicum.warehouse.service.WarehouseService;

import java.util.Map;

@RestController
@RequestMapping("/api/warehouse")
@RequiredArgsConstructor
public class WarehouseController implements WarehouseApi {
    private final WarehouseService warehouseService;

    @Override
    @GetMapping("/api/warehouse/address")
    public WarehouseAddress getWarehouseAddress() {
        return warehouseService.getWarehouseAddress();
    }

    @Override
    @PostMapping("/api/warehouse/check-availability")
    public Map<Long, Integer> checkProductsAvailability(@RequestBody ProductAvailabilityRequest request) {
        return warehouseService.checkProductsAvailability(request.getProductIds());
    }

    @PostMapping("/api/warehouse/products")
    public ResponseEntity<Void> addProductToWarehouse(@RequestBody AddProductRequest request) {
        warehouseService.addProductToWarehouse(
                request.getProductId(),
                request.getQuantity(),
                request.getWidth(),
                request.getHeight(),
                request.getDepth(),
                request.getWeight(),
                request.getFragile()
        );
        return ResponseEntity.ok().build();
    }

    @PatchMapping("/api/warehouse/products/{productId}/quantity")
    public ResponseEntity<Void> increaseProductQuantity(
            @PathVariable Long productId,
            @RequestParam Integer quantity) {
        warehouseService.increaseProductQuantity(productId, quantity);
        return ResponseEntity.ok().build();
    }
}
