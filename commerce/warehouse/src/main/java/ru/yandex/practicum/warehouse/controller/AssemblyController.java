package ru.yandex.practicum.warehouse.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.AssemblyRequest;
import ru.yandex.practicum.commerce.dto.ShippedToDeliveryRequest;
import ru.yandex.practicum.warehouse.exception.NoProductsInWarehouseException;
import ru.yandex.practicum.warehouse.service.AssemblyService;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class AssemblyController {

    private final AssemblyService assemblyService;

    @PostMapping("/assembly")
    public ResponseEntity<Void> assemblyProductForOrderFromShoppingCart(
            @RequestBody AssemblyRequest request) {
        assemblyService.assemblyProductForOrderFromShoppingCart(request);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/shipped")
    public ResponseEntity<Void> shippedToDelivery(
            @RequestBody ShippedToDeliveryRequest request) {
        assemblyService.shippedToDelivery(request);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/return")
    public ResponseEntity<Void> returnProduct(@RequestBody Map<UUID, Long> products) {
        assemblyService.returnProduct(products);
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(NoProductsInWarehouseException.class)
    public ResponseEntity<Void> handleNoProductsInWarehouse(NoProductsInWarehouseException ex) {
        return ResponseEntity.badRequest().build();
    }
}