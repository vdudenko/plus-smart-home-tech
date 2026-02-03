package ru.yandex.practicum.commerce.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
import ru.yandex.practicum.commerce.dto.WarehouseAddress;

import java.util.Map;

public interface WarehouseApi {
    @GetMapping("/api/warehouse/address")
    WarehouseAddress getWarehouseAddress();

    @PostMapping("/api/warehouse/check-availability")
    Map<Long, Integer> checkProductsAvailability(@RequestBody ProductAvailabilityRequest request);
}
