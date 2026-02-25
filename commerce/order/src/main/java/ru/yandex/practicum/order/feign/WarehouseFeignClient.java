package ru.yandex.practicum.order.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.AssemblyRequest;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse")
public interface WarehouseFeignClient {

    @PostMapping("/api/v1/warehouse/assembly")
    void assemblyProductForOrderFromShoppingCart(@RequestBody AssemblyRequest request);

    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();

    @PostMapping("/api/v1/warehouse/return")
    void returnProduct(@RequestBody Map<UUID, Long> products);
}