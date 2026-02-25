package ru.yandex.practicum.delivery.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import ru.yandex.practicum.commerce.dto.AddressDto;

@FeignClient(name = "warehouse")
public interface WarehouseFeignClient {
    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();
}