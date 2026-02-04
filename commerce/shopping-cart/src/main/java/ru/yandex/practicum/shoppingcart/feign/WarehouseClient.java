package ru.yandex.practicum.shoppingcart.feign;

import org.springframework.cloud.openfeign.FeignClient;
import ru.yandex.practicum.commerce.api.WarehouseApi;

@FeignClient(name = "warehouse")
public interface WarehouseClient extends WarehouseApi {
}
