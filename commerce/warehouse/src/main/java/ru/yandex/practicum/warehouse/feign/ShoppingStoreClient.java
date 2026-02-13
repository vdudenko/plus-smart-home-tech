package ru.yandex.practicum.warehouse.feign;

import org.springframework.cloud.openfeign.FeignClient;
import ru.yandex.practicum.commerce.api.ShoppingStoreApi;

@FeignClient(name = "shopping-store")
public interface ShoppingStoreClient extends ShoppingStoreApi { // ← Клиент для витрины
}