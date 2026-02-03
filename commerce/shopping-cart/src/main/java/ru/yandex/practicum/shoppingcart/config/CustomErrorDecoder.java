package ru.yandex.practicum.shoppingcart.config;

import feign.FeignException;
import feign.Response;
import feign.codec.ErrorDecoder;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.practicum.shoppingcart.exception.InsufficientStockException;

@Slf4j
public class CustomErrorDecoder implements ErrorDecoder {
    @Override
    public Exception decode(String methodKey, Response response) {
        log.error("Feign client error: {} - {}", response.status(), response.reason());

        if (response.status() == 503) {
            return new RuntimeException("Warehouse service is unavailable");
        }

        if (response.status() == 404) {
            return new RuntimeException("Resource not found");
        }

        if (response.status() == 400) {
            return new InsufficientStockException("Insufficient stock for requested products");
        }

        return FeignException.errorStatus(methodKey, response);
    }
}
