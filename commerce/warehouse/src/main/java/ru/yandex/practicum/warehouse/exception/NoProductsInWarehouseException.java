package ru.yandex.practicum.warehouse.exception;

public class NoProductsInWarehouseException extends RuntimeException {
    public NoProductsInWarehouseException(String message) {
        super(message);
    }
}