package ru.yandex.practicum.warehouse.util;

import ru.yandex.practicum.commerce.dto.QuantityState;

public class QuantityStateCalculator {

    public static QuantityState calculate(Long quantity) {
        if (quantity == null || quantity <= 0) {
            return QuantityState.ENDED;
        } else if (quantity < 10) {
            return QuantityState.FEW;
        } else if (quantity <= 100) {
            return QuantityState.ENOUGH;
        } else {
            return QuantityState.MANY;
        }
    }
}