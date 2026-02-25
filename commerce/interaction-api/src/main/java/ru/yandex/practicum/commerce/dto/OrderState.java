package ru.yandex.practicum.commerce.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum OrderState {
    NEW("NEW"),
    ON_PAYMENT("ON_PAYMENT"),
    ON_DELIVERY("ON_DELIVERY"),
    DONE("DONE"),
    DELIVERED("DELIVERED"),
    ASSEMBLED("ASSEMBLED"),
    PAID("PAID"),
    COMPLETED("COMPLETED"),
    DELIVERY_FAILED("DELIVERY_FAILED"),
    ASSEMBLY_FAILED("ASSEMBLY_FAILED"),
    PAYMENT_FAILED("PAYMENT_FAILED"),
    PRODUCT_RETURNED("PRODUCT_RETURNED"),
    CANCELED("CANCELED");

    private final String value;

    OrderState(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static OrderState fromValue(String value) {
        for (OrderState state : OrderState.values()) {
            if (state.value.equals(value)) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown order state: " + value);
    }
}