package ru.yandex.practicum.commerce.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ProductState {
    ACTIVE("ACTIVE"),
    DEACTIVATE("DEACTIVATE");

    private final String value;

    ProductState(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static ProductState fromValue(String value) {
        for (ProductState state : ProductState.values()) {
            if (state.value.equals(value)) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown product state: " + value);
    }
}