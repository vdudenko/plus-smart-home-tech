package ru.yandex.practicum.commerce.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum QuantityState {
    ENDED("ENDED"),
    FEW("FEW"),
    ENOUGH("ENOUGH"),
    MANY("MANY");

    private final String value;

    QuantityState(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static QuantityState fromValue(String value) {
        for (QuantityState state : QuantityState.values()) {
            if (state.value.equals(value)) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown quantity state: " + value);
    }
}