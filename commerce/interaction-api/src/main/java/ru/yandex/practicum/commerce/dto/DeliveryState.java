package ru.yandex.practicum.commerce.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DeliveryState {
    CREATED("CREATED"),
    IN_PROGRESS("IN_PROGRESS"),
    DELIVERED("DELIVERED"),
    FAILED("FAILED"),
    CANCELLED("CANCELLED");

    private final String value;

    DeliveryState(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static DeliveryState fromValue(String value) {
        for (DeliveryState state : DeliveryState.values()) {
            if (state.value.equals(value)) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown delivery state: " + value);
    }
}