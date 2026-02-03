package ru.yandex.practicum.commerce.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AvailabilityStatus {
    ENDED("ENDED"),
    FEW("FEW"),
    ENOUGH("ENOUGH"),
    MANY("MANY");

    private final String value;

    AvailabilityStatus(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static AvailabilityStatus fromValue(String value) {
        for (AvailabilityStatus status : AvailabilityStatus.values()) {
            if (status.value.equals(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown availability status: " + value);
    }
}
