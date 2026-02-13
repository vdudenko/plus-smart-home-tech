package ru.yandex.practicum.commerce.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SetProductQuantityStateRequest {
    @JsonProperty("productId")
    private UUID productId;

    @JsonProperty("quantityState")
    private QuantityState quantityState;
}