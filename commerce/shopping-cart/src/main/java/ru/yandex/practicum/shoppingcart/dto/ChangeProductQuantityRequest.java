package ru.yandex.practicum.shoppingcart.dto;

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
public class ChangeProductQuantityRequest {
    @JsonProperty("productId")
    private UUID productId;

    @JsonProperty("newQuantity")
    private Long newQuantity;
}