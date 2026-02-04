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
public class ProductDto {
    @JsonProperty("productId")
    private UUID productId;

    @JsonProperty("productName")
    private String productName;

    @JsonProperty("description")
    private String description;

    @JsonProperty("imageSrc")
    private String imageSrc;

    @JsonProperty("quantityState")
    private QuantityState quantityState;

    @JsonProperty("productState")
    private ProductState productState;

    @JsonProperty("productCategory")
    private ProductCategory productCategory;

    @JsonProperty("price")
    private Double price;
}