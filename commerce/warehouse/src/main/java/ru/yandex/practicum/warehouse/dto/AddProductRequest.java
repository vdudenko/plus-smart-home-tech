package ru.yandex.practicum.warehouse.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AddProductRequest {
    private Long productId;
    private Integer quantity;
    private Double width;
    private Double height;
    private Double depth;
    private Double weight;
    private Boolean fragile;
}
