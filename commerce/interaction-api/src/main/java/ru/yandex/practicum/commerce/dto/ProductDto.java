package ru.yandex.practicum.commerce.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDto {
    private Long id;
    private String name;
    private String description;
    private ProductCategory category;
    private List<String> images;
    private AvailabilityStatus availability;
    private ProductStatus status;
    private Double price;
}
