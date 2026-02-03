package ru.yandex.practicum.shoppingstore.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.shoppingstore.model.Product;

@Component
public class ProductMapper {
    public ProductDto toDto(Product product) {
        if (product == null) {
            return null;
        }
        return ProductDto.builder()
                .id(product.getId())
                .name(product.getName())
                .description(product.getDescription())
                .category(product.getCategory())
                .images(product.getImages())
                .availability(product.getAvailability())
                .status(product.getStatus())
                .price(product.getPrice())
                .build();
    }

    public Product toEntity(ProductDto dto) {
        if (dto == null) {
            return null;
        }
        return Product.builder()
                .id(dto.getId())
                .name(dto.getName())
                .description(dto.getDescription())
                .category(dto.getCategory())
                .images(dto.getImages())
                .availability(dto.getAvailability())
                .status(dto.getStatus())
                .price(dto.getPrice())
                .build();
    }
}
