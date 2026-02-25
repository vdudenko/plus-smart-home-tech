package ru.yandex.practicum.shoppingstore.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.shoppingstore.model.Product;

@Mapper(componentModel = "spring")
public interface ProductMapper {
    @Mapping(source = "id", target = "productId")
    ProductDto toDto(Product product);

    @Mapping(source = "productId", target = "id")
    Product toEntity(ProductDto dto);
}