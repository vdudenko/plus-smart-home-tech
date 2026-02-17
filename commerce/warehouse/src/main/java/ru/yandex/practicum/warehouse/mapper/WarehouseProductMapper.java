package ru.yandex.practicum.warehouse.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import ru.yandex.practicum.commerce.dto.DimensionDto;
import ru.yandex.practicum.commerce.dto.NewProductInWarehouseRequest;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;

@Mapper(componentModel = "spring")
public interface WarehouseProductMapper {

    WarehouseProductMapper INSTANCE = Mappers.getMapper(WarehouseProductMapper.class);

    @Mapping(source = "productId", target = "productId")
    @Mapping(source = "dimension.width", target = "width")   // width из вложенного объекта
    @Mapping(source = "dimension.height", target = "height") // height из вложенного объекта
    @Mapping(source = "dimension.depth", target = "depth")   // depth из вложенного объекта
    @Mapping(source = "weight", target = "weight")
    @Mapping(source = "fragile", target = "fragile")
    WarehouseProduct toEntity(NewProductInWarehouseRequest request);

    @Mapping(source = "productId", target = "productId")
    @Mapping(source = "width", target = "dimension.width")
    @Mapping(source = "height", target = "dimension.height")
    @Mapping(source = "depth", target = "dimension.depth")
    @Mapping(source = "weight", target = "weight")
    @Mapping(source = "fragile", target = "fragile")
    NewProductInWarehouseRequest toDto(WarehouseProduct warehouseProduct);

    default DimensionDto mapDimension(Double width, Double height, Double depth) {
        return DimensionDto.builder()
                .width(width)
                .height(height)
                .depth(depth)
                .build();
    }
}