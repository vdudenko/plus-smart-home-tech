package ru.yandex.practicum.warehouse.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.WarehouseAddress;
import ru.yandex.practicum.warehouse.dto.WarehouseProductDto;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;

import java.security.SecureRandom;
import java.util.Random;

@Component
public class WarehouseProductMapper {
    private static final String[] ADDRESSES = new String[] {"ADDRESS_1", "ADDRESS_2"};
    private static final String CURRENT_ADDRESS = ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

    public WarehouseProductDto toDto(WarehouseProduct product) {
        if (product == null) {
            return null;
        }
        return WarehouseProductDto.builder()
                .id(product.getId())
                .productId(product.getProductId())
                .quantity(product.getQuantity())
                .width(product.getWidth())
                .height(product.getHeight())
                .depth(product.getDepth())
                .weight(product.getWeight())
                .fragile(product.getFragile())
                .build();
    }

    public WarehouseProduct toEntity(WarehouseProductDto dto) {
        if (dto == null) {
            return null;
        }
        return WarehouseProduct.builder()
                .id(dto.getId())
                .productId(dto.getProductId())
                .quantity(dto.getQuantity())
                .width(dto.getWidth())
                .height(dto.getHeight())
                .depth(dto.getDepth())
                .weight(dto.getWeight())
                .fragile(dto.getFragile())
                .build();
    }

    public WarehouseAddress getCurrentWarehouseAddress() {
        return WarehouseAddress.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .apartment(CURRENT_ADDRESS)
                .build();
    }
}
