package ru.yandex.practicum.warehouse.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.WarehouseAddress;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;

import java.security.SecureRandom;
import java.util.Random;

@Component
public class WarehouseProductMapper {
    private static final String[] ADDRESSES = new String[] {"ADDRESS_1", "ADDRESS_2"};
    private static final String CURRENT_ADDRESS = ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

    public WarehouseProduct toEntity(ru.yandex.practicum.commerce.dto.NewProductInWarehouseRequest request) {
        return WarehouseProduct.builder()
                .productId(request.getProductId())
                .quantity(0L) // Начальное количество = 0
                .width(request.getDimension().getWidth())
                .height(request.getDimension().getHeight())
                .depth(request.getDimension().getDepth())
                .weight(request.getWeight())
                .fragile(request.getFragile())
                .build();
    }

    public WarehouseAddress getCurrentWarehouseAddress() {
        return WarehouseAddress.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .flat(CURRENT_ADDRESS) // ИСПРАВЛЕНО: было apartment
                .build();
    }
}
