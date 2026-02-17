package ru.yandex.practicum.warehouse.provider;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.WarehouseAddress;

import java.security.SecureRandom;

@Component
public class WarehouseAddressProvider {

    private final String currentAddress;
    private static final String[] ADDRESSES = {"ADDRESS_1", "ADDRESS_2"};

    public WarehouseAddressProvider() {
        this.currentAddress = ADDRESSES[new SecureRandom().nextInt(ADDRESSES.length)];
    }

    public WarehouseAddress getCurrentWarehouseAddress() {
        return WarehouseAddress.builder()
                .country(currentAddress)
                .city(currentAddress)
                .street(currentAddress)
                .house(currentAddress)
                .flat(currentAddress) // Исправлено: соответствует полю в DTO
                .build();
    }
}