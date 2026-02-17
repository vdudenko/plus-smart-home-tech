package ru.yandex.practicum.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.OrderDto;
import ru.yandex.practicum.delivery.feign.WarehouseFeignClient;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryCostCalculator {

    private static final double BASE_COST = 5.0;
    private static final double FRAGILE_MULTIPLIER = 0.2;
    private static final double WEIGHT_MULTIPLIER = 0.3;
    private static final double VOLUME_MULTIPLIER = 0.2;
    private static final double ADDRESS_MISMATCH_MULTIPLIER = 0.2;
    private static final String WAREHOUSE_ADDRESS_1 = "ADDRESS_1";
    private static final String WAREHOUSE_ADDRESS_2 = "ADDRESS_2";

    private final WarehouseFeignClient warehouseFeignClient;

    @Transactional(readOnly = true)
    public Double calculateDeliveryCost(OrderDto order) {
        AddressDto warehouseAddress = warehouseFeignClient.getWarehouseAddress();
        String warehouseStreet = warehouseAddress.getStreet();

        double cost = calculateBaseCost(warehouseStreet);

        if (Boolean.TRUE.equals(order.getFragile())) {
            cost += cost * FRAGILE_MULTIPLIER;
        }

        if (order.getDeliveryWeight() != null) {
            cost += order.getDeliveryWeight() * WEIGHT_MULTIPLIER;
        }

        if (order.getDeliveryVolume() != null) {
            cost += order.getDeliveryVolume() * VOLUME_MULTIPLIER;
        }

        cost += cost * ADDRESS_MISMATCH_MULTIPLIER;

        log.info("Calculated delivery cost for order {}: {}", order.getOrderId(), cost);
        return cost;
    }

    private double calculateBaseCost(String warehouseStreet) {
        if (warehouseStreet.contains(WAREHOUSE_ADDRESS_1)) {
            return BASE_COST * 1 + BASE_COST;
        } else if (warehouseStreet.contains(WAREHOUSE_ADDRESS_2)) {
            return BASE_COST * 2 + BASE_COST;
        } else {
            return BASE_COST * 1 + BASE_COST;
        }
    }
}