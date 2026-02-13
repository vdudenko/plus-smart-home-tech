package ru.yandex.practicum.warehouse.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductAvailabilityResponse {
    private Map<Long, Integer> availability;
}
