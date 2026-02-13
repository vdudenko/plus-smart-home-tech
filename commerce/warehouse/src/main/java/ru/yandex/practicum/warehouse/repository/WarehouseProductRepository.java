package ru.yandex.practicum.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface WarehouseProductRepository extends JpaRepository<WarehouseProduct, Long> {
    Optional<WarehouseProduct> findByProductId(UUID productId);
    List<WarehouseProduct> findByProductIdIn(List<UUID> productIds);
    boolean existsByProductId(UUID productId);
}
