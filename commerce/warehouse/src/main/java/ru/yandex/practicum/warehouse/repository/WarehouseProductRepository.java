package ru.yandex.practicum.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.warehouse.model.WarehouseProduct;

import java.util.List;
import java.util.Optional;

public interface WarehouseProductRepository extends JpaRepository<WarehouseProduct, Long> {
    Optional<WarehouseProduct> findByProductId(Long productId);
    List<WarehouseProduct> findByProductIdIn(List<Long> productIds);
    boolean existsByProductId(Long productId);
}
