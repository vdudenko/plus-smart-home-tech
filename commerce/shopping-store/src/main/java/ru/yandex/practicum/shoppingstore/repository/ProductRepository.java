package ru.yandex.practicum.shoppingstore.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.shoppingstore.model.Product;
import ru.yandex.practicum.commerce.dto.ProductCategory;
import ru.yandex.practicum.commerce.dto.ProductStatus;

import java.util.List;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {
    List<Product> findByCategoryAndStatus(ProductCategory category, ProductStatus status);
    List<Product> findByStatus(ProductStatus status);
}
