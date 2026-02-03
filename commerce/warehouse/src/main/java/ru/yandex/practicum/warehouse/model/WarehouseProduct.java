package ru.yandex.practicum.warehouse.model;

import jakarta.persistence.Entity;
import jakarta.persistence.*;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Entity
@Table(name = "warehouse_products")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseProduct {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private Long productId;

    @Column(nullable = false)
    private Integer quantity;

    @Column(nullable = false)
    private BigDecimal width;

    @Column(nullable = false)
    private BigDecimal height;

    @Column(nullable = false)
    private BigDecimal depth;

    @Column(nullable = false, precision = 10, scale = 2)
    private BigDecimal weight;

    @Column(nullable = false)
    private Boolean fragile;
}
