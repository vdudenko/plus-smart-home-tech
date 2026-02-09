package ru.yandex.practicum.shoppingcart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.shoppingcart.model.CartItem;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import java.util.UUID;
import java.util.List;

@Repository
public interface CartItemRepository extends JpaRepository<CartItem, Long> {
    List<CartItem> findByCartId(UUID cartId); // ← UUID вместо Long

    @Modifying
    @Query("DELETE FROM CartItem c WHERE c.cart.id = :cartId")
    void deleteByCartId(UUID cartId); // ← UUID вместо Long

    @Modifying
    @Query("DELETE FROM CartItem c WHERE c.cart.id = :cartId AND c.productId = :productId")
    void deleteByCartIdAndProductId(UUID cartId, UUID productId);

    void deleteByCartIdAndProductIdIn(UUID cartId, List<UUID> productIds);
}
