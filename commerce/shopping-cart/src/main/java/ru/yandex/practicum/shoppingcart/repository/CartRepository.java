package ru.yandex.practicum.shoppingcart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.shoppingcart.model.Cart;

import java.util.Optional;

@Repository
public interface CartRepository extends JpaRepository<Cart, Long> {
    Optional<Cart> findByUsernameAndActiveTrue(String username);
    Optional<Cart> findByUsername(String username);
}
