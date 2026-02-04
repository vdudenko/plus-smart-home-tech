package ru.yandex.practicum.shoppingcart.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.shoppingcart.dto.ShoppingCartDto;
import ru.yandex.practicum.shoppingcart.model.Cart;
import ru.yandex.practicum.shoppingcart.model.CartItem;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class CartMapper {
    public ShoppingCartDto toDto(Cart cart, List<CartItem> items) {
        if (cart == null) {
            return null;
        }

        Map<java.util.UUID, Long> products = items.stream()
                .collect(Collectors.toMap(
                        CartItem::getProductId,
                        CartItem::getQuantity
                ));

        return ShoppingCartDto.builder()
                .shoppingCartId(cart.getId())
                .products(products)
                .build();
    }
}