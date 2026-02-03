package ru.yandex.practicum.shoppingcart.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.shoppingcart.dto.CartDto;
import ru.yandex.practicum.shoppingcart.dto.CartItemDto;
import ru.yandex.practicum.shoppingcart.model.Cart;
import ru.yandex.practicum.shoppingcart.model.CartItem;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class CartMapper {
    public CartDto toDto(Cart cart, List<CartItem> items) {
        if (cart == null) {
            return null;
        }
        return CartDto.builder()
                .id(cart.getId())
                .username(cart.getUsername())
                .active(cart.getActive())
                .items(items.stream().map(this::toItemDto).collect(Collectors.toList()))
                .build();
    }

    public CartItemDto toItemDto(CartItem item) {
        if (item == null) {
            return null;
        }
        return CartItemDto.builder()
                .productId(item.getProductId())
                .quantity(item.getQuantity())
                .build();
    }

    public Cart toEntity(CartDto dto) {
        if (dto == null) {
            return null;
        }
        return Cart.builder()
                .id(dto.getId())
                .username(dto.getUsername())
                .active(dto.getActive())
                .build();
    }

    public CartItem toItemEntity(CartItemDto dto, Cart cart) {
        if (dto == null || cart == null) {
            return null;
        }
        return CartItem.builder()
                .cart(cart)
                .productId(dto.getProductId())
                .quantity(dto.getQuantity())
                .build();
    }
}
