package ru.yandex.practicum.shoppingcart.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import ru.yandex.practicum.shoppingcart.dto.ShoppingCartDto;
import ru.yandex.practicum.shoppingcart.model.Cart;
import ru.yandex.practicum.shoppingcart.model.CartItem;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Mapper(componentModel = "spring")
public interface CartMapper {
    @Mapping(source = "items", target = "products", qualifiedByName = "cartItemsToProductMap")
    ShoppingCartDto toDto(Cart cart, List<CartItem> items);

    @Named("cartItemsToProductMap")
    default Map<UUID, Long> mapCartItemsToProductMap(List<CartItem> items) {
        if (items == null || items.isEmpty()) {
            return Collections.emptyMap();
        }

        return items.stream()
                .filter(item -> item.getProductId() != null)
                .collect(Collectors.toMap(
                        CartItem::getProductId,
                        CartItem::getQuantity,
                        (existing, replacement) -> existing
                ));
    }
}