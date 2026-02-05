package ru.yandex.practicum.shoppingcart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.BookedProductsDto;
import ru.yandex.practicum.shoppingcart.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.shoppingcart.exception.NotAuthorizedUserException;
import ru.yandex.practicum.shoppingcart.feign.WarehouseClient;
import ru.yandex.practicum.shoppingcart.mapper.CartMapper;
import ru.yandex.practicum.shoppingcart.model.Cart;
import ru.yandex.practicum.shoppingcart.model.CartItem;
import ru.yandex.practicum.shoppingcart.repository.CartItemRepository;
import ru.yandex.practicum.shoppingcart.repository.CartRepository;
import ru.yandex.practicum.shoppingcart.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.shoppingcart.dto.ShoppingCartDto;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class CartService {
    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final CartMapper cartMapper;
    private final WarehouseClient warehouseClient;

    @Transactional(readOnly = true)
    public ShoppingCartDto getShoppingCart(String username) {
        Cart cart = getActiveCart(username);
        List<CartItem> items = cartItemRepository.findByCartId(cart.getId());
        return cartMapper.toDto(cart, items);
    }

    @Transactional
    public ShoppingCartDto addProductsToCart(String username, Map<UUID, Long> products) {
        Cart cart = getOrCreateCart(username);

        checkProductsAvailability(products);

        List<CartItem> existingItems = cartItemRepository.findByCartId(cart.getId());
        Map<UUID, CartItem> existingMap = existingItems.stream()
                .collect(Collectors.toMap(CartItem::getProductId, item -> item));

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            Long quantity = entry.getValue();

            CartItem item = existingMap.get(productId);
            if (item != null) {
                item.setQuantity(item.getQuantity() + quantity);
                cartItemRepository.save(item);
            } else {
                CartItem newItem = CartItem.builder()
                        .cart(cart)
                        .productId(productId)
                        .quantity(quantity)
                        .build();
                cartItemRepository.save(newItem);
            }
        }

        log.info("Added products to cart for user {}", username);
        return getShoppingCart(username);
    }

    @Transactional
    public void deactivateCart(String username) {
        Cart cart = getActiveCart(username);
        cart.setActive(false);
        cartRepository.save(cart);
        log.info("Cart deactivated for user {}", username);
    }

    @Transactional
    public ShoppingCartDto removeProductsFromCart(String username, List<UUID> productIds) {
        Cart cart = getActiveCart(username);
        List<CartItem> items = cartItemRepository.findByCartId(cart.getId());

        Set<UUID> existingProductIds = items.stream()
                .map(CartItem::getProductId)
                .collect(Collectors.toSet());

        List<UUID> missingProducts = productIds.stream()
                .filter(id -> !existingProductIds.contains(id))
                .collect(Collectors.toList());

        if (!missingProducts.isEmpty()) {
            throw new NoProductsInShoppingCartException(
                    "Нет искомых товаров в корзине: " + missingProducts);
        }

        for (UUID productId : productIds) {
            cartItemRepository.deleteByCartIdAndProductId(cart.getId(), productId);
        }

        log.info("Removed {} products from cart for user {}", productIds.size(), username);
        return getShoppingCart(username);
    }

    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        Cart cart = getActiveCart(username);
        List<CartItem> items = cartItemRepository.findByCartId(cart.getId());

        Optional<CartItem> itemOpt = items.stream()
                .filter(item -> item.getProductId().equals(request.getProductId()))
                .findFirst();

        if (itemOpt.isEmpty()) {
            throw new NoProductsInShoppingCartException(
                    "Товар не найден в корзине: " + request.getProductId());
        }

        Map<UUID, Long> checkMap = new HashMap<>();
        checkMap.put(request.getProductId(), request.getNewQuantity());
        checkProductsAvailability(checkMap);

        CartItem item = itemOpt.get();
        item.setQuantity(request.getNewQuantity());
        cartItemRepository.save(item);

        log.info("Changed quantity for product {} to {} in cart for user {}",
                request.getProductId(), request.getNewQuantity(), username);
        return getShoppingCart(username);
    }

    private Cart getActiveCart(String username) {
        return cartRepository.findByUsernameAndActiveTrue(username)
                .orElseThrow(() -> new NotAuthorizedUserException(
                        "Активная корзина не найдена для пользователя: " + username));
    }

    private Cart getOrCreateCart(String username) {
        return cartRepository.findByUsernameAndActiveTrue(username)
                .orElseGet(() -> {
                    Cart newCart = Cart.builder()
                            .username(username)
                            .active(true)
                            .build();
                    return cartRepository.save(newCart);
                });
    }

    private void checkProductsAvailability(Map<UUID, Long> products) {
        try {
            ru.yandex.practicum.commerce.dto.ShoppingCartDto warehouseCart = ru.yandex.practicum.commerce.dto.ShoppingCartDto.builder()
                    .shoppingCartId(UUID.randomUUID())
                    .products(products)
                    .build();

            BookedProductsDto bookedProducts = warehouseClient.checkProductQuantityEnoughForShoppingCart(warehouseCart);
            log.info("Cart check passed. Delivery weight: {}, volume: {}, fragile: {}",
                    bookedProducts.getDeliveryWeight(), bookedProducts.getDeliveryVolume(), bookedProducts.getFragile());
        } catch (Exception e) {
            throw new NoProductsInShoppingCartException(
                    "Недостаточно товаров на складе: " + e.getMessage());
        }
    }
}