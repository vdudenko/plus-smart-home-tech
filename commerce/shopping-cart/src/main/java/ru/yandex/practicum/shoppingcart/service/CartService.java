package ru.yandex.practicum.shoppingcart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.shoppingcart.exception.CartNotFoundException;
import ru.yandex.practicum.shoppingcart.exception.InsufficientStockException;
import ru.yandex.practicum.shoppingcart.feign.WarehouseClient;
import ru.yandex.practicum.shoppingcart.mapper.CartMapper;
import ru.yandex.practicum.shoppingcart.model.Cart;
import ru.yandex.practicum.shoppingcart.model.CartItem;
import ru.yandex.practicum.shoppingcart.repository.CartItemRepository;
import ru.yandex.practicum.shoppingcart.repository.CartRepository;
import ru.yandex.practicum.shoppingcart.dto.CartDto;
import ru.yandex.practicum.shoppingcart.dto.CartItemDto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class CartService {
    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final CartMapper cartMapper;
    private final WarehouseClient warehouseClient;

    @Transactional(readOnly = true)
    public CartDto getCart(String username) {
        Cart cart = cartRepository.findByUsernameAndActiveTrue(username)
                .orElseThrow(() -> new CartNotFoundException("Active cart not found for user: " + username));

        List<CartItem> items = cartItemRepository.findByCartId(cart.getId());
        return cartMapper.toDto(cart, items);
    }

    @Transactional
    public CartDto addToCart(String username, CartItemDto itemDto) {
        Cart cart = cartRepository.findByUsernameAndActiveTrue(username)
                .orElseGet(() -> createNewCart(username));

        // Проверяем доступность товара на складе через /check эндпоинт
        Map<UUID, Long> products = new HashMap<>();
        products.put(UUID.fromString(itemDto.getProductId().toString()), itemDto.getQuantity().longValue());

        ShoppingCartDto shoppingCartDto = ShoppingCartDto.builder()
                .shoppingCartId(UUID.randomUUID())
                .products(products)
                .build();

        try {
            BookedProductsDto bookedProducts = warehouseClient.checkProductQuantityEnoughForShoppingCart(shoppingCartDto);
            log.info("Cart check passed. Delivery weight: {}, volume: {}, fragile: {}",
                    bookedProducts.getDeliveryWeight(), bookedProducts.getDeliveryVolume(), bookedProducts.getFragile());
        } catch (Exception e) {
            throw new InsufficientStockException("Insufficient stock for requested products: " + e.getMessage());
        }

        // Добавляем товар в корзину
        List<CartItem> existingItems = cartItemRepository.findByCartId(cart.getId());
        CartItem existingItem = existingItems.stream()
                .filter(item -> item.getProductId().equals(itemDto.getProductId()))
                .findFirst()
                .orElse(null);

        if (existingItem != null) {
            existingItem.setQuantity(existingItem.getQuantity() + itemDto.getQuantity());
            cartItemRepository.save(existingItem);
        } else {
            CartItem newItem = cartMapper.toItemEntity(itemDto, cart);
            cartItemRepository.save(newItem);
        }

        log.info("Added {} units of product {} to cart for user {}",
                itemDto.getQuantity(), itemDto.getProductId(), username);

        List<CartItem> updatedItems = cartItemRepository.findByCartId(cart.getId());
        return cartMapper.toDto(cart, updatedItems);
    }

    @Transactional
    public CartDto updateCartItem(String username, Long productId, Integer quantity) {
        Cart cart = cartRepository.findByUsernameAndActiveTrue(username)
                .orElseThrow(() -> new CartNotFoundException("Active cart not found for user: " + username));

        Map<UUID, Long> products = new HashMap<>();
        products.put(UUID.fromString(productId.toString()), quantity.longValue());

        ShoppingCartDto shoppingCartDto = ShoppingCartDto.builder()
                .shoppingCartId(UUID.randomUUID())
                .products(products)
                .build();

        try {
            BookedProductsDto bookedProducts = warehouseClient.checkProductQuantityEnoughForShoppingCart(shoppingCartDto);
            log.info("Cart check passed for update. Delivery weight: {}, volume: {}, fragile: {}",
                    bookedProducts.getDeliveryWeight(), bookedProducts.getDeliveryVolume(), bookedProducts.getFragile());
        } catch (Exception e) {
            throw new InsufficientStockException("Insufficient stock for requested products: " + e.getMessage());
        }

        List<CartItem> items = cartItemRepository.findByCartId(cart.getId());
        CartItem itemToUpdate = items.stream()
                .filter(item -> item.getProductId().equals(productId))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Product not found in cart: " + productId));

        itemToUpdate.setQuantity(quantity);
        cartItemRepository.save(itemToUpdate);

        log.info("Updated quantity of product {} to {} in cart for user {}", productId, quantity, username);

        List<CartItem> updatedItems = cartItemRepository.findByCartId(cart.getId());
        return cartMapper.toDto(cart, updatedItems);
    }

    @Transactional
    public void deactivateCart(String username) {
        Cart cart = cartRepository.findByUsernameAndActiveTrue(username)
                .orElseThrow(() -> new CartNotFoundException("Active cart not found for user: " + username));

        cart.setActive(false);
        cartRepository.save(cart);
        log.info("Cart deactivated for user {}", username);
    }

    private Cart createNewCart(String username) {
        Cart cart = Cart.builder()
                .username(username)
                .active(true)
                .build();
        return cartRepository.save(cart);
    }
}
