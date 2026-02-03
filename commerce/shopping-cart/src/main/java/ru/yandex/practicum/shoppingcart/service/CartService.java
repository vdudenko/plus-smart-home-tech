package ru.yandex.practicum.shoppingcart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.client.circuitbreaker.CircuitBreaker;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
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

@Service
@RequiredArgsConstructor
@Slf4j
public class CartService {
    private final CartRepository cartRepository;
    private final CartItemRepository cartItemRepository;
    private final CartMapper cartMapper;
    private final WarehouseClient warehouseClient;
    private final CircuitBreakerFactory circuitBreakerFactory;

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

        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("warehouse-check");
        Map<Long, Integer> availability = circuitBreaker.run(
                () -> warehouseClient.checkProductsAvailability(
                        ProductAvailabilityRequest.builder()
                                .productIds(List.of(itemDto.getProductId()))
                                .build()
                ),
                throwable -> handleWarehouseFailure(itemDto.getProductId())
        );

        Integer availableQuantity = availability.get(itemDto.getProductId());
        if (availableQuantity == null || availableQuantity < itemDto.getQuantity()) {
            throw new InsufficientStockException(String.format(
                    "Insufficient stock for product %d. Available: %d, Requested: %d",
                    itemDto.getProductId(), availableQuantity != null ? availableQuantity : 0,
                    itemDto.getQuantity()));
        }

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

        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("warehouse-check");
        Map<Long, Integer> availability = circuitBreaker.run(
                () -> warehouseClient.checkProductsAvailability(
                        ProductAvailabilityRequest.builder()
                                .productIds(List.of(productId))
                                .build()
                ),
                throwable -> handleWarehouseFailure(productId)
        );

        Integer availableQuantity = availability.get(productId);
        if (availableQuantity == null || availableQuantity < quantity) {
            throw new InsufficientStockException(String.format(
                    "Insufficient stock for product %d. Available: %d, Requested: %d",
                    productId, availableQuantity != null ? availableQuantity : 0, quantity));
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

    private Map<Long, Integer> handleWarehouseFailure(Long productId) {
        log.warn("Warehouse service is unavailable, returning empty availability");
        Map<Long, Integer> emptyMap = new HashMap<>();
        emptyMap.put(productId, 0);
        return emptyMap;
    }

    private Cart createNewCart(String username) {
        Cart cart = Cart.builder()
                .username(username)
                .active(true)
                .build();
        return cartRepository.save(cart);
    }
}
