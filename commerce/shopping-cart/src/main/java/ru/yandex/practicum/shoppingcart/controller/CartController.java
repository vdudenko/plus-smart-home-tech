package ru.yandex.practicum.shoppingcart.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.shoppingcart.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.shoppingcart.dto.ShoppingCartDto;
import ru.yandex.practicum.shoppingcart.exception.NotAuthorizedUserException;
import ru.yandex.practicum.shoppingcart.service.CartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class CartController {

    private final CartService cartService;

    @GetMapping
    public ResponseEntity<ShoppingCartDto> getShoppingCart(@RequestParam String username) {
        validateUsername(username);
        return ResponseEntity.ok(cartService.getShoppingCart(username));
    }

    @PutMapping
    public ResponseEntity<ShoppingCartDto> addProductToShoppingCart(
            @RequestParam String username,
            @RequestBody Map<UUID, Long> products) {

        validateUsername(username);
        return ResponseEntity.ok(cartService.addProductsToCart(username, products));
    }

    @DeleteMapping
    public ResponseEntity<Void> deactivateCurrentShoppingCart(@RequestParam String username) {
        validateUsername(username);
        cartService.deactivateCart(username);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/remove")
    public ResponseEntity<ShoppingCartDto> removeFromShoppingCart(
            @RequestParam String username,
            @RequestBody List<UUID> productIds) {

        validateUsername(username);
        return ResponseEntity.ok(cartService.removeProductsFromCart(username, productIds));
    }

    @PostMapping("/change-quantity")
    public ResponseEntity<ShoppingCartDto> changeProductQuantity(
            @RequestParam String username,
            @RequestBody ChangeProductQuantityRequest request) {

        validateUsername(username);
        return ResponseEntity.ok(cartService.changeProductQuantity(username, request));
    }

    private void validateUsername(String username) {
        if (username == null || username.trim().isEmpty()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }
    }
}