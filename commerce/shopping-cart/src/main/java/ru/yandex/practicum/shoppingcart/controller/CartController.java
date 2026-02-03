package ru.yandex.practicum.shoppingcart.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.shoppingcart.dto.CartDto;
import ru.yandex.practicum.shoppingcart.dto.CartItemDto;
import ru.yandex.practicum.shoppingcart.service.CartService;

@RestController
@RequestMapping("/api/carts")
@RequiredArgsConstructor
public class CartController {
    private final CartService cartService;

    @GetMapping
    public ResponseEntity<CartDto> getCart(@RequestParam String username) {
        return ResponseEntity.ok(cartService.getCart(username));
    }

    @PostMapping
    public ResponseEntity<CartDto> addToCart(
            @RequestParam String username,
            @RequestBody CartItemDto itemDto) {
        return ResponseEntity.ok(cartService.addToCart(username, itemDto));
    }

    @PatchMapping("/items/{productId}")
    public ResponseEntity<CartDto> updateCartItem(
            @RequestParam String username,
            @PathVariable Long productId,
            @RequestParam Integer quantity) {
        return ResponseEntity.ok(cartService.updateCartItem(username, productId, quantity));
    }

    @PatchMapping("/deactivate")
    public ResponseEntity<Void> deactivateCart(@RequestParam String username) {
        cartService.deactivateCart(username);
        return ResponseEntity.ok().build();
    }
}
