package ru.yandex.practicum.shoppingstore.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.shoppingstore.exception.ProductNotFoundException;
import ru.yandex.practicum.shoppingstore.service.ProductService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-store") // ИЗМЕНЕНО: новый базовый путь
@RequiredArgsConstructor
public class ProductController {
    private final ProductService productService;

    @GetMapping
    public ResponseEntity<Page<ProductDto>> getProducts(
            @RequestParam ProductCategory category,
            @PageableDefault(page = 0, size = 20) Pageable pageable) {
        return ResponseEntity.ok(productService.getProductsByCategory(category, pageable));
    }

    @PutMapping
    public ResponseEntity<ProductDto> createNewProduct(@RequestBody ProductDto productDto) {
        return ResponseEntity.ok(productService.createProduct(productDto));
    }

    @PostMapping
    public ResponseEntity<ProductDto> updateProduct(@RequestBody ProductDto productDto) {
        return ResponseEntity.ok(productService.updateProduct(productDto));
    }

    @PostMapping("/removeProductFromStore")
    public ResponseEntity<Boolean> removeProductFromStore(@RequestBody UUID productId) {
        return ResponseEntity.ok(productService.removeProductFromStore(productId));
    }

    @PostMapping("/quantityState")
    public ResponseEntity<Boolean> setProductQuantityState(@RequestBody SetProductQuantityStateRequest request) {
        return ResponseEntity.ok(productService.setProductQuantityState(request));
    }

    @GetMapping("/{productId}")
    public ResponseEntity<ProductDto> getProduct(@PathVariable UUID productId) {
        return ResponseEntity.ok(productService.getProductById(productId));
    }

    // Обработчик исключений
    @ExceptionHandler(ProductNotFoundException.class)
    public ResponseEntity<Void> handleProductNotFound(ProductNotFoundException ex) {
        return ResponseEntity.notFound().build();
    }
}