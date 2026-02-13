package ru.yandex.practicum.shoppingcart.config;

import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JCircuitBreakerFactory;
import org.springframework.context.annotation.Bean;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigBuilder;
import org.springframework.cloud.client.circuitbreaker.Customizer;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Configuration
public class CustomCircuitBreakerConfig {
    @Bean
    public Customizer<Resilience4JCircuitBreakerFactory> defaultCustomizer() {
        return factory -> factory.configureDefault(id -> new Resilience4JConfigBuilder(id)
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        .failureRateThreshold(50) // Размыкать при 50% ошибок
                        .waitDurationInOpenState(Duration.ofSeconds(10)) // Ждать 10 сек перед попыткой
                        .slidingWindowSize(10) // Окно из 10 вызовов для расчёта
                        .minimumNumberOfCalls(5) // Минимум 5 вызовов перед оценкой
                        .permittedNumberOfCallsInHalfOpenState(3) // 3 вызова в полуоткрытом состоянии
                        .automaticTransitionFromOpenToHalfOpenEnabled(true) // Автоматический переход
                        .build())
                .timeLimiterConfig(TimeLimiterConfig.custom()
                        .timeoutDuration(Duration.ofSeconds(3)) // Таймаут 3 сек
                        .build())
                .build());
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
