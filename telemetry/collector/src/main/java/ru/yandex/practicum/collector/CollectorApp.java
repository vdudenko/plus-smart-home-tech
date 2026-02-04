package ru.yandex.practicum.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CollectorApp {
    public static void main(String[] args) {
        System.setProperty("spring.cloud.config.fail-fast", "false");
        System.setProperty("spring.cloud.config.retry.initial-interval", "1000");
        System.setProperty("spring.cloud.config.retry.max-attempts", "3");

        SpringApplication.run(CollectorApp.class, args);
    }
}
