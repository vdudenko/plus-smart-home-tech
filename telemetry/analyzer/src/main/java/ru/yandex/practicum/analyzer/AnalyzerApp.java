package ru.yandex.practicum.analyzer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.analyzer.processor.SnapshotProcessor;

@SpringBootApplication
public class AnalyzerApp {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApp.class, args);

        HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        // Запускаем HubEventProcessor в отдельном потоке
        Thread hubThread = new Thread(hubEventProcessor, "HubEventProcessor");
        hubThread.setDaemon(false);  // Позволяет JVM остаться живой
        hubThread.start();

        // Запускаем SnapshotProcessor в основном потоке
        snapshotProcessor.start();

        // Ожидаем завершения hubThread при graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            hubEventProcessor.shutdown();  // Реализуйте shutdown() метод
            try {
                hubThread.join(5000);  // Ждем завершения с таймаутом
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}