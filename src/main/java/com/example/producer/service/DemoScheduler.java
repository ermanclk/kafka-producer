package com.example.producer.service;

import com.example.avro.EventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Uygulama başladığında ve her 10 saniyede bir test mesajları gönderir.
 * Gerçek projede bu sınıfı kaldırabilirsiniz.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DemoScheduler {

    private final UserEventProducerService producer;
    private final Random rnd = new Random();

    private static final List<String> USERS = List.of("user-001", "user-002", "user-003");

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() throws InterruptedException {
        log.info("=== Demo: startup events gönderiliyor ===");
        producer.send("user-001", EventType.LOGIN,       Map.of("ip", "192.168.1.10", "browser", "Chrome"));
        Thread.sleep(200);
        producer.send("user-002", EventType.PAGE_VIEW,   Map.of("page", "/dashboard"));
        Thread.sleep(200);
        producer.send("user-001", EventType.PURCHASE,    Map.of("item", "laptop", "price", "1299.99"));
        Thread.sleep(200);
        producer.send("user-003", EventType.ADD_TO_CART, Map.of("item", "mouse", "qty", "2"));
        Thread.sleep(200);
        producer.send("user-002", EventType.LOGOUT,      Map.of("item", "mouse", "qty", "2"));
        log.info("=== Demo: startup events gönderildi ===");
    }

    @Scheduled(fixedDelay = 10_000, initialDelay = 6_000)
    public void scheduledEvent() {
        String userId = USERS.get(rnd.nextInt(USERS.size()));
        EventType type = EventType.values()[rnd.nextInt(EventType.values().length)];
        Map<String, String> meta = switch (type) {
            case LOGIN       -> Map.of("ip", "10.0.0." + rnd.nextInt(255));
            case PURCHASE    -> Map.of("item", "item-" + rnd.nextInt(100), "price", String.valueOf(rnd.nextInt(999) + 1));
            case ADD_TO_CART -> Map.of("item", "item-" + rnd.nextInt(50),  "qty",   String.valueOf(rnd.nextInt(5) + 1));
            case PAGE_VIEW   -> Map.of("page", "/page-" + rnd.nextInt(20));
            case LOGOUT      -> Map.of();
        };
        producer.send(userId, type, meta);
    }
}
