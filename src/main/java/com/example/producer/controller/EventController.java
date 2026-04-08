package com.example.producer.controller;

import com.example.avro.EventType;
import com.example.producer.service.UserEventProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class EventController {

    private final UserEventProducerService producerService;

    /**
     * Genel event gönderme endpoint'i.
     *
     * POST /api/events
     * {
     *   "userId": "user-001",
     *   "eventType": "LOGIN",
     *   "metadata": { "ip": "10.0.0.1" }
     * }
     */
    @PostMapping
    public ResponseEntity<String> send(@RequestBody EventRequest req) {
        EventType type;
        try {
            type = EventType.valueOf(req.eventType().toUpperCase());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body("Geçersiz eventType. Geçerliler: " + Arrays.toString(EventType.values()));
        }
        producerService.send(req.userId(), type, req.metadata() != null ? req.metadata() : Map.of());
        return ResponseEntity.accepted().body("Event kuyruğa alındı");
    }

    // ── Kısayol endpoint'ler ─────────────────────────────────────────────

    @PostMapping("/{userId}/login")
    public ResponseEntity<String> login(
            @PathVariable String userId,
            @RequestParam(defaultValue = "unknown") String ip) {
        producerService.send(userId, EventType.LOGIN, Map.of("ip", ip));
        return ResponseEntity.accepted().body("LOGIN gönderildi");
    }

    @PostMapping("/{userId}/logout")
    public ResponseEntity<String> logout(@PathVariable String userId) {
        producerService.send(userId, EventType.LOGOUT, Map.of());
        return ResponseEntity.accepted().body("LOGOUT gönderildi");
    }
    @PostMapping("/{userId}/purchase")
    public ResponseEntity<String> purchase(
            @PathVariable String userId,
            @RequestParam String item,
            @RequestParam String price) {
        producerService.send(userId, EventType.PURCHASE, Map.of("item", item, "price", price));
        return ResponseEntity.accepted().body("PURCHASE gönderildi");
    }

    @PostMapping("/{userId}/pageview")
    public ResponseEntity<String> pageView(
            @PathVariable String userId,
            @RequestParam String page) {
        producerService.send(userId, EventType.PAGE_VIEW, Map.of("page", page));
        return ResponseEntity.accepted().body("PAGE_VIEW gönderildi");
    }

    // ── Request DTO ──────────────────────────────────────────────────────

    record EventRequest(String userId, String eventType, Map<String, String> metadata) {}
}
