package com.example.producer.service;

import com.example.avro.EventType;
import com.example.avro.UserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserEventProducerService {

    private final KafkaTemplate<String, UserEvent> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    public CompletableFuture<SendResult<String, UserEvent>> send(
            String userId,
            EventType eventType,
            Map<String, String> metadata) {

        UserEvent event = UserEvent.newBuilder()
                .setUserId(userId)
                .setEventType(eventType)
                .setTimestamp(Instant.now().toString())
                .setMetadata(metadata)
                .build();

        log.debug("→ Sending: topic={} key={} eventType={}", topic, userId, eventType);

        CompletableFuture<SendResult<String, UserEvent>> future =
                kafkaTemplate.send(topic, userId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("✗ FAILED | userId={} eventType={} error={}",
                        userId, eventType, ex.getMessage());
            } else {
                log.info("✓ Sent   | topic={} partition={} offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }
}
