package com.example.producer.config;

import com.example.avro.UserEvent;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.config.SerdeConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.apicurio.registry.url}")
    private String registryUrl;

    @Value("${app.kafka.topic}")
    private String topicName;

    @Value("${app.kafka.topic-config.partitions:3}")
    private int partitions;

    @Value("${app.kafka.topic-config.replicas:1}")
    private short replicas;

    @Value("${app.kafka.topic-config.retention-ms:604800000}")
    private String retentionMs;

    @Value("${app.kafka.topic-config.cleanup-policy:delete}")
    private String cleanupPolicy;

    @Value("${app.kafka.topic-config.min-insync-replicas:1}")
    private String minInsyncReplicas;

    @Bean
    public ProducerFactory<String, UserEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Kafka bağlantısı
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Apicurio Registry 3.x — SerdeConfig sabitleri 3.x paketinden
        props.put(SerdeConfig.REGISTRY_URL, registryUrl);
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, true);
        props.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, UserEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public NewTopic userEventsTopic() {
        return new NewTopic(topicName, partitions, replicas)
                .configs(Map.of(
                    "cleanup.policy", cleanupPolicy,
                    "retention.ms", retentionMs,
                    "min.insync.replicas", minInsyncReplicas
                ));
    }
}
