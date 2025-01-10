package com.chamath.kafka_playground.section04_partitionRebalancing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class KafkaConsumer {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void start(String instanceId) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-cg1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // Receive messages from the beginning
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId
        );

        // Subscribe to order-events topic
        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .subscription(List.of("partitioned-service"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> {
                    log.info("key: {}, value: {}", r.key(), r.value());

                    // Logging headers
                    r.headers().forEach(h ->
                            log.info("header key: {}, value: {}", h.key(), new String(h.value()))
                    );

                    // Acknowledge offset
                    r.receiverOffset().acknowledge();
                })
                .subscribe();
    }
}
