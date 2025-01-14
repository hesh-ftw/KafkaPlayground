package com.chamath.kafka_playground.section07_reactiveKafkaWithSpring;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;


@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String,OrderEvent> receiverOptions(KafkaProperties kafkaProperties){
        return ReceiverOptions.<String,OrderEvent>create(kafkaProperties.buildConsumerProperties())
                .consumerProperty(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS,"false")
                .subscription(List.of("order-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, OrderEvent> consumerTemplate(ReceiverOptions<String,OrderEvent> receiverOptions){
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions);
    }

}
