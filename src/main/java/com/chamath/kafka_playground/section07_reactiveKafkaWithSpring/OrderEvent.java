package com.chamath.kafka_playground.section07_reactiveKafkaWithSpring;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderEvent(
        UUID orderId,
        Long customerId,
        LocalDateTime orderDate
){}

