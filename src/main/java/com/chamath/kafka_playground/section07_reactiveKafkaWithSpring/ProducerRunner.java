package com.chamath.kafka_playground.section07_reactiveKafkaWithSpring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class ProducerRunner implements CommandLineRunner {

    private static final Logger log= LoggerFactory.getLogger(ProducerRunner.class);

    @Autowired
    private ReactiveKafkaProducerTemplate<String,OrderEvent> template;

    @Override
    public void run(String... args) throws Exception {
        this.orderEventFlux()
                .flatMap(oe-> this.template.send("order-events",oe.orderId().toString(),oe))
                .doOnNext(r-> log.info("result : {}", r.correlationMetadata()))
                .subscribe();
    }

    //simple flux for emitting events
    private Flux<OrderEvent> orderEventFlux(){
        return Flux.interval(Duration.ofMillis(500))
                .take(1000)
                .map(i-> new OrderEvent(
                        UUID.randomUUID(),
                        i,
                        LocalDateTime.now()
                ));
    }

}
