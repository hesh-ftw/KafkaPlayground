package com.chamath.kafka_playground.section04_partitionRebalancing;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

//partition re-balancing ensure that a topic has multiple partition
public class KafkaProducer {


public static final Logger log= LoggerFactory.getLogger(KafkaProducer.class);

public static void main(String[] args) {


    var producerConfig=Map.<String,Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    );

    var options= SenderOptions.<String, String>create(producerConfig);

    var flux= Flux.interval(Duration.ofMillis(50))
            .take(10_000)
            .map(i-> new ProducerRecord<>("partitioned-service", i.toString(), "order-" +i))
            .map(pr-> SenderRecord.create(pr,pr.key()));

    KafkaSender.create(options)
            .send(flux)
            .doOnNext(r->log.info(" correlation id {}", r.correlationMetadata()))
            .subscribe();


}
}
