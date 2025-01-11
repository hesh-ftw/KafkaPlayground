package com.chamath.kafka_playground.section06_batch_parallel_process;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;


public class KafkaProducer {


public static final Logger log= LoggerFactory.getLogger(KafkaProducer.class);

public static void main(String[] args) {


    var producerConfig=Map.<String,Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    );
    var options= SenderOptions.<String, String>create(producerConfig);


    //producer 100 items as flux
    var flux= Flux.range(1,100)
            .map(i-> new ProducerRecord<>("section6-topic", i.toString(), "order-" +i))
            .map(pr-> SenderRecord.create(pr,pr.key()));

    var sender =KafkaSender.create(options);
            sender.send(flux)
                    .doOnNext(r->log.info(" correlation id {}", r.correlationMetadata()))
                    .then(Mono.delay(Duration.ofSeconds(1)))
                    .subscribe();
        }
}
