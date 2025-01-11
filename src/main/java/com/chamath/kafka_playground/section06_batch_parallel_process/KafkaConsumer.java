package com.chamath.kafka_playground.section06_batch_parallel_process;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


public class KafkaConsumer {

    public static final Logger log= LoggerFactory.getLogger(KafkaConsumer.class);


    public static void main(String[] args) {

        var consumerConfig= Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "inventory-service-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", //receive messages from beginning in the topic
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3
        );


        var options= ReceiverOptions.create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(Pattern.compile("section6-topic"));

        //BATCH PROCESS-sequential

        //receive events as batches and auto acknowledge the batches that already received
        KafkaReceiver.create(options)
                .receiveAutoAck()
                .concatMap(KafkaConsumer::batchProcess) //receive one flux after another
                .log()
                .subscribe();


        /* -------- FOR PARALLEL PROCESS ---------


        KafkaReceiver.create(options)
                .receiveAutoAck()
                .flatMap(KafkaConsumer::batchProcess, 256) //receive all flux in parallel in one second
                .log()
                .subscribe();


         */

    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object,Object>> flux){
        return flux
                .doFirst(()-> log.info("--------------------"))
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .then(Mono.delay(Duration.ofSeconds(1))) //reduce the delay of consumer acknowledgement
                .then();
    }
}
