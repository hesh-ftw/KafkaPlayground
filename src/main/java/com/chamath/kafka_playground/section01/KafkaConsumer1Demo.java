package com.chamath.kafka_playground.section01;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;



        // simple kafka consumer demo using reactor kafka lib.

public class KafkaConsumer1Demo {

    public static final Logger log= LoggerFactory.getLogger(KafkaConsumer1Demo.class);


    public static void main(String[] args) {

        var consumerConfig= Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-cg1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", //receive messages from beginning in the topic
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );


        //subscribe to order-events topic to receive msgs.
        var options= ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))

                /* here the offset will be updated everytime the consumer consumes events,
                 so already committed events won't be shown in next reload - only the new events can be seen  */
                .doOnNext(r-> r.receiverOffset().acknowledge())

                .subscribe();

    }
}
