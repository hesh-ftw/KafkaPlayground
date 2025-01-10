package com.chamath.kafka_playground.section05_seekOffset;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;



        // seeking offset of partitioned topic

public class KafkaConsumer {

    public static final Logger log= LoggerFactory.getLogger(KafkaConsumer.class);


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

                .addAssignListener( c-> {
                    c.forEach(r -> log.info("assigned {} ", r.position())); //shows offset of all 3 partitions in the topic
                    c.forEach(r -> r.seek(r.position() - 2)); //shows offset of last 2 events of all 3 partitions

                    //search last 3 events of the partiton no 2
                    c.stream()
                            .filter(r-> r.topicPartition().partition()== 2)
                            .findFirst()
                            .ifPresent(r-> r.seek(r.position() - 3));
                })
                .subscription(List.of("partitioned-service"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))

                /* here the offset will be updated everytime the consumer consumes events,
                 so already committed events won't be shown in next reload - only the new events can be seen  */
                .doOnNext(r-> r.receiverOffset().acknowledge())

                .subscribe();

    }
}
