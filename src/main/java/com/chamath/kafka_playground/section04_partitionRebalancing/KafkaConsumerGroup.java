package com.chamath.kafka_playground.section04_partitionRebalancing;

//partition re-balancing with multiple consumers
public class KafkaConsumerGroup {

    public static class consumer1{
        public static void main(String[] args) {
            KafkaConsumer.start("1");
        }
    }
    public static class consumer2{
        public static void main(String[] args) {
            KafkaConsumer.start("2");
        }
    }

    public static class consumer3{
        public static void main(String[] args) {
            KafkaConsumer.start("3");
        }
    }

}
