package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTopicRebalance {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerMTopicRebalance.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_assign");

        // ✅✅파티션 할당 전략 명시적으로 Round Robin으로 설정✅✅
//        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        // ✅✅파티션 할당 전략 명시적으로 Cooperative Sticky로 설정✅✅
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        // kafka consumer 객체 생성
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // subscribe
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        // Main Thread 선언
        Thread mainThread = Thread.currentThread();

        // Runtime.getRuntime().addShutdownHook: Main Thread 종료 전 실행
        // kafkaConsumer.wakeup(): poll 시점에 Exception 발생 용도
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                // Main Thread 죽을 때 같이 죽어야 해서 대기 상태로 놔야함
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    logger.info("topic:{}, record key={}, partition={}, record offset={}, record value={}",
                            consumerRecord.topic(), consumerRecord.key(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

}
