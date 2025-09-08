package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;


/**
 * If you wanna make KafkaProducer, ProducerRecord to Integer,
 * you have to declare serializer.class with IntegerSerializer.class
 */
// KafkaProducer Configuration Settings
public class PizzaProducer {

    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());


    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer, String topicName, int iterCount,
                                        int interIntervalMillis, int intervalMillis, int intervalCount, Boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;

        // seed값을 고정하여 Random 객체와 Faker 객체를 생성.
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));

            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("###### intervalCount:" + intervalCount + " intervalMillis" + intervalMillis + "######");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis: " + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }

    }

    // 동기, 비동기 분기처리
    public static void sendMessage(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync) {

        if (!sync) { // ASYNC (비동기)
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message: " + pMessage.get("key") + "partition: " + metadata.partition() + "\n" +
                            "offset: " + metadata.offset());
                } else {
                    logger.error("exception error from broker" + exception.getMessage());
                }
            });
        } else { // SYNC (동기)
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("async message: " + pMessage.get("key") + "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

    }


    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // [1] bootstrap.servers | WHY server"s"? -> Cause we can use multi brokers(servers)
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // [2] key.serializer.class
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // [3] value.serializer.class
//        props.setProperty(ProducerConfig.ACKS_CONFIG, "0"); // [4] props 설정
//        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000"); // [5] Batch Size 설정
//        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // [6] linger.ms 설정
//        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000");


        // Create KafkaProducer Object
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName, -1, 10,
                100, 100, false);

        kafkaProducer.close();

    }

}
