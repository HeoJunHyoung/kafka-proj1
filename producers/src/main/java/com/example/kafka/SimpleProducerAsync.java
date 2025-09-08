package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 * If you wanna make KafkaProducer, ProducerRecord to Integer,
 * you have to declare serializer.class with IntegerSerializer.class
 */
// KafkaProducer Configuration Settings
public class SimpleProducerAsync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // [1] bootstrap.servers | WHY server"s"? -> Cause we can use multi brokers(servers)
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // [2] key.serializer.class
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // [3] value.serializer.class

        // Create KafkaProducer Object
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // Create ProducerRecord Object
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "id-001", "hello world 2");

        // [1] Send KafkaProducer Message (Java Basic)
//        kafkaProducer.send(producerRecord, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                if (exception == null) {
//                    logger.info("\n ###### record metadata received ##### \n" +
//                            "partition: " + metadata.partition() + "\n" +
//                            "offset: " + metadata.offset() + "\n" +
//                            "timestamp: " + metadata.timestamp());
//                } else {
//                    logger.error("exception error from broker" + exception.getMessage());
//                }
//            }
//        });

        // [2] Send KafkaProducer Message (Java Lambda)
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("\n ###### record metadata received ##### \n" +
                        "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset() + "\n" +
                        "timestamp: " + metadata.timestamp());
            } else {
                logger.error("exception error from broker" + exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();

    }
}
