package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * If you wanna make KafkaProducer, ProducerRecord to Integer,
 * you have to declare serializer.class with IntegerSerializer.class
 */
// KafkaProducer Configuration Settings
public class ProducerAsyncCustomCallback {

    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCallback.class.getName());

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); // [1] bootstrap.servers | WHY server"s"? -> Cause we can use multi brokers(servers)
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName()); // [2] key.serializer.class
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // [3] value.serializer.class

        // Create KafkaProducer Object
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {

            // Create ProducerRecord Object
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world" + seq);
            Callback callback = new CustomCallback(seq);

            // [2] Send KafkaProducer Message (Java Lambda)
            kafkaProducer.send(producerRecord, callback);
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();

    }

}
