package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * If you wanna make KafkaProducer, ProducerRecord to Integer,
 * you have to declare serializer.class with IntegerSerializer.class
 */
// KafkaProducer Configuration Settings
public class SimpleProducer {
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

        // Send KafkaProducer Message
        kafkaProducer.send(producerRecord);

        // Terminate KafkaProducer
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
