package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class.getName());

    public static void main(String[] args) {
        String topicName = "file-topic"; // 토픽 이름
        File file = new File("C:\\0. inflearn\\kafka-proj1\\practice\\src\\main\\resources\\pizza_append.txt"); // ✅ 파일 객체 생성

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ✅ kafkaProducer 객체 생성 -> ProducerRecord 생성 -> kafkaProducer.send() 비동기 방식 전송 -> kafkaProducer.close();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        boolean sync = false;

        // EventHandler / FileEventSource / FileEventSourceThread 객체 생성
        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        FileEventSource fileEventSource = new FileEventSource(1000, file, eventHandler);

        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();  // ✅ 계속 모니터링

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            kafkaProducer.close();
        }

    }

}
