package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "file-topic"; // ✅ 토픽 이름
        String filePath = "C:\\0. inflearn\\kafka-proj1\\practice\\src\\main\\resources\\pizza_sample.txt"; // ✅ 파일 경로

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ✅ kafkaProducer 객체 생성 -> ProducerRecord 생성 -> kafkaProducer.send() 비동기 방식 전송 -> kafkaProducer.close();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();

    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {

        String line = ""; // ✅ 라인 변수 선언
        final String delimiter = ",";

        try {
            FileReader fileReader = new FileReader(filePath); // ✅ 파일 리더 객체 생성
            BufferedReader bufferedReader = new BufferedReader(fileReader); // ✅ 버퍼 리더 객체 생성

            // ✅ 1000번 반복
            while ( (line = bufferedReader.readLine()) != null ) {
                String[] tokens = line.split(delimiter); // ✅ ","을 기준으로 split
                String key = tokens[0]; // ✅ 키 추출 (P001)
                StringBuffer value = new StringBuffer();

                for (int i=1; i<tokens.length; i++) {
                    if (i != (tokens.length - 1)) { // ✅ 마지막에는 ","을 안붙이기 위한 if문 처리
                        value.append(tokens[i] + ",");
                    } else {
                        value.append(tokens[i]);
                    }
                }
                sendMessages(kafkaProducer, topicName, key, value.toString()); // ✅ Serialize가 String 값이라서, StringBuffer 형태의 value를 toString()으로 전환
            }
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private static void sendMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {

        // ✅ key, value 기반으로 ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        logger.info("key={}, value={}", key, value);

        // ✅ 비동기(콜백) 전송
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
    }

}
