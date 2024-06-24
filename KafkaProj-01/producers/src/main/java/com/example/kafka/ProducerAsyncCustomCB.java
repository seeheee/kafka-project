package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCB {
    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCB.class.getName());
    public static void main(String[] args) {
        String topicName = "multipart-topic";


        //kafkaProducer configuration setting
        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        for(int seq=0; seq<20; seq++) {
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world " + seq);
            Callback callback = new CustomCallback(seq);
            //ProducerRecord 객체 생성 -- 메세지 전송할 내용


            //동기식 KafkaProducer 메세지 전송
//        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
//        RecordMetadata recordMetadata = future.get();
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
