package com.practice.kafka.event;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler{
    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());
    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topicName, messageEvent.key, messageEvent.value);
        if(this.sync){
            RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
            logger.info("\n ##### record metadata received ##### \n" +
                    "partition" + recordMetadata.partition() + "\n"
                    + "offset" + recordMetadata.offset() + "\n"
                    + "timestamp" + recordMetadata.timestamp());
        }else{
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        logger.info("\n ##### record metadata received ##### \n" +
                                "partition" + metadata.partition() + "\n"
                                + "offset" + metadata.offset() + "\n"
                                + "timestamp" + metadata.timestamp());
                    } else {
                        logger.error("exception error " + exception.getMessage());
                    }
                }
            });
        }
    }
}
