package com.practice.kafka.consumer;

import com.practice.kafka.model.OrderModel;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class OrderSerdeConsumer<K extends Serializable, V extends  Serializable> {
    public static final Logger logger = LoggerFactory.getLogger(OrderSerdeConsumer.class.getName());

    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topics;

    public OrderSerdeConsumer(Properties consumerProps, List<String> topics) {
        //kafkaConsumer 객체 생성
        this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
    }

    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        //main thread
        Thread mainThread = Thread.currentThread();

        //main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) { e.printStackTrace();}
            }
        });

    }
    private void processRecord(ConsumerRecord<K,V> record){
        logger.info("record key:{}, partitions:{}, offset:{}, value:{}",
                record.key(), record.partition(), record.offset(), record.value());

    }

    private void processRecords(ConsumerRecords<K,V> records){
        records.forEach(record -> processRecord(record));
//        for(ConsumerRecord<K,V> record: records){
//            processRecord(record);
//        }
    }

    public void pollConsumes(long durationMillis, String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            closeConsumer();
        }
    }


    private void pollCommitSync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if(consumerRecords.count() > 0 ) {
                this.kafkaConsumer.commitSync();
                logger.info("commit sync has been called");
            }
        } catch(CommitFailedException e) {
            logger.error(e.getMessage());
        }
    }
    public void closeConsumer() {
        this.kafkaConsumer.close();
    }

    private void  pollCommitAsync(long durationMillis) throws WakeupException, Exception{
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        this.kafkaConsumer.commitAsync((offsets, exception) -> {
            if(exception != null){
                logger.error("offset {} is not completed, error:{}", offsets,exception.getMessage());
            }
        });
//        this.kafkaConsumer.commitAsync(new OffsetCommitCallback() {
//            @Override
//            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                if(exception != null){
//                    logger.error("offset {} is not completed, error:{}", offsets,exception.getMessage());
//                }
//            }
//        });
    }


    public static void main(String[] args) {
        String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-serde-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        OrderSerdeConsumer<String, OrderModel> baseConsumer = new OrderSerdeConsumer<String, OrderModel>(props, List.of(topicName));
        baseConsumer.initConsumer();
        String commitMode = "async";

        baseConsumer.pollConsumes(100, commitMode);
        baseConsumer.closeConsumer();

    }


}