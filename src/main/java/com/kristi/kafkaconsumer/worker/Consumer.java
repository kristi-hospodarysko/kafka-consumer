package com.kristi.kafkaconsumer.worker;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import com.kristi.kafkaconsumer.model.ConsumerDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private Integer id;
    private List<String> topics;
    private KafkaConsumer<String, String> consumer;

    public Consumer(Integer id, List<String> topics, String server, String groupId) {
        this.id = id;
        this.topics = topics;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);
    }

    public Integer getId() {
        return id;
    }

    public ConsumerDTO toDTO() {
        return new ConsumerDTO(id, topics);
    }

    @Override
    public void run() {
        consumer.subscribe(topics);

        int retryCount = 10;
        int noRecordsCount = 0;

        try {
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));
                if (records.count() == 0) {
                    noRecordsCount++;
                } else {
                    noRecordsCount = 0;
                    LOGGER.info("Received {} records.", records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("consumerId={} partitionId={}, offset={}, payload: {}",
                                id, record.partition(), record.offset(), record.value());
                    }
                }
            } while (noRecordsCount < retryCount);

        } finally {
            consumer.close();
        }


    }

    public void shutDown(){
        consumer.close();
    }


}
