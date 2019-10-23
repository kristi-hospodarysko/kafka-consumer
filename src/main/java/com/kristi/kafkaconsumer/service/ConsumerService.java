package com.kristi.kafkaconsumer.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

import com.kristi.kafkaconsumer.worker.Consumer;
import com.kristi.kafkaconsumer.model.ConsumerDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    private ExecutorService executorService = Executors.newCachedThreadPool();
    private Map<Integer, Consumer> consumers = new HashMap<>();

    private int baseIndex;

    @Value("${spring.kafka.bootstrap-servers}")
    private String server;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    public List<ConsumerDTO> getConsumers() {
        return consumers.values().stream().map(Consumer::toDTO).collect(Collectors.toList());
    }

    public ConsumerDTO addConsumer(List<String> topics) {
        Consumer consumer = new Consumer(baseIndex++, topics, server, groupId);
        consumers.put(consumer.getId(), consumer);
        executorService.submit(consumer);

        return consumer.toDTO();
    }

    public ConsumerDTO deleteConsumer(Integer id) {
        if (consumers.containsKey(id)) {
            Consumer consumer = consumers.get(id);
            consumer.shutDown();

            consumers.remove(id);

            return consumer.toDTO();
        } else {
            LOGGER.warn("Consumer with id {} was not found", id);
            return null;
        }
    }

    @PreDestroy
    public void closeConsumers() {
        consumers.values().forEach(Consumer::shutDown);
        consumers.clear();
    }
}
