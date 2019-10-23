package com.kristi.kafkaconsumer.model;

import java.util.List;

public class ConsumerDTO {
    private Integer id;
    private List<String> topics;

    public ConsumerDTO(Integer id, List<String> topics) {
        this.id = id;
        this.topics = topics;
    }

    public Integer getId() {
        return id;
    }

    public List<String> getTopics() {
        return topics;
    }
}
