package com.kristi.kafkaconsumer.controller;

import java.util.List;

import com.kristi.kafkaconsumer.model.ConsumerDTO;
import com.kristi.kafkaconsumer.service.ConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("consumers")
public class ConsumerController {

    @Autowired
    ConsumerService consumerService;

    @GetMapping
    public List<ConsumerDTO> getConsumers() {
        return consumerService.getConsumers();
    }

    @PostMapping
    public ConsumerDTO addConsumer(@RequestParam List<String> topics) {
        return consumerService.addConsumer(topics);
    }

    @DeleteMapping("/{id}")
    public ConsumerDTO deleteConsumer(@PathVariable Integer id) {
        return consumerService.deleteConsumer(id);
    }
}
