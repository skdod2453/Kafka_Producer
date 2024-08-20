package com.mykafka.controller;

import com.mykafka.dto.Customer;
import com.mykafka.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    @PostMapping
    public void sendObject(@RequestBody Customer customer) {
        publisher.sendObjectToTopic(customer);
    }

    @GetMapping("/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
//            publisher.sendMessageToTopic(message);
            for (int i = 0; i <= 100000; i++) {
                publisher.sendMessageToTopic(message + " : " + i);
            }
            return ResponseEntity.ok("message published successfully ..");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
}