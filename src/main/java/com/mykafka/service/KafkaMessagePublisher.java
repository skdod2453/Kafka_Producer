package com.mykafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String,Object> template;

    @Value("${topic.name}")
    String topicName = "";

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> future =
                template.send(topicName, message);
        future.whenComplete((result,ex)->{
            if (ex == null) {
                RecordMetadata recordMetadata = result.getRecordMetadata();
                log.info("Sent message= {} with offset= {}", message, recordMetadata.offset());
                log.info("Topic Name = {}", recordMetadata.topic());
                log.info("Topic Partition = {}", recordMetadata.partition());
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });

    }
}   