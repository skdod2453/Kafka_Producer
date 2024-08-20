package com.mykafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class KafkaProducerConfig {
    @Autowired
    Environment env;

    @Bean
    public NewTopic createTopic(){
        String topicName = env.getProperty("topic.name");
        return new NewTopic(topicName, 3, (short) 1);
    }
}