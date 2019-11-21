package com.akamai.kafkaproject.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo
{
    final static String BOOTSTRAP_SERVER = "172.25.164.138:9092";

    public static void main(String[] args)
    {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Collections.singleton("first_topic"));

        ConsumerRecords<String, String> records = null;
        while(true) {
            records = consumer.poll(Duration.ofMillis(100));
            if (records != null)
                for (ConsumerRecord<String, String> consumerRecord: records) {
                    logger.info(String.format("Got partition: %s", consumerRecord.partition()));
                    logger.info(String.format("Got key: %s", consumerRecord.key()));
                    logger.info(String.format("Got offset: %s", consumerRecord.offset()));
                }
        }

    }
}
