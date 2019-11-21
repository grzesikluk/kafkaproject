package com.akamai.kafkaproject.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ProducerDemoKeys
{

    final static String BOOTSTRAP_SERVER = "172.25.164.138:9092";

    public static void main(String[] args) throws InterruptedException
    {

        Logger logger = LoggerFactory.getLogger(ProducerConfig.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        IntStream.range(0, 100).forEach(i -> {
            try {
                producer.send(new ProducerRecord<String, String>("first_topic", Integer.toString(i), "my_message 1"), (recordMetadata, e) -> {
                    if (e == null) {
                        //sent successfully
                        logger.info(String.format("Got data\nTopic: %s\nPartition: %s\nOffset: %s\n Key: %d",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), i));
                    } else {
                        logger.error(e.getMessage());
                    }
                }).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        producer.close();
    }
}
