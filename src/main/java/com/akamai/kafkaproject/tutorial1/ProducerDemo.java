package com.akamai.kafkaproject.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemo
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
            producer.send(new ProducerRecord<String, String>("first_topic", "my_message 1"), new Callback()
            {
                @Override
                public void onCompletion(final RecordMetadata recordMetadata, final Exception e)
                {
                    if (e == null) {
                        //sent successfully
                        logger.info(String.format("Got data\nTopic: %s\nPartition: %s\nOffset: %s\n",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()));
                    } else {
                        logger.error(e.getMessage());
                    }
                }
            });
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        ;

        producer.close();
    }
}
