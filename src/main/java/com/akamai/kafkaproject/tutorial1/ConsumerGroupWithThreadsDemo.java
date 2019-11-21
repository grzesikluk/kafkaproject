package com.akamai.kafkaproject.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerGroupWithThreadsDemo
{
    final static String BOOTSTRAP_SERVER = "172.25.164.138:9092";

    public static void main(String[] args) throws InterruptedException
    {
        new ConsumerGroupWithThreadsDemo().run();
    }

    private void run() throws InterruptedException
    {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_sixt_application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final CountDownLatch latch = new CountDownLatch(10);

        ConsumerThread consumerThread = new ConsumerThread(properties, latch);

        final ExecutorService singleThreadPool = Executors.newSingleThreadExecutor();
        singleThreadPool.execute(new Thread(consumerThread));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        latch.await();
    }

    public static class ConsumerThread implements Runnable
    {
        private final Properties properties;
        CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        ConsumerThread(Properties properties, CountDownLatch latch)
        {
            this.latch = latch;
            this.properties = properties;
        }

        @Override
        public void run()
        {
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton("first_topic"));
            ConsumerRecords<String, String> records;

            try {
                while (true) {
                    records = consumer.poll(Duration.ofMillis(100));
                    if (records != null)
                        for (ConsumerRecord<String, String> consumerRecord : records) {
                            logger.info(String.format("------"));
                            logger.info(String.format("Thread: %s", Thread.currentThread().getName()));
                            logger.info(String.format("Got partition: %s", consumerRecord.partition()));
                            logger.info(String.format("Got key: %s", consumerRecord.key()));
                            logger.info(String.format("Got offset: %s", consumerRecord.offset()));
                            logger.info(String.format("------"));
                        }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown info");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown()
        {
            consumer.wakeup();
        }
    }
}
