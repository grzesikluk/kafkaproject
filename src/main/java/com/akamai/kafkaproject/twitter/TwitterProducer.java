package com.akamai.kafkaproject.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer
{
    final static String BOOTSTRAP_SERVER = "172.25.164.138:9092";
    private final KafkaProducer<String, String> producer;
    private final Client client;
    private final BlockingQueue<String> msgQueue;
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private final String consumerApiKey = "JRiVMWjo3DzalbYCEuYqBNwaX";
    private final String consumerApiSecrets = "wjuinfncJQdkEThelaMogyvqdXwM9jSqjpTJUaGNJtBUw4Sg27";
    private final String accessToken = "943495386534240257-A2k9nUDiyXNaYNS23iIbT2vrrOoCk9z";
    private final String accessTokenSecret = "rmT2l3toIxpX2gMQVeu0A1wS9afh7o1KZOuZnD89wH8jj";

    public TwitterProducer()
    {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
        msgQueue = new LinkedBlockingQueue<>(1000);
        client = getTwitterClient(this.msgQueue);
    }

    public void send(String message, String topic)
    {
        this.producer.send(new ProducerRecord<>(topic, message), (recordMetadata, e) -> {
            if (e == null) {
                //sent successfully
                logger.info(String.format("Sent data\nTopic: %s\nPartition: %s\nOffset: %s\n",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()));
            } else {
                logger.error(e.getMessage());
            }
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() throws InterruptedException
    {
        client.connect();

        while (!client.isDone()) {
            String msg = msgQueue.poll(5, TimeUnit.SECONDS);
            logger.info("Got message from twitter {}", msg);
            send(msg, "twitter");
        }

        client.stop();
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException
    {
        new TwitterProducer().run();
    }

    public Client getTwitterClient(BlockingQueue<String> msgQueue)
    {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        ArrayList<String> terms = Lists.newArrayList("bitcoin");
        Authentication hosebirdAuth = new OAuth1(consumerApiKey, consumerApiSecrets, accessToken, accessTokenSecret);

        hosebirdEndpoint.trackTerms(terms);

        ClientBuilder builder = new ClientBuilder()
                .name("twitter-client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
