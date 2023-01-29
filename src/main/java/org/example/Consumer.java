package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        log.info("This class consumes messages from Kafka");

        // configure consumer properties
        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "::1:9092"); // Workaround for Windows consumer and WSL broker
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "lotr_consumer_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        try {
            // subscribe to topic(s)
            String topic = "lotr_characters";
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                // poll for new messages
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(100));

                // handle message contents
                for (ConsumerRecord<String, String> message : messages) {
                    log.info("key [" + message.key() + "] value [" + message.value() + "]");
                    log.info("partition [" + message.partition() + "] offset [" + message.offset() + "]");
                }
            }
        } catch (Exception err) {
            // catch and handle exceptions
            log.error("Error: ", err);
        } finally {
            // close consumer and commit offsets
            consumer.close();
            log.info("consumer is now closed");
        }
    }
}