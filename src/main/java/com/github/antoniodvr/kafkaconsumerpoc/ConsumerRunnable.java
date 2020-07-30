package com.github.antoniodvr.kafkaconsumerpoc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConsumerRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final UUID id;

    public ConsumerRunnable(Properties props, UUID id, List<String> topics) {
        this.id = id;
        this.topics = topics;
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        try {
            logger.info("Starting consumer [id={}]", this.id);

            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                logger.info("Consumer [id={}] pooled {} messages", this.id, records.count());
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    logger.info("Consumer [id={}] received {}", this.id, data);
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        logger.info("Consumer [id={}] received wakeup", this.id);
        consumer.wakeup();
    }
}