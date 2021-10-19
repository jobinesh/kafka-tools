package com.jobinesh.kafka.client;

import com.jobinesh.kafka.client.message.SimpleMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ConsumerErrorHandler<K, V extends SimpleMessage> implements Retriable<K, V> {
    private Producer<K, V> producer;
    private final static int MAX_RETRIES = 5;

    public ConsumerErrorHandler(Producer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public boolean retry(ConsumerRecord<K, V> record) {
        int retries = record.value().getRetryCount();
        if (MAX_RETRIES < retries) {
            record.value().setRetryCount(retries + 1);
            producer.send(new ProducerRecord<>(record.topic(), record.key(), record.value()));
            return true;
        }
        return false;
    }
}
