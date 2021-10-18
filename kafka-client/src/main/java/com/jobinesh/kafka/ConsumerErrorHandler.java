package com.jobinesh.kafka;

import com.jobinesh.kafka.message.SimpleMessage;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ConsumerErrorHandler<K, V extends SimpleMessage> implements Retriable<K, V> {
    private Producer<K, V> producer;

    public ConsumerErrorHandler(Producer<K, V> producer) {
        this.producer = producer;
    }

    @Override
    public void retry(String topic, K key, V message) {
        producer.send(new ProducerRecord<>(topic, key, message));
    }
}
