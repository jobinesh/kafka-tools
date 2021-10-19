package com.jobinesh.kafka.client;

import com.jobinesh.kafka.client.message.SimpleMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Retriable<K, V extends SimpleMessage> {
    boolean retry(ConsumerRecord<K, V> record);
}
