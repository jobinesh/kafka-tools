package com.jobinesh.kafka;

import com.jobinesh.kafka.message.SimpleMessage;

public interface Retriable<K, V extends SimpleMessage> {
    void retry(String topic, K key, V message);
}
