package com.jobinesh.kafka.client.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serialize objects to UTF-8 JSON.
 */
public class SimpleMessageSerializer<T extends SimpleMessage> implements Serializer<T> {

    private ObjectMapper objectMapper;

    /**
     * Default constructor needed by Kafka
     */
    public SimpleMessageSerializer() {

    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        this.objectMapper = new ObjectMapper();
    }


    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }
}