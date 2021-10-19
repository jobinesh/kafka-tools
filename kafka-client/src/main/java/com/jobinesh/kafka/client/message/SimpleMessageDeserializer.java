package com.jobinesh.kafka.client.message;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class SimpleMessageDeserializer<T extends SimpleMessage> implements Deserializer<T> {
    private ObjectMapper objectMapper;
    private Class<T> type;

    /**
     * Default constructor needed by Kafka
     */
    public SimpleMessageDeserializer() {

    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        this.objectMapper = new ObjectMapper();
    }


    @Override
    public T deserialize(String ignored, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            return objectMapper.readValue(bytes, new TypeReference<T>() {});
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    protected Class<T> getType() {
        return type;
    }

    @Override
    public void close() {

    }
}
