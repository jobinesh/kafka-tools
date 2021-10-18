package com.jobinesh.kafka;

import com.jobinesh.kafka.message.SimpleMessage;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.ProviderNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerBuilder<K, V extends SimpleMessage> {
    private final Logger log = LoggerFactory.getLogger(ConsumerBuilder.class);
    private String configFilePath;
    private final Map<String, String> kafkaProperties = new HashMap<>();

    public ConsumerBuilder() {
    }

    public ConsumerBuilder setConfigFilePath(String filePath) {
        this.configFilePath = filePath;
        return this;
    }

    public ConsumerBuilder setProperty(String key, String value) {
        kafkaProperties.put(key, value);
        return this;
    }

    public KafkaConsumer<K, V> build() {
        try (InputStream is = getInputStream(configFilePath)) {
            Properties properties = new Properties();
            properties.load(is);
            properties.putAll(kafkaProperties);
            KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties);
            return consumer;
        } catch (IOException e) {
            log.error("Error building consumer", e);
            throw new ProviderNotFoundException(e.getMessage());
        }
    }

    private InputStream getInputStream(String path) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path);
        return inputStream;
    }
}
