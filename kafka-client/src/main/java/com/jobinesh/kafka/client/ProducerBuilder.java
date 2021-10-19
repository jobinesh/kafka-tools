package com.jobinesh.kafka.client;

import com.jobinesh.kafka.client.message.SimpleMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.ProviderNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProducerBuilder<K, V extends SimpleMessage> {
    private String configFilePath;
    private Map<String, String> kafkaProperties = new HashMap<>();
    private final Logger log = LoggerFactory.getLogger(ProducerBuilder.class);

    public ProducerBuilder setConfigFilePath(String configFilePath) {
        this.configFilePath = configFilePath;
        return this;
    }

    public ProducerBuilder setProperty(String key, String value) {
        kafkaProperties.put(key, value);
        return this;
    }

    public Producer<K, V> build() {
        Producer<K, V> producer;
        try (InputStream is = getInputStream(configFilePath)) {
            Properties properties = new Properties();
            properties.load(is);
            properties.putAll(kafkaProperties);
            producer = new KafkaProducer<K, V>(properties);
            return producer;
        } catch (IOException ex) {
            log.error("Error building consumer", ex);
            throw new ProviderNotFoundException(ex.getMessage());
        }
    }

    private InputStream getInputStream(String path) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path);
        return inputStream;
    }
}
