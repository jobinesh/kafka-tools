package com.jobinesh.kafka.client;

import com.jobinesh.kafka.client.message.SimpleMessage;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer<K,V extends SimpleMessage> {
    private Producer producer;
    private String topic;

    public SimpleProducer() {
        producer = createProducer();

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void send(K messageNum, V message) {
        producer.send(new ProducerRecord<>(topic, messageNum, message));
        producer.flush();
    }

    public void send(String topic, K messageNum, V message) {
        producer.send(new ProducerRecord<>(topic, messageNum, message));
        producer.flush();
    }

    private Producer<K,V> createProducer() {
        return new ProducerBuilder<K, V>()
                .setConfigFilePath("producer.properties")
                .build();
    }

    public static void main(String[] args) {
        SimpleProducer<String,SimpleMessage> simpleProducerClient = new SimpleProducer<>();
        SimpleMessage msg = new SimpleMessage();
        msg.setMessage("Hello World");
        simpleProducerClient.send("demo", "1",msg);
    }
}
