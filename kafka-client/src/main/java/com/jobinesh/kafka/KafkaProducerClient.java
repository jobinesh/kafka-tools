package com.jobinesh.kafka;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerClient {
    Producer<Long, String> kafkaProducer;
    public KafkaProducerClient() {
        kafkaProducer= createProducer();

    }

    public void send(String topic, Long messageNum, String message){
        kafkaProducer.send(new ProducerRecord<>(topic, messageNum, message));
        kafkaProducer.flush();
    }
    private  Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        //props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
    public static void main(String[] args){
        KafkaProducerClient producerClient=new KafkaProducerClient();
        producerClient.send("demo",Long.valueOf(1),"Hello");
    }
}
