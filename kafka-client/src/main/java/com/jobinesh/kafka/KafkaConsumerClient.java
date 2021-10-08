package com.jobinesh.kafka;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class KafkaConsumerClient {
    public void startConsumerThread(){
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        singleThreadExecutor.submit(new MultithreadedKafkaConsumer("demo"));
    }
    public static void main(String[] args){
        KafkaConsumerClient kafkaConsumerClient =new KafkaConsumerClient();
        kafkaConsumerClient.startConsumerThread();
    }
}
