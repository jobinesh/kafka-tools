package com.jobinesh.kafka.client;

import com.jobinesh.kafka.client.message.SimpleMessage;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerManager<K, V extends SimpleMessage> {
    private int parallelismCount = 1;
    private Collection<String> topics;
    private ExecutorService executor;
    private MultithreadedConsumer<K, V> consumer;

    public int getParallelismCount() {
        return parallelismCount;
    }

    public Collection<String> getTopics() {
        return topics;
    }

    public void setTopics(Collection<String> topics) {
        this.topics = topics;
    }

    public void setParallelismCount(int parallelismCount) {
        this.parallelismCount = parallelismCount;
    }

    public void startConsumers() {

        executor = Executors.newFixedThreadPool(parallelismCount);
        consumer = new MultithreadedConsumer<K, V>(topics);
        executor.submit(consumer);
    }

    public void stopConsumers() {
        consumer.stopConsuming();
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Executor tasks interrupted");
        } finally {
            if (!executor.isTerminated()) {
                System.err.println("Canceling non-finished executor tasks");
            }
            executor.shutdownNow();
        }
    }

    public static void main(String[] args) {
        ConsumerManager<String, SimpleMessage> consumerManager = new ConsumerManager();
        consumerManager.setParallelismCount(1);
        consumerManager.setTopics(Collections.singleton("demo"));
        consumerManager.startConsumers();
    }
}
