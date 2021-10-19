package com.jobinesh.kafka.client;

import com.jobinesh.kafka.client.message.SimpleMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


public class ConsumerRecordProcessor<K, V extends SimpleMessage> implements Runnable {

    private final List<ConsumerRecord<K, V>> records;
    private volatile boolean stopped = false;
    private volatile boolean started = false;
    private volatile boolean finished = false;
    private final CompletableFuture<Long> completion = new CompletableFuture<>();
    private final ReentrantLock startStopLock = new ReentrantLock();
    private final AtomicLong currentOffset = new AtomicLong();
    private ConsumerErrorHandler<K, V> errorHandler;
    private Logger log = LoggerFactory.getLogger(ConsumerRecordProcessor.class);

    public ConsumerRecordProcessor(List<ConsumerRecord<K, V>> records) {
        this.records = records;
    }

    public void setErrorHandler(ConsumerErrorHandler<K, V> errorHandler) {
        this.errorHandler = errorHandler;
    }

    public void run() {
        startStopLock.lock();
        if (stopped) {
            return;
        }
        started = true;
        startStopLock.unlock();

        for (ConsumerRecord<K, V> record : records) {
            if (stopped)
                break;
            // process record here and make sure you catch all exceptions;
            processRecord(record);
            currentOffset.set(record.offset() + 1);
        }
        finished = true;
        completion.complete(currentOffset.get());
    }

    public void processRecord(ConsumerRecord<K, V> record) {

        log.info("-------------ProcessRecord------------");
        //Add logic for processing the record.
        //Below code simulates a task that runs in async mode.
        CompletableFuture
                .runAsync(() -> someAsyncTask(record))
                .exceptionally(exception -> {
                    //  Re-publish the message to the topic if the retries is within threshold
                    if (errorHandler != null) {
                        errorHandler.retry(record);
                    }
                    return null;
                });

    }

    private void someAsyncTask(ConsumerRecord<K, V> record) {
        log.info("----------------Async processing of ConsumerRecord------------");
        log.info(record.toString());
    }


    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {
        startStopLock.lock();
        this.stopped = true;
        if (!started) {
            finished = true;
            completion.complete(currentOffset.get());
        }
        startStopLock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            return -1;
        }
    }

    public boolean isFinished() {
        return finished;
    }

}