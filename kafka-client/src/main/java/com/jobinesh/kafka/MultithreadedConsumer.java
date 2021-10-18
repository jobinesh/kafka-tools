package com.jobinesh.kafka;

import com.jobinesh.kafka.message.SimpleMessage;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultithreadedConsumer<K, V extends SimpleMessage> implements Runnable, ConsumerRebalanceListener {

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);
    private final Map<TopicPartition, ConsumerRecordProcessor> activeTasks = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private long lastCommitTime = System.currentTimeMillis();
    private Collection<String> topics;

    private final Logger log = LoggerFactory.getLogger(MultithreadedConsumer.class);

    public MultithreadedConsumer(Collection<String> topics) {
        this.topics = topics;
        kafkaConsumer = new ConsumerBuilder<K, V>()
                .setConfigFilePath("consumer.properties")
                .build();
    }

    @Override
    public void run() {
        try {
            kafkaConsumer.subscribe(topics, this);
            while (!stopped.get()) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                handleFetchedRecords(records);
                checkActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException we) {
            if (!stopped.get())
                throw we;
        } finally {
            kafkaConsumer.close();
        }
    }


    private void handleFetchedRecords(ConsumerRecords<K, V> records) {
        if (records.count() > 0) {
            List<TopicPartition> partitionsToPause = new ArrayList<>();
            records.partitions().forEach(partition -> {
                List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
                ConsumerRecordProcessor<K,V> consumerRecordProcessor = new ConsumerRecordProcessor<>(partitionRecords);
                partitionsToPause.add(partition);
                executor.submit(consumerRecordProcessor);
                activeTasks.put(partition, consumerRecordProcessor);
            });
            kafkaConsumer.pause(partitionsToPause);
        }
    }

    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 5000) {
                if (!offsetsToCommit.isEmpty()) {
                    kafkaConsumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            log.error("Failed to commit offsets!", e);
        }
    }


    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach((partition, consumerRecordProcessor) -> {
            if (consumerRecordProcessor.isFinished())
                finishedTasksPartitions.add(partition);
            long offset = consumerRecordProcessor.getCurrentOffset();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });
        finishedTasksPartitions.forEach(partition -> activeTasks.remove(partition));
        kafkaConsumer.resume(finishedTasksPartitions);
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        // 1. Stop all tasks handling records from revoked partitions
        Map<TopicPartition, ConsumerRecordProcessor> stoppedTask = new HashMap<>();
        for (TopicPartition partition : partitions) {
            ConsumerRecordProcessor consumerRecordProcessor = activeTasks.remove(partition);
            if (consumerRecordProcessor != null) {
                consumerRecordProcessor.stop();
                stoppedTask.put(partition, consumerRecordProcessor);
            }
        }

        // 2. Wait for stopped tasks to complete processing of current record
        stoppedTask.forEach((partition, consumerRecordProcessor) -> {
            long offset = consumerRecordProcessor.waitForCompletion();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });


        // 3. collect offsets for revoked partitions
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
        partitions.forEach(partition -> {
            OffsetAndMetadata offset = offsetsToCommit.remove(partition);
            if (offset != null)
                revokedPartitionOffsets.put(partition, offset);
        });

        // 4. commit offsets for revoked partitions
        try {
            kafkaConsumer.commitSync(revokedPartitionOffsets);
        } catch (Exception e) {
            log.warn("Failed to commit offsets for revoked partitions!");
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }


    public void stopConsuming() {
        stopped.set(true);
        kafkaConsumer.wakeup();
    }

}
