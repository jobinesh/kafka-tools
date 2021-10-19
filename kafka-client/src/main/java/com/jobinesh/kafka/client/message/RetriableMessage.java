package com.jobinesh.kafka.client.message;

public interface RetriableMessage {
    int getRetryCount();
}
