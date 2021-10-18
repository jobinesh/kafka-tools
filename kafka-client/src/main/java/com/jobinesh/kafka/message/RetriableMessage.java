package com.jobinesh.kafka.message;

public interface RetriableMessage {
    int getRetryCount();
}
