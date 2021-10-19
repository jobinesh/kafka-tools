package com.jobinesh.kafka.client.message;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SimpleMessage implements RetriableMessage {
    private int  retryCount=0;
    private String message;

    @Override
    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "SimpleMessage{" +
                "retryCount=" + retryCount +
                ", message='" + message + '\'' +
                '}';
    }
}
