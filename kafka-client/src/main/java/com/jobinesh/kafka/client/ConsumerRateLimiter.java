package com.jobinesh.kafka.client;

import java.util.concurrent.atomic.LongAdder;

public class ConsumerRateLimiter {
    LongAdder activeTasks = new LongAdder();
}
