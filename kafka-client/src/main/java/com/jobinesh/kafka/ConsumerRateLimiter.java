package com.jobinesh.kafka;

import java.util.concurrent.atomic.LongAdder;

public class ConsumerRateLimiter {
    LongAdder activeTasks = new LongAdder();
}
