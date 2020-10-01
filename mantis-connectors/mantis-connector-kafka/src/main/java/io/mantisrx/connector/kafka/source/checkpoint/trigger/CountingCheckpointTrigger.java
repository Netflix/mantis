/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.kafka.source.checkpoint.trigger;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Time and count based CheckpointTrigger that triggers a checkpoint either if accumulated count exceeds threshold or time
 * sine last checkpoint exceeds the configured checkpoint trigger interval.
 */
public class CountingCheckpointTrigger implements CheckpointTrigger {

    private final int threshold;
    private final AtomicInteger counter;
    private final AtomicBoolean checkpoint = new AtomicBoolean(false);
    private final AtomicBoolean isActive;
    private final Subscription checkpointOffsetsTimer;

    public CountingCheckpointTrigger(final int threshold, final int triggerIntervalMs) {
        this.threshold = threshold;
        this.counter = new AtomicInteger(0);
        this.isActive = new AtomicBoolean(true);
        checkpointOffsetsTimer = Observable.interval(triggerIntervalMs, TimeUnit.MILLISECONDS).subscribe(new Action1<Long>() {
            @Override
            public void call(Long aLong) {
                checkpoint.set(true);
            }
        });
    }

    @Override
    public boolean shouldCheckpoint() {
        return (counter.get() > threshold) || checkpoint.get();
    }

    @Override
    public void update(final int count) {
        counter.addAndGet(count);
    }

    @Override
    public void reset() {
        counter.set(0);
        checkpoint.set(false);
    }

    @Override
    public boolean isActive() {
        return isActive.get();
    }

    @Override
    public void shutdown() {
        if (isActive()) {
            checkpointOffsetsTimer.unsubscribe();
            reset();
            isActive.compareAndSet(true, false);
        }
    }
}
