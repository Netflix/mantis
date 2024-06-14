/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivex.mantis.network.push;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class TimedChunker<T> implements Callable<Void> {

    private static ThreadFactory namedFactory = new NamedThreadFactory("TimedChunkerGroup");
    private MonitoredQueue<T> buffer;
    private ChunkProcessor<T> processor;
    private ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor(namedFactory);
    private int maxBufferLength;
    private int maxTimeMSec;
    private ConnectionManager<T> connectionManager;
    private List<T> internalBuffer;

    private Counter interrupted;
    private Counter numEventsDrained;
    private Counter drainTriggeredByTimer;
    private Counter drainTriggeredByBatch;
    private final MeterRegistry meterRegistry;

    public TimedChunker(MonitoredQueue<T> buffer, int maxBufferLength,
                        int maxTimeMSec, ChunkProcessor<T> processor,
                        ConnectionManager<T> connectionManager,
                        MeterRegistry meterRegistry) {
        this.maxBufferLength = maxBufferLength;
        this.maxTimeMSec = maxTimeMSec;
        this.buffer = buffer;
        this.processor = processor;
        this.connectionManager = connectionManager;
        this.internalBuffer = new ArrayList<>(maxBufferLength);
        this.meterRegistry = meterRegistry;
        interrupted = meterRegistry.counter("TimedChunker_interrupted");
        numEventsDrained = meterRegistry.counter("TimedChunker_numEventsDrained");
        drainTriggeredByTimer = meterRegistry.counter("TimedChunker_drainTriggeredByTimer");
        drainTriggeredByBatch = meterRegistry.counter("TimedChunker_drainTriggeredByBatch");
    }

    @Override
    public Void call() throws Exception {
        ScheduledFuture periodicDrain = scheduledService.scheduleAtFixedRate(() -> {
            drainTriggeredByTimer.increment();
            drain();
            }, maxTimeMSec, maxTimeMSec, TimeUnit.MILLISECONDS);
        while (!stopCondition()) {
            try {
                T data = buffer.get();
                synchronized (internalBuffer) {
                    internalBuffer.add(data);
                }
                if (internalBuffer.size() >= maxBufferLength) {
                    drainTriggeredByBatch.increment();
                    drain();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                periodicDrain.cancel(true);
                interrupted.increment();
            }
        }
        drain();
        return null;
    }

    private boolean stopCondition() {
        return Thread.currentThread().isInterrupted();
    }

    private void drain() {
        if (internalBuffer.size() > 0) {
            List<T> copy = new ArrayList<>(internalBuffer.size());
            synchronized (internalBuffer) {
                // internalBuffer content may have changed since acquiring the lock.
                copy.addAll(internalBuffer);
                internalBuffer.clear();
            }
            if (copy.size() > 0) {
                processor.process(connectionManager, copy);
                numEventsDrained.increment(copy.size());
            }
        }
    }
}
