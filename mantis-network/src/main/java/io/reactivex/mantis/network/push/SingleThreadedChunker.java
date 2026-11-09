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

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


public class SingleThreadedChunker<T> implements Callable<Void> {

    final MonitoredQueue<T> inputQueue;
    final int TIME_PROBE_COUNT = 100000;
    final private int chunkSize;
    final private long maxChunkInterval;
    final private ConnectionManager<T> connectionManager;
    final private ChunkProcessor<T> processor;
    final private Object[] chunk;
    int iteration = 0;
    private int index = 0;

    private final Counter numEventsDrained;
    private final Counter drainTriggeredByTimer;
    private final Counter drainTriggeredByBatch;


    public SingleThreadedChunker(ChunkProcessor<T> processor, MonitoredQueue<T> iQ, int chunkSize, long maxChunkInterval, ConnectionManager<T> connMgr) {
        this.inputQueue = iQ;
        this.chunkSize = chunkSize;
        this.maxChunkInterval = maxChunkInterval;
        this.processor = processor;
        this.connectionManager = connMgr;
        chunk = new Object[this.chunkSize];

        MetricGroupId metricsGroup = new MetricGroupId("SingleThreadedChunker");
        Metrics metrics = new Metrics.Builder()
            .id(metricsGroup)
            .addCounter("numEventsDrained")
            .addCounter("drainTriggeredByTimer")
            .addCounter("drainTriggeredByBatch")
            .build();
        numEventsDrained = metrics.getCounter("numEventsDrained");
        drainTriggeredByTimer = metrics.getCounter("drainTriggeredByTimer");
        drainTriggeredByBatch = metrics.getCounter("drainTriggeredByBatch");
        MetricsRegistry.getInstance().registerAndGet(metrics);
    }

    private boolean stopCondition() {
        return Thread.currentThread().isInterrupted();
    }

    @Override
    public Void call() throws Exception {


        long chunkStartTime = System.currentTimeMillis();
        while (true) {
            iteration++;
            if (iteration == TIME_PROBE_COUNT) {

                long currTime = System.currentTimeMillis();
                if (currTime - maxChunkInterval > chunkStartTime) {
                    drainTriggeredByTimer.increment();
                    drain();
                }
                iteration = 0;
                if (stopCondition()) {
                    break;
                }
            }

            if (index < this.chunkSize) {
                T ele = inputQueue.poll();
                if (ele != null) {

                    chunk[index++] = ele;
                }
            } else {
                drainTriggeredByBatch.increment();
                drain();
                chunkStartTime = System.currentTimeMillis();
                if (stopCondition()) {
                    break;
                }
            }

        }
        return null;
    }

    private void drain() {

        if (index > 0) {
            List<T> copy = new ArrayList<T>(index);
            for (int i = 0; i < index; i++) {
                copy.add((T) chunk[i]);
            }

            processor.process(connectionManager, copy);
            numEventsDrained.increment(copy.size());
            index = 0;
        }
    }

}
