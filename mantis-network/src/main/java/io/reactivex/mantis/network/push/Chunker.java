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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Chunker<T> implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(Chunker.class);

    private MonitoredQueue<T> outboundBuffer;
    private List<T> internalBuffer;
    private int internalBufferSize;
    private ChunkProcessor<T> processor;
    private ConnectionManager<T> connectionManager;
    private Counter numEventsDrained;

    public Chunker(
            ChunkProcessor<T> processor,
            T firstRead, MonitoredQueue<T> outboundBuffer,
            int internalBufferSize, ConnectionManager<T> connectionManager) {
        this.outboundBuffer = outboundBuffer;
        this.processor = processor;
        this.internalBufferSize = internalBufferSize;
        this.connectionManager = connectionManager;
        internalBuffer = new ArrayList<T>(internalBufferSize);
        internalBuffer.add(firstRead);

        MetricGroupId metricsGroup = new MetricGroupId("Chunker");
        Metrics metrics = new Metrics.Builder()
                .id(metricsGroup)
                .addCounter("numEventsDrained")
                .build();
        numEventsDrained = metrics.getCounter("numEventsDrained");
        MetricsRegistry.getInstance().registerAndGet(metrics);

        if (internalBufferSize == 1) {
            drain();
        }
    }

    @Override
    public Void call() throws Exception {
        while (!stopCondition()) {
            try {
                internalBuffer.add(outboundBuffer.get());
                if (internalBuffer.size() >= internalBufferSize) {
                    drain();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("Error occured chunking data", e);
            }
        }
        drain();
        return null;
    }

    private void drain() {
        int size = internalBuffer.size();
        if (size > 0) {
            List<T> copy = new ArrayList<T>(size);
            copy.addAll(internalBuffer);
            processor.process(connectionManager, copy);
            internalBuffer.clear();
            numEventsDrained.increment(size);
        }
    }

    private boolean stopCondition() {
        return Thread.currentThread().isInterrupted();
    }
}
