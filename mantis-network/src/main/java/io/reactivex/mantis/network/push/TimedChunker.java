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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class TimedChunker<T> implements Callable<Void> {

    private static ThreadFactory namedFactory = new NamedThreadFactory("TimedChunkerGroup");
    private MonitoredQueue<T> buffer;
    private ChunkProcessor<T> processor;
    private ExecutorService timerPool = Executors.newSingleThreadExecutor(namedFactory);
    private int maxBufferLength;
    private int maxTimeMSec;
    private ConnectionManager<T> connectionManager;
    private Counter chunkerCancelSuccessInterrupted;
    private Counter chunkerCancelFailureInterrupted;
    private Counter chunkerCancelSuccessTimeout;
    private Counter chunkerCancelFailureTimeout;
    private Counter chunkerCancelSuccessExecution;
    private Counter chunkerCancelFailureExecution;

    public TimedChunker(MonitoredQueue<T> buffer, int maxBufferLength,
                        int maxTimeMSec, ChunkProcessor<T> processor,
                        ConnectionManager<T> connectionManager) {
        this.maxBufferLength = maxBufferLength;
        this.maxTimeMSec = maxTimeMSec;
        this.buffer = buffer;
        this.processor = processor;
        this.connectionManager = connectionManager;

        MetricGroupId metricsGroup = new MetricGroupId("TimedChunker");
        Metrics metrics = new Metrics.Builder()
                .id(metricsGroup)
                .addCounter("chunkerCancelSuccess_interrupted")
                .addCounter("chunkerCancelFailure_interrupted")
                .addCounter("chunkerCancelSuccess_timeout")
                .addCounter("chunkerCancelFailure_timeout")
                .addCounter("chunkerCancelSuccess_execution")
                .addCounter("chunkerCancelFailure_execution")
                .build();
        chunkerCancelSuccessInterrupted = metrics.getCounter("chunkerCancelSuccess_interrupted");
        chunkerCancelFailureInterrupted = metrics.getCounter("chunkerCancelFailure_interrupted");
        chunkerCancelSuccessTimeout = metrics.getCounter("chunkerCancelSuccess_timeout");
        chunkerCancelFailureTimeout = metrics.getCounter("chunkerCancelFailure_timeout");
        chunkerCancelSuccessExecution = metrics.getCounter("chunkerCancelSuccess_execution");
        chunkerCancelFailureExecution = metrics.getCounter("chunkerCancelFailure_execution");
        MetricsRegistry.getInstance().registerAndGet(metrics);
    }

    @Override
    public Void call() throws Exception {
        while (!stopCondition()) {
            boolean timeoutException = false;
            boolean executionException = false;

            T data = buffer.get();
            Future<Void> queryFuture = timerPool.submit(new Chunker<T>(processor, data, buffer, maxBufferLength,
                    connectionManager));
            try {
                queryFuture.get(maxTimeMSec, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                boolean success = queryFuture.cancel(true);
                if (success) {
                    chunkerCancelSuccessInterrupted.increment();
                } else {
                    chunkerCancelFailureInterrupted.increment();
                }
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                executionException = true;
                throw e;
            } catch (TimeoutException e) {
                // cancel task below
                timeoutException = true;
            } finally {
                // send interrupt signal to thread
                boolean success = queryFuture.cancel(true);
                if (timeoutException) {
                    if (success) {
                        chunkerCancelSuccessTimeout.increment();
                    } else {
                        chunkerCancelFailureTimeout.increment();
                    }
                } else if (executionException) {
                    if (success) {
                        chunkerCancelSuccessExecution.increment();
                    } else {
                        chunkerCancelFailureExecution.increment();
                    }
                }
            }
        }
        return null;
    }

    private boolean stopCondition() {
        return Thread.currentThread().isInterrupted();
    }
}
