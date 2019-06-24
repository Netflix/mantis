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

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import com.mantisrx.common.utils.MantisMetricStringConstants;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Gauge;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.spectator.GaugeCallback;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MonitoredThreadPool {

    private static final Logger logger = LoggerFactory.getLogger(MonitoredThreadPool.class);
    private String name;
    private ThreadPoolExecutor threadPool;
    private Metrics metrics;
    private Counter rejectCount;
    private Counter exceptions;

    public MonitoredThreadPool(String name, final ThreadPoolExecutor threadPool) {
        this.name = name;
        this.threadPool = threadPool;
        this.threadPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable arg0, ThreadPoolExecutor arg1) {
                rejected();
            }
        });

        final String poolId = Optional.ofNullable(name).orElse("none");
        final BasicTag tag = new BasicTag(MantisMetricStringConstants.GROUP_ID_TAG, poolId);
        final MetricGroupId metricsGroup = new MetricGroupId("MonitoredThreadPool", tag);

        Gauge activeTasks = new GaugeCallback(metricsGroup, "activeTasks", () -> (double) threadPool.getActiveCount());

        Gauge queueLength = new GaugeCallback(metricsGroup, "queueLength", () -> (double) threadPool.getQueue().size());

        metrics = new Metrics.Builder()
                .id(metricsGroup)
                .addCounter("rejectCount")
                .addCounter("exceptions")
                .addGauge(activeTasks)
                .addGauge(queueLength)
                .build();

        rejectCount = metrics.getCounter("rejectCount");
        exceptions = metrics.getCounter("exceptions");
    }

    private void rejected() {
        logger.warn("Monitored thread pool: " + name + " rejected task.");
        rejectCount.increment();
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public int getMaxPoolSize() {
        return threadPool.getMaximumPoolSize();
    }

    public <T> Future<T> submit(final Callable<T> task) {
        // wrap with exception handling
        Future<T> future = threadPool.submit(new Callable<T>() {
            @Override
            public T call() throws Exception {
                T returnValue = null;
                try {
                    returnValue = task.call();
                } catch (Exception e) {
                    logger.warn("Exception occured in running thread", e);
                    exceptions.increment();
                }
                return returnValue;
            }
        });
        return future;
    }

    public void execute(final Runnable task) {
        // wrap with exception handling
        threadPool.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    task.run();
                } catch (Exception e) {
                    logger.warn("Exception occured in running thread", e);
                    exceptions.increment();
                }
            }

        });
    }
}
