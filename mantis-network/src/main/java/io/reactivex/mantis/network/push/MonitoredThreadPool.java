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

import com.mantisrx.common.utils.MantisMetricStringConstants;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MonitoredThreadPool {

    private static final Logger logger = LoggerFactory.getLogger(MonitoredThreadPool.class);
    private String name;
    private ThreadPoolExecutor threadPool;
    private MeterRegistry meterRegistry;
    private Counter rejectCount;
    private Counter exceptions;
    private Gauge activeTasks;
    private Gauge queueLength;

    public MonitoredThreadPool(String name, final ThreadPoolExecutor threadPool, MeterRegistry meterRegistry) {
        this.name = name;
        this.threadPool = threadPool;
        this.threadPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable arg0, ThreadPoolExecutor arg1) {
                rejected();
            }
        });

        final String poolId = Optional.ofNullable(name).orElse("none");
        final Tags tag = Tags.of(MantisMetricStringConstants.GROUP_ID_TAG, poolId);
        activeTasks = Gauge.builder("MonitoredThreadPool_activeTasks", () -> (double) threadPool.getActiveCount())
                .tags(tag)
                .register(meterRegistry);
        queueLength = Gauge.builder("MonitoredThreadPool_queueLength", () -> (double) threadPool.getQueue().size())
                .tags(tag)
                .register(meterRegistry);
        rejectCount = meterRegistry.counter("MonitoredThreadPool_rejectCount");
        exceptions = meterRegistry.counter("MonitoredThreadPool_exceptions");
    }

    private void rejected() {
        logger.warn("Monitored thread pool: " + name + " rejected task.");
        rejectCount.increment();
    }

    public List<Meter> getMetrics() {
        List<Meter> meters = new ArrayList<>();
        meters.add(rejectCount);
        meters.add(exceptions);
        meters.add(activeTasks);
        meters.add(queueLength);
        return meters;

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
